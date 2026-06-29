from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, cast

from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import BinaryField, Field, F, Index, Model
from django.utils.translation import gettext_lazy as _

from . import sql
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').

# Backends django-tree knows how to maintain a tree on. PostgreSQL uses a
# database trigger; the others compute the path in Python (see `tree.sql` and
# `tree.maintenance`).
SUPPORTED_VENDORS = frozenset({'postgresql', 'sqlite', 'mysql'})

# MySQL cannot fully index a `BLOB` without a prefix length, so the path is a
# bounded `VARBINARY` there -- byte-comparable and fully indexable. 768 bytes is
# hundreds of levels deep; deeper trees would need a wider column.
MYSQL_PATH_BYTES = 768


class PathField(BinaryField):
    description = _('Tree path')

    @classmethod
    def get_indexes(cls, table_name: str, path_field_name: str) -> list[Index]:
        # Ancestor/descendant lookups are whole-path range comparisons, served by
        # the btree index backing the path itself (the `UNIQUE` constraint added by
        # `CreateTreeTrigger`). The `child_of`/`sibling_of` lookups add a depth
        # equality (`tree_level(path) = N`) on top of a path range; a composite
        # `(level, path)` index makes that depth + range seekable, so those lookups
        # become index scans over just the matching rows instead of scanning the
        # whole subtree and filtering by depth. Keeping `level` as the leading
        # column means level-only filters (e.g. roots, `__level=1`) still use this
        # same index. `level` resolves to `tree_level(path)`, an IMMUTABLE helper
        # that must already exist (created by the `tree` migration) when this index
        # is built.
        return [
            Index(
                F(f'{path_field_name}__level'),
                F(path_field_name),
                name=f'{table_name}_{path_field_name}_level_index',
            ),
        ]

    def __init__(
        self, *args: Any, parent_field_name: str = 'parent', **kwargs: Any
    ) -> None:
        for kwarg in ('default', 'null', 'unique'):
            if kwarg in kwargs:
                raise ImproperlyConfigured('Cannot set `PathField.%s`.' % kwarg)

        kwargs['default'] = lambda: Path(self, None)
        kwargs.setdefault('editable', False)
        kwargs['null'] = True

        self.order_by: list[str] = list(kwargs.pop('order_by', []))
        self.parent_field_name = parent_field_name

        super(PathField, self).__init__(*args, **kwargs)

    @property
    def parent_field(self) -> Field:
        return cast(Field, self.model._meta.get_field(self.parent_field_name))

    def contribute_to_class(
        self, cls: type[Model], name: str, *args: Any, **kwargs: Any
    ) -> None:
        if name in self.order_by:
            raise ImproperlyConfigured('`PathField.order_by` cannot reference itself.')
        super(PathField, self).contribute_to_class(cls, name, *args, **kwargs)

    def deconstruct(self) -> tuple[str, str, Sequence[Any], dict[str, Any]]:
        name, path, args, kwargs = super(PathField, self).deconstruct()
        del kwargs['default']
        del kwargs['null']
        if self.order_by:
            kwargs['order_by'] = self.order_by
        if self.parent_field_name != 'parent':
            kwargs['parent_field_name'] = self.parent_field_name
        return name, path, args, kwargs

    def from_db_value(
        self,
        value: 'bytes | memoryview | Path | None',
        expression: Any,
        connection: Any,
    ) -> Path:
        if isinstance(value, Path):
            return value
        if value is not None:
            # psycopg3 already returns `bytes` for `bytea` (so this is a no-op),
            # but psycopg2 returns a `memoryview`, which can't be ordered, hashed,
            # split or `startswith`-ed -- all things `Path` relies on -- so coerce
            # to `bytes` either way.
            value = bytes(value)
        return Path(self, value)

    def to_python(self, value: 'bytes | memoryview | Path | None') -> Path:
        # https://docs.djangoproject.com/en/dev/howto/custom-model-fields/#converting-values-to-python-objects
        if isinstance(value, Path):
            return value
        if isinstance(value, memoryview):
            value = bytes(value)
        return Path(self, value)

    def get_prep_value(
        self, value: 'Path | bytes | bytearray | memoryview | None'
    ) -> bytes | None:
        if isinstance(value, Path):
            return value.value
        if isinstance(value, (memoryview, bytearray)):
            return bytes(value)
        return value

    def db_type(self, connection: Any) -> str | None:
        if connection.vendor == 'mysql':
            return 'varbinary(%d)' % MYSQL_PATH_BYTES
        return super().db_type(connection)

    def _check_database_backend(self, db_alias: str) -> None:
        if connections[db_alias].vendor not in SUPPORTED_VENDORS:
            raise NotImplementedError(
                'django-tree does not support the %r database backend.'
                % connections[db_alias].vendor
            )

    def rebuild(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._check_database_backend(db_alias)
        sql.rebuild(self, db_alias=db_alias)

    def disable_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._check_database_backend(db_alias)
        sql.disable_trigger(self, db_alias=db_alias)

    def enable_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._check_database_backend(db_alias)
        sql.enable_trigger(self, db_alias=db_alias)

    @contextmanager
    @transaction.atomic
    def disabled_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> Iterator[None]:
        self.disable_trigger(db_alias=db_alias)
        try:
            yield
        finally:
            self.enable_trigger(db_alias=db_alias)
