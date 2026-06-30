import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, cast

from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import BinaryField, Field, F, Index, Model
from django.utils.translation import gettext_lazy as _

from .sql import is_trigger_backend
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').

# Backends django-tree knows how to maintain a tree on. PostgreSQL uses a
# database trigger; the others compute the path in Python (see `tree.sql` and
# `tree.maintenance`).
SUPPORTED_VENDORS = frozenset({'postgresql', 'sqlite', 'mysql', 'oracle'})

# MySQL cannot fully index a `BLOB` without a prefix length, so the path is a
# bounded `VARBINARY` there -- byte-comparable and fully indexable. 768 bytes is
# hundreds of levels deep; deeper trees would need a wider column.
MYSQL_PATH_BYTES = 768

# Oracle's `BinaryField` default is `BLOB`, which cannot back a btree index nor
# be range-compared byte-wise -- both of which the path lookups need. `RAW` is
# byte-ordered, indexable and range-comparable, but capped at 2000 bytes (about
# a thousand levels deep; raise `MAX_STRING_SIZE=EXTENDED` for more).
ORACLE_PATH_BYTES = 2000


class PathIndex(Index):
    """Indexes a `PathField`: a functional ``(level, path)`` index on PostgreSQL,
    a plain ``(path)`` index on every other backend.

    PostgreSQL's depth-aware lookups (`child_of`/`sibling_of`/`__level`) seek
    ``tree_level(path)``, an IMMUTABLE helper, so leading with ``level`` turns them
    into index scans over just the matching rows. The other backends have no such
    function -- a functional index could not even be rendered -- and their
    lookups only need the path range, which a plain column index serves.
    """

    def __init__(self, path_field_name: str, *, name: str) -> None:
        self.path_field_name = path_field_name
        super().__init__(
            F(f'{path_field_name}__level'),
            F(path_field_name),
            name=name,
        )

    def deconstruct(self) -> tuple[str, Sequence[Any], dict[str, Any]]:
        # Stable across backends (the per-vendor choice happens at DDL time, in
        # `create_sql`), so the same migration is correct everywhere.
        return (
            f'{self.__class__.__module__}.{self.__class__.__qualname__}',
            (self.path_field_name,),
            {'name': self.name},
        )

    def create_sql(
        self, model: type[Model], schema_editor: Any, using: str = '', **kwargs: Any
    ) -> Any:
        if schema_editor.connection.vendor == 'postgresql':
            return super().create_sql(model, schema_editor, using=using, **kwargs)
        plain = Index(fields=[self.path_field_name], name=self.name)
        return plain.create_sql(model, schema_editor, using=using, **kwargs)


class PathField(BinaryField):
    description = _('Tree path')

    @classmethod
    def get_indexes(cls, table_name: str, path_field_name: str) -> list[Index]:
        # Ancestor/descendant lookups are whole-path range comparisons, served by
        # the btree index backing the path itself. `child_of`/`sibling_of` add a
        # depth restriction on top of that range; on PostgreSQL `PathIndex` makes
        # depth + range seekable with a functional `(level, path)` index, while the
        # other backends fall back to a plain `(path)` range index (see
        # `PathIndex`).
        return [
            PathIndex(
                path_field_name,
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
        # A stored path is either NULL (no path yet) or a non-empty key. psycopg3
        # already returns `bytes` for `bytea` (so this is a no-op), but psycopg2
        # returns a `memoryview`, which can't be ordered, hashed, split or
        # `startswith`-ed -- all things `Path` relies on -- so coerce to `bytes`.
        # Oracle can't store an empty `RAW` and reads a NULL one back as `b''`
        # (Django's `convert_empty_bytes`), so treat empty as "no path".
        value = bytes(value) if value else None
        return Path(self, value)

    def to_python(self, value: 'bytes | memoryview | str | Path | None') -> Path:
        # https://docs.djangoproject.com/en/dev/howto/custom-model-fields/#converting-values-to-python-objects
        if isinstance(value, Path):
            return value
        if isinstance(value, memoryview):
            value = bytes(value)
        elif isinstance(value, str):
            # The hex string produced by `value_to_string`, fed back in by
            # deserializers (e.g. django-reversion, `loaddata`).
            value = bytes.fromhex(value)
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
        if connection.vendor == 'oracle':
            return 'RAW(%d)' % ORACLE_PATH_BYTES
        return super().db_type(connection)

    def value_to_string(self, obj: Model) -> str | None:  # type: ignore[override]
        # Django's base `BinaryField.value_to_string` assumes the stored value
        # is already `bytes` and feeds it straight to `b64encode`, but
        # `value_from_object` returns the `Path` wrapper here -- crashing
        # serializers (e.g. django-reversion) with `TypeError: a bytes-like
        # object is required, not 'Path'`. Unwrap to the underlying `bytes`
        # first, then hex-encode (rather than relying on base64) since `None`
        # also has to round-trip cleanly for paths that haven't been
        # assigned/saved yet.
        value = cast('Path | bytes | None', self.value_from_object(obj))
        if isinstance(value, Path):
            value = value.value
        if value is None:
            return None
        return json.dumps(value.hex())

    def _check_database_backend(self, db_alias: str) -> None:
        if connections[db_alias].vendor not in SUPPORTED_VENDORS:
            raise NotImplementedError(
                'django-tree does not support the %r database backend.'
                % connections[db_alias].vendor
            )

    def rebuild(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._check_database_backend(db_alias)
        if is_trigger_backend(db_alias):
            from .sql import postgresql

            postgresql.rebuild(
                self.model._meta.db_table, self.attname, db_alias=db_alias
            )
        else:
            from .maintenance import PathMaintainer

            PathMaintainer(self, db_alias).rebuild()

    def disable_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._set_trigger_enabled(db_alias, False)

    def enable_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self._set_trigger_enabled(db_alias, True)

    def _set_trigger_enabled(self, db_alias: str, enabled: bool) -> None:
        self._check_database_backend(db_alias)
        if is_trigger_backend(db_alias):
            from .sql import postgresql

            toggle = (
                postgresql.enable_trigger if enabled else postgresql.disable_trigger
            )
            toggle(self.model._meta.db_table, self.attname, db_alias=db_alias)
        else:
            from .maintenance import set_trigger_disabled

            set_trigger_disabled(self, db_alias, not enabled)

    @contextmanager
    @transaction.atomic
    def disabled_trigger(self, db_alias: str = DEFAULT_DB_ALIAS) -> Iterator[None]:
        self.disable_trigger(db_alias=db_alias)
        try:
            yield
        finally:
            self.enable_trigger(db_alias=db_alias)
