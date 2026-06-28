from contextlib import contextmanager
import json

from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import F, FloatField, Index

try:
    from django.utils.translation import ugettext_lazy as _
except ImportError:
    # Django 3+
    from django.utils.translation import gettext_lazy as _

from .sql import postgresql
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').
# TODO: Add queryset methods like `get_descendants` in a mixin.
# TODO: Implement an alternative for other db backends.


class PathField(ArrayField):
    description = _('Tree path')

    @classmethod
    def get_indexes(cls, table_name: str, path_field_name: str):
        # Ancestor/descendant lookups are whole-path range comparisons, served by
        # the btree index backing the path itself (the `UNIQUE` constraint added by
        # `CreateTreeTrigger`). The `child_of`/`sibling_of` lookups add a depth
        # equality (`array_length(path, 1) = N`) on top of a path range; a
        # composite `(level, path)` index makes that depth + range seekable, so
        # those lookups become index scans over just the matching rows instead of
        # scanning the whole subtree and filtering by depth. Keeping `level` as the
        # leading column means level-only filters (e.g. roots, `__level=1`) still
        # use this same index.
        return [
            Index(
                F(f'{path_field_name}__level'),
                F(path_field_name),
                name=f'{table_name}_{path_field_name}_level_index',
            ),
        ]

    def __init__(self, *args, parent_field_name: str = 'parent', **kwargs):
        for kwarg in ('base_field', 'default', 'null', 'unique'):
            if kwarg in kwargs:
                raise ImproperlyConfigured('Cannot set `PathField.%s`.' % kwarg)

        # `double precision` (float8) keeps each path element to a fixed 8 bytes
        # with hardware-speed comparisons, unlike the variable-length `numeric`.
        # Paths only ever hold dyadic fractions (repeated `(prev + next) / 2`),
        # which float8 stores exactly, with ~52 bits of bisection headroom.
        kwargs['base_field'] = FloatField()
        kwargs['default'] = lambda: Path(self, None)
        kwargs.setdefault('editable', False)
        kwargs['null'] = True

        self.order_by = list(kwargs.pop('order_by', []))
        self.parent_field_name = parent_field_name

        super(PathField, self).__init__(*args, **kwargs)

    @property
    def parent_field(self):
        return self.model._meta.get_field(self.parent_field_name)

    def contribute_to_class(self, cls, name, *args, **kwargs):
        if name in self.order_by:
            raise ImproperlyConfigured(
                '`PathField.order_by` cannot reference the field itself (%r).' % name
            )
        super(PathField, self).contribute_to_class(cls, name, *args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(PathField, self).deconstruct()
        del kwargs['base_field']
        if not kwargs['editable']:
            del kwargs['editable']
        del kwargs['default']
        del kwargs['null']
        del kwargs['size']
        if self.order_by:
            kwargs['order_by'] = self.order_by
        if self.parent_field_name != 'parent':
            kwargs['parent_field_name'] = self.parent_field_name
        return name, path, args, kwargs

    def from_db_value(self, value, expression, connection):
        if isinstance(value, Path):
            return value
        return Path(self, value)

    def to_python(self, value):
        # https://docs.djangoproject.com/en/dev/howto/custom-model-fields/#converting-values-to-python-objects
        if isinstance(value, Path):
            return value
        elif isinstance(value, str):
            value = json.loads(value)
        return Path(self, value)

    def get_prep_value(self, value):
        if isinstance(value, Path):
            return value.value
        return value

    def _check_database_backend(self, db_alias):
        if connections[db_alias].vendor != 'postgresql':
            raise NotImplementedError('django-tree is only for PostgreSQL for now.')

    def rebuild(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.rebuild(self.model._meta.db_table, self.attname, db_alias=db_alias)

    def disable_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.disable_trigger(
            self.model._meta.db_table, self.attname, db_alias=db_alias
        )

    def enable_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.enable_trigger(
            self.model._meta.db_table, self.attname, db_alias=db_alias
        )

    @contextmanager
    @transaction.atomic
    def disabled_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self.disable_trigger(db_alias=db_alias)
        try:
            yield
        finally:
            self.enable_trigger(db_alias=db_alias)
