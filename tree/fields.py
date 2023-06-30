from contextlib import contextmanager

from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import DecimalField
from django.utils.translation import ugettext_lazy as _

from .sql import postgresql
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').
# TODO: Add queryset methods like `get_descendants` in a mixin.
# TODO: Implement an alternative using regex for other db backends.


class PathField(ArrayField):
    description = _('Tree path')

    def __init__(self, *args, **kwargs):
        for kwarg in ('base_field', 'default', 'null', 'unique'):
            if kwarg in kwargs:
                raise ImproperlyConfigured('Cannot set `PathField.%s`.'
                                           % kwarg)

        kwargs['base_field'] = DecimalField(max_digits=20, decimal_places=10)
        kwargs['default'] = lambda: Path(self, None)
        kwargs.setdefault('editable', False)
        kwargs['null'] = True

        self.order_by = tuple(kwargs.pop('order_by', ()))

        super(PathField, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name, *args, **kwargs):
        if name in self.order_by:
            raise ImproperlyConfigured(
                '`PathField.order_by` cannot reference itself.' % name)
        super(PathField, self).contribute_to_class(cls, name, *args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(PathField, self).deconstruct()
        del kwargs['base_field']
        if not kwargs['editable']:
            del kwargs['editable']
        del kwargs['default']
        del kwargs['null']
        if self.order_by != ():
            kwargs['order_by'] = self.order_by
        return name, path, args, kwargs

    def from_db_value(self, value, expression, connection):
        if isinstance(value, Path):
            return value
        return Path(self, value)

    def to_python(self, value):
        if isinstance(value, Path):
            return value
        return Path(self, value)

    def get_prep_value(self, value):
        if isinstance(value, Path):
            return value.value
        return value

    # TODO: Move this method to a queryset.
    def get_roots(self):
        return self.model._default_manager.filter(
            **{self.attname + '__len': 1})

    def _check_database_backend(self, db_alias):
        if connections[db_alias].vendor != 'postgresql':
            raise NotImplementedError(
                'django-tree is only for PostgreSQL for now.')

    def rebuild(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.rebuild(self.model._meta.db_table, self.attname,
                           db_alias=db_alias)

    def disable_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.disable_trigger(self.model._meta.db_table, self.attname,
                                   db_alias=db_alias)

    def enable_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self._check_database_backend(db_alias)
        postgresql.enable_trigger(self.model._meta.db_table, self.attname,
                                  db_alias=db_alias)

    @contextmanager
    @transaction.atomic
    def disabled_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        self.disable_trigger(db_alias=db_alias)
        try:
            yield
        finally:
            self.enable_trigger(db_alias=db_alias)
