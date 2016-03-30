from contextlib import contextmanager
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import Field
from django.utils.translation import ugettext_lazy as _

from .sql import postgresql
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').
# TODO: Add queryset methods like `get_descendants` in a mixin.
# TODO: Add model methods like `get_descendants` in a mixin.
# TODO: Implement a way to create a GiST index (probably a migration).
# TODO: Implement an alternative using regex for other db backends.


class PathField(Field):
    description = _('Tree path')

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('editable', False)
        self.original_default = kwargs.get('default')
        kwargs['default'] = lambda: Path(self, self.original_default)
        super(PathField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(PathField, self).deconstruct()
        if not kwargs['editable']:
            del kwargs['editable']
        del kwargs['default']
        if self.original_default is not None:
            kwargs['default'] = self.original_default
        return name, path, args, kwargs

    def db_type(self, connection):
        if connection.vendor == 'postgresql':
            return 'ltree'
        raise NotImplementedError(
            'django-tree is only for PostgreSQL for now.')

    def from_db_value(self, value, expression, connection, context):
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

    def rebuild(self, db_alias=DEFAULT_DB_ALIAS):
        if connections[db_alias].vendor != 'postgresql':
            raise NotImplementedError(
                'django-tree is only for PostgreSQL for now.')
        postgresql.rebuild(self.model._meta.db_table, self.attname,
                           db_alias=db_alias)

    @contextmanager
    @transaction.atomic
    def disabled_trigger(self, db_alias=DEFAULT_DB_ALIAS):
        if connections[db_alias].vendor != 'postgresql':
            raise NotImplementedError(
                'django-tree is only for PostgreSQL for now.')
        postgresql.disable_trigger(self.model._meta.db_table, self.attname,
                                   db_alias=db_alias)
        try:
            yield
        finally:
            postgresql.enable_trigger(self.model._meta.db_table, self.attname,
                                      db_alias=db_alias)
