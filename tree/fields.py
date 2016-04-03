from contextlib import contextmanager
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import Field
from django.utils.translation import ugettext_lazy as _

from .sql import postgresql
from .types import Path


# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').
# TODO: Add queryset methods like `get_descendants` in a mixin.
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

    def _check_database_backend(self, db_alias):
        if connections[db_alias].vendor != 'postgresql':
            raise NotImplementedError(
                'django-tree is only for PostgreSQL for now.')

    def db_type(self, connection):
        self._check_database_backend(connection.alias)
        return 'ltree'

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

    # TODO: Move this method to a queryset.
    def get_roots(self):
        return self.model._default_manager.filter(
            **{self.attname + '__match': '*{1}'})

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
