from django.db import transaction, DEFAULT_DB_ALIAS, connections
from django.db.models import Field
from django.utils.translation import ugettext_lazy as _

from .sql.postgresql import rebuild, UPDATE_SIBLINGS_SQL
from .types import Path


# TODO: Create a database trigger instead of using Field.pre_save,
#       in order to have a robust and faster system.
# TODO: Create a migration for rebuilding.
# TODO: Handle ManyToManyField('self') instead of ForeignKey('self').
# TODO: Add queryset methods like `get_descendants` in a mixin.
# TODO: Add model methods like `get_descendants` in a mixin.
# TODO: Implement a way to create a GiST index (probably a migration).
# TODO: Validate values.
# TODO: Implement an alternative using regex for other db backends.


ALPHANUM = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
ALPHANUM_LEN = len(ALPHANUM)
DEFAULT_MAX_SIBLINGS = ALPHANUM_LEN ** 4


def to_alphanum(i):
    assert i >= 0
    out = ''
    while True:
        i, remainder = divmod(i, ALPHANUM_LEN)
        out = ALPHANUM[remainder] + out
        if i == 0:
            return out


class PathField(Field):
    description = _('Tree path')

    def __init__(self, *args, **kwargs):
        self.parent_field_name = kwargs.pop('parent_field', 'parent')
        self.order_by = tuple(kwargs.pop('order_by', ()))
        kwargs['editable'] = False
        kwargs['default'] = lambda: Path(self, None)
        self.max_siblings = kwargs.pop('max_siblings', DEFAULT_MAX_SIBLINGS)
        i = self.max_siblings
        n = 0
        while i > 1.0:
            i /= ALPHANUM_LEN
            n += 1
        self.label_size = n
        super(PathField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(PathField, self).deconstruct()
        del kwargs['default']
        kwargs.update(
            parent_field=self.parent_field_name,
            order_by=self.order_by,
            max_siblings=self.max_siblings,
        )
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

    def pre_save(self, model_instance, add):
        # `pre_save` is called in a transaction by Model.save_base,
        # so we don't have to worry about atomicity.
        # TODO: Try to move this whole behaviour to SQL only.
        parent = getattr(model_instance, self.parent_field_name)

        if parent is not None:
            parent_path = getattr(parent, self.attname)
            path = getattr(model_instance, self.attname)
            if parent_path.is_descendant_of(path, include_self=True):
                # TODO: Add this behaviour to the model validation.
                raise ValueError(
                    _('Cannot set itself or a descendant as parent.'))

        self._update_siblings_paths(parent, model_instance)
        return getattr(model_instance, self.attname)

    def _get_parent_value(self, parent):
        if parent is None:
            return ''
        parent_value = getattr(parent, self.attname).value
        if parent_value is None:
            parent.save()
            parent_value = getattr(parent, self.attname).value
        return parent_value + '.'

    def _update_siblings_paths(self, parent, model_instance):
        order_by = self.order_by + ('pk',)
        python_order_by = []
        for attname in order_by:
            reversed_order = attname[0] == '-'
            if reversed_order:
                attname = attname[1:]
            python_order_by.append((attname, reversed_order))

        parent_value = self._get_parent_value(parent)

        def get_path(i):
            return parent_value + to_alphanum(i).zfill(self.label_size)

        def set_instance_path(i):
            new_path = get_path(i)
            if new_path != getattr(model_instance, self.attname):
                if model_instance.pk is not None:
                    siblings.append((model_instance.pk, new_path))
                setattr(model_instance, self.attname, self.to_python(new_path))

        i = 0

        siblings = []
        instance_located = False
        siblings_qs = self.model._default_manager.filter(parent=parent)
        if model_instance.pk is not None:
            siblings_qs = siblings_qs.exclude(pk=model_instance.pk)
        for other in siblings_qs.order_by(*order_by):
            if not instance_located:
                for attname, reversed_order in python_order_by:
                    v = getattr(model_instance, attname)
                    other_v = getattr(other, attname)
                    if reversed_order:
                        v, other_v = other_v, v
                    if v is None:
                        continue
                    if other_v is None or v < other_v:
                        instance_located = True
                        break
                if instance_located:
                    set_instance_path(i)
                    i += 1
            new_path = get_path(i)
            if new_path != getattr(other, self.attname):
                siblings.append((other.pk, new_path))
            i += 1
        if not instance_located:
            set_instance_path(i)
            i += 1

        meta = self.model._meta
        order_by = []
        for field_name in self.order_by + ('pk',):
            field = (meta.pk if field_name == 'pk'
                     else meta.get_field(field_name.lstrip('-')))
            order_by.append(
                't2."%s" %s' % (
                    field.attname,
                    'DESC' if field_name[0] == '-' else 'ASC'))
        parent_field = meta.get_field(self.parent_field_name)
        if i > self.max_siblings:
            raise ValueError(
                _('`max_siblings` (%d) has been reached.\n'
                  'You should increase it then rebuild.')
                % self.max_siblings)
        if siblings:
            # FIXME: Fetch the database alias more cleverly.
            with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
                cursor.executemany(
                    UPDATE_SIBLINGS_SQL.format(**{
                        'attname': self.attname,
                        'pk_attname': meta.pk.attname,
                        'label_size': self.label_size,
                        'table': meta.db_table,
                        'parent_attname': parent_field.attname,
                        'order_by': ', '.join(order_by),
                    }), siblings)

    @transaction.atomic
    def rebuild(self, db_alias=DEFAULT_DB_ALIAS):
        if connections[db_alias].vendor == 'postgresql':
            rebuild(self, db_alias=db_alias)
        else:
            # We force update the path of the first root node, so that all its
            # children and next siblings (so all siblings) will be updated.
            qs = self.model._default_manager.all()
            qs.update(**{self.attname: None})
            first_root_node = (
                qs.filter(**{self.field.parent_field_name + '__isnull': True})
                .order_by(*self.field.order_by + ('pk',)).first())
            if first_root_node is None:
                return
            first_root_node.save()
