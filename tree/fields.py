from django.db import transaction, DEFAULT_DB_ALIAS, connections
from django.db.models import Field, Case, When, Value
from django.utils.translation import ugettext_lazy as _

from .functions import TextToPath
from .sql.postgresql import rebuild_tree
from .types import Path


# TODO: Create a migration for `CREATE EXTENSION ltree;`.
# TODO: Create a migration for rebuilding the tree.
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
    description = _('Tree path (PostgreSQL-specific)')

    def __init__(self, *args, **kwargs):
        self.parent_field_name = kwargs.pop('parent_field', 'parent')
        self.order_by = tuple(kwargs.pop('order_by', ()))
        kwargs['editable'] = False
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
        new_paths = self._update_children_paths(parent, model_instance)
        self.model._default_manager.filter(pk__in=new_paths).update(
            path=TextToPath(Case(*[When(pk=pk, then=Value(path.value))
                                   for pk, path in new_paths.items()])))
        return getattr(model_instance, self.attname)

    def _get_parent_value(self, parent):
        if parent is not None:
            parent_value = getattr(parent, self.attname).value
            if parent_value is None:
                parent.save()
                parent_value = getattr(parent, self.attname).value
            return parent_value + '.'
        return ''

    def _update_children_paths(self, parent, model_instance=None):
        order_by = self.order_by + ('pk',)
        siblings = set(self.model._default_manager
                       .filter(parent=parent).order_by())
        if model_instance is not None:
            # This is different from the `add` argument of `pre_save`.
            # If one creates an object with a specific pk, `add` will be `True`
            # but pk will not be `None` and we would lose it when restoring it.
            new_pk = model_instance.pk is None
            if new_pk:
                # FIXME: This will only work if pk is a number field.
                model_instance.pk = -1
            siblings.add(model_instance)
        if len(siblings) > self.max_siblings:
            # TODO: Specify which command the user should run
            #       to rebuild the tree.
            raise ValueError(
                '`max_siblings` (%d) has been reached.\n'
                'You should increase it then rebuild the tree.'
                % self.max_siblings)
        parent_value = self._get_parent_value(parent)
        # FIXME: This doesn't handle descending orders.
        siblings = sorted(siblings, key=lambda o: [getattr(o, attr)
                                                   for attr in order_by])
        new_paths = {}
        for i, sibling in enumerate(siblings):
            label = to_alphanum(i).zfill(self.label_size)
            new_path = self.to_python(parent_value + label)
            if new_path != getattr(sibling, self.attname):
                new_paths[sibling.pk] = new_path
                setattr(sibling, self.attname, new_path)
                new_paths.update(self._update_children_paths(sibling))
        if model_instance is None:
            return new_paths
        if new_pk:
            model_instance.pk = None
        return new_paths

    @transaction.atomic
    def rebuild_tree(self, db_alias=DEFAULT_DB_ALIAS):
        if connections[db_alias].vendor == 'postgresql':
            rebuild_tree(self, db_alias=db_alias)
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
