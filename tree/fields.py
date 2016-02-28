from django.db.models import Field
from django.utils.translation import ugettext_lazy as _

from .types import Path


# TODO: Create a migration for `CREATE EXTENSION ltree;`.
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
        # so we don’t have to worry about atomicity.
        # TODO: Re-implement this method in an efficient way using
        #       bulk updates.
        old_path = self.to_python(getattr(model_instance, self.attname))
        parent = getattr(model_instance, self.parent_field_name)
        value = ''
        if parent is not None:
            parent_value = getattr(parent, self.attname).value
            if parent_value is None:
                parent.save()
                parent_value = getattr(parent, self.attname).value
            value = parent_value + '.'

        position, siblings = self._get_position_among_siblings(model_instance,
                                                               parent, add)
        value += to_alphanum(position).zfill(self.label_size)
        new_path = self.to_python(value)
        setattr(model_instance, self.attname, new_path)
        if new_path != old_path:
            self._update_others(model_instance, old_path, new_path, siblings)
        return new_path

    def _get_position_among_siblings(self, model_instance, parent, add):
        order_by = self.order_by + ('pk',)
        siblings = set(model_instance._meta.
                       model._default_manager.filter(parent=parent))
        if add:
            model_instance.pk = max([s.pk for s in siblings] or [0]) + 1
        siblings.add(model_instance)
        if len(siblings) > self.max_siblings:
            # TODO: Specify which command the user should run
            #       to rebuild the tree.
            raise ValueError(
                '`max_siblings` (%d) has been reached.\n'
                'You should increase it then rebuild the tree.'
                % self.max_siblings)
        siblings = sorted(siblings, key=lambda o: [getattr(o, attr)
                                                   for attr in order_by])
        position = siblings.index(model_instance)
        # We remove the current instance to avoid meeting it when we update
        # siblings.
        siblings.remove(model_instance)
        if add:
            model_instance.pk = None
        return position, siblings

    def _update_others(self, model_instance, old_path, new_path, siblings):
        if model_instance.pk is not None:
            children = (model_instance._meta.
                        model._default_manager.filter(parent=model_instance))
            for child in children:
                # This sets the path value of the parent to the correct value,
                # while skipping an extra SQL query for fetching the parent.
                child.parent = model_instance
                child._can_update_siblings = False
                child.save()
        if getattr(model_instance, '_can_update_siblings', True):
            for sibling in siblings:
                if sibling.path >= old_path or sibling.path >= new_path:
                    sibling._can_update_siblings = False
                    sibling.save()
