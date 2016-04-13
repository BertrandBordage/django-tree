from django.db.models import QuerySet
from django.utils.six import string_types

from .sql.base import to_alphanum, from_alphanum


class Path:
    def __init__(self, field, value):
        self.field = field
        self.attname = getattr(self.field, 'attname', None)
        self.field_bound = self.attname is not None
        self.qs = (self.field.model._default_manager.all()
                   if self.field_bound else QuerySet())
        self.value = value

    def __repr__(self):
        if self.field_bound:
            return '<Path %s %s>' % (self.field, self.value)
        return '<Path %s>' % self.value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        if isinstance(other, Path):
            other = other.value
        return self.value == other

    def __ne__(self, other):
        if isinstance(other, Path):
            other = other.value
        return self.value != other

    def __lt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return False
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return True
        return self.value < other

    def __le__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return False
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return True
        return self.value <= other

    def __gt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return True
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return False
        return self.value > other

    def __ge__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return True
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return False
        return self.value >= other

    def get_children(self):
        if self.value is None:
            return self.qs.none()
        return self.qs.filter(
            **{self.attname + '__match': self.value + '.*{1}'})

    def get_ancestors(self, include_self=False):
        if self.value is None or (self.is_root() and not include_self):
            return self.qs.none()
        paths = []
        path = ''
        for part in self.value.split('.'):
            if path:
                path += '.'
            path += part
            paths.append(path)
        if not include_self:
            paths.pop()
        return self.qs.filter(**{self.attname + '__in': paths})

    def get_descendants(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        return self.qs.filter(
            **{self.attname + '__match': self.value + ('.*' if include_self
                                                       else '.*{1,}')})

    def get_siblings(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        match = '*{1}'
        if not self.is_root():
            match = self.value.rsplit('.', 1)[0] + '.' + match
        if not include_self:
            qs = qs.exclude(**{self.attname: self.value})
        return qs.filter(**{self.attname + '__match': match})

    def get_prev_siblings(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self)
        lookup = '__lte' if include_self else '__lt'
        return (siblings.filter(**{self.attname + lookup: self.value})
                .order_by('-' + self.attname))

    def get_next_siblings(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self)
        lookup = '__gte' if include_self else '__gt'
        return (siblings.filter(**{self.attname + lookup: self.value})
                .order_by(self.attname))

    def get_prev_sibling(self):
        if self.value is None:
            return None

        # TODO: Handle the case where the trigger is not in place.

        if self.is_root():
            parent_path = ''
            current_label = self.value
        else:
            parent_path, current_label = self.value.rsplit('.', 1)
            parent_path += '.'
        if not current_label.lstrip('0'):
            return
        prev_label = parent_path + to_alphanum(
            from_alphanum(current_label) - 1, len(current_label))
        return self.qs.get(**{self.attname: prev_label})

    def get_next_sibling(self):
        if self.value is None:
            return None

        # TODO: Handle the case where the trigger is not in place.

        if self.is_root():
            parent_path = ''
            current_label = self.value
        else:
            parent_path, current_label = self.value.rsplit('.', 1)
            parent_path += '.'
        next_label = parent_path + to_alphanum(
            from_alphanum(current_label) + 1, len(current_label))
        return self.qs.filter(**{self.attname: next_label}).first()

    def get_level(self):
        if self.value is not None:
            return self.value.count('.') + 1

    def is_root(self):
        if self.value is not None:
            return '.' not in self.value

    def is_leaf(self):
        if self.value is not None:
            return not self.get_children().exists()

    def is_ancestor_of(self, other, include_self=False):
        if self.value is None:
            return False
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return False
        if not isinstance(other, string_types):
            raise TypeError('`other` must be a `Path` instance or a string.')
        if not include_self and self.value == other:
            return False
        return other.startswith(self.value)

    def is_descendant_of(self, other, include_self=False):
        if self.value is None:
            return False
        if isinstance(other, Path):
            other = other.value
        if other is None:
            return False
        if not isinstance(other, string_types):
            raise TypeError('`other` must be a `Path` instance or a string.')
        if not include_self and self.value == other:
            return False
        return self.value.startswith(other)


# Tells psycopg2 how to prepare a Path object for the database,
# in case it doesn't go through the ORM.
try:
    import psycopg2
except ImportError:
    pass
else:
    from psycopg2.extensions import adapt, register_adapter, AsIs

    def adapt_path(path):
        return AsIs('%s::ltree' % adapt(path.value))

    register_adapter(Path, adapt_path)
