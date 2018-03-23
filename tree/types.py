from django.db.models import QuerySet
from django.utils.six import string_types

from .sql.base import to_alphanum, from_alphanum


class Path:
    def __init__(self, field, value):
        self.field = field
        self.level_size = self.field.level_size
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
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return True
        return self.value < other

    def __le__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return True
        return self.value <= other

    def __gt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return True
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        return self.value > other

    def __ge__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return True
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        return self.value >= other

    def get_children(self):
        if not self.value:
            return self.qs.none()
        return self.qs.filter(**{self.attname + '__child_of': self.value})

    def get_ancestors(self, include_self=False):
        if not self.value or (self.is_root() and not include_self):
            return self.qs.none()
        paths = [self.value[:i+self.level_size]
                 for i in range(0, len(self.value), self.level_size)]
        if not include_self:
            paths.pop()
        return self.qs.filter(**{self.attname + '__in': paths})

    def get_descendants(self, include_self=False):
        if not self.value:
            return self.qs.none()
        qs = self.qs.filter(**{self.attname + '__descendant_of': self.value})
        if include_self:
            return qs
        return qs.exclude(**{self.attname: self.value})

    def get_siblings(self, include_self=False, queryset=None):
        if not self.value:
            return self.qs.none()

        qs = self.qs if queryset is None else queryset
        qs = qs.filter(**{self.attname + '__sibling_of': self.value})
        if include_self:
            return qs
        return qs.exclude(**{self.attname: self.value})

    def get_prev_siblings(self, include_self=False, queryset=None):
        if not self.value or (
                not include_self and (self.value[-self.level_size:]
                                      == self.field.first_sibling_value)):
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self,
                                     queryset=queryset)
        lookup = '__lte' if include_self else '__lt'
        return (siblings.filter(**{self.attname + lookup: self.value})
                .order_by('-' + self.attname))

    def get_next_siblings(self, include_self=False, queryset=None):
        if not self.value:
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self,
                                     queryset=queryset)
        lookup = '__gte' if include_self else '__gt'
        return (siblings.filter(**{self.attname + lookup: self.value})
                .order_by(self.attname))

    def get_prev_sibling(self, queryset=None):
        if not self.value:
            return
        current_label = self.value[-self.level_size:]
        if current_label == self.field.first_sibling_value:
            return

        if queryset is not None:
            return self.get_prev_siblings(queryset=queryset).first()

        # TODO: Handle the case where the trigger is not in place.

        prev_label = self.value[:-self.level_size] + to_alphanum(
            from_alphanum(current_label) - 1, self.level_size)
        return self.qs.get(**{self.attname: prev_label})

    def get_next_sibling(self, queryset=None):
        if not self.value:
            return None

        if queryset is not None:
            return self.get_next_siblings(queryset=queryset).first()

        # TODO: Handle the case where the trigger is not in place.

        next_label = self.value[:-self.level_size] + to_alphanum(
            from_alphanum(self.value[-self.level_size:]) + 1, self.level_size)
        return self.qs.filter(**{self.attname: next_label}).first()

    def get_level(self):
        if self.value:
            return len(self.value) // self.level_size

    def is_root(self):
        if self.value:
            return len(self.value) == self.level_size

    def is_leaf(self):
        if self.value:
            return not self.get_children().exists()

    def is_ancestor_of(self, other, include_self=False):
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        if not isinstance(other, string_types):
            raise TypeError('`other` must be a `Path` instance or a string.')
        if not include_self and self.value == other:
            return False
        return other.startswith(self.value)

    def is_descendant_of(self, other, include_self=False):
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
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
