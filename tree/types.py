from django.db.models import QuerySet


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
        if isinstance(other, str):
            return self.value == other
        return self.value == other.value

    def __lt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return False
        if isinstance(other, str):
            return self.value < other
        if other.value is None:
            return True
        return self.value < other.value

    def __le__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return False
        if isinstance(other, str):
            return self.value <= other
        if other.value is None:
            return True
        return self.value <= other.value

    def __gt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return True
        if isinstance(other, str):
            return self.value > other
        if other.value is None:
            return False
        return self.value > other.value

    def __ge__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return True
        if isinstance(other, str):
            return self.value >= other
        if other.value is None:
            return False
        return self.value >= other.value

    def get_children(self):
        if self.value is None:
            return self.qs.none()
        return self.qs.filter(
            **{self.attname + '__match': self.value + '.*{1}'})

    def get_ancestors(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        if not include_self:
            qs = qs.exclude(**{self.attname: self.value})
        return qs.filter(**{self.attname + '__ancestor_of': self.value})

    def get_descendants(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        if not include_self:
            qs = qs.exclude(**{self.attname: self.value})
        return qs.filter(**{self.attname + '__descendant_of': self.value})

    def get_siblings(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        match = '*{1}'
        if not self.is_root:
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
        return self.get_prev_siblings().first()

    def get_next_sibling(self):
        return self.get_next_siblings().first()

    @property
    def level(self):
        if self.value is not None:
            return self.value.count('.') + 1

    @property
    def parent(self):
        parent_value = (None if self.value is None or self.is_root
                        else self.value.rsplit('.', 1)[0])
        return self.__class__(self.field, parent_value)

    @property
    def is_root(self):
        if self.value is not None:
            return '.' not in self.value

    @property
    def is_leaf(self):
        return not self.get_children().exists()
