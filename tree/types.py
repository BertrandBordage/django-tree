from django.db import transaction
from django.db.models import QuerySet


class Path:
    def __init__(self, field, value):
        self.field = field
        self.name = getattr(self.field, 'attname', None)
        self.field_bound = self.name is not None
        self.qs = (self.field.model._default_manager.all()
                   if self.field_bound else QuerySet())
        self.value = value

    def __repr__(self):
        if self.field_bound:
            return '<Path %s %s>' % (self.field, self.value)
        return '<Path %s>' % self.value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return self.value == other.value

    def __le__(self, other):
        # We simulate the effects of a NULLS LAST.
        if self.value is None:
            return False
        if isinstance(other, str):
            return self.value <= other
        if other.value is None:
            return True
        return self.value <= other.value

    def get_children(self):
        if self.value is None:
            return self.qs.none()
        return self.qs.filter(**{self.name + '__match': self.value + '.*{1}'})

    def get_ancestors(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        if not include_self:
            qs = qs.exclude(**{self.name: self.value})
        return qs.filter(**{self.name + '__ancestor_of': self.value})

    def get_descendants(self, include_self=False):
        if self.value is None:
            return self.qs.none()
        qs = self.qs
        if not include_self:
            qs = qs.exclude(**{self.name: self.value})
        return qs.filter(**{self.name + '__descendant_of': self.value})

    def get_siblings(self):
        if self.value is None:
            return self.qs.none()
        match = '*{1}'
        if not self.is_root:
            match = self.value.rsplit('.', 1)[0] + '.' + match
        return self.qs.filter(**{self.name + '__match': match})

    def get_prev_siblings(self):
        if self.value is None:
            return self.qs.none()
        siblings = self.get_siblings()
        return (siblings.filter(**{self.name + '__lt': self.value})
                .order_by('-' + self.name))

    def get_next_siblings(self):
        if self.value is None:
            return self.qs.none()
        siblings = self.get_siblings()
        return (siblings.filter(**{self.name + '__gt': self.value})
                .order_by(self.name))

    def get_prev_sibling(self):
        return self.get_prev_siblings().first()

    def get_next_sibling(self):
        return self.get_next_siblings().first()

    @property
    def level(self):
        return self.value.count('.') + 1

    @property
    def is_root(self):
        return '.' not in self.value

    @property
    def is_leaf(self):
        return not self.get_children().exists()

    # FIXME: Move this method somewhere else.
    @transaction.atomic
    def rebuild_tree(self):
        # We force update the path of the first root node, so that all its
        # children and next siblings (so all siblings) will be updated.
        self.qs.update(**{self.name: None})
        first_root_node = (
            self.qs.filter(**{self.field.parent_field_name + '__isnull': True})
            .order_by(*self.field.order_by + ('pk',)).first())
        if first_root_node is None:
            return
        first_root_node.save()
