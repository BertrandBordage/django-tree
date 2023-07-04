import operator
from functools import reduce

from django.core.exceptions import FieldDoesNotExist
from django.db.models import QuerySet, Q
from django.db.models.manager import Manager

from .fields import PathField


def _get_path_fields(model, name=None):
    if name is None:
        return [f for f in model._meta.fields if isinstance(f, PathField)]
    return [model._meta.get_field(name)]


def _get_path_field(model, name):
    path_fields = _get_path_fields(model, name)
    n = len(path_fields)
    if n == 0:
        raise FieldDoesNotExist(
            'A `PathField` needs to be defined '
            'in order to use `TreeModelMixin`.')
    if n == 1:
        return path_fields[0]
    raise ValueError(
        'You need to specify which `PathField` to use for this query '
        'among these values: %s' % [f.name for f in path_fields])


# TODO: Implement a faster `QuerySet.delete` and add it to the benchmark.
class TreeQuerySetMixin:
    def _get_path_field_name(self, name):
        return _get_path_field(self.model, name).name

    def get_descendants(self, include_self=False, path_field=None):
        name = self._get_path_field_name(path_field)
        # TODO: Avoid doing an extra query.
        ancestor_paths = list(self.values_list(name, flat=True))
        queryset = self.model.objects.all()
        if not include_self:
            queryset = queryset.exclude(**{name + '__in': ancestor_paths})
        return queryset.filter(
            reduce(operator.or_, [
                Q(**{name + '__descendant_of': ancestor_path})
                for ancestor_path in ancestor_paths
            ])
        )


class TreeQuerySet(TreeQuerySetMixin, QuerySet):
    pass


class TreeManager(Manager.from_queryset(TreeQuerySet)):
    pass
