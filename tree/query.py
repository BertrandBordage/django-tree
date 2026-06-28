from django.core.exceptions import FieldDoesNotExist
from django.db.models import Exists, OuterRef, QuerySet
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
            'A `PathField` needs to be defined in order to use `TreeModelMixin`.'
        )
    if n == 1:
        return path_fields[0]
    raise ValueError(
        'You need to specify which `PathField` to use for this query '
        'among these values: %s' % [f.name for f in path_fields]
    )


# TODO: Implement a faster `QuerySet.delete` and add it to the benchmark.
class TreeQuerySetMixin:
    def _get_path_field_attname(self, name):
        return _get_path_field(self.model, name).attname

    def filter_roots(self, path_field=None):
        attname = self._get_path_field_attname(path_field)
        return self.filter(**{f'{attname}__level': 1})

    def get_descendants(self, include_self=False, path_field=None):
        attname = self._get_path_field_attname(path_field)
        # A row is a descendant of this queryset when one of its members is an
        # ancestor (or itself). We express that as a single correlated `EXISTS`
        # against the members, instead of fetching every member path and OR-ing
        # one range clause per member (which also needed an extra query).
        members = self.filter(**{f'{attname}__ancestor_of': OuterRef(attname)})
        result = self.model._default_manager.filter(Exists(members))
        if not include_self:
            result = result.exclude(pk__in=self.values('pk'))
        return result


class TreeQuerySet(TreeQuerySetMixin, QuerySet):
    pass


class TreeManager(Manager.from_queryset(TreeQuerySet)):
    pass
