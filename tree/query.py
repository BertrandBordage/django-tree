from django.core.exceptions import FieldDoesNotExist
from django.db.models import QuerySet
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


class TreeQuerySetMixin:
    def _get_path_field_name(self, name):
        return _get_path_field(self.model, name).name

    def get_descendants(self, include_self=False, path_field=None):
        name = self._get_path_field_name(path_field)
        # TODO: Avoids doing an extra query.
        pattern = r'^(%s)' % '|'.join([
            p.value for p in self.values_list(name, flat=True)])
        if not include_self:
            pattern += r'.'
        return self.model.objects.filter(**{name + '__regex': pattern})


class TreeQuerySet(TreeQuerySetMixin, QuerySet):
    pass


class TreeManager(Manager.from_queryset(TreeQuerySet)):
    pass
