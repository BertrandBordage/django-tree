from typing import TYPE_CHECKING, Any, cast

from django.core.exceptions import FieldDoesNotExist
from django.db.models import Exists, Field, Model, OuterRef, QuerySet
from django.db.models.manager import Manager

from .fields import PathField
from .sql import is_trigger_backend

if TYPE_CHECKING:
    # `TreeQuerySetMixin` is only ever combined with `QuerySet` (see
    # `TreeQuerySet` below), so for type-checking we give it that base to resolve
    # `self.model`, `self.filter`, ...; at runtime it stays a plain mixin.
    _QuerySetBase = QuerySet
else:
    _QuerySetBase = object


def _get_path_fields(model: type[Model], name: str | None = None) -> list[PathField]:
    if name is None:
        return [f for f in model._meta.fields if isinstance(f, PathField)]
    return [cast(PathField, model._meta.get_field(name))]


def _watched_names(field: PathField) -> set[str]:
    # The columns the PostgreSQL trigger watches: the path itself, the parent FK
    # and every `order_by` column (by both field name and attname, since a bulk
    # `update()` may name either).
    parent_field = field.parent_field
    names = {field.name, field.attname, parent_field.name, parent_field.attname}
    for order_by in field.order_by:
        name = order_by[1:] if order_by.startswith('-') else order_by
        if name == 'pk':
            continue
        order_field = cast('Field', field.model._meta.get_field(name))
        names.update({order_field.name, order_field.attname})
    return names


def _get_path_field(model: type[Model], name: str | None) -> PathField:
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
class TreeQuerySetMixin(_QuerySetBase):
    def _get_path_field_attname(self, name: str | None) -> str:
        return _get_path_field(self.model, name).attname

    def _reconcile_tree(
        self, changed_names: set[str] | None, force_all: bool = False
    ) -> None:
        # PostgreSQL maintains the tree under bulk writes through its trigger. The
        # other backends have no trigger, so after an ORM bulk write (`update`,
        # `bulk_create`, `bulk_update`) that touches a watched column we rebuild
        # the affected `PathField`(s) in Python. Raw SQL still bypasses this --
        # there is nothing for the ORM to observe -- so it needs a manual
        # `rebuild_paths()`.
        if is_trigger_backend(self.db):
            return
        from .maintenance import is_trigger_disabled

        for field in _get_path_fields(self.model):
            if is_trigger_disabled(field, self.db):
                continue
            if force_all or (changed_names and changed_names & _watched_names(field)):
                field.rebuild(db_alias=self.db)

    def update(self, **kwargs: Any) -> int:
        if is_trigger_backend(self.db):
            return super().update(**kwargs)
        from .maintenance import PathMaintainer, is_trigger_disabled

        changed = set(kwargs)
        maintainers = [
            PathMaintainer(field, self.db)
            for field in _get_path_fields(self.model)
            if not is_trigger_disabled(field, self.db)
            and changed & _watched_names(field)
        ]
        if not maintainers:
            return super().update(**kwargs)

        # Replay the path computation per affected row, like the PostgreSQL
        # trigger, so a re-parent/reorder leaves every other path untouched (a
        # full rebuild would renumber the whole tree). The OLD values are read
        # before the update so descendants move with their subtree.
        pks = list(self.values_list('pk', flat=True))
        old_states = [(m, m.capture_old_many(pks)) for m in maintainers]
        result = super().update(**kwargs)
        base = self.model._base_manager.using(self.db)
        for instance in base.filter(pk__in=pks):
            tree_old = instance.__dict__.setdefault('_tree_old', {})
            for maintainer, olds in old_states:
                tree_old[maintainer.path_attname] = olds.get(instance.pk)
                maintainer.on_save(instance, created=False)
        return result

    def bulk_create(self, objs: Any, *args: Any, **kwargs: Any) -> list:
        objs = list(objs)
        result = super().bulk_create(objs, *args, **kwargs)
        if objs:
            self._reconcile_tree(None, force_all=True)
        return result

    def bulk_update(self, objs: Any, fields: Any, *args: Any, **kwargs: Any) -> int:
        result = super().bulk_update(objs, fields, *args, **kwargs)
        self._reconcile_tree(set(fields))
        return result

    def filter_roots(self, path_field: str | None = None) -> QuerySet:
        attname = self._get_path_field_attname(path_field)
        return self.filter(**{f'{attname}__level': 1})

    def get_descendants(
        self, include_self: bool = False, path_field: str | None = None
    ) -> QuerySet:
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


class TreeManager(Manager.from_queryset(TreeQuerySet)):  # type: ignore[misc]
    pass
