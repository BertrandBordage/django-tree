from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from collections.abc import Iterator

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import DEFAULT_DB_ALIAS, connections, router, transaction
from django.db.models import CASCADE, DO_NOTHING, Model, QuerySet, signals
from django.db.models.deletion import Collector, get_candidate_relations_to_delete

from .fields import PathField
from .query import _get_path_fields, _get_path_field, TreeManager
from .sql.helpers import DELIMITER
from .types import Path

if TYPE_CHECKING:
    # The mixin only ever decorates a `Model` subclass (see `TreeModel`), so for
    # type-checking we give it that base to resolve `self.pk`, `self._meta`, ...;
    # at runtime it stays a plain mixin.
    _ModelBase = Model
else:
    _ModelBase = object


class _SubtreeCollector(Collector):
    """A deletion collector that does not look for the children of its rows."""

    def __init__(self, *args: Any, parent_field: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._parent_field = parent_field

    def related_objects(
        self, related_model: Any, related_fields: Any, objs: Any
    ) -> Any:
        if self._parent_field in related_fields:
            return related_model._base_manager.using(self.using).none()
        return super().related_objects(related_model, related_fields, objs)


class TreeModelMixin(_ModelBase):
    # Per-class caches of `get_ancestors()`, kept in each class's own `__dict__` so
    # that a subclass never reads the one of its parent.
    _tree_default_path_info: tuple[str, ...]
    _tree_empty_qs: QuerySet

    @classmethod
    def _get_path_fields(cls, name: str | None = None) -> list[PathField]:
        return _get_path_fields(cls, name)

    @classmethod
    def _get_path_field(cls, name: str | None) -> PathField:
        return _get_path_field(cls, name)

    def _get_path_value(self, path_field: str | None) -> Path:
        return getattr(self, self._get_path_field(path_field).name)

    def get_children(self, path_field: str | None = None) -> QuerySet:
        return self._get_path_value(path_field).get_children()

    @classmethod
    def _tree_default_path_attname(cls) -> str | None:
        # The attname of the model's only `PathField` (None with several), resolved
        # once per class because `get_ancestors()` sits on a very hot path.
        info = cls.__dict__.get('_tree_default_path_info')
        if info is None:
            fields = _get_path_fields(cls)
            info = cls._tree_default_path_info = (
                (fields[0].attname,) if len(fields) == 1 else ()
            )
        return info[0] if info else None

    @classmethod
    def _tree_empty_queryset(cls) -> QuerySet:
        # One empty queryset per model class. It stays empty whatever is chained to
        # it, and evaluating it caches the empty result, so the ancestors of a root
        # cost neither a query nor a clone.
        empty = cls.__dict__.get('_tree_empty_qs')
        if empty is None:
            empty = cls._tree_empty_qs = cls._default_manager.none()
        return empty

    def get_ancestors(
        self, include_self: bool = False, path_field: str | None = None
    ) -> QuerySet:
        if path_field is None and not include_self:
            attname = type(self)._tree_default_path_attname()
            if attname is not None:
                # Read from `__dict__` so that a deferred path is loaded by the
                # regular code below instead of here.
                value = getattr(self.__dict__.get(attname), 'value', None)
                # A root (one level delimiter) has no ancestors: no query to build.
                if value and value.count(DELIMITER) == 1:
                    return type(self)._tree_empty_queryset()
        return self._get_path_value(path_field).get_ancestors(include_self=include_self)

    def get_descendants(
        self, include_self: bool = False, path_field: str | None = None
    ) -> QuerySet:
        return self._get_path_value(path_field).get_descendants(
            include_self=include_self
        )

    def get_siblings(
        self,
        include_self: bool = False,
        queryset: QuerySet | None = None,
        path_field: str | None = None,
    ) -> QuerySet:
        return self._get_path_value(path_field).get_siblings(
            include_self=include_self, queryset=queryset
        )

    def get_prev_siblings(
        self,
        include_self: bool = False,
        queryset: QuerySet | None = None,
        path_field: str | None = None,
    ) -> QuerySet:
        return self._get_path_value(path_field).get_prev_siblings(
            include_self=include_self, queryset=queryset
        )

    def get_next_siblings(
        self,
        include_self: bool = False,
        queryset: QuerySet | None = None,
        path_field: str | None = None,
    ) -> QuerySet:
        return self._get_path_value(path_field).get_next_siblings(
            include_self=include_self, queryset=queryset
        )

    def get_prev_sibling(
        self, queryset: QuerySet | None = None, path_field: str | None = None
    ) -> Model | None:
        return self._get_path_value(path_field).get_prev_sibling(queryset=queryset)

    def get_next_sibling(
        self, queryset: QuerySet | None = None, path_field: str | None = None
    ) -> Model | None:
        return self._get_path_value(path_field).get_next_sibling(queryset=queryset)

    def get_level(self, path_field: str | None = None) -> int | None:
        return self._get_path_value(path_field).get_level()

    def is_root(self, path_field: str | None = None) -> bool | None:
        return self._get_path_value(path_field).is_root()

    def is_leaf(self, path_field: str | None = None) -> bool | None:
        return self._get_path_value(path_field).is_leaf()

    def is_ancestor_of(
        self,
        other: 'TreeModelMixin',
        include_self: bool = False,
        path_field: str | None = None,
    ) -> bool:
        return self._get_path_value(path_field).is_ancestor_of(
            other._get_path_value(path_field), include_self=include_self
        )

    def is_descendant_of(
        self,
        other: 'TreeModelMixin',
        include_self: bool = False,
        path_field: str | None = None,
    ) -> bool:
        return self._get_path_value(path_field).is_descendant_of(
            other._get_path_value(path_field), include_self=include_self
        )

    def clean(self) -> None:
        super().clean()
        if not self._state.adding:
            for path_field in self._get_path_fields():
                old_path = getattr(self, path_field.attname)
                parent_field = path_field.parent_field
                new_parent = getattr(self, parent_field.name)
                if not new_parent:
                    continue

                if not isinstance(new_parent, Model):
                    try:
                        new_parent = self.__class__._default_manager.get(
                            pk=new_parent,
                        )
                    except ObjectDoesNotExist:
                        new_parent = self

                new_parent_path = getattr(new_parent, path_field.attname)
                if new_parent_path.is_descendant_of(
                    old_path,
                    include_self=True,
                ):
                    raise ValidationError(
                        {
                            parent_field.name: ValidationError(
                                parent_field.error_messages['invalid_choice'],
                                code='invalid_choice',
                                params={'value': str(new_parent)},
                            )
                        }
                    )

    def delete(
        self, using: str | None = None, keep_parents: bool = False
    ) -> tuple[int, dict[str, int]]:
        assert self.pk is not None, (
            "%s object can't be deleted because "
            'its %s attribute is set to None.'
            % (self._meta.object_name, self._meta.pk.attname)
        )
        qs = self.get_descendants(include_self=True)
        if using is not None:
            qs = qs.using(using)
        if not keep_parents and self._can_defer_constraint_checks(using):
            if self._can_delete_subtree_in_one_query():
                return self._delete_subtree_in_one_query(qs, using)
            if self._can_skip_children_lookup():
                return self._delete_subtree_with_collector(qs)
        return qs.delete()

    def _can_defer_constraint_checks(self, using: str | None) -> bool:
        # Both shortcuts below delete a parent and its children together, which only
        # works when the database waits for the end of the transaction to check
        # foreign keys. MySQL and MariaDB check them row by row, so there Django's
        # `CASCADE` first sets the foreign key of the children to NULL. The shortcuts
        # do not, and would delete a parent before its children.
        using = using or router.db_for_write(type(self), instance=self)
        return connections[using].features.can_defer_constraint_checks

    def _can_skip_children_lookup(self) -> bool:
        # Only a `CASCADE` parent foreign key is covered by the subtree itself.
        model = type(self)
        parent_field = self._get_path_field(None).parent_field
        return (
            getattr(parent_field.remote_field, 'on_delete', None) is CASCADE
            and not (model._meta.concrete_model or model)._meta.parents
        )

    def _delete_subtree_with_collector(
        self, qs: QuerySet
    ) -> tuple[int, dict[str, int]]:
        """`QuerySet.delete()` through a collector that does not look for the
        children of the rows it collects. The whole subtree is collected already, so
        that lookup would load every row a second time. Signals, other relations and
        the returned counts are the regular ones."""
        del_query = qs.all()
        del_query._for_write = True  # type: ignore[attr-defined]
        del_query.query.select_for_update = False
        del_query.query.select_related = False
        del_query.query.clear_ordering(force=True)
        collector = _SubtreeCollector(
            using=del_query.db,
            origin=qs,
            parent_field=self._get_path_field(None).parent_field,
        )
        collector.collect(del_query)
        deleted, rows_count = collector.delete()
        # Clear the result cache, in case this queryset gets reused.
        qs._result_cache = None
        return deleted, rows_count

    def _can_delete_subtree_in_one_query(self) -> bool:
        """Whether deleting the whole subtree needs nothing from Django's
        deletion ``Collector``.

        The ``Collector`` fetches every row of the subtree, then walks the
        self-referencing ``CASCADE`` foreign key to find the children it
        already has. Deleting a subtree makes that walk pointless, as a node's
        children are in the subtree by definition, so when nothing else could
        observe or veto the deletion a single ranged ``DELETE`` is enough. This
        is only true when no signal listens to the deletion, no other model
        points at this one, there is no multi-table inheritance, and the parent
        foreign key is ``CASCADE`` (``PROTECT`` must still refuse).
        """
        model = type(self)
        opts = model._meta
        path_field = self._get_path_field(None)
        parent_field = path_field.parent_field
        if (
            (opts.concrete_model or model)._meta.parents
            or any(hasattr(f, 'bulk_related_objects') for f in opts.private_fields)
            or any(
                signal.has_listeners(model)
                for signal in (
                    signals.pre_delete,
                    signals.post_delete,
                    signals.m2m_changed,
                )
            )
        ):
            return False
        for related in get_candidate_relations_to_delete(opts):
            related_field = cast(Any, related).field
            on_delete = related_field.remote_field.on_delete
            if on_delete is DO_NOTHING:
                continue
            if related_field is parent_field and on_delete is CASCADE:
                continue
            return False
        return True

    def _delete_subtree_in_one_query(
        self, qs: QuerySet, using: str | None
    ) -> tuple[int, dict[str, int]]:
        using = using or router.db_for_write(type(self), instance=self)
        count = qs._raw_delete(using=using)
        return (count, {self._meta.label: count}) if count else (0, {})

    @classmethod
    def rebuild_paths(
        cls, db_alias: str = DEFAULT_DB_ALIAS, path_field: str | None = None
    ) -> None:
        """
        Rebuilds the paths of all the ``PathField``s
        if ``path_field`` is ``None``.  Otherwise, only paths from
        the ``PathField`` with the ``path_field`` name are rebuilt.
        """

        for field in cls._get_path_fields(path_field):
            field.rebuild(db_alias=db_alias)

    @classmethod
    def disable_tree_trigger(
        cls, db_alias: str = DEFAULT_DB_ALIAS, path_field: str | None = None
    ) -> None:
        for field in cls._get_path_fields(path_field):
            field.disable_trigger(db_alias=db_alias)

    @classmethod
    def enable_tree_trigger(
        cls, db_alias: str = DEFAULT_DB_ALIAS, path_field: str | None = None
    ) -> None:
        for field in cls._get_path_fields(path_field):
            field.enable_trigger(db_alias=db_alias)

    @classmethod
    @contextmanager
    @transaction.atomic
    def disabled_tree_trigger(
        cls, db_alias: str = DEFAULT_DB_ALIAS, path_field: str | None = None
    ) -> Iterator[None]:
        """
        Context manager for temporarily disabling django-tree triggers.

        If ``path_field`` is ``None``, disables all the triggers.
        Otherwise disables only the trigger
        """

        cls.disable_tree_trigger(db_alias=db_alias, path_field=path_field)
        try:
            yield
        finally:
            cls.enable_tree_trigger(db_alias=db_alias, path_field=path_field)


class TreeModel(TreeModelMixin, Model):
    objects = TreeManager()

    class Meta:
        abstract = True
