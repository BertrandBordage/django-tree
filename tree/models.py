from contextlib import contextmanager
from django.core.exceptions import FieldDoesNotExist
from django.db import DEFAULT_DB_ALIAS, transaction

from .fields import PathField


class TreeModelMixin:
    @classmethod
    def _get_path_fields(cls, name=None):
        if name is None:
            return [f for f in cls._meta.fields if isinstance(f, PathField)]
        return [cls._meta.get_field(name)]

    @classmethod
    def _get_path_field(cls, name):
        path_fields = cls._get_path_fields(name)
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

    def _get_path_value(self, path_field):
        return getattr(self, self._get_path_field(path_field).name)

    def get_children(self, path_field=None):
        return self._get_path_value(path_field).get_children()

    def get_ancestors(self, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .get_ancestors(include_self=include_self))

    def get_descendants(self, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .get_descendants(include_self=include_self))

    def get_siblings(self, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .get_siblings(include_self=include_self))

    def get_prev_siblings(self, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .get_prev_siblings(include_self=include_self))

    def get_next_siblings(self, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .get_next_siblings(include_self=include_self))

    def get_prev_sibling(self, path_field=None):
        return (self._get_path_value(path_field)
                .get_prev_sibling())

    def get_next_sibling(self, path_field=None):
        return (self._get_path_value(path_field)
                .get_next_sibling())

    def get_level(self, path_field=None):
        return self._get_path_value(path_field).get_level()

    def is_root(self, path_field=None):
        return self._get_path_value(path_field).is_root()

    def is_leaf(self, path_field=None):
        return self._get_path_value(path_field).is_leaf()

    def is_ancestor_of(self, other, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .is_ancestor_of(other._get_path_value(path_field),
                                include_self=include_self))

    def is_descendant_of(self, other, include_self=False, path_field=None):
        return (self._get_path_value(path_field)
                .is_descendant_of(other._get_path_value(path_field),
                                  include_self=include_self))

    # TODO: Remove this method.
    @classmethod
    def get_roots(cls, path_field=None):
        return cls._get_path_field(path_field).get_roots()

    def delete(self, using=None, **kwargs):
        assert self.pk is not None, (
            "%s object can't be deleted because "
            "its %s attribute is set to None." %
            (self._meta.object_name, self._meta.pk.attname)
        )
        qs = self.get_descendants(include_self=True)
        if using is not None:
            qs = qs.using(using)
        return qs.delete()

    @classmethod
    def rebuild_paths(cls, db_alias=DEFAULT_DB_ALIAS, path_field=None):
        """
        Rebuilds the paths of all the ``PathField``s
        if ``path_field`` is ``None``.  Otherwise, only paths from
        the ``PathField`` with the ``path_field`` name are rebuilt.
        """

        for field in cls._get_path_fields(path_field):
            field.rebuild(db_alias=db_alias)

    @classmethod
    def disable_tree_trigger(cls, db_alias=DEFAULT_DB_ALIAS, path_field=None):
        for field in cls._get_path_fields(path_field):
            field.disable_trigger(db_alias=db_alias)

    @classmethod
    def enable_tree_trigger(cls, db_alias=DEFAULT_DB_ALIAS, path_field=None):
        for field in cls._get_path_fields(path_field):
            field.enable_trigger(db_alias=db_alias)

    @classmethod
    @contextmanager
    @transaction.atomic
    def disabled_tree_trigger(cls, db_alias=DEFAULT_DB_ALIAS, path_field=None):
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
