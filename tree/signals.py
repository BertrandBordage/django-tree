from functools import lru_cache
from typing import Any

from django.db import connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.signals import connection_created
from django.db.models import Model
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver

from tree.fields import PathField


@lru_cache(maxsize=None)
def _path_fields(sender: type[Model]) -> tuple[PathField, ...]:
    # The save signals run on *every* model, so resolve (and cache) the
    # `PathField`s once per model class instead of scanning on every save.
    return tuple(
        field for field in sender._meta.concrete_fields if isinstance(field, PathField)
    )


def _maintains_in_python(using: str) -> bool:
    # PostgreSQL maintains the tree with a database trigger; every other backend
    # computes the path in Python on the save cycle.
    return connections[using].vendor != 'postgresql'


@receiver(pre_save)
def capture_old_tree_state(
    sender: type[Model], instance: Model, using: str, **kwargs: Any
) -> None:
    if not _maintains_in_python(using):
        return
    fields = _path_fields(sender)
    if not fields:
        return
    from tree.maintenance import PathMaintainer, is_trigger_disabled

    for field in fields:
        if is_trigger_disabled(field, using):
            continue
        PathMaintainer(field, using).capture_old(instance)


@receiver(post_save)
def maintain_paths(
    sender: type[Model],
    instance: Model,
    using: str,
    created: bool = False,
    **kwargs: Any,
) -> None:
    fields = _path_fields(sender)
    if not fields:
        return
    if _maintains_in_python(using):
        from tree.maintenance import PathMaintainer

        for field in fields:
            PathMaintainer(field, using).on_save(instance, created)
        instance.__dict__.pop('_tree_old', None)

    # Drop the cached path so the next access re-reads the canonical value (the
    # trigger-computed one on PostgreSQL, the just-written one elsewhere). Django
    # only exposes `RETURNING pk`, so we cannot get the path back from the write.
    instance_dict = instance.__dict__
    for field in fields:
        instance_dict.pop(field.attname, None)


def _register_tree_path_dumper(connection: BaseDatabaseWrapper) -> None:
    """
    Make django-tree's ``Path`` type adaptable on a single DB connection.

    django-tree registers its psycopg ``Path`` dumper on the *global*
    ``psycopg.adapters`` map (in ``TreeAppConfig.ready()``), but Django's
    psycopg3 connections build their own adapter map and do not inherit that
    global registration. Without this, a raw ``Path`` reaching psycopg (outside
    the ORM's ``get_prep_value``) raises ``cannot adapt type 'Path'``. Mirrors
    ``tree.types.Path.register_psycopg3`` but targets the connection's adapters,
    dumping the path's raw ``bytes`` as ``bytea``.
    """
    if connection.vendor != 'postgresql' or connection.connection is None:
        return
    from psycopg import pq
    from tree.types import Path

    adapters = connection.connection.adapters
    for fmt in (pq.Format.TEXT, pq.Format.BINARY):
        adapters.register_dumper(Path, Path._psycopg3_dumper(fmt))


def _setup_connection(connection: BaseDatabaseWrapper) -> None:
    _register_tree_path_dumper(connection)


@receiver(connection_created)
def setup_tree_connection(
    sender: Any, connection: BaseDatabaseWrapper, **kwargs: Any
) -> None:
    # Register on every newly-opened connection (the idiomatic place for the
    # custom psycopg `Path` type).
    _setup_connection(connection)


# Also cover any connection already open before this receiver was connected
# (persistent connections — ``CONN_MAX_AGE`` is unset — would otherwise never
# pick up the registration, since ``connection_created`` only fires on new ones).
for _conn in connections.all():
    _setup_connection(_conn)
