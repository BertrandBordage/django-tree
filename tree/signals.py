from functools import lru_cache
from typing import Any

from django.db import connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.signals import connection_created
from django.db.models import Model
from django.db.models.signals import post_save
from django.dispatch import receiver

from tree.fields import PathField


@lru_cache(maxsize=None)
def _path_attnames(sender: type[Model]) -> tuple[str, ...]:
    # `defer_paths` runs on *every* model's save, so resolve (and cache) which
    # attributes are `PathField`s once per model class instead of scanning
    # `concrete_fields` and running `isinstance` on every single save.
    return tuple(
        field.attname
        for field in sender._meta.concrete_fields
        if isinstance(field, PathField)
    )


@receiver(post_save)
def defer_paths(sender: type[Model], **kwargs: Any) -> None:
    attnames = _path_attnames(sender)
    if not attnames:
        return
    instance_dict = kwargs['instance'].__dict__
    for attname in attnames:
        # Removes the cached value for the field, making it deferred.
        # That way, Django will run a new query to know what is
        # the new path, only if it is used.
        # I wish we could make Django receive paths from SQL
        # through `RETURNING`, but unfortunately the ORM
        # only uses `RETURNING pk`.
        instance_dict.pop(attname, None)


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


@receiver(connection_created)
def register_tree_path_psycopg_dumper(
    sender: Any, connection: BaseDatabaseWrapper, **kwargs: Any
) -> None:
    # Register on every newly-opened connection (the idiomatic place for
    # custom psycopg types).
    _register_tree_path_dumper(connection)


# Also cover any connection already open before this receiver was connected
# (persistent connections — ``CONN_MAX_AGE`` is unset — would otherwise never
# pick up the dumper, since ``connection_created`` only fires on new ones).
for _conn in connections.all():
    _register_tree_path_dumper(_conn)
