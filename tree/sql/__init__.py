"""Backend dispatch for the operations that maintain a ``PathField``.

PostgreSQL keeps a database-side PL/pgSQL trigger (see :mod:`tree.sql.postgresql`)
that also maintains the tree under raw SQL. Every other backend has no such
trigger: the path is computed in Python (see :mod:`tree.maintenance`), so
``rebuild``/``enable_trigger``/``disable_trigger`` dispatch to that engine and
``CreateTreeTrigger`` installs nothing.
"""

from typing import TYPE_CHECKING

from django.db import DEFAULT_DB_ALIAS, connections

from . import postgresql

if TYPE_CHECKING:
    from ..fields import PathField


def is_trigger_backend(db_alias: str = DEFAULT_DB_ALIAS) -> bool:
    """Whether this backend maintains the tree with a database trigger."""
    return connections[db_alias].vendor == 'postgresql'


def rebuild(field: 'PathField', db_alias: str = DEFAULT_DB_ALIAS) -> None:
    if is_trigger_backend(db_alias):
        postgresql.rebuild(field.model._meta.db_table, field.attname, db_alias=db_alias)
        return
    from ..maintenance import PathMaintainer

    PathMaintainer(field, db_alias).rebuild()


def disable_trigger(field: 'PathField', db_alias: str = DEFAULT_DB_ALIAS) -> None:
    if is_trigger_backend(db_alias):
        postgresql.disable_trigger(
            field.model._meta.db_table, field.attname, db_alias=db_alias
        )
        return
    from ..maintenance import set_trigger_disabled

    set_trigger_disabled(field, db_alias, True)


def enable_trigger(field: 'PathField', db_alias: str = DEFAULT_DB_ALIAS) -> None:
    if is_trigger_backend(db_alias):
        postgresql.enable_trigger(
            field.model._meta.db_table, field.attname, db_alias=db_alias
        )
        return
    from ..maintenance import set_trigger_disabled

    set_trigger_disabled(field, db_alias, False)
