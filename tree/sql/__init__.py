"""Which backend maintains a ``PathField``.

PostgreSQL keeps a database-side PL/pgSQL trigger (see :mod:`tree.sql.postgresql`)
that also maintains the tree under raw SQL. Every other backend has no such
trigger: the path is computed in Python (see :mod:`tree.maintenance`), so
``PathField.rebuild``/``enable_trigger``/``disable_trigger`` dispatch to that
engine and ``CreateTreeTrigger`` installs nothing.
"""

from django.db import DEFAULT_DB_ALIAS, connections


def is_trigger_backend(db_alias: str = DEFAULT_DB_ALIAS) -> bool:
    """Whether this backend maintains the tree with a database trigger."""
    return connections[db_alias].vendor == 'postgresql'
