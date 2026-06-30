from django.db import migrations

from tree.sql.oracle import DROP_TREE_LEVEL_FUNCTION, TREE_LEVEL_FUNCTION
from tree.sql.postgresql import TREE_HELPER_FUNCTIONS


# The `bytea` path encoding relies on a few table-independent PL/pgSQL helpers
# (`tree_mid`, `tree_int_to_seg`, `tree_level`, `tree_parent_prefix`). They are
# also (re)created by every `CreateTreeTrigger`, but the functional
# `(tree_level(path), path)` index is built at `CreateModel` time, before any
# trigger exists, so they must be installed up front. Any app whose initial
# migration creates a `PathField` index must depend on this migration.
#
# On PostgreSQL these helpers back both the trigger and the functional
# `(tree_level(path), path)` index. On Oracle there is no trigger (the path is
# maintained in Python), but `tree_level` still has no portable byte-correct SQL
# form on `RAW`, so a single deterministic helper is installed for the
# `child_of`/`sibling_of` lookups (see `tree.sql.oracle`). SQLite and MySQL need
# nothing here -- their `tree_level` is a per-connection UDF / inline expression.
DROP_TREE_FUNCTIONS = """
    DROP FUNCTION IF EXISTS tree_mid(bytea, bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_int_to_seg(integer, integer) CASCADE;
    DROP FUNCTION IF EXISTS tree_level(bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_upper(bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_parent_prefix(bytea) CASCADE;
"""


def create_functions(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(TREE_HELPER_FUNCTIONS, params=None)
    elif schema_editor.connection.vendor == 'oracle':
        schema_editor.execute(TREE_LEVEL_FUNCTION, params=None)


def drop_functions(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(DROP_TREE_FUNCTIONS, params=None)
    elif schema_editor.connection.vendor == 'oracle':
        schema_editor.execute(DROP_TREE_LEVEL_FUNCTION, params=None)


class Migration(migrations.Migration):
    dependencies = [('tree', '0002_remove_old_functions')]

    operations = [
        # `atomic=False`: Oracle can't roll back DDL, so creating the helper
        # function must not run inside a transaction. Harmless on PostgreSQL (it
        # keeps the migration's own transaction).
        migrations.RunPython(create_functions, drop_functions, atomic=False),
    ]
