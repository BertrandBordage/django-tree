from django.db import migrations

from tree.sql.postgresql import TREE_HELPER_FUNCTIONS


# The `bytea` path encoding relies on a few table-independent PL/pgSQL helpers
# (`tree_mid`, `tree_int_to_seg`, `tree_level`, `tree_parent_prefix`). They are
# also (re)created by every `CreateTreeTrigger`, but the functional
# `(tree_level(path), path)` index is built at `CreateModel` time, before any
# trigger exists, so they must be installed up front. Any app whose initial
# migration creates a `PathField` index must depend on this migration.
#
# These helpers are PostgreSQL-only: on other backends there is no trigger, the
# path is maintained in Python, and `tree_level` (where still needed for the
# index/lookups) is a per-connection SQLite UDF or an inline MySQL expression.
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


def drop_functions(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(DROP_TREE_FUNCTIONS, params=None)


class Migration(migrations.Migration):
    dependencies = [('tree', '0002_remove_old_functions')]

    operations = [
        migrations.RunPython(create_functions, drop_functions),
    ]
