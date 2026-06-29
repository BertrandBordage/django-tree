from django.db import migrations

from tree.sql.postgresql import TREE_HELPER_FUNCTIONS


# The `bytea` path encoding relies on a few table-independent PL/pgSQL helpers
# (`tree_mid`, `tree_int_to_seg`, `tree_level`, `tree_parent_prefix`). They are
# also (re)created by every `CreateTreeTrigger`, but the functional
# `(tree_level(path), path)` index is built at `CreateModel` time, before any
# trigger exists, so they must be installed up front. Any app whose initial
# migration creates a `PathField` index must depend on this migration.
DROP_TREE_FUNCTIONS = """
    DROP FUNCTION IF EXISTS tree_mid(bytea, bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_int_to_seg(integer, integer) CASCADE;
    DROP FUNCTION IF EXISTS tree_level(bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_upper(bytea) CASCADE;
    DROP FUNCTION IF EXISTS tree_parent_prefix(bytea) CASCADE;
"""


class Migration(migrations.Migration):
    dependencies = [('tree', '0002_remove_old_functions')]

    operations = [
        migrations.RunSQL(TREE_HELPER_FUNCTIONS, DROP_TREE_FUNCTIONS),
    ]
