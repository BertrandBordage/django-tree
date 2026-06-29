from django.db import migrations


# Legacy PL/pgSQL functions from the `double precision[]` era. PostgreSQL-only;
# nothing to drop on backends that never had them.
DROP_OLD_FUNCTIONS = """
    DROP FUNCTION IF EXISTS rebuild_paths(
        table_name text, pk text, parent text, path text
    ) CASCADE;
    DROP FUNCTION IF EXISTS update_paths() CASCADE;
    DROP FUNCTION IF EXISTS from_alphanum(label text) CASCADE;
    DROP FUNCTION IF EXISTS to_alphanum(i bigint, size smallint) CASCADE;
"""


def drop_old_functions(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute(DROP_OLD_FUNCTIONS, params=None)


class Migration(migrations.Migration):
    dependencies = [('tree', '0001_initial')]

    operations = [
        migrations.RunPython(drop_old_functions, migrations.RunPython.noop),
    ]
