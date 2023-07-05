from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0001_initial')
    ]

    operations = [
        migrations.RunSQL("""
            DROP FUNCTION IF EXISTS rebuild_paths(
                table_name text, pk text, parent text, path text
            ) CASCADE;
            DROP FUNCTION IF EXISTS update_paths() CASCADE;
            DROP FUNCTION IF EXISTS from_alphanum(label text) CASCADE;
            DROP FUNCTION IF EXISTS to_alphanum(i bigint, size smallint) CASCADE;
        """),
    ]
