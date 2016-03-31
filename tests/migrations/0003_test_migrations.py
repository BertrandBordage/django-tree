from django.db import migrations
from tree.fields import PathField
from tree.operations import (
    CreateTreeTrigger, RebuildPaths, DeleteTreeTrigger, CreateTreeIndex,
    DeleteTreeIndex,
)


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0002_add_tmp_model'),
    ]

    operations = [
        migrations.AddField('Something', 'path', PathField(null=True)),
        CreateTreeTrigger('Something', order_by=('name',)),
        CreateTreeIndex('Something'),
        RebuildPaths('Something'),
        migrations.AlterField('Something', 'path', PathField()),
        DeleteTreeIndex('Something'),
        DeleteTreeTrigger('Something', order_by=('name',)),
        migrations.DeleteModel('Something'),
    ]
