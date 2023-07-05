from django.db import migrations
from tree.fields import PathField
from tree.operations import (
    CreateTreeTrigger, RebuildPaths, DeleteTreeTrigger,
)


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0002_add_tmp_model'),
    ]

    operations = [
        migrations.AddField('Something', 'path', PathField(order_by=['name'])),
        CreateTreeTrigger('Something'),
        RebuildPaths('Something'),
        migrations.AlterField('Something', 'path', PathField(order_by=['name'])),
        DeleteTreeTrigger('Something'),
        migrations.DeleteModel('Something'),
    ]
