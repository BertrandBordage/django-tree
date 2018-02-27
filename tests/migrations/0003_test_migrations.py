from django.db import migrations
from tree.fields import PathField
from tree.operations import (
    CreateTreeTrigger, RebuildPaths, DeleteTreeTrigger,
)
from tree.sql.base import ALPHANUM_LEN


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0002_add_tmp_model'),
    ]

    operations = [
        migrations.AddField('Something', 'path', PathField(order_by=('name',), max_siblings=ALPHANUM_LEN)),
        CreateTreeTrigger('Something'),
        RebuildPaths('Something'),
        migrations.AlterField('Something', 'path', PathField(order_by=('name',), max_siblings=ALPHANUM_LEN*3)),
        DeleteTreeTrigger('Something'),
        migrations.DeleteModel('Something'),
    ]
