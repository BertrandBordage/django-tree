from django.db import models, migrations
from django.db.models import CASCADE

from tree.fields import PathField
from tree.operations import CreateTreeTrigger
from tree.sql.base import ALPHANUM_LEN


class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Place',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=50)),
                ('parent', models.ForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
                ('path', PathField(order_by=('name',), max_siblings=ALPHANUM_LEN*3)),
            ],
            options={
                'ordering': ('path', 'name'),
            },
        ),
        CreateTreeTrigger('tests.Place'),
    ]
