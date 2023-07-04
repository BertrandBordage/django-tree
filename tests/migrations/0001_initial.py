from django.db import models, migrations
from django.db.models import CASCADE, Index, Func, F

from tree.fields import PathField
from tree.models import TreeModelMixin
from tree.operations import CreateTreeTrigger


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
                ('path', PathField(order_by=('name',))),
            ],
            options={
                'ordering': ['path', 'name'],
                'indexes': [
                    Index(Func(F('path'), 1, function='trim_array'), name='place_path_parent_index'),
                    Index(F('path__len'), name='place_path_length_index'),
                ],
            },
            bases=(TreeModelMixin, models.Model),
        ),
        CreateTreeTrigger('tests.Place'),
    ]
