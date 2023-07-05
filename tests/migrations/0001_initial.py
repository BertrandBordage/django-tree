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
                ('path', PathField(order_by=['name'])),
            ],
            options={
                'ordering': ['path', 'name'],
                'indexes': [
                    Index(Func(F('path'), 1, function='trim_array'), name='place_path_parent_index'),
                    Index(F('path__level'), name='place_path_level_index'),
                    Index(F('path__0_1'), name='place_path_slice_1_index'),
                    Index(F('path__0_2'), name='place_path_slice_2_index'),
                    Index(F('path__0_3'), name='place_path_slice_3_index'),
                    Index(F('path__0_4'), name='place_path_slice_4_index'),
                    Index(F('path__0_5'), name='place_path_slice_5_index'),
                ],
            },
            bases=(TreeModelMixin, models.Model),
        ),
        CreateTreeTrigger('tests.Place'),
        migrations.CreateModel(
            name='Person',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('first_name', models.CharField(blank=True, max_length=20)),
                ('last_name', models.CharField(max_length=50)),
                ('parent', models.ForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
                ('path', PathField(order_by=['last_name', 'first_name'], size=None)),
            ],
            options={
                'ordering': ['path'],
                'indexes': [
                    Index(Func(F('path'), 1, function='trim_array'), name='person_path_parent_index'),
                    Index(F('path__len'), name='person_path_length_index')
                ],
            },
            bases=(TreeModelMixin, models.Model),
        ),
        CreateTreeTrigger('tests.Person'),
    ]
