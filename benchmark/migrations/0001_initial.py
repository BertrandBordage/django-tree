from django.db import migrations
from django.db.models import (
    AutoField, CharField, ForeignKey, PositiveIntegerField, Manager, CASCADE,
)
from mptt.fields import TreeForeignKey
from tree.fields import PathField
from tree.operations import CreateTreeTrigger

from ..models import get_random_name


class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0001_initial'),
    ]

    operations = [

        migrations.CreateModel(
            name='MPTTPlace',
            fields=[
                ('id', AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', CharField(max_length=50, unique=True, default=get_random_name)),
                ('lft', PositiveIntegerField(db_index=True, editable=False)),
                ('rght', PositiveIntegerField(db_index=True, editable=False)),
                ('tree_id', PositiveIntegerField(db_index=True, editable=False)),
                ('level', PositiveIntegerField(db_index=True, editable=False)),
                ('parent', TreeForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
            ],
            managers=[
                ('objects', Manager()),
            ],
        ),

        migrations.CreateModel(
            name='TreePlace',
            fields=[
                ('id', AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', CharField(max_length=50, unique=True, default=get_random_name)),
                ('parent', ForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
                ('path', PathField(order_by=('name',), db_index=True)),
            ],
        ),
        CreateTreeTrigger('TreePlace'),

        migrations.CreateModel(
            name='TreebeardALPlace',
            fields=[
                ('id', AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', CharField(max_length=50, unique=True, default=get_random_name)),
                ('parent', ForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
            ],
        ),

        migrations.CreateModel(
            name='TreebeardMPPlace',
            fields=[
                ('id', AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('path', CharField(max_length=255, unique=True)),
                ('depth', PositiveIntegerField()),
                ('numchild', PositiveIntegerField(default=0)),
                ('name', CharField(max_length=50, unique=True, default=get_random_name)),
            ],
        ),

        migrations.CreateModel(
            name='TreebeardNSPlace',
            fields=[
                ('id', AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('lft', PositiveIntegerField(db_index=True)),
                ('rgt', PositiveIntegerField(db_index=True)),
                ('tree_id', PositiveIntegerField(db_index=True)),
                ('depth', PositiveIntegerField(db_index=True)),
                ('name', CharField(max_length=50, unique=True, default=get_random_name)),
            ],
        ),

    ]
