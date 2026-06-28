import uuid

import django.db.models.deletion
from django.db import migrations, models

import tree.fields
import tree.models
from tree.operations import CreateTreeTrigger


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0004_remove_person_person_path_length_index_and_more'),
    ]

    operations = [
        # --- Descending order_by ---
        migrations.CreateModel(
            name='DescendingPlace',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('path', tree.fields.PathField(order_by=['-name'])),
            ],
            options={'ordering': ['path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='descendingplace',
            name='parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to='tests.descendingplace',
            ),
        ),
        CreateTreeTrigger('tests.DescendingPlace'),
        # --- Multiple PathFields on the same model ---
        migrations.CreateModel(
            name='MultiPathPlace',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('code', models.CharField(max_length=50)),
                (
                    'name_path',
                    tree.fields.PathField(
                        order_by=['name'], parent_field_name='name_parent'
                    ),
                ),
                (
                    'code_path',
                    tree.fields.PathField(
                        order_by=['code'], parent_field_name='code_parent'
                    ),
                ),
            ],
            options={'ordering': ['name_path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='multipathplace',
            name='name_parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name='name_children',
                to='tests.multipathplace',
            ),
        ),
        migrations.AddField(
            model_name='multipathplace',
            name='code_parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name='code_children',
                to='tests.multipathplace',
            ),
        ),
        CreateTreeTrigger('tests.MultiPathPlace', 'name_path'),
        CreateTreeTrigger('tests.MultiPathPlace', 'code_path'),
        # --- Non-integer (UUID) primary key ---
        migrations.CreateModel(
            name='UUIDPlace',
            fields=[
                (
                    'id',
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('path', tree.fields.PathField(order_by=['name'])),
            ],
            options={'ordering': ['path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='uuidplace',
            name='parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to='tests.uuidplace',
            ),
        ),
        CreateTreeTrigger('tests.UUIDPlace'),
        # --- on_delete=SET_NULL ---
        migrations.CreateModel(
            name='SetNullPlace',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('path', tree.fields.PathField(order_by=['name'])),
            ],
            options={'ordering': ['path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='setnullplace',
            name='parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to='tests.setnullplace',
            ),
        ),
        CreateTreeTrigger('tests.SetNullPlace'),
        # --- on_delete=PROTECT ---
        migrations.CreateModel(
            name='ProtectPlace',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('path', tree.fields.PathField(order_by=['name'])),
            ],
            options={'ordering': ['path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='protectplace',
            name='parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                to='tests.protectplace',
            ),
        ),
        CreateTreeTrigger('tests.ProtectPlace'),
        # --- Unusual (quoting-requiring) table name ---
        # NOTE: no CreateTreeTrigger here on purpose. Building the trigger
        # function interpolates the quoted table name into the function name,
        # which currently breaks for quoted identifiers and would abort the
        # whole test database setup. `UnusualTableNameTest` installs the
        # trigger at runtime instead, so the failure stays isolated.
        migrations.CreateModel(
            name='WeirdTableNamePlace',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                ('name', models.CharField(max_length=50)),
                ('path', tree.fields.PathField(order_by=['name'])),
            ],
            options={'db_table': 'Tree Weird Table', 'ordering': ['path']},
            bases=(tree.models.TreeModelMixin, models.Model),
        ),
        migrations.AddField(
            model_name='weirdtablenameplace',
            name='parent',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to='tests.weirdtablenameplace',
            ),
        ),
    ]
