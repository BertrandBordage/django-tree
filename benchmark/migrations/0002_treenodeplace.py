import benchmark.models
import django.core.validators
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('benchmark', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='TreeNodePlace',
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
                (
                    'tn_ancestors_pks',
                    models.TextField(
                        blank=True,
                        default='',
                        editable=False,
                        verbose_name='Ancestors pks',
                    ),
                ),
                (
                    'tn_ancestors_count',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Ancestors count'
                    ),
                ),
                (
                    'tn_children_pks',
                    models.TextField(
                        blank=True,
                        default='',
                        editable=False,
                        verbose_name='Children pks',
                    ),
                ),
                (
                    'tn_children_count',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Children count'
                    ),
                ),
                (
                    'tn_depth',
                    models.PositiveIntegerField(
                        default=0,
                        editable=False,
                        validators=[
                            django.core.validators.MinValueValidator(0),
                            django.core.validators.MaxValueValidator(10),
                        ],
                        verbose_name='Depth',
                    ),
                ),
                (
                    'tn_descendants_pks',
                    models.TextField(
                        blank=True,
                        default='',
                        editable=False,
                        verbose_name='Descendants pks',
                    ),
                ),
                (
                    'tn_descendants_count',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Descendants count'
                    ),
                ),
                (
                    'tn_index',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Index'
                    ),
                ),
                (
                    'tn_level',
                    models.PositiveIntegerField(
                        default=1,
                        editable=False,
                        validators=[
                            django.core.validators.MinValueValidator(1),
                            django.core.validators.MaxValueValidator(10),
                        ],
                        verbose_name='Level',
                    ),
                ),
                (
                    'tn_priority',
                    models.PositiveIntegerField(
                        default=0,
                        validators=[
                            django.core.validators.MinValueValidator(0),
                            django.core.validators.MaxValueValidator(9999999999),
                        ],
                        verbose_name='Priority',
                    ),
                ),
                (
                    'tn_order',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Order'
                    ),
                ),
                (
                    'tn_siblings_pks',
                    models.TextField(
                        blank=True,
                        default='',
                        editable=False,
                        verbose_name='Siblings pks',
                    ),
                ),
                (
                    'tn_siblings_count',
                    models.PositiveIntegerField(
                        default=0, editable=False, verbose_name='Siblings count'
                    ),
                ),
                (
                    'name',
                    models.CharField(
                        default=benchmark.models.get_random_name,
                        max_length=50,
                        unique=True,
                    ),
                ),
                (
                    'tn_parent',
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='tn_children',
                        to='benchmark.treenodeplace',
                        verbose_name='Parent',
                    ),
                ),
            ],
            options={
                'ordering': ['tn_order'],
                'abstract': False,
            },
        ),
    ]
