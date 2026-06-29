import benchmark.models
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('benchmark', '0002_treenodeplace'),
    ]

    operations = [
        migrations.CreateModel(
            name='TreeQueriesPlace',
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
                    'name',
                    models.CharField(
                        default=benchmark.models.get_random_name,
                        max_length=50,
                        unique=True,
                    ),
                ),
                (
                    'parent',
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='children',
                        to='benchmark.treequeriesplace',
                        verbose_name='parent',
                    ),
                ),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
