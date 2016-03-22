from django.contrib.postgres.operations import CreateExtension
from django.db import models, migrations
from tree.fields import PathField


class Migration(migrations.Migration):
    operations = [
        CreateExtension('ltree'),
        migrations.CreateModel(
            name='Place',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=50)),
                ('parent', models.ForeignKey(blank=True, to='self', null=True)),
                ('path', PathField(order_by=('name',), max_siblings=36*3, db_index=True)),
            ],
            options={
                'ordering': ('path',),
            },
        ),
    ]