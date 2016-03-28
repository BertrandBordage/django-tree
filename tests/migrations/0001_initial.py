from django.db import models, migrations
from tree.fields import PathField
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
                ('parent', models.ForeignKey('self', blank=True, null=True)),
                ('path', PathField()),
            ],
            options={
                'ordering': ('path', 'name'),
            },
        ),
        CreateTreeTrigger('tests.Place', order_by=('name',), max_siblings=36*3)
    ]
