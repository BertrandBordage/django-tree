from django.db import models, migrations
from tree.fields import PathField
from tree.operations import CreateTreeFunctions


class Migration(migrations.Migration):
    operations = [
        CreateTreeFunctions(),
        migrations.CreateModel(
            name='Place',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=50)),
                ('parent', models.ForeignKey('self', blank=True, null=True)),
                ('path', PathField(order_by=('name',), max_siblings=36*3, db_index=True)),
            ],
            options={
                'ordering': ('path', 'name'),
            },
        ),
    ]
