from django.db import models, migrations
from django.db.models import CASCADE


def populate(apps, schema_editor):
    Something = apps.get_model('tests.Something')
    Something.objects.bulk_create([Something(name='%d' % i)
                                   for i in range(10)])
    bulk = []
    for i in range(10):
        parent = Something.objects.order_by('?')[0]
        bulk.append(Something(name='%s > %d' % (parent.name, i),
                              parent=parent))
    Something.objects.bulk_create(bulk)


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Something',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=50)),
                ('parent', models.ForeignKey('self', blank=True, null=True, on_delete=CASCADE)),
            ],
        ),
        migrations.RunPython(populate),
    ]
