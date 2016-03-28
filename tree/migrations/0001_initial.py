from django.db import migrations
from tree.operations import CreateTreeFunctions


class Migration(migrations.Migration):
    operations = [
        CreateTreeFunctions(),
    ]
