from django.db.models import Model, CharField, ForeignKey

from tree.fields import PathField


class Place(Model):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True)
    path = PathField()

    class Meta:
        ordering = ('path', 'name')
