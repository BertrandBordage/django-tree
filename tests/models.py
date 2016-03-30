from django.db.models import Model, CharField, ForeignKey

from tree.fields import PathField
from tree.models import TreeModelMixin


class Place(Model, TreeModelMixin):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True)
    path = PathField()

    class Meta:
        ordering = ('path', 'name')
