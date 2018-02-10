from django.db.models import Model, CharField, ForeignKey, CASCADE

from tree.fields import PathField
from tree.models import TreeModelMixin


class Place(TreeModelMixin, Model):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField()

    class Meta:
        ordering = ('path', 'name')
