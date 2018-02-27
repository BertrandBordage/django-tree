from django.db.models import Model, CharField, ForeignKey, CASCADE

from tree.fields import PathField
from tree.models import TreeModelMixin
from tree.sql.base import ALPHANUM_LEN


class Place(TreeModelMixin, Model):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=('name',), max_siblings=ALPHANUM_LEN*3)

    class Meta:
        ordering = ('path', 'name')
