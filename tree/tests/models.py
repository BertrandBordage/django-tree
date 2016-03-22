from django.db.models import Model, CharField, ForeignKey

from tree.fields import PathField


class Place(Model):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True)
    path = PathField(order_by=('name',), max_siblings=36*3, db_index=True)

    class Meta:
        ordering = ('path',)
