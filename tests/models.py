from django.db.models import CharField, ForeignKey, CASCADE

from tree.fields import PathField
from tree.models import TreeModel


class Place(TreeModel):
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['name'])

    class Meta:
        ordering = ['path', 'name']
        indexes = [
            *PathField.get_indexes('place', 'path'),
        ]


class Person(TreeModel):
    first_name = CharField(max_length=20, blank=True)
    last_name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['last_name', 'first_name'])

    class Meta:
        ordering = ['path']
        indexes = [
            *PathField.get_indexes('person', 'path'),
        ]
