from django.db.models import CharField, ForeignKey, CASCADE, SmallIntegerField

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

    def __str__(self):
        return self.name


class Person(TreeModel):
    century = SmallIntegerField(null=True, blank=True)
    first_name = CharField(max_length=20, blank=True)
    last_name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['century', 'last_name', 'first_name'])

    class Meta:
        ordering = ['path']
        indexes = [
            *PathField.get_indexes('person', 'path'),
        ]
