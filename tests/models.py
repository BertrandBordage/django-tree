import uuid

from django.db.models import (
    CharField,
    ForeignKey,
    CASCADE,
    PROTECT,
    SET_NULL,
    SmallIntegerField,
    UUIDField,
)

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


class DescendingPlace(TreeModel):
    """A tree whose `PathField` orders siblings by a descending column."""

    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['-name'])

    class Meta:
        ordering = ['path']


class MultiPathPlace(TreeModel):
    """A model carrying two independent `PathField`s, each with its own
    self-referencing parent and ordering."""

    name = CharField(max_length=50)
    code = CharField(max_length=50)
    name_parent = ForeignKey(
        'self',
        null=True,
        blank=True,
        on_delete=CASCADE,
        related_name='name_children',
    )
    code_parent = ForeignKey(
        'self',
        null=True,
        blank=True,
        on_delete=CASCADE,
        related_name='code_children',
    )
    name_path = PathField(order_by=['name'], parent_field_name='name_parent')
    code_path = PathField(order_by=['code'], parent_field_name='code_parent')

    class Meta:
        ordering = ['name_path']


class UUIDPlace(TreeModel):
    """A tree on a non-integer (UUID) primary key."""

    id = UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['name'])

    class Meta:
        ordering = ['path']


class SetNullPlace(TreeModel):
    """A tree whose parent FK is `on_delete=SET_NULL`."""

    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=SET_NULL)
    path = PathField(order_by=['name'])

    class Meta:
        ordering = ['path']


class ProtectPlace(TreeModel):
    """A tree whose parent FK is `on_delete=PROTECT`."""

    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=PROTECT)
    path = PathField(order_by=['name'])

    class Meta:
        ordering = ['path']


class WeirdTableNamePlace(TreeModel):
    """A tree stored in a table whose name requires SQL quoting.

    The tree trigger is intentionally NOT created by a migration for this
    model: building the trigger function interpolates the (quoted) table
    name into the function name, which currently breaks for quoted
    identifiers. See `UnusualTableNameTest`, which installs the trigger at
    runtime so the failure stays isolated to that test.
    """

    name = CharField(max_length=50)
    parent = ForeignKey('self', null=True, blank=True, on_delete=CASCADE)
    path = PathField(order_by=['name'])

    class Meta:
        ordering = ['path']
        db_table = 'Tree Weird Table'
