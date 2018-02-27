from random import choice
from string import ascii_letters

from django.db.models import Model, CharField, ForeignKey, CASCADE
from treebeard.mp_tree import MP_Node
from treebeard.ns_tree import NS_Node
from treebeard.al_tree import AL_Node
from tree.fields import PathField
from tree.models import TreeModelMixin
from mptt.models import MPTTModel, TreeForeignKey


def get_random_name():
    return ''.join([choice(ascii_letters) for _ in range(7)])


class MPTTPlace(MPTTModel):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = TreeForeignKey('self', null=True, blank=True,
                            related_name='children', on_delete=CASCADE)

    class MPTTMeta:
        order_insertion_by = ('name',)


class TreePlace(TreeModelMixin, Model):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = ForeignKey('self', null=True, blank=True, related_name='children',
                        on_delete=CASCADE)
    path = PathField(order_by=('name',), db_index=True)


class TreebeardALPlace(AL_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = ForeignKey('self', null=True, blank=True, related_name='children',
                        on_delete=CASCADE)
    node_order_by = ('name',)


class TreebeardMPPlace(MP_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    node_order_by = ('name',)


class TreebeardNSPlace(NS_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    node_order_by = ('name',)
