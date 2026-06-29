from random import choice
from string import ascii_letters

from django.db.models import CharField, ForeignKey, CASCADE
from treebeard.mp_tree import MP_Node
from treebeard.ns_tree import NS_Node
from treebeard.al_tree import AL_Node
from treenode.models import TreeNodeModel
from tree_queries.models import TreeNode
from tree.fields import PathField
from tree.models import TreeModel
from mptt.models import MPTTModel, TreeForeignKey


def get_random_name():
    return ''.join([choice(ascii_letters) for _ in range(7)])


class MPTTPlace(MPTTModel):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = TreeForeignKey(
        'self', null=True, blank=True, related_name='children', on_delete=CASCADE
    )

    class MPTTMeta:
        order_insertion_by = ('name',)


class TreePlace(TreeModel):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = ForeignKey(
        'self', null=True, blank=True, related_name='children', on_delete=CASCADE
    )
    path = PathField(order_by=['name'])

    class Meta:
        indexes = [
            *PathField.get_indexes('treeplace', 'path'),
        ]


class TreebeardALPlace(AL_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    parent = ForeignKey(
        'self', null=True, blank=True, related_name='children', on_delete=CASCADE
    )
    node_order_by = ('name',)

    # django-treebeard 5.3.0 rewrote AL_Node.get_prev_sibling/get_next_sibling to
    # filter on `sib_order`, which does not exist when `node_order_by` is set,
    # raising AttributeError. Restore treebeard 4.7.1's index-based lookup (which
    # works for `node_order_by` nodes) so the benchmark stays comparable.
    def get_prev_sibling(self):
        siblings = self.get_siblings()
        ids = [obj.pk for obj in siblings]
        if self.pk in ids:
            idx = ids.index(self.pk)
            if idx > 0:
                return siblings[idx - 1]

    def get_next_sibling(self):
        siblings = self.get_siblings()
        ids = [obj.pk for obj in siblings]
        if self.pk in ids:
            idx = ids.index(self.pk)
            if idx < len(siblings) - 1:
                return siblings[idx + 1]


class TreebeardMPPlace(MP_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    node_order_by = ('name',)


class TreebeardNSPlace(NS_Node):
    name = CharField(max_length=50, unique=True, default=get_random_name)
    node_order_by = ('name',)


class TreeNodePlace(TreeNodeModel):
    treenode_display_field = 'name'
    name = CharField(max_length=50, unique=True, default=get_random_name)

    class Meta(TreeNodeModel.Meta):
        pass


class TreeQueriesPlace(TreeNode):
    # `parent` (and the `children` reverse relation) is provided by TreeNode, which
    # keeps a plain adjacency list and resolves the tree with a recursive CTE.
    name = CharField(max_length=50, unique=True, default=get_random_name)

    # Thin aliases over django-tree-queries' native API so this model can take part
    # in the shared benchmark tests, the same way TreebeardALPlace shims its siblings
    # methods above. Each just forwards to the real (CTE-backed) call.
    def get_children(self):
        return self.children.all()

    def get_ancestors(self):
        return self.ancestors()

    def get_descendants(self):
        return self.descendants()

    def get_siblings(self):
        return self.__class__._default_manager.filter(parent_id=self.parent_id)
