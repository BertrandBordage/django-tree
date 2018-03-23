Django-tree
===========

Fast and easy tree structures.

.. image:: http://img.shields.io/pypi/v/django-tree.svg?style=flat-square
   :target: https://pypi.python.org/pypi/django-tree

.. image:: http://img.shields.io/travis/BertrandBordage/django-tree/master.svg?style=flat-square
   :target: https://travis-ci.org/BertrandBordage/django-tree

.. image:: http://img.shields.io/coveralls/BertrandBordage/django-tree/master.svg?style=flat-square
   :target: https://coveralls.io/r/BertrandBordage/django-tree?branch=master

**In alpha, it can’t be used yet in production.**

This tool works in a very similar way to **django-mptt**
and **django-treebeard**, however it’s so different in conception
that it was better and faster to start from scratch
than to rewrite the existing solutions.

Compared to these solutions, django-tree aims to have these advantages
(some of them are already there):

- less intrusive (no more inheriting issues
  due to Model, Manager & Queryset subclasses)
- easier to install
- easier to use
- more complete
- minimalist (less code, less database fields)
- bug-free
- safe (most of the logic is written directly in database)
- faster for all operations

However, there is nothing groundbreaking here: this is only the result of
a proper use of the latest Django improvements, combined with a good knowledge
of SQL.


Installation
------------

Django-tree requires Django 1.8, 1.11 or 2.0 and Python 2 or 3.
For the moment, django-tree is only for PostgreSQL.
It will be adapted in the future for other databases.

After installing the module, you need to add ``'tree',`` to your
``INSTALLED_APPS``, then add a ``PathField`` to a model with a
``ForeignKey('self')``, typically named ``parent`` (use the ``parent_field``
argument of ``CreateTreeTrigger`` if the field has another name).
``PathField`` stores ``Path`` objects which have methods to execute queries,
such as getting all the descendants of the current object, its siblings, etc.
To call these methods more conveniently, you can add ``TreeModelMixin``
to your model.  The inheriting order is not important, as the mixin methods
do not clash with Django.  If you have multiple ``PathField``
on the same model, you will have to specify the field name in the method
you’re calling using ``path_field``.

This should give you a model like this:

.. code:: python

    from django.db.models import Model, CharField, ForeignKey, BooleanField
    from tree.fields import PathField
    from tree.models import TreeModelMixin

    class YourModel(Model, TreeModelMixin):
        name = CharField(max_length=30)
        parent = ForeignKey('self', null=True, blank=True)
        path = PathField()
        public = BooleanField(default=False)

        class Meta:
            ordering = ('path',)

Then you need to create the SQL trigger that will automatically update ``path``.
To do that, create a migration with a dependency
to the latest django-tree migration and add a ``CreateTreeTrigger`` operation:

.. code:: python

    from django.db import migrations
    from tree.operations import CreateTreeTrigger

    class Migration(migrations.Migration):
        dependencies = [
            ('tree', '0001_initial'),
        ]

        operations = [
            CreateTreeTrigger('your_app.YourModel'),
        ]

If you already have data in ``YourModel``, you will need to add an operation
for allowing SQL ``NULL`` values before creating the trigger,
then rebuild the paths and revert the allowance of ``NULL`` values:

.. code:: python

    from django.db import migrations
    from tree.fields import PathField
    from tree.operations import CreateTreeTrigger, RebuildPaths

    class Migration(migrations.Migration):
        dependencies = [
            ('tree', '0001_initial'),
        ]

        operations = [
            migrations.AlterField('YourModel', 'path', PathField(null=True)),
            CreateTreeTrigger('YourModel'),
            RebuildPaths('YourModel', 'path'),
            migrations.AlterField('YourModel', 'path', PathField()),
        ]

However, the model above is not ordered. The children of a same parent will be
ordered by primary key. You can specify how children are ordered using the
``order_by`` argument of ``PathField``. If needed, you can add a field
for users to explicitly order these objects, typically a position field.
Example model:

.. code:: python

    from django.db.models import (
        Model, CharField, ForeignKey, IntegerField, BooleanField)
    from tree.fields import PathField
    from tree.models import TreeModelMixin

    class YourModel(Model, TreeModelMixin):
        name = CharField(max_length=30)
        parent = ForeignKey('self', null=True, blank=True)
        position = IntegerField(default=1)
        path = PathField(order_by=['position', 'name'])
        public = BooleanField(default=False)

        class Meta:
            ordering = ('path',)

And the corresponding migration:

.. code:: python

    from django.db import models, migrations
    from tree.operations import CreateTreeTrigger

    class Migration(migrations.Migration):
        dependencies = [
            ('tree', '0001_initial'),
        ]

        operations = [
            migrations.AddField('YourModel', 'position',
                                models.IntegerField(default=1))
            CreateTreeTrigger('YourModel'),
        ]

Here, the children of a same parent will be ordered by position, and then
by name if the position is the same.

.. note::

    You can also use ``PathField`` without adding a ``CreateTreeTrigger``
    operation. However, the field will not automatically be updated, you
    will have to do it by yourself. In most cases this is not useful, so you
    should not use ``PathField`` without ``CreateTreeTrigger`` unless you know
    what you are doing.


Usage
-----

``PathField`` is automatically filled thanks to ``CreateTreeTrigger``,
you don’t need to set, modify, or even see its value once it is installed.
But you can use the ``Path`` object it stores or the more convenient
``TreeModelMixin`` to get tree information about the current instance,
or make complex queries on the whole tree structure.
Example to show you most of the possibilities:

.. code:: python

    obj = YourModel.objects.all()[0]
    obj.path.get_level()
    obj.get_level()  # Shortcut for the previous method, if you use
                     # `TreeModelMixin`. Same for other object methods below.
    obj.is_root()
    obj.is_leaf()
    obj.get_children()
    obj.get_children().filter(public=True)
    obj.get_ancestors()
    obj.get_ancestors(include_self=True)
    obj.get_descendants(include_self=True)
    obj.get_siblings()
    obj.get_prev_sibling()  # Fetches the previous sibling.
    obj.get_next_sibling()
    # Same as `get_prev_sibling`, except that we get the first public one.
    obj.get_prev_siblings().filter(public=True).first()
    other = YourModel.objects.all()[1]
    obj.is_ancestor_of(other)
    obj.is_descendant_of(other, include_self=True)
    YourModel.get_roots()

    #
    # Advanced usage
    # Use the following methods only if you understand exactly what they mean.
    #

    YourModel.rebuild_paths()  # Rebuilds all paths of this field, useful only
                               # if something is broken, which shouldn’t happen.
    YourModel.disable_tree_trigger()  # Disables the SQL trigger.
    YourModel.enable_tree_trigger()   # Restores the SQL trigger.
    with YourModel.disabled_tree_trigger():
        # What happens inside this context manager is ignored
        # by the SQL trigger.
        # The trigger is restored after that, even if there an error occurred.
        pass

There is also a bunch of less useful lookups and transforms
available. They will be documented with examples in the future.


Differences with MPTT and treebeard
-----------------------------------

Level vs depth
..............

django-mptt and django-treebeard use two different names to designate almost
the same thing: MPTT uses level and treebeard uses depth.
Both are integers to show how much distant is a node from the top of the tree.
The only difference is that level should start by convention with 1 and depth
should start with 0.

Unfortunately, **both MPTT and treebeard are wrong about the indexing**:
MPTT starts its level with 0 and treebeard starts its depth with 1.

**Django-tree finally fixes this issue by implementing a level starting by 1**,
and no depth to avoid confusion. One name had to be chosen, and I find that
“level” represents more accurately the idea that we deal with an abstract tree,
where all the node of the same level are on the same row.
In comparison, “depth” sounds like we’re actually digging a real root,
and it gives the impression that a child of a root
can be at a different depth than a child of another root, like in real life.
