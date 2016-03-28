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

Django-tree requires Django 1.8 or 1.9 and Python 2 or 3.
For the moment, django-tree is only for PostgreSQL because it uses a specific
data type not available in other databases. It will be adapted to also use
a standard text field in the future for other databases, but it may be slower.

After installing the module, you need to add ``'tree',`` to your
``INSTALLED_APPS``, then add a ``PathField`` to a model with a
``ForeignKey('self')``, typically named ``parent`` (use the ``parent_field``
argument of ``CreateTreeTrigger`` if the field has another name).
This should give you a model like this:

.. code:: python

    from django.db.models import Model, CharField, ForeignKey, BooleanField
    from tree.fields import PathField

    class YourModel(Model):
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
``order_by`` argument of ``CreateTreeTrigger``. If needed, you can add a field
for users to explicitly order these objects, typically a position field.
Example model:

.. code:: python

    from django.db.models import (
        Model, CharField, ForeignKey, IntegerField, BooleanField)
    from tree.fields import PathField

    class YourModel(Model):
        name = CharField(max_length=30)
        parent = ForeignKey('self', null=True, blank=True)
        position = IntegerField(default=1)
        path = PathField()
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
            CreateTreeTrigger('YourModel', order_by=('position', 'name')),
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
But you can use the ``Path`` object it returns to get tree information
about the current instance, or make complex queries on the whole tree structure.
Example to show you most of the possibilities:

.. code:: python

    obj = YourModel.objects.all()[0]
    obj.path.depth
    obj.path.level  # Same as depth, but starts with 1 instead of 0.
    obj.path.is_root
    obj.path.is_leaf
    obj.path.get_children()
    obj.path.get_children().filter(public=True)
    obj.path.get_ancestors()
    obj.path.get_ancestors(include_self=True)
    obj.path.get_descendants(include_self=True)
    obj.path.get_siblings()
    obj.path.get_prev_sibling()  # Fetches the previous sibling.
    obj.path.get_next_sibling()
    # Same as `get_prev_sibling`, except that we get the first public one.
    obj.path.get_prev_siblings().filter(public=True).first()
    other = YourModel.objects.all()[1]
    obj.path.is_ancestor_of(other.path)
    obj.path.is_descendant_of(other.path, include_self=True)
    obj.path.rebuild()  # Rebuilds all trees of this field, useful only
                        # if something is broken, which shouldn’t happen.

There is also a bunch of less useful lookups, transforms and functions
available. They will be documented with examples in the future.
