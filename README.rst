Django-tree
===========

Fast and easy tree structures.

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
- safe (thanks to database constraints)
- faster for all operations

However, there is nothing groundbreaking here: this is only the result of
a proper use of the latest Django improvements, combined with a good knowledge
of SQL.


Installation
------------

Django-tree requires Django 1.8 or 1.9 and Python 3.
For the moment, django-tree is only for PostgreSQL because it uses a specific
data type not available in other databases. It will be adapted to also use
a standard text field in the future for other databases, but it will be slower.

After installing the module, you need to add `'tree',` to your
`INSTALLED_APPS`, then add a `PathField` to a model with a
`ForeignKey('self')`, typically named `parent` (use the `parent_field`
argument of `PathField` if the field has another name).
This should give you something like this:

.. code:: python

    from django.db.models import Model, CharField, ForeignKey, BooleanField
    from tree.fields import PathField

    class YourModel(Model):
        name = CharField(max_length=30)
        parent = ForeignKey('self', null=True, blank=True)
        path = PathField(null=True, blank=True)
        public = BooleanField(default=False)

However, the model above is not ordered. The children of a same parent will be
ordered by primary key. You can specify how children are ordered using the
`order_by` argument of `PathField`. If needed, you can add a field for users
to explicitly order these objects. Example:

.. code:: python

    from django.db.models import (
        Model, CharField, ForeignKey, IntegerField, BooleanField)
    from tree.fields import PathField

    class YourModel(Model):
        name = CharField(max_length=30)
        parent = ForeignKey('self', null=True, blank=True)
        position = IntegerField(default=1)
        path = PathField(order_by=('position', 'name'), null=True, blank=True)
        public = BooleanField(default=False)

Here, the children of a same parent will be ordered by position, and then
by name if the position is the same.


Usage
-----

A `PathField` is an automatic field, you don’t need to set, modify, or even see
its value once it is installed. But you can use the `Path` object it returns to
get tree information about the current instance, or make complex queries
on the whole tree structure. Example to show you most of the possibilities:

.. code:: python

    obj = YourModel.objects.first()
    obj.path.rebuild_tree()  # Rebuilds the whole tree,
                             # typically useful after a major migration.
    obj.path.level
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

There is also a bunch of less useful lookups, transforms and functions
available. They will be documented with examples in the future.
