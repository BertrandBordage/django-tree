0.5.1 (2023-07-06)
==================

Big rewrite using arrays of decimals instead of strings to represent the path.

Performance
-----------

For more details, see the `benchmark results <benchmark/results/results.rst>`_.

- Inserting becomes orders of magnitude faster, often faster than django-treebeard and django-mptt.
- Updating becomes faster in all cases, especially when the instance stays at the same place where it becomes orders of magnitude faster.
- Deleting becomes most of the time orders of magnitude faster.
- Reading stays as fast as it was.

Upgrading
---------

- Add a new empty migration in each application that contains ``PathField``s.
- For each ``PathField`` defined in the application, add:
  - ``DeleteTreeTrigger``
  - ``RemoveField`` of the path field
  - ``AddField`` of the path field
  - ``CreateTreeTrigger``
  - ``RebuildPaths``

For example:

.. code-block:: python

    DeleteTreeTrigger('Place'),
    migrations.RemoveField('Place', 'path'),
    migrations.AddField(
        model_name='Place',
        name='path',
        field=PathField(db_index=True, order_by=['name'], size=None),
    ),
    CreateTreeTrigger('place'),
    RebuildPaths('place'),

You can also comment the ``PathField`` in the model itself, run ``makemigrations``
to create a first migration with the ``RemoveField``, add the ``DeleteTreeTrigger`` before,
then uncomment the field in the model, run ``makemigrations`` to generate a second migration with the ``AddField``
in it, and finally add the ``CreateTreeTrigger`` and ``RebuildPaths`` at the end.
