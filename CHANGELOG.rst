0.5.0 (2023-07-05)
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
- For each ``PathField`` defined in the application, add a ``CreateTreeTrigger``
  to the empty migration. For example, ``CreateTreeTrigger('your_app.YourModel')``.
- After each ``CreateTreeTrigger``, add a ``RebuildPaths``. For example, ``RebuildPaths('your_app.YourModel')``.
- Run pending migrations (including ``tree.0002_remove_old_functions``). All should be good now! Old SQL functions & triggers should be removed automatically.
