# Unreleased

- Stores path elements as `double precision` (float8) instead of `numeric`,
  shrinking the path column and every path index while making comparisons
  faster. Bisection only produces dyadic fractions, which float8 stores
  exactly, with far more reordering headroom than the previous
  `numeric(20, 10)` configuration.
- Serves the `descendant_of`, `child_of` and `sibling_of` lookups (and
  `get_descendants`) as range comparisons on the whole path — e.g.
  `path >= P AND path < P || {Infinity}` — so they use the btree index already
  backing the path (the `UNIQUE` constraint) instead of per-level slice indexes.
  `Infinity` is available because the path is now floating-point.
- `PathField.get_indexes()` now creates only the level index; the parent-slice
  and per-level `path__0_N` slice indexes are gone (the range comparisons above
  replace them) and the redundant full-path `db_index` is dropped. Together with
  float8 this roughly halves the on-disk size of a tree.

  Upgrading: existing projects must recast the column and re-space paths with a
  migration that runs `AlterField('YourModel', 'path', PathField(...))` followed
  by `RebuildPaths('YourModel', 'path')`, and update `Meta.indexes` to the new
  `PathField.get_indexes()` output.
- Speeds up reads:
  - The queryset `Path.qs` is now built lazily, so loading rows no longer clones
    a throwaway queryset for every fetched `Path`.
  - `Path.get_descendants()` excludes the node itself with a single strict range
    comparison (new `strict_descendant_of` lookup) instead of an extra
    `array_length(...)` predicate.
  - `Path.get_prev_sibling()`/`get_next_sibling()` issue a single query each
    instead of chaining several queryset clones.
  - `TreeQuerySetMixin.get_descendants()` runs as one correlated `EXISTS` query
    instead of an extra query plus one OR'd range clause per matching row.

# 0.6.2 (2025-09-29)

Fixes psycopg2 compatibility.

# 0.6.1 (2025-09-29)

Fixes the broken version number picked up by uv.

# 0.6.0 (2025-09-29)

- Adds compatibility with Django>=4.0,<6 (thanks to @jacobjove)
- Adds compatibility with psycopg 3
- Makes path values deserializable (thanks to @jacobjove)

# 0.5.6 (2023-07-09)

- Adds a model validation in addition to the existing database error,
  when users try to make a cycle (mark a node as its own parent or ancestor)
- Moves `PathField.get_roots()` to `TreeQuerySetMixin.filter_roots()`.
- Fixes a `TypeError` when using `TreeQuerySetMixin.get_descendants()`
  on an empty queryset.

# 0.5.5 (2023-07-06)

Fixes another PostgreSQL 12 compatibility issue.

# 0.5.4 (2023-07-06)

Fixes an SQL syntax error.

# 0.5.3 (2023-07-06)

Fixes a PostgreSQL implicit type casting that was not done in PostgreSQL 12.

# 0.5.2 (2023-07-06)

Fixes a source of path clashes when the objects have exactly the same values
for all `order_by` columns.

# 0.5.1 (2023-07-06)

Big rewrite using arrays of decimals instead of strings to represent the path.

## Performance

For more details, see the [benchmark results](benchmark/results/results.md).

- Inserting becomes orders of magnitude faster, often faster than django-treebeard and django-mptt.
- Updating becomes faster in all cases, especially when the instance stays at the same place where it becomes orders of magnitude faster.
- Deleting becomes most of the time orders of magnitude faster.
- Reading stays as fast as it was.

## Upgrading

- Add a new empty migration in each application that contains `PathField`s.
- For each `PathField` defined in the application, add:
  - `DeleteTreeTrigger`
  - `RemoveField` of the path field
  - `AddField` of the path field
  - `CreateTreeTrigger`
  - `RebuildPaths`

For example:

```python
    DeleteTreeTrigger('Place'),
    migrations.RemoveField('Place', 'path'),
    migrations.AddField(
        model_name='Place',
        name='path',
        field=PathField(db_index=True, order_by=['name'], size=None),
    ),
    CreateTreeTrigger('place'),
    RebuildPaths('place'),
```

You can also comment the `PathField` in the model itself, run `makemigrations`
to create a first migration with the `RemoveField`, add the `DeleteTreeTrigger` before,
then uncomment the field in the model, run `makemigrations` to generate a second migration with the `AddField`
in it, and finally add the `CreateTreeTrigger` and `RebuildPaths` at the end.
