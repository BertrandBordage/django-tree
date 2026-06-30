# 1.0.1 (2026-07-01)

Fixes `PathField.value_to_string()` for serializers like django-reversion.

# 1.0.0 (2026-06-30)

Adds **SQLite, MySQL and Oracle** support, and rewrites how a path is stored:
each path is now a single compact `bytea` key instead of an array of decimals
(`numeric[]`). This makes most reads and writes faster, removes a long-standing
scaling limit, and changes the type of `path.value` (see Upgrading below).

## What changed

- Adds **SQLite, MySQL and Oracle** support. PostgreSQL keeps its PL/pgSQL
  trigger (and stays consistent even under raw SQL); on the other backends, where
  no portable trigger is possible, the path is computed in Python on the ORM save
  cycle (`save()`, `delete()`, `QuerySet.update()`, `bulk_create`/`bulk_update`),
  producing an identical tree. SQLite and MySQL navigation is fully portable with
  no database functions (the lookups use only `length`/`substr`/`instr` and path
  ranges); Oracle stores the path as `RAW` and installs one small deterministic
  `tree_level` helper (its `child_of`/`sibling_of` need it). Limitations off
  PostgreSQL: raw-SQL writes are not observed and need a manual
  `Model.rebuild_paths()`; sibling order follows the database's collation (and on
  Oracle an empty `order_by` string sorts as NULL); MySQL stores the path as
  `VARBINARY(768)` and Oracle as `RAW(2000)`, capping tree depth; the `__level`
  query filter and the functional `(level, path)` index are PostgreSQL-only (other
  backends use a plain path index, and `get_level()` / `is_root()` still work
  everywhere).
- `PathField` is now a `BinaryField` subclass, and `path.value` is raw `bytes`
  instead of a list of `Decimal`s. If you read `path.value` directly, update your
  code; the `TreeModelMixin`/`Path` helpers (`get_ancestors`, `get_descendants`,
  `get_children`, `get_level`, etc.) are unchanged.
- New siblings are now placed with fractional indexing: each insert computes a
  key strictly between its two neighbours. There is no longer a periodic
  full-renumber of siblings (the old `#17` gap-exhaustion rebuild is gone), so
  inserts stay cheap no matter how many siblings already exist.
- No longer beta: django-tree is now considered production-ready.

## Performance

- Inserting and moving nodes is faster, and no longer slows down as a parent
  gains more children (e.g. ~3× faster inserting under a 2000-child parent).
- `get_children` / `get_siblings` and children counts use a better index and
  scan only the matching rows (e.g. ~4.7× faster `get_children` on a 56k-node
  tree).
- `get_descendants`, `get_prev_sibling`/`get_next_sibling` and several other
  reads now run as a single query each.
- Uses noticeably less disk space overall: the column is a compact key per level
  instead of a decimal array, and the per-level slice indexes and the
  parent-slice index are gone — a model now needs just two path indexes (the
  unique path index and a `(level, path)` index) instead of eight. A ~3900-node
  tree drops from about 1.5 MB to 0.9 MB.

## Upgrading

Each application containing `PathField`s needs one migration that drops the old
trigger, switches the column to `bytea`, and rebuilds the tree from the `parent`
foreign keys. There is no automatic `numeric[]` → `bytea` conversion, so the
column is reset to `NULL` and recomputed by `RebuildPaths`.

The migration must depend on `('tree', '0003_tree_functions')` and run, for each
`PathField`:

```python
class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0003_tree_functions'),
        # ... your previous migration ...
    ]

    operations = [
        DeleteTreeTrigger('Place', 'path'),
        migrations.AlterField(
            model_name='Place',
            name='path',
            field=PathField(order_by=['name']),
        ),
        migrations.RunSQL(
            'ALTER TABLE myapp_place ALTER COLUMN path TYPE bytea USING NULL',
            reverse_sql=migrations.RunSQL.noop,
        ),
        CreateTreeTrigger('Place', 'path'),
        RebuildPaths('Place', 'path'),
    ]
```

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
