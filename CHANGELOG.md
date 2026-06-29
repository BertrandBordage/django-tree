# Unreleased

- Stores each path as a single `bytea` column instead of a `double precision[]`
  (array of float8). A path is the per-level concatenation of an order-preserving
  byte *segment* followed by a `0x00` delimiter, so the whole path is one compact
  key compared with a single `memcmp`, and ancestor/descendant relations are
  byte-prefix / byte-range comparisons. This drops the array overhead from the
  column and from every path index. `PathField` is now a `BinaryField` subclass
  and `path.value` is the raw `bytes` (it was a list of floats).
- Places new siblings with fractional indexing: the trigger generates an
  order-preserving byte key strictly between the two neighbouring siblings. A
  tight gap simply grows the key by a byte, so insertions never run out of
  headroom — the float8 gap-exhaustion renumbering (#17) is gone.
- Serves the `descendant_of`, `child_of` and `sibling_of` lookups (and
  `get_descendants`) as range comparisons on the whole path —
  `path >= P AND path < tree_upper(P)` — so they use the btree index already
  backing the path (the `UNIQUE` constraint) instead of per-level slice indexes.
  `tree_upper(P)` replaces P's trailing `0x00` delimiter with `0x01`, the bytea
  analogue of the old float `Infinity` upper bound.
- Adds a few table-independent PL/pgSQL helpers (`tree_mid`, `tree_int_to_seg`,
  `tree_level`, `tree_upper`, `tree_parent_prefix`) backing the encoding. They are
  installed by the `tree` `0003_tree_functions` migration — so the functional
  `(tree_level(path), path)` index can be built before any trigger — and
  re-created by every `CreateTreeTrigger`.
- `PathField.get_indexes()` still creates a single composite `(level, path)`
  index; `level` now resolves to `tree_level(path)` (a `0x00`-delimiter count)
  instead of `array_length`. The parent-slice and per-level `path__0_N` slice
  indexes remain gone.

  Upgrading: existing projects must drop the old trigger, recast the column and
  rebuild with a migration that runs `DeleteTreeTrigger('YourModel', 'path')`, an
  `AlterField` paired with `RunSQL('ALTER TABLE ... ALTER COLUMN path TYPE bytea
  USING NULL')` (no float8[]→bytea cast exists; the tree is preserved in the
  parent FK), `CreateTreeTrigger('YourModel', 'path')` and
  `RebuildPaths('YourModel', 'path')`. The migration must depend on
  `('tree', '0003_tree_functions')`.
- Speeds up reads:
  - `Path.__init__` only stores the two essential attributes; `attname`,
    `field_bound` and `qs` are now derived lazily, so loading rows no longer does
    redundant per-row work (e.g. cloning a throwaway queryset for every `Path`).
  - `Path.get_descendants()` excludes the node itself with a single strict range
    comparison (new `strict_descendant_of` lookup) instead of an extra
    `array_length(...)` predicate.
  - `Path.get_prev_sibling()`/`get_next_sibling()` issue a single query each
    instead of chaining several queryset clones.
  - `TreeQuerySetMixin.get_descendants()` runs as one correlated `EXISTS` query
    instead of an extra query plus one OR'd range clause per matching row.
  - The composite `(level, path)` index lets `child_of`/`sibling_of` (and the
    `get_children`/`get_siblings`/children-count queries built on them) scan just
    the matching rows via an index seek, instead of range-scanning the whole
    subtree and filtering by depth — a win that grows with subtree size (e.g.
    ~4.7× faster `get_children` on a 56k-node tree). The trade-off is a larger
    level index (it now stores the path), so disk usage rises accordingly.
- Speeds up writes:
  - The path-maintenance trigger fetches the parent path and both surrounding
    siblings with scalar subqueries (each neighbour via `ORDER BY path ... LIMIT 1`,
    a top-1 read rather than sorting the whole sibling set) instead of one scan
    building two ordered `array_agg`s. Insert/move cost on a node with N existing
    siblings drops from the `array_agg` sort's super-linear growth to roughly flat
    — e.g. inserting under a 2000-child parent is ~3× faster — and ties it on small
    fan-outs.
  - `RebuildPaths` now emits minimal-width path segments (one byte per level for
    up to 254 children, like inserts do) instead of a fixed four, so a rebuilt
    tree keeps the same compact paths as an inserted one (≈60% fewer path bytes on
    a deep tree) and its path indexes stay small.
  - The `post_save` path-deferral receiver resolves which fields are `PathField`s
    once per model class (cached) instead of scanning `concrete_fields` and
    running `isinstance` on every save — the handler runs ~2.6× faster and the
    cost is removed from every non-tree model's save too.
  - The trigger function emits its parent/sibling lookup, descendant rewrite and
    rebuild as static SQL instead of `EXECUTE`'d dynamic strings. The statements
    are now planned once and plan-cached instead of being re-planned on every
    firing — e.g. inserting under a small parent drops from ~0.1 ms to ~0.02 ms of
    trigger time (planning had dominated there); the gain shrinks on large
    fan-outs where the sibling scan dominates.

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
