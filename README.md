# Django-tree

Fast and easy tree structures for Django, maintained inside the database.

[![](https://img.shields.io/pypi/v/django-tree.svg?style=flat-square)](https://pypi.python.org/pypi/django-tree) [![](https://img.shields.io/github/actions/workflow/status/BertrandBordage/django-tree/ci.yml?branch=master&style=flat-square)](https://github.com/BertrandBordage/django-tree/actions/workflows/ci.yml) [![](https://img.shields.io/codecov/c/github/BertrandBordage/django-tree/master.svg?style=flat-square)](https://codecov.io/gh/BertrandBordage/django-tree)

django-tree solves the same problem as **django-treebeard**,
**django-tree-queries**, **django-mptt** and **django-treenode**: storing and
querying tree (hierarchy) structures with Django. It does it differently: you add a `PathField` to an ordinary model
with a self-referencing `ForeignKey`, and the hierarchy is maintained
**by the database** — not in your Python code. There is no model, manager
or queryset to subclass; an optional `TreeModelMixin` only adds convenience
methods (`get_descendants()`, `get_ancestors()`, …).

On **PostgreSQL** the path is maintained by a PL/pgSQL trigger, so bulk
operations, `QuerySet.update()` and raw SQL all keep the tree consistent. On
**SQLite** and **MySQL** there is no such trigger, so the path is computed in
Python on the ORM save cycle (`save()`, `delete()`, `QuerySet.update()`,
`bulk_create`/`bulk_update`); writes that bypass the ORM (raw SQL) need a manual
`Model.rebuild_paths()`.


## Table of contents

- [Comparison](#comparison)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Differences with MPTT and treebeard](#differences-with-mptt-and-treebeard)
- [Contributing](#contributing)
- [License](#license)


## Comparison

> [!NOTE]
> django-treebeard ships three interchangeable algorithms — **MP** (materialized
> path), **NS** (nested sets) and **AL** (adjacency list) — shown as separate
> columns.

### Features

| | django-tree | [treebeard MP](https://github.com/django-treebeard/django-treebeard) | [treebeard NS](https://github.com/django-treebeard/django-treebeard) | [treebeard AL](https://github.com/django-treebeard/django-treebeard) | [django-mptt](https://github.com/django-mptt/django-mptt) | [django-tree-queries](https://github.com/feincms/django-tree-queries) | [django-treenode](https://github.com/fabiocaccamo/django-treenode) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Works on any Django database** | ✅ PostgreSQL, SQLite, MySQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Drop-in (no model/manager subclassing)** | ✅ add one field | ❌ subclass `MP_Node` | ❌ subclass `NS_Node` | ❌ subclass `AL_Node` | ❌ subclass `MPTTModel` | ❌ subclass `TreeNode` | ❌ subclass `TreeNodeModel` |
| **Build & move with plain `parent` + `save()`** | ✅ | ❌ API | ❌ API | ❌ API | ✅ | ✅ | ✅ |
| **Several independent trees per model** | ✅ multiple `PathField`s | ❌ one hierarchy | ❌ one hierarchy | ❌ one hierarchy | ❌ one hierarchy | ❌ one hierarchy | ❌ one hierarchy |
| **Tree kept correct by the database** | ✅ PostgreSQL: SQL trigger<br>❌ SQLite, MySQL: in Python | ❌ in Python | ❌ in Python | ❌ in Python | ❌ in Python | ✅ FK only, nothing denormalized | ❌ in Python + cache |
| **Survives bulk writes / `update()` / raw SQL** | ✅ PostgreSQL<br>🟡 SQLite, MySQL: bulk/`update()` yes, raw SQL no | ❌ Python API only | ❌ Python API only | ❌ Python API only | ❌ | ✅ | ❌ manual resync |
| **Tree filters as composable ORM lookups** | ✅ `__descendant_of`, `__child_of` | 🟡 manager methods | 🟡 manager methods | 🟡 manager methods | 🟡 manager methods | 🟡 `with_tree_fields()` | 🟡 cached properties |
| **Admin integration** | ❌ form field only | ✅ drag-and-drop | ✅ drag-and-drop | ✅ drag-and-drop | ✅ drag-and-drop | ✅ cut/paste | ✅ |
| **Template tags to render trees** | ❌ | 🟡 | 🟡 | 🟡 | ✅ `{% recursetree %}` | ✅ `{% recursetree %}` | 🟡 |
| **Production-ready** | ✅ | ✅ | ✅ | ✅ | 🟡 works, unmaintained | ✅ | ✅ |

✅ yes / good · 🟡 partial or depends on the variant · ❌ no / poor.

### Performance

Absolute latency and disk usage measured on a tree of **3905 rows**. Every test
runs on every implementation: those lacking a native method use a simple,
unofficial ORM equivalent, so the whole grid is comparable. Each cell shows the
measurement; below it, the rank in that row (`#n`) and a marker.

| | django-tree | treebeard MP | treebeard NS | treebeard AL | django-mptt | django-tree-queries | django-treenode |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Reads · best** | 83 µs<br>🟢 #7 | 0.5 µs<br>🟢 #1 👑 | 0.7 µs<br>🟢 #3 | 10 µs<br>🟢 #5 | 1.7 µs<br>🟢 #4 | 75 µs<br>🟢 #6 | 0.5 µs<br>🟢 #1 👑 |
| **Reads · typical** | 410 µs<br>🟢 #3 | 250 µs<br>🟢 #1 👑 | 393 µs<br>🟢 #3 | 1.6 ms<br>🟢 #6 | 300 µs<br>🟢 #2 | 1.1 ms<br>🟢 #5 | 1.9 ms<br>🟢 #7 |
| **Reads · worst** | 62 ms<br>🟠 #1 👑 | 344 ms<br>🔴 #3 | 518 ms<br>🔴 #4 | 853 ms<br>🔴 #6 | 118 ms<br>🔴 #2 | 627 ms<br>🔴 #5 | 5 min<br>💩 #7 |
| **Writes · best** | 223 µs<br>🟢 #4 | 235 µs<br>🟢 #5 | 205 µs<br>🟢 #3 | 193 µs<br>🟢 #2 | 307 µs<br>🟢 #6 | 183 µs<br>🟢 #1 👑 | 390 ms<br>🔴 #7 |
| **Writes · typical** | 2.2 ms<br>🟢 #3 | 5.7 ms<br>🟠 #4 | 6.1 ms<br>🟠 #5 | 969 µs<br>🟢 #1 👑 | 13 ms<br>🟠 #6 | 1.0 ms<br>🟢 #2 | 837 ms<br>🔴 #7 |
| **Writes · worst** | 2.1 s<br>💩 #3 | 9.0 s<br>💩 #4 | 21.8 s<br>💩 #6 | 926 ms<br>🔴 #2 | 19.0 s<br>💩 #5 | 829 ms<br>🔴 #1 👑 | 25 min<br>💩 #7 |
| **Storage** | 0.91 MB<br>🟢 #5 | 0.97 MB<br>🟢 #6 | 0.79 MB<br>🟢 #3 | 0.57 MB<br>🟢 #1 👑 | 0.85 MB<br>🟢 #4 | 0.57 MB<br>🟢 #1 👑 | 0.98 MB<br>🟢 #6 |

Two results within 5 % share a rank. Markers use the same thresholds for reads
and writes:

- 👑 best of the row — shown after the rank.
- 🟢 fine · 🟠 laggy (> 3 ms) · 🔴 very laggy (> 100 ms) · 💩 horrible (> 1 s).

See the [full benchmark](benchmark/results/results.md) for every test.

In short:

- **django-tree** keeps the tree correct in the database itself, so on
  PostgreSQL bulk operations, `update()` and raw SQL stay safe, with balanced
  reads and writes — at the cost of being without admin
  drag-and-drop or tree-rendering template tags yet. On SQLite and MySQL the
  path is maintained in Python on the ORM save cycle instead (raw SQL then needs
  a manual rebuild).
- **treebeard** offers three algorithms with the same brittle Python API and no
  database constraint: **MP** reads fast, **NS** writes slowly like MPTT, **AL**
  writes fast and is tiny on disk but some reads are catastrophic.
- **MPTT** stores the tree safely but writes get very slow on large or
  write-heavy tables and need periodic rebuilds. No longer maintained.
- **tree-queries** derives the hierarchy from a plain `parent` FK with recursive
  CTEs, so nothing can get out of sync and writes and storage are the cheapest
  and it runs on most databases — at the cost of slow, sometimes very slow, reads.
- **treenode** keeps denormalized caches of the whole tree, but every write
  rebuilds them — by far the slowest writes here — and bulk writes need a manual
  resync.


## Requirements

- **PostgreSQL** 12+, **SQLite** or **MySQL**. On PostgreSQL the hierarchy is
  maintained by a PL/pgSQL trigger using only standard, long-standing features
  (also under raw SQL; CI runs on PostgreSQL 16); on SQLite and MySQL it is
  maintained in Python on the ORM save cycle, so raw-SQL writes need a manual
  `Model.rebuild_paths()`. MySQL stores the path as `VARBINARY(768)`, capping
  tree depth.
- **Django** 4.2+
- **Python** 3.10+


## Installation

Install the package from PyPI:

```bash
pip install django-tree
```

Then add `'tree'` to your `INSTALLED_APPS`.


## Quick start

Add a `PathField` to a model that has a `ForeignKey('self')` — typically named
`parent` — and add `TreeModelMixin` for the convenience query methods
(`get_children()`, `get_descendants()`, …). The mixin order is not important,
as its methods do not clash with Django.

```python
from django.db.models import Model, CharField, ForeignKey, BooleanField
from tree.fields import PathField
from tree.models import TreeModelMixin

class YourModel(Model, TreeModelMixin):
    name = CharField(max_length=30)
    parent = ForeignKey('self', null=True, blank=True)
    path = PathField()
    public = BooleanField(default=False)

    class Meta:
        ordering = ['path']
        # Recommended: speeds up child/sibling/level queries.
        indexes = [*PathField.get_indexes('yourmodel', 'path')]
```

Then create a migration that depends on the latest django-tree migration and
adds a `CreateTreeTrigger` operation — this installs the SQL trigger that keeps
`path` up to date automatically:

```python
from django.db import migrations
from tree.operations import CreateTreeTrigger

class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0003_tree_functions'),
    ]

    operations = [
        CreateTreeTrigger('your_app.YourModel'),
    ]
```

Once the trigger exists, the field maintains itself — building and moving nodes
is just `parent` + `save()`:

```python
root = YourModel.objects.create(name='root')
child = YourModel.objects.create(name='child', parent=root)
root.get_descendants()   # QuerySet of every node under `root`
child.get_ancestors()    # QuerySet from the root down to `child`'s parent
```

That's the whole setup. See [Usage](#usage) for the full API, custom child
ordering, and adding the trigger to a table that already holds data.

If you have multiple `PathField`s on the same model, pass the field name as the
`path_field` argument of the method you call. If your self-referencing key is
not named `parent`, pass its name to the `parent_field` argument of
`CreateTreeTrigger`.


## Usage

`PathField` is automatically filled thanks to `CreateTreeTrigger`,
you don’t need to set, modify, or even see its value once it is installed.
But you can use the `Path` object it stores or the more convenient
`TreeModelMixin` to get tree information about the current instance,
or make complex queries on the whole tree structure.
Example to show you most of the possibilities:

```python
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
YourModel.objects.filter_roots()

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
    # The trigger is restored after that, even if an error occurred.
    pass
```

> [!NOTE]
> On **SQLite** and **MySQL** there is no SQL trigger: `disable_tree_trigger()`
> / `enable_tree_trigger()` toggle the Python maintenance instead, and
> `rebuild_paths()` is also how you resync the tree after a write that bypasses
> the ORM (raw SQL, `cursor.execute`, …), which those backends cannot intercept.
> On **PostgreSQL** the trigger keeps everything consistent on its own, so you
> never need `rebuild_paths()` in normal use.

There is also a bunch of less useful lookups and transforms
available. They will be documented with examples in the future.

### Ordering children

By default the children of a same parent are ordered by primary key. Pass
`order_by` to `PathField` to order them differently — for instance by an
explicit position field, falling back to the name:

```python
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
        ordering = ['path']
        indexes = [*PathField.get_indexes('yourmodel', 'path')]
```

And the corresponding migration:

```python
from django.db import models, migrations
from tree.operations import CreateTreeTrigger

class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0003_tree_functions'),
    ]

    operations = [
        migrations.AddField('YourModel', 'position',
                            models.IntegerField(default=1)),
        CreateTreeTrigger('YourModel'),
    ]
```

### Adding the trigger to a table that already has data

`PathField` is always nullable, so existing rows simply start with a `NULL`
path. Create the trigger, then rebuild the paths from the `parent` FKs:

```python
from django.db import migrations
from tree.operations import CreateTreeTrigger, RebuildPaths

class Migration(migrations.Migration):
    dependencies = [
        ('tree', '0003_tree_functions'),
    ]

    operations = [
        CreateTreeTrigger('YourModel'),
        RebuildPaths('YourModel', 'path'),
    ]
```

> [!NOTE]
> You can also use `PathField` without adding a `CreateTreeTrigger`
> operation. However, the field will not automatically be updated, you
> will have to do it by yourself. In most cases this is not useful, so you
> should not use `PathField` without `CreateTreeTrigger` unless you know
> what you are doing.


## Differences with MPTT and treebeard

### Level vs depth

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


## Contributing

To run the `run_tests.py` and `run_benchmark.py` scripts:
- Make sure you have `uv` installed
- `uv sync --group benchmark`
- `docker run --rm -e POSTGRES_DB=tree -e POSTGRES_USER=tree -e POSTGRES_PASSWORD=test-only -p 5432:5432 postgres:latest -d`
- `uv run run_tests.py` to run regression tests
- `uv run run_benchmark.py` to run the full benchmark against other tree solutions (very long)


## License

django-tree is released under the BSD license. See [LICENSE](LICENSE).
