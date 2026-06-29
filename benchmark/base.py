# ruff: noqa: F811

from __future__ import print_function
import os
from time import time
from typing import Type, List, Optional, Iterable

from django.db import connections, router, transaction
from django.db.models import Max, F, Model
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
from tqdm import tqdm

from .models import (
    TreePlace,
    MPTTPlace,
    TreebeardMPPlace,
    TreebeardNSPlace,
    TreebeardALPlace,
    TreeNodePlace,
)
from .utils import prefix_unit, SkipTest


DISK_USAGE = 'Disk usage (bytes)'
READ_LATENCY = 'Read latency (s)'
WRITE_LATENCY = 'Write latency (s)'

BYTES_FORMATTER = FuncFormatter(lambda v, pos: prefix_unit(v, 'B', -3))
SECONDS_FORMATTER = FuncFormatter(lambda v, pos: prefix_unit(v, 's'))

# Default tree shape: 5 levels of 5 siblings each, i.e. 5 + 5² + 5³ + 5⁴ + 5⁵ =
# 3905 nodes. Its length also fixes the tree depth used for every `--max-objects`.
DEFAULT_SIBLINGS_PER_LEVEL = (5, 5, 5, 5, 5)

# Every implementation except django-treenode, whose API differs enough (it returns
# plain lists instead of querysets and has no previous/next-sibling navigation) that
# it has to be registered with dedicated tests rather than the shared default.
NON_TREENODE_MODELS = (
    MPTTPlace,
    TreePlace,
    TreebeardALPlace,
    TreebeardMPPlace,
    TreebeardNSPlace,
)


def derive_siblings_per_level(max_objects, depth=len(DEFAULT_SIBLINGS_PER_LEVEL)):
    """Derive a `depth`-level branching tuple holding at most `max_objects` nodes.

    The shape is the most *uniform* tree that fits: the largest branching factor
    `b` whose fully-uniform `(b, …, b)` tree stays within `max_objects`, with the
    leaf level then widened (≥ `b` children each) to spend the leftover budget.
    `max_objects` is therefore an upper bound on the data (hence "maximum
    amount"), and the depth — and so the depth-sensitive tests — stays constant
    while only the breadth scales.

    No special case is needed for the default: `derive_siblings_per_level(3905)`
    reproduces `(5, 5, 5, 5, 5)` exactly, because 3905 is precisely 5 + 5² + … + 5⁵.
    """

    def uniform_total(b):
        n, total = 1, 0
        for _ in range(depth):
            n *= b
            total += n
        return total

    if uniform_total(1) > max_objects:
        # Too small even for a one-child-per-node chain; use the minimum tree.
        return (1,) * depth
    b = 1
    while uniform_total(b + 1) <= max_objects:
        b += 1
    internal = 0  # Nodes above the leaf level.
    leaf_parents = 1  # Nodes on the deepest non-leaf level (the leaf parents).
    for _ in range(depth - 1):
        leaf_parents *= b
        internal += leaf_parents
    leaves_per_parent = (max_objects - internal) // leaf_parents
    return (b,) * (depth - 1) + (leaves_per_parent,)


# Lock the identical-default guarantee: the default run must stay byte-identical.
assert derive_siblings_per_level(3905) == DEFAULT_SIBLINGS_PER_LEVEL


class Benchmark:
    models = {
        MPTTPlace: 'MPTT',
        TreePlace: 'tree',
        TreebeardALPlace: 'treebeard AL',
        TreebeardMPPlace: 'treebeard MP',
        TreebeardNSPlace: 'treebeard NS',
        TreeNodePlace: 'treenode',
    }
    tests = {}
    ticks_formatters = {
        DISK_USAGE: BYTES_FORMATTER,
        READ_LATENCY: SECONDS_FORMATTER,
        WRITE_LATENCY: SECONDS_FORMATTER,
    }
    results_path = 'benchmark/results/'

    def __init__(
        self,
        run_django_tree_only: bool = False,
        selected_tests: Optional[List[str]] = None,
        checkpoint_step: int = 100,
        max_objects: int = 3905,
    ):
        self.run_django_tree_only = run_django_tree_only
        self.selected_tests = selected_tests
        # The whole tree is built; `max_objects` is an upper bound on its size.
        # The default (3905) reproduces the historical (5, 5, 5, 5, 5) tree.
        self.siblings_per_level = derive_siblings_per_level(max_objects)
        # Minimum number of new objects between two measurement checkpoints. The
        # whole tree is still built (same final data); this only controls how many
        # object counts we stop at to run the test suite. A larger value trades
        # data-point density for speed without changing how any individual
        # measurement is taken. The database is vacuumed at every checkpoint, so
        # each measurement is taken on a freshly optimised table.
        self.checkpoint_step = checkpoint_step
        self.data = []
        self.router = router.routers[0]

        self.rows_count = 0
        n = 1
        for i in self.siblings_per_level:
            n *= i
            self.rows_count += n

        if not os.path.exists(self.results_path):
            os.makedirs(self.results_path)

    @property
    def current_db_alias(self):
        return self.router.db_alias

    @current_db_alias.setter
    def current_db_alias(self, db_alias):
        self.router.db_alias = db_alias

    def add_data(self, model, test_name, count, value, y_label=READ_LATENCY):
        self.data.append(
            {
                'Database': connections[self.current_db_alias].vendor,
                'Test name': test_name,
                'Count': count,
                'Implementation': self.models[model],
                'Value': value,
                'Y label': y_label,
            }
        )

    def populate_database(self, model, level=1, parents=(None,)):
        n_siblings = self.siblings_per_level[level - 1]
        for parent in parents:
            if model in (TreePlace, TreebeardALPlace):
                bulk = [model(parent=parent) for _ in range(n_siblings)]
                model.objects.bulk_create(bulk)
                objects = model.objects.filter(parent=parent)
            elif model in (TreebeardMPPlace, TreebeardNSPlace):
                # We fetch again each parent because the path can change
                # during the creation of children from the previous parent.
                if parent is not None:
                    parent = model.objects.get(pk=parent.pk)
                objects = [
                    model.add_root() if parent is None else parent.add_child()
                    for _ in range(n_siblings)
                ]
            elif model is TreeNodePlace:
                objects = [
                    model.objects.create(tn_parent=parent) for _ in range(n_siblings)
                ]
            else:
                objects = [
                    model.objects.create(parent=parent) for _ in range(n_siblings)
                ]
            yield model.objects.count()
            if level < len(self.siblings_per_level):
                for count in self.populate_database(model, level + 1, objects):
                    yield count

    def create_databases(self):
        old_db_names = {}
        for alias in connections:
            conn = connections[alias]
            old_db_names[alias] = conn.settings_dict['NAME']
            conn.creation.create_test_db(autoclobber=True)

    @classmethod
    def register_test(cls, name, models=None, y_label=READ_LATENCY):
        if models is None:
            models = cls.models
        if not isinstance(models, Iterable):
            models = (models,)

        def inner(test_class):
            for model in models:
                cls.tests[(name, model, y_label)] = test_class
            return test_class

        return inner

    def skip_test(self, test_name: str) -> bool:
        return self.selected_tests and test_name not in self.selected_tests

    def run_tests(self, tested_model, count):
        connection = connections[self.current_db_alias]
        # Every test at this checkpoint sees identical data, so the root/branch/leaf
        # selection (the expensive, untimed part of each test's setup) is computed
        # once and reused across tests instead of being recomputed per test. The
        # cache is reset per checkpoint because the tree grows between checkpoints.
        self._selection_cache = {}
        for (test_name, model, y_label), test_class in self.tests.items():
            if model is not tested_model or self.skip_test(test_name):
                continue
            benchmark_test = test_class(self, model)
            try:
                benchmark_test.setup()
            except SkipTest:
                value = elapsed_time = None
            else:
                start = time()
                if benchmark_test.rollback:
                    with transaction.atomic(using=self.current_db_alias):
                        value = benchmark_test.run()
                        connection.needs_rollback = True
                else:
                    value = benchmark_test.run()
                elapsed_time = time() - start
            if value is None:
                value = elapsed_time
            self.add_data(model, test_name, count, value, y_label=y_label)

    # Sentinel meaning "this selection legitimately has no candidate at this
    # checkpoint", cached so the (expensive) lookup is not retried per test.
    _SKIP = object()

    def select_root(self, model, fresh=False):
        cache = self._selection_cache
        if 'root' not in cache:
            qs = model._default_manager.all()
            if model in (TreebeardMPPlace, TreebeardNSPlace):
                qs = qs.filter(depth=1)
            elif model is TreeNodePlace:
                qs = qs.filter(tn_parent__isnull=True)
            else:
                qs = qs.filter(parent__isnull=True)
            cache['root'] = qs[qs.count() // 2]
        obj = cache['root']
        # Write tests mutate the selected object (rename, reparent, delete), so they
        # get a fresh, isolated copy; read tests can safely share the cached object.
        return model._default_manager.get(pk=obj.pk) if fresh else obj

    def select_branch(self, model, root, fresh=False):
        cache = self._selection_cache
        key = ('branch', root is not None)
        if key not in cache:
            qs = model._default_manager.all()
            if root is not None:
                descendants = root.get_descendants()
                if isinstance(descendants, list):
                    descendants = [d.pk for d in descendants]
                qs = qs.exclude(pk__in=descendants)
            if model is MPTTPlace:
                qs = qs.filter(level=1)
            elif model is TreePlace:
                qs = qs.filter(path__level=2)
            elif model is TreebeardALPlace:
                qs = qs.filter(parent__isnull=False, parent__parent__isnull=True)
            elif model is TreeNodePlace:
                qs = qs.filter(tn_level=2)
            else:
                qs = qs.filter(depth=2)
            try:
                cache[key] = qs[qs.count() // 2]
            except IndexError:
                cache[key] = self._SKIP
        obj = cache[key]
        if obj is self._SKIP:
            raise SkipTest
        return model._default_manager.get(pk=obj.pk) if fresh else obj

    def select_leaf(self, model, root, branch, fresh=False):
        cache = self._selection_cache
        key = ('leaf', root is not None, branch is not None)
        if key not in cache:
            qs = model._default_manager.all()
            if root is not None:
                descendants = root.get_descendants()
                if isinstance(descendants, list):
                    descendants = [d.pk for d in descendants]
                qs = qs.exclude(pk=root.pk).exclude(pk__in=descendants)
            if branch is not None:
                descendants = branch.get_descendants()
                if isinstance(descendants, list):
                    descendants = [d.pk for d in descendants]
                qs = qs.exclude(pk=branch.pk).exclude(pk__in=descendants)
            if model in (TreebeardMPPlace, TreebeardNSPlace):
                qs = qs.annotate(n=Max('depth')).filter(depth=F('n'), depth__gt=1)
            elif model is TreeNodePlace:
                qs = qs.filter(tn_children_count=0, tn_parent__isnull=False)
            else:
                qs = qs.filter(children__isnull=True, parent__isnull=False)
            try:
                cache[key] = qs[qs.count() // 2]
            except IndexError:
                cache[key] = self._SKIP
        obj = cache[key]
        if obj is self._SKIP:
            raise SkipTest
        return model._default_manager.get(pk=obj.pk) if fresh else obj

    def plot(self, df, database_name, test_name, y_label):
        # Smooth over a fixed fraction of the recorded data points, sizing the
        # rolling window by the number of points rather than the object-count
        # range. This keeps the window below the row count whatever the
        # --checkpoint-step is (otherwise the rolling mean is entirely NaN and
        # the plot's y-limits blow up), while staying identical to the previous
        # df.index.max() // 20 at the default step (781 points -> window 195).
        means = df.rolling(max(len(df) // 4, 1)).mean()
        ax = means.plot(
            title=test_name,
            alpha=0.8,
            xlim=(0, means.index.max() * 1.05),
            ylim=(0, means.max().max() * 1.05),
        )
        ax.set(xlabel='Amount of objects in table', ylabel=y_label)

        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda v, pos: prefix_unit(v, '', -3))
        )
        if y_label in self.ticks_formatters:
            ax.yaxis.set_major_formatter(self.ticks_formatters[y_label])

        legend = ax.legend(
            loc='upper center',
            bbox_to_anchor=(0.5, 0.0),
            bbox_transform=plt.gcf().transFigure,
            fancybox=True,
            shadow=True,
            ncol=3,
        )

        filename = ('%s - %s.svg' % (database_name, test_name)).replace(' ', '_')
        plt.savefig(
            os.path.join(self.results_path, filename),
            bbox_extra_artists=(legend,),
            bbox_inches='tight',
        )

    def force_update_db_stats_and_indexes(self, model: Type[Model]):
        with connections[self.current_db_alias].cursor() as cursor:
            # This makes sure the table statistics and disk usage are optimised.
            # VACUUM FULL rewrites the table into a fresh file and rebuilds every
            # index as part of that rewrite, so a separate REINDEX is redundant.
            cursor.execute('VACUUM FULL ANALYZE "%s";' % model._meta.db_table)

    def run(self):
        self.create_databases()

        for db_alias in connections:
            self.current_db_alias = db_alias
            connection = connections[db_alias]

            for model in sorted(self.models, key=lambda m: m.__name__):
                if self.run_django_tree_only and model is not TreePlace:
                    continue
                print('-' * 50)
                print('%s on %s' % (self.models[model], connection.vendor))
                it = self.populate_database(model)
                progress = tqdm(it, total=self.rows_count)
                elapsed_time = 0.0
                last_checkpoint = 0
                while True:
                    try:
                        start = time() - elapsed_time
                        count = next(it)
                        elapsed_time = time() - start
                    except StopIteration:
                        break
                    progress.update(count - progress.n)
                    # Only stop to measure at checkpoints; the tree is always built
                    # in full. The last count is always a checkpoint so every run
                    # ends on the complete tree.
                    if (
                        count - last_checkpoint < self.checkpoint_step
                        and count != self.rows_count
                    ):
                        continue
                    last_checkpoint = count
                    # Optimise the table before measuring, so every checkpoint is
                    # taken on a freshly vacuumed table.
                    self.force_update_db_stats_and_indexes(model)
                    self.add_data(
                        model,
                        'Create all objects',
                        count,
                        elapsed_time,
                        y_label=WRITE_LATENCY,
                    )
                    self.run_tests(model, count)
                # We delete the objects to avoid impacting
                # the following tests and to clear some disk space.
                model.objects.all().delete()

        csv_path = os.path.join(self.results_path, 'data.csv.gz')
        if self.run_django_tree_only:
            df = pd.read_csv(csv_path)
            df = df[df['Implementation'] != self.models[TreePlace]]
            df = pd.concat([df, pd.DataFrame(self.data)], ignore_index=True)
        else:
            df = pd.DataFrame(self.data)
        # Stored gzip-compressed (pandas infers it from the .gz extension, no extra
        # dependency): ~10x smaller in the repo. Sorted first and written with a
        # zeroed gzip timestamp so identical data always produces identical bytes,
        # keeping diffs minimal.
        df = df.sort_values(
            ['Database', 'Y label', 'Test name', 'Implementation', 'Count']
        )
        df.to_csv(csv_path, index=False, compression={'method': 'gzip', 'mtime': 0})

        # For every individual measurement (a Database/Test/Count group), rank the
        # competing implementations from fastest/smallest (1) to slowest/largest;
        # lower is always better here, whether it's latency or disk usage. Skipped
        # or unsupported measurements are NaN and stay out of the ranking. Averaging
        # those ranks per implementation answers "on average, which comes first,
        # second, third, …" across all the tests of each category.
        stats_df = df.set_index(['Database', 'Test name', 'Count'])
        stats_df.sort_index(inplace=True)
        stats_df['Rank'] = stats_df.groupby(level=[0, 1, 2])['Value'].rank()
        stats_df = stats_df.groupby(['Y label', 'Implementation'])[['Rank']].mean()
        stats_df['Rank'] = stats_df['Rank'].apply(lambda r: '%.2f' % r)
        stats_df.to_html(os.path.join(self.results_path, 'stats.html'), header=False)

        df.set_index('Count', inplace=True)
        for database_name in df['Database'].unique():
            for test_name in df['Test name'].unique():
                if self.skip_test(test_name):
                    continue
                sub_df = df[
                    (df['Database'] == database_name) & (df['Test name'] == test_name)
                ]
                y_labels = sub_df['Y label'].unique()
                assert len(y_labels) == 1
                sub_df = sub_df.pivot(columns='Implementation', values='Value')
                self.plot(sub_df, database_name, test_name, y_labels[0])


class BenchmarkTest:
    rollback: bool = False

    def __init__(self, benchmark, model):
        self.benchmark = benchmark
        self.model = model

    def setup(self):
        pass

    def run(self):
        raise NotImplementedError


class BenchmarkWriteTest(BenchmarkTest):
    rollback = True


@Benchmark.register_test('Table disk usage (including indexes)', y_label=DISK_USAGE)
class TestDiskUsage(BenchmarkTest):
    def run(self):
        with connections[self.benchmark.current_db_alias].cursor() as cursor:
            cursor.execute(
                "SELECT pg_total_relation_size('%s');" % self.model._meta.db_table
            )
            return cursor.fetchone()[0]


class GetRootMixin:
    def setup(self):
        self.root = self.benchmark.select_root(self.model, fresh=self.rollback)
        super().setup()


class GetBranchMixin:
    def setup(self):
        super().setup()
        # Selection depends on whether a root was also picked (to exclude its
        # subtree), so pass it along when present, matching the original logic.
        self.branch = self.benchmark.select_branch(
            self.model, getattr(self, 'root', None), fresh=self.rollback
        )


class GetLeafMixin:
    def setup(self):
        super(GetLeafMixin, self).setup()
        self.leaf = self.benchmark.select_leaf(
            self.model,
            getattr(self, 'root', None),
            getattr(self, 'branch', None),
            fresh=self.rollback,
        )


#
# Children
#


@Benchmark.register_test('Get children [root]')
class TestGetChildrenRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_children())


@Benchmark.register_test('Get children [branch]')
class TestGetChildrenBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_children())


@Benchmark.register_test('Get children [leaf]')
class TestGetChildrenLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_children())


#
# Children count
#


@Benchmark.register_test('Get children count [root]', (MPTTPlace, TreePlace))
class TestGetChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children().count()


@Benchmark.register_test(
    'Get children count [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children_count()


@Benchmark.register_test('Get children count [branch]', (MPTTPlace, TreePlace))
class TestGetChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children().count()


@Benchmark.register_test(
    'Get children count [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children_count()


@Benchmark.register_test('Get children count [leaf]', (MPTTPlace, TreePlace))
class TestGetChildrenCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children().count()


@Benchmark.register_test(
    'Get children count [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetChildrenCounteaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children_count()


#
# Filtered children count
#


@Benchmark.register_test('Get filtered children count [root]', NON_TREENODE_MODELS)
class TestGetFilteredChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [root]', TreeNodePlace)
class TestGetFilteredChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children_queryset().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [branch]', NON_TREENODE_MODELS)
class TestGetFilteredChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [branch]', TreeNodePlace)
class TestGetFilteredChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children_queryset().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [leaf]', NON_TREENODE_MODELS)
class TestGetFilteredChildrenCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [leaf]', TreeNodePlace)
class TestGetFilteredChildrenCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children_queryset().filter(pk__contains='1').count()


#
# Ancestors
#


@Benchmark.register_test('Get ancestors [root]')
class TestGetAncestorsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_ancestors())


@Benchmark.register_test(
    'Get ancestors [branch]',
)
class TestGetAncestorsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_ancestors())


@Benchmark.register_test('Get ancestors [leaf]')
class TestGetAncestorsLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_ancestors())


#
# Descendants
#


@Benchmark.register_test('Get descendants [root]')
class TestGetDescendantsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_descendants())


@Benchmark.register_test('Get descendants [branch]')
class TestGetDescendantsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_descendants())


@Benchmark.register_test('Get descendants [leaf]')
class TestGetDescendantsLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_descendants())


@Benchmark.register_test('Get descendants from queryset', (MPTTPlace, TreePlace))
class TestGetDescendantsFromQuerySet(BenchmarkTest):
    def setup(self):
        self.qs = self.model._default_manager.annotate(n=F('pk') % 5).filter(n=0)
        super().setup()

    def run(self):
        list(self.qs.get_descendants())


#
# Descendants count
#


@Benchmark.register_test('Get descendants count [root]', TreePlace)
class TestGetDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [root]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendant_count()


@Benchmark.register_test('Get descendants count [root]', TreeNodePlace)
class TestGetDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendants_count()


@Benchmark.register_test('Get descendants count [branch]', TreePlace)
class TestGetDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [branch]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendant_count()


@Benchmark.register_test('Get descendants count [branch]', TreeNodePlace)
class TestGetDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendants_count()


@Benchmark.register_test('Get descendants count [leaf]', TreePlace)
class TestGetDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [leaf]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendant_count()


@Benchmark.register_test('Get descendants count [leaf]', TreeNodePlace)
class TestGetDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendants_count()


#
# Filtered descendants count
#


@Benchmark.register_test('Get filtered descendants count [root]', NON_TREENODE_MODELS)
class TestGetFilteredDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super().setup()

    def run(self):
        self.root.get_descendants().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [root]', TreeNodePlace)
class TestGetFilteredDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendants_queryset().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [branch]', NON_TREENODE_MODELS)
class TestGetFilteredDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super().setup()

    def run(self):
        self.branch.get_descendants().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [branch]', TreeNodePlace)
class TestGetFilteredDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendants_queryset().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [leaf]', NON_TREENODE_MODELS)
class TestGetFilteredDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super().setup()

    def run(self):
        self.leaf.get_descendants().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [leaf]', TreeNodePlace)
class TestGetFilteredDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendants_queryset().filter(pk__contains='1').count()


#
# Siblings
#


@Benchmark.register_test('Get siblings [root]', (MPTTPlace, TreePlace))
class TestGetSiblingsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetSiblingsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_siblings())


@Benchmark.register_test('Get siblings [branch]', (MPTTPlace, TreePlace))
class TestGetSiblingsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetSiblingsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_siblings())


@Benchmark.register_test('Get siblings [leaf]', (MPTTPlace, TreePlace))
class TestGetSiblingsLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace, TreeNodePlace),
)
class TestGetSiblingsLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_siblings())


#
# Previous sibling
#


@Benchmark.register_test('Get previous sibling [root]', MPTTPlace)
class TestGetPrevSiblingRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [root]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetPrevSiblingRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [branch]', MPTTPlace)
class TestGetPrevSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [branch]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetPrevSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [leaf]', MPTTPlace)
class TestGetPrevSiblingLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [leaf]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace),
)
class TestGetPrevSiblingLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_prev_sibling()


#
# Next sibling
#


@Benchmark.register_test('Get next sibling [root]', NON_TREENODE_MODELS)
class TestGetNextSiblingRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_next_sibling()


@Benchmark.register_test('Get next sibling [branch]', NON_TREENODE_MODELS)
class TestGetNextSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_next_sibling()


@Benchmark.register_test('Get next sibling [leaf]', NON_TREENODE_MODELS)
class TestGetNextSiblingLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_next_sibling()


#
# Get roots
#


@Benchmark.register_test('Get roots', MPTTPlace)
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model._default_manager.root_nodes())


@Benchmark.register_test('Get roots', TreePlace)
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model.objects.filter_roots())


@Benchmark.register_test(
    'Get roots', (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace)
)
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model.get_root_nodes())


@Benchmark.register_test('Get roots', TreeNodePlace)
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model.get_roots())


#
# Rebuild
#


@Benchmark.register_test('Rebuild paths', MPTTPlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkWriteTest):
    def run(self):
        self.model._default_manager.rebuild()


@Benchmark.register_test('Rebuild paths', TreePlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkWriteTest):
    def run(self):
        self.model.rebuild_paths()


@Benchmark.register_test(
    'Rebuild paths', (TreebeardALPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestRebuildPaths(BenchmarkWriteTest):
    def setup(self):
        raise SkipTest


@Benchmark.register_test('Rebuild paths', TreebeardMPPlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkWriteTest):
    def run(self):
        self.model.fix_tree()


@Benchmark.register_test('Rebuild paths', TreeNodePlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkWriteTest):
    def run(self):
        self.model.update_tree()


#
# Create
#


@Benchmark.register_test(
    'Create [root]',
    (MPTTPlace, TreePlace, TreebeardALPlace, TreeNodePlace),
    y_label=WRITE_LATENCY,
)
class TestCreateRoot(BenchmarkWriteTest):
    def run(self):
        self.model.objects.create()


@Benchmark.register_test(
    'Create [root]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestCreateRoot(BenchmarkWriteTest):
    def run(self):
        self.model.add_root()


@Benchmark.register_test(
    'Create [branch]', (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY
)
class TestCreateBranch(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.model.objects.create(parent=self.root)


@Benchmark.register_test(
    'Create [branch]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestCreateBranch(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.add_child()


@Benchmark.register_test('Create [branch]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestCreateBranch(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.model.objects.create(tn_parent=self.root)


@Benchmark.register_test(
    'Create [leaf]', (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY
)
class TestCreateLeaf(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.model.objects.create(parent=self.leaf)


@Benchmark.register_test(
    'Create [leaf]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestCreateLeaf(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.add_child()


@Benchmark.register_test('Create [leaf]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestCreateLeaf(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.model.objects.create(tn_parent=self.leaf)


#
# Save without any change
#


@Benchmark.register_test('Save without change [root]', y_label=WRITE_LATENCY)
class TestSaveRootWithoutChange(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.save()


@Benchmark.register_test('Save without change [branch]', y_label=WRITE_LATENCY)
class TestSaveBranchWithoutChange(GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.save()


@Benchmark.register_test('Save without change [leaf]', y_label=WRITE_LATENCY)
class TestSaveLeafWithoutChange(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.save()


#
# Move
#


@Benchmark.register_test('Move [same root path]', y_label=WRITE_LATENCY)
class TestMoveSameRootPath(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.name += ' 2'
        self.root.save()


@Benchmark.register_test('Move [same branch path]', y_label=WRITE_LATENCY)
class TestMoveSameBranchPath(GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.name += ' 2'
        self.branch.save()


@Benchmark.register_test('Move [same leaf path]', y_label=WRITE_LATENCY)
class TestMoveSameLeafPath(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.name += ' 2'
        self.leaf.save()


@Benchmark.register_test(
    'Move [root to branch]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveRootToBranch(GetBranchMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.parent = self.branch
        self.root.save()


@Benchmark.register_test(
    'Move [root to branch]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveRootToBranch(GetBranchMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.move(self.branch, pos='sorted-child')


@Benchmark.register_test('Move [root to branch]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveRootToBranch(GetBranchMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.set_parent(self.branch)


@Benchmark.register_test(
    'Move [root to leaf]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveRootToLeaf(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.parent = self.leaf
        self.root.save()


@Benchmark.register_test(
    'Move [root to leaf]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveRootToLeaf(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.move(self.leaf, pos='sorted-child')


@Benchmark.register_test('Move [root to leaf]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveRootToLeaf(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.set_parent(self.leaf)


@Benchmark.register_test(
    'Move [branch to root]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveBranchToRoot(GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.parent = None
        self.branch.save()


@Benchmark.register_test(
    'Move [branch to root]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveBranchToRoot(GetBranchMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.move(self.root, pos='sorted-sibling')


@Benchmark.register_test('Move [branch to root]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveBranchToRoot(GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.set_parent(None)


@Benchmark.register_test(
    'Move [branch to leaf]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveBranchToLeaf(GetLeafMixin, GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.parent = self.leaf
        self.branch.save()


@Benchmark.register_test(
    'Move [branch to leaf]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveBranchToLeaf(GetLeafMixin, GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.move(self.leaf, pos='sorted-child')


@Benchmark.register_test('Move [branch to leaf]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveBranchToLeaf(GetLeafMixin, GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.set_parent(self.leaf)


@Benchmark.register_test(
    'Move [leaf to root]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveLeafToRoot(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.parent = None
        self.leaf.save()


@Benchmark.register_test(
    'Move [leaf to root]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveLeafToRoot(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.move(self.root, pos='sorted-sibling')


@Benchmark.register_test('Move [leaf to root]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveLeafToRoot(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.set_parent(None)


@Benchmark.register_test(
    'Move [leaf to branch]',
    (MPTTPlace, TreePlace, TreebeardALPlace),
    y_label=WRITE_LATENCY,
)
class TestMoveLeafToBranch(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.parent = self.root
        self.leaf.save()


@Benchmark.register_test(
    'Move [leaf to branch]', (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY
)
class TestMoveLeafToBranch(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.move(self.root, pos='sorted-child')


@Benchmark.register_test('Move [leaf to branch]', TreeNodePlace, y_label=WRITE_LATENCY)
class TestMoveLeafToBranch(GetLeafMixin, GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.set_parent(self.root)


#
# Delete
#


@Benchmark.register_test('Delete [root]', y_label=WRITE_LATENCY)
class TestDeleteRoot(GetRootMixin, BenchmarkWriteTest):
    def run(self):
        self.root.delete()


@Benchmark.register_test('Delete [branch]', y_label=WRITE_LATENCY)
class TestDeleteBranch(GetBranchMixin, BenchmarkWriteTest):
    def run(self):
        self.branch.delete()


@Benchmark.register_test('Delete [leaf]', y_label=WRITE_LATENCY)
class TestDeleteLeaf(GetLeafMixin, BenchmarkWriteTest):
    def run(self):
        self.leaf.delete()
