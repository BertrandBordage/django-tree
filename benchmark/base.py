from __future__ import print_function
from collections import Iterable
import os
from time import time

from django.db import connections, router, transaction
from django.db.models import Max, F
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
from tqdm import tqdm

from .models import (
    TreePlace, MPTTPlace, TreebeardMPPlace, TreebeardNSPlace, TreebeardALPlace,
)
from .utils import prefix_unit, SkipTest


DISK_USAGE = 'Disk usage (bytes)'
READ_LATENCY = 'Read latency (s)'
WRITE_LATENCY = 'Write latency (s)'

BYTES_FORMATTER = FuncFormatter(lambda v, pos: prefix_unit(v, 'B', -3))
SECONDS_FORMATTER = FuncFormatter(lambda v, pos: prefix_unit(v, 's'))


class Benchmark:
    models = {
        MPTTPlace: 'MPTT',
        TreePlace: 'tree',
        TreebeardALPlace: 'treebeard AL',
        TreebeardMPPlace: 'treebeard MP',
        TreebeardNSPlace: 'treebeard NS',
    }
    siblings_per_level = (
        5, 5, 5, 5, 5,
    )
    tests = {}
    ticks_formatters = {
        DISK_USAGE: BYTES_FORMATTER,
        READ_LATENCY: SECONDS_FORMATTER,
        WRITE_LATENCY: SECONDS_FORMATTER,
    }
    results_path = 'benchmark/results/'

    def __init__(self):
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
        self.data.append({
            'Database': connections[self.current_db_alias].vendor,
            'Test name': test_name,
            'Count': count,
            'Implementation': self.models[model],
            'Value': value,
            'Y label': y_label,
        })

    def populate_database(self, model, level=1, parents=(None,)):
        n_siblings = self.siblings_per_level[level-1]
        for parent in parents:
            if model in (TreePlace, TreebeardALPlace):
                bulk = [model(parent=parent)
                        for _ in range(n_siblings)]
                model.objects.bulk_create(bulk)
                objects = model.objects.filter(parent=parent)
            elif model in (TreebeardMPPlace, TreebeardNSPlace):
                # We fetch again each parent because the path can change
                # during the creation of children from the previous parent.
                if parent is not None:
                    parent = model.objects.get(pk=parent.pk)
                objects = [model.add_root() if parent is None
                           else parent.add_child() for _ in range(n_siblings)]
            else:
                objects = [model.objects.create(parent=parent)
                           for _ in range(n_siblings)]
            yield model.objects.count()
            if level < len(self.siblings_per_level):
                for count in self.populate_database(model, level+1, objects):
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

    def run_tests(self, tested_model, count):
        connection = connections[self.current_db_alias]
        for (test_name, model, y_label), test_class in self.tests.items():
            if model is not tested_model:
                continue
            benchmark_test = test_class(self, model)
            with transaction.atomic(using=self.current_db_alias):
                try:
                    benchmark_test.setup()
                except SkipTest:
                    value = elapsed_time = None
                else:
                    start = time()
                    value = benchmark_test.run()
                    elapsed_time = time() - start
                connection.needs_rollback = True
            if value is None:
                value = elapsed_time
            self.add_data(model, test_name, count, value, y_label=y_label)

    def plot(self, df, database_name, test_name, y_label):
        means = df.rolling(70).mean()
        ax = means.plot(
            title=test_name, alpha=0.8,
            xlim=(0, means.index.max() * 1.05),
            ylim=(0, means.max().max() * 1.05),
        )
        ax.set(xlabel='Amount of objects in table', ylabel=y_label)

        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda v, pos: prefix_unit(v, '', -3)))
        if y_label in self.ticks_formatters:
            ax.yaxis.set_major_formatter(self.ticks_formatters[y_label])

        legend = ax.legend(
            loc='upper center', bbox_to_anchor=(0.5, 0.0),
            bbox_transform=plt.gcf().transFigure,
            fancybox=True, shadow=True, ncol=3)

        filename = ('%s - %s.svg' % (database_name,
                                     test_name)).replace(' ', '_')
        plt.savefig(
            os.path.join(self.results_path, filename),
            bbox_extra_artists=(legend,), bbox_inches='tight',
        )

    def run(self):
        self.create_databases()

        for db_alias in connections:
            self.current_db_alias = db_alias
            connection = connections[db_alias]

            for model in sorted(self.models, key=lambda m: m.__name__):
                print('-' * 50)
                print('%s on %s' % (self.models[model], connection.vendor))
                it = self.populate_database(model)
                progress = tqdm(it, total=self.rows_count)
                elapsed_time = 0.0
                while True:
                    try:
                        start = time() - elapsed_time
                        count = next(it)
                        elapsed_time = time() - start
                    except StopIteration:
                        break
                    progress.update(count - progress.n)
                    self.add_data(model, 'Create all objects', count,
                                  elapsed_time, y_label=WRITE_LATENCY)
                    with connection.cursor() as cursor:
                        # This makes sure the table statistics are
                        # up to date and the disk usage is optimised.
                        cursor.execute(
                            'VACUUM ANALYZE "%s";' % model._meta.db_table)
                        # This makes sure the indexes are up to date.
                        cursor.execute(
                            'REINDEX TABLE "%s";' % model._meta.db_table)
                    self.run_tests(model, count)
                # We delete the objects to avoid impacting
                # the following tests and to clear some disk space.
                model.objects.all().delete()

        df = pd.DataFrame(self.data)
        df.to_csv(os.path.join(self.results_path, 'data.csv'), index=False)

        stats_df = df.set_index(['Database', 'Test name', 'Count'])
        stats_df.sort_index(inplace=True)
        group_by = stats_df.groupby(level=[0, 1, 2])
        min_values_series = group_by.min()['Value']
        max_values_series = group_by.max()['Value']
        stats_df['Value'] = ((stats_df['Value'] - min_values_series)
                             / (max_values_series - min_values_series))
        stats_df['Value'] = stats_df['Value'].fillna(1.0)
        stats_df = stats_df.groupby(['Y label', 'Implementation']).mean()
        stats_df['Value'] = ((20 * (1 - stats_df['Value']))
                             .apply(lambda f: '%.1f / 20' % f))
        stats_df.to_html(os.path.join(self.results_path, 'stats.html'),
                         header=False)

        df.set_index('Count', inplace=True)
        for database_name in df['Database'].unique():
            for test_name in df['Test name'].unique():
                sub_df = df[(df['Database'] == database_name)
                            & (df['Test name'] == test_name)]
                y_labels = sub_df['Y label'].unique()
                assert len(y_labels) == 1
                sub_df = sub_df.pivot(columns='Implementation', values='Value')
                self.plot(sub_df, database_name, test_name, y_labels[0])


class BenchmarkTest:
    def __init__(self, benchmark, model):
        self.benchmark = benchmark
        self.model = model

    def setup(self):
        pass

    def run(self):
        raise NotImplementedError


@Benchmark.register_test('Table disk usage (including indexes)',
                         y_label=DISK_USAGE)
class TestDiskUsage(BenchmarkTest):
    def run(self):
        with connections[self.benchmark.current_db_alias].cursor() as cursor:
            cursor.execute("SELECT pg_relation_size('%s');"
                           % self.model._meta.db_table)
            return cursor.fetchone()[0]


class GetRootMixin:
    def setup(self):
        qs = self.model._default_manager.all()
        qs = (qs.filter(depth=1)
              if self.model in (TreebeardMPPlace, TreebeardNSPlace)
              else qs.filter(parent__isnull=True))
        self.root = qs[qs.count() // 2]
        super(GetRootMixin, self).setup()


class GetBranchMixin:
    def setup(self):
        super(GetBranchMixin, self).setup()

        qs = self.model._default_manager.all()
        if hasattr(self, 'root'):
            descendants = self.root.get_descendants()
            if isinstance(descendants, list):
                descendants = [d.pk for d in descendants]
            qs = qs.exclude(pk__in=descendants)

        if self.model is MPTTPlace:
            qs = qs.filter(level=1)
        elif self.model is TreePlace:
            qs = qs.filter(path__level=2)
        elif self.model is TreebeardALPlace:
            qs = qs.filter(parent__isnull=False, parent__parent__isnull=True)
        else:
            qs = qs.filter(depth=2)
        try:
            self.branch = qs[qs.count() // 2]
        except IndexError:
            raise SkipTest


class GetLeafMixin:
    def setup(self):
        super(GetLeafMixin, self).setup()

        qs = self.model._default_manager.all()
        if hasattr(self, 'root'):
            descendants = self.root.get_descendants()
            if isinstance(descendants, list):
                descendants = [d.pk for d in descendants]
            qs = qs.exclude(pk=self.root.pk).exclude(pk__in=descendants)
        if hasattr(self, 'branch'):
            descendants = self.branch.get_descendants()
            if isinstance(descendants, list):
                descendants = [d.pk for d in descendants]
            qs = qs.exclude(pk=self.branch.pk).exclude(pk__in=descendants)

        qs = (qs.annotate(n=Max('depth')).filter(depth=F('n'), depth__gt=1)
              if self.model in (TreebeardMPPlace, TreebeardNSPlace)
              else qs.filter(children__isnull=True, parent__isnull=False))
        try:
            self.leaf = qs[qs.count() // 2]
        except IndexError:
            raise SkipTest


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


@Benchmark.register_test('Get children count [root]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children().count()


@Benchmark.register_test(
    'Get children count [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children_count()


@Benchmark.register_test('Get children count [branch]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children().count()


@Benchmark.register_test(
    'Get children count [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children_count()


@Benchmark.register_test('Get children count [leaf]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children().count()


@Benchmark.register_test(
    'Get children count [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCounteaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children_count()


#
# Filtered children count
#


@Benchmark.register_test('Get filtered children count [root]')
class TestGetFilteredChildrenCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_children().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [branch]')
class TestGetFilteredChildrenCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_children().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered children count [leaf]')
class TestGetFilteredChildrenCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_children().filter(pk__contains='1').count()


#
# Ancestors
#


@Benchmark.register_test('Get ancestors [root]')
class TestGetAncestorsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_ancestors())


@Benchmark.register_test('Get ancestors [branch]',)
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


#
# Descendants count
#


@Benchmark.register_test('Get descendants count [root]', TreePlace)
class TestGetDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [root]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_descendant_count()


@Benchmark.register_test('Get descendants count [branch]', TreePlace)
class TestGetDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [branch]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_descendant_count()


@Benchmark.register_test('Get descendants count [leaf]', TreePlace)
class TestGetDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [leaf]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_descendant_count()


#
# Filtered descendants count
#


@Benchmark.register_test('Get filtered descendants count [root]')
class TestGetFilteredDescendantsCountRoot(GetRootMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super(TestGetFilteredDescendantsCountRoot, self).setup()

    def run(self):
        self.root.get_descendants().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [branch]')
class TestGetFilteredDescendantsCountBranch(GetBranchMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super(TestGetFilteredDescendantsCountBranch, self).setup()

    def run(self):
        self.branch.get_descendants().filter(pk__contains='1').count()


@Benchmark.register_test('Get filtered descendants count [leaf]')
class TestGetFilteredDescendantsCountLeaf(GetLeafMixin, BenchmarkTest):
    def setup(self):
        if self.model is TreebeardALPlace:
            raise SkipTest
        super(TestGetFilteredDescendantsCountLeaf, self).setup()

    def run(self):
        self.leaf.get_descendants().filter(pk__contains='1').count()


#
# Siblings
#


@Benchmark.register_test('Get siblings [root]', (MPTTPlace, TreePlace))
class TestGetSiblingsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetSiblingsRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.root.get_siblings())


@Benchmark.register_test('Get siblings [branch]', (MPTTPlace, TreePlace))
class TestGetSiblingsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetSiblingsBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.branch.get_siblings())


@Benchmark.register_test('Get siblings [leaf]', (MPTTPlace, TreePlace))
class TestGetSiblingsLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.leaf.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
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
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSiblingRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [branch]', MPTTPlace)
class TestGetPrevSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [branch]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [leaf]', MPTTPlace)
class TestGetPrevSiblingLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [leaf]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSiblingLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.get_prev_sibling()


#
# Next sibling
#


@Benchmark.register_test('Get next sibling [root]')
class TestGetNextSiblingRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.get_next_sibling()


@Benchmark.register_test('Get next sibling [branch]')
class TestGetNextSiblingBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.get_next_sibling()


@Benchmark.register_test('Get next sibling [leaf]')
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
        list(self.model.get_roots())


@Benchmark.register_test(
    'Get roots', (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model.get_root_nodes())


#
# Rebuild
#


@Benchmark.register_test('Rebuild paths', MPTTPlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkTest):
    def run(self):
        self.model._default_manager.rebuild()


@Benchmark.register_test('Rebuild paths', TreePlace, y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkTest):
    def run(self):
        self.model.rebuild_paths()


@Benchmark.register_test('Rebuild paths', (TreebeardALPlace, TreebeardNSPlace),
                         y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkTest):
    def setup(self):
        raise SkipTest


@Benchmark.register_test('Rebuild paths', TreebeardMPPlace,
                         y_label=WRITE_LATENCY)
class TestRebuildPaths(BenchmarkTest):
    def run(self):
        self.model.fix_tree()


#
# Create
#


@Benchmark.register_test(
    'Create [root]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestCreateRoot(BenchmarkTest):
    def run(self):
        self.model.objects.create()


@Benchmark.register_test(
    'Create [root]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestCreateRoot(BenchmarkTest):
    def run(self):
        self.model.add_root()


@Benchmark.register_test(
    'Create [branch]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestCreateBranch(GetRootMixin, BenchmarkTest):
    def run(self):
        self.model.objects.create(parent=self.root)


@Benchmark.register_test(
    'Create [branch]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestCreateBranch(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.add_child()


@Benchmark.register_test(
    'Create [leaf]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestCreateLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.model.objects.create(parent=self.leaf)


@Benchmark.register_test(
    'Create [leaf]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestCreateLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.add_child()


#
# Move
#


@Benchmark.register_test(
    'Move [root to branch]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveRootToBranch(GetBranchMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.parent = self.branch
        self.root.save()


@Benchmark.register_test(
    'Move [root to branch]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveRootToBranch(GetBranchMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.move(self.branch, pos='sorted-child')


@Benchmark.register_test(
    'Move [root to leaf]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveRootToLeaf(GetLeafMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.parent = self.leaf
        self.root.save()


@Benchmark.register_test(
    'Move [root to leaf]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveRootToLeaf(GetLeafMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.move(self.leaf, pos='sorted-child')


@Benchmark.register_test(
    'Move [branch to root]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveBranchToRoot(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.parent = None
        self.branch.save()


@Benchmark.register_test(
    'Move [branch to root]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveBranchToRoot(GetBranchMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.branch.move(self.root, pos='sorted-sibling')


@Benchmark.register_test(
    'Move [branch to leaf]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveBranchToLeaf(GetLeafMixin, GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.parent = self.leaf
        self.branch.save()


@Benchmark.register_test(
    'Move [branch to leaf]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveBranchToLeaf(GetLeafMixin, GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.move(self.leaf, pos='sorted-child')


@Benchmark.register_test(
    'Move [leaf to root]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveLeafToRoot(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.parent = None
        self.leaf.save()


@Benchmark.register_test(
    'Move [leaf to root]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveLeafToRoot(GetLeafMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.leaf.move(self.root, pos='sorted-sibling')


@Benchmark.register_test(
    'Move [leaf to branch]',
    (MPTTPlace, TreePlace, TreebeardALPlace), y_label=WRITE_LATENCY)
class TestMoveLeafToBranch(GetLeafMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.leaf.parent = self.root
        self.leaf.save()


@Benchmark.register_test(
    'Move [leaf to branch]',
    (TreebeardMPPlace, TreebeardNSPlace), y_label=WRITE_LATENCY)
class TestMoveLeafToBranch(GetLeafMixin, GetRootMixin, BenchmarkTest):
    def run(self):
        self.leaf.move(self.root, pos='sorted-child')


#
# Delete
#


@Benchmark.register_test('Delete [root]', y_label=WRITE_LATENCY)
class TestDeleteRoot(GetRootMixin, BenchmarkTest):
    def run(self):
        self.root.delete()


@Benchmark.register_test('Delete [branch]', y_label=WRITE_LATENCY)
class TestDeleteBranch(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.branch.delete()


@Benchmark.register_test('Delete [leaf]', y_label=WRITE_LATENCY)
class TestDeleteLeaf(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.leaf.delete()
