from __future__ import print_function
from collections import Iterable
import os
from time import time

from django.db import connections, router
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from .models import (
    TreePlace, MPTTPlace, TreebeardMPPlace, TreebeardNSPlace, TreebeardALPlace,
)
from .utils import prefix_unit, SkipTest, LineDisplay


class Benchmark:
    models = {
        MPTTPlace: 'MPTT',
        TreePlace: 'tree',
        TreebeardALPlace: 'treebeard AL',
        TreebeardMPPlace: 'treebeard MP',
        TreebeardNSPlace: 'treebeard NS',
    }
    siblings_per_level = (
        10, 10, 10, 10,
    )
    tests = {}
    ticks_formatters = {
        'Time (s)': FuncFormatter(lambda v, pos: prefix_unit(v, 's')),
        'Disk usage (bytes)': FuncFormatter(
            lambda v, pos: prefix_unit(v, 'B', -3)),
    }
    results_path = 'benchmark/results/'

    def __init__(self):
        self.data = []
        self.router = router.routers[0]
        if not os.path.exists(self.results_path):
            os.makedirs(self.results_path)

    @property
    def current_db_alias(self):
        return self.router.db_alias

    @current_db_alias.setter
    def current_db_alias(self, db_alias):
        self.router.db_alias = db_alias

    def add_data(self, model, test_name, count, value, y_label='Time (s)'):
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
            yield level, model.objects.count()
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
    def register_test(cls, name, models=None, y_label='Time (s)'):
        if models is None:
            models = cls.models
        if not isinstance(models, Iterable):
            models = (models,)

        def inner(test_class):
            for model in models:
                cls.tests[(name, model, y_label)] = test_class

        return inner

    def run_tests(self, tested_model, level, count):
        for (test_name, model, y_label), test_class in self.tests.items():
            if model is not tested_model:
                continue
            benchmark_test = test_class(self, model, level)
            try:
                benchmark_test.setup()
            except SkipTest:
                continue
            start = time()
            value = benchmark_test.run()
            elapsed_time = time() - start
            if value is None:
                value = elapsed_time
            self.add_data(model, test_name, count, value, y_label=y_label)

    def plot(self, df, database_name, test_name, y_label):
        ax = df.rolling(100).mean().plot(
            title=test_name, alpha=0.8,
            xlim=(0, df.index.max() * 1.05),
            ylim=(0, df.max().max() * 1.05),
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

        plt.savefig(
            os.path.join(self.results_path,
                         '%s - %s.svg' % (database_name, test_name)),
            bbox_extra_artists=(legend,), bbox_inches='tight',
        )

    def run(self):
        self.create_databases()

        for db_alias in connections:
            self.current_db_alias = db_alias
            connection = connections[db_alias]

            for model in self.models:
                print('-' * 50)
                print('%s on %s' % (self.models[model], connection.vendor))
                it = self.populate_database(model)
                elapsed_time = 0.0
                with LineDisplay() as line:
                    while True:
                        line.update('Creating new objects...')
                        try:
                            start = time() - elapsed_time
                            level, count = next(it)
                            elapsed_time = time() - start
                        except StopIteration:
                            break
                        self.add_data(model, 'Create all objects',
                                      count, elapsed_time)
                        with connection.cursor() as cursor:
                            # This makes sure the database statistics are
                            # up to date and the disk usage is optimised.
                            cursor.execute(
                                'VACUUM ANALYZE "%s";' % model._meta.db_table)
                        line.update('Testing with %d objects...' % count)
                        self.run_tests(model, level, count)
                    # We delete the objects to avoid impacting
                    # the following tests and to clear some disk space.
                    line.update('Deleting all objects from the table...')
                    model.objects.all().delete()

        df = pd.DataFrame(self.data)
        df.to_csv(os.path.join(self.results_path, 'data.csv'))

        stats_df = df.set_index(['Database', 'Test name', 'Count'])
        stats_df.sort_index(inplace=True)
        group_by = stats_df.groupby(level=[0, 1, 2])
        min_values_series = group_by.min()['Value']
        max_values_series = group_by.max()['Value']
        stats_df['Value'] = ((stats_df['Value'] - min_values_series)
                             / (max_values_series - min_values_series))
        stats_df = stats_df.groupby(['Y label',
                                     'Implementation']).mean()
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
    def __init__(self, benchmark, model, level):
        self.benchmark = benchmark
        self.model = model
        self.level = level

    def setup(self):
        pass

    def run(self):
        raise NotImplementedError


@Benchmark.register_test('Table disk usage (including indexes)',
                         y_label='Disk usage (bytes)')
class TestDiskUsage(BenchmarkTest):
    def run(self):
        with connections[self.benchmark.current_db_alias].cursor() as cursor:
            cursor.execute("SELECT pg_relation_size('%s');"
                           % self.model._meta.db_table)
            return cursor.fetchone()[0]


class GetRootMixin:
    def setup(self):
        self.obj = self.model.objects.order_by('pk').first()


class GetBranchMixin:
    def setup(self):
        if self.level < 3:
            raise SkipTest
        if self.model in (TreePlace, MPTTPlace, TreebeardALPlace):
            self.obj = self.model.objects.filter(
                parent__isnull=False, children__isnull=False).first()
        elif self.model in (TreebeardMPPlace, TreebeardNSPlace):
            self.obj = self.model.objects.filter(depth=1).first()


class GetLeafMixin:
    def setup(self):
        self.obj = self.model.objects.order_by('-pk').first()


#
# Children
#


@Benchmark.register_test('Get children [root]')
class TestGetChildren(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_children())


@Benchmark.register_test('Get children [branch]')
class TestGetChildren(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_children())


@Benchmark.register_test('Get children [leaf]')
class TestGetChildren(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_children())


#
# Children count
#


@Benchmark.register_test('Get children count [root]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCount(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children().count()


@Benchmark.register_test(
    'Get children count [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCount(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children_count()


@Benchmark.register_test('Get children count [branch]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCount(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children().count()


@Benchmark.register_test(
    'Get children count [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCount(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children_count()


@Benchmark.register_test('Get children count [leaf]',
                         (MPTTPlace, TreePlace))
class TestGetChildrenCount(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children().count()


@Benchmark.register_test(
    'Get children count [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetChildrenCount(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_children_count()


#
# Ancestors
#


@Benchmark.register_test('Get ancestors [root]')
class TestGetAncestors(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_ancestors())


@Benchmark.register_test('Get ancestors [branch]',)
class TestGetAncestors(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_ancestors())


@Benchmark.register_test('Get ancestors [leaf]')
class TestGetAncestors(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_ancestors())


#
# Descendants
#


@Benchmark.register_test('Get descendants [root]')
class TestGetDescendants(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_descendants())


@Benchmark.register_test('Get descendants [branch]')
class TestGetDescendants(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_descendants())


@Benchmark.register_test('Get descendants [leaf]')
class TestGetDescendants(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_descendants())


#
# Descendants count
#


@Benchmark.register_test('Get descendants count [root]', TreePlace)
class TestGetDescendantsCount(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [root]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCount(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendant_count()


@Benchmark.register_test('Get descendants count [branch]', TreePlace)
class TestGetDescendantsCount(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [branch]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCount(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendant_count()


@Benchmark.register_test('Get descendants count [leaf]', TreePlace)
class TestGetDescendantsCount(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendants().count()


@Benchmark.register_test(
    'Get descendants count [leaf]',
    (MPTTPlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetDescendantsCount(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_descendant_count()


#
# Siblings
#


@Benchmark.register_test('Get siblings [root]', (MPTTPlace, TreePlace))
class TestGetSiblings(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [root]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetSiblings(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())


@Benchmark.register_test('Get siblings [branch]', (MPTTPlace, TreePlace))
class TestGetSiblings(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [branch]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetSiblings(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())


@Benchmark.register_test('Get siblings [leaf]', (MPTTPlace, TreePlace))
class TestGetSiblings(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings(include_self=True))


@Benchmark.register_test(
    'Get siblings [leaf]',
    (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetSiblings(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())


#
# Previous sibling
#


@Benchmark.register_test('Get previous sibling [root]', MPTTPlace)
class TestGetPrevSibling(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [root]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSibling(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [branch]', MPTTPlace)
class TestGetPrevSibling(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [branch]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSibling(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_prev_sibling()


@Benchmark.register_test('Get previous sibling [leaf]', MPTTPlace)
class TestGetPrevSibling(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_previous_sibling()


@Benchmark.register_test(
    'Get previous sibling [leaf]',
    (TreePlace, TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetPrevSibling(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_prev_sibling()


#
# Next sibling
#


@Benchmark.register_test('Get next sibling [root]')
class TestGetNextSibling(GetRootMixin, BenchmarkTest):
    def run(self):
        self.obj.get_next_sibling()


@Benchmark.register_test('Get next sibling [branch]')
class TestGetNextSibling(GetBranchMixin, BenchmarkTest):
    def run(self):
        self.obj.get_next_sibling()


@Benchmark.register_test('Get next sibling [leaf]')
class TestGetNextSibling(GetLeafMixin, BenchmarkTest):
    def run(self):
        self.obj.get_next_sibling()


#
# Get roots
#


@Benchmark.register_test('Get roots', MPTTPlace)
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model._default_manager.root_nodes())


@Benchmark.register_test('Get roots', TreePlace)
class TestGetSiblings(BenchmarkTest):
    def run(self):
        list(self.model.get_roots())


@Benchmark.register_test(
    'Get roots', (TreebeardALPlace, TreebeardMPPlace, TreebeardNSPlace))
class TestGetRoots(BenchmarkTest):
    def run(self):
        list(self.model.get_root_nodes())
