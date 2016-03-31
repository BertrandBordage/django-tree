from __future__ import print_function
from collections import Iterable
import os
from time import time

from django.db import connections, router
import matplotlib.pyplot as plt
import pandas as pd

from .models import (
    TreePlace, MPTTPlace, TreebeardMPPlace, TreebeardNSPlace, TreebeardALPlace,
)


class SkipTest(Exception):
    pass


class Benchmark:
    models = {
        MPTTPlace: 'MPTT',
        TreePlace: 'tree',
        TreebeardALPlace: 'treebeard AL',
        TreebeardMPPlace: 'treebeard MP',
        TreebeardNSPlace: 'treebeard NS',
    }
    siblings_per_level = (
        7, 6, 5, 4, 3, 2, 2, 2, 2,
    )
    time_it_iterations = 30
    tests = {}
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
            'Implementation': self.models[model],
            'Test name': test_name,
            'Value': value,
            'Y label': y_label,
            'Count': count,
        })

    def populate_database(self, model, level=1, parents=(None,)):
        n_siblings = self.siblings_per_level[level-1]
        if model in (TreePlace, TreebeardALPlace):
            bulk = []
            for parent in parents:
                bulk.extend([model(parent=parent)
                             for _ in range(n_siblings)])
            model.objects.bulk_create(bulk)
            objects = model.objects.all()
            if parents != (None,):
                objects = objects.filter(parent__in=parents)
        elif model in (TreebeardMPPlace, TreebeardNSPlace):
            objects = []
            for parent in parents:
                # We have to fetch again each because the path can have changed
                # during the creation of children from the previous parent.
                if parent is not None:
                    parent = model.objects.get(pk=parent.pk)
                for _ in range(n_siblings):
                    objects.append(model.add_root() if parent is None
                                   else parent.add_child())
        else:
            objects = []
            for parent in parents:
                for _ in range(n_siblings):
                    objects.append(model.objects.create(parent=parent))
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
            for i in range(self.time_it_iterations):
                value = benchmark_test.run()
            elapsed_time = (time() - start) / self.time_it_iterations
            if value is None:
                value = elapsed_time
            self.add_data(model, test_name, count, value, y_label)

    def plot_series(self, series, database_name, test_name, y_label):
        ax = series.plot(marker='x', title=test_name)
        ax.set(xlabel='Amount of objects in table', ylabel=y_label)
        plt.savefig(os.path.join(
            self.results_path,
            '%s - %s.svg' % (database_name, test_name)))

    def run(self):
        self.create_databases()

        for db_alias in connections:
            self.current_db_alias = db_alias

            for model in self.models:
                print('-' * 50)
                print('%s on %s:' % (
                    self.models[model],
                    connections[db_alias].vendor))
                it = self.populate_database(model)
                elapsed_time = 0.0
                while True:
                    print('Creating new objects...', end='\r')
                    try:
                        start = time() - elapsed_time
                        level, count = next(it)
                        elapsed_time = time() - start
                    except StopIteration:
                        break
                    self.add_data(model, 'Create all objects',
                                  count, elapsed_time)
                    print('Testing %d objects...' % count, end='\r')
                    self.run_tests(model, level, count)

        df = pd.DataFrame(self.data)
        df.set_index('Count', inplace=True)
        for database_name in df['Database'].unique():
            for test_name in df['Test name'].unique():
                sub_df = df[(df['Database'] == database_name)
                            & (df['Test name'] == test_name)]
                y_labels = sub_df['Y label'].unique()
                assert len(y_labels) == 1
                sub_df = sub_df.pivot(columns='Implementation', values='Value')
                self.plot_series(sub_df, database_name, test_name, y_labels[0])


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


@Benchmark.register_test('Get siblings [root]')
class TestGetSiblings(GetRootMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())


@Benchmark.register_test('Get siblings [branch]')
class TestGetSiblings(GetBranchMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())


@Benchmark.register_test('Get siblings [leaf]')
class TestGetSiblings(GetLeafMixin, BenchmarkTest):
    def run(self):
        list(self.obj.get_siblings())
