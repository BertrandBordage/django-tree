# coding: utf-8

from __future__ import unicode_literals

import decimal
from unittest import expectedFailure

from django.db import transaction, InternalError, connection
from django.test import TransactionTestCase

from .models import Place, Person


# TODO: Test same order_by values.
# TODO: Test order_by with descending orders.
# TODO: Test what happens when we move a node after itself
#       while staying in the same siblinghood
#       (it should not create a hole at the former position).
# TODO: Test ORM update/delete.
# TODO: Test raw SQL insertion/update/delete.
# TODO: Test if rebuild works with NULL path values.
# TODO: Test using Path objects as sql parameters.
# TODO: Test multiple path fields on the same model.
# TODO: Test `disable_trigger`, `enable_trigger`, & `disabled_trigger`.
# TODO: Test if `disabled_trigger` does not affect
#       a concurrent node creation/update.
# TODO: Test if breaking a transaction reverts the changes done by the trigger
#       when updating nodes during that transaction.
# TODO: Test non-integer primary keys.
# TODO: Test other `on_delete` behaviour than `CASCADE`.
# TODO: Test unusual table names.


def path(*path_components):
    return [decimal.Decimal(f'{value:.10f}') for value in path_components]


class CommonTest(TransactionTestCase):
    maxDiff = 1000

    def create_place(self, name, parent=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            p = Place.objects.create(name=name, parent=parent)
        # We fetch the object again to populate the path.
        return Place.objects.get(pk=p.pk)

    def create_test_places(self):
        self.correct_raw_places_data = [
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ]
        self.correct_places_data = [
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, 0), 'Eure'),
            (path(0, 0, 1), 'Manche'),
            (path(0, 0, 2), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ]
        france = self.create_place('France')
        yield france
        normandie = self.create_place('Normandie', france)
        yield normandie
        yield self.create_place('Seine-Maritime', normandie)
        yield self.create_place('Eure', normandie)
        yield self.create_place('Manche', normandie)
        osterreich = self.create_place('Österreich')
        yield osterreich
        vienne = self.create_place('Vienne', osterreich)
        yield vienne
        poitou_charentes = self.create_place('Poitou-Charentes', france)
        yield poitou_charentes
        yield self.create_place('Poitiers', vienne)
        vienne.parent = poitou_charentes
        vienne.save()
        yield vienne

    def create_all_test_places(self):
        list(self.create_test_places())

    def assertPlaces(self, values, queryset=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            if queryset is None:
                queryset = Place.objects.all()
            places = list(queryset)
            self.assertListEqual([(p.path.value, p.name) for p in places],
                                 values)


class PathTest(CommonTest):
    maxDiff = None

    def test_path_on_creation(self):
        with self.assertNumQueries(1):
            place1 = Place.objects.create(name='place1')
        # 1 query because the path got deferred,  forcing Django to run
        # a new query to get the updated value when we need it. Same below.
        with self.assertNumQueries(1):
            self.assertEqual(place1.path.value, path(0))
        with self.assertNumQueries(1):
            place2 = Place.objects.create(name='place2', parent=place1)
        with self.assertNumQueries(1):
            self.assertEqual(place2.path.value, path(0, 0))
        with self.assertNumQueries(1):
            place2.parent = None
            place2.save()
        with self.assertNumQueries(1):
            self.assertEqual(place2.path.value, path(1))

    def test_insert(self):
        it = self.create_test_places()
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, 0), 'Seine-Maritime'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, 0), 'Seine-Maritime'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(1), 'Österreich'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(1), 'Österreich'),
            (path(1, 0), 'Vienne'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(1), 'Österreich'),
            (path(1, 0), 'Vienne'),
        ])
        next(it)
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(1), 'Österreich'),
            (path(1, 0), 'Vienne'),
            (path(1, 0, 0), 'Poitiers'),
        ])
        next(it)
        self.assertPlaces(self.correct_raw_places_data)
        Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_delete(self):
        self.create_all_test_places()

        self.assertPlaces(self.correct_raw_places_data)

        # Leaf
        manche = Place.objects.get(name='Manche')
        with self.assertNumQueries(3):
            manche.delete()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(3):
            normandie.delete()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(3):
            france.delete()
        self.assertPlaces([
            (path(1), 'Österreich'),
        ])

    def test_move_root_to_prev_root(self):
        self.create_all_test_places()

        osterreich = Place.objects.get(name='Österreich')
        osterreich.name = 'Autriche'
        with self.assertNumQueries(1):
            osterreich.save()
        self.assertPlaces([
            (path(-1), 'Autriche'),
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'Autriche'),
            (path(1), 'France'),
            (path(1, 0), 'Normandie'),
            (path(1, 0, 0), 'Eure'),
            (path(1, 0, 1), 'Manche'),
            (path(1, 0, 2), 'Seine-Maritime'),
            (path(1, 1), 'Poitou-Charentes'),
            (path(1, 1, 0), 'Vienne'),
            (path(1, 1, 0, 0), 'Poitiers'),
        ])

    def test_move_root_to_next_root(self):
        self.create_all_test_places()

        france = Place.objects.get(name='France')
        france.name = 'République française'
        with self.assertNumQueries(1):
            france.save()
        self.assertPlaces([
            (path(1), 'Österreich'),
            (path(2), 'République française'),
            (path(2, 0), 'Normandie'),
            (path(2, 0, -1), 'Eure'),
            (path(2, 0, -0.5), 'Manche'),
            (path(2, 0, 0), 'Seine-Maritime'),
            (path(2, 1), 'Poitou-Charentes'),
            (path(2, 1, 0), 'Vienne'),
            (path(2, 1, 0, 0), 'Poitiers'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'Österreich'),
            (path(1), 'République française'),
            (path(1, 0), 'Normandie'),
            (path(1, 0, 0), 'Eure'),
            (path(1, 0, 1), 'Manche'),
            (path(1, 0, 2), 'Seine-Maritime'),
            (path(1, 1), 'Poitou-Charentes'),
            (path(1, 1, 0), 'Vienne'),
            (path(1, 1, 0, 0), 'Poitiers'),
        ])

    def test_move_root_to_prev_branch(self):
        self.create_all_test_places()

        little_france = Place.objects.create(name='Île-de-France')
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(0.5), 'Île-de-France'),
            (path(1), 'Österreich'),
        ])

        little_france.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            little_france.save()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, -1), 'Île-de-France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Île-de-France'),
            (path(0, 1), 'Normandie'),
            (path(0, 1, 0), 'Eure'),
            (path(0, 1, 1), 'Manche'),
            (path(0, 1, 2), 'Seine-Maritime'),
            (path(0, 2), 'Poitou-Charentes'),
            (path(0, 2, 0), 'Vienne'),
            (path(0, 2, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

    def test_move_root_to_next_branch(self):
        self.create_all_test_places()

        bretagne = Place.objects.create(name='Bretagne')
        self.assertPlaces([
            (path(-1), 'Bretagne'),
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

        bretagne.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            bretagne.save()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, -1), 'Bretagne'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Bretagne'),
            (path(0, 1), 'Normandie'),
            (path(0, 1, 0), 'Eure'),
            (path(0, 1, 1), 'Manche'),
            (path(0, 1, 2), 'Seine-Maritime'),
            (path(0, 2), 'Poitou-Charentes'),
            (path(0, 2, 0), 'Vienne'),
            (path(0, 2, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

    def test_move_root_to_prev_leaf(self):
        self.create_all_test_places()

        grattenoix = Place.objects.create(name='Grattenoix')
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(0.5), 'Grattenoix'),
            (path(1), 'Österreich'),
        ])

        grattenoix.parent = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            grattenoix.save()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 0, 0, 0), 'Grattenoix'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, 0), 'Eure'),
            (path(0, 0, 1), 'Manche'),
            (path(0, 0, 2), 'Seine-Maritime'),
            (path(0, 0, 2, 0), 'Grattenoix'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

    def test_move_root_to_next_leaf(self):
        self.create_all_test_places()

        evreux = Place.objects.create(name='Évreux')
        self.assertPlaces([
            (path(-1), 'Évreux'),
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

        evreux.parent = Place.objects.get(name='Eure')
        with self.assertNumQueries(1):
            evreux.save()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -1, 0), 'Évreux'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces([
            (path(0), 'France'),
            (path(0, 0), 'Normandie'),
            (path(0, 0, 0), 'Eure'),
            (path(0, 0, 0, 0), 'Évreux'),
            (path(0, 0, 1), 'Manche'),
            (path(0, 0, 2), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'),
            (path(1), 'Österreich'),
        ])

    # TODO: Add move_branch_to_prev_root.
    # TODO: Add move_branch_to_next_root.
    # TODO: Add move_branch_to_prev_branch.
    # TODO: Add move_branch_to_next_branch.
    # TODO: Add move_branch_to_prev_leaf.
    # TODO: Add move_branch_to_next_leaf.
    # TODO: Add move_leaf_to_prev_root.
    # TODO: Add move_leaf_to_next_root.
    # TODO: Add move_leaf_to_prev_branch.
    # TODO: Add move_leaf_to_next_branch.
    # TODO: Add move_leaf_to_prev_leaf.
    # TODO: Add move_leaf_to_next_leaf.

    def test_get_level(self):
        self.create_all_test_places()

        with self.assertNumQueries(1):
            data = [(p.get_level(), p.name) for p in Place.objects.all()]
            self.assertListEqual(data, [
                (1, 'France'),
                (2, 'Normandie'),
                (3, 'Eure'),
                (3, 'Manche'),
                (3, 'Seine-Maritime'),
                (2, 'Poitou-Charentes'),
                (3, 'Vienne'),
                (4, 'Poitiers'),
                (1, 'Österreich'),
            ])

    def test_is_root(self):
        self.create_all_test_places()

        places = [p.name for p in Place.objects.all() if p.is_root()]
        self.assertListEqual(places, ['France', 'Österreich'])

    def test_is_leaf(self):
        self.create_all_test_places()

        places = [p.name for p in Place.objects.all() if p.is_leaf()]
        self.assertListEqual(places, ['Eure', 'Manche', 'Seine-Maritime',
                                      'Poitiers', 'Österreich'])

    def test_get_children(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_children()
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_children()
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_children()
                     .values_list('name', flat=True)), [])

    def test_get_ancestors(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_ancestors(include_self=True)
                     .values_list('name', flat=True)), ['France'])

        with self.assertNumQueries(0):
            self.assertListEqual(
                list(france.get_ancestors()
                     .values_list('name', flat=True)), [])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_ancestors(include_self=True)
                     .values_list('name', flat=True)), ['France', 'Normandie'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_ancestors()
                     .values_list('name', flat=True)), ['France'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_ancestors(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Normandie', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_ancestors()
                     .values_list('name', flat=True)), ['France', 'Normandie'])

    def test_get_descendants(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_descendants(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Normandie', 'Eure', 'Manche', 'Seine-Maritime',
                 'Poitou-Charentes', 'Vienne', 'Poitiers'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_descendants()
                     .values_list('name', flat=True)),
                ['Normandie', 'Eure', 'Manche', 'Seine-Maritime',
                 'Poitou-Charentes', 'Vienne', 'Poitiers'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_descendants(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Eure', 'Manche', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_descendants()
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_descendants(include_self=True)
                     .values_list('name', flat=True)), ['Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_descendants()
                     .values_list('name', flat=True)), [])

    def test_get_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Österreich'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_siblings()
                     .values_list('name', flat=True)), ['Österreich'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_siblings()
                     .values_list('name', flat=True)), ['Poitou-Charentes'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_siblings()
                     .values_list('name', flat=True)), ['Eure', 'Manche'])

    def test_filtered_get_siblings(self):
        self.create_all_test_places()
        queryset = Place.objects.filter(name__lt='O')

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_siblings(include_self=True, queryset=queryset)
                     .values_list('name', flat=True)),
                ['France'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_siblings(queryset=queryset)
                     .values_list('name', flat=True)), [])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_siblings(include_self=True,
                                            queryset=queryset)
                     .values_list('name', flat=True)),
                ['Normandie'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_siblings(queryset=queryset)
                     .values_list('name', flat=True)), [])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_siblings(include_self=True,
                                                 queryset=queryset)
                     .values_list('name', flat=True)),
                ['Eure', 'Manche'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_siblings(queryset=queryset)
                     .values_list('name', flat=True)), ['Eure', 'Manche'])

    def test_get_prev_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)), ['France'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_prev_siblings()
                     .values_list('name', flat=True)), [])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)), ['Normandie'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_prev_siblings()
                     .values_list('name', flat=True)), [])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Seine-Maritime', 'Manche', 'Eure'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_prev_siblings()
                     .values_list('name', flat=True)), ['Manche', 'Eure'])

    def test_get_next_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Österreich'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_next_siblings()
                     .values_list('name', flat=True)), ['Österreich'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_next_siblings()
                     .values_list('name', flat=True)), ['Poitou-Charentes'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_next_siblings()
                     .values_list('name', flat=True)), [])

    def test_get_prev_sibling(self):
        self.create_all_test_places()

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertIsNone(france.get_prev_sibling())

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertIsNone(normandie.get_prev_sibling())

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertEqual(
                seine_maritime.get_prev_sibling().name, 'Manche')

    def test_get_next_sibling(self):
        self.create_all_test_places()

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.get_next_sibling().name, 'Österreich')

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertEqual(
                normandie.get_next_sibling().name, 'Poitou-Charentes')

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertIsNone(seine_maritime.get_next_sibling())

    def test_filtered_get_prev_sibling(self):
        self.create_all_test_places()
        queryset = Place.objects.filter(name__lt='O')

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertIsNone(france.get_prev_sibling(queryset=queryset))

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertIsNone(normandie.get_prev_sibling(queryset=queryset))

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertEqual(
                seine_maritime.get_prev_sibling(queryset=queryset).name,
                'Manche')

    def test_filtered_get_next_sibling(self):
        self.create_all_test_places()
        queryset = Place.objects.filter(name__lt='P')

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.get_next_sibling(queryset=queryset).name,
                             'Österreich')

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertIsNone(normandie.get_next_sibling(queryset=queryset))

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertIsNone(
                seine_maritime.get_next_sibling(queryset=queryset))

    def test_new_path(self):
        place = Place()

        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_children()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_ancestors()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_descendants()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_siblings()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_prev_siblings()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(place.get_next_siblings()), [])
        with self.assertNumQueries(0):
            self.assertIsNone(place.get_prev_sibling())
        with self.assertNumQueries(0):
            self.assertIsNone(place.get_next_sibling())
        with self.assertNumQueries(0):
            self.assertIsNone(place.get_level())
        with self.assertNumQueries(0):
            self.assertIsNone(place.is_root())
        with self.assertNumQueries(0):
            self.assertIsNone(place.is_leaf())

    def test_comparisons(self):
        self.create_all_test_places()

        france = Place.objects.get(name='France').path
        self.assertTrue(france == france)
        self.assertFalse(france != france)
        self.assertFalse(france > france)
        self.assertTrue(france >= france)
        self.assertFalse(france < france)
        self.assertTrue(france <= france)

        # vs None
        self.assertFalse(france == '')
        self.assertTrue(france != '')
        self.assertTrue(france < '')
        self.assertTrue(france <= '')
        self.assertFalse(france > '')
        self.assertFalse(france >= '')

        # vs None
        self.assertFalse(france == None)
        self.assertTrue(france != None)
        self.assertTrue(france < None)
        self.assertTrue(france <= None)
        self.assertFalse(france > None)
        self.assertFalse(france >= None)

        # vs new node
        new_node = Place().path
        self.assertFalse(france == new_node)
        self.assertTrue(france != new_node)
        self.assertTrue(france < new_node)
        self.assertTrue(france <= new_node)
        self.assertFalse(france > new_node)
        self.assertFalse(france >= new_node)

        # Same level
        osterreich = Place.objects.get(name='Österreich').path
        self.assertEqual(france.get_level(), osterreich.get_level())
        self.assertFalse(france == osterreich)
        self.assertTrue(france != osterreich)
        self.assertTrue(france < osterreich)
        self.assertTrue(france <= osterreich)
        self.assertFalse(france > osterreich)
        self.assertFalse(france >= osterreich)

        # Inferior level
        normandie = Place.objects.get(name='Normandie').path
        self.assertLess(france.get_level(), normandie.get_level())
        self.assertFalse(france == normandie)
        self.assertTrue(france != normandie)
        self.assertTrue(france < normandie)
        self.assertTrue(france <= normandie)
        self.assertFalse(france > normandie)
        self.assertFalse(france >= normandie)

        # Superior level
        self.assertGreater(normandie.get_level(), osterreich.get_level())
        self.assertFalse(normandie == osterreich)
        self.assertTrue(normandie != osterreich)
        self.assertTrue(normandie < osterreich)
        self.assertTrue(normandie <= osterreich)
        self.assertFalse(normandie > osterreich)
        self.assertFalse(normandie >= osterreich)

    def test_is_ancestor_of(self):
        self.create_all_test_places()

        for place in Place.objects.all():
            self.assertFalse(place.is_ancestor_of(place))
            self.assertTrue(place.is_ancestor_of(place,
                                                 include_self=True))
            for ancestor in place.get_ancestors():
                self.assertTrue(ancestor.is_ancestor_of(place))

    def test_is_descendant_of(self):
        self.create_all_test_places()

        for place in Place.objects.all():
            self.assertFalse(place.is_descendant_of(place))
            self.assertTrue(place.is_descendant_of(place,
                                                   include_self=True))
            for descendant in place.get_descendants():
                self.assertTrue(descendant.is_descendant_of(place))

    def test_get_roots(self):
        self.create_all_test_places()

        self.assertPlaces([
            (path(0), 'France'),
            (path(1), 'Österreich'),
        ], queryset=Place.get_roots())

    def test_rebuild(self):
        self.create_all_test_places()

        with Place.disabled_tree_trigger():
            updated_places = []
            for i, place in enumerate(Place.objects.order_by('name')):
                place.path = [i]
                updated_places.append(place)
            Place.objects.bulk_update(updated_places, ['path'])
        self.assertPlaces([
            (path(0), 'Eure'), (path(1), 'France'), (path(2), 'Manche'),
            (path(3), 'Normandie'), (path(4), 'Österreich'), (path(5), 'Poitiers'),
            (path(6), 'Poitou-Charentes'), (path(7), 'Seine-Maritime'),
            (path(8), 'Vienne')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Root
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='France').update(path=[89])
        self.assertPlaces([
            (path(0, 0), 'Normandie'), (path(0, 0, 0), 'Eure'),
            (path(0, 0, 1), 'Manche'), (path(0, 0, 2), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'), (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'), (path(1), 'Österreich'),
            (path(89), 'France')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Branch
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='Normandie').update(path=[89, 89])
        self.assertPlaces([
            (path(0), 'France'), (path(0, 0, 0), 'Eure'),
            (path(0, 0, 1), 'Manche'), (path(0, 0, 2), 'Seine-Maritime'),
            (path(0, 1), 'Poitou-Charentes'), (path(0, 1, 0), 'Vienne'),
            (path(0, 1, 0, 0), 'Poitiers'), (path(1), 'Österreich'),
            (path(89, 89), 'Normandie')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Leaf
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='Seine-Maritime').update(path=[0, 89])
        self.assertPlaces([
            (path(0), 'France'), (path(0, 0), 'Normandie'), (path(0, 0, 0), 'Eure'),
            (path(0, 0, 1), 'Manche'), (path(0, 1), 'Poitou-Charentes'),
            (path(0, 1, 0), 'Vienne'), (path(0, 1, 0, 0), 'Poitiers'),
            (path(0, 89), 'Seine-Maritime'), (path(1), 'Österreich')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_cycle(self):
        # Simple cycle
        a = Place.objects.create(name='a')
        a.parent = a
        with self.assertRaisesMessage(
                InternalError, 'Cannot set itself or a descendant as parent.'):
            with transaction.atomic():
                with self.assertNumQueries(1):
                    a.save()

        # Complex cycle
        b = Place.objects.create(name='b', parent=a)
        c = Place.objects.create(name='c', parent=b)
        d = Place.objects.create(name='d', parent=c)
        a.parent = d
        with self.assertRaisesMessage(
                InternalError, 'Cannot set itself or a descendant as parent.'):
            with transaction.atomic():
                with self.assertNumQueries(1):
                    a.save()

    def test_path_in_cursor(self):
        place1 = self.create_place('place1')
        with connection.cursor() as cursor:
            cursor.execute('SELECT %s;', [place1.path])
        place2 = self.create_place('place2', place1)
        with connection.cursor() as cursor:
            cursor.execute('SELECT %s;', [place2.path])


class MultipleOrderByFieldsTest(TransactionTestCase):
    def setUp(self):
        self.correct_raw_persons_data = [
            (path(-2), 'Leopold', 'Mozart'),
            (path(-2, -1), 'Maria Anna', 'Mozart'),
            (path(-2, 0), 'Wolfgang Amadeus', 'Mozart'),
            (path(-1.75), '', 'Strauss'),
            (path(-1.5), 'Johann (father)', 'Strauss'),
            (path(-1.5, 0), 'Johann (son)', 'Strauss'),
            (path(-1), 'Piotr Ilyich', 'Tchaikovski'),
            (path(0), 'Antonio Lucio', 'Vivaldi'),
        ]
        self.correct_persons_data = [
            (path(0), 'Leopold', 'Mozart'),
            (path(0, 0), 'Maria Anna', 'Mozart'),
            (path(0, 1), 'Wolfgang Amadeus', 'Mozart'),
            (path(1), '', 'Strauss'),
            (path(2), 'Johann (father)', 'Strauss'),
            (path(2, 0), 'Johann (son)', 'Strauss'),
            (path(3), 'Piotr Ilyich', 'Tchaikovski'),
            (path(4), 'Antonio Lucio', 'Vivaldi'),
        ]
        self.vivaldi = Person.objects.create(
            first_name='Antonio Lucio', last_name='Vivaldi',
        )
        self.wolfgang_mozart = Person.objects.create(
            first_name='Wolfgang Amadeus', last_name='Mozart',
        )
        self.leopold_mozart = Person.objects.create(
            first_name='Leopold', last_name='Mozart',
        )
        self.wolfgang_mozart.parent = self.leopold_mozart
        self.wolfgang_mozart.save()
        self.maria_anna_mozart = Person.objects.create(
            parent=self.leopold_mozart,
            first_name='Maria Anna', last_name='Mozart',
        )
        self.tchaikovski = Person.objects.create(
            first_name='Piotr Ilyich', last_name='Tchaikovski',
        )
        self.strauss_father = Person.objects.create(
            first_name='Johann (father)', last_name='Strauss',
        )
        self.strauss_son = Person.objects.create(
            parent=self.strauss_father,
            first_name='Johann (son)', last_name='Strauss',
        )
        self.strauss = Person.objects.create(
            last_name='Strauss',
        )

    def assertPersons(self, values, queryset=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            if queryset is None:
                queryset = Person.objects.all()
            persons = list(queryset)
            self.assertListEqual(
                [(p.path.value, p.first_name, p.last_name) for p in persons],
                values,
            )

    def test_rebuild(self):
        self.assertPersons(self.correct_raw_persons_data)
        with Person.disabled_tree_trigger():
            for i, person in enumerate(
                Person.objects.order_by('-last_name', '-first_name')
            ):
                person.path = [i]
                person.save()
        self.assertPersons([
            (path(0), 'Antonio Lucio', 'Vivaldi'),
            (path(1), 'Piotr Ilyich', 'Tchaikovski'),
            (path(2), 'Johann (son)', 'Strauss'),
            (path(3), 'Johann (father)', 'Strauss'),
            (path(4), '', 'Strauss'),
            (path(5), 'Wolfgang Amadeus', 'Mozart'),
            (path(6), 'Maria Anna', 'Mozart'),
            (path(7), 'Leopold', 'Mozart'),
        ])
        Person.rebuild_paths()
        self.assertPersons(self.correct_persons_data)


class QuerySetTest(CommonTest):
    def test_get_descendants(self):
        self.create_all_test_places()

        places = Place.objects.filter(name__in=('Normandie', 'Österreich'))
        self.assertPlaces([
            (path(0, 0), 'Normandie'),
            (path(1), 'Österreich'),
        ], places)

        self.assertPlaces([
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
        ], places.get_descendants())
        self.assertPlaces([
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(1), 'Österreich'),
        ], places.get_descendants(include_self=True))

        osterreich = Place.objects.get(name='Österreich')
        self.create_place('Vienne (AU)', osterreich)

        self.assertPlaces([
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(1, 0), 'Vienne (AU)'),
        ], places.get_descendants())
        self.assertPlaces([
            (path(0, 0), 'Normandie'),
            (path(0, 0, -1), 'Eure'),
            (path(0, 0, -0.5), 'Manche'),
            (path(0, 0, 0), 'Seine-Maritime'),
            (path(1), 'Österreich'),
            (path(1, 0), 'Vienne (AU)'),
        ], places.get_descendants(include_self=True))
