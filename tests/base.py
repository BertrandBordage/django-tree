# coding: utf-8

from __future__ import unicode_literals

import doctest
import uuid
from unittest import mock

from django.apps import apps
from django.core.exceptions import (
    FieldDoesNotExist,
    ImproperlyConfigured,
    ValidationError,
)
from django.db import transaction, connection
from django.db.migrations.recorder import MigrationRecorder
from django.db.migrations.state import ProjectState
from django.db.models import ProtectedError, QuerySet
from django.db.utils import IntegrityError, ProgrammingError
from django.test import SimpleTestCase, TransactionTestCase

from tree.fields import PathField
from tree.forms import TreeChoiceField
from tree.operations import CreateTreeTrigger, DeleteTreeTrigger, RebuildPaths
from tree.query import _get_path_field
from tree.sql import base as sql_base
from tree.types import Path

from .models import (
    Place,
    Person,
    DescendingPlace,
    MultiPathPlace,
    UUIDPlace,
    SetNullPlace,
    ProtectPlace,
    WeirdTableNamePlace,
)


# The following behaviours are now covered:
#   - same `order_by` values .............. MultipleOrderByFieldsTest
#   - descending `order_by` ............... DescendingOrderByTest
#   - moving a node after itself .......... PathTest.test_resave_node_in_place_*
#   - ORM update/delete ................... PathTest.test_orm_*
#   - raw SQL insert/update/delete ........ PathTest.test_raw_sql_*
#   - rebuild with NULL paths ............. PathTest.test_rebuild_with_*null_paths
#   - Path objects as SQL parameters ...... PathTest.test_path_as_sql_parameter
#   - multiple path fields ................ MultiplePathFieldsTest
#   - disable/enable/disabled trigger ..... PathTest.test_disable*_trigger*
#   - breaking a transaction .............. PathTest.test_transaction_rollback_*
#   - non-integer primary keys ............ NonIntegerPrimaryKeyTest
#   - `on_delete` other than CASCADE ...... OnDeleteBehaviourTest
#   - unusual table names ................. UnusualTableNameTest
#
# TODO: Test if `disabled_trigger` does not affect
#       a concurrent node creation/update.
#       (Needs multiple connections/threads and is timing-sensitive; not
#       implemented yet.)


def path(*path_components):
    # A reference path used only as a *structural* descriptor: its length encodes
    # the node's depth and its prefix relationships encode ancestry. The stored
    # `bytea` keys are an implementation detail of the trigger, so tests assert the
    # tree's shape (order, depth, ancestry) rather than literal path values.
    return [float(value) for value in path_components]


def assert_structure(test, instances, values, label=lambda p: p.name):
    """Assert that ``instances`` (in path order) match the reference ``values``.

    ``values`` is a list of ``(reference_path, label)`` tuples. Three things are
    checked: the labels appear in the given order, each node's depth matches the
    reference path length, and ancestry (byte-prefix of the real paths) matches
    the prefix relationships of the reference paths.
    """
    refs = [v[0] for v in values]
    test.assertListEqual([label(p) for p in instances], [v[-1] for v in values])
    test.assertListEqual(
        [p.path.get_level() for p in instances], [len(r) for r in refs]
    )
    for i, ref_i in enumerate(refs):
        for j, ref_j in enumerate(refs):
            expected = list(ref_j[: len(ref_i)]) == list(ref_i)
            actual = instances[j].path.value.startswith(instances[i].path.value)
            test.assertEqual(
                expected,
                actual,
                'ancestry mismatch between %r and %r' % (values[i][-1], values[j][-1]),
            )


class CommonTest(TransactionTestCase):
    maxDiff = 1000

    def create_place(self, name, parent=None):
        with self.assertNumQueries(1):
            p = Place.objects.create(name=name, parent=parent)
        with self.assertNumQueries(1):
            p.clean()
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
        vienne.clean()
        vienne.save()
        yield vienne

    def create_all_test_places(self):
        list(self.create_test_places())

    def assertPlaces(self, values, queryset=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            if queryset is None:
                queryset = Place.objects.all()
            places = list(queryset)
        assert_structure(self, places, values)


class PathTest(CommonTest):
    maxDiff = None

    def test_path_on_creation(self):
        with self.assertNumQueries(1):
            place1 = Place.objects.create(name='place1')
        # 1 query because the path got deferred,  forcing Django to run
        # a new query to get the updated value when we need it. Same below.
        with self.assertNumQueries(1):
            self.assertTrue(place1.path.is_root())
        with self.assertNumQueries(1):
            place2 = Place.objects.create(name='place2', parent=place1)
        with self.assertNumQueries(1):
            self.assertEqual(place2.path.get_level(), 2)
            self.assertTrue(place2.path.is_descendant_of(place1.path))
        with self.assertNumQueries(1):
            place2.parent = None
            place2.clean()
            place2.save()
        with self.assertNumQueries(1):
            self.assertTrue(place2.path.is_root())

    def test_insert(self):
        it = self.create_test_places()
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Seine-Maritime'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, 0), 'Seine-Maritime'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
                (path(1, 0), 'Vienne'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(1), 'Österreich'),
                (path(1, 0), 'Vienne'),
            ]
        )
        next(it)
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(1), 'Österreich'),
                (path(1, 0), 'Vienne'),
                (path(1, 0, 0), 'Poitiers'),
            ]
        )
        next(it)
        self.assertPlaces(self.correct_raw_places_data)
        Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_delete(self):
        self.create_all_test_places()

        self.assertPlaces(self.correct_raw_places_data)

        # Leaf
        manche = Place.objects.get(name='Manche')
        with self.assertNumQueries(5):
            manche.delete()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(5):
            normandie.delete()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(5):
            france.delete()
        self.assertPlaces(
            [
                (path(1), 'Österreich'),
            ]
        )

    def test_move_root_to_prev_root(self):
        self.create_all_test_places()

        osterreich = Place.objects.get(name='Österreich')
        osterreich.name = 'Autriche'
        with self.assertNumQueries(1):
            osterreich.clean()
            osterreich.save()
        self.assertPlaces(
            [
                (path(-1), 'Autriche'),
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'Autriche'),
                (path(1), 'France'),
                (path(1, 0), 'Normandie'),
                (path(1, 0, 0), 'Eure'),
                (path(1, 0, 1), 'Manche'),
                (path(1, 0, 2), 'Seine-Maritime'),
                (path(1, 1), 'Poitou-Charentes'),
                (path(1, 1, 0), 'Vienne'),
                (path(1, 1, 0, 0), 'Poitiers'),
            ]
        )

    def test_move_root_to_next_root(self):
        self.create_all_test_places()

        france = Place.objects.get(name='France')
        france.name = 'République française'
        with self.assertNumQueries(1):
            france.clean()
            france.save()
        self.assertPlaces(
            [
                (path(1), 'Österreich'),
                (path(2), 'République française'),
                (path(2, 0), 'Normandie'),
                (path(2, 0, -1), 'Eure'),
                (path(2, 0, -0.5), 'Manche'),
                (path(2, 0, 0), 'Seine-Maritime'),
                (path(2, 1), 'Poitou-Charentes'),
                (path(2, 1, 0), 'Vienne'),
                (path(2, 1, 0, 0), 'Poitiers'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'Österreich'),
                (path(1), 'République française'),
                (path(1, 0), 'Normandie'),
                (path(1, 0, 0), 'Eure'),
                (path(1, 0, 1), 'Manche'),
                (path(1, 0, 2), 'Seine-Maritime'),
                (path(1, 1), 'Poitou-Charentes'),
                (path(1, 1, 0), 'Vienne'),
                (path(1, 1, 0, 0), 'Poitiers'),
            ]
        )

    def test_move_root_to_prev_branch(self):
        self.create_all_test_places()

        little_france = Place.objects.create(name='Île-de-France')
        self.assertPlaces(
            [
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
            ]
        )

        little_france.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            little_france.clean()
        with self.assertNumQueries(1):
            little_france.save()
        self.assertPlaces(
            [
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
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
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
            ]
        )

    def test_move_root_to_next_branch(self):
        self.create_all_test_places()

        bretagne = Place.objects.create(name='Bretagne')
        self.assertPlaces(
            [
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
            ]
        )

        bretagne.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            bretagne.clean()
        with self.assertNumQueries(1):
            bretagne.save()
        self.assertPlaces(
            [
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
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
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
            ]
        )

    def test_move_root_to_prev_leaf(self):
        self.create_all_test_places()

        grattenoix = Place.objects.create(name='Grattenoix')
        self.assertPlaces(
            [
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
            ]
        )

        grattenoix.parent = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            grattenoix.clean()
        with self.assertNumQueries(1):
            grattenoix.save()
        self.assertPlaces(
            [
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
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
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
            ]
        )

    def test_move_root_to_next_leaf(self):
        self.create_all_test_places()

        evreux = Place.objects.create(name='Évreux')
        self.assertPlaces(
            [
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
            ]
        )

        evreux.parent = Place.objects.get(name='Eure')
        with self.assertNumQueries(1):
            evreux.clean()
        with self.assertNumQueries(1):
            evreux.save()
        self.assertPlaces(
            [
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
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
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
            ]
        )

    def test_move_branch_to_prev_root(self):
        self.create_all_test_places()

        # Poitou-Charentes is a branch (it carries the Vienne > Poitiers
        # subtree). Renaming it to sort before every root and detaching it
        # turns it into the new first root, dragging its subtree along.
        branch = Place.objects.get(name='Poitou-Charentes')
        branch.name = 'Aquitaine'
        branch.parent = None
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(-1), 'Aquitaine'),
                (path(-1, 0), 'Vienne'),
                (path(-1, 0, 0), 'Poitiers'),
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'Aquitaine'),
                (path(0, 0), 'Vienne'),
                (path(0, 0, 0), 'Poitiers'),
                (path(1), 'France'),
                (path(1, 0), 'Normandie'),
                (path(1, 0, 0), 'Eure'),
                (path(1, 0, 1), 'Manche'),
                (path(1, 0, 2), 'Seine-Maritime'),
                (path(2), 'Österreich'),
            ]
        )

    def test_move_branch_to_next_root(self):
        self.create_all_test_places()

        branch = Place.objects.get(name='Poitou-Charentes')
        branch.name = 'Zélande'
        branch.parent = None
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
                (path(2), 'Zélande'),
                (path(2, 0), 'Vienne'),
                (path(2, 0, 0), 'Poitiers'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(1), 'Österreich'),
                (path(2), 'Zélande'),
                (path(2, 0), 'Vienne'),
                (path(2, 0, 0), 'Poitiers'),
            ]
        )

    def test_move_branch_to_prev_branch(self):
        self.create_all_test_places()

        # Move the Poitou-Charentes branch so it lands right before the
        # Normandie branch among France's children.
        branch = Place.objects.get(name='Poitou-Charentes')
        branch.name = 'Bretagne'
        branch.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, -1), 'Bretagne'),
                (path(0, -1, 0), 'Vienne'),
                (path(0, -1, 0, 0), 'Poitiers'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Bretagne'),
                (path(0, 0, 0), 'Vienne'),
                (path(0, 0, 0, 0), 'Poitiers'),
                (path(0, 1), 'Normandie'),
                (path(0, 1, 0), 'Eure'),
                (path(0, 1, 1), 'Manche'),
                (path(0, 1, 2), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_branch_to_next_branch(self):
        self.create_all_test_places()

        # Move the Normandie branch so it lands right after the
        # Poitou-Charentes branch among France's children.
        branch = Place.objects.get(name='Normandie')
        branch.name = 'Quercy'
        branch.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(0, 2), 'Quercy'),
                (path(0, 2, -1), 'Eure'),
                (path(0, 2, -0.5), 'Manche'),
                (path(0, 2, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Poitou-Charentes'),
                (path(0, 0, 0), 'Vienne'),
                (path(0, 0, 0, 0), 'Poitiers'),
                (path(0, 1), 'Quercy'),
                (path(0, 1, 0), 'Eure'),
                (path(0, 1, 1), 'Manche'),
                (path(0, 1, 2), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_branch_to_prev_leaf(self):
        self.create_all_test_places()

        # Move the Poitou-Charentes branch under Normandie, before the
        # Eure leaf.
        branch = Place.objects.get(name='Poitou-Charentes')
        branch.name = 'Aaa'
        branch.parent = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -2), 'Aaa'),
                (path(0, 0, -2, 0), 'Vienne'),
                (path(0, 0, -2, 0, 0), 'Poitiers'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Aaa'),
                (path(0, 0, 0, 0), 'Vienne'),
                (path(0, 0, 0, 0, 0), 'Poitiers'),
                (path(0, 0, 1), 'Eure'),
                (path(0, 0, 2), 'Manche'),
                (path(0, 0, 3), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_branch_to_next_leaf(self):
        self.create_all_test_places()

        # Move the Poitou-Charentes branch under Normandie, after the
        # Seine-Maritime leaf.
        branch = Place.objects.get(name='Poitou-Charentes')
        branch.name = 'Zzz'
        branch.parent = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            branch.clean()
            branch.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 0, 1), 'Zzz'),
                (path(0, 0, 1, 0), 'Vienne'),
                (path(0, 0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(0, 0, 3), 'Zzz'),
                (path(0, 0, 3, 0), 'Vienne'),
                (path(0, 0, 3, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_leaf_to_prev_root(self):
        self.create_all_test_places()

        # Seine-Maritime is a leaf. Renaming it to sort first and detaching
        # it turns it into the new first root.
        leaf = Place.objects.get(name='Seine-Maritime')
        leaf.name = 'Aaa'
        leaf.parent = None
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(-1), 'Aaa'),
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'Aaa'),
                (path(1), 'France'),
                (path(1, 0), 'Normandie'),
                (path(1, 0, 0), 'Eure'),
                (path(1, 0, 1), 'Manche'),
                (path(1, 1), 'Poitou-Charentes'),
                (path(1, 1, 0), 'Vienne'),
                (path(1, 1, 0, 0), 'Poitiers'),
                (path(2), 'Österreich'),
            ]
        )

    def test_move_leaf_to_next_root(self):
        self.create_all_test_places()

        leaf = Place.objects.get(name='Seine-Maritime')
        leaf.name = 'Zzz'
        leaf.parent = None
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
                (path(2), 'Zzz'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
                (path(2), 'Zzz'),
            ]
        )

    def test_move_leaf_to_prev_branch(self):
        self.create_all_test_places()

        # Move the Poitiers leaf so it lands before the Normandie branch
        # among France's children.
        leaf = Place.objects.get(name='Poitiers')
        leaf.name = 'Aaa'
        leaf.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, -1), 'Aaa'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Aaa'),
                (path(0, 1), 'Normandie'),
                (path(0, 1, 0), 'Eure'),
                (path(0, 1, 1), 'Manche'),
                (path(0, 1, 2), 'Seine-Maritime'),
                (path(0, 2), 'Poitou-Charentes'),
                (path(0, 2, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_leaf_to_next_branch(self):
        self.create_all_test_places()

        # Move the Poitiers leaf so it lands after the Poitou-Charentes
        # branch among France's children.
        leaf = Place.objects.get(name='Poitiers')
        leaf.name = 'Quercy'
        leaf.parent = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 2), 'Quercy'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 2), 'Quercy'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_leaf_to_prev_leaf(self):
        self.create_all_test_places()

        # Move the Poitiers leaf under Normandie, before the Eure leaf.
        leaf = Place.objects.get(name='Poitiers')
        leaf.name = 'Aaa'
        leaf.parent = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -2), 'Aaa'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Aaa'),
                (path(0, 0, 1), 'Eure'),
                (path(0, 0, 2), 'Manche'),
                (path(0, 0, 3), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )

    def test_move_leaf_to_next_leaf(self):
        self.create_all_test_places()

        # Move the Poitiers leaf under Normandie, after the
        # Seine-Maritime leaf.
        leaf = Place.objects.get(name='Poitiers')
        leaf.name = 'Zzz'
        leaf.parent = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            leaf.clean()
            leaf.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 0, 1), 'Zzz'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(0, 0, 3), 'Zzz'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(1), 'Österreich'),
            ]
        )

    def test_resave_node_in_place_keeps_paths(self):
        # Re-saving a node without moving it must not shift it or leave a
        # hole at its former position.
        self.create_all_test_places()
        manche = Place.objects.get(name='Manche')
        with self.assertNumQueries(1):
            manche.save()
        self.assertPlaces(self.correct_raw_places_data)

        # Same when the parent is explicitly (re)set to the current one.
        manche.parent = Place.objects.get(name='Normandie')
        manche.clean()
        manche.save()
        self.assertPlaces(self.correct_raw_places_data)

        # Changing a watched `order_by` value while the node stays in the same
        # slot (Manche remains alphabetically between Eure and Seine-Maritime)
        # must keep its current path rather than re-deriving a new one, so no
        # hole opens at its former position.
        manche.name = 'Manche-bis'
        manche.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche-bis'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_orm_update_on_order_by_field(self):
        # `name` is part of the `PathField.order_by`, so the trigger watches
        # it: a bulk `update(name=...)` repositions the row immediately.
        self.create_all_test_places()
        Place.objects.filter(name='Eure').update(name='Zzz-Eure')
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 0, 1), 'Zzz-Eure'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_orm_update_parent_keeps_tree_consistent(self):
        # The README promises the path is kept up to date automatically.
        # The trigger watches the parent FK column, so a bulk
        # `update(parent=...)` recomputes the path right away.
        self.create_all_test_places()
        normandie = Place.objects.get(name='Normandie')
        Place.objects.filter(name='Poitiers').update(parent=normandie)
        children = list(normandie.get_children().values_list('name', flat=True))
        self.assertIn(
            'Poitiers',
            children,
            'Poitiers should be a child of Normandie after the bulk '
            're-parent, but its path was not recomputed: %r'
            % (Place.objects.get(name='Poitiers').path.value,),
        )

    def test_orm_delete_via_queryset(self):
        self.create_all_test_places()
        # `QuerySet.delete()` removes the matched rows; the `parent` FK
        # cascades onto the descendants.
        deleted, _ = Place.objects.filter(name='Normandie').delete()
        self.assertEqual(deleted, 4)  # Normandie + Eure + Manche + Seine-Maritime
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_raw_sql_insert(self):
        # A raw `INSERT` fires the trigger, which computes the new path.
        self.create_all_test_places()
        normandie = Place.objects.get(name='Normandie')
        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO %s (name, parent_id) VALUES (%%s, %%s);'
                % Place._meta.db_table,
                ['Calvados', normandie.pk],
            )
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -2), 'Calvados'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_raw_sql_update_parent_keeps_tree_consistent(self):
        # Same as the ORM bulk update: a raw `UPDATE` of the FK column alone
        # fires the path trigger, which recomputes the path.
        self.create_all_test_places()
        normandie = Place.objects.get(name='Normandie')
        with connection.cursor() as cursor:
            cursor.execute(
                'UPDATE %s SET parent_id = %%s WHERE name = %%s;'
                % Place._meta.db_table,
                [normandie.pk, 'Poitiers'],
            )
        children = list(normandie.get_children().values_list('name', flat=True))
        self.assertIn(
            'Poitiers',
            children,
            'Poitiers should be a child of Normandie after the raw '
            're-parent, but its path was not recomputed: %r'
            % (Place.objects.get(name='Poitiers').path.value,),
        )

    def test_raw_sql_delete(self):
        self.create_all_test_places()
        with connection.cursor() as cursor:
            cursor.execute(
                'DELETE FROM %s WHERE name = %%s;' % Place._meta.db_table,
                ['Manche'],
            )
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_rebuild_with_null_paths(self):
        self.create_all_test_places()
        # Wipe every path, then rebuild from scratch.
        with Place.disabled_tree_trigger():
            Place.objects.update(path=None)
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_rebuild_with_some_null_paths(self):
        self.create_all_test_places()
        with Place.disabled_tree_trigger():
            Place.objects.filter(name__in=['Manche', 'Vienne']).update(path=None)
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_path_as_sql_parameter(self):
        # A `Path` can be passed as a query parameter and round-trips back to
        # the row it identifies (extends `test_path_in_cursor`).
        self.create_all_test_places()
        france = Place.objects.get(name='France')
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT name FROM %s WHERE path = %%s;' % Place._meta.db_table,
                [france.path],
            )
            self.assertEqual(cursor.fetchall(), [('France',)])

    def test_disable_and_enable_trigger(self):
        # While disabled, the trigger does not compute the path on insert.
        Place.disable_tree_trigger()
        try:
            disabled = Place.objects.create(name='disabled')
        finally:
            Place.enable_tree_trigger()
        self.assertIsNone(Place.objects.get(pk=disabled.pk).path.value)

        # Once re-enabled, paths are computed again.
        enabled = Place.objects.create(name='enabled')
        self.assertIsNotNone(Place.objects.get(pk=enabled.pk).path.value)

    def test_disabled_trigger_context_manager(self):
        self.create_all_test_places()
        # Inside the context manager, even a change to a watched column is
        # ignored by the trigger, so the path goes stale; rebuild restores it.
        with Place.disabled_tree_trigger():
            seine = Place.objects.get(name='Seine-Maritime')
            seine.name = 'Aaa-Seine'
            seine.save()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Aaa-Seine'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )
        Place.rebuild_paths()
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Aaa-Seine'),
                (path(0, 0, 1), 'Eure'),
                (path(0, 0, 2), 'Manche'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
            ]
        )

    def test_transaction_rollback_reverts_trigger_changes(self):
        self.create_all_test_places()
        before = [(p.path.value, p.name) for p in Place.objects.all()]

        class Rollback(Exception):
            pass

        with self.assertRaises(Rollback):
            with transaction.atomic():
                vienne = Place.objects.get(name='Vienne')
                vienne.parent = Place.objects.get(name='Normandie')
                vienne.clean()
                vienne.save()
                # The trigger moved Vienne inside the transaction...
                self.assertTrue(
                    Place.objects.get(name='Vienne').is_descendant_of(
                        Place.objects.get(name='Normandie')
                    )
                )
                raise Rollback()

        # ...but rolling back reverts everything the trigger did.
        after = [(p.path.value, p.name) for p in Place.objects.all()]
        self.assertListEqual(after, before)

    def test_get_level(self):
        self.create_all_test_places()

        with self.assertNumQueries(1):
            data = [(p.get_level(), p.name) for p in Place.objects.all()]
            self.assertListEqual(
                data,
                [
                    (1, 'France'),
                    (2, 'Normandie'),
                    (3, 'Eure'),
                    (3, 'Manche'),
                    (3, 'Seine-Maritime'),
                    (2, 'Poitou-Charentes'),
                    (3, 'Vienne'),
                    (4, 'Poitiers'),
                    (1, 'Österreich'),
                ],
            )

    def test_is_root(self):
        self.create_all_test_places()

        places = [p.name for p in Place.objects.all() if p.is_root()]
        self.assertListEqual(places, ['France', 'Österreich'])

    def test_is_leaf(self):
        self.create_all_test_places()

        places = [p.name for p in Place.objects.all() if p.is_leaf()]
        self.assertListEqual(
            places, ['Eure', 'Manche', 'Seine-Maritime', 'Poitiers', 'Österreich']
        )

    def test_get_children(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_children().values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'],
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_children().values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_children().values_list('name', flat=True)), []
            )

    def test_get_ancestors(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_ancestors(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France'],
            )

        with self.assertNumQueries(0):
            self.assertListEqual(
                list(france.get_ancestors().values_list('name', flat=True)), []
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_ancestors(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France', 'Normandie'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_ancestors().values_list('name', flat=True)),
                ['France'],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_ancestors(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France', 'Normandie', 'Seine-Maritime'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_ancestors().values_list('name', flat=True)),
                ['France', 'Normandie'],
            )

    def test_get_descendants(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_descendants(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                [
                    'France',
                    'Normandie',
                    'Eure',
                    'Manche',
                    'Seine-Maritime',
                    'Poitou-Charentes',
                    'Vienne',
                    'Poitiers',
                ],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_descendants().values_list('name', flat=True)),
                [
                    'Normandie',
                    'Eure',
                    'Manche',
                    'Seine-Maritime',
                    'Poitou-Charentes',
                    'Vienne',
                    'Poitiers',
                ],
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_descendants(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Normandie', 'Eure', 'Manche', 'Seine-Maritime'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_descendants().values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_descendants(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Seine-Maritime'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_descendants().values_list('name', flat=True)),
                [],
            )

    def test_get_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France', 'Österreich'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_siblings().values_list('name', flat=True)),
                ['Österreich'],
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Normandie', 'Poitou-Charentes'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_siblings().values_list('name', flat=True)),
                ['Poitou-Charentes'],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Eure', 'Manche', 'Seine-Maritime'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_siblings().values_list('name', flat=True)),
                ['Eure', 'Manche'],
            )

    def test_filtered_get_siblings(self):
        self.create_all_test_places()
        queryset = Place.objects.filter(name__lt='O')

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_siblings(
                        include_self=True, queryset=queryset
                    ).values_list('name', flat=True)
                ),
                ['France'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_siblings(queryset=queryset).values_list(
                        'name', flat=True
                    )
                ),
                [],
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_siblings(
                        include_self=True, queryset=queryset
                    ).values_list('name', flat=True)
                ),
                ['Normandie'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_siblings(queryset=queryset).values_list(
                        'name', flat=True
                    )
                ),
                [],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_siblings(
                        include_self=True, queryset=queryset
                    ).values_list('name', flat=True)
                ),
                ['Eure', 'Manche'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_siblings(queryset=queryset).values_list(
                        'name', flat=True
                    )
                ),
                ['Eure', 'Manche'],
            )

    def test_get_prev_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_prev_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_prev_siblings().values_list('name', flat=True)), []
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_prev_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Normandie'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_prev_siblings().values_list('name', flat=True)), []
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_prev_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Seine-Maritime', 'Manche', 'Eure'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_prev_siblings().values_list('name', flat=True)),
                ['Manche', 'Eure'],
            )

    def test_get_next_siblings(self):
        self.create_all_test_places()

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    france.get_next_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['France', 'Österreich'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.get_next_siblings().values_list('name', flat=True)),
                ['Österreich'],
            )

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    normandie.get_next_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Normandie', 'Poitou-Charentes'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.get_next_siblings().values_list('name', flat=True)),
                ['Poitou-Charentes'],
            )

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(
                    seine_maritime.get_next_siblings(include_self=True).values_list(
                        'name', flat=True
                    )
                ),
                ['Seine-Maritime'],
            )

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.get_next_siblings().values_list('name', flat=True)),
                [],
            )

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
            self.assertEqual(seine_maritime.get_prev_sibling().name, 'Manche')

    def test_get_next_sibling(self):
        self.create_all_test_places()

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.get_next_sibling().name, 'Österreich')

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertEqual(normandie.get_next_sibling().name, 'Poitou-Charentes')

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
                seine_maritime.get_prev_sibling(queryset=queryset).name, 'Manche'
            )

    def test_filtered_get_next_sibling(self):
        self.create_all_test_places()
        queryset = Place.objects.filter(name__lt='P')

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(
                france.get_next_sibling(queryset=queryset).name, 'Österreich'
            )

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertIsNone(normandie.get_next_sibling(queryset=queryset))

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertIsNone(seine_maritime.get_next_sibling(queryset=queryset))

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
        self.assertFalse(france == None)  # noqa: E711
        self.assertTrue(france != None)  # noqa: E711
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
            self.assertTrue(place.is_ancestor_of(place, include_self=True))
            for ancestor in place.get_ancestors():
                self.assertTrue(ancestor.is_ancestor_of(place))

    def test_is_descendant_of(self):
        self.create_all_test_places()

        for place in Place.objects.all():
            self.assertFalse(place.is_descendant_of(place))
            self.assertTrue(place.is_descendant_of(place, include_self=True))
            for descendant in place.get_descendants():
                self.assertTrue(descendant.is_descendant_of(place))

    def test_filter_roots(self):
        self.create_all_test_places()

        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(1), 'Österreich'),
            ],
            queryset=Place.objects.filter_roots(),
        )

    def test_rebuild(self):
        self.create_all_test_places()

        with Place.disabled_tree_trigger():
            updated_places = []
            for i, place in enumerate(Place.objects.order_by('name')):
                # Assign arbitrary, distinct single-segment paths (byte values
                # increase with `i`, so path order follows name order).
                place.path = bytes([i + 2]) + b'\x00'
                updated_places.append(place)
            Place.objects.bulk_update(updated_places, ['path'])
        self.assertPlaces(
            [
                (path(0), 'Eure'),
                (path(1), 'France'),
                (path(2), 'Manche'),
                (path(3), 'Normandie'),
                (path(4), 'Österreich'),
                (path(5), 'Poitiers'),
                (path(6), 'Poitou-Charentes'),
                (path(7), 'Seine-Maritime'),
                (path(8), 'Vienne'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Root
        with Place.disabled_tree_trigger():
            # A level-1 path that sorts after every other root.
            Place.objects.filter(name='France').update(path=b'\xff\x00')
        self.assertPlaces(
            [
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
                (path(89), 'France'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Branch
        with Place.disabled_tree_trigger():
            # A level-2 path detached from France's subtree, sorting last.
            Place.objects.filter(name='Normandie').update(path=b'\xff\x00\xff\x00')
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 0, 2), 'Seine-Maritime'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(1), 'Österreich'),
                (path(89, 89), 'Normandie'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Leaf
        with Place.disabled_tree_trigger():
            # A level-2 path under France that sorts after France's children.
            france = Place.objects.get(name='France')
            Place.objects.filter(name='Seine-Maritime').update(
                path=france.path.value + b'\xff\x00'
            )
        self.assertPlaces(
            [
                (path(0), 'France'),
                (path(0, 0), 'Normandie'),
                (path(0, 0, 0), 'Eure'),
                (path(0, 0, 1), 'Manche'),
                (path(0, 1), 'Poitou-Charentes'),
                (path(0, 1, 0), 'Vienne'),
                (path(0, 1, 0, 0), 'Poitiers'),
                (path(0, 89), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ]
        )
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

    def test_cycle(self):
        # Simple cycle
        a = Place.objects.create(name='a')
        a.parent = a

        with self.assertRaisesMessage(
            ValidationError,
            "{'parent': [\"Value 'a' is not a valid choice.\"]}",
        ):
            with transaction.atomic():
                with self.assertNumQueries(1):
                    a.clean()

        with self.assertRaisesMessage(
            ProgrammingError,
            'Cannot set itself or a descendant as parent.',
        ):
            with transaction.atomic():
                with self.assertNumQueries(1):
                    a.save()

        # Complex cycle
        b = Place.objects.create(name='b', parent=a)
        c = Place.objects.create(name='c', parent=b)
        d = Place.objects.create(name='d', parent=c)
        a.parent = d

        with self.assertRaisesMessage(
            ValidationError,
            "{'parent': [\"Value 'd' is not a valid choice.\"]}",
        ):
            with transaction.atomic():
                with self.assertNumQueries(1):
                    a.clean()

        with self.assertRaisesMessage(
            ProgrammingError, 'Cannot set itself or a descendant as parent.'
        ):
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
    maxDiff = None

    def setUp(self):
        self.correct_raw_persons_data = [
            (path(-3), 18, 'Leopold', 'Mozart'),
            (path(-3, -1), 18, 'Maria Anna', 'Mozart'),
            (path(-3, 0), 18, 'Wolfgang Amadeus', 'Mozart'),
            (path(-1), 18, 'Antonio Lucio', 'Vivaldi'),
            (path(-0.75), 19, 'Johann (father)', 'Strauss'),
            (path(-0.75, 0), 19, 'Johann (son)', 'Strauss'),
            (path(-0.5), 19, 'Piotr Ilyich', 'Tchaikovski'),
            (path(-0.25), 20, '', 'Strauss'),
            (path(0), None, '', 'Anonymous'),
        ]
        self.correct_persons_data = [
            (path(0), 18, 'Leopold', 'Mozart'),
            (path(0, 0), 18, 'Maria Anna', 'Mozart'),
            (path(0, 1), 18, 'Wolfgang Amadeus', 'Mozart'),
            (path(1), 18, 'Antonio Lucio', 'Vivaldi'),
            (path(2), 19, 'Johann (father)', 'Strauss'),
            (path(2, 0), 19, 'Johann (son)', 'Strauss'),
            (path(3), 19, 'Piotr Ilyich', 'Tchaikovski'),
            (path(4), 20, '', 'Strauss'),
            (path(5), None, '', 'Anonymous'),
        ]
        self.anonymous = Person.objects.create(
            last_name='Anonymous',
        )
        self.vivaldi = Person.objects.create(
            century=18,
            first_name='Antonio Lucio',
            last_name='Vivaldi',
        )
        self.wolfgang_mozart = Person.objects.create(
            century=18,
            first_name='Wolfgang Amadeus',
            last_name='Mozart',
        )
        self.leopold_mozart = Person.objects.create(
            century=18,
            first_name='Leopold',
            last_name='Mozart',
        )
        self.wolfgang_mozart.parent = self.leopold_mozart
        self.wolfgang_mozart.clean()
        self.wolfgang_mozart.save()
        self.maria_anna_mozart = Person.objects.create(
            parent=self.leopold_mozart,
            century=18,
            first_name='Maria Anna',
            last_name='Mozart',
        )
        self.tchaikovski = Person.objects.create(
            century=19,
            first_name='Piotr Ilyich',
            last_name='Tchaikovski',
        )
        self.strauss_father = Person.objects.create(
            century=19,
            first_name='Johann (father)',
            last_name='Strauss',
        )
        self.strauss_son = Person.objects.create(
            parent=self.strauss_father,
            century=19,
            first_name='Johann (son)',
            last_name='Strauss',
        )
        self.strauss = Person.objects.create(
            century=20,
            last_name='Strauss',
        )

    def assertPersons(self, values, queryset=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            if queryset is None:
                queryset = Person.objects.all()
            persons = list(queryset)
        # Reuse the structural check, labelling each person by its ordering
        # fields. `values` rows are `(reference_path, century, first, last)`.
        assert_structure(
            self,
            persons,
            [(v[0], v[1:]) for v in values],
            label=lambda p: (p.century, p.first_name, p.last_name),
        )

    def test_rebuild(self):
        self.assertPersons(self.correct_raw_persons_data)
        with Person.disabled_tree_trigger():
            for i, person in enumerate(
                Person.objects.order_by('-last_name', '-first_name')
            ):
                # Assign distinct single-segment paths that cannot collide with
                # the existing ones; the segment grows with `i` so path order
                # follows the iteration order. This deliberately flattens the
                # tree before rebuilding it.
                person.path = b'\xff' + bytes([i + 1]) + b'\x00'
                person.save()
        self.assertPersons(
            [
                (path(10), 18, 'Antonio Lucio', 'Vivaldi'),
                (path(11), 19, 'Piotr Ilyich', 'Tchaikovski'),
                (path(12), 19, 'Johann (son)', 'Strauss'),
                (path(13), 19, 'Johann (father)', 'Strauss'),
                (path(14), 20, '', 'Strauss'),
                (path(15), 18, 'Wolfgang Amadeus', 'Mozart'),
                (path(16), 18, 'Maria Anna', 'Mozart'),
                (path(17), 18, 'Leopold', 'Mozart'),
                (path(18), None, '', 'Anonymous'),
            ]
        )
        Person.rebuild_paths()
        self.assertPersons(self.correct_persons_data)

    def test_clash_on_insert(self):
        """
        Checks that instances with exactly the same `order_by` values
        are assigned different paths on insertion, sorted by primary key.
        """
        vivaldi2 = Person.objects.create(
            century=18,
            first_name='Antonio Lucio',
            last_name='Vivaldi',
        )
        self.assertGreater(vivaldi2.pk, self.vivaldi.pk)
        self.assertGreater(vivaldi2.path, self.vivaldi.path)
        self.assertPersons(
            [
                (path(-1), 18, 'Antonio Lucio', 'Vivaldi'),
                (path(-0.875), 18, 'Antonio Lucio', 'Vivaldi'),
            ],
            queryset=Person.objects.filter(last_name='Vivaldi'),
        )
        Person.rebuild_paths()
        self.assertPersons(
            [
                (path(1), 18, 'Antonio Lucio', 'Vivaldi'),
                (path(2), 18, 'Antonio Lucio', 'Vivaldi'),
            ],
            queryset=Person.objects.filter(last_name='Vivaldi'),
        )

    def test_clash_on_update(self):
        """
        Checks that instances with exactly the same `order_by` values
        are assigned different paths on update, sorted by primary key.
        """
        vivaldi2 = Person.objects.create(
            century=18,
            first_name='Some',
            last_name='Guy',
        )
        self.assertGreater(vivaldi2.pk, self.vivaldi.pk)
        self.assertLess(vivaldi2.path, self.vivaldi.path)
        self.assertPersons(
            [
                (path(-4), 18, 'Some', 'Guy'),
                (path(-1), 18, 'Antonio Lucio', 'Vivaldi'),
            ],
            queryset=Person.objects.filter(last_name__in=['Vivaldi', 'Guy']),
        )
        vivaldi2.first_name = 'Antonio Lucio'
        vivaldi2.last_name = 'Vivaldi'
        vivaldi2.clean()
        vivaldi2.save()
        self.assertPersons(
            [
                (path(-1), 18, 'Antonio Lucio', 'Vivaldi'),
                (path(-0.875), 18, 'Antonio Lucio', 'Vivaldi'),
            ],
            queryset=Person.objects.filter(last_name='Vivaldi'),
        )
        Person.rebuild_paths()
        self.assertPersons(
            [
                (path(1), 18, 'Antonio Lucio', 'Vivaldi'),
                (path(2), 18, 'Antonio Lucio', 'Vivaldi'),
            ],
            queryset=Person.objects.filter(last_name='Vivaldi'),
        )


class QuerySetTest(CommonTest):
    def test_get_descendants(self):
        self.create_all_test_places()

        places = Place.objects.filter(name__in=('Normandie', 'Österreich'))
        self.assertPlaces(
            [
                (path(0, 0), 'Normandie'),
                (path(1), 'Österreich'),
            ],
            places,
        )

        self.assertPlaces(
            [
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
            ],
            places.get_descendants(),
        )
        self.assertPlaces(
            [
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
            ],
            places.get_descendants(include_self=True),
        )

        osterreich = Place.objects.get(name='Österreich')
        self.create_place('Vienne (AU)', osterreich)

        self.assertPlaces(
            [
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1, 0), 'Vienne (AU)'),
            ],
            places.get_descendants(),
        )
        self.assertPlaces(
            [
                (path(0, 0), 'Normandie'),
                (path(0, 0, -1), 'Eure'),
                (path(0, 0, -0.5), 'Manche'),
                (path(0, 0, 0), 'Seine-Maritime'),
                (path(1), 'Österreich'),
                (path(1, 0), 'Vienne (AU)'),
            ],
            places.get_descendants(include_self=True),
        )


class Issue17Test(CommonTest):
    # https://github.com/BertrandBordage/django-tree/issues/17
    #
    # Inserting a node uses an order-preserving key strictly between the previous
    # and the next sibling's segments. With the old float8 paths, cramming many
    # nodes into one gap exhausted the bisection headroom until a computed
    # midpoint collided with an existing value, raising an IntegrityError on the
    # path unique constraint (the error reported in #17 was
    # `Key (path)=({277.9999999987}) already exists`).
    #
    # With the `bytea` fractional-index keys a tight gap simply grows the key by a
    # byte, so the insertions keep distinct, ordered paths however many land in
    # the same gap -- no renumbering needed.
    def test_inserting_many_nodes_in_the_same_gap(self):
        root = self.create_place('root')
        # Two siblings delimiting the gap we will keep inserting into.
        self.create_place('a', root)
        self.create_place('b', root)
        # Each new name sorts after the previous one but still before 'b', so the
        # gap toward 'b' keeps halving -- far past where float8 ran out.
        n = 69
        for i in range(1, n + 1):
            self.create_place('a%04d' % i, root)

        # Every node must keep a distinct path: root + 'a' + 'b' + n children.
        paths = [tuple(p.path.value) for p in Place.objects.all()]
        self.assertEqual(len(paths), n + 3)
        self.assertEqual(len(set(paths)), len(paths))

    def test_cramming_a_gap_keeps_descendants_consistent(self):
        # Same crammed-gap scenario, but the bounding siblings carry their own
        # subtrees. Cramming the gap must never disturb a sibling's subtree:
        # every descendant must stay strictly under its ancestor.
        root = self.create_place('root')
        # 'a' is the lower bound and has a two-level subtree.
        a = self.create_place('a', root)
        a1 = self.create_place('a1', a)
        self.create_place('a1a', a1)
        # 'z' is the upper bound and has one child.
        z = self.create_place('z', root)
        self.create_place('z1', z)
        # Cram many nodes into the (a, z) gap, each sorting just before 'z', so
        # the gap is halved well past where float8 would have run out.
        n = 60
        for i in range(1, n + 1):
            self.create_place('b%04d' % i, root)

        places = list(Place.objects.all())
        by_pk = {p.pk: p for p in places}

        # Every path is distinct.
        paths = [tuple(p.path.value) for p in places]
        self.assertEqual(len(set(paths)), len(paths))

        # Every node sits directly under its FK parent in the path space: its
        # path extends the parent's path by exactly one level. A descendant left
        # behind by the gap growth would fail this.
        for p in places:
            if p.parent_id is None:
                self.assertEqual(p.path.get_level(), 1)
            else:
                parent_path = by_pk[p.parent_id].path
                self.assertTrue(p.path.value.startswith(parent_path.value))
                self.assertEqual(p.path.get_level(), parent_path.get_level() + 1)

        # The bounding subtrees survived the gap cramming intact and ordered.
        a = Place.objects.get(name='a')
        self.assertEqual(
            [p.name for p in a.get_descendants(include_self=True).order_by('path')],
            ['a', 'a1', 'a1a'],
        )
        z = Place.objects.get(name='z')
        self.assertEqual(
            [p.name for p in z.get_descendants(include_self=True).order_by('path')],
            ['z', 'z1'],
        )

        # Root's children stay in ascending name order after the cramming.
        child_names = list(
            root.get_children().order_by('path').values_list('name', flat=True)
        )
        self.assertEqual(child_names, sorted(child_names))
        self.assertEqual(child_names[0], 'a')
        self.assertEqual(child_names[-1], 'z')


class DescendingOrderByTest(TransactionTestCase):
    """`PathField(order_by=['-name'])` — descending sibling ordering."""

    maxDiff = None

    def _children_names_by_path(self, root):
        return list(root.get_children().order_by('path').values_list('name', flat=True))

    def test_rebuild_orders_siblings_descending(self):
        # Build the tree with the trigger disabled (paths left NULL), then
        # rebuild: the rebuild honours the descending `order_by`.
        with DescendingPlace.disabled_tree_trigger():
            root = DescendingPlace.objects.create(name='Root')
            for name in ['Alpha', 'Beta', 'Gamma']:
                DescendingPlace.objects.create(name=name, parent=root)
        DescendingPlace.rebuild_paths()
        root = DescendingPlace.objects.get(name='Root')
        self.assertTrue(root.path.is_root())
        self.assertListEqual(
            self._children_names_by_path(root), ['Gamma', 'Beta', 'Alpha']
        )

    def test_insert_orders_siblings_descending(self):
        # Inserting siblings one by one keeps them in descending order: the
        # per-insert placement honours the descending direction, so inserting
        # in ascending name order does not collide on the unique path
        # constraint.
        root = DescendingPlace.objects.create(name='Root')
        try:
            for name in ['Alpha', 'Beta', 'Gamma']:
                DescendingPlace.objects.create(name=name, parent=root)
        except IntegrityError as e:
            self.fail(
                'Inserting siblings into a descending tree should not '
                'collide, but raised: %s' % e
            )
        self.assertListEqual(
            self._children_names_by_path(root), ['Gamma', 'Beta', 'Alpha']
        )


class MultiplePathFieldsTest(TransactionTestCase):
    """A model carrying two independent `PathField`s."""

    maxDiff = None

    def setUp(self):
        # `name`-tree:  root -> a -> b
        # `code`-tree:  root -> {a, b}   (b is a direct child of root here)
        self.root = MultiPathPlace.objects.create(name='root', code='root')
        self.a = MultiPathPlace.objects.create(
            name='A', code='B', name_parent=self.root, code_parent=self.root
        )
        self.b = MultiPathPlace.objects.create(
            name='B', code='A', name_parent=self.a, code_parent=self.root
        )

    def test_each_path_field_tracks_its_own_hierarchy(self):
        b = MultiPathPlace.objects.get(name='B')
        name_ancestors = list(
            b.get_ancestors(path_field='name_path').values_list('name', flat=True)
        )
        code_ancestors = list(
            b.get_ancestors(path_field='code_path').values_list('name', flat=True)
        )
        # In the `name` tree, B is nested under A (and root).
        self.assertEqual(name_ancestors, ['root', 'A'])
        # In the `code` tree, B hangs directly off root, not under A.
        self.assertEqual(code_ancestors, ['root'])

    def test_path_field_must_be_specified_when_ambiguous(self):
        with self.assertRaises(ValueError):
            self.root.get_children()
        # Explicitly naming the field resolves the ambiguity.
        self.assertEqual(
            list(
                self.root.get_children(path_field='name_path').values_list(
                    'name', flat=True
                )
            ),
            ['A'],
        )


class NonIntegerPrimaryKeyTest(TransactionTestCase):
    """Tree maintained on a model with a UUID primary key."""

    maxDiff = None

    def test_tree_on_uuid_pk(self):
        root = UUIDPlace.objects.create(name='Root')
        self.assertIsInstance(root.pk, uuid.UUID)
        UUIDPlace.objects.create(name='Aaa', parent=root)
        UUIDPlace.objects.create(name='Bbb', parent=root)
        expected = [
            (path(0), 'Root'),
            (path(0, 0), 'Aaa'),
            (path(0, 1), 'Bbb'),
        ]
        assert_structure(self, list(UUIDPlace.objects.order_by('path')), expected)
        # Rebuild is stable on a non-integer pk.
        UUIDPlace.rebuild_paths()
        assert_structure(self, list(UUIDPlace.objects.order_by('path')), expected)

    def test_uuid_pk_breaks_order_by_ties(self):
        # The trigger appends `pk` to `order_by` to break ties between siblings
        # sharing the same ordering values. With a UUID pk, that tie-break must
        # still yield distinct, non-colliding paths.
        root = UUIDPlace.objects.create(name='Root')
        UUIDPlace.objects.create(name='Same', parent=root)
        UUIDPlace.objects.create(name='Same', parent=root)
        # The two same-named siblings get distinct paths and stay direct
        # children of root. Their exact values on insert depend on the random
        # UUID tie-break, so only the distinctness/depth is asserted here.
        root = UUIDPlace.objects.get(name='Root')
        children = list(UUIDPlace.objects.exclude(pk=root.pk))
        self.assertEqual(len({c.path.value for c in children}), 2)
        for child in children:
            self.assertEqual(child.path.get_level(), 2)
            self.assertTrue(child.path.is_descendant_of(root.path))
        # A rebuild normalises the tie-break to consecutive slots, whichever UUID
        # happens to sort first.
        UUIDPlace.rebuild_paths()
        assert_structure(
            self,
            list(UUIDPlace.objects.order_by('path')),
            [
                (path(0), 'Root'),
                (path(0, 0), 'Same'),
                (path(0, 1), 'Same'),
            ],
        )


class OnDeleteBehaviourTest(TransactionTestCase):
    """`on_delete` behaviours other than the `CASCADE` covered elsewhere."""

    maxDiff = None

    def test_set_null_keeps_children(self):
        root = SetNullPlace.objects.create(name='Root')
        child = SetNullPlace.objects.create(name='Child', parent=root)
        self.assertEqual(child.path.get_level(), 2)
        # Delete only the parent row (not the whole subtree): the FK is
        # `SET_NULL`, so the child survives with a null parent.
        SetNullPlace.objects.filter(pk=root.pk).delete()
        child.refresh_from_db()
        self.assertIsNone(child.parent_id)
        # The `SET_NULL` update fires the path trigger, so the now-orphan child
        # is re-pathed as a root immediately (its exact value depends on the
        # sibling ordering at delete time, hence only the depth is asserted).
        self.assertEqual(child.path.get_level(), 1)
        SetNullPlace.rebuild_paths()
        child.refresh_from_db()
        self.assertTrue(child.path.is_root())

    def test_protect_blocks_parent_deletion(self):
        root = ProtectPlace.objects.create(name='Root')
        child = ProtectPlace.objects.create(name='Child', parent=root)
        self.assertEqual(child.path.get_level(), 2)
        # The FK is `PROTECT`, so deleting a referenced parent is refused.
        with self.assertRaises(ProtectedError):
            ProtectPlace.objects.filter(pk=root.pk).delete()
        # The whole tree is left untouched by the refused deletion.
        root = ProtectPlace.objects.get(pk=root.pk)
        child = ProtectPlace.objects.get(pk=child.pk)
        self.assertTrue(root.path.is_root())
        self.assertEqual(child.path.get_level(), 2)
        self.assertTrue(child.path.is_descendant_of(root.path))


class UnusualTableNameTest(TransactionTestCase):
    """A model stored in a table whose name requires SQL quoting.

    Its `CreateTreeTrigger` is installed here at runtime rather than in a
    migration, so the create/drop path is exercised end-to-end in isolation.
    The generated SQL quotes the composite function/constraint names and the
    table reference, so a table name needing quoting works like any other.
    """

    maxDiff = None

    def _drop_trigger(self, op, state):
        with connection.schema_editor(atomic=True) as editor:
            op.database_backwards('tests', editor, state, state)

    def test_trigger_supports_quoted_table_name(self):
        op = CreateTreeTrigger('tests.WeirdTableNamePlace')
        state = ProjectState.from_apps(apps)
        with connection.schema_editor(atomic=True) as editor:
            op.database_forwards('tests', editor, state, state)
        # Only reached if the trigger was created successfully.
        self.addCleanup(self._drop_trigger, op, state)

        root = WeirdTableNamePlace.objects.create(name='Root')
        WeirdTableNamePlace.objects.create(name='Child', parent=root)
        root = WeirdTableNamePlace.objects.get(name='Root')
        child = WeirdTableNamePlace.objects.get(name='Child')
        self.assertTrue(root.path.is_root())
        self.assertEqual(child.path.get_level(), 2)
        self.assertTrue(child.path.is_descendant_of(root.path))


class PathObjectTest(CommonTest):
    """`Path` behaviours not exercised through the ORM navigation tests:
    iteration, an empty/unbound path, NULLS-LAST comparison when the left
    operand is empty, and the ancestry edge cases."""

    def test_repr_str_and_iteration(self):
        self.create_all_test_places()
        france = Place.objects.get(name='France').path
        self.assertIn('Path', repr(france))
        self.assertEqual(str(france), str(france.value))
        self.assertEqual(list(france), france.value.split(b'\x00')[:-1])

        empty = Place().path
        self.assertEqual(list(empty), [])

        # A `Path` whose field was never bound to a model (no `attname`).
        unbound = Path(PathField(), b'\x02\x00')
        self.assertFalse(unbound.field_bound)
        self.assertIsInstance(unbound.qs, QuerySet)
        self.assertEqual(repr(unbound), '<Path %s>' % b'\x02\x00')

    def test_comparisons_with_empty_left_operand(self):
        self.create_all_test_places()
        france = Place.objects.get(name='France').path
        empty = Place().path
        # An empty path sorts last (NULLS LAST), whatever the operator.
        self.assertFalse(empty < france)
        self.assertFalse(empty <= france)
        self.assertTrue(empty > france)
        self.assertTrue(empty >= france)

    def test_ancestry_edge_cases(self):
        self.create_all_test_places()
        france = Place.objects.get(name='France').path
        empty = Place().path
        # An empty path is neither an ancestor nor a descendant of anything,
        # and nothing is an ancestor/descendant of an empty path.
        self.assertFalse(empty.is_ancestor_of(france))
        self.assertFalse(empty.is_descendant_of(france))
        self.assertFalse(france.is_ancestor_of(empty))
        self.assertFalse(france.is_descendant_of(empty))
        # A non-`Path`, non-bytes argument is rejected.
        with self.assertRaises(TypeError):
            france.is_ancestor_of(42)
        with self.assertRaises(TypeError):
            france.is_descendant_of(42)


class PathFieldTest(CommonTest):
    def test_forbidden_kwargs(self):
        for kwarg in ('default', 'null', 'unique'):
            with self.assertRaises(ImproperlyConfigured):
                PathField(**{kwarg: True})

    def test_order_by_cannot_reference_itself(self):
        field = PathField(order_by=['self_ref'])
        with self.assertRaises(ImproperlyConfigured):
            field.contribute_to_class(Place, 'self_ref')

    def test_value_conversions(self):
        self.create_all_test_places()
        field = Place._meta.get_field('path')
        france = Place.objects.get(name='France').path
        # An already-built `Path` is returned untouched.
        self.assertIs(field.from_db_value(france, None, connection), france)
        self.assertIs(field.to_python(france), france)
        # `memoryview` (psycopg2) and raw `bytes` are both coerced to `bytes`.
        self.assertEqual(field.to_python(memoryview(b'\x02\x00')).value, b'\x02\x00')
        self.assertEqual(field.to_python(b'\x03\x00').value, b'\x03\x00')
        self.assertEqual(field.get_prep_value(memoryview(b'\x04\x00')), b'\x04\x00')

    def test_non_postgresql_backend_raises(self):
        field = Place._meta.get_field('path')
        with mock.patch.object(connection, 'vendor', 'sqlite'):
            with self.assertRaises(NotImplementedError):
                field._check_database_backend('default')

    def test_disabled_trigger_context_manager(self):
        field = Place._meta.get_field('path')
        with field.disabled_trigger():
            disabled = Place.objects.create(name='disabled')
        self.assertIsNone(Place.objects.get(pk=disabled.pk).path.value)
        # The trigger is back on once the context manager exits.
        enabled = Place.objects.create(name='enabled')
        self.assertIsNotNone(Place.objects.get(pk=enabled.pk).path.value)


class TreeModelMixinExtraTest(CommonTest):
    def test_clean_with_unresolved_parent_pk(self):
        self.create_all_test_places()
        france = Place.objects.get(name='France')
        # Simulate a deferred parent FK still holding an unresolved (here
        # missing) pk rather than a model instance.
        Place._meta.get_field('parent').set_cached_value(france, 10**9)
        with self.assertRaises(ValidationError):
            france.clean()

    def test_delete_with_explicit_using(self):
        self.create_all_test_places()
        normandie = Place.objects.get(name='Normandie')
        normandie.delete(using='default')
        self.assertFalse(Place.objects.filter(name='Normandie').exists())
        # The whole subtree went with it.
        self.assertFalse(Place.objects.filter(name='Eure').exists())


class OperationsTest(TransactionTestCase):
    def _drop(self, op, state):
        with connection.schema_editor(atomic=True) as editor:
            op.database_forwards('tests', editor, state, state)

    def test_describe_strings(self):
        self.assertIn('trigger', CreateTreeTrigger('place').describe())
        self.assertIn('trigger', DeleteTreeTrigger('place').describe())
        self.assertIn('tree structure', RebuildPaths('place').describe())

    def test_non_postgresql_backend_raises(self):
        class FakeConnection:
            vendor = 'sqlite'

        class FakeEditor:
            connection = FakeConnection()

        editor = FakeEditor()
        for op in (CreateTreeTrigger('place'), RebuildPaths('place')):
            with self.assertRaises(NotImplementedError):
                op.check_database_backend(editor)

    def test_delete_trigger_is_reversible(self):
        op = DeleteTreeTrigger('tests.WeirdTableNamePlace')
        state = ProjectState.from_apps(apps)
        # Reversing a `DeleteTreeTrigger` re-creates the trigger.
        with connection.schema_editor(atomic=True) as editor:
            op.database_backwards('tests', editor, state, state)
        self.addCleanup(self._drop, op, state)

        root = WeirdTableNamePlace.objects.create(name='Root')
        child = WeirdTableNamePlace.objects.create(name='Child', parent=root)
        self.assertEqual(
            WeirdTableNamePlace.objects.get(pk=child.pk).path.get_level(), 2
        )

    def test_rebuild_paths_backwards_is_noop(self):
        state = ProjectState.from_apps(apps)
        with connection.schema_editor(atomic=True) as editor:
            RebuildPaths('place').database_backwards('tests', editor, state, state)

    def test_get_pre_params_skips_pk_in_order_by(self):
        # A `pk` entry in `order_by` is the tie-break the trigger applies on its
        # own, so it contributes no watched update column.
        field = Place._meta.get_field('path')
        original_order_by = field.order_by
        field.order_by = ['pk', 'name']
        try:
            params = CreateTreeTrigger('place').get_pre_params(Place)
        finally:
            field.order_by = original_order_by
        self.assertNotIn('pk', params['update_columns'])


class QueryHelperTest(SimpleTestCase):
    def test_get_path_field_requires_a_path_field(self):
        with self.assertRaises(FieldDoesNotExist):
            _get_path_field(MigrationRecorder.Migration, None)


class TreeChoiceFieldTest(CommonTest):
    def test_label_from_instance(self):
        self.create_all_test_places()
        field = TreeChoiceField(queryset=Place.objects.all())
        france = Place.objects.get(name='France')
        normandie = Place.objects.get(name='Normandie')
        self.assertEqual(field.label_from_instance(france), str(france))
        self.assertEqual(field.label_from_instance(normandie), '── %s' % normandie)


class PsycopgAdapterTest(SimpleTestCase):
    def test_psycopg3_dumpers(self):
        from psycopg import pq

        path = Path(None, b'\x02\x00')
        binary = Path._psycopg3_dumper(pq.Format.BINARY)(Path, None)
        self.assertEqual(binary.dump(path), b'\x02\x00')
        self.assertEqual(binary.dump(Path(None, None)), b'')
        text = Path._psycopg3_dumper(pq.Format.TEXT)(Path, None)
        self.assertEqual(text.dump(path), b'\\x0200')

    def test_register_psycopg2(self):
        import sys
        import types as module_types

        captured = {}
        fake = module_types.ModuleType('psycopg2')
        fake.Binary = lambda value: ('Binary', value)
        extensions = module_types.ModuleType('psycopg2.extensions')
        extensions.AsIs = lambda value: ('AsIs', value)
        extensions.register_adapter = lambda cls, fn: captured.__setitem__(cls, fn)
        fake.extensions = extensions
        with mock.patch.dict(
            sys.modules, {'psycopg2': fake, 'psycopg2.extensions': extensions}
        ):
            Path.register_psycopg2()
        adapt = captured[Path]
        self.assertEqual(adapt(Path(None, None)), ('AsIs', 'NULL'))
        self.assertEqual(adapt(Path(None, b'\x02\x00')), ('Binary', b'\x02\x00'))

    def test_register_psycopg_falls_back_to_psycopg2(self):
        def fake_find_spec(name):
            return None if name == 'psycopg' else object()

        # `find_spec` is imported into `tree.types`, so patch it there.
        with mock.patch('tree.types.find_spec', side_effect=fake_find_spec):
            with mock.patch.object(Path, 'register_psycopg2') as register_psycopg2:
                Path.register_psycopg()
                register_psycopg2.assert_called_once()


def load_tests(loader, tests, pattern):
    # The SQL-building helpers in `tree.sql.base` are specified by doctests; run
    # them as part of the suite so they are covered and verified.
    tests.addTests(doctest.DocTestSuite(sql_base))
    return tests
