# coding: utf-8

from __future__ import unicode_literals
from unittest import expectedFailure

from django.db import transaction, InternalError
from django.test import TransactionTestCase

from .models import Place


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
# TODO: Test path arrays.


class PathTest(TransactionTestCase):
    def create_place(self, name, parent=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            return Place.objects.create(name=name, parent=parent)

    def create_test_places(self):
        self.correct_places_data = [
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich')]
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

    def assertPlaces(self, values, queryset=None, n_queries=1):
        with self.assertNumQueries(n_queries):
            if queryset is None:
                queryset = Place.objects.all()
            places = list(queryset)
            self.assertListEqual([(p.path, p.name) for p in places], values)

    def test_insert(self):
        it = self.create_test_places()
        next(it)
        self.assertPlaces([('00', 'France')])
        next(it)
        self.assertPlaces([('00', 'France'), ('00.00', 'Normandie')])
        next(it)
        self.assertPlaces([('00', 'France'), ('00.00', 'Normandie'),
                           ('00.00.00', 'Seine-Maritime')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'),
            ('00.00.00', 'Eure'), ('00.00.01', 'Seine-Maritime')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('01', 'Österreich')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('01', 'Österreich'), ('01.00', 'Vienne')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('01', 'Österreich'),
            ('01.00', 'Vienne')])
        next(it)
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('01', 'Österreich'),
            ('01.00', 'Vienne'), ('01.00.00', 'Poitiers')])
        next(it)
        self.assertPlaces(self.correct_places_data)

    # TODO: Fix the unique constraint order.
    @expectedFailure
    def test_move_root_to_prev_root(self):
        list(self.create_test_places())

        osterreich = Place.objects.get(name='Österreich')
        osterreich.name = 'Autriche'
        osterreich.save()
        self.assertPlaces([
            ('00', 'Autriche'), ('01', 'France'),
            ('01.00', 'Normandie'), ('01.00.00', 'Eure'),
            ('01.00.01', 'Manche'), ('01.00.02', 'Seine-Maritime'),
            ('01.01', 'Poitou-Charentes'), ('01.01.00', 'Vienne'),
            ('01.01.00.00', 'Poitiers')])

    # TODO: Fix the unique constraint order.
    @expectedFailure
    def test_move_root_to_next_root(self):
        list(self.create_test_places())

        france = Place.objects.get(name='France')
        france.name = 'République française'
        france.save()
        self.assertPlaces([
            ('00', 'Österreich'), ('01', 'République française'),
            ('01.00', 'Normandie'), ('01.00.00', 'Eure'),
            ('01.00.01', 'Manche'), ('01.00.02', 'Seine-Maritime'),
            ('01.01', 'Poitou-Charentes'), ('01.01.00', 'Vienne'),
            ('01.01.00.00', 'Poitiers')])

    # TODO: Remove holes after moving an object to another parent
    #       or after deleting it.
    @expectedFailure
    def test_move_root_to_prev_branch(self):
        list(self.create_test_places())

        little_france = Place.objects.create(name='Île-de-France')
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'),
            ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Île-de-France'),
            ('02', 'Österreich')])

        little_france.parent = Place.objects.get(name='France')
        little_france.save()
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Île-de-France'),
            ('00.01', 'Normandie'), ('00.01.00', 'Eure'),
            ('00.01.01', 'Manche'), ('00.01.02', 'Seine-Maritime'),
            ('00.02', 'Poitou-Charentes'), ('00.02.00', 'Vienne'),
            ('00.02.00.00', 'Poitiers'), ('01', 'Österreich')])

    # TODO: Remove holes after moving an object to another parent
    #       or after deleting it.
    @expectedFailure
    def test_move_root_to_next_branch(self):
        list(self.create_test_places())

        bretagne = Place.objects.create(name='Bretagne')
        self.assertPlaces([
            ('00', 'Bretagne'),
            ('01', 'France'), ('01.00', 'Normandie'),
            ('01.00.00', 'Eure'),
            ('01.00.01', 'Manche'), ('01.00.02', 'Seine-Maritime'),
            ('01.01', 'Poitou-Charentes'), ('01.01.00', 'Vienne'),
            ('01.01.00.00', 'Poitiers'), ('02', 'Österreich')])

        bretagne.parent = Place.objects.get(name='France')
        bretagne.save()
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Bretagne'), ('00.01', 'Normandie'),
            ('00.01.00', 'Eure'),
            ('00.01.01', 'Manche'), ('00.01.02', 'Seine-Maritime'),
            ('00.02', 'Poitou-Charentes'), ('00.02.00', 'Vienne'),
            ('00.02.00.00', 'Poitiers'), ('01', 'Österreich')])

    # TODO: Remove holes after moving an object to another parent
    #       or after deleting it.
    @expectedFailure
    def test_move_root_to_prev_leaf(self):
        list(self.create_test_places())

        grattenoix = Place.objects.create(name='Grattenoix')
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Grattenoix'),
            ('02', 'Österreich')])

        grattenoix.parent = Place.objects.get(name='Seine-Maritime')
        grattenoix.save()
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.00.02.00', 'Grattenoix'), ('00.01', 'Poitou-Charentes'),
            ('00.01.00', 'Vienne'), ('00.01.00.00', 'Poitiers'),
            ('01', 'Österreich')])

    # TODO: Remove holes after moving an object to another parent
    #       or after deleting it.
    @expectedFailure
    def test_move_root_to_next_leaf(self):
        list(self.create_test_places())

        evreux = Place.objects.create(name='Évreux')
        self.assertPlaces([
            ('00', 'Évreux'), ('01', 'France'), ('01.00', 'Normandie'),
            ('01.00.00', 'Eure'), ('01.00.01', 'Manche'),
            ('01.00.02', 'Seine-Maritime'), ('01.01', 'Poitou-Charentes'),
            ('01.01.00', 'Vienne'), ('01.01.00.00', 'Poitiers'),
            ('02', 'Österreich')])

        evreux.parent = Place.objects.get(name='Eure')
        evreux.save()
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.00.00', 'Évreux'), ('00.00.01', 'Manche'),
            ('00.00.02', 'Seine-Maritime'), ('00.01', 'Poitou-Charentes'),
            ('00.01.00', 'Vienne'), ('00.01.00.00', 'Poitiers'),
            ('01', 'Österreich')])

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

    def test_max_siblings(self):
        max_siblings = 108
        bulk = [Place(name='Anything') for _ in range(max_siblings)]
        with self.assertNumQueries(1):
            Place.objects.bulk_create(bulk)

        self.assertListEqual(
            list(Place.objects.order_by('-name', '-pk')
                 .values_list('path', flat=True)[:5]),
            ['2Z', '2Y', '2X', '2W', '2V'])

        with self.assertNumQueries(1):
            with self.assertRaisesMessage(
                    InternalError,
                    '`max_siblings` (%d) has been reached.\n'
                    'You should increase it then rebuild.'
                    % max_siblings):
                Place.objects.create(name='Anything')

    def test_get_level(self):
        list(self.create_test_places())

        with self.assertNumQueries(1):
            data = [(p.get_level(), p.name) for p in Place.objects.all()]
            self.assertListEqual(data, [
                (1, 'France'), (2, 'Normandie'), (3, 'Eure'), (3, 'Manche'),
                (3, 'Seine-Maritime'), (2, 'Poitou-Charentes'), (3, 'Vienne'),
                (4, 'Poitiers'), (1, 'Österreich')])

    def test_is_root(self):
        list(self.create_test_places())

        places = [p.name for p in Place.objects.all() if p.is_root()]
        self.assertListEqual(places, ['France', 'Österreich'])

    def test_is_leaf(self):
        list(self.create_test_places())

        places = [p.name for p in Place.objects.all() if p.is_leaf()]
        self.assertListEqual(places, ['Eure', 'Manche', 'Seine-Maritime',
                                      'Poitiers', 'Österreich'])

    def test_get_children(self):
        list(self.create_test_places())

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
        list(self.create_test_places())

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
        list(self.create_test_places())

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
        list(self.create_test_places())

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

    def test_get_prev_siblings(self):
        list(self.create_test_places())

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
        list(self.create_test_places())

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
        list(self.create_test_places())

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.get_prev_sibling(), None)

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertEqual(normandie.get_prev_sibling(), None)

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertEqual(
                seine_maritime.get_prev_sibling().name, 'Manche')

    def test_get_next_sibling(self):
        list(self.create_test_places())

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
            self.assertEqual(seine_maritime.get_next_sibling(), None)

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
            self.assertEqual(place.get_prev_sibling(), None)
        with self.assertNumQueries(0):
            self.assertEqual(place.get_next_sibling(), None)
        with self.assertNumQueries(0):
            self.assertEqual(place.get_level(), None)
        with self.assertNumQueries(0):
            self.assertEqual(place.is_root(), None)
        with self.assertNumQueries(0):
            self.assertEqual(place.is_leaf(), None)

    def test_comparisons(self):
        list(self.create_test_places())

        france = Place.objects.get(name='France').path
        self.assertTrue(france == france)
        self.assertFalse(france != france)
        self.assertFalse(france > france)
        self.assertTrue(france >= france)
        self.assertFalse(france < france)
        self.assertTrue(france <= france)

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
        list(self.create_test_places())

        for place in Place.objects.all():
            self.assertFalse(place.is_ancestor_of(place))
            self.assertTrue(place.is_ancestor_of(place,
                                                 include_self=True))
            for ancestor in place.get_ancestors():
                self.assertTrue(ancestor.is_ancestor_of(place))

    def test_is_descendant_of(self):
        list(self.create_test_places())

        for place in Place.objects.all():
            self.assertFalse(place.is_descendant_of(place))
            self.assertTrue(place.is_descendant_of(place,
                                                   include_self=True))
            for descendant in place.get_descendants():
                self.assertTrue(descendant.is_descendant_of(place))

    def test_get_roots(self):
        list(self.create_test_places())

        self.assertPlaces([('00', 'France'), ('01', 'Österreich')],
                          queryset=Place.get_roots())

    def test_rebuild(self):
        list(self.create_test_places())

        with Place.disabled_tree_trigger():
            for i, place in enumerate(Place.objects.order_by('name')):
                place.path = str(i)
                place.save()
        self.assertPlaces([
            ('0', 'Eure'), ('1', 'France'), ('2', 'Manche'),
            ('3', 'Normandie'), ('4', 'Österreich'), ('5', 'Poitiers'),
            ('6', 'Poitou-Charentes'), ('7', 'Seine-Maritime'),
            ('8', 'Vienne')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Root
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='France').update(path='2Z')
        self.assertPlaces([
            ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich'),
            ('2Z', 'France')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Branch
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='Normandie').update(path='2Z.2Z')
        self.assertPlaces([
            ('00', 'France'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich'),
            ('2Z.2Z', 'Normandie')])
        with self.assertNumQueries(1):
            Place.rebuild_paths()
        self.assertPlaces(self.correct_places_data)

        # Leaf
        with Place.disabled_tree_trigger():
            Place.objects.filter(name='Seine-Maritime').update(path='00.2Z')
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.01', 'Poitou-Charentes'),
            ('00.01.00', 'Vienne'), ('00.01.00.00', 'Poitiers'),
            ('00.2Z', 'Seine-Maritime'), ('01', 'Österreich')])
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
