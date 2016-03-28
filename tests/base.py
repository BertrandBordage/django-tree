# coding: utf-8

from __future__ import unicode_literals
from django.db import transaction, InternalError
from django.test import TestCase

from .models import Place


# TODO: Test same order_by values.
# TODO: Test order_by with descending orders.
# TODO: Test what happens when we move a node after itself
#       while staying in the same siblinghood
#       (it should not create a hole at the former position).
# TODO: Test raw SQL insertion/update/delete.
# TODO: Test if rebuild works with NULL path values.
# TODO: Test using Path objects as sql parameters.


class PathTest(TestCase):
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

    def assertPlaces(self, values, n_queries=1):
        with self.assertNumQueries(n_queries):
            places = list(Place.objects.all())
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

    def test_rebuild(self):
        list(self.create_test_places())

        Place.objects.update(path='00')
        self.assertPlaces([
            ('00', 'Eure'), ('00', 'France'), ('00', 'Manche'),
            ('00', 'Normandie'), ('00', 'Österreich'), ('00', 'Poitiers'),
            ('00', 'Poitou-Charentes'), ('00', 'Seine-Maritime'),
            ('00', 'Vienne')])
        with self.assertNumQueries(1):
            Place._meta.get_field('path').rebuild()
        self.assertPlaces(self.correct_places_data)

        # Root
        Place.objects.filter(name='France').update(path='2Z')
        self.assertPlaces([
            ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich'),
            ('2Z', 'France')])
        with self.assertNumQueries(1):
            Place._meta.get_field('path').rebuild()
        self.assertPlaces(self.correct_places_data)

        # Branch
        Place.objects.filter(name='Normandie').update(path='2Z.2Z')
        self.assertPlaces([
            ('00', 'France'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich'),
            ('2Z.2Z', 'Normandie')])
        with self.assertNumQueries(1):
            Place._meta.get_field('path').rebuild()
        self.assertPlaces(self.correct_places_data)

        # Leaf
        Place.objects.filter(name='Seine-Maritime').update(path='00.2Z')
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.01', 'Poitou-Charentes'),
            ('00.01.00', 'Vienne'), ('00.01.00.00', 'Poitiers'),
            ('00.2Z', 'Seine-Maritime'), ('01', 'Österreich')])
        with self.assertNumQueries(1):
            Place._meta.get_field('path').rebuild()
        self.assertPlaces(self.correct_places_data)

    def test_max_siblings(self):
        max_siblings = 108
        bulk = [Place(name='Anything') for _ in range(max_siblings)]
        # TODO: Find a way to `bulk_create` in a single SQL query.
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

    def test_depth(self):
        list(self.create_test_places())

        with self.assertNumQueries(1):
            data = [(p.path.depth, p.name) for p in Place.objects.all()]
            self.assertListEqual(data, [
                (0, 'France'), (1, 'Normandie'), (2, 'Eure'), (2, 'Manche'),
                (2, 'Seine-Maritime'), (1, 'Poitou-Charentes'), (2, 'Vienne'),
                (3, 'Poitiers'), (0, 'Österreich')])

    def test_level(self):
        list(self.create_test_places())

        with self.assertNumQueries(1):
            paths = [p.path for p in Place.objects.all()]
            self.assertListEqual(
                [p.level for p in paths], [p.depth + 1 for p in paths])

    def test_is_root(self):
        list(self.create_test_places())

        places = [p.name for p in Place.objects.all() if p.path.is_root]
        self.assertListEqual(places, ['France', 'Österreich'])

    def test_is_leaf(self):
        list(self.create_test_places())

        places = [p.name for p in Place.objects.all() if p.path.is_leaf]
        self.assertListEqual(places, ['Eure', 'Manche', 'Seine-Maritime',
                                      'Poitiers', 'Österreich'])

    def test_get_children(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_children()
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_children()
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_children()
                     .values_list('name', flat=True)), [])

    def test_get_ancestors(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_ancestors(include_self=True)
                     .values_list('name', flat=True)), ['France'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_ancestors()
                     .values_list('name', flat=True)), [])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_ancestors(include_self=True)
                     .values_list('name', flat=True)), ['France', 'Normandie'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_ancestors()
                     .values_list('name', flat=True)), ['France'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_ancestors(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Normandie', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_ancestors()
                     .values_list('name', flat=True)), ['France', 'Normandie'])

    def test_get_descendants(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_descendants(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Normandie', 'Eure', 'Manche', 'Seine-Maritime',
                 'Poitou-Charentes', 'Vienne', 'Poitiers'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_descendants()
                     .values_list('name', flat=True)),
                ['Normandie', 'Eure', 'Manche', 'Seine-Maritime',
                 'Poitou-Charentes', 'Vienne', 'Poitiers'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_descendants(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Eure', 'Manche', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_descendants()
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_descendants(include_self=True)
                     .values_list('name', flat=True)), ['Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_descendants()
                     .values_list('name', flat=True)), [])

    def test_get_siblings(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Österreich'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_siblings()
                     .values_list('name', flat=True)), ['Österreich'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_siblings()
                     .values_list('name', flat=True)), ['Poitou-Charentes'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Eure', 'Manche', 'Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_siblings()
                     .values_list('name', flat=True)), ['Eure', 'Manche'])

    def test_get_prev_siblings(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)), ['France'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_prev_siblings()
                     .values_list('name', flat=True)), [])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)), ['Normandie'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_prev_siblings()
                     .values_list('name', flat=True)), [])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_prev_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Seine-Maritime', 'Manche', 'Eure'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_prev_siblings()
                     .values_list('name', flat=True)), ['Manche', 'Eure'])

    def test_get_next_siblings(self):
        list(self.create_test_places())

        # Root

        france = Place.objects.get(name='France')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['France', 'Österreich'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(france.path.get_next_siblings()
                     .values_list('name', flat=True)), ['Österreich'])

        # Branch

        normandie = Place.objects.get(name='Normandie')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Normandie', 'Poitou-Charentes'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(normandie.path.get_next_siblings()
                     .values_list('name', flat=True)), ['Poitou-Charentes'])

        # Leaf

        seine_maritime = Place.objects.get(name='Seine-Maritime')

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_next_siblings(include_self=True)
                     .values_list('name', flat=True)),
                ['Seine-Maritime'])

        with self.assertNumQueries(1):
            self.assertListEqual(
                list(seine_maritime.path.get_next_siblings()
                     .values_list('name', flat=True)), [])

    def test_get_prev_sibling(self):
        list(self.create_test_places())

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.path.get_prev_sibling(), None)

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertEqual(normandie.path.get_prev_sibling(), None)

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertEqual(
                seine_maritime.path.get_prev_sibling().name, 'Manche')

    def test_get_next_sibling(self):
        list(self.create_test_places())

        # Root
        france = Place.objects.get(name='France')
        with self.assertNumQueries(1):
            self.assertEqual(france.path.get_next_sibling().name, 'Österreich')

        # Branch
        normandie = Place.objects.get(name='Normandie')
        with self.assertNumQueries(1):
            self.assertEqual(
                normandie.path.get_next_sibling().name, 'Poitou-Charentes')

        # Leaf
        seine_maritime = Place.objects.get(name='Seine-Maritime')
        with self.assertNumQueries(1):
            self.assertEqual(seine_maritime.path.get_next_sibling(), None)

    def test_new_path(self):
        path = Place().path

        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_children()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_ancestors()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_descendants()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_siblings()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_prev_siblings()), [])
        with self.assertNumQueries(0):
            self.assertListEqual(list(path.get_next_siblings()), [])
        with self.assertNumQueries(0):
            self.assertEqual(path.get_prev_sibling(), None)
        with self.assertNumQueries(0):
            self.assertEqual(path.get_next_sibling(), None)
        with self.assertNumQueries(0):
            self.assertEqual(path.depth, None)
        with self.assertNumQueries(0):
            self.assertEqual(path.level, None)
        with self.assertNumQueries(0):
            self.assertEqual(path.is_root, None)
        with self.assertNumQueries(0):
            self.assertEqual(path.is_leaf, None)

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

        # Same depth
        osterreich = Place.objects.get(name='Österreich').path
        self.assertEqual(france.depth, osterreich.depth)
        self.assertFalse(france == osterreich)
        self.assertTrue(france != osterreich)
        self.assertTrue(france < osterreich)
        self.assertTrue(france <= osterreich)
        self.assertFalse(france > osterreich)
        self.assertFalse(france >= osterreich)

        # Inferior depth
        normandie = Place.objects.get(name='Normandie').path
        self.assertLess(france.depth, normandie.depth)
        self.assertFalse(france == normandie)
        self.assertTrue(france != normandie)
        self.assertTrue(france < normandie)
        self.assertTrue(france <= normandie)
        self.assertFalse(france > normandie)
        self.assertFalse(france >= normandie)

        # Superior depth
        self.assertGreater(normandie.depth, osterreich.depth)
        self.assertFalse(normandie == osterreich)
        self.assertTrue(normandie != osterreich)
        self.assertTrue(normandie < osterreich)
        self.assertTrue(normandie <= osterreich)
        self.assertFalse(normandie > osterreich)
        self.assertFalse(normandie >= osterreich)

    def test_is_ancestor_of(self):
        list(self.create_test_places())

        for place in Place.objects.all():
            self.assertFalse(place.path.is_ancestor_of(place.path))
            self.assertTrue(place.path.is_ancestor_of(place.path,
                                                      include_self=True))
            for ancestor in place.path.get_ancestors():
                self.assertTrue(ancestor.path.is_ancestor_of(place.path))

    def test_is_descendant_of(self):
        list(self.create_test_places())

        for place in Place.objects.all():
            self.assertFalse(place.path.is_descendant_of(place.path))
            self.assertTrue(place.path.is_descendant_of(place.path,
                                                        include_self=True))
            for descendant in place.path.get_descendants():
                self.assertTrue(descendant.path.is_descendant_of(place.path))

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
