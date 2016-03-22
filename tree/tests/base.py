# coding: utf-8

from __future__ import unicode_literals
from django.test import TestCase

from .models import Place


class PathTest(TestCase):
    def create_place(self, name, parent=None, n_queries=4):
        with self.assertNumQueries(n_queries):
            return Place.objects.create(name=name, parent=parent)

    def create_test_places(self):
        france = self.create_place('France')
        yield france
        normandie = self.create_place('Normandie', france)
        yield normandie
        yield self.create_place('Seine-Maritime', normandie)
        yield self.create_place('Eure', normandie, n_queries=5)
        yield self.create_place('Manche', normandie, n_queries=5)
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
        self.assertPlaces([
            ('00', 'France'), ('00.00', 'Normandie'), ('00.00.00', 'Eure'),
            ('00.00.01', 'Manche'), ('00.00.02', 'Seine-Maritime'),
            ('00.01', 'Poitou-Charentes'), ('00.01.00', 'Vienne'),
            ('00.01.00.00', 'Poitiers'), ('01', 'Österreich')])

    def test_max_siblings(self):
        path_field = Place._meta.get_field('path')
        bulk = [Place(name='Anything') for _ in range(path_field.max_siblings)]
        with self.assertNumQueries(325):
            Place.objects.bulk_create(bulk)

        # FIXME: Find a way to update the tree without having to call
        # `rebuild_tree`.
        self.assertListEqual(
            list(Place.objects.order_by('-path').values_list('path',
                                                             flat=True)[:5]),
            ['00', '00', '00', '00', '00'])

        path_field.rebuild_tree()

        self.assertListEqual(
            list(Place.objects.order_by('-path').values_list('path',
                                                             flat=True)[:5]),
            ['2Z', '2Y', '2X', '2W', '2V'])

        with self.assertNumQueries(1):
            with self.assertRaisesMessage(
                    ValueError,
                    '`max_siblings` (%d) has been reached.\n'
                    'You should increase it then rebuild the tree.'
                    % path_field.max_siblings):
                Place.objects.create(name='Anything')

    def test_level(self):
        list(self.create_test_places())

        places = [(p.path.level, p.name) for p in Place.objects.all()]
        self.assertListEqual(places, [
            (1, 'France'), (2, 'Normandie'), (3, 'Eure'), (3, 'Manche'),
            (3, 'Seine-Maritime'), (2, 'Poitou-Charentes'), (3, 'Vienne'),
            (4, 'Poitiers'), (1, 'Österreich')])

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
            self.assertEqual(path.level, None)
        with self.assertNumQueries(0):
            self.assertEqual(path.is_root, None)
        with self.assertNumQueries(0):
            self.assertEqual(path.is_leaf, None)
