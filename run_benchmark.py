#!/usr/bin/env python

import argparse
import os

import django


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-django-tree-only', action='store_true')
    parser.add_argument('--db-optimization-interval', type=int, default=100)
    parser.add_argument('selected_tests', nargs='*', type=str)
    args = parser.parse_args()
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark.settings')
    django.setup()
    from benchmark.base import Benchmark

    Benchmark(
        run_django_tree_only=args.run_django_tree_only,
        db_optimization_interval=args.db_optimization_interval,
        selected_tests=args.selected_tests,
    ).run()
