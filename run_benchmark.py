#!/usr/bin/env python

import argparse
import os

import django


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-django-tree-only', action='store_true')
    args = parser.parse_args()
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark.settings')
    django.setup()
    from benchmark.base import Benchmark
    Benchmark(run_django_tree_only=args.run_django_tree_only).run()
