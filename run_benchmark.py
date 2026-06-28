#!/usr/bin/env python

import argparse
import os

import django


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-django-tree-only', action='store_true')
    parser.add_argument(
        '--checkpoint-step',
        type=int,
        default=100,
        help='Minimum number of new objects between two measurement checkpoints. '
        'The whole tree is still built; a larger value records fewer data points '
        'and runs faster. Defaults to 100.',
    )
    parser.add_argument('selected_tests', nargs='*', type=str)
    args = parser.parse_args()
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark.settings')
    django.setup()
    from benchmark.base import Benchmark

    Benchmark(
        run_django_tree_only=args.run_django_tree_only,
        selected_tests=args.selected_tests,
        checkpoint_step=args.checkpoint_step,
    ).run()
