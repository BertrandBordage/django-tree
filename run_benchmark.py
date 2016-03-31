#!/usr/bin/env python

import os

import django


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark.settings')
    django.setup()
    from benchmark.base import Benchmark
    Benchmark().run()
