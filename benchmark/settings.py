import os
from typing import Any

# Pick the database backend from `TREE_DB_ENGINE` (postgresql by default), the
# same switch as the test suite, so the benchmark can also run on SQLite/MySQL.
_ENGINE = os.environ.get('TREE_DB_ENGINE', 'postgresql')

DATABASES: dict[str, dict[str, Any]]

if _ENGINE == 'sqlite':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.environ.get('TREE_DB_NAME', ':memory:'),
        },
    }
elif _ENGINE == 'mysql':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'HOST': os.environ.get('TREE_DB_HOST', '127.0.0.1'),
            'PORT': os.environ.get('TREE_DB_PORT', '3306'),
            'NAME': os.environ.get('TREE_DB_NAME', 'tree'),
            'USER': os.environ.get('TREE_DB_USER', 'tree'),
            'PASSWORD': os.environ.get('TREE_DB_PASSWORD', 'test-only'),
            'OPTIONS': {'charset': 'utf8mb4'},
        },
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'HOST': os.environ.get('TREE_DB_HOST', 'localhost'),
            'PORT': os.environ.get('TREE_DB_PORT', '5432'),
            'NAME': os.environ.get('TREE_DB_NAME', 'tree'),
            'USER': os.environ.get('TREE_DB_USER', 'tree'),
            'PASSWORD': os.environ.get('TREE_DB_PASSWORD', 'test-only'),
        },
    }

INSTALLED_APPS = (
    'tree',
    'mptt',
    'treebeard',
    'treenode',
    'tree_queries',
    'benchmark',
)

SECRET_KEY = 'not important here'

DATABASE_ROUTERS = ('benchmark.router.AutoRouter',)

# Keep the historical `AutoField` ids: Django 6.0 changed the implicit default
# to `BigAutoField`, which would otherwise diverge from the existing migrations.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
