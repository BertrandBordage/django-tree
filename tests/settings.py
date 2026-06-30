import os
from typing import Any

# Pick the database backend from `TREE_DB_ENGINE` (postgresql by default) so the
# same test suite runs against PostgreSQL, SQLite and MySQL.
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
elif _ENGINE == 'oracle':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.oracle',
            # `NAME` is an Easy Connect DSN (`host:port/service`); leaving `PORT`
            # empty makes Django use it verbatim, so a PDB *service* name works
            # (`makedsn` would treat `NAME` as a SID and fail against the gvenzl
            # `FREEPDB1` service). Connect as a DBA so the test runner can create
            # the test user/tablespace.
            'NAME': os.environ.get('TREE_DB_NAME', 'localhost:1521/FREEPDB1'),
            'USER': os.environ.get('TREE_DB_USER', 'system'),
            'PASSWORD': os.environ.get('TREE_DB_PASSWORD', 'test-only'),
            'TEST': {
                'USER': os.environ.get('TREE_TEST_DB_USER', 'tree'),
                'PASSWORD': os.environ.get('TREE_TEST_DB_PASSWORD', 'test-only'),
            },
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
    # `django.contrib.postgres` is PostgreSQL-only and not needed elsewhere.
    *(('django.contrib.postgres',) if _ENGINE == 'postgresql' else ()),
    'tree',
    'tests',
)

SECRET_KEY = 'not important here'

# Keep the historical `AutoField` ids: Django 6.0 changed the implicit default
# to `BigAutoField`, which would otherwise require a new migration.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
