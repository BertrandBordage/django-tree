import os

# Pick the database backend from `TREE_DB_ENGINE` (postgresql by default) so the
# same test suite runs against PostgreSQL, SQLite and MySQL.
_ENGINE = os.environ.get('TREE_DB_ENGINE', 'postgresql')

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
    # `django.contrib.postgres` is PostgreSQL-only and not needed elsewhere.
    *(('django.contrib.postgres',) if _ENGINE == 'postgresql' else ()),
    'tree',
    'tests',
)

SECRET_KEY = 'not important here'

# Keep the historical `AutoField` ids: Django 6.0 changed the implicit default
# to `BigAutoField`, which would otherwise require a new migration.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
