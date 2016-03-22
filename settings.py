DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'tree',
        'USER': 'tree',
    },
}

MIGRATION_MODULES = {
    'tree': 'tree.tests.migrations',
}

INSTALLED_APPS = (
    'tree',
)

SECRET_KEY = 'not important here'
