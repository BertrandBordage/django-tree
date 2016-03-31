DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'tree',
        'USER': 'tree',
    },
}

INSTALLED_APPS = (
    'tree',
    'mptt',
    'treebeard',

    'benchmark',
)

SECRET_KEY = 'not important here'

DATABASE_ROUTERS = ('benchmark.router.AutoRouter',)
