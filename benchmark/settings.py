DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'HOST': 'localhost',
        'PORT': '5432',
        'NAME': 'tree',
        'USER': 'tree',
        'PASSWORD': 'test-only',
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
