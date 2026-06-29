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

# Keep the historical `AutoField` ids: Django 6.0 changed the implicit default
# to `BigAutoField`, which would otherwise diverge from the existing migrations.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
