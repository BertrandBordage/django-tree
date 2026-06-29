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
    'django.contrib.postgres',
    'tree',
    'tests',
)

SECRET_KEY = 'not important here'

# Keep the historical `AutoField` ids: Django 6.0 changed the implicit default
# to `BigAutoField`, which would otherwise require a new migration.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
