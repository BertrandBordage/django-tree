DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'tree',
        'USER': 'tree',
    },
}

INSTALLED_APPS = (
    'tree',
    'tests',
)

SECRET_KEY = 'not important here'
