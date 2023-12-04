import os

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "NAME": "tree",
        "USER": os.getenv("POSTGRES_USER", "tree"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", None),
        "PORT": 5432,
    },
}

INSTALLED_APPS = (
    "tree",
    "tests",
)

SECRET_KEY = "not important here"
