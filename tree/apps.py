from django.apps import AppConfig

from .fields import PathField
from .lookups import AncestorOf, SiblingOf, ChildOf, DescendantOf
from .transforms import Level
from .types import Path


class TreeAppConfig(AppConfig):
    name = 'tree'
    verbose_name = 'Tree'

    def ready(self):
        Path.register_psycopg()

        PathField.register_lookup(AncestorOf)
        PathField.register_lookup(SiblingOf)
        PathField.register_lookup(ChildOf)
        PathField.register_lookup(DescendantOf)

        PathField.register_lookup(Level)

        # Loads signals
        from . import signals  # noqa: F401
