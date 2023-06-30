from django.apps import AppConfig

from .fields import PathField
from .lookups import AncestorOf, SiblingOf, ChildOf, DescendantOf


class TreeAppConfig(AppConfig):
    name = 'tree'
    verbose_name = 'Tree'

    def ready(self):
        PathField.register_lookup(AncestorOf)
        PathField.register_lookup(SiblingOf)
        PathField.register_lookup(ChildOf)
        PathField.register_lookup(DescendantOf)
