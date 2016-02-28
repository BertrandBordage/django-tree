from django.apps import AppConfig

from .fields import PathField
from .lookups import DescendantOf, AncestorOf, Match, MatchAny, Search
from .transforms import Level


class TreeAppConfig(AppConfig):
    name = 'tree'
    verbose_name = 'Tree'

    def ready(self):
        PathField.register_lookup(DescendantOf)
        PathField.register_lookup(AncestorOf)
        PathField.register_lookup(Match)
        PathField.register_lookup(MatchAny)
        PathField.register_lookup(Search)

        PathField.register_lookup(Level)
