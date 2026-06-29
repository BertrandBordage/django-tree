from typing import cast

from django.db.models import Model
from django.forms import ModelChoiceField

from .models import TreeModelMixin


class TreeChoiceField(ModelChoiceField):
    def label_from_instance(self, obj: Model) -> str:
        node = cast(TreeModelMixin, obj)
        if node.is_root():
            return str(obj)
        level = node.get_level() or 0
        return '%s %s' % ('──' * (level - 1), obj)
