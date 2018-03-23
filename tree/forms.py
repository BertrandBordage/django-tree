from django.forms import ModelChoiceField


class TreeChoiceField(ModelChoiceField):
    def label_from_instance(self, obj):
        if obj.is_root():
            return str(obj)
        return '%s %s' % ('──' * (obj.get_level()-1), obj)
