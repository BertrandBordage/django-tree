from django.db.models import IntegerField, Transform


class Level(Transform):
    lookup_name = 'level'
    template = 'array_length(%(expressions)s, 1)'

    @property
    def output_field(self):
        return IntegerField()
