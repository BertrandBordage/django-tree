from django.db.models import IntegerField, Transform


class Level(Transform):
    lookup_name = 'level'
    function = 'nlevel'

    @property
    def output_field(self):
        return IntegerField()
