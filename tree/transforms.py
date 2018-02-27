from django.db.models import IntegerField, Transform


class Level(Transform):
    lookup_name = 'level'
    function = 'length'

    @property
    def template(self):
        return ('%%(function)s(%%(expressions)s) / %s'
                % self.lhs.output_field.level_size)

    @property
    def output_field(self):
        return IntegerField()
