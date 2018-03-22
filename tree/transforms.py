import django
from django.db.models import IntegerField, Transform


class Level(Transform):
    lookup_name = 'level'
    function = 'length'

    if django.VERSION < (1, 9):
        def as_sql(self, compiler, connection):
            lhs, params = compiler.compile(self.lhs)
            return self.template % {'function': self.function,
                                    'expressions': lhs}, params

    @property
    def template(self):
        return ('%%(function)s(%%(expressions)s) / %s'
                % self.lhs.output_field.level_size)

    @property
    def output_field(self):
        return IntegerField()
