from django.db.models import IntegerField, Transform


class Level(Transform):
    lookup_name = 'level'
    # Depth = number of 0x00 level delimiters in the `bytea` path. `tree_level` is
    # the IMMUTABLE helper installed by `CreateTreeTrigger` (and the `tree`
    # migration), so it can back the functional `(level, path)` index too.
    template = 'tree_level(%(expressions)s)'

    @property
    def output_field(self):
        return IntegerField()
