from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import IntegerField, Transform
from django.db.models.sql.compiler import SQLCompiler


class Level(Transform):
    lookup_name = 'level'
    # Depth = number of 0x00 level delimiters in the `bytea` path. `tree_level` is
    # the IMMUTABLE helper installed by `CreateTreeTrigger` (and the `tree`
    # migration), so it can back the functional `(level, path)` index too.
    template = 'tree_level(%(expressions)s)'

    @property
    def output_field(self) -> IntegerField:
        return IntegerField()

    def as_sql(
        self,
        compiler: SQLCompiler,
        connection: BaseDatabaseWrapper,
        function: str | None = None,
        template: str | None = None,
        arg_joiner: str | None = None,
        **extra_context: Any,
    ) -> tuple[str, Any]:
        if connection.vendor == 'postgresql':
            return super().as_sql(
                compiler,
                connection,
                function=function,
                template=template,
                arg_joiner=arg_joiner,
                **extra_context,
            )
        # Counting the 0x00 delimiters has no portable, NUL-safe SQL form (SQLite's
        # `replace`/string functions mishandle embedded 0x00), so `__level` as a
        # query filter is PostgreSQL-only. The depth-free `child_of`/`sibling_of`
        # lookups (see `tree.lookups`) and the Python-side `Path.get_level()` /
        # `is_root()` cover the rest off PostgreSQL.
        raise NotImplementedError(
            'The `__level` lookup/transform is only available on PostgreSQL. '
            'Use `get_level()` / `is_root()` on a loaded instance instead.'
        )
