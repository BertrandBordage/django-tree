from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import IntegerField, Transform
from django.db.models.sql.compiler import SQLCompiler

from .sql.base import path_level_sql


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
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        if connection.vendor == 'postgresql':
            return super().as_sql(compiler, connection)
        # SQLite reuses the `tree_level` name (a per-connection Python UDF); MySQL
        # counts the 0x00 bytes inline. Both back the functional `(level, path)`
        # index used by the same `Index` definition.
        lhs, params = compiler.compile(self.lhs)
        sql, repeats = path_level_sql(connection, lhs)
        return sql, list(params) * repeats
