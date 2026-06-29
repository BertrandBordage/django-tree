from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Lookup
from django.db.models.sql.compiler import SQLCompiler

from .sql.base import path_level_sql
from .sql.helpers import tree_level, tree_parent_prefix, tree_upper


# The descendant/child/sibling lookups are expressed as range comparisons on the
# whole path so that they use the btree index backing the path (the `UNIQUE`
# constraint created by `CreateTreeTrigger`), instead of slicing the column
# (which is not sargable and forced dedicated slice indexes).
#
# A path is `<segment> 0x00` per level, with `0x00` the reserved level delimiter.
# The descendants of a path P are exactly the rows whose path falls in
# `[P, tree_upper(P))`: `tree_upper(P)` drops P's trailing `0x00` delimiter and
# appends `0x01`. Every descendant continues with the `0x00` delimiter (which
# sorts below `0x01`), while the next sibling has a larger byte within P's last
# segment and sorts at or above it. `0x01` thus plays the role the float
# `Infinity` upper bound used to. `tree_upper` is the IMMUTABLE helper installed
# with the trigger, so it stays a constant per query (index-friendly), and it
# returns NULL for the empty prefix (the virtual root), leaving the range
# unbounded above.
#
# On PostgreSQL the lookups call the PL/pgSQL helpers (`tree_upper`,
# `tree_level`, `tree_parent_prefix`) -- that SQL is kept verbatim. On every other
# backend there is no trigger and no such function: when the right-hand operand is
# a constant path (the case for the whole tree-navigation API) the helper is
# precomputed in Python and bound as a parameter, which is just as index-friendly
# and works identically on SQLite and MySQL. The depth predicate of `child_of` /
# `sibling_of` uses `path_level_sql` (a `tree_level` UDF on SQLite, an inline byte
# count on MySQL).


def _as_bytes(value: Any) -> bytes:
    # `get_prep_lookup` has already applied `PathField.get_prep_value`, so `value`
    # is the raw bytes (or empty/None for the virtual root above all roots).
    return bytes(value) if value else b''


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # `length` returns the byte count for bytea/BLOB/VARBINARY alike, so this
        # works on every backend (also with a column right-hand operand, e.g. the
        # `OuterRef` used by `QuerySet.get_descendants`).
        length = 'octet_length' if connection.vendor == 'postgresql' else 'length'
        return (
            '%s = substr(%s, 1, %s(%s))' % (lhs, rhs, length, lhs),
            [*lhs_params, *rhs_params, *lhs_params],
        )


class DescendantOf(Lookup):
    lookup_name = 'descendant_of'
    strict = False

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        operator = '>' if self.strict else '>='
        if connection.vendor == 'postgresql':
            rhs, rhs_params = self.process_rhs(compiler, connection)
            return (
                '%s %s %s AND %s < tree_upper(%s)' % (lhs, operator, rhs, lhs, rhs),
                [*lhs_params, *rhs_params, *lhs_params, *rhs_params],
            )
        if not self.rhs_is_direct_value():
            raise NotImplementedError(
                'The `%s` lookup only supports a constant path off PostgreSQL.'
                % self.lookup_name
            )
        path = _as_bytes(self.rhs)
        upper = tree_upper(path)
        if upper is None:
            return '%s %s %%s' % (lhs, operator), [*lhs_params, path]
        return (
            '%s %s %%s AND %s < %%s' % (lhs, operator, lhs),
            [*lhs_params, path, *lhs_params, upper],
        )


class StrictDescendantOf(DescendantOf):
    lookup_name = 'strict_descendant_of'
    # Same range as `descendant_of`, but the strict lower bound `> P` excludes P
    # itself. Since the path is `UNIQUE`, P matches exactly one row (the node
    # itself), so this returns its strict descendants without an extra depth
    # predicate.
    strict = True


class ChildOf(Lookup):
    lookup_name = 'child_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        if connection.vendor == 'postgresql':
            rhs, rhs_params = self.process_rhs(compiler, connection)
            return (
                '%s > %s AND %s < tree_upper(%s) '
                'AND tree_level(%s) = tree_level(%s) + 1'
                % (lhs, rhs, lhs, rhs, lhs, rhs),
                [
                    *lhs_params,
                    *rhs_params,
                    *lhs_params,
                    *rhs_params,
                    *lhs_params,
                    *rhs_params,
                ],
            )
        if not self.rhs_is_direct_value():
            raise NotImplementedError(
                'The `child_of` lookup only supports a constant path off PostgreSQL.'
            )
        path = _as_bytes(self.rhs)
        upper = tree_upper(path)
        level = tree_level(path)
        assert level is not None
        level_sql, repeats = path_level_sql(connection, lhs)
        sql = '%s > %%s' % lhs
        params = [*lhs_params, path]
        if upper is not None:
            sql += ' AND %s < %%s' % lhs
            params += [*lhs_params, upper]
        sql += ' AND %s = %%s' % level_sql
        params += [*(lhs_params * repeats), level + 1]
        return sql, params


class SiblingOf(Lookup):
    lookup_name = 'sibling_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        if connection.vendor == 'postgresql':
            rhs, rhs_params = self.process_rhs(compiler, connection)
            # The siblings of P are the children of its parent, i.e. the rows in
            # the parent's descendant range at P's depth. `tree_parent_prefix(P)`
            # strips P's own segment (and trailing delimiter), yielding the parent
            # path. For a root that prefix is empty, so `tree_upper` returns NULL
            # and the upper bound is dropped (every other root qualifies).
            parent = 'tree_parent_prefix(%s)' % rhs
            return (
                '%s > %s AND (tree_upper(%s) IS NULL OR %s < tree_upper(%s)) '
                'AND tree_level(%s) = tree_level(%s)'
                % (lhs, parent, parent, lhs, parent, lhs, rhs),
                [
                    *lhs_params,
                    *rhs_params,
                    *rhs_params,
                    *lhs_params,
                    *rhs_params,
                    *lhs_params,
                    *rhs_params,
                ],
            )
        if not self.rhs_is_direct_value():
            raise NotImplementedError(
                'The `sibling_of` lookup only supports a constant path off PostgreSQL.'
            )
        path = _as_bytes(self.rhs)
        parent_path = tree_parent_prefix(path)
        upper = tree_upper(parent_path)
        level = tree_level(path)
        level_sql, repeats = path_level_sql(connection, lhs)
        sql = '%s > %%s' % lhs
        params = [*lhs_params, parent_path]
        if upper is not None:
            sql += ' AND %s < %%s' % lhs
            params += [*lhs_params, upper]
        sql += ' AND %s = %%s' % level_sql
        params += [*(lhs_params * repeats), level]
        return sql, params
