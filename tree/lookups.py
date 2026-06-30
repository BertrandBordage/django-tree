from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Lookup
from django.db.models.sql.compiler import SQLCompiler

from .sql.helpers import tree_parent_prefix, tree_upper


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
# and works identically on SQLite and MySQL.
#
# `child_of` / `sibling_of` additionally need to keep only *direct* children of a
# prefix Q (depth Q+1), not deeper descendants. Off PostgreSQL this avoids any
# delimiter *count* (which has no portable, NUL-safe SQL form -- SQLite's `replace`
# mishandles `0x00` bytes): a row is a direct child of Q iff it falls in Q's
# descendant range and the only delimiter past Q is its own trailing one, i.e.
# `instr(substr(path, len(Q) + 1), x'00') = length(path) - len(Q)`. `instr`,
# `substr` and `length` are all byte-correct on SQLite BLOBs and MySQL VARBINARY.


def _as_bytes(value: Any) -> bytes:
    # `get_prep_lookup` has already applied `PathField.get_prep_value`, so `value`
    # is the raw bytes (or empty/None for the virtual root above all roots).
    return bytes(value) if value else b''


def _children_of_prefix(
    lhs: str, lhs_params: list[Any], prefix: bytes
) -> tuple[str, list[Any]]:
    """Portable SQL selecting the direct children of the constant ``prefix``.

    The descendant range of ``prefix``, restricted to rows whose only delimiter
    past it is their own trailing one (so deeper descendants drop out) -- using
    only byte-correct ``length``/``substr``/``instr`` (SQLite, MySQL). For the
    empty prefix (the virtual root) the upper bound is dropped, selecting every
    root.
    """
    upper = tree_upper(prefix)
    n = len(prefix)
    sql = '%s > %%s' % lhs
    params: list[Any] = [*lhs_params, prefix]
    if upper is not None:
        sql += ' AND %s < %%s' % lhs
        params += [*lhs_params, upper]
    sql += " AND instr(substr(%s, %%s), x'00') = length(%s) - %%s" % (lhs, lhs)
    params += [*lhs_params, n + 1, *lhs_params, n]
    return sql, params


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
        # Direct children of P are the children of the prefix P itself.
        return _children_of_prefix(lhs, lhs_params, _as_bytes(self.rhs))


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
        # The siblings of P are the children of its parent prefix (P itself
        # included; the navigation API excludes self separately).
        parent_path = tree_parent_prefix(_as_bytes(self.rhs))
        return _children_of_prefix(lhs, lhs_params, parent_path)
