from collections.abc import Sequence
from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Lookup
from django.db.models.sql.compiler import SQLCompiler

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
# and works identically on SQLite and MySQL.
#
# `child_of` / `sibling_of` additionally need to keep only *direct* children of a
# prefix Q (depth Q+1), not deeper descendants. On SQLite and MySQL this avoids any
# delimiter *count* (which has no portable, NUL-safe SQL form -- SQLite's `replace`
# mishandles `0x00` bytes): a row is a direct child of Q iff it falls in Q's
# descendant range and the only delimiter past Q is its own trailing one, i.e.
# `instr(substr(path, len(Q) + 1), x'00') = length(path) - len(Q)`. `instr`,
# `substr` and `length` are all byte-correct on SQLite BLOBs and MySQL VARBINARY.
#
# Oracle is the exception: `substr`/`length`/`instr` operate on the hex text of a
# `RAW`, not its bytes (so `instr` would match a `00` straddling two bytes), and
# `UTL_RAW` has no `instr`. There the depth is taken straight from the installed
# `tree_level` helper (see `tree.sql.oracle`): `tree_level(path) = tree_level(Q) + 1`,
# with `tree_level(Q)` precomputed in Python. `UTL_RAW.SUBSTR`/`UTL_RAW.LENGTH` give
# the byte-correct prefix slice the `ancestor_of` lookup needs.


def _as_bytes(value: Any) -> bytes:
    # `get_prep_lookup` has already applied `PathField.get_prep_value`, so `value`
    # is the raw bytes (or empty/None for the virtual root above all roots).
    return bytes(value) if value else b''


def _binds_empty_as_null(connection: BaseDatabaseWrapper, value: bytes) -> bool:
    # Oracle stores a zero-length `RAW` as NULL, so an empty-path bound (the
    # virtual root above all roots) can't drive a `>`/`>=`/`<` comparison there --
    # it would bind as NULL and match nothing. Such a bound always means "no
    # bound" (every stored path is non-empty), so callers drop it on Oracle.
    return connection.vendor == 'oracle' and not value


def _children_of_prefix(
    lhs: str,
    lhs_params: Sequence[Any],
    prefix: bytes,
    connection: BaseDatabaseWrapper,
) -> tuple[str, list[Any]]:
    """SQL selecting the direct children of the constant ``prefix``.

    The descendant range of ``prefix``, restricted to rows one level below it (so
    deeper descendants drop out). On SQLite/MySQL this uses byte-correct
    ``length``/``substr``/``instr``; on Oracle it uses the installed ``tree_level``
    helper against a Python-precomputed target depth. For the empty prefix (the
    virtual root) the upper bound is dropped, selecting every root.
    """
    upper = tree_upper(prefix)
    n = len(prefix)
    clauses: list[str] = []
    params: list[Any] = []
    # The empty virtual-root prefix binds as NULL on Oracle (see
    # `_binds_empty_as_null`); `> ''` matches every row anyway, so drop it there.
    if not _binds_empty_as_null(connection, prefix):
        clauses.append('%s > %%s' % lhs)
        params += [*lhs_params, prefix]
    if upper is not None:
        clauses.append('%s < %%s' % lhs)
        params += [*lhs_params, upper]
    if connection.vendor == 'oracle':
        target_level = (tree_level(prefix) or 0) + 1
        clauses.append('tree_level(%s) = %%s' % lhs)
        params += [*lhs_params, target_level]
    else:
        clauses.append("instr(substr(%s, %%s), x'00') = length(%s) - %%s" % (lhs, lhs))
        params += [*lhs_params, n + 1, *lhs_params, n]
    return ' AND '.join(clauses), params


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        if connection.vendor == 'oracle':
            # `substr`/`length` slice the hex text of a `RAW`, not its bytes;
            # `UTL_RAW` is the byte-correct equivalent. Works with a column RHS too
            # (the `OuterRef` used by `QuerySet.get_descendants`). `UTL_RAW.SUBSTR`
            # raises when the length exceeds the buffer, so cap it: a candidate
            # longer than the target simply can't be its ancestor.
            return (
                '%s = UTL_RAW.SUBSTR(%s, 1, '
                'LEAST(UTL_RAW.LENGTH(%s), UTL_RAW.LENGTH(%s)))' % (lhs, rhs, lhs, rhs),
                [*lhs_params, *rhs_params, *lhs_params, *rhs_params],
            )
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
        if _binds_empty_as_null(connection, path):
            # Virtual root: every stored row is a (strict) descendant. The empty
            # bound binds as NULL on Oracle, so express the unbounded range as a
            # plain non-NULL test instead of `>= ''`.
            return '%s IS NOT NULL' % lhs, [*lhs_params]
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
        return _children_of_prefix(lhs, lhs_params, _as_bytes(self.rhs), connection)


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
        return _children_of_prefix(lhs, lhs_params, parent_path, connection)
