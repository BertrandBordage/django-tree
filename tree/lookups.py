from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Lookup
from django.db.models.sql.compiler import SQLCompiler


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


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s = substr(%s, 1, octet_length(%s))' % (lhs, rhs, lhs),
            [*lhs_params, *rhs_params, *lhs_params],
        )


class DescendantOf(Lookup):
    lookup_name = 'descendant_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s >= %s AND %s < tree_upper(%s)' % (lhs, rhs, lhs, rhs),
            [*lhs_params, *rhs_params, *lhs_params, *rhs_params],
        )


class StrictDescendantOf(Lookup):
    lookup_name = 'strict_descendant_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # Same range as `descendant_of`, but the strict lower bound `> P` excludes
        # P itself. Since the path is `UNIQUE`, P matches exactly one row (the node
        # itself), so this returns its strict descendants without an extra depth
        # predicate.
        return (
            '%s > %s AND %s < tree_upper(%s)' % (lhs, rhs, lhs, rhs),
            [*lhs_params, *rhs_params, *lhs_params, *rhs_params],
        )


class ChildOf(Lookup):
    lookup_name = 'child_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s > %s AND %s < tree_upper(%s) '
            'AND tree_level(%s) = tree_level(%s) + 1' % (lhs, rhs, lhs, rhs, lhs, rhs),
            [
                *lhs_params,
                *rhs_params,
                *lhs_params,
                *rhs_params,
                *lhs_params,
                *rhs_params,
            ],
        )


class SiblingOf(Lookup):
    lookup_name = 'sibling_of'

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # The siblings of P are the children of its parent, i.e. the rows in the
        # parent's descendant range at P's depth. `tree_parent_prefix(P)` strips
        # P's own segment (and trailing delimiter), yielding the parent path. For
        # a root that prefix is empty, so `tree_upper` returns NULL and the upper
        # bound is dropped (every other root qualifies).
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
