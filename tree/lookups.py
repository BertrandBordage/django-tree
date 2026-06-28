from django.db.models import Lookup


# The descendant/child/sibling lookups are expressed as range comparisons on the
# whole path so that they use the btree index backing the path (the `UNIQUE`
# constraint created by `CreateTreeTrigger`), instead of slicing the column
# (which is not sargable and forced dedicated slice indexes).
#
# The descendants of a path P are exactly the rows whose path falls in
# `[P, P || {Infinity})`: appending any element keeps the prefix and stays below
# `P || {Infinity}`, while the next sibling (a larger value at P's last level)
# sits above it. `Infinity` only exists for floating-point arrays, which is why
# this relies on the `double precision` base type.
INFINITY = "ARRAY['Infinity']::double precision[]"


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s = (%s)[:array_length(%s, 1)]' % (lhs, rhs, lhs),
            lhs_params + rhs_params + lhs_params,
        )


class DescendantOf(Lookup):
    lookup_name = 'descendant_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s >= %s AND %s < %s || %s' % (lhs, rhs, lhs, rhs, INFINITY),
            lhs_params + rhs_params + lhs_params + rhs_params,
        )


class StrictDescendantOf(Lookup):
    lookup_name = 'strict_descendant_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # Same range as `descendant_of`, but the strict lower bound `> P` excludes
        # P itself. Since the path is `UNIQUE`, P matches exactly one row (the node
        # itself), so this returns its strict descendants without the extra
        # `array_length(...) > N` predicate.
        return (
            '%s > %s AND %s < %s || %s' % (lhs, rhs, lhs, rhs, INFINITY),
            lhs_params + rhs_params + lhs_params + rhs_params,
        )


class ChildOf(Lookup):
    lookup_name = 'child_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s > %s AND %s < %s || %s '
            'AND array_length(%s, 1) = array_length(%s, 1) + 1'
            % (lhs, rhs, lhs, rhs, INFINITY, lhs, rhs),
            lhs_params + rhs_params + lhs_params + rhs_params + lhs_params + rhs_params,
        )


class SiblingOf(Lookup):
    lookup_name = 'sibling_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # The siblings of P are the children of its parent, i.e. the rows sharing
        # the prefix `P[:-1]` and having the same depth as P.
        # TODO: Simplify the parent slice using `trim_array`
        #       once support for PostgreSQL < 14 is dropped.
        parent = '(%s)[:array_length(%s, 1) - 1]' % (rhs, rhs)
        return (
            '%s > %s AND %s < %s || %s '
            'AND array_length(%s, 1) = array_length(%s, 1)'
            % (lhs, parent, lhs, parent, INFINITY, lhs, rhs),
            lhs_params
            + rhs_params * 2
            + lhs_params
            + rhs_params * 2
            + lhs_params
            + rhs_params,
        )
