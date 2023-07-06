from django.db.models import Lookup


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '%s = (%s)[:array_length(%s, 1)]' % (lhs, rhs, lhs),
            lhs_params + rhs_params + lhs_params,
        )


class SiblingOf(Lookup):
    lookup_name = 'sibling_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # TODO: Simplify using `trim_array` once support for PostgreSQL < 14
        #       is dropped.
        return (
            '(%s)[:array_length(%s, 1) - 1] = (%s)[:array_length(%s, 1) - 1]'
            % (lhs, lhs, rhs, rhs),
            lhs_params + lhs_params + rhs_params + rhs_params,
        )


class ChildOf(Lookup):
    lookup_name = 'child_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        # TODO: Simplify using `trim_array` once support for PostgreSQL < 14
        #       is dropped.
        return (
            '(%s)[:array_length(%s, 1) - 1] = %s' % (lhs, lhs, rhs),
            lhs_params + lhs_params + rhs_params
        )


class DescendantOf(Lookup):
    lookup_name = 'descendant_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            '(%s)[:array_length(%s, 1)] = %s' % (lhs, rhs, rhs),
            lhs_params + rhs_params + rhs_params,
        )
