from django.db.models import Lookup


class AncestorOf(Lookup):
    lookup_name = 'ancestor_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return "%s LIKE %s || '%%%%'" % (rhs, lhs), rhs_params + lhs_params


class SiblingOf(Lookup):
    lookup_name = 'sibling_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        level_size = self.lhs.output_field.level_size
        return "%s LIKE left(%s, %s) || '%s'" % (
            lhs, rhs, -level_size, '_' * level_size,
        ), lhs_params + rhs_params


class ChildOf(Lookup):
    lookup_name = 'child_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return "%s LIKE %s || '%s'" % (
            lhs, rhs, '_' * self.lhs.output_field.level_size,
        ), lhs_params + rhs_params


class DescendantOf(Lookup):
    lookup_name = 'descendant_of'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return "%s LIKE %s || '%%%%'" % (lhs, rhs), lhs_params + rhs_params
