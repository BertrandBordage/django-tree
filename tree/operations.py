from django.db.migrations.operations.base import Operation

from .sql import postgresql


class CreateTreeFunctions(Operation):
    reversible = True
    atomic = True

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if schema_editor.connection.vendor != 'postgresql':
            raise NotImplementedError
        for sql_query in postgresql.CREATE_FUNCTIONS_QUERIES:
            schema_editor.execute(sql_query)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if schema_editor.connection.vendor != 'postgresql':
            raise NotImplementedError
        for sql_query in postgresql.DROP_FUNCTIONS_QUERIES:
            schema_editor.execute(sql_query)

    def describe(self):
        return 'Create functions & extensions required by django-tree'
