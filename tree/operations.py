from django.db.migrations.operations.base import Operation

from .sql import postgresql
from .sql.base import quote_ident


class CheckDatabaseMixin:
    def check_database_backend(self, schema_editor):
        if schema_editor.connection.vendor != 'postgresql':
            raise NotImplementedError(
                'django-tree is only for PostgreSQL for now.')


class GetModelMixin:
    def get_model(self, app_label, state):
        get_model = state.apps.get_model
        return (get_model(self.model_lookup) if '.' in self.model_lookup
                else get_model(app_label, self.model_lookup))


class CreateTreeTrigger(Operation, GetModelMixin, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def __init__(self, model_lookup, path_field='path'):
        self.model_lookup = model_lookup
        self.path_field_lookup = path_field

    def get_pre_params(self, model):
        meta = model._meta
        path_field = meta.get_field(self.path_field_lookup)
        parent_field = path_field.parent_field
        order_by = path_field.order_by

        # TODO: Handle related lookups in `order_by`.
        path = quote_ident(path_field.attname)
        update_columns = [path]
        for field_name in order_by:
            descending = field_name[0] == '-'
            if descending:
                field_name = field_name[1:]

            if field_name == 'pk':
                continue

            quoted_field_name = quote_ident(meta.get_field(field_name).attname)
            update_columns.append(quoted_field_name)

        return dict(
            table=quote_ident(meta.db_table),
            pk=quote_ident(meta.pk.attname),
            parent=quote_ident(parent_field.attname),
            path=path,
            update_columns=', '.join(update_columns),
        )

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor,
                          from_state, to_state):
        self.check_database_backend(schema_editor)
        model = self.get_model(app_label, to_state)
        # We escape the modulo operator '%' otherwise Django considers it
        # as a placeholder for a parameter.
        schema_editor.execute(
            postgresql.get_update_paths_function_creation(
                model=model,
                path_field_lookup=self.path_field_lookup,
            ).replace('%', '%%')
        )
        for sql_query in postgresql.CREATE_TRIGGER_QUERIES:
            schema_editor.execute(sql_query.format(
                **self.get_pre_params(model=model)))

    def database_backwards(self, app_label, schema_editor,
                           from_state, to_state):
        self.check_database_backend(schema_editor)
        for sql_query in postgresql.DROP_TRIGGER_QUERIES:
            schema_editor.execute(sql_query.format(
                **self.get_pre_params(self.get_model(app_label, to_state))))

    def describe(self):
        return 'Creates a trigger that automatically updates a `PathField`'


class DeleteTreeTrigger(CreateTreeTrigger):
    def database_forwards(self, *args, **kwargs):
        super(DeleteTreeTrigger, self).database_backwards(*args, **kwargs)

    def database_backwards(self, *args, **kwargs):
        super(DeleteTreeTrigger, self).database_forwards(*args, **kwargs)

    def describe(self):
        return 'Deletes the trigger that automatically updates a `PathField`'


class RebuildPaths(Operation, GetModelMixin, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def __init__(self, model_lookup, path_field='path'):
        self.model_lookup = model_lookup
        self.path_field = path_field
        super(RebuildPaths, self).__init__()

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor,
                          from_state, to_state):
        self.check_database_backend(schema_editor)
        model = self.get_model(app_label, to_state)
        postgresql.rebuild(model._meta.db_table, self.path_field,
                           db_alias=schema_editor.connection.alias)

    def database_backwards(self, app_label, schema_editor,
                           from_state, to_state):
        self.check_database_backend(schema_editor)

    def describe(self):
        return 'Rebuilds all the tree structure of a given django-tree field'
