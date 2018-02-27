from django.db.migrations.operations.base import Operation

from .sql import postgresql


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


class CreateTreeFunctions(Operation, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        self.check_database_backend(schema_editor)
        for sql_query in postgresql.CREATE_FUNCTIONS_QUERIES:
            schema_editor.execute(sql_query)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        self.check_database_backend(schema_editor)
        for sql_query in postgresql.DROP_FUNCTIONS_QUERIES:
            schema_editor.execute(sql_query)

    def describe(self):
        return 'Creates functions required by django-tree'


class DeleteTreeFunctions(CreateTreeFunctions):
    def database_forwards(self, *args, **kwargs):
        super(DeleteTreeFunctions, self).database_backwards(*args, **kwargs)

    def database_backwards(self, *args, **kwargs):
        super(DeleteTreeFunctions, self).database_forwards(*args, **kwargs)

    def describe(self):
        return 'Deletes functions required by django-tree'


class CreateTreeTrigger(Operation, GetModelMixin, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def __init__(self, model_lookup, path_field='path', parent_field='parent'):
        self.model_lookup = model_lookup
        self.path_field_lookup = path_field
        self.parent_field_lookup = parent_field

    def get_pre_params(self, model):
        meta = model._meta
        pk = meta.pk
        path_field = meta.get_field(self.path_field_lookup)
        path_name = path_field.attname
        order_by = path_field.order_by
        if not (pk.attname in order_by or pk.name in order_by
                or 'pk' in order_by):
            order_by += ('pk',)

        # TODO: Handle related lookups in `order_by`.
        sql_order_by = []
        update_columns = [path_name]
        for field_name in order_by:
            descending = field_name[0] == '-'
            if descending:
                field_name = field_name[1:]
            field = (meta.pk if field_name == 'pk'
                     else meta.get_field(field_name))
            update_columns.append('"%s"' % field.attname)
            sql_order_by.append(
                '\\"%s\\" %s' % (field.attname,
                                 ('DESC' if descending else 'ASC')))

        return dict(
            table=meta.db_table,
            pk=meta.pk.attname,
            parent=meta.get_field(self.parent_field_lookup).attname,
            path=path_name,
            max_siblings=path_field.max_siblings,
            level_size=path_field.level_size,
            update_columns=', '.join(update_columns),
            order_by=", ".join(sql_order_by),
        )

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor,
                          from_state, to_state):
        self.check_database_backend(schema_editor)
        for sql_query in postgresql.CREATE_TRIGGER_QUERIES:
            schema_editor.execute(sql_query.format(
                **self.get_pre_params(self.get_model(app_label, to_state))))

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
