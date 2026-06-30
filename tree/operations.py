from typing import TYPE_CHECKING, Any, cast

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.operations.base import Operation
from django.db.migrations.state import ProjectState
from django.db.models import Field, Model

from .sql import postgresql
from .sql.base import quote_ident

if TYPE_CHECKING:
    from .fields import PathField


class CheckDatabaseMixin:
    def check_database_backend(self, schema_editor: BaseDatabaseSchemaEditor) -> None:
        from .fields import SUPPORTED_VENDORS

        vendor = schema_editor.connection.vendor
        if vendor not in SUPPORTED_VENDORS:
            raise NotImplementedError(
                'django-tree does not support the %r database backend.' % vendor
            )

    def uses_trigger(self, schema_editor: BaseDatabaseSchemaEditor) -> bool:
        # Only PostgreSQL installs a database-side trigger; the other backends
        # maintain the path in Python (see `tree.maintenance`).
        return schema_editor.connection.vendor == 'postgresql'


class GetModelMixin:
    model_lookup: str

    def get_model(self, app_label: str, state: ProjectState) -> type[Model]:
        get_model = state.apps.get_model
        return (
            get_model(self.model_lookup)
            if '.' in self.model_lookup
            else get_model(app_label, self.model_lookup)
        )


class CreateTreeTrigger(Operation, GetModelMixin, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def __init__(self, model_lookup: str, path_field: str = 'path') -> None:
        self.model_lookup = model_lookup
        self.path_field_lookup = path_field

    def get_pre_params(self, model: type[Model]) -> dict[str, str]:
        meta = model._meta
        path_field = cast('PathField', meta.get_field(self.path_field_lookup))
        parent_field = path_field.parent_field
        order_by = path_field.order_by

        # TODO: `order_by` resolves local model fields and `pk` only; related
        #       lookups (e.g. `parent__name`) are not yet supported here.
        path = quote_ident(path_field.attname)
        parent = quote_ident(parent_field.attname)
        # The parent column must be watched too, otherwise re-parenting through
        # a bulk `update(parent=...)` or raw SQL would not fire the trigger.
        update_columns = [path, parent]
        for field_name in order_by:
            descending = field_name[0] == '-'
            if descending:
                field_name = field_name[1:]

            if field_name == 'pk':
                continue

            field = cast(Field, meta.get_field(field_name))
            quoted_field_name = quote_ident(field.attname)
            update_columns.append(quoted_field_name)

        return dict(
            table=quote_ident(meta.db_table),
            pk=quote_ident(meta.pk.attname),
            parent=parent,
            path=path,
            update_columns=', '.join(update_columns),
            function=quote_ident(f'update_{meta.db_table}_{path_field.attname}_paths'),
            rebuild_function=quote_ident(
                f'rebuild_{meta.db_table}_{path_field.attname}'
            ),
            constraint=quote_ident(f'{meta.db_table}_{path_field.attname}_unique'),
        )

    def state_forwards(self, app_label: str, state: ProjectState) -> None:
        pass

    def database_forwards(
        self,
        app_label: str,
        schema_editor: BaseDatabaseSchemaEditor,
        from_state: ProjectState,
        to_state: ProjectState,
    ) -> None:
        self.check_database_backend(schema_editor)
        if not self.uses_trigger(schema_editor):
            return
        model = self.get_model(app_label, to_state)
        # `params=None` runs the SQL without parameter interpolation, so a literal
        # `%` (e.g. the modulo operator) is sent verbatim instead of being read as
        # a placeholder -- no `%`-escaping needed. These statements carry no
        # parameters anyway (every value is a quoted identifier from the model).
        schema_editor.execute(
            postgresql.get_update_paths_function_creation(
                model=model,
                path_field_lookup=self.path_field_lookup,
            ),
            params=None,
        )
        for sql_query in postgresql.CREATE_TRIGGER_QUERIES:
            schema_editor.execute(
                sql_query.format(**self.get_pre_params(model=model)), params=None
            )

    def database_backwards(
        self,
        app_label: str,
        schema_editor: BaseDatabaseSchemaEditor,
        from_state: ProjectState,
        to_state: ProjectState,
    ) -> None:
        self.check_database_backend(schema_editor)
        if not self.uses_trigger(schema_editor):
            return
        for sql_query in postgresql.DROP_TRIGGER_QUERIES:
            schema_editor.execute(
                sql_query.format(
                    **self.get_pre_params(self.get_model(app_label, to_state))
                ),
                params=None,
            )

    def describe(self) -> str:
        return 'Creates a trigger that automatically updates a `PathField`'


class DeleteTreeTrigger(CreateTreeTrigger):
    def database_forwards(self, *args: Any, **kwargs: Any) -> None:
        super(DeleteTreeTrigger, self).database_backwards(*args, **kwargs)

    def database_backwards(self, *args: Any, **kwargs: Any) -> None:
        super(DeleteTreeTrigger, self).database_forwards(*args, **kwargs)

    def describe(self) -> str:
        return 'Deletes the trigger that automatically updates a `PathField`'


class RebuildPaths(Operation, GetModelMixin, CheckDatabaseMixin):
    reversible = True
    atomic = True

    def __init__(self, model_lookup: str, path_field: str = 'path') -> None:
        self.model_lookup = model_lookup
        self.path_field = path_field
        super(RebuildPaths, self).__init__()

    def state_forwards(self, app_label: str, state: ProjectState) -> None:
        pass

    def database_forwards(
        self,
        app_label: str,
        schema_editor: BaseDatabaseSchemaEditor,
        from_state: ProjectState,
        to_state: ProjectState,
    ) -> None:
        self.check_database_backend(schema_editor)
        from .fields import PathField

        model = self.get_model(app_label, to_state)
        field = cast(PathField, model._meta.get_field(self.path_field))
        field.rebuild(db_alias=schema_editor.connection.alias)

    def database_backwards(
        self,
        app_label: str,
        schema_editor: BaseDatabaseSchemaEditor,
        from_state: ProjectState,
        to_state: ProjectState,
    ) -> None:
        self.check_database_backend(schema_editor)

    def describe(self) -> str:
        return 'Rebuilds all the tree structure of a given django-tree field'
