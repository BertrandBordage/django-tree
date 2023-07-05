from typing import List, Optional, Type

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models import Model

from .base import (
    quote_ident, get_prev_sibling_where_clause, get_next_sibling_where_clause,
    compare_columns, join_and,
)


def execute_format(
    sql: str,
    *args: str,
    using: Optional[List[str]] = None,
    into: Optional[List[str]] = None,
):
    if using is None:
        using = []
    if into is None:
        into = []

    sql = sql.replace("'", "''")

    args = ', ' + (', '.join(args)) if args else ''
    using = ' USING ' + ', '.join(using) if using else ''
    into = ' INTO ' + ', '.join(into) if into else ''
    return f"EXECUTE format('{sql}'{args}){using}{into};"


def get_update_paths_function_creation(
    model: Type[Model], path_field_lookup: str,
):
    meta = model._meta
    pk = meta.pk
    path_field = meta.get_field(path_field_lookup)
    parent_field = path_field.parent_field
    order_by = path_field.order_by
    if not (pk.attname in order_by or pk.name in order_by
            or 'pk' in order_by):
        order_by += ('pk',)

    # TODO: Handle related lookups in `order_by`.
    where_columns = []
    sql_order_by = []
    sql_reversed_order_by = []
    for field_name in order_by:
        descending = field_name[0] == '-'
        if descending:
            field_name = field_name[1:]
        field = (meta.pk if field_name == 'pk'
                 else meta.get_field(field_name))
        quoted_field_name = quote_ident(field.attname)
        if field_name != 'pk':
            where_columns.append(quoted_field_name)
        sql_order_by.append(
            f'{quoted_field_name} {"DESC" if descending else "ASC"}'
        )
        sql_reversed_order_by.append(
            f'{quoted_field_name} {"ASC" if descending else "DESC"}'
        )

    table = meta.db_table
    pk = quote_ident(meta.pk.attname)
    parent = quote_ident(parent_field.attname)
    path = quote_ident(path_field.attname)
    sql_t2_order_by = ', '.join([
        f't2.{ordered_column}' for ordered_column in sql_order_by
    ])
    sql_order_by = ', '.join(sql_order_by)
    sql_reversed_order_by = ', '.join(sql_reversed_order_by)

    rebuild = execute_format(f"""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT {parent}, NULL::decimal[]
                FROM {table}
                WHERE {parent} IS NULL
                LIMIT 1
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    t1.path || row_number() OVER (
                        PARTITION BY t1.pk ORDER BY {sql_t2_order_by}
                    ) - 1
                FROM generate_paths AS t1
                INNER JOIN {table} AS t2 ON (
                    t2.{parent} = t1.pk
                    OR (t1.pk IS NULL AND t2.{parent} IS NULL))
            )
        ), updated AS (
            UPDATE {table} AS t2 SET {path} = t1.path
            FROM generate_paths AS t1
            WHERE t2.{pk} = t1.pk AND t2.{pk} != $1.{pk}
                AND (t2.{path} IS NULL OR t2.{path} != t1.path)
        )
        SELECT path FROM generate_paths
        WHERE pk = $1.{pk}
    """, using=['OLD'], into=[f'NEW.{path}'])

    get_new_parent_path = execute_format(f"""
        SELECT {path} FROM {table} WHERE {pk} = $1.{parent}
    """, using=['NEW'], into=['new_parent_path'])

    get_prev_sibling_decimal = execute_format(f"""
        SELECT {path}[array_length({path}, 1)]
        FROM {table}
        WHERE
            {get_prev_sibling_where_clause(where_columns, '$1')}
            AND {compare_columns(parent, f'$1.{parent}')}
            AND {pk} != $1.{pk}
        ORDER BY {sql_reversed_order_by}
        LIMIT 1
    """, using=['NEW'], into=['prev_sibling_decimal'])

    get_next_sibling_decimal = execute_format(f"""
        SELECT {path}[array_length({path}, 1)]
        FROM {table}
        WHERE
            {get_next_sibling_where_clause(where_columns, '$1')}
            AND {compare_columns(parent, f'$1.{parent}')}
            AND {pk} != $1.{pk}
        ORDER BY {sql_order_by}
        LIMIT 1
    """, using=['NEW'], into=['next_sibling_decimal'])

    update_descendants = execute_format(f"""
        UPDATE {table}
        SET {path} = $1.{path}
            || {path}[array_length($2.{path}, 1) + 1:]
        WHERE {path}[:array_length($2.{path}, 1)] = $2.{path} AND {pk} != $2.{pk}
    """, using=['NEW', 'OLD'])

    row_unchanged = join_and([
        compare_columns(f'OLD.{where_column}', f'NEW.{where_column}')
        for where_column in [parent, *where_columns]
    ])

    # TODO: Add `LIMIT 1` where appropriate to see if it optimises a bit.
    return f"""
        CREATE OR REPLACE FUNCTION update_{table}_{path}_paths() RETURNS trigger AS $$
        DECLARE
            prev_sibling_decimal decimal := NULL;
            next_sibling_decimal decimal := NULL;
            new_parent_path decimal[];
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.{path} = '{{NULL}}'::decimal[] THEN
                    {rebuild}
                    RETURN NEW;
                END IF;

                IF {row_unchanged} THEN
                    RETURN NEW;
                END IF;
            END IF;

            {get_prev_sibling_decimal}
            {get_next_sibling_decimal}

            IF prev_sibling_decimal IS NULL AND next_sibling_decimal IS NULL THEN
                prev_sibling_decimal := 0;
                next_sibling_decimal := 0;
            ELSE
                IF prev_sibling_decimal IS NULL THEN
                    -- We use `- 2` so that the middle between prev and next
                    -- will be next - 1, that way the new lower bound
                    -- is the former lower bound - 1.
                    prev_sibling_decimal := coalesce(next_sibling_decimal, 0) - 2;
                END IF;
                IF next_sibling_decimal IS NULL THEN
                    -- We use `+ 2` so that the middle between prev and next
                    -- will be prev + 1, that way the new upper bound
                    -- is the former upper bound + 1.
                    next_sibling_decimal := coalesce(prev_sibling_decimal, 0) + 2;
                END IF;
            END IF;
            
            -- Preserve the current path when it is still relevant,
            -- even though it might not be at the middle between prev and next.
            IF TG_OP = 'UPDATE'
                AND {compare_columns(f'NEW.{parent}', f'OLD.{parent}')}
                AND OLD.{path}[array_length(OLD.{path}, 1)]
                    BETWEEN prev_sibling_decimal AND next_sibling_decimal
            THEN
                RETURN NEW;
            END IF;
            
            {get_new_parent_path}
            IF TG_OP = 'UPDATE' THEN
                -- TODO: Add this behaviour to the model validation.
                IF new_parent_path[:array_length(OLD.{path}, 1)] = OLD.{path} THEN
                    RAISE 'Cannot set itself or a descendant as parent.';
                END IF;
            END IF;
            
            NEW.{path} = new_parent_path || (
                prev_sibling_decimal + next_sibling_decimal
            ) / 2;

            IF TG_OP = 'UPDATE' THEN
                {update_descendants}
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """


CREATE_TRIGGER_QUERIES = (
    """
    CREATE TRIGGER "update_{path}_before"
    BEFORE INSERT OR UPDATE OF {update_columns}
    ON "{table}"
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE FUNCTION update_{table}_{path}_paths();
    """,
    """
    CREATE OR REPLACE FUNCTION rebuild_{table}_{path}() RETURNS void AS $$
    BEGIN
        UPDATE {table} SET {path} = '{{NULL}}'::decimal[] FROM (
            SELECT * FROM {table}
            WHERE {parent} IS NULL
            LIMIT 1
            FOR UPDATE
        ) AS t
        WHERE {table}.{pk} = t.{pk};
    END;
    $$ LANGUAGE plpgsql;
    """,
    # TODO: Find a way to create this unique constraint
    #       somewhere else.
    """
    ALTER TABLE "{table}"
    ADD CONSTRAINT "{table}_{path}_unique" UNIQUE ("{path}")
    -- FIXME: Remove this `INITIALLY DEFERRED` whenever possible.
    INITIALLY DEFERRED;
    """,
)

DROP_TRIGGER_QUERIES = (
    # TODO: Find a way to delete this unique constraint
    #       somewhere else.
    'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{table}_{path}_unique";'
    'DROP TRIGGER IF EXISTS "update_{path}_before" ON "{table}";',
    'DROP FUNCTION IF EXISTS update_{table}_{path}_paths();',
)


def rebuild(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute(f'SELECT rebuild_{table}_{path_field}();')


def disable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" '
            f'DISABLE TRIGGER "update_{path_field}_before";'
        )


def enable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" '
            f'ENABLE TRIGGER "update_{path_field}_before";'
        )
