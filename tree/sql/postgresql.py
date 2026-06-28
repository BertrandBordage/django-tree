from typing import List, Optional, Type

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models import Model

from .base import (
    quote_ident,
    get_prev_sibling_where_clause,
    get_next_sibling_where_clause,
    compare_columns,
    join_and,
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
    model: Type[Model],
    path_field_lookup: str,
):
    meta = model._meta
    pk = meta.pk
    path_field = meta.get_field(path_field_lookup)
    parent_field = path_field.parent_field
    order_by = path_field.order_by
    if not (pk.attname in order_by or pk.name in order_by or 'pk' in order_by):
        order_by = [*order_by, 'pk']

    # TODO: Handle related lookups in `order_by`.
    where_columns = []
    descending_flags = []
    sql_order_by = []
    for field_name in order_by:
        descending = field_name[0] == '-'
        if descending:
            field_name = field_name[1:]
        field = meta.pk if field_name == 'pk' else meta.get_field(field_name)
        quoted_field_name = quote_ident(field.attname)
        where_columns.append(quoted_field_name)
        descending_flags.append(descending)
        sql_order_by.append(f'{quoted_field_name} {"DESC" if descending else "ASC"}')

    function = quote_ident(f'update_{meta.db_table}_{path_field.attname}_paths')
    table = quote_ident(meta.db_table)
    pk = quote_ident(meta.pk.attname)
    parent = quote_ident(parent_field.attname)
    path = quote_ident(path_field.attname)
    # Only the rebuild's recursive CTE and the renumbering still need an explicit
    # ORDER BY; the sibling lookups order implicitly via min/max.
    sql_t2_order_by = ', '.join(
        [f't2.{ordered_column}' for ordered_column in sql_order_by]
    )
    sql_order_by = ', '.join(sql_order_by)

    rebuild = execute_format(
        f"""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT {parent}, NULL::double precision[]
                FROM {table}
                WHERE {parent} IS NULL
                LIMIT 1
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    t1.path || (
                        row_number() OVER (
                            PARTITION BY t1.pk ORDER BY {sql_t2_order_by}
                        ) - 1
                    )::double precision
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
        LIMIT 1
    """,
        using=['OLD'],
        into=[f'NEW.{path}'],
    )

    get_new_parent_path = execute_format(
        f"""
        SELECT {path} FROM {table} WHERE {pk} = $1.{parent}
    """,
        using=['NEW'],
        into=['new_parent_path'],
    )

    # The previous and next sibling values are the last path elements of the
    # two siblings immediately surrounding the new position. A path's last element
    # is strictly monotonic with `order_by` among siblings (the same invariant the
    # read side relies on, e.g. `get_prev_sibling` ordering by `path`), so the
    # previous sibling's value is simply the greatest value among siblings
    # ordered before us, and the next sibling's the smallest among those ordered
    # after. Both therefore come from a single scan of the sibling set using
    # `max(...) FILTER` / `min(...) FILTER`, instead of two separate
    # `ORDER BY ... LIMIT 1` queries.
    get_sibling_values = execute_format(
        f"""
        SELECT
            max({path}[array_length({path}, 1)])
                FILTER (WHERE {get_prev_sibling_where_clause(where_columns, '$1', descending_flags)}),
            min({path}[array_length({path}, 1)])
                FILTER (WHERE {get_next_sibling_where_clause(where_columns, '$1', descending_flags)})
        FROM {table}
        WHERE
            {compare_columns(parent, f'$1.{parent}')}
            AND {pk} != $1.{pk}
    """,
        using=['NEW'],
        into=['prev_sibling_value', 'next_sibling_value'],
    )

    update_descendants = execute_format(
        f"""
        UPDATE {table}
        SET {path} = $1.{path}
            || {path}[array_length($2.{path}, 1) + 1:]
        WHERE {path}[:array_length($2.{path}, 1)] = $2.{path} AND {pk} != $2.{pk}
    """,
        using=['NEW', 'OLD'],
    )

    # Rank of the new node amongst its siblings (number of siblings sorting
    # before it), used as its slot when the siblings are renumbered.
    get_new_sibling_rank = execute_format(
        f"""
        SELECT count(*)
        FROM {table}
        WHERE
            {get_prev_sibling_where_clause(where_columns, '$1', descending_flags)}
            AND {compare_columns(parent, f'$1.{parent}')}
            AND {pk} != $1.{pk}
    """,
        using=['NEW'],
        into=['new_sibling_rank'],
    )

    # Renumber every child of the new node's parent to a consecutive integer
    # value (skipping the slot the new node will take), cascading the change
    # to each sibling's whole subtree. This spaces crammed siblings back out
    # when a gap has been exhausted.
    renumber_siblings = execute_format(
        f"""
        WITH sibling_renumber AS (
            SELECT
                {path} AS old_path,
                (row_number() OVER (ORDER BY {sql_order_by}) - 1)
                    + CASE WHEN {get_prev_sibling_where_clause(where_columns, '$1', descending_flags)}
                        THEN 0 ELSE 1 END AS new_value
            FROM {table}
            WHERE {compare_columns(parent, f'$1.{parent}')}
                AND {pk} != $1.{pk}
                AND array_length({path}, 1) = coalesce(array_length($2, 1), 0) + 1
        )
        UPDATE {table} AS t
        SET {path} = $2 || sr.new_value::double precision
            || t.{path}[coalesce(array_length($2, 1), 0) + 2:]
        FROM sibling_renumber AS sr
        WHERE t.{path}[:coalesce(array_length($2, 1), 0) + 1] = sr.old_path
    """,
        using=['NEW', 'new_parent_path'],
    )

    row_unchanged = join_and(
        [
            compare_columns(f'OLD.{where_column}', f'NEW.{where_column}')
            for where_column in [parent, *where_columns]
        ]
    )

    return f"""
        CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$
        DECLARE
            prev_sibling_value double precision := NULL;
            next_sibling_value double precision := NULL;
            new_sibling_value double precision := NULL;
            new_sibling_rank integer := NULL;
            new_parent_path double precision[];
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.{path} = '{{NULL}}'::double precision[] THEN
                    {rebuild}
                    RETURN NEW;
                END IF;

                IF {row_unchanged} THEN
                    RETURN NEW;
                END IF;
            END IF;

            {get_sibling_values}

            IF prev_sibling_value IS NULL AND next_sibling_value IS NULL THEN
                prev_sibling_value := 0;
                next_sibling_value := 0;
            ELSE
                IF prev_sibling_value IS NULL THEN
                    -- We use `- 2` so that the middle between prev and next
                    -- will be next - 1, that way the new lower bound
                    -- is the former lower bound - 1.
                    prev_sibling_value := coalesce(next_sibling_value, 0) - 2;
                END IF;
                IF next_sibling_value IS NULL THEN
                    -- We use `+ 2` so that the middle between prev and next
                    -- will be prev + 1, that way the new upper bound
                    -- is the former upper bound + 1.
                    next_sibling_value := coalesce(prev_sibling_value, 0) + 2;
                END IF;
            END IF;

            -- Preserve the current path when it is still relevant,
            -- even though it might not be at the middle between prev and next.
            IF TG_OP = 'UPDATE'
                AND {compare_columns(f'NEW.{parent}', f'OLD.{parent}')}
                AND OLD.{path}[array_length(OLD.{path}, 1)]
                    BETWEEN prev_sibling_value AND next_sibling_value
            THEN
                RETURN NEW;
            END IF;

            {get_new_parent_path}
            IF TG_OP = 'UPDATE' THEN
                IF new_parent_path[:array_length(OLD.{path}, 1)] = OLD.{path} THEN
                    RAISE 'Cannot set itself or a descendant as parent.';
                END IF;
            END IF;

            new_sibling_value := (
                prev_sibling_value + next_sibling_value
            ) / 2;
            -- When the midpoint can no longer fall strictly between the two
            -- neighbours, float8 has run out of bisection headroom and the gap
            -- is exhausted: renumber this parent's children to consecutive
            -- integers so siblings are spaced out again, and give the new node
            -- its own slot.
            IF new_sibling_value <= prev_sibling_value
                OR new_sibling_value >= next_sibling_value THEN
                {get_new_sibling_rank}
                {renumber_siblings}
                new_sibling_value := new_sibling_rank;
            END IF;

            NEW.{path} = new_parent_path || new_sibling_value;

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
    ON {table}
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE FUNCTION {function}();
    """,
    """
    CREATE OR REPLACE FUNCTION {rebuild_function}() RETURNS void AS $$
    BEGIN
        UPDATE {table} SET {path} = '{{NULL}}'::double precision[] FROM (
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
    ALTER TABLE {table}
    ADD CONSTRAINT {constraint} UNIQUE ({path})
    -- FIXME: Remove this `INITIALLY DEFERRED` whenever possible.
    INITIALLY DEFERRED;
    """,
)

DROP_TRIGGER_QUERIES = (
    # TODO: Find a way to delete this unique constraint
    #       somewhere else.
    'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint};'
    'DROP TRIGGER IF EXISTS "update_{path}_before" ON {table};',
    'DROP FUNCTION IF EXISTS {function}();',
)


def rebuild(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    rebuild_function = quote_ident(f'rebuild_{table}_{path_field}')
    with connections[db_alias].cursor() as cursor:
        cursor.execute(f'SELECT {rebuild_function}();')


def disable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" DISABLE TRIGGER "update_{path_field}_before";'
        )


def enable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" ENABLE TRIGGER "update_{path_field}_before";'
        )
