from collections import OrderedDict
from string import Formatter

from django.db import DEFAULT_DB_ALIAS, connections


class Arg:
    pattern = '%{position}${format_type}'
    format_types = ('I', 'L', 's')

    def __init__(self, position):
        self.position = position

    def __format__(self, format_spec):
        if format_spec and format_spec not in self.format_types:
            raise ValueError("Unknown format_spec '%s'" % format_spec)
        if not format_spec and self.format_types:
            format_spec = self.format_types[0]
        return self.pattern.format(position=self.position,
                                   format_type=format_spec)


class UsingArg(Arg):
    pattern = '${position}'
    format_types = ()


class AnyArg(OrderedDict):
    arg_class = Arg

    def __init__(self, *args, **kwargs):
        super(AnyArg, self).__init__(*args, **kwargs)
        self.position = 1

    def __getitem__(self, item):
        if item not in self:
            self[item] = self.arg_class(self.position)
            self.position += 1
        return super(AnyArg, self).__getitem__(item)


class AnyUsingArg(AnyArg):
    arg_class = UsingArg


def format_sql_in_function(sql, into=None):
    kwargs = AnyArg({'USING': AnyUsingArg()})
    # TODO: Replace Formatter with sql.format(**kwargs) when dropping Python 2.
    sql = Formatter().vformat(sql, (), kwargs).replace("'", "''")
    using = kwargs.pop('USING')
    args = ', '.join([k for k in kwargs])
    if args:
        args = ', ' + args

    extra = ''
    if into is not None:
        extra += ' INTO ' + ', '.join(into)
    if using:
        extra += ' USING ' + ', '.join([a for a in using])

    return "EXECUTE format('%s'%s)%s;" % (sql, args, extra)


# TODO: Add `LIMIT 1` where appropriate to see if it optimises a bit.
UPDATE_PATHS_FUNCTION = """
    CREATE OR REPLACE FUNCTION update_paths() RETURNS trigger AS $$
    DECLARE
        table_name text := TG_TABLE_NAME;
        pk text := TG_ARGV[0];
        parent text := TG_ARGV[1];
        path text := TG_ARGV[2];
        order_by text[] := TG_ARGV[3];
        reversed_order_by text[] := TG_ARGV[4];
        where_columns text[] := TG_ARGV[5];
        where_column text;
        parent_unchanged bool;
        column_unchanged bool;
        row_unchanged bool := true;
        prev_sibling_where_clause text := NULL;
        prev_sibling_decimal decimal := NULL;
        next_sibling_where_clause text := NULL;
        next_sibling_decimal decimal := NULL;
        order_by_cols text := array_to_string(order_by, ',');
        reversed_order_by_cols text := array_to_string(reversed_order_by, ',');
        order_by_cols2 text := 't2.' || array_to_string(order_by, ',t2.');
        old_path decimal[] := NULL;
        new_path decimal[];
        new_parent_path decimal[];
    BEGIN
        IF TG_OP = 'DELETE' THEN
            -- TODO: Bulk delete descendants of the current row,
            --       if the parent foreign key has `on_delete=CASCADE`
            --       and it is compatible with Django’s Collector.
            --       Do the equivalent with `SET_NULL`.
            RETURN OLD;
        END IF;

        IF TG_OP = 'UPDATE' THEN
            {get_old_path}
            {get_new_path}
            IF new_path = '{{NULL}}'::decimal[] THEN
                {rebuild}
                {set_new_path}
                RETURN NEW;
            END IF;

            -- Optimizations to speed up saving
            -- when the relevant tree data is unchanged.
            {get_parent_unchanged}
            IF parent_unchanged THEN
                FOREACH where_column IN ARRAY where_columns LOOP
                    {get_column_unchanged}
                    IF NOT column_unchanged THEN
                        row_unchanged := false;
                        EXIT;
                    END IF;
                END LOOP;
                IF row_unchanged THEN
                    RETURN NEW;
                END IF;
            END IF;

        END IF;


        {get_new_parent_path}
        IF TG_OP = 'UPDATE' THEN
            -- TODO: Add this behaviour to the model validation.
            IF new_parent_path[:array_length(old_path, 1)] = old_path THEN
                RAISE 'Cannot set itself or a descendant as parent.';
            END IF;
        END IF;
        
        {get_prev_sibling_where_clause}
        {get_prev_sibling_decimal}
        {get_next_sibling_where_clause}
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
        new_path := new_parent_path || (
            prev_sibling_decimal + next_sibling_decimal
        ) / 2;

        IF TG_OP = 'UPDATE' AND new_path != old_path THEN
            {update_descendants}
        END IF;

        {set_new_path}
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """.format(
    get_old_path=format_sql_in_function("""
        SELECT {USING[OLD]}.{path}
    """, into=['old_path']),
    get_new_path=format_sql_in_function(
        'SELECT {USING[NEW]}.{path}', into=['new_path']),
    rebuild=format_sql_in_function("""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT {parent}, NULL::decimal[]
                FROM {table_name}
                WHERE {parent} IS NULL
                LIMIT 1
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    t1.path || row_number() OVER (
                        PARTITION BY t1.pk ORDER BY {order_by_cols2:s}
                    ) - 1
                FROM generate_paths AS t1
                INNER JOIN {table_name} AS t2 ON (
                    t2.{parent} = t1.pk
                    OR (t1.pk IS NULL AND t2.{parent} IS NULL))
            )
        ), updated AS (
            UPDATE {table_name} AS t2 SET {path} = t1.path
            FROM generate_paths AS t1
            WHERE t2.{pk} = t1.pk AND t2.{pk} != {USING[OLD]}.{pk}
                AND (t2.{path} IS NULL OR t2.{path} != t1.path)
        )
        SELECT path FROM generate_paths
        WHERE pk = {USING[OLD]}.{pk}
    """, into=['new_path']),
    get_parent_unchanged=format_sql_in_function("""
        SELECT
            {USING[OLD]}.{parent} IS NULL
            AND {USING[NEW]}.{parent} IS NULL
            OR {USING[OLD]}.{parent} = {USING[NEW]}.{parent}
    """, into=['parent_unchanged']),
    get_column_unchanged=format_sql_in_function("""
        SELECT
            {USING[OLD]}.{where_column} IS NULL
            AND {USING[NEW]}.{where_column} IS NULL
            OR {USING[OLD]}.{where_column} = {USING[NEW]}.{where_column}
    """, into=['column_unchanged']),
    get_new_parent_path=format_sql_in_function("""
        SELECT {path} FROM {table_name} WHERE {pk} = {USING[NEW]}.{parent}
    """, into=['new_parent_path']),
    # FIXME: This clause is wrong when using multiple `order_by` columns!
    # FIXME: The way we quote JSON values here is probably incorrect!
    # FIXME: We should not strip NULLs from the WHERE clause.
    get_prev_sibling_where_clause=format_sql_in_function("""
        SELECT COALESCE(array_to_string(array_agg(column_name || ' <= ' || quote_literal(value)), ' AND '), 'TRUE')
        FROM json_each_text(json_strip_nulls(row_to_json({USING[NEW]}))) AS data(column_name, value)
        INNER JOIN unnest({USING[where_columns]}) AS columns(where_column) ON where_column = column_name
    """, into=['prev_sibling_where_clause']),
    get_prev_sibling_decimal=format_sql_in_function("""
        SELECT {path}[array_length({path}, 1)]
        FROM {table_name}
        WHERE
            {prev_sibling_where_clause:s}
            AND (
                {parent} = {USING[NEW]}.{parent}
                OR ({USING[NEW]}.{parent} IS NULL AND {parent} IS NULL)
            )
            AND {pk} != {USING[NEW]}.{pk}
        ORDER BY {reversed_order_by_cols:s}
        LIMIT 1
    """, into=['prev_sibling_decimal']),
    # FIXME: This clause is wrong when using multiple `order_by` columns!
    # FIXME: The way we quote JSON values here is probably incorrect!
    # FIXME: We should not strip NULLs from the WHERE clause.
    get_next_sibling_where_clause=format_sql_in_function("""
        SELECT COALESCE(array_to_string(array_agg(column_name || ' >= ' || quote_literal(value)), ' AND '), 'TRUE')
        FROM json_each_text(json_strip_nulls(row_to_json({USING[NEW]}))) AS data(column_name, value)
        INNER JOIN unnest({USING[where_columns]}) AS columns(where_column) ON where_column = column_name
    """, into=['next_sibling_where_clause']),
    get_next_sibling_decimal=format_sql_in_function("""
        SELECT {path}[array_length({path}, 1)]
        FROM {table_name}
        WHERE
            {next_sibling_where_clause:s}
            AND (
                {parent} = {USING[NEW]}.{parent}
                OR ({USING[NEW]}.{parent} IS NULL AND {parent} IS NULL)
            )
            AND {pk} != {USING[NEW]}.{pk}
        ORDER BY {order_by_cols:s}
        LIMIT 1
    """, into=['next_sibling_decimal']),
    update_descendants=format_sql_in_function("""
        UPDATE {table_name}
        SET {path} = {USING[new_path]}
            || {path}[array_length({USING[OLD]}.{path}, 1) + 1:]
        WHERE {path}[:array_length({USING[OLD]}.{path}, 1)] = {USING[OLD]}.{path} AND {pk} != {USING[OLD]}.{pk}
    """),
    set_new_path=format_sql_in_function("""
        SELECT *
        FROM json_populate_record({USING[NEW]},
                                  '{{"{path:s}": "{new_path:s}"}}'::json)
    """, into=['NEW']),
)


REBUILD_PATHS_FUNCTION = """
    CREATE OR REPLACE FUNCTION rebuild_paths(
        table_name text, pk text, parent text, path text) RETURNS void AS $$
    BEGIN
        {}
    END;
    $$ LANGUAGE plpgsql;
    """.format(
    format_sql_in_function("""
        UPDATE {table_name} SET {path} = '{{NULL}}'::decimal[] FROM (
            SELECT * FROM {table_name}
            WHERE {parent} IS NULL
            LIMIT 1
            FOR UPDATE
        ) AS t
        WHERE {table_name}.{pk} = t.{pk}
    """),
)


CREATE_FUNCTIONS_QUERIES = (
    UPDATE_PATHS_FUNCTION,
    REBUILD_PATHS_FUNCTION,
)
# We escape the modulo operator '%' otherwise Django considers it
# as a placeholder for a parameter.
CREATE_FUNCTIONS_QUERIES = [s.replace('%', '%%')
                            for s in CREATE_FUNCTIONS_QUERIES]


DROP_FUNCTIONS_QUERIES = (
    """
    DROP FUNCTION IF EXISTS rebuild_paths(table_name text, pk text,
                                          parent text, path text);
    """,
    'DROP FUNCTION IF EXISTS update_paths();',
)

CREATE_TRIGGER_QUERIES = (
    """
    CREATE TRIGGER "update_{path}_before"
    BEFORE INSERT OR UPDATE OF {update_columns}
    ON "{table}"
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE PROCEDURE update_paths(
        '{pk}', '{parent}', '{path}',
        '{{{order_by}}}', '{{{reversed_order_by}}}', '{{{where_columns}}}'
    );
    """,
    """
    CREATE TRIGGER "update_{path}_after"
    AFTER DELETE
    ON "{table}"
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE PROCEDURE update_paths(
        '{pk}', '{parent}', '{path}',
        '{{{order_by}}}', '{{{reversed_order_by}}}', '{{{where_columns}}}'
    );
    """,
    """
    CREATE OR REPLACE FUNCTION rebuild_{table}_{path}() RETURNS void AS $$
    BEGIN
        PERFORM rebuild_paths('{table}', '{pk}', '{parent}', '{path}');
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
    'DROP TRIGGER IF EXISTS "update_{path}_after" ON "{table}";',
    'DROP TRIGGER IF EXISTS "update_{path}_before" ON "{table}";',
    'DROP FUNCTION IF EXISTS rebuild_{table}_{path}();',
)


def rebuild(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('SELECT rebuild_{}_{}();'.format(table, path_field))


def disable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('ALTER TABLE "{}" DISABLE TRIGGER "update_{}_after";'
                       .format(table, path_field))
        cursor.execute('ALTER TABLE "{}" DISABLE TRIGGER "update_{}_before";'
                       .format(table, path_field))


def enable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('ALTER TABLE "{}" ENABLE TRIGGER "update_{}_before";'
                       .format(table, path_field))
        cursor.execute('ALTER TABLE "{}" ENABLE TRIGGER "update_{}_after";'
                       .format(table, path_field))
