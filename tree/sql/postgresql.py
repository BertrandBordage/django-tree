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
        order_by_cols text := array_to_string(order_by, ',');
        order_by_cols2 text := 't2.' || array_to_string(order_by, ',t2.');
        old_path decimal[] := NULL;
        new_path decimal[];
        old_parent_path decimal[] := NULL;
        new_parent_path decimal[];
        parent_changed boolean;
    BEGIN
        IF TG_OP != 'INSERT' THEN
            {get_old_paths}
        END IF;

        IF TG_OP = 'DELETE' THEN
            {update_old_siblings}
            RETURN OLD;
        END IF;

        IF TG_OP = 'UPDATE' THEN
            {get_new_path}
            IF new_path = '{{NULL}}'::decimal[] THEN
                {rebuild}
                {set_new_path}
                RETURN NEW;
            END IF;
        END IF;


        {get_new_parent_path}
        IF TG_OP = 'UPDATE' THEN
            -- TODO: Add this behaviour to the model validation.
            IF new_parent_path[:array_length(old_path, 1)] = old_path THEN
                RAISE 'Cannot set itself or a descendant as parent.';
            END IF;
        END IF;

        IF TG_OP = 'INSERT' THEN
            parent_changed := TRUE;
        ELSE
            {get_parent_changed}
        END IF;

        IF parent_changed THEN
            IF TG_OP = 'UPDATE' THEN
                {update_old_siblings}
            END IF;
        END IF;

        {get_new_parent_path}

        {update_new_siblings}
        {set_new_path}
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """.format(
    get_old_paths=format_sql_in_function("""
        SELECT {USING[OLD]}.{path},
               trim_array({USING[OLD]}.{path}, 1)
    """, into=['old_path', 'old_parent_path']),
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
                    array_append(
                        t1.path, 
                        row_number() OVER (PARTITION BY t1.pk
                                           ORDER BY {order_by_cols2:s}) - 1
                    )
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
    update_old_siblings=format_sql_in_function("""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT
                    {pk},
                    array_append(
                        {USING[old_parent_path]},
                        {path}[array_length({path}, 1)] - 1
                    )
                FROM {table_name}
                WHERE
                    {path} > {USING[old_path]}
                    AND trim_array({path}, 1) = {USING[old_parent_path]}
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    t1.path || t2.{path}[array_length(t1.path, 1) + 1:]
                FROM generate_paths AS t1
                INNER JOIN {table_name} AS t2 ON t2.{parent} = t1.pk
            )
        )
        UPDATE {table_name} AS t2 SET {path} = t1.path
        FROM generate_paths AS t1
        WHERE t2.{pk} = t1.pk
    """),
    get_parent_changed=format_sql_in_function("""
        SELECT COALESCE({USING[OLD]}.{parent} != {USING[NEW]}.{parent}, TRUE)
    """, into=['parent_changed']),
    get_new_parent_path=format_sql_in_function("""
        SELECT {path} FROM {table_name} WHERE {pk} = {USING[NEW]}.{parent}
    """, into=['new_parent_path']),
    update_new_siblings=format_sql_in_function("""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT
                    {pk},
                    array_append(
                        {USING[new_parent_path]},
                        row_number() OVER (ORDER BY {order_by_cols:s}) - 1
                    )
                FROM ((
                        SELECT *
                        FROM {table_name}
                        WHERE (
                            {parent} = {USING[NEW]}.{parent}
                            OR ({USING[NEW]}.{parent} IS NULL
                                AND {parent} IS NULL)
                        ) AND COALESCE({pk} != {USING[NEW]}.{pk}, TRUE)
                    ) UNION ALL (
                        SELECT {USING[NEW]}.*
                    )
                ) AS t
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    array_append(
                        t1.path,
                        row_number() OVER (PARTITION BY t1.pk
                                           ORDER BY {order_by_cols2:s}) - 1
                    )
                FROM generate_paths AS t1
                INNER JOIN {table_name} AS t2 ON t2.{parent} = t1.pk
            )
        ), updated AS (
            UPDATE {table_name} AS t2 SET {path} = t1.path
            FROM generate_paths AS t1
            WHERE t2.{pk} = t1.pk AND t2.{pk} != {USING[NEW]}.{pk}
                AND (t2.{path} IS NULL OR t2.{path} != t1.path)
        )
        SELECT path FROM generate_paths
        WHERE pk = {USING[NEW]}.{pk}
            OR ({USING[NEW]}.{pk} IS NULL AND pk IS NULL)
    """, into=['new_path']),
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
        '{pk}', '{parent}', '{path}', '{{{order_by}}}'
    );
    """,
    """
    CREATE TRIGGER "update_{path}_after"
    AFTER DELETE
    ON "{table}"
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE PROCEDURE update_paths(
        '{pk}', '{parent}', '{path}', '{{{order_by}}}'
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
