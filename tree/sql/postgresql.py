from django.db import DEFAULT_DB_ALIAS, connections

from .base import ALPHANUM, ALPHANUM_LEN


CREATE_FUNCTIONS_QUERIES = (
    'CREATE EXTENSION IF NOT EXISTS ltree;',
    """
    CREATE OR REPLACE FUNCTION to_alphanum(i bigint) RETURNS text AS $$
    DECLARE
        ALPHANUM text := '{}';
        ALPHANUM_LEN int := {};
        out text := '';
        remainder int := 0;
    BEGIN
        LOOP
            remainder := i % ALPHANUM_LEN;
            i := i / ALPHANUM_LEN;
            out := substring(ALPHANUM from remainder+1 for 1) || out;
            IF i = 0 THEN
                RETURN out;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    """.format(ALPHANUM, ALPHANUM_LEN),
    """
    CREATE OR REPLACE FUNCTION update_paths() RETURNS trigger AS $$
    DECLARE
        table_name text := TG_TABLE_NAME;
        pk text := TG_ARGV[0];
        parent text := TG_ARGV[1];
        path text := TG_ARGV[2];
        order_by text[] := TG_ARGV[3];
        max_siblings int := TG_ARGV[4];
        label_size int := TG_ARGV[5];
        current_path ltree := NULL;
        parent_path ltree;
        new_path ltree;
        n_siblings integer;
    BEGIN
        IF TG_OP = 'UPDATE' THEN
            EXECUTE format('SELECT $1.%I', path) INTO current_path USING OLD;
        END IF;
        EXECUTE format('
            SELECT %1$I FROM %2$I WHERE %3$I = $1.%4$I
        ', path, table_name, pk, parent) INTO parent_path USING NEW;
        IF parent_path IS NULL THEN
            parent_path := ''::ltree;
        END IF;

        -- TODO: Add this behaviour to the model validation.
        IF parent_path <@ current_path THEN
            RAISE 'Cannot set itself or a descendant as parent.';
        END IF;

        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        EXECUTE format('
            WITH RECURSIVE generate_paths(pk, path) AS ((
                    SELECT
                        %1$I,
                        $1 || lpad(
                            to_alphanum(row_number() OVER (ORDER BY %2$s) - 1),
                            %3$L, ''0'')
                    FROM ((
                            SELECT *
                            FROM %4$I
                            WHERE
                                (CASE
                                    WHEN $2.%5$I IS NULL
                                        THEN %5$I IS NULL
                                    ELSE %5$I = $2.%5$I END)
                                AND (CASE
                                    WHEN $2.%1$I IS NULL
                                        THEN TRUE
                                    ELSE %1$I != $2.%1$I END)
                        ) UNION ALL (
                            SELECT $2.*
                        )
                    ) AS t
                ) UNION ALL (
                    SELECT
                        t2.%1$I,
                        t1.path || lpad(
                            to_alphanum(row_number() OVER (PARTITION BY t1.pk
                                                           ORDER BY %6$s) - 1),
                            %3$L, ''0'')
                    FROM generate_paths AS t1
                    INNER JOIN %4$I AS t2 ON t2.%5$I = t1.pk
                )
            ), updated AS (
                UPDATE %4$I AS t2 SET %7$I = t1.path::ltree
                FROM generate_paths AS t1
                WHERE t2.%1$I = t1.pk AND t2.%1$I != $2.%1$I
                    AND (t2.%7$I IS NULL OR t2.%7$I != t1.path)
            )
            SELECT path, (SELECT COUNT(*) FROM generate_paths)
            FROM generate_paths
            WHERE (
                CASE WHEN $2.%1$I IS NULL THEN pk IS NULL
                ELSE pk = $2.%1$I END)
        ',
            pk,
            array_to_string(order_by, ','),
            label_size,
            table_name,
            parent,
            't2.' || array_to_string(order_by, ',t2.'),
            path)
        INTO new_path, n_siblings
        USING parent_path, NEW;
        -- FIXME: `json_populate_record` is not available in PostgreSQL < 9.3.
        EXECUTE format('
            SELECT *
            FROM json_populate_record($1, ''{"%s": "%s"}''::json)
        ', path, new_path) INTO NEW USING NEW;

        IF n_siblings > max_siblings THEN
            RAISE '`max_siblings` (%) has been reached.\n'
                'You should increase it then rebuild.', max_siblings;
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """,
    """
    CREATE OR REPLACE FUNCTION rebuild_paths(table_name text, pk text,
                                             parent text) RETURNS void AS $$
    BEGIN
        EXECUTE format('
            UPDATE %1$I SET %2$I = %2$I WHERE %3$I IS NULL
        ', table_name, pk, parent);
    END;
    $$ LANGUAGE plpgsql;
    """,
)
# We escape the modulo operator '%' otherwise Django considers it
# as a placeholder for a parameter.
CREATE_FUNCTIONS_QUERIES = [s.replace('%', '%%')
                            for s in CREATE_FUNCTIONS_QUERIES]


DROP_FUNCTIONS_QUERIES = (
    """
    DROP FUNCTION IF EXISTS rebuild_paths(table_name text, pk text,
                                          parent text);
    """,
    'DROP FUNCTION IF EXISTS update_paths();',
    'DROP FUNCTION IF EXISTS to_alphanum(i bigint);',
    'DROP EXTENSION IF EXISTS ltree;',
)

CREATE_TRIGGER_QUERIES = (
    """
    CREATE TRIGGER "update_{path}"
    BEFORE INSERT OR UPDATE OF {update_columns}
    ON "{table}"
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
    EXECUTE PROCEDURE update_paths(
        "{pk}", "{parent}", "{path}", '{{{order_by}}}',
        {max_siblings}, {label_size});
    """,
    """
    CREATE OR REPLACE FUNCTION rebuild_{table}_{path}() RETURNS void AS $$
    BEGIN
        UPDATE "{table}" SET "{path}" = "{table}"."{path}" FROM (
            SELECT * FROM "{table}"
            WHERE "{parent}" IS NULL
            LIMIT 1
            FOR UPDATE
        ) AS t
        WHERE "{table}"."{pk}" = t."{pk}";
    END;
    $$ LANGUAGE plpgsql;
    """,
    # TODO: Find a way to create this deferrable unique constraint
    #       somewhere else.
    """
    ALTER TABLE "{table}"
    ADD CONSTRAINT "{table}_{path}_unique" UNIQUE ("{path}") DEFERRABLE;
    """,
)

DROP_TRIGGER_QUERIES = (
    # TODO: Find a way to delete this deferrable unique constraint
    #       somewhere else.
    'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{table}_{path}_unique";'
    'DROP TRIGGER IF EXISTS "update_{path}" ON "{table}";',
    'DROP FUNCTION IF EXISTS rebuild_{table}_{path}();',
)


CREATE_INDEX_QUERIES = (
    'CREATE INDEX "{table}_{path}" ON "{table}" USING gist("{path}");',
)

DROP_INDEX_QUERIES = (
    'DROP INDEX "{table}_{path}";',
)


def rebuild(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('SELECT rebuild_{}_{}();'.format(table, path_field))


def disable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('ALTER TABLE "{}" DISABLE TRIGGER "update_{}";'
                       .format(table, path_field))


def enable_trigger(table, path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        cursor.execute('ALTER TABLE "{}" ENABLE TRIGGER "update_{}";'
                       .format(table, path_field))
