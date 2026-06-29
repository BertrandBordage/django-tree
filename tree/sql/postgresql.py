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


# Table-independent helper functions backing the `bytea` path encoding. A path is
# the concatenation, per depth level, of `<segment bytes> 0x00`: `0x00` is the
# reserved level delimiter (never appears inside a segment), so `bytea`'s unsigned
# byte-wise comparison reproduces the tree order and ancestor = byte-prefix at a
# level boundary. Created with `CREATE OR REPLACE` (idempotent) both here (so every
# trigger install is self-contained) and by a `tree` migration (so the functional
# `tree_level(path)` index can be built before any trigger exists).
#
# These contain no `%`, so they survive the `.replace('%', '%%')` escaping in
# `operations.CreateTreeTrigger` and can be reused verbatim in migration `RunSQL`.
TREE_HELPER_FUNCTIONS = r"""
CREATE OR REPLACE FUNCTION tree_mid(a bytea, b bytea) RETURNS bytea AS $$
DECLARE
    result bytea := ''::bytea;
    i integer := 0;
    x integer;
    y integer;
BEGIN
    -- Order-preserving "between" key (fractional indexing). Segment bytes are
    -- base-256 fraction digits whose value is `byte - 1`, so byte 0x01 is the
    -- smallest digit (0). `a` is the lower neighbour (NULL => -infinity) and `b`
    -- the upper (NULL => +infinity). Past the end of `a` (and a NULL `a`) we read
    -- the low filler digit 0x01, past the end of `b` (and a NULL `b`) the virtual
    -- value 256 (one above the largest byte). The returned segment is strictly
    -- between `a` and `b`, never contains 0x00, and always ends on an emitted byte
    -- >= 0x02, which keeps every gap (head, tail, internal) splittable forever, so
    -- the path never needs renumbering.
    LOOP
        IF a IS NOT NULL AND i < octet_length(a) THEN
            x := get_byte(a, i);
        ELSE
            x := 1;
        END IF;
        IF b IS NOT NULL AND i < octet_length(b) THEN
            y := get_byte(b, i);
        ELSE
            y := 256;
        END IF;
        IF y - x >= 2 THEN
            RETURN result || set_byte('\x00'::bytea, 0, x + (y - x) / 2);
        END IF;
        result := result || set_byte('\x00'::bytea, 0, x);
        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION tree_int_to_seg(rank integer, width integer)
    RETURNS bytea AS $$
DECLARE
    result bytea := ''::bytea;
    i integer;
    r integer := rank;
BEGIN
    -- Fixed-width big-endian base-254 encoding of a rebuild rank, digits mapped to
    -- bytes 0x02..0xFF (left-padded with 0x02). Order-preserving for a fixed width,
    -- never emits 0x00 or 0x01 -- leaving 0x01-prefixed room below every rebuilt
    -- sibling so `tree_mid` can still insert before the first one.
    FOR i IN 1 .. width LOOP
        result := set_byte('\x00'::bytea, 0, mod(r, 254) + 2) || result;
        r := r / 254;
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION tree_level(p bytea) RETURNS integer AS $$
DECLARE
    n integer := 0;
    i integer;
BEGIN
    -- Depth = number of 0x00 level delimiters.
    IF p IS NULL THEN
        RETURN NULL;
    END IF;
    FOR i IN 0 .. octet_length(p) - 1 LOOP
        IF get_byte(p, i) = 0 THEN
            n := n + 1;
        END IF;
    END LOOP;
    RETURN n;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION tree_upper(p bytea) RETURNS bytea AS $$
BEGIN
    -- Exclusive upper bound of p's descendant range: drop p's trailing 0x00
    -- delimiter and append 0x01 (which sorts above the delimiter but below every
    -- segment byte). NULL for the empty prefix (the virtual root above all
    -- roots), meaning the range is unbounded above.
    IF p IS NULL OR octet_length(p) = 0 THEN
        RETURN NULL;
    END IF;
    RETURN substr(p, 1, octet_length(p) - 1) || '\x01'::bytea;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION tree_parent_prefix(p bytea) RETURNS bytea AS $$
DECLARE
    i integer;
BEGIN
    -- `p` ends with its own 0x00 terminator; return everything up to and including
    -- the previous 0x00 (the parent's terminator), or '' for a root.
    IF p IS NULL OR octet_length(p) = 0 THEN
        RETURN ''::bytea;
    END IF;
    FOR i IN REVERSE octet_length(p) - 1 .. 1 LOOP
        IF get_byte(p, i - 1) = 0 THEN
            RETURN substr(p, 1, i);
        END IF;
    END LOOP;
    RETURN ''::bytea;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""


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
    # Both the rebuild's recursive CTE and the sibling lookups order by the path;
    # the latter as `ORDER BY path ... LIMIT 1` to read a single neighbour.
    sql_t2_order_by = ', '.join(
        [f't2.{ordered_column}' for ordered_column in sql_order_by]
    )

    rebuild = execute_format(
        f"""
        -- TODO: Handle concurrent writes during this query (using FOR UPDATE).
        WITH RECURSIVE generate_paths(pk, path) AS ((
                SELECT {parent}, ''::bytea
                FROM {table}
                WHERE {parent} IS NULL
                LIMIT 1
            ) UNION ALL (
                SELECT
                    t2.{pk},
                    t1.path || tree_int_to_seg(
                        (row_number() OVER (
                            PARTITION BY t1.pk ORDER BY {sql_t2_order_by}
                        ) - 1)::integer,
                        -- Minimal segment width (base-254 digits) for this
                        -- parent's child count, so rebuilt paths stay as compact
                        -- as inserted ones instead of a fixed 4 bytes.
                        CASE
                            WHEN count(*) OVER (PARTITION BY t1.pk) <= 254 THEN 1
                            WHEN count(*) OVER (PARTITION BY t1.pk) <= 64516 THEN 2
                            WHEN count(*) OVER (PARTITION BY t1.pk) <= 16387064
                                THEN 3
                            ELSE 4
                        END
                    ) || '\\x00'::bytea
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

    # Three scalar subqueries yield the parent's path and both neighbouring
    # siblings' full paths. A path's own segment is strictly monotonic with
    # `order_by` among siblings (the same invariant the read side relies on, e.g.
    # `get_prev_sibling` ordering by `path`), and siblings share the parent prefix,
    # so the previous sibling is the greatest path among those ordered before us
    # and the next is the smallest among those ordered after. `ORDER BY path
    # LIMIT 1` reads just that one neighbour with a top-1 pass instead of sorting
    # and materialising the whole sibling set (the portable stand-in for the
    # PostgreSQL-18-only `min`/`max(bytea)`).
    sibling_match = f'{compare_columns(parent, f"$1.{parent}")} AND {pk} != $1.{pk}'
    prev_sibling_where = get_prev_sibling_where_clause(
        where_columns, '$1', descending_flags
    )
    next_sibling_where = get_next_sibling_where_clause(
        where_columns, '$1', descending_flags
    )
    get_sibling_values = execute_format(
        f"""
        SELECT
            (SELECT {path} FROM {table} WHERE {pk} = $1.{parent}),
            (SELECT {path} FROM {table}
                WHERE {sibling_match} AND {prev_sibling_where}
                ORDER BY {path} DESC LIMIT 1),
            (SELECT {path} FROM {table}
                WHERE {sibling_match} AND {next_sibling_where}
                ORDER BY {path} ASC LIMIT 1)
    """,
        using=['NEW'],
        into=['new_parent_path', 'prev_sibling_path', 'next_sibling_path'],
    )

    # When a node moves, rewrite every descendant's stored prefix (the moved node's
    # old path) to its new path, preserving the descendant-local suffix.
    update_descendants = execute_format(
        f"""
        UPDATE {table}
        SET {path} = $1.{path} || substr({path}, octet_length($2.{path}) + 1)
        WHERE substr({path}, 1, octet_length($2.{path})) = $2.{path}
            AND {pk} != $2.{pk}
    """,
        using=['NEW', 'OLD'],
    )

    row_unchanged = join_and(
        [
            compare_columns(f'OLD.{where_column}', f'NEW.{where_column}')
            for where_column in [parent, *where_columns]
        ]
    )

    return (
        TREE_HELPER_FUNCTIONS
        + f"""
        CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$
        DECLARE
            prev_sibling_path bytea := NULL;
            next_sibling_path bytea := NULL;
            prev_sibling_seg bytea := NULL;
            next_sibling_seg bytea := NULL;
            old_sibling_seg bytea := NULL;
            new_parent_path bytea;
            parent_len integer;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.{path} IS NULL THEN
                    {rebuild}
                    RETURN NEW;
                END IF;

                IF {row_unchanged} THEN
                    RETURN NEW;
                END IF;
            END IF;

            {get_sibling_values}
            new_parent_path := coalesce(new_parent_path, ''::bytea);
            parent_len := octet_length(new_parent_path);

            IF prev_sibling_path IS NOT NULL THEN
                prev_sibling_seg := substr(
                    prev_sibling_path, parent_len + 1,
                    octet_length(prev_sibling_path) - parent_len - 1
                );
            END IF;
            IF next_sibling_path IS NOT NULL THEN
                next_sibling_seg := substr(
                    next_sibling_path, parent_len + 1,
                    octet_length(next_sibling_path) - parent_len - 1
                );
            END IF;

            -- Preserve the current path when it is still relevant, even though it
            -- might not be at the middle between prev and next.
            IF TG_OP = 'UPDATE'
                AND {compare_columns(f'NEW.{parent}', f'OLD.{parent}')}
            THEN
                old_sibling_seg := substr(
                    OLD.{path}, parent_len + 1,
                    octet_length(OLD.{path}) - parent_len - 1
                );
                IF (prev_sibling_seg IS NULL OR old_sibling_seg > prev_sibling_seg)
                    AND (next_sibling_seg IS NULL
                         OR old_sibling_seg < next_sibling_seg)
                THEN
                    RETURN NEW;
                END IF;
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF substr(new_parent_path, 1, octet_length(OLD.{path})) = OLD.{path}
                THEN
                    RAISE 'Cannot set itself or a descendant as parent.';
                END IF;
            END IF;

            NEW.{path} = new_parent_path
                || tree_mid(prev_sibling_seg, next_sibling_seg)
                || '\\x00'::bytea;

            IF TG_OP = 'UPDATE' THEN
                {update_descendants}
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


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
        UPDATE {table} SET {path} = NULL FROM (
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
