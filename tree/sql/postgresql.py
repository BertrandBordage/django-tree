from typing import TYPE_CHECKING, cast

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models import Field, Model

from .base import (
    quote_ident,
    get_prev_sibling_where_clause,
    get_next_sibling_where_clause,
    compare_columns,
    join_and,
)

if TYPE_CHECKING:
    from ..fields import PathField


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


def get_update_paths_function_creation(
    model: type[Model],
    path_field_lookup: str,
) -> str:
    meta = model._meta
    pk_field = cast(Field, meta.pk)
    path_field = cast('PathField', meta.get_field(path_field_lookup))
    parent_field = path_field.parent_field
    order_by = path_field.order_by
    if not (
        pk_field.attname in order_by or pk_field.name in order_by or 'pk' in order_by
    ):
        order_by = [*order_by, 'pk']

    # TODO: `order_by` resolves local model fields and `pk` only; related lookups
    #       (e.g. `parent__name`) are not yet supported here.
    where_columns = []
    descending_flags = []
    sql_order_by = []
    for field_name in order_by:
        descending = field_name[0] == '-'
        if descending:
            field_name = field_name[1:]
        field = cast(
            Field, meta.pk if field_name == 'pk' else meta.get_field(field_name)
        )
        quoted_field_name = quote_ident(field.attname)
        where_columns.append(quoted_field_name)
        descending_flags.append(descending)
        sql_order_by.append(f'{quoted_field_name} {"DESC" if descending else "ASC"}')

    function = quote_ident(f'update_{meta.db_table}_{path_field.attname}_paths')
    table = quote_ident(meta.db_table)
    pk = quote_ident(pk_field.attname)
    parent = quote_ident(parent_field.attname)
    path = quote_ident(path_field.attname)
    # The sibling lookups order by the path, as `ORDER BY path ... LIMIT 1`, to read
    # a single neighbour.
    sql_order_by_columns = ', '.join(sql_order_by)

    # All three statements below are emitted as *static* SQL in the trigger
    # function body (not `EXECUTE`d dynamic strings): the table and column names
    # are fixed at `CREATE FUNCTION` time, so PL/pgSQL prepares and plan-caches
    # each statement once per session instead of re-planning it on every trigger
    # firing. `NEW`/`OLD` are referenced directly (PL/pgSQL passes their fields as
    # query parameters).
    rebuild = f"""
        -- NOTE: This full rebuild does not lock the rows it reparents, so a
        -- concurrent write during the rebuild is not serialized. `FOR UPDATE`
        -- cannot be added here: the recursive term uses window functions
        -- (`row_number()` / `count(*) OVER`), which PostgreSQL forbids together
        -- with row-level locking. A different rebuild shape would be needed to
        -- close this race.
        --
        -- Every sibling's rank and sibling count is computed once, in a single
        -- pass over the table (`tree_ranked`); the recursion then only joins that
        -- set to the paths found so far with a plain equality (`parent = pk`),
        -- which PostgreSQL can hash-join. The roots are the recursion's seed.
        WITH RECURSIVE tree_ranked AS (
            SELECT
                {pk} AS tree_pk,
                {parent} AS tree_parent,
                (row_number() OVER (
                    PARTITION BY {parent} ORDER BY {sql_order_by_columns}
                ) - 1)::integer AS tree_rank,
                -- Minimal segment width (base-254 digits) for this parent's child
                -- count, so rebuilt paths stay as compact as inserted ones
                -- instead of a fixed 4 bytes.
                CASE
                    WHEN count(*) OVER (PARTITION BY {parent}) <= 254 THEN 1
                    WHEN count(*) OVER (PARTITION BY {parent}) <= 64516 THEN 2
                    WHEN count(*) OVER (PARTITION BY {parent}) <= 16387064 THEN 3
                    ELSE 4
                END AS tree_width
            FROM {table}
        ), generate_paths(pk, path) AS (
                SELECT
                    r.tree_pk,
                    tree_int_to_seg(r.tree_rank, r.tree_width) || '\\x00'::bytea
                FROM tree_ranked AS r
                WHERE r.tree_parent IS NULL
            UNION ALL
                SELECT
                    r.tree_pk,
                    t1.path
                        || tree_int_to_seg(r.tree_rank, r.tree_width)
                        || '\\x00'::bytea
                FROM generate_paths AS t1
                INNER JOIN tree_ranked AS r ON r.tree_parent = t1.pk
        ), updated AS (
            UPDATE {table} AS t2 SET {path} = t1.path
            FROM generate_paths AS t1
            WHERE t2.{pk} = t1.pk AND t2.{pk} != OLD.{pk}
                AND (t2.{path} IS NULL OR t2.{path} != t1.path)
        )
        SELECT path
        INTO NEW.{path}
        FROM generate_paths
        WHERE pk = OLD.{pk}
        LIMIT 1
    """

    # The parent's path, then both neighbouring siblings' full paths. A path's own
    # segment is strictly monotonic with `order_by` among siblings (the same
    # invariant the read side relies on, e.g. `get_prev_sibling` ordering by
    # `path`), and siblings share the parent prefix, so the previous sibling is the
    # greatest path among those ordered before us and the next is the smallest
    # among those ordered after. `ORDER BY path LIMIT 1` reads just that one
    # neighbour with a top-1 pass instead of sorting and materialising the whole
    # sibling set (the portable stand-in for the PostgreSQL-18-only
    # `min`/`max(bytea)`).
    #
    # The siblings are also bounded by their level and by the parent's descendant
    # range, so the scan seeks `(tree_level(path), path)` (or the path index alone,
    # without the level) and reads at most the siblings. Left unbounded, the
    # planner walks the whole path index backwards from its end, filtering by
    # parent as it goes (it expects a match early), which costs a scan of the table
    # on every insert or move. The roots have no range, their level is enough.
    sibling_match = f'{compare_columns(parent, f"NEW.{parent}")} AND {pk} != NEW.{pk}'
    prev_sibling_where = get_prev_sibling_where_clause(
        where_columns, 'NEW', descending_flags
    )
    next_sibling_where = get_next_sibling_where_clause(
        where_columns, 'NEW', descending_flags
    )

    def neighbour(where: str, order: str, into: str, in_parent_range: bool) -> str:
        bounds = (
            f'AND {path} > new_parent_path AND {path} < tree_upper(new_parent_path)'
            if in_parent_range
            else ''
        )
        return f"""
                SELECT {path} INTO {into} FROM {table}
                WHERE tree_level({path}) = sibling_level {bounds}
                    AND {sibling_match} AND {where}
                ORDER BY {path} {order} LIMIT 1
        """

    get_sibling_values = f"""
            SELECT {path} INTO new_parent_path FROM {table} WHERE {pk} = NEW.{parent};
            new_parent_path := coalesce(new_parent_path, ''::bytea);
            parent_len := octet_length(new_parent_path);
            sibling_level := tree_level(new_parent_path) + 1;
            IF parent_len = 0 THEN
                {neighbour(prev_sibling_where, 'DESC', 'prev_sibling_path', False)};
                {neighbour(next_sibling_where, 'ASC', 'next_sibling_path', False)};
            ELSE
                {neighbour(prev_sibling_where, 'DESC', 'prev_sibling_path', True)};
                {neighbour(next_sibling_where, 'ASC', 'next_sibling_path', True)};
            END IF
    """

    # When a node moves, rewrite every descendant's stored prefix (the moved node's
    # old path) to its new path, preserving the descendant-local suffix. The
    # descendants are the whole-path range `[OLD.path, tree_upper(OLD.path))`, so
    # the btree behind the path is seeked instead of slicing every row's path.
    update_descendants = f"""
        UPDATE {table}
        SET {path} = NEW.{path} || substr({path}, octet_length(OLD.{path}) + 1)
        WHERE {path} >= OLD.{path} AND {path} < tree_upper(OLD.{path})
            AND {pk} != OLD.{pk}
    """

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
            sibling_level integer;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                -- Full rebuild, as requested by the rebuild function.
                IF NEW.{path} = ''::bytea THEN
                    {rebuild};
                    RETURN NEW;
                END IF;

                -- NULL means "unknown": keep the stored path when the row and its
                -- parent have one, else fall back to the full rebuild.
                IF NEW.{path} IS NULL THEN
                    IF OLD.{path} IS NOT NULL AND (
                        NEW.{parent} IS NULL
                        OR (SELECT {path} FROM {table} WHERE {pk} = NEW.{parent})
                            IS NOT NULL
                    ) THEN
                        NEW.{path} := OLD.{path};
                    ELSE
                        {rebuild};
                        RETURN NEW;
                    END IF;
                END IF;

                IF {row_unchanged} THEN
                    RETURN NEW;
                END IF;
            END IF;

            {get_sibling_values};

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
                {update_descendants};
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


# An empty path, which no real path is, asks for a full rebuild. NULL cannot: it
# means "unknown".
CREATE_REBUILD_FUNCTION_QUERY = """
    CREATE OR REPLACE FUNCTION {rebuild_function}() RETURNS void AS $$
    BEGIN
        UPDATE {table} SET {path} = ''::bytea FROM (
            SELECT * FROM {table}
            WHERE {parent} IS NULL
            LIMIT 1
            FOR UPDATE
        ) AS t
        WHERE {table}.{pk} = t.{pk};
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
    CREATE_REBUILD_FUNCTION_QUERY,
    # The path uniqueness constraint is created here (alongside the trigger)
    # rather than via Django's `unique=True`/`Index` machinery because it must be
    # `INITIALLY DEFERRED`: a reparent or rebuild shifts many rows' paths within a
    # single statement, so a moved node transiently shares a path with another row
    # mid-statement. Immediate (per-row) checking would reject those valid
    # intermediate states; deferring the check to commit time lets the statement
    # finish with the tree once again consistent. Django's field/index API can't
    # express `INITIALLY DEFERRED`, hence the raw DDL here.
    """
    ALTER TABLE {table}
    ADD CONSTRAINT {constraint} UNIQUE ({path})
    INITIALLY DEFERRED;
    """,
)

DROP_TRIGGER_QUERIES = (
    # Dropped here for symmetry with its creation above (see the deferred-constraint
    # rationale in `CREATE_TRIGGER_QUERIES`).
    'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint};',
    'DROP TRIGGER IF EXISTS "update_{path}_before" ON {table};',
    'DROP FUNCTION IF EXISTS {function}();',
)


def rebuild(table: str, path_field: str, db_alias: str = DEFAULT_DB_ALIAS) -> None:
    rebuild_function = quote_ident(f'rebuild_{table}_{path_field}')
    with connections[db_alias].cursor() as cursor:
        cursor.execute(f'SELECT {rebuild_function}();')


def disable_trigger(
    table: str, path_field: str, db_alias: str = DEFAULT_DB_ALIAS
) -> None:
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" DISABLE TRIGGER "update_{path_field}_before";'
        )


def enable_trigger(
    table: str, path_field: str, db_alias: str = DEFAULT_DB_ALIAS
) -> None:
    with connections[db_alias].cursor() as cursor:
        cursor.execute(
            f'ALTER TABLE "{table}" ENABLE TRIGGER "update_{path_field}_before";'
        )
