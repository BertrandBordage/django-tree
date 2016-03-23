from django.db import DEFAULT_DB_ALIAS, connections


CREATE_FUNCTIONS_QUERIES = (
    'CREATE EXTENSION IF NOT EXISTS ltree;',
    # FIXME: This query has the modulo operator '%' escaped otherwise Django
    #        considers it as a query parameter.
    """
    CREATE OR REPLACE FUNCTION to_alphanum(i bigint) RETURNS text AS $$
    DECLARE
        ALPHANUM text := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        ALPHANUM_LEN int := length(ALPHANUM);
        out text := '';
        remainder int := 0;
    BEGIN
        LOOP
            remainder := i %% ALPHANUM_LEN;
            i := i / ALPHANUM_LEN;
            out := substring(ALPHANUM from remainder+1 for 1) || out;
            IF i = 0 THEN
                RETURN out;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    """,
)


DROP_FUNCTIONS_QUERIES = (
    'DROP EXTENSION IF EXISTS ltree;',
    'DROP FUNCTION IF EXISTS to_alphanum(i bigint);',
)


UPDATE_SQL = """
WITH RECURSIVE generate_paths(pk, path) AS ((
    %s
  ) UNION ALL (
    SELECT
      t2."{pk_attname}",
      t1.path || lpad(
        to_alphanum(row_number() OVER (PARTITION BY t1.pk
                                       ORDER BY {order_by}) - 1),
        {label_size}, '0')::ltree
    FROM generate_paths AS t1
    INNER JOIN "{table}" AS t2 ON t2."{parent_attname}" = t1.pk
  )
)
UPDATE "{table}" AS t2 SET "{attname}" = t1.path::ltree
FROM generate_paths AS t1
WHERE t2."{pk_attname}" = t1.pk AND t2."{attname}" != t1."{attname}";
"""

UPDATE_SIBLINGS_SQL = UPDATE_SQL % 'VALUES (%s, %s::ltree)'

REBUILD_SQL = UPDATE_SQL % """
SELECT
  "{pk_attname}",
  lpad(to_alphanum(row_number() OVER (ORDER BY {order_by}) - 1),
       {label_size}, '0')::ltree
FROM "{table}" AS t2
WHERE "{parent_attname}" IS NULL
"""


def rebuild(path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        meta = path_field.model._meta
        order_by = []
        for field_name in path_field.order_by + ('pk',):
            field = (meta.pk if field_name == 'pk'
                     else meta.get_field(field_name.lstrip('-')))
            order_by.append(
                't2."%s" %s' % (
                    field.attname,
                    'DESC' if field_name[0] == '-' else 'ASC'))
        parent_field = meta.get_field(path_field.parent_field_name)
        cursor.execute(
            REBUILD_SQL.format(**{
                'attname': path_field.attname,
                'pk_attname': meta.pk.attname,
                'label_size': path_field.label_size,
                'table': meta.db_table,
                'parent_attname': parent_field.attname,
                'order_by': ', '.join(order_by),
            }))
