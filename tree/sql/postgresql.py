from django.db import DEFAULT_DB_ALIAS, connections


TO_ALPHANUM_SQL = """
CREATE OR REPLACE FUNCTION to_alphanum(i bigint) RETURNS text AS $$
DECLARE
    ALPHANUM text := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    ALPHANUM_LEN int := length(ALPHANUM);
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
"""


REBUILD_SQL = """
WITH RECURSIVE generate_paths(id, path) AS ((
    SELECT
      "%(pk_attname)s",
      lpad(to_alphanum(row_number() OVER (ORDER BY %(order_by)s) - 1),
           %(label_size)s, '0')
    FROM "%(table)s" AS t2
    WHERE "%(parent_attname)s" IS NULL
  ) UNION ALL (
    SELECT
      t2."%(pk_attname)s",
      t1.path || '.'
      || lpad(to_alphanum(row_number() OVER (PARTITION BY t1.id
                                             ORDER BY %(order_by)s) - 1),
              %(label_size)s, '0')
    FROM generate_paths AS t1
    INNER JOIN "%(table)s" AS t2 ON t2."%(parent_attname)s" = t1.id
  )
)
UPDATE "%(table)s" AS t2 SET "%(attname)s" = t1.path::ltree
FROM generate_paths AS t1
WHERE t2."%(pk_attname)s" = t1.id;
"""


def rebuild_tree(path_field, db_alias=DEFAULT_DB_ALIAS):
    with connections[db_alias].cursor() as cursor:
        meta = path_field.model._meta
        order_by = []
        for field_name in path_field.order_by + ('pk',):
            field = (meta.pk if field_name == 'pk'
                     else meta.get_field(field_name.lstrip('-')))
            order_by.append(
                't2."%s" %s' % (
                    field.attname,
                    'DESC' if field_name.startswith('-') else 'ASC'))
        parent_field = meta.get_field(path_field.parent_field_name)
        cursor.execute(
            TO_ALPHANUM_SQL +
            (REBUILD_SQL % {
                'attname': path_field.attname,
                'pk_attname': meta.pk.attname,
                'label_size': path_field.label_size,
                'table': meta.db_table,
                'parent_attname': parent_field.attname,
                'order_by': ', '.join(order_by),
            }))
