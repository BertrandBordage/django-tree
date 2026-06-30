"""Oracle support for the ``RAW`` path encoding.

Oracle is a *non-trigger* backend: the tree is maintained in Python (see
:mod:`tree.maintenance`), exactly like SQLite and MySQL. The path column is a
``RAW`` (see :class:`tree.fields.PathField`) -- byte-ordered, indexable and
range-comparable, unlike the ``BLOB`` that ``BinaryField`` maps to by default.

Almost every lookup reduces to a plain ``RAW`` range comparison, a constant the
ORM precomputes in Python (:mod:`tree.sql.helpers`), or built-in ``UTL_RAW``.
The one thing that has no portable, byte-correct SQL form on ``RAW`` is counting
the ``0x00`` level delimiters: Oracle's ``INSTR`` operates on the hex
representation and would match a ``00`` straddling two bytes. So a single
deterministic ``tree_level`` helper is installed (by the ``tree`` migration
``0003_tree_functions``) and used by ``child_of`` / ``sibling_of`` to keep only
direct children of a prefix.
"""

# Counts the 0x00 level delimiters of a RAW path (its depth). DETERMINISTIC so it
# may sit in a WHERE clause (and a function-based index) freely. A zero-length RAW
# is NULL on Oracle, so the loop simply never runs for the virtual root.
TREE_LEVEL_FUNCTION = """
CREATE OR REPLACE FUNCTION tree_level(p RAW) RETURN INTEGER DETERMINISTIC IS
    n INTEGER := 0;
    i INTEGER;
BEGIN
    IF p IS NULL THEN
        RETURN NULL;
    END IF;
    FOR i IN 1 .. UTL_RAW.LENGTH(p) LOOP
        IF UTL_RAW.SUBSTR(p, i, 1) = HEXTORAW('00') THEN
            n := n + 1;
        END IF;
    END LOOP;
    RETURN n;
END;
"""

# `DROP FUNCTION IF EXISTS` only exists on Oracle 23+, so swallow ORA-04043
# (object does not exist) to stay compatible with 19c/21c too.
DROP_TREE_LEVEL_FUNCTION = """
BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION tree_level';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -4043 THEN
            RAISE;
        END IF;
END;
"""
