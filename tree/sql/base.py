import re
from typing import List, Optional


# TODO: Integrate the doctests to the test runner.


UNNECESSARY_QUOTE_RE = re.compile(r'^[a-z_]+$')


def quote_ident(identifier: str):
    """
    >>> quote_ident('usual')
    'usual'
    >>> quote_ident('usual_with_underscores')
    'usual_with_underscores'
    >>> quote_ident('column with spaces')
    '"column with spaces"'
    >>> quote_ident('quote"inside')
    '"quote""inside"'
    >>> quote_ident('Capitalized')
    '"Capitalized"'
    >>> quote_ident('_private')
    '_private'
    >>> quote_ident('a')
    'a'
    >>> quote_ident('€')
    '"€"'
    >>> quote_ident('prénom')
    '"prénom"'
    >>> quote_ident('"already quoted"')
    '\"""already quoted""\"'
    """
    if UNNECESSARY_QUOTE_RE.match(identifier) is not None:
        return identifier
    identifier = identifier.replace('"', '""')
    return f'"{identifier}"'


def join_or(expressions: List[str]):
    if len(expressions) == 1:
        return expressions[0]
    return f'({" OR ".join(expressions)})'


def join_and(expressions: List[str]):
    return ' AND '.join(expressions)


def compare_columns(
    left: str, right: str, greater: Optional[bool] = None,
    strict: bool = False, nulls_last: bool = True,
):
    """
    >>> compare_columns('name', 'NEW.name')
    '(name IS NULL AND NEW.name IS NULL OR coalesce(name = NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', strict=True)
    '(name IS NULL AND NEW.name IS NULL OR coalesce(name = NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', nulls_last=False)
    '(name IS NULL AND NEW.name IS NULL OR coalesce(name = NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=True)
    '(name IS NULL OR coalesce(name >= NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=True, strict=True)
    '(name IS NULL AND NEW.name IS NOT NULL OR coalesce(name > NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=True, strict=True, nulls_last=False)
    '(NEW.name IS NULL AND name IS NOT NULL OR coalesce(name > NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=False)
    '(NEW.name IS NULL OR coalesce(name <= NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=False, strict=True)
    '(NEW.name IS NULL AND name IS NOT NULL OR coalesce(name < NEW.name, FALSE))'
    >>> compare_columns('name', 'NEW.name', greater=False, nulls_last=False)
    '(name IS NULL OR coalesce(name <= NEW.name, FALSE))'
    """
    operator = {
        None: '=',
        True: '>' if strict else '>=',
        False: '<' if strict else '<=',
    }[greater]
    operation = f'coalesce({left} {operator} {right}, FALSE)'
    if operator == '=':
        null_condition = join_and([f'{left} IS NULL', f'{right} IS NULL'])
    else:
        null_on_right = not nulls_last if greater else nulls_last
        null_condition = (
            f'{right} IS NULL' if null_on_right
            else f'{left} IS NULL'
        )
        if strict:
            null_condition = join_and([
                null_condition,
                f'{left} IS NOT NULL' if null_on_right
                else f'{right} IS NOT NULL'
            ])
    return join_or([null_condition, operation])


def get_nearby_sibling_where_clause(
    columns_in_order: List[str],
    record_name: str,
    greater: bool = True,
    nulls_last: bool = True,
):
    """
    >>> get_nearby_sibling_where_clause(["col1"], 'NEW')
    '(col1 IS NULL OR coalesce(col1 >= NEW.col1, FALSE))'
    >>> get_nearby_sibling_where_clause(["col1"], 'NEW', greater=False)
    '(NEW.col1 IS NULL OR coalesce(col1 <= NEW.col1, FALSE))'
    >>> get_nearby_sibling_where_clause(["col1", "col2", "col3"], 'NEW', greater=True)
    '((col1 IS NULL AND NEW.col1 IS NOT NULL OR coalesce(col1 > NEW.col1, FALSE)) OR (col1 IS NULL AND NEW.col1 IS NULL OR coalesce(col1 = NEW.col1, FALSE)) AND (col2 IS NULL AND NEW.col2 IS NOT NULL OR coalesce(col2 > NEW.col2, FALSE)) OR (col1 IS NULL AND NEW.col1 IS NULL OR coalesce(col1 = NEW.col1, FALSE)) AND (col2 IS NULL AND NEW.col2 IS NULL OR coalesce(col2 = NEW.col2, FALSE)) AND (col3 IS NULL OR coalesce(col3 >= NEW.col3, FALSE)))'
    """
    return join_or([
        join_and([
            compare_columns(
                column,
                f'{record_name}.{column}',
                greater=greater if i == column_index - 1 else None,
                strict=column_index < len(columns_in_order),
                nulls_last=nulls_last,
            )
            for i, column in enumerate(columns_in_order[:column_index])
        ])
        for column_index in range(1, len(columns_in_order) + 1)
    ])


def get_prev_sibling_where_clause(
    columns_in_order: List[str], record_name: str,
):
    return get_nearby_sibling_where_clause(
        columns_in_order=columns_in_order,
        record_name=record_name,
        greater=False,
    )


def get_next_sibling_where_clause(
    columns_in_order: List[str], record_name: str,
):
    return get_nearby_sibling_where_clause(
        columns_in_order=columns_in_order,
        record_name=record_name,
        greater=True,
    )
