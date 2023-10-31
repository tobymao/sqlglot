from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot._typing import E
from sqlglot.dialects.dialect import Dialect, DialectType


@t.overload
def normalize_identifiers(expression: E, dialect: DialectType = None) -> E:
    ...


@t.overload
def normalize_identifiers(expression: str, dialect: DialectType = None) -> exp.Expression:
    ...


def normalize_identifiers(expression, dialect=None):
    """
    Normalize all unquoted identifiers to either lower or upper case, depending
    on the dialect. This essentially makes those identifiers case-insensitive.

    It's possible to make this a no-op by adding a special comment next to the
    identifier of interest:

        SELECT a /* sqlglot.meta case_sensitive */ FROM table

    In this example, the identifier `a` will not be normalized.

    Note:
        Some dialects (e.g. BigQuery) treat identifiers as case-insensitive even
        when they're quoted, so in these cases all identifiers are normalized.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('SELECT Bar.A AS A FROM "Foo".Bar')
        >>> normalize_identifiers(expression).sql()
        'SELECT bar.a AS a FROM "Foo".bar'
        >>> normalize_identifiers("foo", dialect="snowflake").sql(dialect="snowflake")
        'FOO'

    Args:
        expression: The expression to transform.
        dialect: The dialect to use in order to decide how to normalize identifiers.

    Returns:
        The transformed expression.
    """
    if isinstance(expression, str):
        expression = exp.parse_identifier(expression, dialect=dialect)

    dialect = Dialect.get_or_raise(dialect)

    def _normalize(node: E) -> E:
        if not node.meta.get("case_sensitive"):
            exp.replace_children(node, _normalize)
            node = dialect.normalize_identifier(node)
        return node

    return _normalize(expression)
