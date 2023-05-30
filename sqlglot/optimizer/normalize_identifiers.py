from sqlglot import exp
from sqlglot._typing import E
from sqlglot.dialects.dialect import RESOLVES_IDENTIFIERS_AS_UPPERCASE, DialectType


def normalize_identifiers(expression: E, dialect: DialectType = None) -> E:
    """
    Normalize all unquoted identifiers to either lower or upper case, depending on
    the dialect. This essentially makes those identifiers case-insensitive.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('SELECT Bar.A AS A FROM "Foo".Bar')
        >>> normalize_identifiers(expression).sql()
        'SELECT bar.a AS a FROM "Foo".bar'

    Args:
        expression: The expression to transform.
        dialect: The dialect to use in order to decide how to normalize identifiers.

    Returns:
        The transformed expression.
    """
    return expression.transform(_normalize, dialect, copy=False)


def _normalize(node: exp.Expression, dialect: DialectType = None) -> exp.Expression:
    if isinstance(node, exp.Identifier) and not node.quoted:
        node.set(
            "this",
            node.this.upper()
            if dialect in RESOLVES_IDENTIFIERS_AS_UPPERCASE
            else node.this.lower(),
        )

    return node
