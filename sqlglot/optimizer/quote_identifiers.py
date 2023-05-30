from sqlglot import exp
from sqlglot.helper import case_sensitive
from sqlglot._typing import E
from sqlglot.dialects.dialect import DialectType as DialectType


def quote_identifiers(
    expression: E, dialect: DialectType = None, identify: bool = True, copy: bool = True
) -> E:
    """Makes sure all identifiers that need to be quoted are quoted."""

    def _quote(expression: E) -> E:
        if isinstance(expression, exp.Identifier):
            name = expression.this
            expression.set(
                "quoted",
                identify
                or case_sensitive(name, dialect=dialect)
                or not exp.SAFE_IDENTIFIER_RE.match(name),
            )
        return expression

    return expression.transform(_quote, copy=copy)
