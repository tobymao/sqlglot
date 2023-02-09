from __future__ import annotations

import typing as t

from sqlglot import exp


def expand_laterals(expression: exp.Expression) -> exp.Expression:
    """
    Expand lateral column alias references.

    This assumes `qualify_columns` as already run.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT x.a + 1 AS b, b + 1 AS c FROM x"
        >>> expression = sqlglot.parse_one(sql)
        >>> expand_laterals(expression).sql()
        'SELECT x.a + 1 AS b, x.a + 1 + 1 AS c FROM x'

    Args:
        expression: expression to optimize
    Returns:
        optimized expression
    """
    for select in expression.find_all(exp.Select):
        alias_to_expression: t.Dict[str, exp.Expression] = {}
        for projection in select.expressions:
            for column in projection.find_all(exp.Column):
                if not column.table and column.name in alias_to_expression:
                    column.replace(alias_to_expression[column.name].copy())
                if isinstance(projection, exp.Alias):
                    alias_to_expression[projection.alias] = projection.this
    return expression
