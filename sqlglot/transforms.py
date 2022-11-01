from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from sqlglot.generator import Generator

from sqlglot import expressions as exp


def unalias_group(expression: exp.Expression) -> exp.Expression:
    """
    Replace references to select aliases in GROUP BY clauses.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT a AS b FROM x GROUP BY b").transform(unalias_group).sql()
        'SELECT a AS b FROM x GROUP BY 1'

    Args:
        expression: the expression that will be transformed.

    Returns:
        The transformed expression.
    """
    if isinstance(expression, exp.Group) and isinstance(expression.parent, exp.Select):
        aliased_selects = {
            e.alias: (i, e.this)
            for i, e in enumerate(expression.parent.expressions, start=1)
            if isinstance(e, exp.Alias)
        }

        expression = expression.copy()

        top_level_expression = None
        for item, parent, _ in expression.walk(bfs=False):
            top_level_expression = item if isinstance(parent, exp.Group) else top_level_expression
            if isinstance(item, exp.Column) and not item.table:
                alias_index, col_expression = aliased_selects.get(item.name, (None, None))
                if alias_index and top_level_expression != col_expression:
                    item.replace(exp.Literal.number(alias_index))

    return expression


def preprocess(
    transforms: t.List[t.Callable[[exp.Expression], exp.Expression]],
    to_sql: t.Callable[[Generator, exp.Expression], str],
) -> t.Callable[[Generator, exp.Expression], str]:
    """
    Creates a new transform by chaining a sequence of transformations and converts the resulting
    expression to SQL, using an appropriate `Generator.TRANSFORMS` function.

    Args:
        transforms: sequence of transform functions. These will be called in order.
        to_sql: final transform that converts the resulting expression to a SQL string.

    Returns:
        Function that can be used as a generator transform.
    """

    def _to_sql(self, expression):
        expression = transforms[0](expression)
        for t in transforms[1:]:
            expression = t(expression)
        return to_sql(self, expression)

    return _to_sql


def delegate(attr: str) -> t.Callable:
    """
    Create a new method that delegates to `attr`. This is useful for creating `Generator.TRANSFORMS`
    functions that delegate to existing generator methods.
    """

    def _transform(self, *args, **kwargs):
        return getattr(self, attr)(*args, **kwargs)

    return _transform


UNALIAS_GROUP = {exp.Group: preprocess([unalias_group], delegate("group_sql"))}
