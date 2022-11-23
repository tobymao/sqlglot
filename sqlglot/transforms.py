from __future__ import annotations

import typing as t

from sqlglot.helper import find_new_name

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


def eliminate_distinct_on(expression: exp.Expression) -> exp.Expression:
    """
    Convert SELECT DISTINCT ON statements to a subquery with a window function.

    This is useful for dialects that don't support SELECT DISTINCT ON but support window functions.

    Args:
        expression: the expression that will be transformed.

    Returns:
        The transformed expression.
    """
    if (
        isinstance(expression, exp.Select)
        and expression.args.get("distinct")
        and expression.args["distinct"].args.get("on")
        and isinstance(expression.args["distinct"].args["on"], exp.Tuple)
    ):
        distinct_cols = [e.copy() for e in expression.args["distinct"].args["on"].expressions]
        outer_selects = [e.copy() for e in expression.expressions]
        nested = expression.copy()
        nested.args["distinct"].pop()
        row_number = find_new_name(expression.named_selects, "_row_number")
        window = exp.Window(
            this=exp.RowNumber(),
            partition_by=distinct_cols,
        )
        order = nested.args.get("order")
        if order:
            window.set("order", order.copy())
            order.pop()
        window = exp.alias_(window, row_number)
        nested.select(window, copy=False)
        return exp.select(*outer_selects).from_(nested.subquery()).where(f'"{row_number}" = 1')
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
ELIMINATE_DISTINCT_ON = {exp.Select: preprocess([eliminate_distinct_on], delegate("select_sql"))}
