from sqlglot.helper import while_changing
from sqlglot.optimizer.simplify import simplify
import sqlglot.expressions as exp


def normalize(expression, dnf=False):
    """
    Rewrite sqlglot AST into conjunctive normal form.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("(x AND y) OR z")
        >>> normalize(expression).sql()
        '(x OR z) AND (y OR z)'

    Args:
        expression (sqlglot.Expression): expression to normalize
        dnf (bool): rewrite in disjunctive normal form instead
    Returns:
        sqlglot.Expression: normalized expression
    """
    expression = simplify(expression).transform(de_morgans_law, copy=False)
    expression = while_changing(
        expression,
        lambda e: distributive_law(e, exp.Or) if dnf else distributive_law(e, exp.And),
    )
    return expression


def de_morgans_law(expression):
    """
    NOT (x OR y) -> NOT x AND NOT y
    NOT (x AND y) -> NOT x OR NOT y
    """

    if isinstance(expression, exp.Not) and isinstance(expression.this, exp.Paren):
        condition = expression.this.unnest()

        if isinstance(condition, exp.And):
            return exp.or_(exp.not_(condition.left), exp.not_(condition.right))

        if isinstance(condition, exp.Or):
            return exp.and_(exp.not_(condition.left), exp.not_(condition.right))

    return expression


def distributive_law(expression, to_exp):
    """
    x OR (y AND z) -> (x OR y) AND (x OR z)
    (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
    """
    if to_exp == exp.And:
        from_exp = exp.Or
    elif to_exp == exp.Or:
        from_exp = exp.And
    else:
        raise ValueError("Not support expression")

    expression = simplify(expression)
    exp.replace_children(expression, lambda e: distributive_law(e, to_exp))

    if isinstance(expression, from_exp):
        l = expression.left.unnest()
        r = expression.right.unnest()

        from_func = exp.and_ if from_exp == exp.And else exp.or_
        to_func = exp.and_ if to_exp == exp.And else exp.or_

        if isinstance(r, to_exp):
            return _distribute(l, r, from_func, to_func)
        if isinstance(l, to_exp):
            return _distribute(r, l, from_func, to_func)

    return expression


def _distribute(a, b, from_func, to_func):
    if isinstance(a, exp.Connector):
        exp.replace_children(
            a,
            lambda c: to_func(
                exp.paren(from_func(c, b.left)),
                exp.paren(from_func(c, b.right)),
            ),
        )
    else:
        a = to_func(from_func(a, b.left), from_func(a, b.right))

    return a
