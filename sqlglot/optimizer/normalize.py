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
        '(z OR x) AND (z OR y)'

    Args:
        expression (sqlglot.Expression): expression to normalize
        dnf (bool): rewrite in disjunctive normal form instead
    Returns:
        sqlglot.Expression: normalized expression
    """
    expression = simplify(expression).transform(de_morgans_law, copy=False)
    expression = while_changing(
        expression,
        lambda e: distributive_law(e, exp.And, exp.Or) if dnf else distributive_law(e, exp.Or, exp.And)
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


def distributive_law(expression, from_exp, to_exp):
    """
    x OR (y AND z) -> (x OR y) AND (x OR z)
    (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
    """
    expression = simplify(expression)
    exp.replace_children(expression, lambda e: distributive_law(e, from_exp, to_exp))

    if isinstance(expression, from_exp):
        l = expression.left.unnest()
        r = expression.right.unnest()

        if isinstance(r, to_exp):
            return _distribute(l, r)
        if isinstance(l, to_exp):
            return _distribute(r, l)

    return expression


def _distribute(a, b):
    if isinstance(a, (exp.And, exp.Or)):
        exp.replace_children(
            a,
            lambda c: exp.and_(
                exp.paren(exp.or_(c, b.left)),
                exp.paren(exp.or_(c, b.right)),
            ),
        )
    else:
        a = exp.and_(exp.or_(a, b.left), exp.or_(a, b.right))

    return a
