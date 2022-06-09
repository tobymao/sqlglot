from sqlglot.helper import while_changing
from sqlglot.optimizer.simplify import simplify, compare_and_prune, flatten
import sqlglot.expressions as exp


def normalize(expression, dnf=False, max_distance=128):
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
        max_distance (int): the maximal estimated distance from cnf to attempt conversion
    Returns:
        sqlglot.Expression: normalized expression
    """
    expression = simplify(expression).transform(de_morgans_law, copy=False)

    expression = while_changing(
        expression, lambda e: distributive_law(e, dnf, max_distance)
    )
    return simplify(expression)


def normalized(expression, dnf=False):
    ancestor, root = (exp.And, exp.Or) if dnf else (exp.Or, exp.And)

    return not any(
        connector.find_ancestor(ancestor) for connector in expression.find_all(root)
    )


def normalization_distance(expression, dnf=False):
    """
    The difference in the number of predicates between the current expression and the normalized form.

    This is used as an estimate of the cost of the conversion which is exponential in complexity.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("(a AND b) OR (c AND d)")
        >>> normalization_distance(expression)
        4

    Args:
        expression (sqlglot.Expression): expression to compute distance
        dnf (bool): compute to dnf distance instead
    Returns:
        int: difference
    """
    return sum(_predicate_lengths(expression, dnf)) - (
        len(list(expression.find_all(exp.Connector))) + 1
    )


def _predicate_lengths(expression, dnf):
    """
    Returns a list of predicate lengths when expanded to normalized form.

    (A AND B) OR C -> [2, 2] because len(A OR C), len(B OR C).
    """
    expression = expression.unnest()

    if not isinstance(expression, exp.Connector):
        return [1]

    left, right = expression.args.values()

    if isinstance(expression, exp.And if dnf else exp.Or):
        x = [
            a + b
            for a in _predicate_lengths(left, dnf)
            for b in _predicate_lengths(right, dnf)
        ]
        return x
    return _predicate_lengths(left, dnf) + _predicate_lengths(right, dnf)


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


def distributive_law(expression, dnf, max_distance):
    """
    x OR (y AND z) -> (x OR y) AND (x OR z)
    (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
    """
    if isinstance(expression.unnest(), exp.Connector):
        if normalization_distance(expression, dnf) > max_distance:
            return expression

    to_exp, from_exp = (exp.Or, exp.And) if dnf else (exp.And, exp.Or)

    expression = expression.transform(flatten, copy=False).transform(
        compare_and_prune, copy=False
    )

    exp.replace_children(expression, lambda e: distributive_law(e, dnf, max_distance))

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
