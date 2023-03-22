from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import while_changing
from sqlglot.optimizer.simplify import flatten, uniq_sort

logger = logging.getLogger("sqlglot")


def normalize(expression: exp.Expression, dnf: t.Optional[bool] = None, max_distance: int = 128):
    """
    Rewrite sqlglot AST into conjunctive normal form or disjunctive normal form.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("(x AND y) OR z")
        >>> normalize(expression, dnf=False).sql()
        '(x OR z) AND (y OR z)'

    Args:
        expression: expression to normalize
        dnf: rewrite in disjunctive normal form instead. if none then it will choose the closest form.
        max_distance (int): the maximal estimated distance from cnf/dnf to attempt conversion
    Returns:
        sqlglot.Expression: normalized expression
    """
    cache: t.Dict[int, str] = {}

    for node, *_ in tuple(
        expression.walk(prune=lambda e, *_: isinstance(expression, exp.Connector))
    ):
        if isinstance(node, exp.Connector):
            if dnf is None:
                dnf = normalization_distance(node, dnf=False) > normalization_distance(
                    node, dnf=True
                )

            root = node is expression
            try:
                node = while_changing(node, lambda e: distributive_law(e, dnf, max_distance, cache))
            except OptimizeError as e:
                logger.error(f"Optimization Error: %s", e)
                return expression

            if root:
                expression = node

    return expression


def normalized(expression, dnf=False):
    ancestor, root = (exp.And, exp.Or) if dnf else (exp.Or, exp.And)

    return not any(connector.find_ancestor(ancestor) for connector in expression.find_all(root))


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
        sum(1 for _ in expression.find_all(exp.Connector)) + 1
    )


def _predicate_lengths(expression, dnf):
    """
    Returns a list of predicate lengths when expanded to normalized form.

    (A AND B) OR C -> [2, 2] because len(A OR C), len(B OR C).
    """
    expression = expression.unnest()

    if not isinstance(expression, exp.Connector):
        return (1,)

    left, right = expression.args.values()

    if isinstance(expression, exp.And if dnf else exp.Or):
        return tuple(
            a + b for a in _predicate_lengths(left, dnf) for b in _predicate_lengths(right, dnf)
        )
    return _predicate_lengths(left, dnf) + _predicate_lengths(right, dnf)


def distributive_law(expression, dnf, max_distance, cache=None):
    """
    x OR (y AND z) -> (x OR y) AND (x OR z)
    (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
    """
    if normalized(expression, dnf=dnf):
        return expression

    distance = normalization_distance(expression, dnf=dnf)

    if distance > max_distance:
        raise OptimizeError(f"Normalization distance {distance} exceeds max {max_distance}")

    exp.replace_children(expression, lambda e: distributive_law(e, dnf, max_distance, cache))
    to_exp, from_exp = (exp.Or, exp.And) if dnf else (exp.And, exp.Or)

    if isinstance(expression, from_exp):
        a, b = expression.unnest_operands()

        from_func = exp.and_ if from_exp == exp.And else exp.or_
        to_func = exp.and_ if to_exp == exp.And else exp.or_

        if isinstance(a, to_exp) and isinstance(b, to_exp):
            if len(tuple(a.find_all(exp.Connector))) > len(tuple(b.find_all(exp.Connector))):
                return _distribute(a, b, from_func, to_func, cache)
            return _distribute(b, a, from_func, to_func, cache)
        if isinstance(a, to_exp):
            return _distribute(b, a, from_func, to_func, cache)
        if isinstance(b, to_exp):
            return _distribute(a, b, from_func, to_func, cache)

    return expression


def _distribute(a, b, from_func, to_func, cache):
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

    return _simplify(a, cache)


def _simplify(node, cache):
    node = uniq_sort(flatten(node), cache)
    exp.replace_children(node, lambda n: _simplify(n, cache))
    return node
