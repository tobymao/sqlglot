from dataclasses import dataclass
from functools import reduce
from sqlglot.expressions import (
    Anonymous,
    Column,
    Connector,
    DataType,
    Expression,
    Group,
    Identifier,
    In,
    Literal,
    Select,
    Table,
    TableAlias,
    With,
)
from sqlglot.errors import DiffError
from sqlglot.helper import ensure_list


@dataclass
class Insert:
    expression: Expression


@dataclass
class Remove:
    expression: Expression


@dataclass
class Keep:
    source: Expression
    target: Expression


def diff(source, target):
    """
    Returns changes between the source and the target expressions.

    The underlying logic relies on a variance of the Myers diff algorithm with a custom heuristics for checking
    equality between nodes. Provided the Myers algorithm operates on lists, the source and the target expressions
    are first unfolded into their list representations using the topological order.

    Examples:
        >>> diff(parse_one("a + b"), parse_one("a + c"))
        [
            Keep(
                source=(ADD this: ...),
                target=(ADD this: ...)
            ),
            Keep(
                source=(COLUMN this: (IDENTIFIER this: a, quoted: False)),
                target=(COLUMN this: (IDENTIFIER this: a, quoted: False))
            ),
            Remove(expression=(COLUMN this: (IDENTIFIER this: b, quoted: False))),
            Insert(expression=(COLUMN this: (IDENTIFIER this: c, quoted: False)))
        ]

    Args:
        source (sqlglot.Expression): the source expression.
        target (sqlglot.Expression): the target expression against which the diff should be calculated.
    Returns:
        the list of Insert, Remove and Keep objects for each node in the source and the target expression trees.
        This list represents a sequence of steps needed to transform the source expression tree into the target one.
    """
    source_expression_list = _expression_to_list(source)
    target_expression_list = _expression_to_list(target)
    return _myers_diff(source_expression_list, target_expression_list)


def _myers_diff(source, target):
    front = {1: (0, [])}

    for d in range(0, len(source) + len(target) + 1):
        for k in range(-d, d + 1, 2):
            go_down = k == -d or (k != d and front[k - 1][0] < front[k + 1][0])

            if go_down:
                old_x, history = front[k + 1]
                x = old_x
            else:
                old_x, history = front[k - 1]
                x = old_x + 1
            y = x - k

            history = history[:]

            if 1 <= y <= len(target) and go_down:
                history.append(Insert(target[y - 1]))
            elif 1 <= x <= len(source):
                history.append(Remove(source[x - 1]))

            while (
                x < len(source)
                and y < len(target)
                and _is_expression_unchanged(source[x], target[y])
            ):
                history.append(Keep(source[x], target[y]))
                x += 1
                y += 1

            if x >= len(source) and y >= len(target):
                return history

            front[k] = x, history

    raise DiffError("Unexpected state")


EQUALS_EXPRESSIONS = [Literal, Column, Table, TableAlias]
THIS_EQUALS_EXPRESSIONS = [DataType, Anonymous]


def _is_expression_unchanged(source, target):

    if any(_all_same_type(t, source, target) for t in EQUALS_EXPRESSIONS):
        return source == target

    if any(_all_same_type(t, source, target) for t in THIS_EQUALS_EXPRESSIONS):
        return source.this == target.this

    return type(source) is type(target)


EXCLUDED_EXPRESSION_TYPES = (Identifier,)


def _expression_to_list(expression):
    return [
        n[0]
        for n in _normalize_args_order(expression.copy()).bfs()
        if not isinstance(n[0], EXCLUDED_EXPRESSION_TYPES)
    ]


def _normalize_args_order(expression):
    for arg in _expression_only_args(expression):
        _normalize_args_order(arg)

    if isinstance(expression, (Group, In, Select, With)):
        for k, a in expression.args.items():
            if isinstance(a, list) and all(
                isinstance(a_item, Expression) for a_item in a
            ):
                expression.args[k] = sorted(a, key=Expression.sql)
    elif isinstance(expression, Connector):
        unfolded_args = sorted(expression.flatten(), key=Expression.sql)
        folded_args = reduce(
            lambda l, r: type(expression)(this=l, expression=r), unfolded_args[:-1]
        )
        expression.set("this", folded_args)
        expression.set("expression", unfolded_args[-1])

    return expression


def _expression_only_args(expression):
    args = []
    if expression:
        for a in expression.args.values():
            args.extend(ensure_list(a))
    return [a for a in args if isinstance(a, Expression)]


def _all_same_type(tpe, *args):
    return all(isinstance(a, tpe) for a in args)
