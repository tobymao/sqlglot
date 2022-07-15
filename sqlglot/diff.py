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
    Join,
    In,
    Literal,
    Table,
    TableAlias,
    With,
)
from sqlglot.errors import DiffError
from sqlglot.helper import ensure_list


EXCLUDED_EXPRESSION_TYPES = {Identifier}
SORT_ARGS_EXPRESSION_TYPES = {Connector, Group, In, With}


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


def diff(
    source,
    target,
    excluded_expression_types=None,
    sort_args_expression_types=None,
):
    """
    Returns changes between the source and the target expressions.

    The underlying logic relies on a variance of the Myers diff algorithm with a custom heuristics for checking
    equality between nodes. Provided the Myers algorithm operates on lists, the source and the target expressions
    are first unfolded into their list representations using the topological order.

    NOTE: this functionality is largely based on heuristics. It does a pretty good job at estimating which AST nodes have
    been impacted but neither accuracy nor optimality of this estimate are guaranteed. Use it at your own risk.

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
        target (sqlglot.Exexcluded_expression_typespression): the target expression against which the diff should be calculated.
        excluded_expression_types (optional): the set of AST node types which should be excluded from the comparison.
        sort_args_expression_types (optional): the set of AST node types for which arguments should be sorted. This parameter
            can be used to tweak the accuracy of the algorithm. For example if a calling code expects the two ASTs to be mostly
            the same and the position of a majority of nodes is expected to remain unchanged, it may make sense to omit sorting
            for some nodes.
    Returns:
        the list of Insert, Remove and Keep objects for each node in the source and the target expression trees.
        This list represents a sequence of steps needed to transform the source expression tree into the target one.
    """
    if excluded_expression_types is None:
        excluded_expression_types = EXCLUDED_EXPRESSION_TYPES
    if sort_args_expression_types is None:
        sort_args_expression_types = SORT_ARGS_EXPRESSION_TYPES

    source_expression_list = _expression_to_list(
        source, excluded_expression_types, sort_args_expression_types
    )
    target_expression_list = _expression_to_list(
        target, excluded_expression_types, sort_args_expression_types
    )
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

    if _all_same_type(Join, source, target):
        return source.args.get("side") == target.args.get("side")

    return type(source) is type(target)


def _expression_to_list(
    expression, excluded_expression_types, sort_args_expression_types
):
    return [
        n[0]
        for n in _normalize_args_order(
            expression.copy(), sort_args_expression_types
        ).bfs()
        if not isinstance(n[0], tuple(excluded_expression_types))
    ]


def _normalize_args_order(expression, sort_args_expression_types):
    for arg in _expression_only_args(expression):
        _normalize_args_order(arg, sort_args_expression_types)

    if isinstance(expression, Connector) and Connector in sort_args_expression_types:
        unfolded_args = sorted(expression.flatten(), key=Expression.sql)
        folded_args = reduce(
            lambda l, r: type(expression)(this=l, expression=r), unfolded_args[:-1]
        )
        expression.set("this", folded_args)
        expression.set("expression", unfolded_args[-1])
    elif isinstance(expression, tuple(sort_args_expression_types)):
        for k, a in expression.args.items():
            if isinstance(a, list) and all(
                isinstance(a_item, Expression) for a_item in a
            ):
                expression.args[k] = sorted(a, key=Expression.sql)

    return expression


def _expression_only_args(expression):
    args = []
    if expression:
        for a in expression.args.values():
            args.extend(ensure_list(a))
    return [a for a in args if isinstance(a, Expression)]


def _all_same_type(tpe, *args):
    return all(isinstance(a, tpe) for a in args)
