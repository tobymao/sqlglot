from dataclasses import dataclass
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
    expr: Expression


@dataclass
class Remove:
    expr: Expression


@dataclass
class Keep:
    expr_src: Expression
    expr_tgt: Expression


def diff(expr_src, expr_tgt):
    """
    Returns changes between the source and the target expressions.

    The underlying logic relies on a variance of the Myers diff algorithm with a custom heuristics for checking
    equality between nodes. Provided the Myers algorithm operates on lists, the source and the target expressions
    are first unfolded into their list representations using the topological order.

    Examples:
        >>> diff(parse_one("a + b"), parse_one("a + c"))
        [
            Keep(
                expr_src=(ADD this: ...),
                expr_tgt=(ADD this: ...)
            ),
            Keep(
                expr_src=(COLUMN this: (IDENTIFIER this: a, quoted: False)),
                expr_tgt=(COLUMN this: (IDENTIFIER this: a, quoted: False))
            ),
            Remove(expr=(COLUMN this: (IDENTIFIER this: b, quoted: False))),
            Insert(expr=(COLUMN this: (IDENTIFIER this: c, quoted: False)))
        ]

    Args:
        expr_src (sqlglot.Expression): the source expression.
        expr_tgt (sqlglot.Expression): the target expression against which the diff should be calculated.
    Returns:
        the list of Insert, Remove and Keep objects for each node in the source and the target expression trees.
        This list represents a sequence of steps needed to transform the source expression tree into the target one.
    """
    expr_list_src = _expr_to_list(expr_src)
    expr_list_tgt = _expr_to_list(expr_tgt)
    return _myers_diff(expr_list_src, expr_list_tgt)


def _myers_diff(expr_list_src, expr_list_tgt):
    front = {1: (0, [])}

    for d in range(0, len(expr_list_src) + len(expr_list_tgt) + 1):
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

            if 1 <= y <= len(expr_list_tgt) and go_down:
                history.append(Insert(expr_list_tgt[y - 1]))
            elif 1 <= x <= len(expr_list_src):
                history.append(Remove(expr_list_src[x - 1]))

            while (
                x < len(expr_list_src)
                and y < len(expr_list_tgt)
                and _is_expr_unchanged(expr_list_src[x], expr_list_tgt[y])
            ):
                history.append(Keep(expr_list_src[x], expr_list_tgt[y]))
                x += 1
                y += 1

            if x >= len(expr_list_src) and y >= len(expr_list_tgt):
                return history

            front[k] = x, history

    raise DiffError("Unexpected state")


def _is_expr_unchanged(expr_src, expr_tgt):
    equals_exprs = [Literal, Column, Table, TableAlias]
    this_equals_exprs = [DataType, Anonymous]

    if any(_all_same_type(t, expr_src, expr_tgt) for t in equals_exprs):
        return expr_src == expr_tgt

    if any(_all_same_type(t, expr_src, expr_tgt) for t in this_equals_exprs):
        return expr_src.this == expr_tgt.this

    return type(expr_src) is type(expr_tgt)


EXCLUDED_EXPR_TYPES = (Identifier,)


def _expr_to_list(expr):
    return [
        n[0]
        for n in _normalize_args_order(expr.copy()).walk()
        if not isinstance(n[0], EXCLUDED_EXPR_TYPES)
    ]


def _normalize_args_order(expr):
    for arg in _expression_only_args(expr):
        _normalize_args_order(arg)

    if isinstance(expr, (Group, In, Select, With)):
        for k, a in expr.args.items():
            if isinstance(a, list) and all(
                isinstance(a_item, Expression) for a_item in a
            ):
                expr.args[k] = sorted(a, key=Expression.sql)
    elif isinstance(expr, Connector):
        args = [expr.this, expr.args.get("expression")]
        args = sorted(args, key=Expression.sql)
        expr.args["this"], expr.args["expression"] = args

    return expr


def _expression_only_args(expr):
    args = []
    if expr:
        for a in expr.args.values():
            args.extend(ensure_list(a))
    return [a for a in args if isinstance(a, Expression)]


def _all_same_type(tpe, *args):
    return all(isinstance(a, tpe) for a in args)
