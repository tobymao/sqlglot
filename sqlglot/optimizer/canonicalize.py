from __future__ import annotations

import itertools

from sqlglot import exp


def canonicalize(expression: exp.Expression) -> exp.Expression:
    """Converts a sql expression into a standard form.

    This method relies on annotate_types because many of the
    conversions rely on type inference.

    Args:
        expression: The expression to canonicalize.
    """
    exp.replace_children(expression, canonicalize)

    expression = add_text_to_concat(expression)
    expression = replace_date_funcs(expression)
    expression = coerce_type(expression)
    expression = remove_redundant_casts(expression)
    expression = ensure_bool_predicates(expression)
    expression = remove_ascending_order(expression)

    return expression


def add_text_to_concat(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Add) and node.type and node.type.this in exp.DataType.TEXT_TYPES:
        node = exp.Concat(expressions=[node.left, node.right])
    return node


def replace_date_funcs(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Date) and not node.expressions and not node.args.get("zone"):
        return exp.cast(node.this, to=exp.DataType.Type.DATE)
    if isinstance(node, exp.Timestamp) and not node.expression:
        return exp.cast(node.this, to=exp.DataType.Type.TIMESTAMP)
    return node


def coerce_type(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Binary):
        _coerce_date(node.left, node.right)
    elif isinstance(node, exp.Between):
        _coerce_date(node.this, node.args["low"])
    elif isinstance(node, exp.Extract) and not node.expression.type.is_type(
        *exp.DataType.TEMPORAL_TYPES
    ):
        _replace_cast(node.expression, exp.DataType.Type.DATETIME)

    return node


def remove_redundant_casts(expression: exp.Expression) -> exp.Expression:
    if (
        isinstance(expression, exp.Cast)
        and expression.to.type
        and expression.this.type
        and expression.to.type.this == expression.this.type.this
    ):
        return expression.this
    return expression


def ensure_bool_predicates(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Connector):
        _replace_int_predicate(expression.left)
        _replace_int_predicate(expression.right)

    elif isinstance(expression, (exp.Where, exp.Having)) or (
        # We can't replace num in CASE x WHEN num ..., because it's not the full predicate
        isinstance(expression, exp.If)
        and not (isinstance(expression.parent, exp.Case) and expression.parent.this)
    ):
        _replace_int_predicate(expression.this)

    return expression


def remove_ascending_order(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Ordered) and expression.args.get("desc") is False:
        # Convert ORDER BY a ASC to ORDER BY a
        expression.set("desc", None)

    return expression


def _coerce_date(a: exp.Expression, b: exp.Expression) -> None:
    for a, b in itertools.permutations([a, b]):
        if (
            a.type
            and a.type.this == exp.DataType.Type.DATE
            and b.type
            and b.type.this not in (exp.DataType.Type.DATE, exp.DataType.Type.INTERVAL)
        ):
            _replace_cast(b, exp.DataType.Type.DATE)


def _replace_cast(node: exp.Expression, to: exp.DataType.Type) -> None:
    node.replace(exp.cast(node.copy(), to=to))


def _replace_int_predicate(expression: exp.Expression) -> None:
    if isinstance(expression, exp.Coalesce):
        for _, child in expression.iter_expressions():
            _replace_int_predicate(child)
    elif expression.type and expression.type.this in exp.DataType.INTEGER_TYPES:
        expression.replace(exp.NEQ(this=expression.copy(), expression=exp.Literal.number(0)))
