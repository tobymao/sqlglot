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
    expression = coerce_type(expression)
    expression = remove_redundant_casts(expression)

    if isinstance(expression, exp.Identifier):
        expression.set("quoted", True)

    return expression


def add_text_to_concat(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Add) and node.type and node.type.this in exp.DataType.TEXT_TYPES:
        node = exp.Concat(this=node.this, expression=node.expression)
    return node


def coerce_type(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Binary):
        _coerce_date(node.left, node.right)
    elif isinstance(node, exp.Between):
        _coerce_date(node.this, node.args["low"])
    elif isinstance(node, exp.Extract):
        if node.expression.type.this not in exp.DataType.TEMPORAL_TYPES:
            _replace_cast(node.expression, "datetime")
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


def _coerce_date(a: exp.Expression, b: exp.Expression) -> None:
    for a, b in itertools.permutations([a, b]):
        if (
            a.type
            and a.type.this == exp.DataType.Type.DATE
            and b.type
            and b.type.this != exp.DataType.Type.DATE
        ):
            _replace_cast(b, "date")


def _replace_cast(node: exp.Expression, to: str) -> None:
    data_type = exp.DataType.build(to)
    cast = exp.Cast(this=node.copy(), to=data_type)
    cast.type = data_type
    node.replace(cast)
