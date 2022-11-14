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
    return expression


def add_text_to_concat(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Add) and node.type in exp.DataType.TEXT_TYPES:
        node = exp.Concat(this=node.this, expression=node.expression)
    return node


def coerce_type(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Binary):
        for a, b in itertools.permutations(node.args.values()):
            if a.type == exp.DataType.Type.DATE and b.type != exp.DataType.Type.DATE:
                b.replace(exp.Cast(this=b.copy(), to=exp.DataType.build("date")))
    return node
