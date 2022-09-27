from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import ensure_list

ANNOTATORS = {
    exp.Cast: lambda expr, schema, annotators: _annotate_cast(expr, schema, annotators),
    exp.DataType: lambda expr, schema, annotators: _annotate_data_type(expr, schema, annotators),
    exp.Select: lambda expr, schema, annotators: _annotate_select(expr, schema, annotators),
    exp.Literal: lambda expr, schema, annotators: _annotate_literal(expr, schema, annotators),
    exp.Boolean: lambda expr, schema, annotators: _annotate_boolean(expr, schema, annotators),
}


def annotate_expression_types(expression, schema=None, annotators=ANNOTATORS):
    """
    Recursively infer & annotate types in an expression syntax tree against a schema.

    (TODO -- replace this with a better example after adding some functionality)
    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('SELECT 5')
        >>> annotate_expression_types(sqlglot.parse_one('SELECT 5'), None)
        (SELECT expressions:
          (LITERAL this: 5, is_string: False, type: Type.INT))

    Args:
        expression (sqlglot.Expression): Expression to annotate.
        schema (dict|sqlglot.optimizer.Schema): Database schema.
        annotators (dict): Maps expression type to corresponding annotation function.
    Returns:
        sqlglot.Expression: expression annotated with types
    """

    return annotate(expression, schema, annotators)


def annotate(expression, schema, annotators):
    if not expression:
        return None

    # Note: maybe there are some expressions that can be allowed to *not* have a type.
    # In this case, instead of raising we could return None instead. Other alternatives:
    #
    # - Always return None if there's no available annotator method (too flexible maybe?).
    # - Provide a default annotator returning None for expressions we don't care to annotate.

    annotator = annotators.get(type(expression))
    if not annotator:
        raise OptimizeError(f"Unable to annotate expression of type: {type(expression)}")

    return annotator(expression, schema, annotators)


def _annotate_cast(expr, schema, annotators):
    expr.type = expr.args["to"].this

    annotate(expr.this, schema, annotators)
    annotate(expr.args["to"], schema, annotators)

    return expr


def _annotate_data_type(expr, schema, annotators):
    expr.type = expr.this

    for expression in expr.expressions:
        annotate(expression, schema, annotators)

    return expr


def _annotate_select(expression, schema, annotators):
    for value in expression.args.values():
        for v in ensure_list(value):
            annotate(v, schema, annotators)

    return expression


def _annotate_literal(expression, schema, annotators):
    if expression.is_string:
        expression.type = exp.DataType.Type.VARCHAR
    elif expression.is_int:
        expression.type = exp.DataType.Type.INT
    else:
        expression.type = exp.DataType.Type.FLOAT

    return expression


def _annotate_boolean(expression, schema, annotators):
    expression.type = exp.DataType.Type.BOOLEAN
    return expression
