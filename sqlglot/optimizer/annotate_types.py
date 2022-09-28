from sqlglot import exp
from sqlglot.helper import ensure_list

ANNOTATORS = {
    **{
        expr_type: lambda expr, schema, annotators, coerces_to: _annotate_operator(expr, schema, annotators, coerces_to)
        for expr_type in exp.ALL_OPERATORS
    },
    exp.Cast: lambda expr, schema, annotators, coerces_to: _annotate_cast(expr, schema, annotators, coerces_to),
    exp.DataType: lambda expr, schema, annotators, coerces_to: _annotate_data_type(
        expr, schema, annotators, coerces_to
    ),
    exp.Literal: lambda expr, schema, annotators, coerces_to: _annotate_literal(expr, schema, annotators, coerces_to),
    exp.Boolean: lambda expr, schema, annotators, coerces_to: _annotate_boolean(expr, schema, annotators, coerces_to),
}

# Reference: https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
COERCES_TO = {
    # CHAR < NCHAR < VARCHAR < NVARCHAR < TEXT
    exp.DataType.Type.TEXT: set(),
    exp.DataType.Type.NVARCHAR: {exp.DataType.Type.TEXT},
    exp.DataType.Type.VARCHAR: {exp.DataType.Type.NVARCHAR, exp.DataType.Type.TEXT},
    exp.DataType.Type.NCHAR: {exp.DataType.Type.VARCHAR, exp.DataType.Type.NVARCHAR, exp.DataType.Type.TEXT},
    exp.DataType.Type.CHAR: {
        exp.DataType.Type.NCHAR,
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.NVARCHAR,
        exp.DataType.Type.TEXT,
    },
    # TINYINT < SMALLINT < INT < BIGINT < DECIMAL < FLOAT < DOUBLE
    exp.DataType.Type.DOUBLE: set(),
    exp.DataType.Type.FLOAT: {exp.DataType.Type.DOUBLE},
    exp.DataType.Type.DECIMAL: {exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE},
    exp.DataType.Type.BIGINT: {exp.DataType.Type.DECIMAL, exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE},
    exp.DataType.Type.INT: {
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.DECIMAL,
        exp.DataType.Type.FLOAT,
        exp.DataType.Type.DOUBLE,
    },
    exp.DataType.Type.SMALLINT: {
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.DECIMAL,
        exp.DataType.Type.FLOAT,
        exp.DataType.Type.DOUBLE,
    },
    exp.DataType.Type.TINYINT: {
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.DECIMAL,
        exp.DataType.Type.FLOAT,
        exp.DataType.Type.DOUBLE,
    },
    # DATE < DATETIME < TIMESTAMP < TIMESTAMPTZ < TIMESTAMPLTZ
    exp.DataType.Type.TIMESTAMPLTZ: set(),
    exp.DataType.Type.TIMESTAMPTZ: {exp.DataType.Type.TIMESTAMPLTZ},
    exp.DataType.Type.TIMESTAMP: {exp.DataType.Type.TIMESTAMPTZ, exp.DataType.Type.TIMESTAMPLTZ},
    exp.DataType.Type.DATETIME: {
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPTZ,
        exp.DataType.Type.TIMESTAMPLTZ,
    },
    exp.DataType.Type.DATE: {
        exp.DataType.Type.DATETIME,
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPTZ,
        exp.DataType.Type.TIMESTAMPLTZ,
    },
}


def annotate_types(expression, schema=None, annotators=ANNOTATORS, coerces_to=COERCES_TO):
    """
    Recursively infer & annotate types in an expression syntax tree against a schema.

    (TODO -- replace this with a better example after adding some functionality)
    Example:
        >>> import sqlglot
        >>> annotated_expression = annotate_types(sqlglot.parse_one('5 + 5.3'))
        >>> annotated_expression.type
        <Type.DOUBLE: 'DOUBLE'>

    Args:
        expression (sqlglot.Expression): Expression to annotate.
        schema (dict|sqlglot.optimizer.Schema): Database schema.
        annotators (dict): Maps expression type to corresponding annotation function.
        coerces_to (dict): Maps expression type to set of types that it can be coerced into.
    Returns:
        sqlglot.Expression: expression annotated with types
    """

    if not isinstance(expression, exp.Expression):
        return None

    annotator = annotators.get(expression.__class__, _annotate_args)
    return annotator(expression, schema, annotators, coerces_to)


def _annotate_args(expression, schema, annotators, coerces_to):
    for value in expression.args.values():
        for v in ensure_list(value):
            annotate_types(v, schema, annotators, coerces_to)

    return expression


def _annotate_cast(expr, schema, annotators, coerces_to):
    expr.type = expr.args["to"].this
    return _annotate_args(expr, schema, annotators, coerces_to)


def _annotate_data_type(expr, schema, annotators, coerces_to):
    expr.type = expr.this
    return _annotate_args(expr, schema, annotators, coerces_to)


def _maybe_coerce(type1, type2):
    return type2 if type2 in COERCES_TO[type1] else type1


def _annotate_operator(expression, schema, annotators, coerces_to):
    _annotate_args(expression, schema, annotators, coerces_to)

    if isinstance(expression, (exp.Condition, exp.Predicate)) and not isinstance(expression, exp.Paren):
        expression.type = exp.DataType.Type.BOOLEAN
    elif isinstance(expression, exp.Binary):
        expression.type = _maybe_coerce(expression.left.type, expression.right.type)
    else:
        expression.type = expression.this.type

    return expression


def _annotate_literal(expression, schema, annotators, coerces_to):
    if expression.is_string:
        expression.type = exp.DataType.Type.VARCHAR
    elif expression.is_int:
        expression.type = exp.DataType.Type.INT
    else:
        expression.type = exp.DataType.Type.DOUBLE

    return expression


def _annotate_boolean(expression, schema, annotators, coerces_to):
    expression.type = exp.DataType.Type.BOOLEAN
    return expression
