from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import ensure_list

ANNOTATORS = {
    exp.Cast: lambda expr, schema, annotators: _annotate_cast(expr, schema, annotators),
    exp.Cache: lambda expr, schema, annotators: _annotate_cache(expr, schema, annotators),
    exp.Uncache: lambda: None,
    exp.DataType: lambda expr, schema, annotators: _annotate_data_type(expr, schema, annotators),
    exp.Select: lambda expr, schema, annotators: _annotate_select(expr, schema, annotators),
    exp.Paren: lambda expr, schema, annotators: _annotate_paren(expr, schema, annotators),
    exp.Add: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.Sub: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.Mul: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.Div: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.Mod: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.DPipe: lambda expr, schema, annotators: _annotate_binary(expr, schema, annotators),
    exp.And: lambda expr, schema, annotators: _annotate_connector(expr, schema, annotators),
    exp.Or: lambda expr, schema, annotators: _annotate_connector(expr, schema, annotators),
    exp.Literal: lambda expr, schema, annotators: _annotate_literal(expr, schema, annotators),
    exp.Boolean: lambda expr, schema, annotators: _annotate_boolean(expr, schema, annotators),
}

# This maps a type to a set of related types with greater precedence,
# eg. exp.DataType.Type.FLOAT is mapped to {exp.DataType.Type.DOUBLE}
PRECEDED_BY = {}


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

    _create_precedence_mapping()
    return _annotate(expression, schema, annotators)


# Note: CTEs can augment the schema. In the following example, we need to lookup the
# schema to find the type of x and thus we should update our schema, after we annotate
# the CTE, at least temporarily.
#
# WITH foo AS (SELECT * FROM bar) SELECT x + 3 FROM foo


def _annotate(expression, schema, annotators):
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


def _coerce(type1, type2):
    return type2 if type2 in PRECEDED_BY[type1] else type1


def _annotate_cast(expr, schema, annotators):
    expr.type = expr.args["to"].this

    _annotate(expr.this, schema, annotators)
    _annotate(expr.args["to"], schema, annotators)

    return expr


def _annotate_cache(expr, schema, annotators):
    _annotate(expr.expression, schema, annotators)
    _annotate(expr.args.get("with"), schema, annotators)

    return expr


def _annotate_data_type(expr, schema, annotators):
    expr.type = expr.this

    for expression in expr.expressions:
        _annotate(expression, schema, annotators)

    return expr


def _annotate_select(expression, schema, annotators):
    for value in expression.args.values():
        for v in ensure_list(value):
            _annotate(v, schema, annotators)

    return expression


def _annotate_paren(expression, schema, annotators):
    _annotate(expression.this, schema, annotators)

    expression.type = expression.this.type

    return expression


def _annotate_binary(expression, schema, annotators):
    _annotate(expression.left, schema, annotators)
    _annotate(expression.right, schema, annotators)

    expression.type = _coerce(expression.left.type, expression.right.type)

    return expression


def _annotate_connector(expr, schema, annotators):
    _annotate(expression.left, schema, annotators)
    _annotate(expression.right, schema, annotators)

    expression.type = exp.DataType.Type.BOOLEAN

    return expression


def _annotate_literal(expression, schema, annotators):
    if expression.is_string:
        expression.type = exp.DataType.Type.VARCHAR
    elif expression.is_int:
        expression.type = exp.DataType.Type.INT
    else:
        expression.type = exp.DataType.Type.DOUBLE

    return expression


def _annotate_boolean(expression, schema, annotators):
    expression.type = exp.DataType.Type.BOOLEAN
    return expression


def _set_precedence(types):
    """
    Sets the correct precedence for a set of related types.

    Example:
        >>> PRECEDED_BY = {}
        >>> types = [exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT]
        >>> _set_precedence(types)
        >>> print(PRECEDED_BY)
        {exp.DataType.Type.DOUBLE: set(), exp.DataType.Type.FLOAT: exp.DataType.Type.DOUBLE}

    Args:
        types (list[exp.DataType.Type]): types, in descending order w.r.t their relative precedence
    """

    PRECEDED_BY[types[0]] = set()
    for t1, t2 in zip(types[:-1], types[1:]):
        PRECEDED_BY[t2] = {t1}.union(PRECEDED_BY[t1])


def _create_precedence_mapping():
    # Reference: https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html

    CHAR_TYPES = [
        exp.DataType.Type.TEXT,
        exp.DataType.Type.NVARCHAR,
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.NCHAR,
        exp.DataType.Type.CHAR,
    ]

    NUMERIC_TYPES = [
        exp.DataType.Type.DOUBLE,
        exp.DataType.Type.FLOAT,
        exp.DataType.Type.DECIMAL,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.INT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
    ]

    TIME_TYPES = [
        exp.DataType.Type.TIMESTAMPLTZ,
        exp.DataType.Type.TIMESTAMPTZ,
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.DATETIME,
        exp.DataType.Type.DATE,
    ]

    _set_precedence(CHAR_TYPES)
    _set_precedence(NUMERIC_TYPES)
    _set_precedence(TIME_TYPES)
