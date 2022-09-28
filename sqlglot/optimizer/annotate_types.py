from sqlglot import exp
from sqlglot.helper import ensure_list, subclasses


def annotate_types(expression, schema=None, annotators=None, coerces_to=None):
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

    return TypeAnnotator(schema, annotators, coerces_to).annotate(expression)


class TypeAnnotator:
    ANNOTATORS = {
        **{
            expr_type: lambda self, expr: self._annotate_unary(expr)
            for expr_type in subclasses(exp.__name__, exp.Unary)
        },
        **{
            expr_type: lambda self, expr: self._annotate_binary(expr)
            for expr_type in subclasses(exp.__name__, exp.Binary)
        },
        exp.Cast: lambda self, expr: self._annotate_cast(expr),
        exp.DataType: lambda self, expr: self._annotate_data_type(expr),
        exp.Literal: lambda self, expr: self._annotate_literal(expr),
        exp.Boolean: lambda self, expr: self._annotate_boolean(expr),
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

    def __init__(self, schema=None, annotators=None, coerces_to=None):
        self.schema = schema
        self.annotators = annotators or self.ANNOTATORS
        self.coerces_to = coerces_to or self.COERCES_TO

    def annotate(self, expression):
        if not isinstance(expression, exp.Expression):
            return None

        annotator = self.annotators.get(expression.__class__)
        return annotator(self, expression) if annotator else self._annotate_args(expression)

    def _annotate_args(self, expression):
        for value in expression.args.values():
            for v in ensure_list(value):
                self.annotate(v)

        return expression

    def _annotate_cast(self, expression):
        expression.type = expression.args["to"].this
        return self._annotate_args(expression)

    def _annotate_data_type(self, expression):
        expression.type = expression.this
        return self._annotate_args(expression)

    def _maybe_coerce(self, type1, type2):
        return type2 if type2 in self.coerces_to[type1] else type1

    def _annotate_binary(self, expression):
        self._annotate_args(expression)

        if isinstance(expression, (exp.Condition, exp.Predicate)):
            expression.type = exp.DataType.Type.BOOLEAN
        else:
            expression.type = self._maybe_coerce(expression.left.type, expression.right.type)

        return expression

    def _annotate_unary(self, expression):
        self._annotate_args(expression)

        if isinstance(expression, exp.Condition) and not isinstance(expression, exp.Paren):
            expression.type = exp.DataType.Type.BOOLEAN
        else:
            expression.type = expression.this.type

        return expression

    def _annotate_literal(self, expression):
        if expression.is_string:
            expression.type = exp.DataType.Type.VARCHAR
        elif expression.is_int:
            expression.type = exp.DataType.Type.INT
        else:
            expression.type = exp.DataType.Type.DOUBLE

        return expression

    def _annotate_boolean(self, expression):
        expression.type = exp.DataType.Type.BOOLEAN
        return expression
