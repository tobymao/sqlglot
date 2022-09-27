from sqlglot import exp
from sqlglot.errors import OptimizeError


def annotate_expression_types(expression, schema):
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
    Returns:
        sqlglot.Expression: expression annotated with types
    """

    return TypeAnnotator(schema).annotate(expression)


class TypeAnnotator:
    """
    TypeAnnotator provides a method for each type of expression, responsible for
    inferring the type of the corresponding expression and then annotating it.

    Args:
        schema (dict|sqlglot.optimizer.Schema): Database schema.
    """

    ANNOTATORS = {
        exp.Select: lambda self, expr: self._annotate_select(expr),
        exp.Literal: lambda self, expr: self._annotate_literal(expr),
        exp.Boolean: lambda self, expr: self._annotate_boolean(expr),
    }

    def __init__(self, schema):
        self.schema = schema

    def annotate(self, expression):
        if not expression:
            return None

        # Note: maybe there are some expressions that can be allowed to *not* have a type.
        # In this case, instead of raising we could return None instead. Other alternatives:
        #
        # - Always return None if there's no available annotator method (too flexible maybe?).
        # - Provide a default annotator returning None for expressions we don't care to annotate.

        annotator = self.ANNOTATORS.get(type(expression))
        if not annotator:
            raise OptimizeError(f"Unable to annotate expression of type: {type(expression)}")

        return annotator(self, expression)

    def _annotate_select(self, expression):
        for arg, value in expression.args.items():
            if isinstance(value, list):
                expression.set(arg, [self.annotate(child_expression) for child_expression in value])
            else:
                expression.set(arg, self.annotate(value))

        return expression

    def _annotate_literal(self, expression):
        if expression.is_string:
            type_ = exp.DataType.Type.VARCHAR
        elif expression.is_int:
            type_ = exp.DataType.Type.INT
        else:
            type_ = exp.DataType.Type.FLOAT

        expression.set("type", type_)
        return expression

    def _annotate_boolean(self, expression):
        expression.set("type", exp.DataType.Type.BOOLEAN)
        return expression
