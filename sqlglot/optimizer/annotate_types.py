from sqlglot import exp
from sqlglot.helper import ensure_list, subclasses
from sqlglot.optimizer.schema import ensure_schema
from sqlglot.optimizer.scope import Scope, traverse_scope


def annotate_types(expression, schema=None, annotators=None, coerces_to=None):
    """
    Recursively infer & annotate types in an expression syntax tree against a schema.
    Assumes that we've already executed the optimizer's qualify_columns step.

    Example:
        >>> import sqlglot
        >>> schema = {"y": {"cola": "SMALLINT"}}
        >>> sql = "SELECT x.cola + 2.5 AS cola FROM (SELECT y.cola AS cola FROM y AS y) AS x"
        >>> annotated_expr = annotate_types(sqlglot.parse_one(sql), schema=schema)
        >>> annotated_expr.expressions[0].type  # Get the type of "x.cola + 2.5 AS cola"
        <Type.DOUBLE: 'DOUBLE'>

    Args:
        expression (sqlglot.Expression): Expression to annotate.
        schema (dict|sqlglot.optimizer.Schema): Database schema.
        annotators (dict): Maps expression type to corresponding annotation function.
        coerces_to (dict): Maps expression type to set of types that it can be coerced into.
    Returns:
        sqlglot.Expression: expression annotated with types
    """

    schema = ensure_schema(schema)

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
        **{
            expr_type: lambda self, expr: self._annotate_function(expr)
            for expr_type in subclasses(exp.__name__, exp.Func)
        },
        exp.Cast: lambda self, expr: self._annotate_cast(expr),
        exp.DataType: lambda self, expr: self._annotate_data_type(expr),
        exp.Alias: lambda self, expr: self._annotate_unary(expr),
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

    FUNCTION_TYPES = {
        exp.Anonymous: exp.DataType.Type.UNKNOWN,
        exp.ApproxDistinct: exp.DataType.Type.BIGINT,
        exp.Avg: exp.DataType.Type.DOUBLE,
        exp.Ceil: exp.DataType.Type.INT,
        exp.Count: exp.DataType.Type.BIGINT,
        exp.CurrentDate: exp.DataType.Type.DATE,
        exp.CurrentDatetime: exp.DataType.Type.DATETIME,
        exp.CurrentTime: exp.DataType.Type.TIMESTAMP,
        exp.CurrentTimestamp: exp.DataType.Type.TIMESTAMP,
        exp.DateAdd: exp.DataType.Type.DATE,
        exp.DateSub: exp.DataType.Type.DATE,
        exp.DateDiff: exp.DataType.Type.INT,
        exp.DatetimeAdd: exp.DataType.Type.DATETIME,
        exp.DatetimeSub: exp.DataType.Type.DATETIME,
        exp.DatetimeDiff: exp.DataType.Type.INT,
        exp.Extract: exp.DataType.Type.INT,
        exp.TimestampAdd: exp.DataType.Type.TIMESTAMP,
        exp.TimestampSub: exp.DataType.Type.TIMESTAMP,
        exp.TimestampDiff: exp.DataType.Type.INT,
        exp.TimeAdd: exp.DataType.Type.TIMESTAMP,
        exp.TimeSub: exp.DataType.Type.TIMESTAMP,
        exp.TimeDiff: exp.DataType.Type.INT,
        exp.DateStrToDate: exp.DataType.Type.DATE,
        exp.DateToDateStr: exp.DataType.Type.VARCHAR,
        exp.DateToDi: exp.DataType.Type.INT,
        exp.Day: exp.DataType.Type.TINYINT,
        exp.DiToDate: exp.DataType.Type.DATE,
        exp.Exp: exp.DataType.Type.DOUBLE,
        exp.Floor: exp.DataType.Type.INT,
        exp.If: exp.DataType.Type.BOOLEAN,
        exp.Initcap: exp.DataType.Type.VARCHAR,
        exp.Length: exp.DataType.Type.BIGINT,
        exp.Levenshtein: exp.DataType.Type.INT,
        exp.Ln: exp.DataType.Type.DOUBLE,
        exp.Log: exp.DataType.Type.DOUBLE,
        exp.Log2: exp.DataType.Type.DOUBLE,
        exp.Log10: exp.DataType.Type.DOUBLE,
        exp.Lower: exp.DataType.Type.VARCHAR,
        exp.Month: exp.DataType.Type.TINYINT,
        exp.Pow: exp.DataType.Type.DOUBLE,
        exp.Quantile: exp.DataType.Type.DOUBLE,
        exp.ApproxQuantile: exp.DataType.Type.DOUBLE,
        exp.RegexpLike: exp.DataType.Type.BOOLEAN,
        exp.Round: exp.DataType.Type.DOUBLE,
        exp.SafeDivide: exp.DataType.Type.DOUBLE,
        exp.Substring: exp.DataType.Type.VARCHAR,
        exp.StrPosition: exp.DataType.Type.INT,
        exp.StrToDate: exp.DataType.Type.DATE,
        exp.StrToTime: exp.DataType.Type.TIMESTAMP,
        exp.Sqrt: exp.DataType.Type.DOUBLE,
        exp.Stddev: exp.DataType.Type.DOUBLE,
        exp.StddevPop: exp.DataType.Type.DOUBLE,
        exp.StddevSamp: exp.DataType.Type.DOUBLE,
        exp.TimeToStr: exp.DataType.Type.VARCHAR,
        exp.TimeToTimeStr: exp.DataType.Type.VARCHAR,
        exp.TimeStrToDate: exp.DataType.Type.DATE,
        exp.TimeStrToTime: exp.DataType.Type.TIMESTAMP,
        exp.Trim: exp.DataType.Type.VARCHAR,
        exp.TsOrDsToDateStr: exp.DataType.Type.VARCHAR,
        exp.TsOrDsToDate: exp.DataType.Type.DATE,
        exp.TsOrDiToDi: exp.DataType.Type.INT,
        exp.UnixToStr: exp.DataType.Type.VARCHAR,
        exp.UnixToTime: exp.DataType.Type.TIMESTAMP,
        exp.UnixToTimeStr: exp.DataType.Type.VARCHAR,
        exp.Upper: exp.DataType.Type.VARCHAR,
        exp.Variance: exp.DataType.Type.DOUBLE,
        exp.VariancePop: exp.DataType.Type.DOUBLE,
        exp.Week: exp.DataType.Type.TINYINT,
        exp.Year: exp.DataType.Type.TINYINT,
    }

    TRAVERSABLES = (exp.Select, exp.Union, exp.UDTF, exp.Subquery)

    def __init__(self, schema=None, annotators=None, coerces_to=None):
        self.schema = schema
        self.annotators = annotators or self.ANNOTATORS
        self.coerces_to = coerces_to or self.COERCES_TO

    def annotate(self, expression):
        if isinstance(expression, self.TRAVERSABLES):
            for scope in traverse_scope(expression):
                subscope_selects = {
                    name: {select.alias_or_name: select for select in source.selects}
                    for name, source in scope.sources.items()
                    if isinstance(source, Scope)
                }

                # First annotate the current scope's column references
                for col in scope.columns:
                    source = scope.sources[col.table]
                    if isinstance(source, exp.Table):
                        col.type = self.schema.get_column_type(source, col)
                    else:
                        col.type = subscope_selects[col.table][col.name].type

                # Then (possibly) annotate the remaining expressions in the scope
                self._maybe_annotate(scope.expression)

        return self._maybe_annotate(expression)  # This takes care of non-traversable expressions

    def _maybe_annotate(self, expression):
        if not isinstance(expression, exp.Expression):
            return None

        if expression.type:
            return expression  # We've already inferred the expression's type

        annotator = self.annotators.get(expression.__class__)
        return annotator(self, expression) if annotator else self._annotate_args(expression, set_type=True)

    def _annotate_args(self, expression, set_type=False):
        for value in expression.args.values():
            for v in ensure_list(value):
                self._maybe_annotate(v)

        if set_type:
            expression.type = exp.DataType.Type.UNKNOWN

        return expression

    def _annotate_cast(self, expression):
        expression.type = expression.args["to"].this
        return self._annotate_args(expression)

    def _annotate_data_type(self, expression):
        expression.type = expression.this
        return self._annotate_args(expression)

    def _maybe_coerce(self, type1, type2):
        if exp.DataType.Type.UNKNOWN in (type1, type2):
            return exp.DataType.Type.UNKNOWN  # We propagate the UNKNOWN type upwards if found

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

    def _annotate_function(self, expression):
        expression.type = self.FUNCTION_TYPES.get(expression.__class__) or exp.DataType.Type.UNKNOWN
        return self._annotate_args(expression)

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
