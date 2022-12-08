from sqlglot import exp
from sqlglot.helper import ensure_collection, ensure_list, subclasses
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import ensure_schema


def annotate_types(expression, schema=None, annotators=None, coerces_to=None):
    """
    Recursively infer & annotate types in an expression syntax tree against a schema.
    Assumes that we've already executed the optimizer's qualify_columns step.

    Example:
        >>> import sqlglot
        >>> schema = {"y": {"cola": "SMALLINT"}}
        >>> sql = "SELECT x.cola + 2.5 AS cola FROM (SELECT y.cola AS cola FROM y AS y) AS x"
        >>> annotated_expr = annotate_types(sqlglot.parse_one(sql), schema=schema)
        >>> annotated_expr.expressions[0].type.this  # Get the type of "x.cola + 2.5 AS cola"
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
        exp.Cast: lambda self, expr: self._annotate_with_type(expr, expr.args["to"]),
        exp.TryCast: lambda self, expr: self._annotate_with_type(expr, expr.args["to"]),
        exp.DataType: lambda self, expr: self._annotate_with_type(expr, expr),
        exp.Alias: lambda self, expr: self._annotate_unary(expr),
        exp.Between: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.BOOLEAN),
        exp.In: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.BOOLEAN),
        exp.Literal: lambda self, expr: self._annotate_literal(expr),
        exp.Boolean: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.BOOLEAN),
        exp.Null: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.NULL),
        exp.Anonymous: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.UNKNOWN),
        exp.ApproxDistinct: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.BIGINT
        ),
        exp.Avg: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Min: lambda self, expr: self._annotate_by_args(expr, "this"),
        exp.Max: lambda self, expr: self._annotate_by_args(expr, "this"),
        exp.Sum: lambda self, expr: self._annotate_by_args(expr, "this", promote=True),
        exp.Ceil: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.Count: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.BIGINT),
        exp.CurrentDate: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.CurrentDatetime: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DATETIME
        ),
        exp.CurrentTime: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.CurrentTimestamp: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.DateAdd: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.DateSub: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.DateDiff: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.DatetimeAdd: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DATETIME
        ),
        exp.DatetimeSub: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DATETIME
        ),
        exp.DatetimeDiff: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.Extract: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.TimestampAdd: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.TimestampSub: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.TimestampDiff: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.TimeAdd: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TIMESTAMP),
        exp.TimeSub: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TIMESTAMP),
        exp.TimeDiff: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.DateStrToDate: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DATE
        ),
        exp.DateToDateStr: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.DateToDi: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.Day: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TINYINT),
        exp.DiToDate: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.Exp: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Floor: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.Case: lambda self, expr: self._annotate_by_args(expr, "default", "ifs"),
        exp.If: lambda self, expr: self._annotate_by_args(expr, "true", "false"),
        exp.Coalesce: lambda self, expr: self._annotate_by_args(expr, "this", "expressions"),
        exp.IfNull: lambda self, expr: self._annotate_by_args(expr, "this", "expression"),
        exp.ConcatWs: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.GroupConcat: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.ArrayConcat: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.Initcap: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.Length: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.BIGINT),
        exp.Levenshtein: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.Ln: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Log: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Log2: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Log10: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Lower: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.Month: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TINYINT),
        exp.Pow: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Quantile: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.ApproxQuantile: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DOUBLE
        ),
        exp.RegexpLike: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.BOOLEAN
        ),
        exp.Round: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.SafeDivide: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Substring: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.StrPosition: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.StrToDate: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.StrToTime: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.Sqrt: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.Stddev: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.StddevPop: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.StddevSamp: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.TimeToStr: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.TimeToTimeStr: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.TimeStrToDate: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DATE
        ),
        exp.TimeStrToTime: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.Trim: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.TsOrDsToDateStr: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.TsOrDsToDate: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DATE),
        exp.TsOrDiToDi: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.INT),
        exp.UnixToStr: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.UnixToTime: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.TIMESTAMP
        ),
        exp.UnixToTimeStr: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.VARCHAR
        ),
        exp.Upper: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARCHAR),
        exp.Variance: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.DOUBLE),
        exp.VariancePop: lambda self, expr: self._annotate_with_type(
            expr, exp.DataType.Type.DOUBLE
        ),
        exp.Week: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TINYINT),
        exp.Year: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.TINYINT),
    }

    # Reference: https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
    COERCES_TO = {
        # CHAR < NCHAR < VARCHAR < NVARCHAR < TEXT
        exp.DataType.Type.TEXT: set(),
        exp.DataType.Type.NVARCHAR: {exp.DataType.Type.TEXT},
        exp.DataType.Type.VARCHAR: {exp.DataType.Type.NVARCHAR, exp.DataType.Type.TEXT},
        exp.DataType.Type.NCHAR: {
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.NVARCHAR,
            exp.DataType.Type.TEXT,
        },
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
        exp.DataType.Type.BIGINT: {
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DOUBLE,
        },
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
        exp.DataType.Type.TIMESTAMP: {
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMPLTZ,
        },
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

    TRAVERSABLES = (exp.Select, exp.Union, exp.UDTF, exp.Subquery)

    def __init__(self, schema=None, annotators=None, coerces_to=None):
        self.schema = schema
        self.annotators = annotators or self.ANNOTATORS
        self.coerces_to = coerces_to or self.COERCES_TO

    def annotate(self, expression):
        if isinstance(expression, self.TRAVERSABLES):
            for scope in traverse_scope(expression):
                selects = {}
                for name, source in scope.sources.items():
                    if not isinstance(source, Scope):
                        continue
                    if isinstance(source.expression, exp.Values):
                        selects[name] = {
                            alias: column
                            for alias, column in zip(
                                source.expression.alias_column_names,
                                source.expression.expressions[0].expressions,
                            )
                        }
                    else:
                        selects[name] = {
                            select.alias_or_name: select for select in source.expression.selects
                        }
                # First annotate the current scope's column references
                for col in scope.columns:
                    source = scope.sources.get(col.table)
                    if isinstance(source, exp.Table):
                        col.type = self.schema.get_column_type(source, col)
                    elif source:
                        col.type = selects[col.table][col.name].type
                # Then (possibly) annotate the remaining expressions in the scope
                self._maybe_annotate(scope.expression)
        return self._maybe_annotate(expression)  # This takes care of non-traversable expressions

    def _maybe_annotate(self, expression):
        if not isinstance(expression, exp.Expression):
            return None

        if expression.type:
            return expression  # We've already inferred the expression's type

        annotator = self.annotators.get(expression.__class__)

        return (
            annotator(self, expression)
            if annotator
            else self._annotate_with_type(expression, exp.DataType.Type.UNKNOWN)
        )

    def _annotate_args(self, expression):
        for value in expression.args.values():
            for v in ensure_collection(value):
                self._maybe_annotate(v)

        return expression

    def _maybe_coerce(self, type1, type2):
        # We propagate the NULL / UNKNOWN types upwards if found
        if isinstance(type1, exp.DataType):
            type1 = type1.this
        if isinstance(type2, exp.DataType):
            type2 = type2.this

        if exp.DataType.Type.NULL in (type1, type2):
            return exp.DataType.Type.NULL
        if exp.DataType.Type.UNKNOWN in (type1, type2):
            return exp.DataType.Type.UNKNOWN

        return type2 if type2 in self.coerces_to.get(type1, {}) else type1

    def _annotate_binary(self, expression):
        self._annotate_args(expression)

        left_type = expression.left.type.this
        right_type = expression.right.type.this

        if isinstance(expression, (exp.And, exp.Or)):
            if left_type == exp.DataType.Type.NULL and right_type == exp.DataType.Type.NULL:
                expression.type = exp.DataType.Type.NULL
            elif exp.DataType.Type.NULL in (left_type, right_type):
                expression.type = exp.DataType.build(
                    "NULLABLE", expressions=exp.DataType.build("BOOLEAN")
                )
            else:
                expression.type = exp.DataType.Type.BOOLEAN
        elif isinstance(expression, (exp.Condition, exp.Predicate)):
            expression.type = exp.DataType.Type.BOOLEAN
        else:
            expression.type = self._maybe_coerce(left_type, right_type)

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

    def _annotate_with_type(self, expression, target_type):
        expression.type = target_type
        return self._annotate_args(expression)

    def _annotate_by_args(self, expression, *args, promote=False):
        self._annotate_args(expression)
        expressions = []
        for arg in args:
            arg_expr = expression.args.get(arg)
            expressions.extend(expr for expr in ensure_list(arg_expr) if expr)

        last_datatype = None
        for expr in expressions:
            last_datatype = self._maybe_coerce(last_datatype or expr.type, expr.type)

        expression.type = last_datatype or exp.DataType.Type.UNKNOWN

        if promote:
            if expression.type.this in exp.DataType.INTEGER_TYPES:
                expression.type = exp.DataType.Type.BIGINT
            elif expression.type.this in exp.DataType.FLOAT_TYPES:
                expression.type = exp.DataType.Type.DOUBLE

        return expression
