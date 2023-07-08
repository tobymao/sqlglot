from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot._typing import E
from sqlglot.helper import ensure_list, subclasses
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import Schema, ensure_schema

if t.TYPE_CHECKING:
    B = t.TypeVar("B", bound=exp.Binary)


def annotate_types(
    expression: E,
    schema: t.Optional[t.Dict | Schema] = None,
    annotators: t.Optional[t.Dict[t.Type[E], t.Callable[[TypeAnnotator, E], E]]] = None,
    coerces_to: t.Optional[t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]]] = None,
) -> E:
    """
    Infers the types of an expression, annotating its AST accordingly.

    Example:
        >>> import sqlglot
        >>> schema = {"y": {"cola": "SMALLINT"}}
        >>> sql = "SELECT x.cola + 2.5 AS cola FROM (SELECT y.cola AS cola FROM y AS y) AS x"
        >>> annotated_expr = annotate_types(sqlglot.parse_one(sql), schema=schema)
        >>> annotated_expr.expressions[0].type.this  # Get the type of "x.cola + 2.5 AS cola"
        <Type.DOUBLE: 'DOUBLE'>

    Args:
        expression: Expression to annotate.
        schema: Database schema.
        annotators: Maps expression type to corresponding annotation function.
        coerces_to: Maps expression type to set of types that it can be coerced into.

    Returns:
        The expression annotated with types.
    """

    schema = ensure_schema(schema)

    return TypeAnnotator(schema, annotators, coerces_to).annotate(expression)


def _annotate_with_type_lambda(data_type: exp.DataType.Type) -> t.Callable[[TypeAnnotator, E], E]:
    return lambda self, e: self._annotate_with_type(e, data_type)


class _TypeAnnotator(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        # Highest-to-lowest type precedence, as specified in Spark's docs (ANSI):
        # https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
        text_precedence = (
            exp.DataType.Type.TEXT,
            exp.DataType.Type.NVARCHAR,
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.NCHAR,
            exp.DataType.Type.CHAR,
        )
        numeric_precedence = (
            exp.DataType.Type.DOUBLE,
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.BIGINT,
            exp.DataType.Type.INT,
            exp.DataType.Type.SMALLINT,
            exp.DataType.Type.TINYINT,
        )
        timelike_precedence = (
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMP,
            exp.DataType.Type.DATETIME,
            exp.DataType.Type.DATE,
        )

        for type_precedence in (text_precedence, numeric_precedence, timelike_precedence):
            coerces_to = set()
            for data_type in type_precedence:
                klass.COERCES_TO[data_type] = coerces_to.copy()
                coerces_to |= {data_type}

        return klass


class TypeAnnotator(metaclass=_TypeAnnotator):
    TYPE_TO_EXPRESSIONS: t.Dict[exp.DataType.Type, t.Set[t.Type[exp.Expression]]] = {
        exp.DataType.Type.BIGINT: {
            exp.ApproxDistinct,
            exp.ArraySize,
            exp.Count,
            exp.Length,
        },
        exp.DataType.Type.BOOLEAN: {
            exp.Between,
            exp.Boolean,
            exp.In,
            exp.RegexpLike,
        },
        exp.DataType.Type.DATE: {
            exp.CurrentDate,
            exp.Date,
            exp.DateAdd,
            exp.DateFromParts,
            exp.DateStrToDate,
            exp.DateSub,
            exp.DateTrunc,
            exp.DiToDate,
            exp.StrToDate,
            exp.TimeStrToDate,
            exp.TsOrDsToDate,
        },
        exp.DataType.Type.DATETIME: {
            exp.CurrentDatetime,
            exp.DatetimeAdd,
            exp.DatetimeSub,
        },
        exp.DataType.Type.DOUBLE: {
            exp.ApproxQuantile,
            exp.Avg,
            exp.Exp,
            exp.Ln,
            exp.Log,
            exp.Log2,
            exp.Log10,
            exp.Pow,
            exp.Quantile,
            exp.Round,
            exp.SafeDivide,
            exp.Sqrt,
            exp.Stddev,
            exp.StddevPop,
            exp.StddevSamp,
            exp.Variance,
            exp.VariancePop,
        },
        exp.DataType.Type.INT: {
            exp.Ceil,
            exp.DateDiff,
            exp.DatetimeDiff,
            exp.Extract,
            exp.TimestampDiff,
            exp.TimeDiff,
            exp.DateToDi,
            exp.Floor,
            exp.Levenshtein,
            exp.StrPosition,
            exp.TsOrDiToDi,
        },
        exp.DataType.Type.TIMESTAMP: {
            exp.CurrentTime,
            exp.CurrentTimestamp,
            exp.StrToTime,
            exp.TimeAdd,
            exp.TimeStrToTime,
            exp.TimeSub,
            exp.TimestampAdd,
            exp.TimestampSub,
            exp.UnixToTime,
        },
        exp.DataType.Type.TINYINT: {
            exp.Day,
            exp.Month,
            exp.Week,
            exp.Year,
        },
        exp.DataType.Type.VARCHAR: {
            exp.ArrayConcat,
            exp.Concat,
            exp.ConcatWs,
            exp.DateToDateStr,
            exp.GroupConcat,
            exp.Initcap,
            exp.Lower,
            exp.SafeConcat,
            exp.Substring,
            exp.TimeToStr,
            exp.TimeToTimeStr,
            exp.Trim,
            exp.TsOrDsToDateStr,
            exp.UnixToStr,
            exp.UnixToTimeStr,
            exp.Upper,
        },
    }

    ANNOTATORS: t.Dict = {
        **{
            expr_type: lambda self, e: self._annotate_unary(e)
            for expr_type in subclasses(exp.__name__, (exp.Unary, exp.Alias))
        },
        **{
            expr_type: lambda self, e: self._annotate_binary(e)
            for expr_type in subclasses(exp.__name__, exp.Binary)
        },
        **{
            expr_type: _annotate_with_type_lambda(data_type)
            for data_type, expressions in TYPE_TO_EXPRESSIONS.items()
            for expr_type in expressions
        },
        exp.Anonymous: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.UNKNOWN),
        exp.Cast: lambda self, e: self._annotate_with_type(e, e.args["to"]),
        exp.Case: lambda self, e: self._annotate_by_args(e, "default", "ifs"),
        exp.Coalesce: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.DataType: lambda self, e: self._annotate_with_type(e, e.copy()),
        exp.If: lambda self, e: self._annotate_by_args(e, "true", "false"),
        exp.Interval: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.INTERVAL),
        exp.Least: lambda self, e: self._annotate_by_args(e, "expressions"),
        exp.Literal: lambda self, e: self._annotate_literal(e),
        exp.Map: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.MAP),
        exp.Max: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.Min: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.Null: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.NULL),
        exp.Sum: lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True),
        exp.TryCast: lambda self, e: self._annotate_with_type(e, e.args["to"]),
        exp.VarMap: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.MAP),
    }

    # Specifies what types a given type can be coerced into (autofilled)
    COERCES_TO: t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]] = {}

    def __init__(
        self,
        schema: Schema,
        annotators: t.Optional[t.Dict[t.Type[E], t.Callable[[TypeAnnotator, E], E]]] = None,
        coerces_to: t.Optional[t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]]] = None,
    ) -> None:
        self.schema = schema
        self.annotators = annotators or self.ANNOTATORS
        self.coerces_to = coerces_to or self.COERCES_TO

    def annotate(self, expression: E) -> E:
        for scope in traverse_scope(expression):
            selects = {}
            for name, source in scope.sources.items():
                if not isinstance(source, Scope):
                    continue
                if isinstance(source.expression, exp.UDTF):
                    values = []

                    if isinstance(source.expression, exp.Lateral):
                        if isinstance(source.expression.this, exp.Explode):
                            values = [source.expression.this.this]
                    else:
                        values = source.expression.expressions[0].expressions

                    if not values:
                        continue

                    selects[name] = {
                        alias: column
                        for alias, column in zip(
                            source.expression.alias_column_names,
                            values,
                        )
                    }
                else:
                    selects[name] = {
                        select.alias_or_name: select for select in source.expression.selects
                    }

            # First annotate the current scope's column references
            for col in scope.columns:
                if not col.table:
                    continue

                source = scope.sources.get(col.table)
                if isinstance(source, exp.Table):
                    col.type = self.schema.get_column_type(source, col)
                elif source and col.table in selects and col.name in selects[col.table]:
                    col.type = selects[col.table][col.name].type

            # Then (possibly) annotate the remaining expressions in the scope
            self._maybe_annotate(scope.expression)

        return self._maybe_annotate(expression)  # This takes care of non-traversable expressions

    def _maybe_annotate(self, expression: E) -> E:
        if expression.type:
            return expression  # We've already inferred the expression's type

        annotator = self.annotators.get(expression.__class__)

        return (
            annotator(self, expression)
            if annotator
            else self._annotate_with_type(expression, exp.DataType.Type.UNKNOWN)
        )

    def _annotate_args(self, expression: E) -> E:
        for _, value in expression.iter_expressions():
            self._maybe_annotate(value)

        return expression

    def _maybe_coerce(
        self, type1: exp.DataType | exp.DataType.Type, type2: exp.DataType | exp.DataType.Type
    ) -> exp.DataType.Type:
        # We propagate the NULL / UNKNOWN types upwards if found
        if isinstance(type1, exp.DataType):
            type1 = type1.this
        if isinstance(type2, exp.DataType):
            type2 = type2.this

        if exp.DataType.Type.NULL in (type1, type2):
            return exp.DataType.Type.NULL
        if exp.DataType.Type.UNKNOWN in (type1, type2):
            return exp.DataType.Type.UNKNOWN

        return type2 if type2 in self.coerces_to.get(type1, {}) else type1  # type: ignore

    # Note: the following "no_type_check" decorators were added because mypy was yelling due
    # to assigning Type values to expression.type (since its getter returns Optional[DataType]).
    # This is a known mypy issue: https://github.com/python/mypy/issues/3004

    @t.no_type_check
    def _annotate_binary(self, expression: B) -> B:
        self._annotate_args(expression)

        left_type = expression.left.type.this
        right_type = expression.right.type.this

        if isinstance(expression, exp.Connector):
            if left_type == exp.DataType.Type.NULL and right_type == exp.DataType.Type.NULL:
                expression.type = exp.DataType.Type.NULL
            elif exp.DataType.Type.NULL in (left_type, right_type):
                expression.type = exp.DataType.build(
                    "NULLABLE", expressions=exp.DataType.build("BOOLEAN")
                )
            else:
                expression.type = exp.DataType.Type.BOOLEAN
        elif isinstance(expression, exp.Predicate):
            expression.type = exp.DataType.Type.BOOLEAN
        else:
            expression.type = self._maybe_coerce(left_type, right_type)

        return expression

    @t.no_type_check
    def _annotate_unary(self, expression: E) -> E:
        self._annotate_args(expression)

        if isinstance(expression, exp.Condition) and not isinstance(expression, exp.Paren):
            expression.type = exp.DataType.Type.BOOLEAN
        else:
            expression.type = expression.this.type

        return expression

    @t.no_type_check
    def _annotate_literal(self, expression: exp.Literal) -> exp.Literal:
        if expression.is_string:
            expression.type = exp.DataType.Type.VARCHAR
        elif expression.is_int:
            expression.type = exp.DataType.Type.INT
        else:
            expression.type = exp.DataType.Type.DOUBLE

        return expression

    @t.no_type_check
    def _annotate_with_type(self, expression: E, target_type: exp.DataType.Type) -> E:
        expression.type = target_type
        return self._annotate_args(expression)

    @t.no_type_check
    def _annotate_by_args(self, expression: E, *args: str, promote: bool = False) -> E:
        self._annotate_args(expression)

        expressions: t.List[exp.Expression] = []
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
