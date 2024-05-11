from __future__ import annotations

import functools
import typing as t

from sqlglot import exp
from sqlglot.helper import (
    ensure_list,
    is_date_unit,
    is_iso_date,
    is_iso_datetime,
    seq_get,
    subclasses,
)
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import Schema, ensure_schema

if t.TYPE_CHECKING:
    from sqlglot._typing import B, E

    BinaryCoercionFunc = t.Callable[[exp.Expression, exp.Expression], exp.DataType.Type]
    BinaryCoercions = t.Dict[
        t.Tuple[exp.DataType.Type, exp.DataType.Type],
        BinaryCoercionFunc,
    ]


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


def _coerce_date_literal(l: exp.Expression, unit: t.Optional[exp.Expression]) -> exp.DataType.Type:
    date_text = l.name
    is_iso_date_ = is_iso_date(date_text)

    if is_iso_date_ and is_date_unit(unit):
        return exp.DataType.Type.DATE

    # An ISO date is also an ISO datetime, but not vice versa
    if is_iso_date_ or is_iso_datetime(date_text):
        return exp.DataType.Type.DATETIME

    return exp.DataType.Type.UNKNOWN


def _coerce_date(l: exp.Expression, unit: t.Optional[exp.Expression]) -> exp.DataType.Type:
    if not is_date_unit(unit):
        return exp.DataType.Type.DATETIME
    return l.type.this if l.type else exp.DataType.Type.UNKNOWN


def swap_args(func: BinaryCoercionFunc) -> BinaryCoercionFunc:
    @functools.wraps(func)
    def _swapped(l: exp.Expression, r: exp.Expression) -> exp.DataType.Type:
        return func(r, l)

    return _swapped


def swap_all(coercions: BinaryCoercions) -> BinaryCoercions:
    return {**coercions, **{(b, a): swap_args(func) for (a, b), func in coercions.items()}}


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
            exp.DateFromParts,
            exp.DateStrToDate,
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
            exp.Div,
            exp.Exp,
            exp.Ln,
            exp.Log,
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
            exp.DatetimeDiff,
            exp.DateDiff,
            exp.Extract,
            exp.TimestampDiff,
            exp.TimeDiff,
            exp.DateToDi,
            exp.Floor,
            exp.Levenshtein,
            exp.Sign,
            exp.StrPosition,
            exp.TsOrDiToDi,
        },
        exp.DataType.Type.JSON: {
            exp.ParseJSON,
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
            exp.Quarter,
        },
        exp.DataType.Type.VARCHAR: {
            exp.ArrayConcat,
            exp.Concat,
            exp.ConcatWs,
            exp.DateToDateStr,
            exp.GroupConcat,
            exp.Initcap,
            exp.Lower,
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
        exp.Abs: lambda self, e: self._annotate_by_args(e, "this"),
        exp.Anonymous: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.UNKNOWN),
        exp.Array: lambda self, e: self._annotate_by_args(e, "expressions", array=True),
        exp.ArrayAgg: lambda self, e: self._annotate_by_args(e, "this", array=True),
        exp.ArrayConcat: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.Bracket: lambda self, e: self._annotate_bracket(e),
        exp.Cast: lambda self, e: self._annotate_with_type(e, e.args["to"]),
        exp.Case: lambda self, e: self._annotate_by_args(e, "default", "ifs"),
        exp.Coalesce: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.DataType: lambda self, e: self._annotate_with_type(e, e.copy()),
        exp.DateAdd: lambda self, e: self._annotate_timeunit(e),
        exp.DateSub: lambda self, e: self._annotate_timeunit(e),
        exp.DateTrunc: lambda self, e: self._annotate_timeunit(e),
        exp.Distinct: lambda self, e: self._annotate_by_args(e, "expressions"),
        exp.Div: lambda self, e: self._annotate_div(e),
        exp.Dot: lambda self, e: self._annotate_dot(e),
        exp.Explode: lambda self, e: self._annotate_explode(e),
        exp.Filter: lambda self, e: self._annotate_by_args(e, "this"),
        exp.GenerateDateArray: lambda self, e: self._annotate_with_type(
            e, exp.DataType.build("ARRAY<DATE>")
        ),
        exp.If: lambda self, e: self._annotate_by_args(e, "true", "false"),
        exp.Interval: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.INTERVAL),
        exp.Least: lambda self, e: self._annotate_by_args(e, "expressions"),
        exp.Literal: lambda self, e: self._annotate_literal(e),
        exp.Map: lambda self, e: self._annotate_map(e),
        exp.Max: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.Min: lambda self, e: self._annotate_by_args(e, "this", "expressions"),
        exp.Null: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.NULL),
        exp.Nullif: lambda self, e: self._annotate_by_args(e, "this", "expression"),
        exp.PropertyEQ: lambda self, e: self._annotate_by_args(e, "expression"),
        exp.Slice: lambda self, e: self._annotate_with_type(e, exp.DataType.Type.UNKNOWN),
        exp.Struct: lambda self, e: self._annotate_struct(e),
        exp.Sum: lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True),
        exp.Timestamp: lambda self, e: self._annotate_with_type(
            e,
            exp.DataType.Type.TIMESTAMPTZ if e.args.get("with_tz") else exp.DataType.Type.TIMESTAMP,
        ),
        exp.ToMap: lambda self, e: self._annotate_to_map(e),
        exp.TryCast: lambda self, e: self._annotate_with_type(e, e.args["to"]),
        exp.Unnest: lambda self, e: self._annotate_unnest(e),
        exp.VarMap: lambda self, e: self._annotate_map(e),
    }

    NESTED_TYPES = {
        exp.DataType.Type.ARRAY,
    }

    # Specifies what types a given type can be coerced into (autofilled)
    COERCES_TO: t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]] = {}

    # Coercion functions for binary operations.
    # Map of type pairs to a callable that takes both sides of the binary operation and returns the resulting type.
    BINARY_COERCIONS: BinaryCoercions = {
        **swap_all(
            {
                (t, exp.DataType.Type.INTERVAL): lambda l, r: _coerce_date_literal(
                    l, r.args.get("unit")
                )
                for t in exp.DataType.TEXT_TYPES
            }
        ),
        **swap_all(
            {
                # text + numeric will yield the numeric type to match most dialects' semantics
                (text, numeric): lambda l, r: t.cast(
                    exp.DataType.Type, l.type if l.type in exp.DataType.NUMERIC_TYPES else r.type
                )
                for text in exp.DataType.TEXT_TYPES
                for numeric in exp.DataType.NUMERIC_TYPES
            }
        ),
        **swap_all(
            {
                (exp.DataType.Type.DATE, exp.DataType.Type.INTERVAL): lambda l, r: _coerce_date(
                    l, r.args.get("unit")
                ),
            }
        ),
    }

    def __init__(
        self,
        schema: Schema,
        annotators: t.Optional[t.Dict[t.Type[E], t.Callable[[TypeAnnotator, E], E]]] = None,
        coerces_to: t.Optional[t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]]] = None,
        binary_coercions: t.Optional[BinaryCoercions] = None,
    ) -> None:
        self.schema = schema
        self.annotators = annotators or self.ANNOTATORS
        self.coerces_to = coerces_to or self.COERCES_TO
        self.binary_coercions = binary_coercions or self.BINARY_COERCIONS

        # Caches the ids of annotated sub-Expressions, to ensure we only visit them once
        self._visited: t.Set[int] = set()

    def _set_type(
        self, expression: exp.Expression, target_type: t.Optional[exp.DataType | exp.DataType.Type]
    ) -> None:
        expression.type = target_type or exp.DataType.Type.UNKNOWN  # type: ignore
        self._visited.add(id(expression))

    def annotate(self, expression: E) -> E:
        for scope in traverse_scope(expression):
            self.annotate_scope(scope)
        return self._maybe_annotate(expression)  # This takes care of non-traversable expressions

    def annotate_scope(self, scope: Scope) -> None:
        selects = {}
        for name, source in scope.sources.items():
            if not isinstance(source, Scope):
                continue
            if isinstance(source.expression, exp.UDTF):
                values = []

                if isinstance(source.expression, exp.Lateral):
                    if isinstance(source.expression.this, exp.Explode):
                        values = [source.expression.this.this]
                elif isinstance(source.expression, exp.Unnest):
                    values = [source.expression]
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
                self._set_type(col, self.schema.get_column_type(source, col))
            elif source:
                if col.table in selects and col.name in selects[col.table]:
                    self._set_type(col, selects[col.table][col.name].type)
                elif isinstance(source.expression, exp.Unnest):
                    self._set_type(col, source.expression.type)

        # Then (possibly) annotate the remaining expressions in the scope
        self._maybe_annotate(scope.expression)

    def _maybe_annotate(self, expression: E) -> E:
        if id(expression) in self._visited:
            return expression  # We've already inferred the expression's type

        annotator = self.annotators.get(expression.__class__)

        return (
            annotator(self, expression)
            if annotator
            else self._annotate_with_type(expression, exp.DataType.Type.UNKNOWN)
        )

    def _annotate_args(self, expression: E) -> E:
        for value in expression.iter_expressions():
            self._maybe_annotate(value)

        return expression

    def _maybe_coerce(
        self, type1: exp.DataType | exp.DataType.Type, type2: exp.DataType | exp.DataType.Type
    ) -> exp.DataType | exp.DataType.Type:
        type1_value = type1.this if isinstance(type1, exp.DataType) else type1
        type2_value = type2.this if isinstance(type2, exp.DataType) else type2

        # We propagate the NULL / UNKNOWN types upwards if found
        if exp.DataType.Type.NULL in (type1_value, type2_value):
            return exp.DataType.Type.NULL
        if exp.DataType.Type.UNKNOWN in (type1_value, type2_value):
            return exp.DataType.Type.UNKNOWN

        return type2_value if type2_value in self.coerces_to.get(type1_value, {}) else type1_value

    def _annotate_binary(self, expression: B) -> B:
        self._annotate_args(expression)

        left, right = expression.left, expression.right
        left_type, right_type = left.type.this, right.type.this  # type: ignore

        if isinstance(expression, exp.Connector):
            if left_type == exp.DataType.Type.NULL and right_type == exp.DataType.Type.NULL:
                self._set_type(expression, exp.DataType.Type.NULL)
            elif exp.DataType.Type.NULL in (left_type, right_type):
                self._set_type(
                    expression,
                    exp.DataType.build("NULLABLE", expressions=exp.DataType.build("BOOLEAN")),
                )
            else:
                self._set_type(expression, exp.DataType.Type.BOOLEAN)
        elif isinstance(expression, exp.Predicate):
            self._set_type(expression, exp.DataType.Type.BOOLEAN)
        elif (left_type, right_type) in self.binary_coercions:
            self._set_type(expression, self.binary_coercions[(left_type, right_type)](left, right))
        else:
            self._set_type(expression, self._maybe_coerce(left_type, right_type))

        return expression

    def _annotate_unary(self, expression: E) -> E:
        self._annotate_args(expression)

        if isinstance(expression, exp.Condition) and not isinstance(expression, exp.Paren):
            self._set_type(expression, exp.DataType.Type.BOOLEAN)
        else:
            self._set_type(expression, expression.this.type)

        return expression

    def _annotate_literal(self, expression: exp.Literal) -> exp.Literal:
        if expression.is_string:
            self._set_type(expression, exp.DataType.Type.VARCHAR)
        elif expression.is_int:
            self._set_type(expression, exp.DataType.Type.INT)
        else:
            self._set_type(expression, exp.DataType.Type.DOUBLE)

        return expression

    def _annotate_with_type(self, expression: E, target_type: exp.DataType.Type) -> E:
        self._set_type(expression, target_type)
        return self._annotate_args(expression)

    @t.no_type_check
    def _annotate_by_args(
        self,
        expression: E,
        *args: str,
        promote: bool = False,
        array: bool = False,
    ) -> E:
        self._annotate_args(expression)

        expressions: t.List[exp.Expression] = []
        for arg in args:
            arg_expr = expression.args.get(arg)
            expressions.extend(expr for expr in ensure_list(arg_expr) if expr)

        last_datatype = None
        for expr in expressions:
            expr_type = expr.type

            # Stop at the first nested data type found - we don't want to _maybe_coerce nested types
            if expr_type.args.get("nested"):
                last_datatype = expr_type
                break

            if not expr_type.is_type(exp.DataType.Type.NULL, exp.DataType.Type.UNKNOWN):
                last_datatype = self._maybe_coerce(last_datatype or expr_type, expr_type)

        self._set_type(expression, last_datatype or exp.DataType.Type.UNKNOWN)

        if promote:
            if expression.type.this in exp.DataType.INTEGER_TYPES:
                self._set_type(expression, exp.DataType.Type.BIGINT)
            elif expression.type.this in exp.DataType.FLOAT_TYPES:
                self._set_type(expression, exp.DataType.Type.DOUBLE)

        if array:
            self._set_type(
                expression,
                exp.DataType(
                    this=exp.DataType.Type.ARRAY, expressions=[expression.type], nested=True
                ),
            )

        return expression

    def _annotate_timeunit(
        self, expression: exp.TimeUnit | exp.DateTrunc
    ) -> exp.TimeUnit | exp.DateTrunc:
        self._annotate_args(expression)

        if expression.this.type.this in exp.DataType.TEXT_TYPES:
            datatype = _coerce_date_literal(expression.this, expression.unit)
        elif expression.this.type.this in exp.DataType.TEMPORAL_TYPES:
            datatype = _coerce_date(expression.this, expression.unit)
        else:
            datatype = exp.DataType.Type.UNKNOWN

        self._set_type(expression, datatype)
        return expression

    def _annotate_bracket(self, expression: exp.Bracket) -> exp.Bracket:
        self._annotate_args(expression)

        bracket_arg = expression.expressions[0]
        this = expression.this

        if isinstance(bracket_arg, exp.Slice):
            self._set_type(expression, this.type)
        elif this.type.is_type(exp.DataType.Type.ARRAY):
            self._set_type(expression, seq_get(this.type.expressions, 0))
        elif isinstance(this, (exp.Map, exp.VarMap)) and bracket_arg in this.keys:
            index = this.keys.index(bracket_arg)
            value = seq_get(this.values, index)
            self._set_type(expression, value.type if value else None)
        else:
            self._set_type(expression, exp.DataType.Type.UNKNOWN)

        return expression

    def _annotate_div(self, expression: exp.Div) -> exp.Div:
        self._annotate_args(expression)

        left_type, right_type = expression.left.type.this, expression.right.type.this  # type: ignore

        if (
            expression.args.get("typed")
            and left_type in exp.DataType.INTEGER_TYPES
            and right_type in exp.DataType.INTEGER_TYPES
        ):
            self._set_type(expression, exp.DataType.Type.BIGINT)
        else:
            self._set_type(expression, self._maybe_coerce(left_type, right_type))
            if expression.type and expression.type.this not in exp.DataType.REAL_TYPES:
                self._set_type(
                    expression, self._maybe_coerce(expression.type, exp.DataType.Type.DOUBLE)
                )

        return expression

    def _annotate_dot(self, expression: exp.Dot) -> exp.Dot:
        self._annotate_args(expression)
        self._set_type(expression, None)
        this_type = expression.this.type

        if this_type and this_type.is_type(exp.DataType.Type.STRUCT):
            for e in this_type.expressions:
                if e.name == expression.expression.name:
                    self._set_type(expression, e.kind)
                    break

        return expression

    def _annotate_explode(self, expression: exp.Explode) -> exp.Explode:
        self._annotate_args(expression)
        self._set_type(expression, seq_get(expression.this.type.expressions, 0))
        return expression

    def _annotate_unnest(self, expression: exp.Unnest) -> exp.Unnest:
        self._annotate_args(expression)
        child = seq_get(expression.expressions, 0)

        if child and child.is_type(exp.DataType.Type.ARRAY):
            expr_type = seq_get(child.type.expressions, 0)
        else:
            expr_type = None

        self._set_type(expression, expr_type)
        return expression

    def _annotate_struct_value(
        self, expression: exp.Expression
    ) -> t.Optional[exp.DataType] | exp.ColumnDef:
        alias = expression.args.get("alias")
        if alias:
            return exp.ColumnDef(this=alias.copy(), kind=expression.type)

        # Case: key = value or key := value
        if expression.expression:
            return exp.ColumnDef(this=expression.this.copy(), kind=expression.expression.type)

        return expression.type

    def _annotate_struct(self, expression: exp.Struct) -> exp.Struct:
        self._annotate_args(expression)
        self._set_type(
            expression,
            exp.DataType(
                this=exp.DataType.Type.STRUCT,
                expressions=[self._annotate_struct_value(expr) for expr in expression.expressions],
                nested=True,
            ),
        )
        return expression

    @t.overload
    def _annotate_map(self, expression: exp.Map) -> exp.Map: ...

    @t.overload
    def _annotate_map(self, expression: exp.VarMap) -> exp.VarMap: ...

    def _annotate_map(self, expression):
        self._annotate_args(expression)

        keys = expression.args.get("keys")
        values = expression.args.get("values")

        map_type = exp.DataType(this=exp.DataType.Type.MAP)
        if isinstance(keys, exp.Array) and isinstance(values, exp.Array):
            key_type = seq_get(keys.type.expressions, 0) or exp.DataType.Type.UNKNOWN
            value_type = seq_get(values.type.expressions, 0) or exp.DataType.Type.UNKNOWN

            if key_type != exp.DataType.Type.UNKNOWN and value_type != exp.DataType.Type.UNKNOWN:
                map_type.set("expressions", [key_type, value_type])
                map_type.set("nested", True)

        self._set_type(expression, map_type)
        return expression

    def _annotate_to_map(self, expression: exp.ToMap) -> exp.ToMap:
        self._annotate_args(expression)

        map_type = exp.DataType(this=exp.DataType.Type.MAP)
        arg = expression.this
        if arg.is_type(exp.DataType.Type.STRUCT):
            for coldef in arg.type.expressions:
                kind = coldef.kind
                if kind != exp.DataType.Type.UNKNOWN:
                    map_type.set("expressions", [exp.DataType.build("varchar"), kind])
                    map_type.set("nested", True)
                    break

        self._set_type(expression, map_type)
        return expression
