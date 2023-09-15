from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    binary_from_function,
    create_with_partitions_sql,
    format_time_lambda,
    is_parse_json,
    pivot_column_names,
    rename_func,
    trim_sql,
)
from sqlglot.dialects.hive import Hive
from sqlglot.helper import seq_get


def _create_sql(self: Spark2.Generator, e: exp.Create) -> str:
    kind = e.args["kind"]
    properties = e.args.get("properties")

    if (
        kind.upper() == "TABLE"
        and e.expression
        and any(
            isinstance(prop, exp.TemporaryProperty)
            for prop in (properties.expressions if properties else [])
        )
    ):
        return f"CREATE TEMPORARY VIEW {self.sql(e, 'this')} AS {self.sql(e, 'expression')}"
    return create_with_partitions_sql(self, e)


def _map_sql(self: Spark2.Generator, expression: exp.Map) -> str:
    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not keys or not values:
        return "MAP()"

    return f"MAP_FROM_ARRAYS({self.sql(keys)}, {self.sql(values)})"


def _parse_as_cast(to_type: str) -> t.Callable[[t.List], exp.Expression]:
    return lambda args: exp.Cast(this=seq_get(args, 0), to=exp.DataType.build(to_type))


def _str_to_date(self: Spark2.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Hive.DATE_FORMAT:
        return f"TO_DATE({this})"
    return f"TO_DATE({this}, {time_format})"


def _unix_to_time_sql(self: Spark2.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale is None:
        return f"CAST(FROM_UNIXTIME({timestamp}) AS TIMESTAMP)"
    if scale == exp.UnixToTime.SECONDS:
        return f"TIMESTAMP_SECONDS({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TIMESTAMP_MILLIS({timestamp})"
    if scale == exp.UnixToTime.MICROS:
        return f"TIMESTAMP_MICROS({timestamp})"

    raise ValueError("Improper scale for timestamp")


def _unalias_pivot(expression: exp.Expression) -> exp.Expression:
    """
    Spark doesn't allow PIVOT aliases, so we need to remove them and possibly wrap a
    pivoted source in a subquery with the same alias to preserve the query's semantics.

    Example:
        >>> from sqlglot import parse_one
        >>> expr = parse_one("SELECT piv.x FROM tbl PIVOT (SUM(a) FOR b IN ('x')) piv")
        >>> print(_unalias_pivot(expr).sql(dialect="spark"))
        SELECT piv.x FROM (SELECT * FROM tbl PIVOT(SUM(a) FOR b IN ('x'))) AS piv
    """
    if isinstance(expression, exp.From) and expression.this.args.get("pivots"):
        pivot = expression.this.args["pivots"][0]
        if pivot.alias:
            alias = pivot.args["alias"].pop()
            return exp.From(
                this=expression.this.replace(
                    exp.select("*").from_(expression.this.copy()).subquery(alias=alias)
                )
            )

    return expression


def _unqualify_pivot_columns(expression: exp.Expression) -> exp.Expression:
    """
    Spark doesn't allow the column referenced in the PIVOT's field to be qualified,
    so we need to unqualify it.

    Example:
        >>> from sqlglot import parse_one
        >>> expr = parse_one("SELECT * FROM tbl PIVOT (SUM(tbl.sales) FOR tbl.quarter IN ('Q1', 'Q2'))")
        >>> print(_unqualify_pivot_columns(expr).sql(dialect="spark"))
        SELECT * FROM tbl PIVOT(SUM(tbl.sales) FOR quarter IN ('Q1', 'Q1'))
    """
    if isinstance(expression, exp.Pivot):
        expression.args["field"].transform(
            lambda node: exp.column(node.output_name, quoted=node.this.quoted)
            if isinstance(node, exp.Column)
            else node,
            copy=False,
        )

    return expression


def _insert_sql(self: Spark2.Generator, expression: exp.Insert) -> str:
    if expression.expression.args.get("with"):
        expression = expression.copy()
        expression.set("with", expression.expression.args.pop("with"))
    return self.insert_sql(expression)


class Spark2(Hive):
    class Parser(Hive.Parser):
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            "AGGREGATE": exp.Reduce.from_arg_list,
            "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
            "BOOLEAN": _parse_as_cast("boolean"),
            "DATE": _parse_as_cast("date"),
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=exp.var(seq_get(args, 0))
            ),
            "DAYOFMONTH": lambda args: exp.DayOfMonth(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DAYOFWEEK": lambda args: exp.DayOfWeek(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DAYOFYEAR": lambda args: exp.DayOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DOUBLE": _parse_as_cast("double"),
            "FLOAT": _parse_as_cast("float"),
            "IIF": exp.If.from_arg_list,
            "INT": _parse_as_cast("int"),
            "MAP_FROM_ARRAYS": exp.Map.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),
            "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
            "STRING": _parse_as_cast("string"),
            "TIMESTAMP": _parse_as_cast("timestamp"),
            "TO_TIMESTAMP": lambda args: _parse_as_cast("timestamp")(args)
            if len(args) == 1
            else format_time_lambda(exp.StrToTime, "spark")(args),
            "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
            "TRUNC": lambda args: exp.DateTrunc(unit=seq_get(args, 1), this=seq_get(args, 0)),
            "WEEKOFYEAR": lambda args: exp.WeekOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        }

        FUNCTION_PARSERS = {
            **Hive.Parser.FUNCTION_PARSERS,
            "BROADCAST": lambda self: self._parse_join_hint("BROADCAST"),
            "BROADCASTJOIN": lambda self: self._parse_join_hint("BROADCASTJOIN"),
            "MAPJOIN": lambda self: self._parse_join_hint("MAPJOIN"),
            "MERGE": lambda self: self._parse_join_hint("MERGE"),
            "SHUFFLEMERGE": lambda self: self._parse_join_hint("SHUFFLEMERGE"),
            "MERGEJOIN": lambda self: self._parse_join_hint("MERGEJOIN"),
            "SHUFFLE_HASH": lambda self: self._parse_join_hint("SHUFFLE_HASH"),
            "SHUFFLE_REPLICATE_NL": lambda self: self._parse_join_hint("SHUFFLE_REPLICATE_NL"),
        }

        def _parse_add_column(self) -> t.Optional[exp.Expression]:
            return self._match_text_seq("ADD", "COLUMNS") and self._parse_schema()

        def _parse_drop_column(self) -> t.Optional[exp.Drop | exp.Command]:
            return self._match_text_seq("DROP", "COLUMNS") and self.expression(
                exp.Drop, this=self._parse_schema(), kind="COLUMNS"
            )

        def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
            if len(aggregations) == 1:
                return [""]
            return pivot_column_names(aggregations, dialect="spark")

    class Generator(Hive.Generator):
        QUERY_HINTS = True
        NVL2_SUPPORTED = True

        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,
            exp.EngineProperty: exp.Properties.Location.UNSUPPORTED,
            exp.AutoIncrementProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CharacterSetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CollateProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TRANSFORMS = {
            **Hive.Generator.TRANSFORMS,
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArraySum: lambda self, e: f"AGGREGATE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.AtTimeZone: lambda self, e: f"FROM_UTC_TIMESTAMP({self.sql(e, 'this')}, {self.sql(e, 'zone')})",
            exp.BitwiseLeftShift: rename_func("SHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("SHIFTRIGHT"),
            exp.Create: _create_sql,
            exp.DateFromParts: rename_func("MAKE_DATE"),
            exp.DateTrunc: lambda self, e: self.func("TRUNC", e.this, e.args.get("unit")),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.FileFormatProperty: lambda self, e: f"USING {e.name.upper()}",
            exp.From: transforms.preprocess([_unalias_pivot]),
            exp.Insert: _insert_sql,
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Map: _map_sql,
            exp.Pivot: transforms.preprocess([_unqualify_pivot_columns]),
            exp.Reduce: rename_func("AGGREGATE"),
            exp.RegexpReplace: lambda self, e: self.func(
                "REGEXP_REPLACE",
                e.this,
                e.expression,
                e.args["replacement"],
                e.args.get("position"),
            ),
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", exp.Literal.string(e.text("unit")), e.this
            ),
            exp.Trim: trim_sql,
            exp.UnixToTime: _unix_to_time_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.WithinGroup: transforms.preprocess(
                [transforms.remove_within_group_for_percentiles]
            ),
        }
        TRANSFORMS.pop(exp.ArrayJoin)
        TRANSFORMS.pop(exp.ArraySort)
        TRANSFORMS.pop(exp.ILike)
        TRANSFORMS.pop(exp.Left)
        TRANSFORMS.pop(exp.MonthsBetween)
        TRANSFORMS.pop(exp.Right)

        WRAP_DERIVED_VALUES = False
        CREATE_FUNCTION_RETURN_AS = False

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            if is_parse_json(expression.this):
                schema = f"'{self.sql(expression, 'to')}'"
                return self.func("FROM_JSON", expression.this.this, schema)

            if is_parse_json(expression):
                return self.func("TO_JSON", expression.this)

            return super(Hive.Generator, self).cast_sql(expression, safe_prefix=safe_prefix)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            return super().columndef_sql(
                expression,
                sep=": "
                if isinstance(expression.parent, exp.DataType)
                and expression.parent.is_type("struct")
                else sep,
            )

    class Tokenizer(Hive.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
