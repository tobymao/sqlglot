from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    binary_from_function,
    build_formatted_time,
    is_parse_json,
    pivot_column_names,
    rename_func,
    unit_to_str,
)
from sqlglot.dialects.hive import Hive
from sqlglot.helper import seq_get
from sqlglot.parser import build_trim
from sqlglot.tokens import TokenType
from sqlglot.transforms import (
    preprocess,
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    move_schema_columns_to_partitioned_by,
)
from sqlglot.typing.spark2 import EXPRESSION_METADATA


def _map_sql(self: Spark2.Generator, expression: exp.Map) -> str:
    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not keys or not values:
        return self.func("MAP")

    return self.func("MAP_FROM_ARRAYS", keys, values)


def _build_as_cast(to_type: str) -> t.Callable[[t.List], exp.Expression]:
    return lambda args: exp.Cast(this=seq_get(args, 0), to=exp.DataType.build(to_type))


def _str_to_date(self: Spark2.Generator, expression: exp.StrToDate) -> str:
    time_format = self.format_time(expression)
    if time_format == Hive.DATE_FORMAT:
        return self.func("TO_DATE", expression.this)
    return self.func("TO_DATE", expression.this, time_format)


def _unix_to_time_sql(self: Spark2.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale is None:
        return self.sql(exp.cast(exp.func("from_unixtime", timestamp), exp.DataType.Type.TIMESTAMP))
    if scale == exp.UnixToTime.SECONDS:
        return self.func("TIMESTAMP_SECONDS", timestamp)
    if scale == exp.UnixToTime.MILLIS:
        return self.func("TIMESTAMP_MILLIS", timestamp)
    if scale == exp.UnixToTime.MICROS:
        return self.func("TIMESTAMP_MICROS", timestamp)

    unix_seconds = exp.Div(this=timestamp, expression=exp.func("POW", 10, scale))
    return self.func("TIMESTAMP_SECONDS", unix_seconds)


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
                    exp.select("*")
                    .from_(expression.this.copy(), copy=False)
                    .subquery(alias=alias, copy=False)
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
        expression.set(
            "fields", [transforms.unqualify_columns(field) for field in expression.fields]
        )

    return expression


def temporary_storage_provider(expression: exp.Expression) -> exp.Expression:
    # spark2, spark, Databricks require a storage provider for temporary tables
    provider = exp.FileFormatProperty(this=exp.Literal.string("parquet"))
    expression.args["properties"].append("expressions", provider)
    return expression


class Spark2(Hive):
    ALTER_TABLE_SUPPORTS_CASCADE = False

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://spark.apache.org/docs/latest/api/sql/index.html#initcap
    # https://docs.databricks.com/aws/en/sql/language-manual/functions/initcap
    # https://github.com/apache/spark/blob/master/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L859-L905
    INITCAP_DEFAULT_DELIMITER_CHARS = " "

    class Tokenizer(Hive.Tokenizer):
        HEX_STRINGS = [("X'", "'"), ("x'", "'")]

        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
        }

    class Parser(Hive.Parser):
        TRIM_PATTERN_FIRST = True
        CHANGE_COLUMN_ALTER_SYNTAX = True

        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            "AGGREGATE": exp.Reduce.from_arg_list,
            "BOOLEAN": _build_as_cast("boolean"),
            "DATE": _build_as_cast("date"),
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=exp.var(seq_get(args, 0))
            ),
            "DAYOFMONTH": lambda args: exp.DayOfMonth(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DAYOFWEEK": lambda args: exp.DayOfWeek(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DAYOFYEAR": lambda args: exp.DayOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "DOUBLE": _build_as_cast("double"),
            "FLOAT": _build_as_cast("float"),
            "FORMAT_STRING": exp.Format.from_arg_list,
            "FROM_UTC_TIMESTAMP": lambda args, dialect: exp.AtTimeZone(
                this=exp.cast(
                    seq_get(args, 0) or exp.Var(this=""),
                    exp.DataType.Type.TIMESTAMP,
                    dialect=dialect,
                ),
                zone=seq_get(args, 1),
            ),
            "LTRIM": lambda args: build_trim(args, reverse_args=True),
            "INT": _build_as_cast("int"),
            "MAP_FROM_ARRAYS": exp.Map.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "RTRIM": lambda args: build_trim(args, is_left=False, reverse_args=True),
            "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),
            "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
            "STRING": _build_as_cast("string"),
            "SLICE": exp.ArraySlice.from_arg_list,
            "TIMESTAMP": _build_as_cast("timestamp"),
            "TO_TIMESTAMP": lambda args: (
                _build_as_cast("timestamp")(args)
                if len(args) == 1
                else build_formatted_time(exp.StrToTime, "spark")(args)
            ),
            "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
            "TO_UTC_TIMESTAMP": lambda args, dialect: exp.FromTimeZone(
                this=exp.cast(
                    seq_get(args, 0) or exp.Var(this=""),
                    exp.DataType.Type.TIMESTAMP,
                    dialect=dialect,
                ),
                zone=seq_get(args, 1),
            ),
            "TRUNC": lambda args: exp.DateTrunc(unit=seq_get(args, 1), this=seq_get(args, 0)),
            "WEEKOFYEAR": lambda args: exp.WeekOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        }

        FUNCTION_PARSERS = {
            **Hive.Parser.FUNCTION_PARSERS,
            "APPROX_PERCENTILE": lambda self: self._parse_quantile_function(exp.ApproxQuantile),
            "BROADCAST": lambda self: self._parse_join_hint("BROADCAST"),
            "BROADCASTJOIN": lambda self: self._parse_join_hint("BROADCASTJOIN"),
            "MAPJOIN": lambda self: self._parse_join_hint("MAPJOIN"),
            "MERGE": lambda self: self._parse_join_hint("MERGE"),
            "SHUFFLEMERGE": lambda self: self._parse_join_hint("SHUFFLEMERGE"),
            "MERGEJOIN": lambda self: self._parse_join_hint("MERGEJOIN"),
            "SHUFFLE_HASH": lambda self: self._parse_join_hint("SHUFFLE_HASH"),
            "SHUFFLE_REPLICATE_NL": lambda self: self._parse_join_hint("SHUFFLE_REPLICATE_NL"),
        }

        def _parse_drop_column(self) -> t.Optional[exp.Drop | exp.Command]:
            return self._match_text_seq("DROP", "COLUMNS") and self.expression(
                exp.Drop, this=self._parse_schema(), kind="COLUMNS"
            )

        def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
            if len(aggregations) == 1:
                return []
            return pivot_column_names(aggregations, dialect="spark")

    class Generator(Hive.Generator):
        QUERY_HINTS = True
        NVL2_SUPPORTED = True
        CAN_IMPLEMENT_ARRAY_ANY = True
        ALTER_SET_TYPE = "TYPE"

        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,
            exp.EngineProperty: exp.Properties.Location.UNSUPPORTED,
            exp.AutoIncrementProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CharacterSetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CollateProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TS_OR_DS_EXPRESSIONS = (
            *Hive.Generator.TS_OR_DS_EXPRESSIONS,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfYear,
            exp.WeekOfYear,
        )

        TRANSFORMS = {
            **Hive.Generator.TRANSFORMS,
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArraySum: lambda self,
            e: f"AGGREGATE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.ArrayToString: rename_func("ARRAY_JOIN"),
            exp.ArraySlice: rename_func("SLICE"),
            exp.AtTimeZone: lambda self, e: self.func(
                "FROM_UTC_TIMESTAMP", e.this, e.args.get("zone")
            ),
            exp.BitwiseLeftShift: rename_func("SHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("SHIFTRIGHT"),
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    lambda e: ctas_with_tmp_tables_to_create_tmp_view(
                        e, temporary_storage_provider
                    ),
                    move_schema_columns_to_partitioned_by,
                ]
            ),
            exp.DateFromParts: rename_func("MAKE_DATE"),
            exp.DateTrunc: lambda self, e: self.func("TRUNC", e.this, unit_to_str(e)),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            # (DAY_OF_WEEK(datetime) % 7) + 1 is equivalent to DAYOFWEEK_ISO(datetime)
            exp.DayOfWeekIso: lambda self, e: f"(({self.func('DAYOFWEEK', e.this)} % 7) + 1)",
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.Format: rename_func("FORMAT_STRING"),
            exp.From: transforms.preprocess([_unalias_pivot]),
            exp.FromTimeZone: lambda self, e: self.func(
                "TO_UTC_TIMESTAMP", e.this, e.args.get("zone")
            ),
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
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.eliminate_distinct_on,
                    transforms.unnest_to_explode,
                    transforms.any_to_exists,
                ]
            ),
            exp.SHA2Digest: lambda self, e: self.func(
                "SHA2", e.this, e.args.get("length") or exp.Literal.number(256)
            ),
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.UnixToTime: _unix_to_time_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.WithinGroup: transforms.preprocess(
                [transforms.remove_within_group_for_percentiles]
            ),
        }
        TRANSFORMS.pop(exp.ArraySort)
        TRANSFORMS.pop(exp.ILike)
        TRANSFORMS.pop(exp.Left)
        TRANSFORMS.pop(exp.MonthsBetween)
        TRANSFORMS.pop(exp.Right)

        WRAP_DERIVED_VALUES = False
        CREATE_FUNCTION_RETURN_AS = False

        def struct_sql(self, expression: exp.Struct) -> str:
            from sqlglot.generator import Generator

            return Generator.struct_sql(self, expression)

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            arg = expression.this
            is_json_extract = isinstance(
                arg, (exp.JSONExtract, exp.JSONExtractScalar)
            ) and not arg.args.get("variant_extract")

            # We can't use a non-nested type (eg. STRING) as a schema
            if expression.to.args.get("nested") and (is_parse_json(arg) or is_json_extract):
                schema = f"'{self.sql(expression, 'to')}'"
                return self.func("FROM_JSON", arg if is_json_extract else arg.this, schema)

            if is_parse_json(expression):
                return self.func("TO_JSON", arg)

            return super(Hive.Generator, self).cast_sql(expression, safe_prefix=safe_prefix)

        def fileformatproperty_sql(self, expression: exp.FileFormatProperty) -> str:
            if expression.args.get("hive_format"):
                return super().fileformatproperty_sql(expression)

            return f"USING {expression.name.upper()}"

        def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
            this = self.sql(expression, "this")
            new_name = self.sql(expression, "rename_to") or this
            comment = self.sql(expression, "comment")
            if new_name == this:
                if comment:
                    return f"ALTER COLUMN {this} COMMENT {comment}"
                return super(Hive.Generator, self).altercolumn_sql(expression)
            return f"RENAME COLUMN {this} TO {new_name}"

        def renamecolumn_sql(self, expression: exp.RenameColumn) -> str:
            return super(Hive.Generator, self).renamecolumn_sql(expression)
