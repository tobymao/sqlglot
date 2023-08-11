from __future__ import annotations

from typing import Union

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    parse_timestamp_trunc,
    rename_func,
    time_format,
)
from sqlglot.dialects.mysql import MySQL


def _to_date_sql(
    self: generator.Generator, expression: Union[exp.Anonymous, exp.TsOrDsToDate]
) -> str:
    if isinstance(expression, exp.Anonymous):
        expressions = expression.args.get("expressions")
        if expressions is None or len(expressions) == 0:
            raise ValueError("Missing expressions for TO_DATE")
        expr = expressions[0]
        time_format = expressions[1] if len(expressions) > 1 else None
        expr_TIMESTAMP = self.sql(expression, "this")
        if expr_TIMESTAMP == "TIMESTAMP":
            return f"TIMESTAMP({expr})"
    elif isinstance(expression, exp.TsOrDsToDate):
        expr = expression.args.get("this")
        time_format = expression.args.get("format")
    else:
        raise ValueError("Invalid expression type")

    if time_format is not None:
        return f"STR_TO_DATE({expr}, {time_format})"
    else:
        return f"TO_DATE({expr})"


def handle_nvl2(self, expression: exp.Nvl2) -> str:
    this = self.sql(expression, "this")
    expr1 = self.sql(expression, "true")
    expr2 = self.sql(expression, "false")
    return f"CASE WHEN {this} IS NOT NULL THEN {expr2} ELSE {expr1} END"


def handle_to_char(self, expression: exp.ToChar) -> str:
    self.sql(expression, "this")
    decimal_places, has_decimal = parse_format_string(self.sql(expression, "format"))
    if has_decimal:
        return f"Round({self.sql(expression, 'this')},{decimal_places})"
    else:
        return f"DATE_FORMAT({self.sql(expression, 'this')}, {self.format_time(expression)})"


def parse_format_string(format_string):
    decimal_places = None
    if "." in format_string:
        decimal_places = len(format_string) - format_string.index(".") - 2
    has_decimal = decimal_places is not None
    return decimal_places, has_decimal


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": parse_timestamp_trunc,
            "REGEXP": exp.RegexpLike.from_arg_list,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.Anonymous: _to_date_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.DateTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.SetAgg: rename_func("COLLECT_SET"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.SafeDPipe: rename_func("CONCAT"),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.ToChar: handle_to_char,
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",  # Only for day level
            exp.TsOrDsToDate: lambda self, e: self.func("TO_DATE", e.this),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("doris")(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.Map: rename_func("ARRAY_MAP"),
            exp.Nvl2: handle_nvl2,
        }
