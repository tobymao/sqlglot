from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    parse_timestamp_trunc,
    rename_func,
    time_format,
)
from sqlglot.dialects.mysql import MySQL

DATE_DELTA_INTERVAL = {
    "year": "year",
    "yyyy": "year",
    "yy": "year",
    "quarter": "quarter",
    "qq": "quarter",
    "q": "quarter",
    "month": "month",
    "mm": "month",
    "m": "month",
    "week": "week",
    "ww": "week",
    "wk": "week",
    "day": "day",
    "dd": "day",
    "d": "day",
}


def handle_date_trunc(self, expression: exp.DateTrunc) -> str:
    unit = self.sql(expression, "unit").strip("\"'").lower()
    this = self.sql(expression, "this")
    if unit.isalpha():
        mapped_unit = (
            DATE_DELTA_INTERVAL.get(unit) if DATE_DELTA_INTERVAL.get(unit) != None else unit
        )
        return f"DATE_TRUNC({this}, '{mapped_unit}')"
    elif unit.isdigit():
        return f"TRUNCATE({this}, {unit})"
    return f"DATE({this})"


def handle_to_date(self: Doris.Generator, expression: exp.TsOrDsToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format and time_format not in (Doris.TIME_FORMAT, Doris.DATE_FORMAT):
        return f"DATE_FORMAT({this}, {time_format})"
    if isinstance(expression.this, exp.TsOrDsToDate):
        return this
    return f"TO_DATE({this})"


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        **MySQL.TIME_MAPPING,
        "%Y": "yyyy",
        "%m": "MM",
        "%d": "dd",
        "%s": "ss",
        "%H": "HH24",
        "%i": "mi",
    }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
            "DATE_TRUNC": parse_timestamp_trunc,
            "REGEXP": exp.RegexpLike.from_arg_list,
            "TO_DATE": exp.TsOrDsToDate.from_arg_list,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TIMESTAMP_FUNC_TYPES = set()

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.DateTrunc: handle_date_trunc,
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.Map: rename_func("ARRAY_MAP"),
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.ToChar: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",  # Only for day level
            exp.TsOrDsToDate: handle_to_date,
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("doris")(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }
