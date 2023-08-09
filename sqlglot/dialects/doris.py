from __future__ import annotations

import typing as t

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    rename_func,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get


def _to_date_sql(self: MySQL.Generator, expression: exp.TsOrDsToDate) -> str:
    this = self.sql(expression, "this")
    self.format_time(expression)
    return f"TO_DATE({this})"


def _time_format(
    self: generator.Generator, expression: exp.UnixToStr | exp.StrToUnix
) -> t.Optional[str]:
    time_format = self.format_time(expression)
    if time_format == Doris.TIME_FORMAT:
        return None
    return time_format


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        "%M": "%B",
        "%m": "%%-M",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%S": "%S",
        "%u": "%W",
        "%k": "%-H",
        "%l": "%-I",
        "%W": "%a",
        "%Y": "%Y",
        "%d": "%%-d",
        "%H": "%%-H",
        "%s": "%%-S",
        "%D": "%%-j",
        "%a": "%%p",
        "%y": "%%Y",
        "%": "%%",
    }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=seq_get(args, 0)
            ),
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
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.Coalesce: rename_func("NVL"),
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
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.ToChar: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",# Only for day level
            exp.TsOrDsToDate: _to_date_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, _time_format(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.Map: rename_func("ARRAY_MAP"),
        }
