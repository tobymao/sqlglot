from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    rename_func,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get


class StarRocks(MySQL):
    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=seq_get(args, 0)
            ),
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
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.DateDiff: rename_func("DATEDIFF"),
            exp.RegexpLike: rename_func("REGEXP"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", exp.Literal.string(e.text("unit")), e.this
            ),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        TRANSFORMS.pop(exp.DateTrunc)
