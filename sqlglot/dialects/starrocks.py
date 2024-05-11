from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    build_timestamp_trunc,
    rename_func,
    unit_to_str,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get


class StarRocks(MySQL):
    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": build_timestamp_trunc,
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 0), expression=seq_get(args, 1), unit=exp.Literal.string("DAY")
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 1), expression=seq_get(args, 2), unit=seq_get(args, 0)
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
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", unit_to_str(e), e.this, e.expression
            ),
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.StrToUnix: lambda self, e: self.func("UNIX_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: self.func("FROM_UNIXTIME", e.this, self.format_time(e)),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        TRANSFORMS.pop(exp.DateTrunc)
