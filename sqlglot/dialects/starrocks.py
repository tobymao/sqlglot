from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import arrow_json_extract_sql, rename_func
from sqlglot.dialects.mysql import MySQL


class StarRocks(MySQL):
    class Generator(MySQL.Generator):  # type: ignore
        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,  # type: ignore
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.DateDiff: rename_func("DATEDIFF"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }
        TRANSFORMS.pop(exp.DateTrunc)
