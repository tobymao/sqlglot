from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    create_with_partitions_sql,
    datestrtodate_sql,
    format_time_lambda,
    no_trycast_sql,
    rename_func,
    str_position_sql,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
)


def _date_add_sql(kind: str) -> t.Callable[[generator.Generator, exp.DateAdd | exp.DateSub], str]:
    def func(self: generator.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
        this = self.sql(expression, "this")
        unit = exp.var(expression.text("unit").upper() or "DAY")
        return (
            f"DATE_{kind}({this}, {self.sql(exp.Interval(this=expression.expression, unit=unit))})"
        )

    return func


def _str_to_date(self: generator.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Drill.DATE_FORMAT:
        return f"CAST({this} AS DATE)"
    return f"TO_DATE({this}, {time_format})"


class Drill(Dialect):
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        "y": "%Y",
        "Y": "%Y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
        "s": "%-S",
        "SSSSSS": "%f",
        "a": "%p",
        "DD": "%j",
        "D": "%-j",
        "E": "%a",
        "EE": "%a",
        "EEE": "%a",
        "EEEE": "%A",
        "''T''": "T",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]
        ENCODE = "utf-8"

    class Parser(parser.Parser):
        STRICT_CAST = False
        CONCAT_NULL_OUTPUTS_STRING = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "drill"),
            "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
            "TO_CHAR": format_time_lambda(exp.TimeToStr, "drill"),
        }

        LOG_DEFAULTS_TO_LN = True

    class Generator(generator.Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.SMALLINT: "INTEGER",
            exp.DataType.Type.TINYINT: "INTEGER",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.NCHAR: "VARCHAR",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ArrayContains: rename_func("REPEATED_CONTAINS"),
            exp.ArraySize: rename_func("REPEATED_COUNT"),
            exp.Create: create_with_partitions_sql,
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateToDi: lambda self, e: f"CAST(TO_DATE({self.sql(e, 'this')}, {Drill.DATEINT_FORMAT}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS VARCHAR), {Drill.DATEINT_FORMAT})",
            exp.If: lambda self, e: f"`IF`({self.format_args(e.this, e.args.get('true'), e.args.get('false'))})",
            exp.ILike: lambda self, e: f" {self.sql(e, 'this')} `ILIKE` {self.sql(e, 'expression')}",
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.StrPosition: str_position_sql,
            exp.StrToDate: _str_to_date,
            exp.Pow: rename_func("POW"),
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.TryCast: no_trycast_sql,
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD(CAST({self.sql(e, 'this')} AS DATE), {self.sql(exp.Interval(this=e.expression, unit=exp.var('DAY')))})",
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("drill"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)",
        }

        def normalize_func(self, name: str) -> str:
            return name if exp.SAFE_IDENTIFIER_RE.match(name) else f"`{name}`"
