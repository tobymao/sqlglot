from __future__ import annotations

import re
import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    create_with_partitions_sql,
    datestrtodate_sql,
    format_time_lambda,
    no_pivot_sql,
    no_trycast_sql,
    rename_func,
    str_position_sql,
    timestrtotime_sql,
)


def _str_to_time_sql(self: generator.Generator, expression: exp.TsOrDsToDate) -> str:
    return f"STRPTIME({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_to_date_sql(self: generator.Generator, expression: exp.TsOrDsToDate) -> str:
    time_format = self.format_time(expression)
    if time_format and time_format not in (Drill.time_format, Drill.date_format):
        return f"CAST({_str_to_time_sql(self, expression)} AS DATE)"
    return f"CAST({self.sql(expression, 'this')} AS DATE)"


def _date_add_sql(kind: str) -> t.Callable[[generator.Generator, exp.DateAdd | exp.DateSub], str]:
    def func(self: generator.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
        this = self.sql(expression, "this")
        unit = exp.Var(this=expression.text("unit").upper() or "DAY")
        return (
            f"DATE_{kind}({this}, {self.sql(exp.Interval(this=expression.expression, unit=unit))})"
        )

    return func


def if_sql(self: generator.Generator, expression: exp.If) -> str:
    """
    Drill requires backticks around certain SQL reserved words, IF being one of them,  This function
    adds the backticks around the keyword IF.
    Args:
        self: The Drill dialect
        expression: The input IF expression

    Returns:  The expression with IF in backticks.

    """
    expressions = self.format_args(
        expression.this, expression.args.get("true"), expression.args.get("false")
    )
    return f"`IF`({expressions})"


def _str_to_date(self: generator.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Drill.date_format:
        return f"CAST({this} AS DATE)"
    return f"TO_DATE({this}, {time_format})"


class Drill(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"
    date_format = "'yyyy-MM-dd'"
    dateint_format = "'yyyyMMdd'"
    time_format = "'yyyy-MM-dd HH:mm:ss'"

    time_mapping = {
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

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
            "TO_CHAR": format_time_lambda(exp.TimeToStr, "drill"),
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
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
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA_ROOT,
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ArrayContains: rename_func("REPEATED_CONTAINS"),
            exp.ArraySize: rename_func("REPEATED_COUNT"),
            exp.Create: create_with_partitions_sql,
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateToDi: lambda self, e: f"CAST(TO_DATE({self.sql(e, 'this')}, {Drill.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS VARCHAR), {Drill.dateint_format})",
            exp.If: if_sql,
            exp.ILike: lambda self, e: f" {self.sql(e, 'this')} `ILIKE` {self.sql(e, 'expression')}",
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Pivot: no_pivot_sql,
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.StrPosition: str_position_sql,
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TryCast: no_trycast_sql,
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD(CAST({self.sql(e, 'this')} AS DATE), {self.sql(exp.Interval(this=e.expression, unit=exp.Var(this='DAY')))})",
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)",
        }

        def normalize_func(self, name: str) -> str:
            return name if re.match(exp.SAFE_IDENTIFIER_RE, name) else f"`{name}`"
