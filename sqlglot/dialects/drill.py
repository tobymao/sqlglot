from __future__ import annotations

from sqlglot import exp, parser, tokens, generator
from sqlglot.dialects.dialect import (
    Dialect,
    create_with_partitions_sql,
    format_time_lambda,
    rename_func, str_position_sql,
    no_trycast_sql
)
from sqlglot.dialects.postgres import _lateral_sql
from sqlglot.tokens import TokenType


def _to_timestamp(args):
    # TO_TIMESTAMP accepts either a single double argument or (text, text)
    if len(args) == 1 and args[0].is_number:
        return exp.UnixToTime.from_arg_list(args)
    return format_time_lambda(exp.StrToTime, "drill")(args)

def _str_to_time_sql(self, expression):
    return f"STRPTIME({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_to_date_sql(self, expression):
    time_format = self.format_time(expression)
    if time_format and time_format not in (DuckDB.time_format, DuckDB.date_format):
        return f"CAST({_str_to_time_sql(self, expression)} AS DATE)"
    return f"CAST({self.sql(expression, 'this')} AS DATE)"


def if_sql(self, expression):
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


class Drill(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"
    date_format = "'yyyy-MM-dd'"

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
        "''T''": "T"
    }
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
        ESCAPES = ["\\"]
        ENCODE = "utf-8"

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "VARBINARY": TokenType.BINARY
        }

        date_format = "'yyyy-MM-dd'"
        dateint_format = "'yyyyMMdd'"
        time_format = "'yyyy-MM-dd HH:mm:ss'"

    class Parser(parser.Parser):
        STRICT_CAST = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
            "TO_CHAR": format_time_lambda(exp.TimeToStr, "drill"),
        }

    class Generator(generator.Generator):
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
            exp.DataType.Type.DATETIME: "TIMESTAMP"
        }

        ROOT_PROPERTIES = {
            exp.PartitionedByProperty
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Lateral: _lateral_sql,
            exp.ArrayContains: rename_func("REPEATED_CONTAINS"),
            exp.ArraySize: rename_func("REPEATED_COUNT"),
            exp.Create: create_with_partitions_sql,
            exp.If: if_sql,
            exp.ILike: lambda self, e: f" {self.sql(e, 'this')} `ILIKE` {self.sql(e, 'expression')}",
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'value')}",
            exp.StrPosition: str_position_sql,
            exp.StrToDate: lambda self, e: f"CAST({_str_to_time_sql(self, e)} AS DATE)",
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: lambda self, e: f"CAST({self.sql(e, 'this')} AS TIMESTAMP)",
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TryCast: no_trycast_sql,
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
        }
