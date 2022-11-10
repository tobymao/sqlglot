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
        # https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-TABLE
        return exp.UnixToTime.from_arg_list(args)
    # https://www.postgresql.org/docs/current/functions-formatting.html
    return format_time_lambda(exp.StrToTime, "postgres")(args)


def if_sql(self, expression):
    expressions = self.format_args(
        expression.this, expression.args.get("true"), expression.args.get("false")
    )
    return f"`IF`({expressions})"


class Drill(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"
    date_format = "'yyyy-MM-dd'"

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
        ESCAPES = ["\\"]
        ENCODE = "utf-8"

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "VARBINARY": TokenType.BINARY
        }

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
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.SMALLINT: "INTEGER",
            exp.DataType.Type.TINYINT: "INTEGER",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP"
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Lateral: _lateral_sql,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",

            exp.UnixToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')})",
            exp.ArrayContains: rename_func("REPEATED_CONTAINS"),
            exp.ArraySize: rename_func("REPEATED_COUNT"),
            exp.If: if_sql,
            exp.ILike: lambda self, e: f" {self.sql(e, 'this')} `ILIKE` {self.sql(e, 'expression')}",
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.StrPosition: str_position_sql,
            exp.TryCast: no_trycast_sql,
        }
