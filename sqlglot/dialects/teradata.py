from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.teradata import TeradataGenerator
from sqlglot.parsers.teradata import TeradataParser
from sqlglot.tokens import TokenType


class Teradata(Dialect):
    TYPED_DIVISION = True

    TIME_MAPPING = {
        "YY": "%y",
        "Y4": "%Y",
        "YYYY": "%Y",
        "M4": "%B",
        "M3": "%b",
        "M": "%-M",
        "MI": "%M",
        "MM": "%m",
        "MMM": "%b",
        "MMMM": "%B",
        "D": "%-d",
        "DD": "%d",
        "D3": "%j",
        "DDD": "%j",
        "H": "%-H",
        "HH": "%H",
        "HH24": "%H",
        "S": "%-S",
        "SS": "%S",
        "SSSSSS": "%f",
        "E": "%a",
        "EE": "%a",
        "E3": "%a",
        "E4": "%A",
        "EEE": "%a",
        "EEEE": "%A",
    }

    class Tokenizer(tokens.Tokenizer):
        # Tested each of these and they work, although there is no
        # Teradata documentation explicitly mentioning them.
        HEX_STRINGS = [("X'", "'"), ("x'", "'"), ("0x", "")]
        # https://docs.teradata.com/r/Teradata-Database-SQL-Functions-Operators-Exprs-and-Predicates/March-2017/Comparison-Operators-and-Functions/Comparison-Operators/ANSI-Compliance
        # https://docs.teradata.com/r/SQL-Functions-Operators-Exprs-and-Predicates/June-2017/Arithmetic-Trigonometric-Hyperbolic-Operators/Functions
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "**": TokenType.DSTAR,
            "^=": TokenType.NEQ,
            "BYTEINT": TokenType.SMALLINT,
            "COLLECT": TokenType.COMMAND,
            "DEL": TokenType.DELETE,
            "EQ": TokenType.EQ,
            "GE": TokenType.GTE,
            "GT": TokenType.GT,
            "HELP": TokenType.COMMAND,
            "INS": TokenType.INSERT,
            "LE": TokenType.LTE,
            "LOCKING": TokenType.LOCK,
            "LT": TokenType.LT,
            "MINUS": TokenType.EXCEPT,
            "MOD": TokenType.MOD,
            "NE": TokenType.NEQ,
            "NOT=": TokenType.NEQ,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "SEL": TokenType.SELECT,
            "ST_GEOMETRY": TokenType.GEOMETRY,
            "TOP": TokenType.TOP,
            "UPD": TokenType.UPDATE,
        }
        KEYWORDS.pop("/*+")

        # Teradata does not support % as a modulo operator
        SINGLE_TOKENS = {**tokens.Tokenizer.SINGLE_TOKENS}
        SINGLE_TOKENS.pop("%")

    Parser = TeradataParser

    Generator = TeradataGenerator
