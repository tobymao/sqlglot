from __future__ import annotations


from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.exasol import ExasolGenerator
from sqlglot.parsers.exasol import DATE_UNITS as DATE_UNITS, ExasolParser
from sqlglot.tokens import TokenType


class Exasol(Dialect):
    # https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm#SQLidentifier
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    # https://docs.exasol.com/db/latest/sql_references/data_types/datatypesoverview.htm
    SUPPORTS_USER_DEFINED_TYPES = False
    # https://docs.exasol.com/db/latest/sql/select.htm
    SUPPORTS_COLUMN_JOIN_MARKS = True
    NULL_ORDERING = "nulls_are_last"
    # https://docs.exasol.com/db/latest/sql_references/literals.htm#StringLiterals
    CONCAT_COALESCE = True

    TIME_MAPPING = {
        "yyyy": "%Y",
        "YYYY": "%Y",
        "yy": "%y",
        "YY": "%y",
        "mm": "%m",
        "MM": "%m",
        "MONTH": "%B",
        "MON": "%b",
        "dd": "%d",
        "DD": "%d",
        "DAY": "%A",
        "DY": "%a",
        "H12": "%I",
        "H24": "%H",
        "HH": "%H",
        "ID": "%u",
        "vW": "%V",
        "IW": "%V",
        "vYYY": "%G",
        "IYYY": "%G",
        "MI": "%M",
        "SS": "%S",
        "uW": "%W",
        "UW": "%U",
        "Z": "%z",
    }

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]")]
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "USER": TokenType.CURRENT_USER,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/if.htm
            "ENDIF": TokenType.END,
            "LONG VARCHAR": TokenType.TEXT,
            "REGEXP_LIKE": TokenType.RLIKE,
            "SEPARATOR": TokenType.SEPARATOR,
            "SYSTIMESTAMP": TokenType.SYSTIMESTAMP,
        }
        KEYWORDS.pop("DIV")

    Parser = ExasolParser

    Generator = ExasolGenerator
