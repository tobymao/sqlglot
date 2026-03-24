from __future__ import annotations


from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.drill import DrillGenerator
from sqlglot.parsers.drill import DrillParser


class Drill(Dialect):
    NORMALIZE_FUNCTIONS: bool | str = False
    PRESERVE_ORIGINAL_NAMES = True
    NULL_ORDERING = "nulls_are_last"
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"
    SUPPORTS_USER_DEFINED_TYPES = False
    TYPED_DIVISION = True
    CONCAT_COALESCE = True

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
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        KEYWORDS = tokens.Tokenizer.KEYWORDS.copy()
        KEYWORDS.pop("/*+")

    Parser = DrillParser

    Generator = DrillGenerator
