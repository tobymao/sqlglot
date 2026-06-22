from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.dremio import DremioGenerator
from sqlglot.parsers.dremio import DremioParser


class Dremio(Dialect):
    SUPPORTS_USER_DEFINED_TYPES = False
    CONCAT_COALESCE = True
    CONCAT_WS_COALESCE = True
    TYPED_DIVISION = True
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_VALUES_DEFAULT = False

    TIME_MAPPING = {
        # year
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        # month / day
        "MM": "%m",
        "mm": "%m",
        "MON": "%b",
        "mon": "%b",
        "MONTH": "%B",
        "month": "%B",
        "DDD": "%j",
        "ddd": "%j",
        "DD": "%d",
        "dd": "%d",
        "DY": "%a",
        "dy": "%a",
        "DAY": "%A",
        "day": "%A",
        # hours / minutes / seconds
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "HH": "%I",
        "hh": "%I",  # 24- / 12-hour
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FFF": "%f",
        "fff": "%f",
        "AMPM": "%p",
        "ampm": "%p",
        # ISO week / century etc.
        "WW": "%W",
        "ww": "%W",
        "D": "%w",
        "d": "%w",
        "CC": "%C",
        "cc": "%C",
        # timezone
        "TZD": "%Z",
        "tzd": "%Z",  # abbreviation (UTC, PST, ...)
        "TZO": "%z",
        "tzo": "%z",  # numeric offset (+0200)
    }

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "//", ("/*", "*/")]

    Parser = DremioParser

    Generator = DremioGenerator
