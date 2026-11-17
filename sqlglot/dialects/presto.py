from __future__ import annotations

import typing as t

from sqlglot import exp, tokens
from sqlglot.dialects.dialect import (
    AliasPrecedence,
    Dialect,
    NormalizationStrategy,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.presto import PrestoGenerator
from sqlglot.parsers.presto import PrestoParser
from sqlglot.tokens import TokenType
from sqlglot.trie import new_trie
from sqlglot.typing.presto import EXPRESSION_METADATA


class Presto(Dialect):
    INDEX_OFFSET = 1
    NULL_ORDERING = "nulls_are_last"
    TIME_FORMAT = MySQL.TIME_FORMAT
    STRICT_STRING_CONCAT = True
    TYPED_DIVISION = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    LOG_BASE_FIRST: bool | None = None
    SUPPORTS_LIMIT_ALL = True
    COLUMN_ALIAS_PRECEDENCE = {
        exp.Having: AliasPrecedence.SOURCE,
        exp.Qualify: AliasPrecedence.SOURCE,
    }
    SUPPORTS_VALUES_DEFAULT = False
    LEAST_GREATEST_IGNORES_NULLS = False
    UUID_IS_STRING_TYPE = False

    TIME_MAPPING = MySQL.TIME_MAPPING

    # FORMAT_DATETIME's Joda patterns, which DATE_FORMAT's MySQL codes can't always express, e.g. SSS.
    # Fields that would format differently elsewhere are left out: fractions of a second other than
    # ss.SSS (Joda keeps only milliseconds, and BigQuery writes them only with seconds), z and Z
    # (other dialects don't keep the zone) and d (BigQuery pads it with a space).
    # https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
    JODA_TIME_MAPPING = {
        "yyyy": "%Y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "DDD": "%j",
        "D": "%-j",
        "EEEE": "%A",
        "EEE": "%a",
        "a": "%p",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
        "s": "%-S",
        "ss.SSS": "%S.%f_three",
    }
    JODA_TIME_TRIE = new_trie(JODA_TIME_MAPPING)
    INVERSE_JODA_TIME_MAPPING = {v: k for k, v in JODA_TIME_MAPPING.items()}
    INVERSE_JODA_TIME_TRIE = new_trie(INVERSE_JODA_TIME_MAPPING)

    # https://github.com/trinodb/trino/issues/17
    # https://github.com/trinodb/trino/issues/12289
    # https://github.com/prestodb/presto/issues/2863
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    SUPPORTED_SETTINGS = {
        *Dialect.SUPPORTED_SETTINGS,
        "variant_extract_is_json_extract",
    }

    class Tokenizer(tokens.Tokenizer):
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        UNICODE_STRINGS = [
            (prefix + q, q)
            for q in t.cast(list[str], tokens.Tokenizer.QUOTES)
            for prefix in ("U&", "u&")
        ]

        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "DEALLOCATE PREPARE": TokenType.COMMAND,
            "DESCRIBE INPUT": TokenType.COMMAND,
            "DESCRIBE OUTPUT": TokenType.COMMAND,
            "RESET SESSION": TokenType.COMMAND,
            "START": TokenType.BEGIN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "ROW": TokenType.STRUCT,
            "IPADDRESS": TokenType.IPADDRESS,
            "IPPREFIX": TokenType.IPPREFIX,
            "TDIGEST": TokenType.TDIGEST,
            "HYPERLOGLOG": TokenType.HLLSKETCH,
        }
        KEYWORDS.pop("/*+")
        KEYWORDS.pop("QUALIFY")

    Parser = PrestoParser

    Generator = PrestoGenerator
