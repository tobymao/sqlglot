from __future__ import annotations

import typing as t

from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.presto import PrestoGenerator
from sqlglot.parsers.presto import PrestoParser
from sqlglot.tokens import TokenType
from sqlglot.typing.presto import EXPRESSION_METADATA


class Presto(Dialect):
    INDEX_OFFSET = 1
    NULL_ORDERING = "nulls_are_last"
    TIME_FORMAT = MySQL.TIME_FORMAT
    STRICT_STRING_CONCAT = True
    TYPED_DIVISION = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    LOG_BASE_FIRST: bool | None = None
    SUPPORTS_VALUES_DEFAULT = False
    LEAST_GREATEST_IGNORES_NULLS = False
    UUID_IS_STRING_TYPE = False

    TIME_MAPPING = MySQL.TIME_MAPPING

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
