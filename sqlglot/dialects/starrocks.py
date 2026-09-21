from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.starrocks import StarRocksGenerator
from sqlglot.parsers.starrocks import StarRocksParser
from sqlglot.tokens import TokenType


class StarRocks(MySQL):
    STRICT_JSON_PATH_SYNTAX = False
    INDEX_OFFSET = 1

    DEFAULT_FUNCTIONS_COLUMN_NAMES = {
        exp.GenerateSeries: "generate_series",
    }

    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            # IGNORE isn't a keyword here (no INSERT IGNORE or index hints), and tokenizing it
            # as one keeps the parser from seeing IGNORE NULLS
            **{k: v for k, v in MySQL.Tokenizer.KEYWORDS.items() if k != "IGNORE"},
            "LARGEINT": TokenType.INT128,
            "REFRESH": TokenType.REFRESH,
        }

    Parser = StarRocksParser

    Generator = StarRocksGenerator
