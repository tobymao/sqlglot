from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import UsingColumnOrder
from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.starrocks import StarRocksGenerator
from sqlglot.parsers.starrocks import StarRocksParser
from sqlglot.tokens import TokenType


class StarRocks(MySQL):
    STRICT_JSON_PATH_SYNTAX = False
    INDEX_OFFSET = 1
    USING_COLUMN_ORDER = UsingColumnOrder.USING_LIST

    DEFAULT_FUNCTIONS_COLUMN_NAMES = {
        exp.GenerateSeries: "generate_series",
    }

    class Tokenizer(MySQL.Tokenizer):
        DASH_COMMENT_REQUIRES_BOUNDARY = False
        COMMENTS_TERMINATE_AT_NEWLINE_ONLY = False

        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "LARGEINT": TokenType.INT128,
            "REFRESH": TokenType.REFRESH,
        }
        KEYWORDS.pop("IGNORE")

    Parser = StarRocksParser

    Generator = StarRocksGenerator
