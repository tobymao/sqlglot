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
        # Unlike MySQL, "#" doesn't start a comment
        COMMENTS = ["--", ("/*", "*/")]
        DASH_COMMENT_REQUIRES_BOUNDARY = False
        COMMENTS_TERMINATE_AT_NEWLINE_ONLY = False

        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "ADMIN": TokenType.COMMAND,
            "BACKUP": TokenType.COMMAND,
            "CANCEL": TokenType.COMMAND,
            "DEALLOCATE": TokenType.COMMAND,
            "EXPORT": TokenType.COMMAND,
            "HLL": TokenType.HLLSKETCH,
            "INSTALL": TokenType.COMMAND,
            "LARGEINT": TokenType.INT128,
            "PAUSE": TokenType.COMMAND,
            "RECOVER": TokenType.COMMAND,
            "REFRESH": TokenType.REFRESH,
            "RESTORE": TokenType.COMMAND,
            "RESUME": TokenType.COMMAND,
            "STOP": TokenType.COMMAND,
            "SUBMIT": TokenType.COMMAND,
            "UNINSTALL": TokenType.COMMAND,
        }
        KEYWORDS.pop("IGNORE")
        KEYWORDS.pop("#>")
        KEYWORDS.pop("#>>")

    Parser = StarRocksParser

    Generator = StarRocksGenerator
