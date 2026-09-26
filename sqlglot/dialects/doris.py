from __future__ import annotations

from sqlglot.dialects.mysql import MySQL
from sqlglot.tokens import TokenType
from sqlglot.generators.doris import DorisGenerator
from sqlglot.parsers.doris import DorisParser


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "IPV4": TokenType.IPV4,
            "IPV6": TokenType.IPV6,
            "LARGEINT": TokenType.INT128,
        }

    Parser = DorisParser

    Generator = DorisGenerator
