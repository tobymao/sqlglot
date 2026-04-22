from __future__ import annotations

from sqlglot.dialects.presto import Presto
from sqlglot.generators.trino import TrinoGenerator
from sqlglot.parsers.trino import TrinoParser
from sqlglot.tokens import TokenType


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True
    CONCAT_WS_COALESCE = True

    class Tokenizer(Presto.Tokenizer):
        KEYWORDS = {
            **Presto.Tokenizer.KEYWORDS,
            "REFRESH": TokenType.REFRESH,
        }

    Parser = TrinoParser

    Generator = TrinoGenerator
