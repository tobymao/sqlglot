from __future__ import annotations

from sqlglot.dialects.presto import Presto
from sqlglot.generators.trino import TrinoGenerator
from sqlglot.parsers.trino import TrinoParser
from sqlglot.tokens import TokenType


class Trino(Presto):
    # https://trino.io/docs/current/sql/select.html
    SET_OP_INTERSECT_HIGHER_PRECEDENCE = True
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True
    CONCAT_WS_COALESCE = True

    class Tokenizer(Presto.Tokenizer):
        KEYWORDS = {
            **Presto.Tokenizer.KEYWORDS,
            "REFRESH": TokenType.REFRESH,
            "DECLARE": TokenType.DECLARE,
        }
        # Trino has no `SQL SECURITY` clause, only bare `SECURITY DEFINER`/`INVOKER`;
        # the merged base keyword otherwise eats the `SQL` in `LANGUAGE SQL SECURITY DEFINER`.
        KEYWORDS.pop("SQL SECURITY")

    Parser = TrinoParser

    Generator = TrinoGenerator
