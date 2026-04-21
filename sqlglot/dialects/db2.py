from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.db2 import Db2 as Db2Generator
from sqlglot.tokens import TokenType


class Db2(Dialect):
    NULL_ORDERING = "nulls_are_large"
    TYPED_DIVISION = True

    class Tokenizer(tokens.Tokenizer):
        VAR_SINGLE_TOKENS = {"@"}

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "DBCLOB": TokenType.TEXT,
            "GRAPHIC": TokenType.NCHAR,
            "VARGRAPHIC": TokenType.NVARCHAR,
            "TIMESTMP": TokenType.TIMESTAMP,
        }

    Generator = Db2Generator
