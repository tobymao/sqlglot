from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.sqlite import SQLiteGenerator
from sqlglot.parsers.sqlite import SQLiteParser
from sqlglot.tokens import TokenType


class SQLite(Dialect):
    # https://sqlite.org/forum/forumpost/5e575586ac5c711b?raw
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    TYPED_DIVISION = True
    SAFE_DIVISION = True
    SAFE_TO_ELIMINATE_DOUBLE_NEGATION = False

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ATTACH": TokenType.ATTACH,
            "DETACH": TokenType.DETACH,
            "INDEXED BY": TokenType.INDEXED_BY,
            "MATCH": TokenType.MATCH,
        }

        KEYWORDS.pop("/*+")

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.REPLACE}

    Parser = SQLiteParser

    Generator = SQLiteGenerator
