from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    UsingColumnOrder,
)
from sqlglot.generators.sqlite import SQLiteGenerator
from sqlglot.parsers.sqlite import SQLiteParser
from sqlglot.tokens import TokenType


class SQLite(Dialect):
    # https://sqlite.org/forum/forumpost/5e575586ac5c711b?raw
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    ASCII_ONLY_NORMALIZATION = True
    TYPED_DIVISION = True
    SAFE_DIVISION = True
    SAFE_TO_ELIMINATE_DOUBLE_NEGATION = False
    USING_COLUMN_ORDER = UsingColumnOrder.IN_PLACE
    CONCAT_COALESCE = True
    CONCAT_WS_COALESCE = True

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

        NESTED_COMMENTS = False
        COMMENTS_TERMINATE_AT_NEWLINE_ONLY = True

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
