from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.prql import PRQLGenerator
from sqlglot.parsers.prql import PRQLParser
from sqlglot.tokens import TokenType


class PRQL(Dialect):
    DPIPE_IS_STRING_CONCAT = False

    Generator = PRQLGenerator

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["`"]
        QUOTES = ["'", '"']

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "=": TokenType.ALIAS,
            "'": TokenType.QUOTE,
            '"': TokenType.QUOTE,
            "`": TokenType.IDENTIFIER,
            "#": TokenType.COMMENT,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    Parser = PRQLParser
