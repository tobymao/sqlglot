from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.tableau import TableauGenerator
from sqlglot.parsers.tableau import TableauParser


class Tableau(Dialect):
    LOG_BASE_FIRST = False

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = [("[", "]")]
        QUOTES = ["'", '"']

    Generator = TableauGenerator

    Parser = TableauParser
