from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.dax import DAXGenerator
from sqlglot.parsers.dax import DAXParser


class DAX(Dialect):
    DPIPE_IS_STRING_CONCAT = False

    Generator = DAXGenerator

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["'", ("[", "]")]
        QUOTES = ['"']
        STRING_ESCAPES = ['"']
        COMMENTS = ["--", "//", ("/*", "*/")]

    Parser = DAXParser
