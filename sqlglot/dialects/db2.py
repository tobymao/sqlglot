from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.db2 import Db2 as Db2Generator


class Db2(Dialect):
    NULL_ORDERING = "nulls_are_large"
    TYPED_DIVISION = True

    class Tokenizer(tokens.Tokenizer):
        VAR_SINGLE_TOKENS = {"@"}

    Generator = Db2Generator
