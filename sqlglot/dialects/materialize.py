from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser


class Materialize(Postgres):
    NORMALIZE_NOT_NULL = True

    UNESCAPED_SEQUENCES = {"\\a": "a", "\\v": "v"}

    class Tokenizer(Postgres.Tokenizer):
        NUMERIC_ESCAPES = {"u": (16, 4, 4, 0xFFFF), "U": (16, 8, 8, 0x10FFFF)}

    Parser = MaterializeParser

    Generator = MaterializeGenerator
