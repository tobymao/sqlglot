from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser
from sqlglot.typing import annotate_negated_literal


class Materialize(Postgres):
    EXPRESSION_METADATA = {
        **Postgres.EXPRESSION_METADATA,
        exp.Neg: {"annotator": annotate_negated_literal},
    }
    NORMALIZE_NOT_NULL = True

    UNESCAPED_SEQUENCES = {"\\a": "a", "\\v": "v"}

    class Tokenizer(Postgres.Tokenizer):
        UNICODE_IDENTIFIERS = []
        NUMERIC_ESCAPES = {"u": (16, 4, 4, 0xFFFF), "U": (16, 8, 8, 0x10FFFF)}

    Parser = MaterializeParser

    Generator = MaterializeGenerator
