from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.risingwave import RisingWaveGenerator
from sqlglot.parsers.risingwave import RisingWaveParser
from sqlglot.tokens import TokenType
from sqlglot.typing import annotate_negated_literal


class RisingWave(Postgres):
    EXPRESSION_METADATA = {
        **Postgres.EXPRESSION_METADATA,
        exp.Neg: {"annotator": annotate_negated_literal},
    }
    NORMALIZE_NOT_NULL = True

    REQUIRES_PARENTHESIZED_STRUCT_ACCESS = True
    SUPPORTS_STRUCT_STAR_EXPANSION = True

    UNESCAPED_SEQUENCES = {"\\a": "a", "\\v": "v"}

    class Tokenizer(Postgres.Tokenizer):
        UNICODE_IDENTIFIERS = []

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "SOURCE": TokenType.SOURCE,
        }

    Parser = RisingWaveParser

    Generator = RisingWaveGenerator
