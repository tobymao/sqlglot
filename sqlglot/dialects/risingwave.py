from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.risingwave import RisingWaveGenerator
from sqlglot.parsers.risingwave import RisingWaveParser
from sqlglot.tokens import TokenType


class RisingWave(Postgres):
    REQUIRES_PARENTHESIZED_STRUCT_ACCESS = True
    SUPPORTS_STRUCT_STAR_EXPANSION = True

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "SOURCE": TokenType.SOURCE,
        }

    Parser = RisingWaveParser

    Generator = RisingWaveGenerator
