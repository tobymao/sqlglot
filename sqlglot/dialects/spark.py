from __future__ import annotations

import typing as t

from sqlglot.dialects.spark2 import Spark2
from sqlglot.generators.spark import SparkGenerator
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType
from sqlglot.typing.spark import EXPRESSION_METADATA


class Spark(Spark2):
    SUPPORTS_ORDER_BY_ALL = True
    SUPPORTS_NULL_TYPE = True
    ARRAY_FUNCS_PROPAGATES_NULLS = True
    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    class Tokenizer(Spark2.Tokenizer):
        STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = False

        RAW_STRINGS = [
            (prefix + q, q)
            for q in t.cast(list[str], Spark2.Tokenizer.QUOTES)
            for prefix in ("r", "R")
        ]

        KEYWORDS = {
            **Spark2.Tokenizer.KEYWORDS,
            "DECLARE": TokenType.DECLARE,
        }

    Parser = SparkParser

    Generator = SparkGenerator
