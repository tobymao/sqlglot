from __future__ import annotations

import typing as t

from sqlglot.dialects.spark2 import Spark2
from sqlglot.generators.spark import SparkGenerator
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType
from sqlglot.typing.spark import EXPRESSION_METADATA


class Spark(Spark2):
    SUPPORTS_ORDER_BY_ALL = True
    SUPPORTS_LIMIT_ALL = True
    SUPPORTS_NULL_TYPE = True
    ARRAY_FUNCS_PROPAGATES_NULLS = True
    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # Spark 3+ parses MM/dd strictly (single-digit months/days don't parse), unlike the
    # lax %m/%d other dialects produce. When *parsing* (StrToTime/StrToDate/...), MM/dd
    # map to a distinct canonical token so the strict roundtrip is preserved; formatting
    # keeps the regular padded %m/%d -> MM/dd (TIME_MAPPING is unchanged).
    STRICT_TIME_MAPPING = {
        **Spark2.TIME_MAPPING,
        "MM": "%mstrict",
        "dd": "%dstrict",
    }
    # Generating a parse format is lenient: %m/%d -> M/d (matching strptime), while the
    # strict tokens map back to MM/dd.
    LENIENT_INVERSE_TIME_MAPPING = {
        **{v: k for k, v in STRICT_TIME_MAPPING.items()},
        "%m": "M",
        "%d": "d",
    }

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
