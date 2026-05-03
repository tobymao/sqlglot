from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.feldera import FelderaGenerator
from sqlglot.parsers.feldera import FelderaParser
from sqlglot.tokens import TokenType


class Feldera(Postgres):
    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE
    NULL_ORDERING = "nulls_are_last"
    TYPED_DIVISION = True
    SAFE_DIVISION = False
    CONCAT_COALESCE = False
    DATE_FORMAT = "'%Y-%m-%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"
    TIME_MAPPING = {}

    class Tokenizer(Postgres.Tokenizer):
        BIT_STRINGS = []
        BYTE_STRINGS = []
        HEX_STRINGS = []
        HEREDOC_STRINGS = []
        IDENTIFIERS = ['"']
        QUOTES = ["'"]
        STRING_ESCAPES = ["'"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BOOL": TokenType.BOOLEAN,
            "BYTEA": TokenType.VARBINARY,
            "DATETIME": TokenType.TIMESTAMP,
            "DEC": TokenType.DECIMAL,
            "FLOAT4": TokenType.FLOAT,
            "FLOAT8": TokenType.DOUBLE,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "INDEX": TokenType.INDEX,
            "INTERNED": TokenType.VAR,
            "INT2": TokenType.SMALLINT,
            "INT4": TokenType.INT,
            "INT8": TokenType.BIGINT,
            "INT64": TokenType.BIGINT,
            "LATENESS": TokenType.VAR,
            "LINEAR": TokenType.VAR,
            "MINUS": TokenType.EXCEPT,
            "NUMBER": TokenType.DECIMAL,
            "REMOVE": TokenType.VAR,
            "SIGNED": TokenType.INT,
            "STRING": TokenType.VARCHAR,
            "UNSIGNED": TokenType.UBIGINT,
            "WATERMARK": TokenType.VAR,
        }

    Parser = FelderaParser
    Generator = FelderaGenerator