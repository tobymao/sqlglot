from __future__ import annotations

import typing as t

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.generators.db2 import Db2 as Db2Generator
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    pass


class Db2(Dialect):
    # DB2 is case-insensitive by default for unquoted identifiers
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    # DB2 supports NULL ordering
    NULL_ORDERING = "nulls_are_large"

    # DB2 specific settings
    TYPED_DIVISION = True
    SAFE_DIVISION = True

    class Tokenizer(tokens.Tokenizer):
        # DB2 uses @ for variables
        VAR_SINGLE_TOKENS = {"@"}

        # DB2 specific keywords
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CHAR": TokenType.CHAR,
            "CLOB": TokenType.TEXT,
            "DBCLOB": TokenType.TEXT,
            "DECFLOAT": TokenType.DECIMAL,
            "GRAPHIC": TokenType.NCHAR,
            "VARGRAPHIC": TokenType.NVARCHAR,
            "SMALLINT": TokenType.SMALLINT,
            "INTEGER": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "REAL": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "DECIMAL": TokenType.DECIMAL,
            "NUMERIC": TokenType.DECIMAL,
            "VARCHAR": TokenType.VARCHAR,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "TIMESTMP": TokenType.TIMESTAMP,
            "SYSIBM": TokenType.SCHEMA,
            "SYSFUN": TokenType.SCHEMA,
            "SYSTOOLS": TokenType.SCHEMA,
        }

    Generator = Db2Generator


# Made with Bob
