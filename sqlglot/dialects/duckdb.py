from __future__ import annotations

import typing as t

from sqlglot import exp, tokens

from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.duckdb import DuckDBGenerator, WS_CONTROL_CHARS_TO_DUCK  # noqa: F401
from sqlglot.parsers.duckdb import DuckDBParser
from sqlglot.tokens import TokenType
from sqlglot.typing.duckdb import EXPRESSION_METADATA


class DuckDB(Dialect):
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = True
    SAFE_DIVISION = True
    INDEX_OFFSET = 1
    CONCAT_COALESCE = True
    SUPPORTS_ORDER_BY_ALL = True
    SUPPORTS_FIXED_SIZE_ARRAYS = True
    STRICT_JSON_PATH_SYNTAX = False
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True

    # https://duckdb.org/docs/sql/introduction.html#creating-a-new-table
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "DAYOFWEEKISO": "ISODOW",
    }

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    DATE_PART_MAPPING.pop("WEEKDAY")

    INVERSE_TIME_MAPPING = {
        "%e": "%-d",  # BigQuery's space-padded day (%e) -> DuckDB's no-padding day (%-d)
        "%:z": "%z",  # In DuckDB %z	can represent +/-HH:MM, +/-HHMM, or +/-HH.
        "%-z": "%z",
        "%f_zero": "%n",
        "%f_one": "%n",
        "%f_two": "%n",
        "%f_three": "%g",
        "%f_four": "%n",
        "%f_five": "%n",
        "%f_seven": "%n",
        "%f_eight": "%n",
        "%f_nine": "%n",
    }

    def to_json_path(self, path: t.Optional[exp.Expr]) -> t.Optional[exp.Expr]:
        if isinstance(path, exp.Literal):
            # DuckDB also supports the JSON pointer syntax, where every path starts with a `/`.
            # Additionally, it allows accessing the back of lists using the `[#-i]` syntax.
            # This check ensures we'll avoid trying to parse these as JSON paths, which can
            # either result in a noisy warning or in an invalid representation of the path.
            path_text = path.name
            if path_text.startswith("/") or "[#" in path_text:
                return path

        return super().to_json_path(path)

    class Tokenizer(tokens.Tokenizer):
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]
        BYTE_STRING_ESCAPES = ["'", "\\"]
        HEREDOC_STRINGS = ["$"]

        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "//": TokenType.DIV,
            "**": TokenType.DSTAR,
            "^@": TokenType.CARET_AT,
            "@>": TokenType.AT_GT,
            "<@": TokenType.LT_AT,
            "ATTACH": TokenType.ATTACH,
            "BINARY": TokenType.VARBINARY,
            "BITSTRING": TokenType.BIT,
            "BPCHAR": TokenType.TEXT,
            "CHAR": TokenType.TEXT,
            "DATETIME": TokenType.TIMESTAMPNTZ,
            "DETACH": TokenType.DETACH,
            "FORCE": TokenType.FORCE,
            "INSTALL": TokenType.INSTALL,
            "INT8": TokenType.BIGINT,
            "LOGICAL": TokenType.BOOLEAN,
            "MACRO": TokenType.FUNCTION,
            "ONLY": TokenType.ONLY,
            "PIVOT_WIDER": TokenType.PIVOT,
            "POSITIONAL": TokenType.POSITIONAL,
            "RESET": TokenType.COMMAND,
            "ROW": TokenType.STRUCT,
            "SIGNED": TokenType.INT,
            "STRING": TokenType.TEXT,
            "SUMMARIZE": TokenType.SUMMARIZE,
            "TIMESTAMP": TokenType.TIMESTAMPNTZ,
            "TIMESTAMP_S": TokenType.TIMESTAMP_S,
            "TIMESTAMP_MS": TokenType.TIMESTAMP_MS,
            "TIMESTAMP_NS": TokenType.TIMESTAMP_NS,
            "TIMESTAMP_US": TokenType.TIMESTAMP,
            "UBIGINT": TokenType.UBIGINT,
            "UINTEGER": TokenType.UINT,
            "USMALLINT": TokenType.USMALLINT,
            "UTINYINT": TokenType.UTINYINT,
            "VARCHAR": TokenType.TEXT,
        }
        KEYWORDS.pop("/*+")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    Parser = DuckDBParser

    Generator = DuckDBGenerator
