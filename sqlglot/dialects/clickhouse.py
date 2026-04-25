from __future__ import annotations


from sqlglot import exp, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.clickhouse import ClickHouseGenerator
from sqlglot.parsers.clickhouse import ClickHouseParser
from sqlglot.tokens import TokenType
from sqlglot.typing.clickhouse import EXPRESSION_METADATA


class ClickHouse(Dialect):
    INDEX_OFFSET = 1
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    LOG_BASE_FIRST: bool | None = None
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    PRESERVE_ORIGINAL_NAMES = True
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    HEX_STRING_IS_INTEGER_TYPE = True

    # https://github.com/ClickHouse/ClickHouse/issues/33935#issue-1112165779
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    UNESCAPED_SEQUENCES = {
        "\\0": "\0",
    }

    CREATABLE_KIND_MAPPING = {"DATABASE": "SCHEMA"}

    SET_OP_DISTINCT_BY_DEFAULT: dict[type[exp.Expr], bool | None] = {
        exp.Except: False,
        exp.Intersect: False,
        exp.Union: None,
    }

    def generate_values_aliases(self, expression: exp.Values) -> list[exp.Identifier]:
        # Clickhouse allows VALUES to have an embedded structure e.g:
        # VALUES('person String, place String', ('Noah', 'Paris'), ...)
        # In this case, we don't want to qualify the columns
        values = expression.expressions[0].expressions

        structure = (
            values[0]
            if (len(values) > 1 and values[0].is_string and isinstance(values[1], exp.Tuple))
            else None
        )
        if structure:
            # Split each column definition into the column name e.g:
            # 'person String, place String' -> ['person', 'place']
            structure_coldefs = [coldef.strip() for coldef in structure.name.split(",")]
            column_aliases = [
                exp.to_identifier(coldef.split(" ")[0]) for coldef in structure_coldefs
            ]
        else:
            # Default column aliases in CH are "c1", "c2", etc.
            column_aliases = [
                exp.to_identifier(f"c{i + 1}") for i in range(len(values[0].expressions))
            ]

        return column_aliases

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]
        IDENTIFIER_ESCAPES = ["\\"]
        STRING_ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("0b", "")]
        HEX_STRINGS = [("0x", ""), ("0X", "")]
        HEREDOC_STRINGS = ["$"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            ".:": TokenType.DOTCOLON,
            ".^": TokenType.DOTCARET,
            "ATTACH": TokenType.COMMAND,
            "DATE32": TokenType.DATE32,
            "DETACH": TokenType.DETACH,
            "DATETIME64": TokenType.DATETIME64,
            "DICTIONARY": TokenType.DICTIONARY,
            "DYNAMIC": TokenType.DYNAMIC,
            "ENUM8": TokenType.ENUM8,
            "ENUM16": TokenType.ENUM16,
            "EXCHANGE": TokenType.COMMAND,
            "EXPLAIN": TokenType.DESCRIBE,
            "FINAL": TokenType.FINAL,
            "FIXEDSTRING": TokenType.FIXEDSTRING,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "GLOBAL": TokenType.GLOBAL,
            "LOWCARDINALITY": TokenType.LOWCARDINALITY,
            "MAP": TokenType.MAP,
            "NESTED": TokenType.NESTED,
            "NOTHING": TokenType.NOTHING,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "TUPLE": TokenType.STRUCT,
            "UINT16": TokenType.USMALLINT,
            "UINT32": TokenType.UINT,
            "UINT64": TokenType.UBIGINT,
            "UINT8": TokenType.UTINYINT,
            "IPV4": TokenType.IPV4,
            "IPV6": TokenType.IPV6,
            "POINT": TokenType.POINT,
            "RING": TokenType.RING,
            "LINESTRING": TokenType.LINESTRING,
            "MULTILINESTRING": TokenType.MULTILINESTRING,
            "POLYGON": TokenType.POLYGON,
            "MULTIPOLYGON": TokenType.MULTIPOLYGON,
            "AGGREGATEFUNCTION": TokenType.AGGREGATEFUNCTION,
            "SIMPLEAGGREGATEFUNCTION": TokenType.SIMPLEAGGREGATEFUNCTION,
            "SYSTEM": TokenType.COMMAND,
            "PREWHERE": TokenType.PREWHERE,
        }

        KEYWORDS.pop("/*+")

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.HEREDOC_STRING,
        }

    Parser = ClickHouseParser

    Generator = ClickHouseGenerator
