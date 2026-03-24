from __future__ import annotations

from sqlglot import exp, jsonpath, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.snowflake import SnowflakeGenerator
from sqlglot.parsers.snowflake import (
    SnowflakeParser,
)
from sqlglot.tokens import TokenType
from sqlglot.typing.snowflake import EXPRESSION_METADATA


class Snowflake(Dialect):
    # https://docs.snowflake.com/en/sql-reference/identifiers-syntax
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    SUPPORTS_USER_DEFINED_TYPES = False
    PREFER_CTE_ALIAS_COLUMN = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    COPY_PARAMS_ARE_CSV = False
    ARRAY_AGG_INCLUDES_NULLS = None
    ARRAY_FUNCS_PROPAGATES_NULLS = True
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
    TRY_CAST_REQUIRES_STRING = True
    SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = True
    LEAST_GREATEST_IGNORES_NULLS = False

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://docs.snowflake.com/en/en/sql-reference/functions/initcap
    INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v!?@"^#$&~_,.:;+\\-*%/|\\[\\](){}<>'

    INVERSE_TIME_MAPPING = {
        "T": "T",  # in TIME_MAPPING we map '"T"' with the double quotes to 'T', and we want to prevent 'T' from being mapped back to '"T"' so that 'AUTO' doesn't become 'AU"T"O'
    }

    TIME_MAPPING = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "mmmm": "%B",
        "MON": "%b",
        "mon": "%b",
        "MM": "%m",
        "mm": "%m",
        "DD": "%d",
        "dd": "%-d",
        "DY": "%a",
        "dy": "%w",
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FF": "%f_nine",  # %f_ internal representation with precision specified
        "ff": "%f_nine",
        "FF0": "%f_zero",
        "ff0": "%f_zero",
        "FF1": "%f_one",
        "ff1": "%f_one",
        "FF2": "%f_two",
        "ff2": "%f_two",
        "FF3": "%f_three",
        "ff3": "%f_three",
        "FF4": "%f_four",
        "ff4": "%f_four",
        "FF5": "%f_five",
        "ff5": "%f_five",
        "FF6": "%f",
        "ff6": "%f",
        "FF7": "%f_seven",
        "ff7": "%f_seven",
        "FF8": "%f_eight",
        "ff8": "%f_eight",
        "FF9": "%f_nine",
        "ff9": "%f_nine",
        "TZHTZM": "%z",
        "tzhtzm": "%z",
        "TZH:TZM": "%:z",  # internal representation for ±HH:MM
        "tzh:tzm": "%:z",
        "TZH": "%-z",  # internal representation ±HH
        "tzh": "%-z",
        '"T"': "T",  # remove the optional double quotes around the separator between the date and time
        # Seems like Snowflake treats AM/PM in the format string as equivalent,
        # only the time (stamp) value's AM/PM affects the output
        "AM": "%p",
        "am": "%p",
        "PM": "%p",
        "pm": "%p",
    }

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "ISOWEEK": "WEEKISO",
        # The base Dialect maps EPOCH_SECOND -> EPOCH, but we need to preserve
        # EPOCH_SECOND as a distinct value for two reasons:
        # 1. Type annotation: EPOCH_SECOND returns BIGINT, while EPOCH returns DOUBLE
        # 2. Transpilation: DuckDB's EPOCH() returns float, so we cast EPOCH_SECOND
        #    to BIGINT to match Snowflake's integer behavior
        # Without this override, EXTRACT(EPOCH_SECOND FROM ts) would be normalized
        # to EXTRACT(EPOCH FROM ts) and lose the integer semantics.
        "EPOCH_SECOND": "EPOCH_SECOND",
        "EPOCH_SECONDS": "EPOCH_SECOND",
    }

    PSEUDOCOLUMNS = {"LEVEL"}

    def can_quote(self, identifier: exp.Identifier, identify: str | bool = "safe") -> bool:
        # This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
        # unquoted DUAL keyword in a special way and does not map it to a user-defined table
        return super().can_quote(identifier, identify) and not (
            isinstance(identifier.parent, exp.Table)
            and not identifier.quoted
            and identifier.name.lower() == "dual"
        )

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        SINGLE_TOKENS = jsonpath.JSONPathTokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("$")

    Parser = SnowflakeParser

    class Tokenizer(tokens.Tokenizer):
        STRING_ESCAPES = ["\\", "'"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        RAW_STRINGS = ["$$"]
        COMMENTS = ["--", "//", ("/*", "*/")]
        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BYTEINT": TokenType.INT,
            "FILE://": TokenType.URI_START,
            "FILE FORMAT": TokenType.FILE_FORMAT,
            "GET": TokenType.GET,
            "INTEGRATION": TokenType.INTEGRATION,
            "MATCH_CONDITION": TokenType.MATCH_CONDITION,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NCHAR VARYING": TokenType.VARCHAR,
            "PACKAGE": TokenType.PACKAGE,
            "POLICY": TokenType.POLICY,
            "POOL": TokenType.POOL,
            "PUT": TokenType.PUT,
            "REMOVE": TokenType.COMMAND,
            "RM": TokenType.COMMAND,
            "ROLE": TokenType.ROLE,
            "RULE": TokenType.RULE,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "SEMANTIC VIEW": TokenType.SEMANTIC_VIEW,
            "SQL_DOUBLE": TokenType.DOUBLE,
            "SQL_VARCHAR": TokenType.VARCHAR,
            "STAGE": TokenType.STAGE,
            "STORAGE INTEGRATION": TokenType.STORAGE_INTEGRATION,
            "STREAMLIT": TokenType.STREAMLIT,
            "TAG": TokenType.TAG,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TOP": TokenType.TOP,
            "VOLUME": TokenType.VOLUME,
            "WAREHOUSE": TokenType.WAREHOUSE,
            # https://docs.snowflake.com/en/sql-reference/data-types-numeric#float
            # FLOAT is a synonym for DOUBLE in Snowflake
            "FLOAT": TokenType.DOUBLE,
        }
        KEYWORDS.pop("/*+")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
            "!": TokenType.EXCLAMATION,
        }

        VAR_SINGLE_TOKENS = {"$"}

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    Generator = SnowflakeGenerator
