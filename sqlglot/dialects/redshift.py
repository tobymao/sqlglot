from __future__ import annotations

from sqlglot.typing.redshift import EXPRESSION_METADATA
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.redshift import RedshiftGenerator
from sqlglot.parsers.redshift import RedshiftParser
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    NORMALIZE_NOT_NULL = True

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()
    SUPPORTS_USER_DEFINED_TYPES = False
    INDEX_OFFSET = 0
    COPY_PARAMS_ARE_CSV = False
    HEX_LOWERCASE = True
    HAS_DISTINCT_ARRAY_CONSTRUCTORS = True
    COALESCE_COMPARISON_NON_STANDARD = True
    REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL = False
    ARRAY_FUNCS_PROPAGATES_NULLS = True

    # ref: https://docs.aws.amazon.com/redshift/latest/dg/r_FORMAT_strings.html
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"

    TIME_MAPPING = {
        **Postgres.TIME_MAPPING,
        "MON": "%b",
        "MONTH": "%B",
    }

    Parser = RedshiftParser

    UNESCAPED_SEQUENCES = {"\\a": "a", "\\v": "v"}

    class Tokenizer(Postgres.Tokenizer):
        NUMERIC_ESCAPES = {"0": (8, 1, 3, 0o777)}
        NUMERIC_ESCAPES_ARE_BYTES = True
        DROP_UNKNOWN_ESCAPES = True
        BIT_STRINGS = []
        HEX_STRINGS = []
        STRING_ESCAPES = ["\\", "'"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "BINARY VARYING": TokenType.VARBINARY,
            "CURRENT_USER_ID": TokenType.CURRENT_USER_ID,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "MINUS": TokenType.EXCEPT,
            "SUPER": TokenType.SUPER,
            "TOP": TokenType.TOP,
            "UNLOAD": TokenType.COMMAND,
            "USER": TokenType.CURRENT_USER,
            "VARBYTE": TokenType.VARBINARY,
        }
        KEYWORDS.pop("VALUES")

        # Redshift allows # to appear as a table identifier prefix
        SINGLE_TOKENS = Postgres.Tokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("#")

    Generator = RedshiftGenerator
