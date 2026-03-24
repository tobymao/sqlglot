from __future__ import annotations

from sqlglot import exp, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType


class Postgres(Dialect):
    INDEX_OFFSET = 1
    TYPED_DIVISION = True
    CONCAT_COALESCE = True
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    TABLESAMPLE_SIZE_IS_PERCENT = True
    TABLES_REFERENCEABLE_AS_COLUMNS = True

    DEFAULT_FUNCTIONS_COLUMN_NAMES = {
        exp.ExplodingGenerateSeries: "generate_series",
    }

    TIME_MAPPING = {
        "d": "%u",  # 1-based day of week
        "D": "%u",  # 1-based day of week
        "dd": "%d",  # day of month
        "DD": "%d",  # day of month
        "ddd": "%j",  # zero padded day of year
        "DDD": "%j",  # zero padded day of year
        "FMDD": "%-d",  # - is no leading zero for Python; same for FM in postgres
        "FMDDD": "%-j",  # day of year
        "FMHH12": "%-I",  # 9
        "FMHH24": "%-H",  # 9
        "FMMI": "%-M",  # Minute
        "FMMM": "%-m",  # 1
        "FMSS": "%-S",  # Second
        "HH12": "%I",  # 09
        "HH24": "%H",  # 09
        "mi": "%M",  # zero padded minute
        "MI": "%M",  # zero padded minute
        "mm": "%m",  # 01
        "MM": "%m",  # 01
        "OF": "%z",  # utc offset
        "ss": "%S",  # zero padded second
        "SS": "%S",  # zero padded second
        "TMDay": "%A",  # TM is locale dependent
        "TMDy": "%a",
        "TMMon": "%b",  # Sep
        "TMMonth": "%B",  # September
        "TZ": "%Z",  # uppercase timezone name
        "US": "%f",  # zero padded microsecond
        "ww": "%U",  # 1-based week of year
        "WW": "%U",  # 1-based week of year
        "yy": "%y",  # 15
        "YY": "%y",  # 15
        "yyyy": "%Y",  # 2015
        "YYYY": "%Y",  # 2015
    }

    class Tokenizer(tokens.Tokenizer):
        BIT_STRINGS = [("b'", "'"), ("B'", "'")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]
        BYTE_STRING_ESCAPES = ["'", "\\"]
        HEREDOC_STRINGS = ["$"]

        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "~": TokenType.RLIKE,
            "@@": TokenType.DAT,
            "@>": TokenType.AT_GT,
            "<@": TokenType.LT_AT,
            "?&": TokenType.QMARK_AMP,
            "?|": TokenType.QMARK_PIPE,
            "#-": TokenType.HASH_DASH,
            "|/": TokenType.PIPE_SLASH,
            "||/": TokenType.DPIPE_SLASH,
            "BEGIN": TokenType.BEGIN,
            "BIGSERIAL": TokenType.BIGSERIAL,
            "CSTRING": TokenType.PSEUDO_TYPE,
            "DECLARE": TokenType.COMMAND,
            "DO": TokenType.COMMAND,
            "EXEC": TokenType.COMMAND,
            "HSTORE": TokenType.HSTORE,
            "INT8": TokenType.BIGINT,
            "MONEY": TokenType.MONEY,
            "NAME": TokenType.NAME,
            "OID": TokenType.OBJECT_IDENTIFIER,
            "ONLY": TokenType.ONLY,
            "POINT": TokenType.POINT,
            "REFRESH": TokenType.COMMAND,
            "REINDEX": TokenType.COMMAND,
            "RESET": TokenType.COMMAND,
            "SERIAL": TokenType.SERIAL,
            "SMALLSERIAL": TokenType.SMALLSERIAL,
            "TEMP": TokenType.TEMPORARY,
            "REGCLASS": TokenType.OBJECT_IDENTIFIER,
            "REGCOLLATION": TokenType.OBJECT_IDENTIFIER,
            "REGCONFIG": TokenType.OBJECT_IDENTIFIER,
            "REGDICTIONARY": TokenType.OBJECT_IDENTIFIER,
            "REGNAMESPACE": TokenType.OBJECT_IDENTIFIER,
            "REGOPER": TokenType.OBJECT_IDENTIFIER,
            "REGOPERATOR": TokenType.OBJECT_IDENTIFIER,
            "REGPROC": TokenType.OBJECT_IDENTIFIER,
            "REGPROCEDURE": TokenType.OBJECT_IDENTIFIER,
            "REGROLE": TokenType.OBJECT_IDENTIFIER,
            "REGTYPE": TokenType.OBJECT_IDENTIFIER,
            "FLOAT": TokenType.DOUBLE,
            "XML": TokenType.XML,
            "VARIADIC": TokenType.VARIADIC,
            "INOUT": TokenType.INOUT,
        }
        KEYWORDS.pop("/*+")
        KEYWORDS.pop("DIV")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.HEREDOC_STRING,
        }

        VAR_SINGLE_TOKENS = {"$"}

    Parser = PostgresParser

    Generator = PostgresGenerator
