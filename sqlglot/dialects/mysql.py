from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.mysql import MySQLGenerator
from sqlglot.parsers.mysql import MySQLParser
from sqlglot.tokens import TokenType
from sqlglot.typing.mysql import EXPRESSION_METADATA


class MySQL(Dialect):
    PROMOTE_TO_INFERRED_DATETIME_TYPE = True

    # https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    IDENTIFIERS_CAN_START_WITH_DIGIT = True

    # We default to treating all identifiers as case-sensitive, since it matches MySQL's
    # behavior on Linux systems. For MacOS and Windows systems, one can override this
    # setting by specifying `dialect="mysql, normalization_strategy = lowercase"`.
    #
    # See also https://dev.mysql.com/doc/refman/8.2/en/identifier-case-sensitivity.html
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    TIME_FORMAT = "'%Y-%m-%d %T'"
    DPIPE_IS_STRING_CONCAT = False
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    SAFE_TO_ELIMINATE_DOUBLE_NEGATION = False
    LEAST_GREATEST_IGNORES_NULLS = False

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    TIME_MAPPING = {
        "%M": "%B",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%u": "%W",
        "%k": "%-H",
        "%l": "%-I",
        "%T": "%H:%M:%S",
        "%W": "%A",
    }

    VALID_INTERVAL_UNITS = {
        *Dialect.VALID_INTERVAL_UNITS,
        "SECOND_MICROSECOND",
        "MINUTE_MICROSECOND",
        "MINUTE_SECOND",
        "HOUR_MICROSECOND",
        "HOUR_SECOND",
        "HOUR_MINUTE",
        "DAY_MICROSECOND",
        "DAY_SECOND",
        "DAY_MINUTE",
        "DAY_HOUR",
        "YEAR_MONTH",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["'", '"', "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]
        # https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
        ESCAPE_FOLLOW_CHARS = ["0", "b", "n", "r", "t", "Z", "%", "_"]

        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BLOB": TokenType.BLOB,
            "CHARSET": TokenType.CHARACTER_SET,
            "DISTINCTROW": TokenType.DISTINCT,
            "EXPLAIN": TokenType.DESCRIBE,
            "FORCE": TokenType.FORCE,
            "IGNORE": TokenType.IGNORE,
            "KEY": TokenType.KEY,
            "LOCK TABLES": TokenType.COMMAND,
            "LONGBLOB": TokenType.LONGBLOB,
            "LONGTEXT": TokenType.LONGTEXT,
            "MEDIUMBLOB": TokenType.MEDIUMBLOB,
            "MEDIUMINT": TokenType.MEDIUMINT,
            "MEDIUMTEXT": TokenType.MEDIUMTEXT,
            "MEMBER OF": TokenType.MEMBER_OF,
            "MOD": TokenType.MOD,
            "SEPARATOR": TokenType.SEPARATOR,
            "SERIAL": TokenType.SERIAL,
            "SIGNED": TokenType.BIGINT,
            "SIGNED INTEGER": TokenType.BIGINT,
            "SOUNDS LIKE": TokenType.SOUNDS_LIKE,
            "START": TokenType.BEGIN,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
            "TINYBLOB": TokenType.TINYBLOB,
            "TINYTEXT": TokenType.TINYTEXT,
            "UNLOCK TABLES": TokenType.COMMAND,
            "UNSIGNED": TokenType.UBIGINT,
            "UNSIGNED INTEGER": TokenType.UBIGINT,
            "YEAR": TokenType.YEAR,
            "_ARMSCII8": TokenType.INTRODUCER,
            "_ASCII": TokenType.INTRODUCER,
            "_BIG5": TokenType.INTRODUCER,
            "_BINARY": TokenType.INTRODUCER,
            "_CP1250": TokenType.INTRODUCER,
            "_CP1251": TokenType.INTRODUCER,
            "_CP1256": TokenType.INTRODUCER,
            "_CP1257": TokenType.INTRODUCER,
            "_CP850": TokenType.INTRODUCER,
            "_CP852": TokenType.INTRODUCER,
            "_CP866": TokenType.INTRODUCER,
            "_CP932": TokenType.INTRODUCER,
            "_DEC8": TokenType.INTRODUCER,
            "_EUCJPMS": TokenType.INTRODUCER,
            "_EUCKR": TokenType.INTRODUCER,
            "_GB18030": TokenType.INTRODUCER,
            "_GB2312": TokenType.INTRODUCER,
            "_GBK": TokenType.INTRODUCER,
            "_GEOSTD8": TokenType.INTRODUCER,
            "_GREEK": TokenType.INTRODUCER,
            "_HEBREW": TokenType.INTRODUCER,
            "_HP8": TokenType.INTRODUCER,
            "_KEYBCS2": TokenType.INTRODUCER,
            "_KOI8R": TokenType.INTRODUCER,
            "_KOI8U": TokenType.INTRODUCER,
            "_LATIN1": TokenType.INTRODUCER,
            "_LATIN2": TokenType.INTRODUCER,
            "_LATIN5": TokenType.INTRODUCER,
            "_LATIN7": TokenType.INTRODUCER,
            "_MACCE": TokenType.INTRODUCER,
            "_MACROMAN": TokenType.INTRODUCER,
            "_SJIS": TokenType.INTRODUCER,
            "_SWE7": TokenType.INTRODUCER,
            "_TIS620": TokenType.INTRODUCER,
            "_UCS2": TokenType.INTRODUCER,
            "_UJIS": TokenType.INTRODUCER,
            # https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
            "_UTF8": TokenType.INTRODUCER,
            "_UTF16": TokenType.INTRODUCER,
            "_UTF16LE": TokenType.INTRODUCER,
            "_UTF32": TokenType.INTRODUCER,
            "_UTF8MB3": TokenType.INTRODUCER,
            "_UTF8MB4": TokenType.INTRODUCER,
            "@@": TokenType.SESSION_PARAMETER,
        }

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.REPLACE} - {TokenType.SHOW}

    Parser = MySQLParser

    Generator = MySQLGenerator
