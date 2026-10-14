from __future__ import annotations

from copy import deepcopy
from collections import defaultdict

from sqlglot import exp, jsonpath, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.hive import HiveGenerator
from sqlglot.parsers.hive import HiveParser
from sqlglot.tokens import TokenType
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.typing.hive import EXPRESSION_METADATA


# Spark EXPLODE arity depends on arg type: ARRAY→(col,), MAP→(key,value); POS prepends pos.
def _explode_columns(func: exp.Expr, _dialect: Dialect) -> tuple[str, ...]:
    arg = func.this
    is_map = isinstance(arg, exp.VarMap) or (arg.type and arg.type.is_type(exp.DType.MAP))
    base: tuple[str, ...] = ("key", "value") if is_map else ("col",)
    return (("pos",) + base) if isinstance(func, exp.Posexplode) else base


class Hive(Dialect):
    ALIAS_POST_TABLESAMPLE = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    CONCAT_WS_COALESCE = True
    ARRAY_AGG_INCLUDES_NULLS = None
    REGEXP_EXTRACT_DEFAULT_GROUP = 1
    ALTER_TABLE_SUPPORTS_CASCADE = True

    # https://spark.apache.org/docs/latest/sql-ref-identifier.html#description
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    # INLINE/STACK column count depends on literal argument values; defer to the user.
    TVF_COLUMNS = {
        **Dialect.TVF_COLUMNS,
        exp.Explode: _explode_columns,
        exp._ExplodeOuter: _explode_columns,
        exp.Posexplode: _explode_columns,
        exp.PosexplodeOuter: _explode_columns,
        exp.Inline: Dialect.UNKNOWABLE_TVF_COLUMNS,
        exp.Stack: Dialect.UNKNOWABLE_TVF_COLUMNS,
    }

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362046#LanguageManualUDF-StringFunctions
    # https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/exec/Utilities.java#L266-L269
    INITCAP_DEFAULT_DELIMITER_CHARS = " \t\n\r\f\u000b\u001c\u001d\u001e\u001f"

    # Support only the non-ANSI mode (default for Hive, Spark2, Spark)
    COERCES_TO = defaultdict(set, deepcopy(TypeAnnotator.COERCES_TO))
    for target_type in {
        *exp.DataType.NUMERIC_TYPES,
        *exp.DataType.TEMPORAL_TYPES,
        exp.DType.INTERVAL,
    }:
        COERCES_TO[target_type] |= exp.DataType.TEXT_TYPES

    TIME_MAPPING = {
        "y": "%Y",
        "Y": "%Y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        # Hive 4.0+ parses MM/dd/HH/hh/mm/ss strictly (java.time.DateTimeFormatter, see
        # HIVE-25458/HIVE-25576)
        "MM": "%mstrict",
        "M": "%-m",
        "dd": "%dstrict",
        "d": "%-d",
        "HH": "%Hstrict",
        "H": "%-H",
        "hh": "%Istrict",
        "h": "%-I",
        "mm": "%Mstrict",
        "m": "%-M",
        "ss": "%Sstrict",
        "s": "%-S",
        "SSSSSS": "%f",
        "a": "%p",
        "DD": "%j",
        "D": "%-j",
        "E": "%a",
        "EE": "%a",
        "EEE": "%a",
        "EEEE": "%A",
        "z": "%Z",
        "Z": "%z",
    }

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        VAR_TOKENS = {
            *jsonpath.JSONPathTokenizer.VAR_TOKENS,
            TokenType.DASH,
        }

    UNESCAPED_SEQUENCES = {
        "\\0": "\0",
        "\\Z": "\x1a",
        "\\%": "\\%",
        "\\_": "\\_",
        "\\a": "a",
        "\\f": "f",
        "\\v": "v",
    }

    class Tokenizer(tokens.Tokenizer):
        NUMERIC_ESCAPES = {"u": (16, 4, 4, 0xFFFF), "0": (8, 3, 3, 0x7F)}
        DROP_UNKNOWN_ESCAPES = True
        LONE_SURROGATE_REPLACEMENT = "?"
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ADD ARCHIVE": TokenType.COMMAND,
            "ADD ARCHIVES": TokenType.COMMAND,
            "ADD FILE": TokenType.COMMAND,
            "ADD FILES": TokenType.COMMAND,
            "ADD JAR": TokenType.COMMAND,
            "ADD JARS": TokenType.COMMAND,
            "MINUS": TokenType.EXCEPT,
            "MSCK REPAIR": TokenType.COMMAND,
            "REFRESH": TokenType.REFRESH,
            "SERDEPROPERTIES": TokenType.SERDE_PROPERTIES,
        }

        NUMERIC_LITERALS = {
            "L": "BIGINT",
            "S": "SMALLINT",
            "Y": "TINYINT",
            "D": "DOUBLE",
            "F": "FLOAT",
            "BD": "DECIMAL",
        }

    Parser = HiveParser

    Generator = HiveGenerator
