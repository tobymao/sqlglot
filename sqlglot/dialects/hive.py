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


class Hive(Dialect):
    ALIAS_POST_TABLESAMPLE = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    CONCAT_WS_COALESCE = True
    ARRAY_AGG_INCLUDES_NULLS = None
    REGEXP_EXTRACT_DEFAULT_GROUP = 1
    ALTER_TABLE_SUPPORTS_CASCADE = True
    UUID_IS_STRING_TYPE = False

    # https://spark.apache.org/docs/latest/sql-ref-identifier.html#description
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

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
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
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

    class Tokenizer(tokens.Tokenizer):
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
            "TIMESTAMP AS OF": TokenType.TIMESTAMP_SNAPSHOT,
            "VERSION AS OF": TokenType.VERSION_SNAPSHOT,
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
