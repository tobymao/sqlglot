from __future__ import annotations

import typing as t


from sqlglot.optimizer.annotate_types import TypeAnnotator

from sqlglot import exp, jsonpath, tokens
from sqlglot._typing import E
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.tokens import TokenType
from sqlglot.typing.bigquery import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


class BigQuery(Dialect):
    WEEK_OFFSET = -1
    UNNEST_COLUMN_ONLY = True
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = False
    HEX_LOWERCASE = True
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    EXPAND_ONLY_GROUP_ALIAS_REF = True
    PRESERVE_ORIGINAL_NAMES = True
    HEX_STRING_IS_INTEGER_TYPE = True
    BYTE_STRING_IS_BYTES_TYPE = True
    UUID_IS_STRING_TYPE = True
    ANNOTATE_ALL_SCOPES = True
    PROJECTION_ALIASES_SHADOW_SOURCE_NAMES = True
    TABLES_REFERENCEABLE_AS_COLUMNS = True
    SUPPORTS_STRUCT_STAR_EXPANSION = True
    EXCLUDES_PSEUDOCOLUMNS_FROM_STAR = True
    QUERY_RESULTS_ARE_STRUCTS = True
    JSON_EXTRACT_SCALAR_SCALAR_ONLY = True
    JSON_PATH_SINGLE_DOT_IS_WILDCARD = True
    LEAST_GREATEST_IGNORES_NULLS = False
    DEFAULT_NULL_TYPE = exp.DType.BIGINT
    PRIORITIZE_NON_LITERAL_TYPES = True
    ALIAS_POST_VERSION = False

    # https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
    INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v\\[\\](){}/|<>!?@"^#$&~_,.:;*%+\\-'

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    # bigquery udfs are case sensitive
    NORMALIZE_FUNCTIONS = False

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
    TIME_MAPPING = {
        "%x": "%m/%d/%y",
        "%D": "%m/%d/%y",
        "%E6S": "%S.%f",
        "%e": "%-d",
        "%F": "%Y-%m-%d",
        "%T": "%H:%M:%S",
        "%c": "%a %b %e %H:%M:%S %Y",
    }

    INVERSE_TIME_MAPPING = {
        # Preserve %E6S instead of expanding to %T.%f - since both %E6S & %T.%f are semantically different in BigQuery
        # %E6S is semantically different from %T.%f: %E6S works as a single atomic specifier for seconds with microseconds, while %T.%f expands incorrectly and fails to parse.
        "%H:%M:%S.%f": "%H:%M:%E6S",
    }

    FORMAT_MAPPING = {
        "DD": "%d",
        "MM": "%m",
        "MON": "%b",
        "MONTH": "%B",
        "YYYY": "%Y",
        "YY": "%y",
        "HH": "%I",
        "HH12": "%I",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
        "SSSSS": "%f",
        "TZH": "%z",
    }

    # The _PARTITIONTIME and _PARTITIONDATE pseudo-columns are not returned by a SELECT * statement
    # https://cloud.google.com/bigquery/docs/querying-partitioned-tables#query_an_ingestion-time_partitioned_table
    # https://cloud.google.com/bigquery/docs/querying-wildcard-tables#scanning_a_range_of_tables_using_table_suffix
    # https://cloud.google.com/bigquery/docs/query-cloud-storage-data#query_the_file_name_pseudo-column
    PSEUDOCOLUMNS = {
        "_PARTITIONTIME",
        "_PARTITIONDATE",
        "_TABLE_SUFFIX",
        "_FILE_NAME",
        "_DBT_MAX_PARTITION",
    }

    # All set operations require either a DISTINCT or ALL specifier
    SET_OP_DISTINCT_BY_DEFAULT = dict.fromkeys((exp.Except, exp.Intersect, exp.Union), None)

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
    COERCES_TO = {
        **TypeAnnotator.COERCES_TO,
        exp.DType.BIGDECIMAL: {exp.DType.DOUBLE},
    }
    COERCES_TO[exp.DType.DECIMAL] |= {exp.DType.BIGDECIMAL}
    COERCES_TO[exp.DType.BIGINT] |= {exp.DType.BIGDECIMAL}
    COERCES_TO[exp.DType.VARCHAR] |= {
        exp.DType.DATE,
        exp.DType.DATETIME,
        exp.DType.TIME,
        exp.DType.TIMESTAMP,
        exp.DType.TIMESTAMPTZ,
    }

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    def normalize_identifier(self, expression: E) -> E:
        if (
            isinstance(expression, exp.Identifier)
            and self.normalization_strategy is NormalizationStrategy.CASE_INSENSITIVE
        ):
            parent = expression.parent
            while isinstance(parent, exp.Dot):
                parent = parent.parent

            # In BigQuery, CTEs are case-insensitive, but UDF and table names are case-sensitive
            # by default. The following check uses a heuristic to detect tables based on whether
            # they are qualified. This should generally be correct, because tables in BigQuery
            # must be qualified with at least a dataset, unless @@dataset_id is set.
            case_sensitive = (
                isinstance(parent, exp.UserDefinedFunction)
                or (
                    isinstance(parent, exp.Table)
                    and parent.db
                    and (parent.meta.get("quoted_table") or not parent.meta.get("maybe_column"))
                )
                or expression.meta.get("is_table")
            )
            if not case_sensitive:
                expression.set("this", expression.this.lower())

            return t.cast(E, expression)

        return super().normalize_identifier(expression)

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        VAR_TOKENS = {
            *jsonpath.JSONPathTokenizer.VAR_TOKENS,
            TokenType.DASH,
            TokenType.NUMBER,
        }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"', '"""', "'''"]
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        HEX_STRINGS = [("0x", ""), ("0X", "")]

        BYTE_STRINGS = [(prefix + q, q) for q in t.cast(list[str], QUOTES) for prefix in ("b", "B")]

        RAW_STRINGS = [(prefix + q, q) for q in t.cast(list[str], QUOTES) for prefix in ("r", "R")]

        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ANY TYPE": TokenType.VARIANT,
            "BEGIN": TokenType.COMMAND,
            "BEGIN TRANSACTION": TokenType.BEGIN,
            "BYTEINT": TokenType.INT,
            "BYTES": TokenType.BINARY,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "DATETIME": TokenType.TIMESTAMP,
            "DECLARE": TokenType.DECLARE,
            "ELSEIF": TokenType.COMMAND,
            "EXCEPTION": TokenType.COMMAND,
            "EXPORT": TokenType.EXPORT,
            "FLOAT64": TokenType.DOUBLE,
            "FOR SYSTEM TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "LOOP": TokenType.COMMAND,
            "MODEL": TokenType.MODEL,
            "NOT DETERMINISTIC": TokenType.VOLATILE,
            "RECORD": TokenType.STRUCT,
            "REPEAT": TokenType.COMMAND,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
            "WHILE": TokenType.COMMAND,
        }
        KEYWORDS.pop("DIV")
        KEYWORDS.pop("VALUES")
        KEYWORDS.pop("/*+")

    Parser = BigQueryParser

    Generator = BigQueryGenerator
