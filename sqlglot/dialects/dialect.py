from __future__ import annotations

import importlib
import logging
import typing as t
import sys

from enum import Enum, auto
from functools import reduce

from sqlglot import exp
from sqlglot.dialects import DIALECT_MODULE_NAMES
from sqlglot.errors import ParseError
from sqlglot.generator import Generator, unsupported_args
from sqlglot.helper import (
    AutoName,
    flatten,
    is_int,
    seq_get,
    suggest_closest_match_and_fail,
    to_bool,
)
from sqlglot.jsonpath import JSONPathTokenizer, parse as parse_json_path
from sqlglot.parser import Parser
from sqlglot.time import TIMEZONES, format_time, subsecond_precision
from sqlglot.tokens import Token, Tokenizer, TokenType
from sqlglot.trie import new_trie
from sqlglot.typing import EXPRESSION_METADATA

from importlib.metadata import entry_points

DATE_ADD_OR_DIFF = t.Union[
    exp.DateAdd,
    exp.DateDiff,
    exp.DateSub,
    exp.TsOrDsAdd,
    exp.TsOrDsDiff,
]
DATE_ADD_OR_SUB = t.Union[exp.DateAdd, exp.TsOrDsAdd, exp.DateSub]
JSON_EXTRACT_TYPE = t.Union[
    exp.JSONExtract, exp.JSONExtractScalar, exp.JSONBExtract, exp.JSONBExtractScalar
]
DATETIME_DELTA = t.Union[
    exp.DateAdd,
    exp.DatetimeAdd,
    exp.DatetimeSub,
    exp.TimeAdd,
    exp.TimeSub,
    exp.TimestampAdd,
    exp.TimestampSub,
    exp.TsOrDsAdd,
]
DATETIME_ADD = (exp.DateAdd, exp.TimeAdd, exp.DatetimeAdd, exp.TsOrDsAdd, exp.TimestampAdd)

if t.TYPE_CHECKING:
    from sqlglot._typing import B, E, F

logger = logging.getLogger("sqlglot")

UNESCAPED_SEQUENCES = {
    "\\a": "\a",
    "\\b": "\b",
    "\\f": "\f",
    "\\n": "\n",
    "\\r": "\r",
    "\\t": "\t",
    "\\v": "\v",
    "\\\\": "\\",
}

PLUGIN_GROUP_NAME = "sqlglot.dialects"


class Dialects(str, Enum):
    """Dialects supported by SQLGLot."""

    DIALECT = ""

    ATHENA = "athena"
    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DATABRICKS = "databricks"
    DORIS = "doris"
    DREMIO = "dremio"
    DRILL = "drill"
    DRUID = "druid"
    DUCKDB = "duckdb"
    DUNE = "dune"
    FABRIC = "fabric"
    HIVE = "hive"
    MATERIALIZE = "materialize"
    MYSQL = "mysql"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    PRESTO = "presto"
    PRQL = "prql"
    REDSHIFT = "redshift"
    RISINGWAVE = "risingwave"
    SNOWFLAKE = "snowflake"
    SOLR = "solr"
    SPARK = "spark"
    SPARK2 = "spark2"
    SQLITE = "sqlite"
    STARROCKS = "starrocks"
    TABLEAU = "tableau"
    TERADATA = "teradata"
    TRINO = "trino"
    TSQL = "tsql"
    EXASOL = "exasol"


class NormalizationStrategy(str, AutoName):
    """Specifies the strategy according to which identifiers should be normalized."""

    LOWERCASE = auto()
    """Unquoted identifiers are lowercased."""

    UPPERCASE = auto()
    """Unquoted identifiers are uppercased."""

    CASE_SENSITIVE = auto()
    """Always case-sensitive, regardless of quotes."""

    CASE_INSENSITIVE = auto()
    """Always case-insensitive (lowercase), regardless of quotes."""

    CASE_INSENSITIVE_UPPERCASE = auto()
    """Always case-insensitive (uppercase), regardless of quotes."""


class _Dialect(type):
    _classes: t.Dict[str, t.Type[Dialect]] = {}

    def __eq__(cls, other: t.Any) -> bool:
        if cls is other:
            return True
        if isinstance(other, str):
            return cls is cls.get(other)
        if isinstance(other, Dialect):
            return cls is type(other)

        return False

    def __hash__(cls) -> int:
        return hash(cls.__name__.lower())

    @property
    def classes(cls):
        if len(DIALECT_MODULE_NAMES) != len(cls._classes):
            for key in DIALECT_MODULE_NAMES:
                cls._try_load(key)

        return cls._classes

    @classmethod
    def _try_load(cls, key: str | Dialects) -> None:
        if isinstance(key, Dialects):
            key = key.value

        # 1. Try standard sqlglot modules first
        if key in DIALECT_MODULE_NAMES:
            module = importlib.import_module(f"sqlglot.dialects.{key}")
            # If module was already imported, the class may not be in _classes
            # Find and register the dialect class from the module
            if key not in cls._classes:
                for attr_name in dir(module):
                    attr = getattr(module, attr_name, None)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, Dialect)
                        and attr.__name__.lower() == key
                    ):
                        cls._classes[key] = attr
                        break
            return

        # 2. Try entry points (for plugins)
        try:
            all_eps = entry_points()
            # Python 3.10+ has select() method, older versions use dict-like access
            if hasattr(all_eps, "select"):
                eps = all_eps.select(group=PLUGIN_GROUP_NAME, name=key)
            else:
                # For older Python versions, entry_points() returns a dict-like object
                group_eps = all_eps.get(PLUGIN_GROUP_NAME, [])  # type: ignore
                eps = [ep for ep in group_eps if ep.name == key]  # type: ignore

            for entry_point in eps:
                dialect_class = entry_point.load()
                # Verify it's a Dialect subclass
                # issubclass() returns False if not a subclass, TypeError only if not a class at all
                if isinstance(dialect_class, type) and issubclass(dialect_class, Dialect):
                    # Register the dialect using the entry point name (key)
                    # The metaclass may have registered it by class name, but we need it by entry point name
                    if key not in cls._classes:
                        cls._classes[key] = dialect_class
                    return
        except ImportError:
            # entry_point.load() failed (bad plugin - module/class doesn't exist)
            pass

        # 3. Try direct import (for backward compatibility)
        # This allows namespace packages or explicit imports to work
        try:
            importlib.import_module(f"sqlglot.dialects.{key}")
        except ImportError:
            pass

    @classmethod
    def __getitem__(cls, key: str) -> t.Type[Dialect]:
        if key not in cls._classes:
            cls._try_load(key)

        return cls._classes[key]

    @classmethod
    def get(
        cls, key: str, default: t.Optional[t.Type[Dialect]] = None
    ) -> t.Optional[t.Type[Dialect]]:
        if key not in cls._classes:
            cls._try_load(key)

        return cls._classes.get(key, default)

    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)
        enum = Dialects.__members__.get(clsname.upper())
        cls._classes[enum.value if enum is not None else clsname.lower()] = klass

        klass.TIME_TRIE = new_trie(klass.TIME_MAPPING)
        klass.FORMAT_TRIE = (
            new_trie(klass.FORMAT_MAPPING) if klass.FORMAT_MAPPING else klass.TIME_TRIE
        )
        # Merge class-defined INVERSE_TIME_MAPPING with auto-generated mappings
        # This allows dialects to define custom inverse mappings for roundtrip correctness
        klass.INVERSE_TIME_MAPPING = {v: k for k, v in klass.TIME_MAPPING.items()} | (
            klass.__dict__.get("INVERSE_TIME_MAPPING") or {}
        )
        klass.INVERSE_TIME_TRIE = new_trie(klass.INVERSE_TIME_MAPPING)
        klass.INVERSE_FORMAT_MAPPING = {v: k for k, v in klass.FORMAT_MAPPING.items()}
        klass.INVERSE_FORMAT_TRIE = new_trie(klass.INVERSE_FORMAT_MAPPING)

        klass.INVERSE_CREATABLE_KIND_MAPPING = {
            v: k for k, v in klass.CREATABLE_KIND_MAPPING.items()
        }

        base = seq_get(bases, 0)
        base_tokenizer = (getattr(base, "tokenizer_class", Tokenizer),)
        base_jsonpath_tokenizer = (getattr(base, "jsonpath_tokenizer_class", JSONPathTokenizer),)
        base_parser = (getattr(base, "parser_class", Parser),)
        base_generator = (getattr(base, "generator_class", Generator),)

        klass.tokenizer_class = klass.__dict__.get(
            "Tokenizer", type("Tokenizer", base_tokenizer, {})
        )
        klass.jsonpath_tokenizer_class = klass.__dict__.get(
            "JSONPathTokenizer", type("JSONPathTokenizer", base_jsonpath_tokenizer, {})
        )
        klass.parser_class = klass.__dict__.get("Parser", type("Parser", base_parser, {}))
        klass.generator_class = klass.__dict__.get(
            "Generator", type("Generator", base_generator, {})
        )

        klass.QUOTE_START, klass.QUOTE_END = list(klass.tokenizer_class._QUOTES.items())[0]
        klass.IDENTIFIER_START, klass.IDENTIFIER_END = list(
            klass.tokenizer_class._IDENTIFIERS.items()
        )[0]

        def get_start_end(token_type: TokenType) -> t.Tuple[t.Optional[str], t.Optional[str]]:
            return next(
                (
                    (s, e)
                    for s, (e, t) in klass.tokenizer_class._FORMAT_STRINGS.items()
                    if t == token_type
                ),
                (None, None),
            )

        klass.BIT_START, klass.BIT_END = get_start_end(TokenType.BIT_STRING)
        klass.HEX_START, klass.HEX_END = get_start_end(TokenType.HEX_STRING)
        klass.BYTE_START, klass.BYTE_END = get_start_end(TokenType.BYTE_STRING)
        klass.UNICODE_START, klass.UNICODE_END = get_start_end(TokenType.UNICODE_STRING)

        if "\\" in klass.tokenizer_class.STRING_ESCAPES:
            klass.UNESCAPED_SEQUENCES = {
                **UNESCAPED_SEQUENCES,
                **klass.UNESCAPED_SEQUENCES,
            }

        klass.ESCAPED_SEQUENCES = {v: k for k, v in klass.UNESCAPED_SEQUENCES.items()}

        klass.SUPPORTS_COLUMN_JOIN_MARKS = "(+)" in klass.tokenizer_class.KEYWORDS

        if enum not in ("", "bigquery", "snowflake"):
            klass.INITCAP_SUPPORTS_CUSTOM_DELIMITERS = False

        if enum not in ("", "bigquery"):
            klass.generator_class.SELECT_KINDS = ()

        if enum not in ("", "athena", "presto", "trino", "duckdb"):
            klass.generator_class.TRY_SUPPORTED = False
            klass.generator_class.SUPPORTS_UESCAPE = False

        if enum not in ("", "databricks", "hive", "spark", "spark2"):
            modifier_transforms = klass.generator_class.AFTER_HAVING_MODIFIER_TRANSFORMS.copy()
            for modifier in ("cluster", "distribute", "sort"):
                modifier_transforms.pop(modifier, None)

            klass.generator_class.AFTER_HAVING_MODIFIER_TRANSFORMS = modifier_transforms

        if enum not in ("", "doris", "mysql"):
            klass.parser_class.ID_VAR_TOKENS = klass.parser_class.ID_VAR_TOKENS | {
                TokenType.STRAIGHT_JOIN,
            }
            klass.parser_class.TABLE_ALIAS_TOKENS = klass.parser_class.TABLE_ALIAS_TOKENS | {
                TokenType.STRAIGHT_JOIN,
            }

        if enum not in ("", "databricks", "oracle", "redshift", "snowflake", "spark"):
            klass.generator_class.SUPPORTS_DECODE_CASE = False

        if not klass.SUPPORTS_SEMI_ANTI_JOIN:
            klass.parser_class.TABLE_ALIAS_TOKENS = klass.parser_class.TABLE_ALIAS_TOKENS | {
                TokenType.ANTI,
                TokenType.SEMI,
            }

        if enum not in (
            "",
            "postgres",
            "duckdb",
            "redshift",
            "snowflake",
            "presto",
            "trino",
            "mysql",
            "singlestore",
        ):
            no_paren_functions = klass.parser_class.NO_PAREN_FUNCTIONS.copy()
            no_paren_functions.pop(TokenType.LOCALTIME, None)
            if enum != "oracle":
                no_paren_functions.pop(TokenType.LOCALTIMESTAMP, None)
            klass.parser_class.NO_PAREN_FUNCTIONS = no_paren_functions

        if enum in (
            "",
            "postgres",
            "duckdb",
            "trino",
        ):
            no_paren_functions = klass.parser_class.NO_PAREN_FUNCTIONS.copy()
            no_paren_functions[TokenType.CURRENT_CATALOG] = exp.CurrentCatalog
            klass.parser_class.NO_PAREN_FUNCTIONS = no_paren_functions
        else:
            # For dialects that don't support this keyword, treat it as a regular identifier
            # This fixes the "Unexpected token" error in BQ, Spark, etc.
            klass.parser_class.ID_VAR_TOKENS = klass.parser_class.ID_VAR_TOKENS | {
                TokenType.CURRENT_CATALOG,
            }

        if enum in (
            "",
            "duckdb",
            "spark",
            "postgres",
            "tsql",
        ):
            no_paren_functions = klass.parser_class.NO_PAREN_FUNCTIONS.copy()
            no_paren_functions[TokenType.SESSION_USER] = exp.SessionUser
            klass.parser_class.NO_PAREN_FUNCTIONS = no_paren_functions
        else:
            klass.parser_class.ID_VAR_TOKENS = klass.parser_class.ID_VAR_TOKENS | {
                TokenType.SESSION_USER,
            }

        klass.VALID_INTERVAL_UNITS = {
            *klass.VALID_INTERVAL_UNITS,
            *klass.DATE_PART_MAPPING.keys(),
            *klass.DATE_PART_MAPPING.values(),
        }

        return klass


class Dialect(metaclass=_Dialect):
    INDEX_OFFSET = 0
    """The base index offset for arrays."""

    WEEK_OFFSET = 0
    """First day of the week in DATE_TRUNC(week). Defaults to 0 (Monday). -1 would be Sunday."""

    UNNEST_COLUMN_ONLY = False
    """Whether `UNNEST` table aliases are treated as column aliases."""

    ALIAS_POST_TABLESAMPLE = False
    """Whether the table alias comes after tablesample."""

    TABLESAMPLE_SIZE_IS_PERCENT = False
    """Whether a size in the table sample clause represents percentage."""

    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE
    """Specifies the strategy according to which identifiers should be normalized."""

    IDENTIFIERS_CAN_START_WITH_DIGIT = False
    """Whether an unquoted identifier can start with a digit."""

    DPIPE_IS_STRING_CONCAT = True
    """Whether the DPIPE token (`||`) is a string concatenation operator."""

    STRICT_STRING_CONCAT = False
    """Whether `CONCAT`'s arguments must be strings."""

    SUPPORTS_USER_DEFINED_TYPES = True
    """Whether user-defined data types are supported."""

    SUPPORTS_SEMI_ANTI_JOIN = True
    """Whether `SEMI` or `ANTI` joins are supported."""

    SUPPORTS_COLUMN_JOIN_MARKS = False
    """Whether the old-style outer join (+) syntax is supported."""

    COPY_PARAMS_ARE_CSV = True
    """Separator of COPY statement parameters."""

    NORMALIZE_FUNCTIONS: bool | str = "upper"
    """
    Determines how function names are going to be normalized.
    Possible values:
        "upper" or True: Convert names to uppercase.
        "lower": Convert names to lowercase.
        False: Disables function name normalization.
    """

    PRESERVE_ORIGINAL_NAMES: bool = False
    """
    Whether the name of the function should be preserved inside the node's metadata,
    can be useful for roundtripping deprecated vs new functions that share an AST node
    e.g JSON_VALUE vs JSON_EXTRACT_SCALAR in BigQuery
    """

    LOG_BASE_FIRST: t.Optional[bool] = True
    """
    Whether the base comes first in the `LOG` function.
    Possible values: `True`, `False`, `None` (two arguments are not supported by `LOG`)
    """

    NULL_ORDERING = "nulls_are_small"
    """
    Default `NULL` ordering method to use if not explicitly set.
    Possible values: `"nulls_are_small"`, `"nulls_are_large"`, `"nulls_are_last"`
    """

    TYPED_DIVISION = False
    """
    Whether the behavior of `a / b` depends on the types of `a` and `b`.
    False means `a / b` is always float division.
    True means `a / b` is integer division if both `a` and `b` are integers.
    """

    SAFE_DIVISION = False
    """Whether division by zero throws an error (`False`) or returns NULL (`True`)."""

    CONCAT_COALESCE = False
    """A `NULL` arg in `CONCAT` yields `NULL` by default, but in some dialects it yields an empty string."""

    HEX_LOWERCASE = False
    """Whether the `HEX` function returns a lowercase hexadecimal string."""

    DATE_FORMAT = "'%Y-%m-%d'"
    DATEINT_FORMAT = "'%Y%m%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

    TIME_MAPPING: t.Dict[str, str] = {}
    """Associates this dialect's time formats with their equivalent Python `strftime` formats."""

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_model_rules_date_time
    # https://docs.teradata.com/r/Teradata-Database-SQL-Functions-Operators-Expressions-and-Predicates/March-2017/Data-Type-Conversions/Character-to-DATE-Conversion/Forcing-a-FORMAT-on-CAST-for-Converting-Character-to-DATE
    FORMAT_MAPPING: t.Dict[str, str] = {}
    """
    Helper which is used for parsing the special syntax `CAST(x AS DATE FORMAT 'yyyy')`.
    If empty, the corresponding trie will be constructed off of `TIME_MAPPING`.
    """

    UNESCAPED_SEQUENCES: t.Dict[str, str] = {}
    """Mapping of an escaped sequence (`\\n`) to its unescaped version (`\n`)."""

    PSEUDOCOLUMNS: t.Set[str] = set()
    """
    Columns that are auto-generated by the engine corresponding to this dialect.
    For example, such columns may be excluded from `SELECT *` queries.
    """

    PREFER_CTE_ALIAS_COLUMN = False
    """
    Some dialects, such as Snowflake, allow you to reference a CTE column alias in the
    HAVING clause of the CTE. This flag will cause the CTE alias columns to override
    any projection aliases in the subquery.

    For example,
        WITH y(c) AS (
            SELECT SUM(a) FROM (SELECT 1 a) AS x HAVING c > 0
        ) SELECT c FROM y;

        will be rewritten as

        WITH y(c) AS (
            SELECT SUM(a) AS c FROM (SELECT 1 AS a) AS x HAVING c > 0
        ) SELECT c FROM y;
    """

    COPY_PARAMS_ARE_CSV = True
    """
    Whether COPY statement parameters are separated by comma or whitespace
    """

    FORCE_EARLY_ALIAS_REF_EXPANSION = False
    """
    Whether alias reference expansion (_expand_alias_refs()) should run before column qualification (_qualify_columns()).

    For example:
        WITH data AS (
        SELECT
            1 AS id,
            2 AS my_id
        )
        SELECT
            id AS my_id
        FROM
            data
        WHERE
            my_id = 1
        GROUP BY
            my_id,
        HAVING
            my_id = 1

    In most dialects, "my_id" would refer to "data.my_id" across the query, except:
        - BigQuery, which will forward the alias to GROUP BY + HAVING clauses i.e
          it resolves to "WHERE my_id = 1 GROUP BY id HAVING id = 1"
        - Clickhouse, which will forward the alias across the query i.e it resolves
        to "WHERE id = 1 GROUP BY id HAVING id = 1"
    """

    EXPAND_ONLY_GROUP_ALIAS_REF = False
    """Whether alias reference expansion before qualification should only happen for the GROUP BY clause."""

    ANNOTATE_ALL_SCOPES = False
    """Whether to annotate all scopes during optimization. Used by BigQuery for UNNEST support."""

    DISABLES_ALIAS_REF_EXPANSION = False
    """
    Whether alias reference expansion is disabled for this dialect.

    Some dialects like Oracle do NOT support referencing aliases in projections or WHERE clauses.
    The original expression must be repeated instead.

    For example, in Oracle:
        SELECT y.foo AS bar, bar * 2 AS baz FROM y  -- INVALID
        SELECT y.foo AS bar, y.foo * 2 AS baz FROM y  -- VALID
    """

    SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = False
    """
    Whether alias references are allowed in JOIN ... ON clauses.

    Most dialects do not support this, but Snowflake allows alias expansion in the JOIN ... ON
    clause (and almost everywhere else)

    For example, in Snowflake:
        SELECT a.id AS user_id FROM a JOIN b ON user_id = b.id  -- VALID

    Reference: https://docs.snowflake.com/en/sql-reference/sql/select#usage-notes
    """

    SUPPORTS_ORDER_BY_ALL = False
    """
    Whether ORDER BY ALL is supported (expands to all the selected columns) as in DuckDB, Spark3/Databricks
    """

    PROJECTION_ALIASES_SHADOW_SOURCE_NAMES = False
    """
    Whether projection alias names can shadow table/source names in GROUP BY and HAVING clauses.

    In BigQuery, when a projection alias has the same name as a source table, the alias takes
    precedence in GROUP BY and HAVING clauses, and the table becomes inaccessible by that name.

    For example, in BigQuery:
        SELECT id, ARRAY_AGG(col) AS custom_fields
        FROM custom_fields
        GROUP BY id
        HAVING id >= 1

    The "custom_fields" source is shadowed by the projection alias, so we cannot qualify "id"
    with "custom_fields" in GROUP BY/HAVING.
    """

    TABLES_REFERENCEABLE_AS_COLUMNS = False
    """
    Whether table names can be referenced as columns (treated as structs).

    BigQuery allows tables to be referenced as columns in queries, automatically treating
    them as struct values containing all the table's columns.

    For example, in BigQuery:
        SELECT t FROM my_table AS t  -- Returns entire row as a struct
    """

    SUPPORTS_STRUCT_STAR_EXPANSION = False
    """
    Whether the dialect supports expanding struct fields using star notation (e.g., struct_col.*).

    BigQuery allows struct fields to be expanded with the star operator:
        SELECT t.struct_col.* FROM table t
    RisingWave also allows struct field expansion with the star operator using parentheses:
        SELECT (t.struct_col).* FROM table t

    This expands to all fields within the struct.
    """

    EXCLUDES_PSEUDOCOLUMNS_FROM_STAR = False
    """
    Whether pseudocolumns should be excluded from star expansion (SELECT *).

    Pseudocolumns are special dialect-specific columns (e.g., Oracle's ROWNUM, ROWID, LEVEL,
    or BigQuery's _PARTITIONTIME, _PARTITIONDATE) that are implicitly available but not part
    of the table schema. When this is True, SELECT * will not include these pseudocolumns;
    they must be explicitly selected.
    """

    QUERY_RESULTS_ARE_STRUCTS = False
    """
    Whether query results are typed as structs in metadata for type inference.

    In BigQuery, subqueries store their column types as a STRUCT in metadata,
    enabling special type inference for ARRAY(SELECT ...) expressions:
        ARRAY(SELECT x, y FROM t) → ARRAY<STRUCT<...>>

    For single column subqueries, BigQuery unwraps the struct:
        ARRAY(SELECT x FROM t) → ARRAY<type_of_x>

    This is metadata-only for type inference.
    """

    REQUIRES_PARENTHESIZED_STRUCT_ACCESS = False
    """
    Whether struct field access requires parentheses around the expression.

    RisingWave requires parentheses for struct field access in certain contexts:
        SELECT (col.field).subfield FROM table  -- Parentheses required

    Without parentheses, the parser may not correctly interpret nested struct access.

    Reference: https://docs.risingwave.com/sql/data-types/struct#retrieve-data-in-a-struct
    """

    SUPPORTS_NULL_TYPE = False
    """
    Whether NULL/VOID is supported as a valid data type (not just a value).

    Databricks and Spark v3+ support NULL as an actual type, allowing expressions like:
        SELECT NULL AS col  -- Has type NULL, not just value NULL
        CAST(x AS VOID)     -- Valid type cast
    """

    COALESCE_COMPARISON_NON_STANDARD = False
    """
    Whether COALESCE in comparisons has non-standard NULL semantics.

    We can't convert `COALESCE(x, 1) = 2` into `NOT x IS NULL AND x = 2` for redshift,
    because they are not always equivalent. For example,  if `x` is `NULL` and it comes
    from a table, then the result is `NULL`, despite `FALSE AND NULL` evaluating to `FALSE`.

    In standard SQL and most dialects, these expressions are equivalent, but Redshift treats
    table NULLs differently in this context.
    """

    HAS_DISTINCT_ARRAY_CONSTRUCTORS = False
    """
    Whether the ARRAY constructor is context-sensitive, i.e in Redshift ARRAY[1, 2, 3] != ARRAY(1, 2, 3)
    as the former is of type INT[] vs the latter which is SUPER
    """

    SUPPORTS_FIXED_SIZE_ARRAYS = False
    """
    Whether expressions such as x::INT[5] should be parsed as fixed-size array defs/casts e.g.
    in DuckDB. In dialects which don't support fixed size arrays such as Snowflake, this should
    be interpreted as a subscript/index operator.
    """

    STRICT_JSON_PATH_SYNTAX = True
    """Whether failing to parse a JSON path expression using the JSONPath dialect will log a warning."""

    ON_CONDITION_EMPTY_BEFORE_ERROR = True
    """Whether "X ON EMPTY" should come before "X ON ERROR" (for dialects like T-SQL, MySQL, Oracle)."""

    ARRAY_AGG_INCLUDES_NULLS: t.Optional[bool] = True
    """Whether ArrayAgg needs to filter NULL values."""

    PROMOTE_TO_INFERRED_DATETIME_TYPE = False
    """
    This flag is used in the optimizer's canonicalize rule and determines whether x will be promoted
    to the literal's type in x::DATE < '2020-01-01 12:05:03' (i.e., DATETIME). When false, the literal
    is cast to x's type to match it instead.
    """

    SUPPORTS_VALUES_DEFAULT = True
    """Whether the DEFAULT keyword is supported in the VALUES clause."""

    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = False
    """Whether number literals can include underscores for better readability"""

    HEX_STRING_IS_INTEGER_TYPE: bool = False
    """Whether hex strings such as x'CC' evaluate to integer or binary/blob type"""

    REGEXP_EXTRACT_DEFAULT_GROUP = 0
    """The default value for the capturing group."""

    REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL = True
    """Whether REGEXP_EXTRACT returns NULL when the position arg exceeds the string length."""

    SET_OP_DISTINCT_BY_DEFAULT: t.Dict[t.Type[exp.Expression], t.Optional[bool]] = {
        exp.Except: True,
        exp.Intersect: True,
        exp.Union: True,
    }
    """
    Whether a set operation uses DISTINCT by default. This is `None` when either `DISTINCT` or `ALL`
    must be explicitly specified.
    """

    CREATABLE_KIND_MAPPING: dict[str, str] = {}
    """
    Helper for dialects that use a different name for the same creatable kind. For example, the Clickhouse
    equivalent of CREATE SCHEMA is CREATE DATABASE.
    """

    ALTER_TABLE_SUPPORTS_CASCADE = False
    """
    Hive by default does not update the schema of existing partitions when a column is changed.
    the CASCADE clause is used to indicate that the change should be propagated to all existing partitions.
    the Spark dialect, while derived from Hive, does not support the CASCADE clause.
    """

    # Whether ADD is present for each column added by ALTER TABLE
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = True

    # Whether the value/LHS of the TRY_CAST(<value> AS <type>) should strictly be a
    # STRING type (Snowflake's case) or can be of any type
    TRY_CAST_REQUIRES_STRING: t.Optional[bool] = None

    # Whether the double negation can be applied
    # Not safe with MySQL and SQLite due to type coercion (may not return boolean)
    SAFE_TO_ELIMINATE_DOUBLE_NEGATION = True

    # Whether the INITCAP function supports custom delimiter characters as the second argument
    # Default delimiter characters for INITCAP function: whitespace and non-alphanumeric characters
    INITCAP_SUPPORTS_CUSTOM_DELIMITERS = True
    INITCAP_DEFAULT_DELIMITER_CHARS = " \t\n\r\f\v!\"#$%&'()*+,\\-./:;<=>?@\\[\\]^_`{|}~"

    BYTE_STRING_IS_BYTES_TYPE: bool = False
    """
    Whether byte string literals (ex: BigQuery's b'...') are typed as BYTES/BINARY
    """

    UUID_IS_STRING_TYPE: bool = False
    """
    Whether a UUID is considered a string or a UUID type.
    """

    JSON_EXTRACT_SCALAR_SCALAR_ONLY = False
    """
    Whether JSON_EXTRACT_SCALAR returns null if a non-scalar value is selected.
    """

    DEFAULT_FUNCTIONS_COLUMN_NAMES: t.Dict[t.Type[exp.Func], t.Union[str, t.Tuple[str, ...]]] = {}
    """
    Maps function expressions to their default output column name(s).

    For example, in Postgres, generate_series function outputs a column named "generate_series" by default,
    so we map the ExplodingGenerateSeries expression to "generate_series" string.
    """

    DEFAULT_NULL_TYPE = exp.DataType.Type.UNKNOWN
    """
    The default type of NULL for producing the correct projection type.

    For example, in BigQuery the default type of the NULL value is INT64.
    """

    LEAST_GREATEST_IGNORES_NULLS = True
    """
    Whether LEAST/GREATEST functions ignore NULL values, e.g:
    - BigQuery, Snowflake, MySQL, Presto/Trino: LEAST(1, NULL, 2) -> NULL
    - Spark, Postgres, DuckDB, TSQL: LEAST(1, NULL, 2) -> 1
    """

    PRIORITIZE_NON_LITERAL_TYPES = False
    """
    Whether to prioritize non-literal types over literals during type annotation.
    """

    # --- Autofilled ---

    tokenizer_class = Tokenizer
    jsonpath_tokenizer_class = JSONPathTokenizer
    parser_class = Parser
    generator_class = Generator

    # A trie of the time_mapping keys
    TIME_TRIE: t.Dict = {}
    FORMAT_TRIE: t.Dict = {}

    INVERSE_TIME_MAPPING: t.Dict[str, str] = {}
    INVERSE_TIME_TRIE: t.Dict = {}
    INVERSE_FORMAT_MAPPING: t.Dict[str, str] = {}
    INVERSE_FORMAT_TRIE: t.Dict = {}

    INVERSE_CREATABLE_KIND_MAPPING: dict[str, str] = {}

    ESCAPED_SEQUENCES: t.Dict[str, str] = {}

    # Delimiters for string literals and identifiers
    QUOTE_START = "'"
    QUOTE_END = "'"
    IDENTIFIER_START = '"'
    IDENTIFIER_END = '"'

    VALID_INTERVAL_UNITS: t.Set[str] = set()

    # Delimiters for bit, hex, byte and unicode literals
    BIT_START: t.Optional[str] = None
    BIT_END: t.Optional[str] = None
    HEX_START: t.Optional[str] = None
    HEX_END: t.Optional[str] = None
    BYTE_START: t.Optional[str] = None
    BYTE_END: t.Optional[str] = None
    UNICODE_START: t.Optional[str] = None
    UNICODE_END: t.Optional[str] = None

    DATE_PART_MAPPING = {
        "Y": "YEAR",
        "YY": "YEAR",
        "YYY": "YEAR",
        "YYYY": "YEAR",
        "YR": "YEAR",
        "YEARS": "YEAR",
        "YRS": "YEAR",
        "MM": "MONTH",
        "MON": "MONTH",
        "MONS": "MONTH",
        "MONTHS": "MONTH",
        "D": "DAY",
        "DD": "DAY",
        "DAYS": "DAY",
        "DAYOFMONTH": "DAY",
        "DAY OF WEEK": "DAYOFWEEK",
        "WEEKDAY": "DAYOFWEEK",
        "DOW": "DAYOFWEEK",
        "DW": "DAYOFWEEK",
        "WEEKDAY_ISO": "DAYOFWEEKISO",
        "DOW_ISO": "DAYOFWEEKISO",
        "DW_ISO": "DAYOFWEEKISO",
        "DAYOFWEEK_ISO": "DAYOFWEEKISO",
        "DAY OF YEAR": "DAYOFYEAR",
        "DOY": "DAYOFYEAR",
        "DY": "DAYOFYEAR",
        "W": "WEEK",
        "WK": "WEEK",
        "WEEKOFYEAR": "WEEK",
        "WOY": "WEEK",
        "WY": "WEEK",
        "WEEK_ISO": "WEEKISO",
        "WEEKOFYEARISO": "WEEKISO",
        "WEEKOFYEAR_ISO": "WEEKISO",
        "Q": "QUARTER",
        "QTR": "QUARTER",
        "QTRS": "QUARTER",
        "QUARTERS": "QUARTER",
        "H": "HOUR",
        "HH": "HOUR",
        "HR": "HOUR",
        "HOURS": "HOUR",
        "HRS": "HOUR",
        "M": "MINUTE",
        "MI": "MINUTE",
        "MIN": "MINUTE",
        "MINUTES": "MINUTE",
        "MINS": "MINUTE",
        "S": "SECOND",
        "SEC": "SECOND",
        "SECONDS": "SECOND",
        "SECS": "SECOND",
        "MS": "MILLISECOND",
        "MSEC": "MILLISECOND",
        "MSECS": "MILLISECOND",
        "MSECOND": "MILLISECOND",
        "MSECONDS": "MILLISECOND",
        "MILLISEC": "MILLISECOND",
        "MILLISECS": "MILLISECOND",
        "MILLISECON": "MILLISECOND",
        "MILLISECONDS": "MILLISECOND",
        "US": "MICROSECOND",
        "USEC": "MICROSECOND",
        "USECS": "MICROSECOND",
        "MICROSEC": "MICROSECOND",
        "MICROSECS": "MICROSECOND",
        "USECOND": "MICROSECOND",
        "USECONDS": "MICROSECOND",
        "MICROSECONDS": "MICROSECOND",
        "NS": "NANOSECOND",
        "NSEC": "NANOSECOND",
        "NANOSEC": "NANOSECOND",
        "NSECOND": "NANOSECOND",
        "NSECONDS": "NANOSECOND",
        "NANOSECS": "NANOSECOND",
        "EPOCH_SECOND": "EPOCH",
        "EPOCH_SECONDS": "EPOCH",
        "EPOCH_MILLISECONDS": "EPOCH_MILLISECOND",
        "EPOCH_MICROSECONDS": "EPOCH_MICROSECOND",
        "EPOCH_NANOSECONDS": "EPOCH_NANOSECOND",
        "TZH": "TIMEZONE_HOUR",
        "TZM": "TIMEZONE_MINUTE",
        "DEC": "DECADE",
        "DECS": "DECADE",
        "DECADES": "DECADE",
        "MIL": "MILLENNIUM",
        "MILS": "MILLENNIUM",
        "MILLENIA": "MILLENNIUM",
        "C": "CENTURY",
        "CENT": "CENTURY",
        "CENTS": "CENTURY",
        "CENTURIES": "CENTURY",
    }

    # Specifies what types a given type can be coerced into
    COERCES_TO: t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]] = {}

    # Specifies type inference & validation rules for expressions
    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # Determines the supported Dialect instance settings
    SUPPORTED_SETTINGS = {
        "normalization_strategy",
        "version",
    }

    @classmethod
    def get_or_raise(cls, dialect: DialectType) -> Dialect:
        """
        Look up a dialect in the global dialect registry and return it if it exists.

        Args:
            dialect: The target dialect. If this is a string, it can be optionally followed by
                additional key-value pairs that are separated by commas and are used to specify
                dialect settings, such as whether the dialect's identifiers are case-sensitive.

        Example:
            >>> dialect = dialect_class = get_or_raise("duckdb")
            >>> dialect = get_or_raise("mysql, normalization_strategy = case_sensitive")

        Returns:
            The corresponding Dialect instance.
        """

        if not dialect:
            return cls()
        if isinstance(dialect, _Dialect):
            return dialect()
        if isinstance(dialect, Dialect):
            return dialect
        if isinstance(dialect, str):
            try:
                dialect_name, *kv_strings = dialect.split(",")
                kv_pairs = (kv.split("=") for kv in kv_strings)
                kwargs = {}
                for pair in kv_pairs:
                    key = pair[0].strip()
                    value: t.Union[bool | str | None] = None

                    if len(pair) == 1:
                        # Default initialize standalone settings to True
                        value = True
                    elif len(pair) == 2:
                        value = pair[1].strip()

                    kwargs[key] = to_bool(value)

            except ValueError:
                raise ValueError(
                    f"Invalid dialect format: '{dialect}'. "
                    "Please use the correct format: 'dialect [, k1 = v2 [, ...]]'."
                )

            result = cls.get(dialect_name.strip())
            if not result:
                # Include both built-in dialects and any loaded dialects for better error messages
                all_dialects = set(DIALECT_MODULE_NAMES) | set(cls._classes.keys())
                suggest_closest_match_and_fail("dialect", dialect_name, all_dialects)

            assert result is not None
            return result(**kwargs)

        raise ValueError(f"Invalid dialect type for '{dialect}': '{type(dialect)}'.")

    @classmethod
    def format_time(
        cls, expression: t.Optional[str | exp.Expression]
    ) -> t.Optional[exp.Expression]:
        """Converts a time format in this dialect to its equivalent Python `strftime` format."""
        if isinstance(expression, str):
            return exp.Literal.string(
                # the time formats are quoted
                format_time(expression[1:-1], cls.TIME_MAPPING, cls.TIME_TRIE)
            )

        if expression and expression.is_string:
            return exp.Literal.string(format_time(expression.this, cls.TIME_MAPPING, cls.TIME_TRIE))

        return expression

    def __init__(self, **kwargs) -> None:
        parts = str(kwargs.pop("version", sys.maxsize)).split(".")
        parts.extend(["0"] * (3 - len(parts)))
        self.version = tuple(int(p) for p in parts[:3])

        normalization_strategy = kwargs.pop("normalization_strategy", None)
        if normalization_strategy is None:
            self.normalization_strategy = self.NORMALIZATION_STRATEGY
        else:
            self.normalization_strategy = NormalizationStrategy(normalization_strategy.upper())

        self.settings = kwargs

        for unsupported_setting in kwargs.keys() - self.SUPPORTED_SETTINGS:
            suggest_closest_match_and_fail("setting", unsupported_setting, self.SUPPORTED_SETTINGS)

    def __eq__(self, other: t.Any) -> bool:
        # Does not currently take dialect state into account
        return type(self) == other

    def __hash__(self) -> int:
        # Does not currently take dialect state into account
        return hash(type(self))

    def normalize_identifier(self, expression: E) -> E:
        """
        Transforms an identifier in a way that resembles how it'd be resolved by this dialect.

        For example, an identifier like `FoO` would be resolved as `foo` in Postgres, because it
        lowercases all unquoted identifiers. On the other hand, Snowflake uppercases them, so
        it would resolve it as `FOO`. If it was quoted, it'd need to be treated as case-sensitive,
        and so any normalization would be prohibited in order to avoid "breaking" the identifier.

        There are also dialects like Spark, which are case-insensitive even when quotes are
        present, and dialects like MySQL, whose resolution rules match those employed by the
        underlying operating system, for example they may always be case-sensitive in Linux.

        Finally, the normalization behavior of some engines can even be controlled through flags,
        like in Redshift's case, where users can explicitly set enable_case_sensitive_identifier.

        SQLGlot aims to understand and handle all of these different behaviors gracefully, so
        that it can analyze queries in the optimizer and successfully capture their semantics.
        """
        if (
            isinstance(expression, exp.Identifier)
            and self.normalization_strategy is not NormalizationStrategy.CASE_SENSITIVE
            and (
                not expression.quoted
                or self.normalization_strategy
                in (
                    NormalizationStrategy.CASE_INSENSITIVE,
                    NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE,
                )
            )
        ):
            normalized = (
                expression.this.upper()
                if self.normalization_strategy
                in (
                    NormalizationStrategy.UPPERCASE,
                    NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE,
                )
                else expression.this.lower()
            )
            expression.set("this", normalized)

        return expression

    def case_sensitive(self, text: str) -> bool:
        """Checks if text contains any case sensitive characters, based on the dialect's rules."""
        if self.normalization_strategy is NormalizationStrategy.CASE_INSENSITIVE:
            return False

        unsafe = (
            str.islower
            if self.normalization_strategy is NormalizationStrategy.UPPERCASE
            else str.isupper
        )
        return any(unsafe(char) for char in text)

    def can_quote(self, identifier: exp.Identifier, identify: str | bool = "safe") -> bool:
        """Checks if an identifier can be quoted

        Args:
            identifier: The identifier to check.
            identify:
                `True`: Always returns `True` except for certain cases.
                `"safe"`: Only returns `True` if the identifier is case-insensitive.
                `"unsafe"`: Only returns `True` if the identifier is case-sensitive.

        Returns:
            Whether the given text can be identified.
        """
        if identifier.quoted:
            return True
        if not identify:
            return False
        if isinstance(identifier.parent, exp.Func):
            return False
        if identify is True:
            return True

        is_safe = not self.case_sensitive(identifier.this) and bool(
            exp.SAFE_IDENTIFIER_RE.match(identifier.this)
        )

        if identify == "safe":
            return is_safe
        if identify == "unsafe":
            return not is_safe

        raise ValueError(f"Unexpected argument for identify: '{identify}'")

    def quote_identifier(self, expression: E, identify: bool = True) -> E:
        """
        Adds quotes to a given expression if it is an identifier.

        Args:
            expression: The expression of interest. If it's not an `Identifier`, this method is a no-op.
            identify: If set to `False`, the quotes will only be added if the identifier is deemed
                "unsafe", with respect to its characters and this dialect's normalization strategy.
        """
        if isinstance(expression, exp.Identifier):
            expression.set("quoted", self.can_quote(expression, identify or "unsafe"))
        return expression

    def to_json_path(self, path: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if isinstance(path, exp.Literal):
            path_text = path.name
            if path.is_number:
                path_text = f"[{path_text}]"
            try:
                return parse_json_path(path_text, self)
            except ParseError as e:
                if self.STRICT_JSON_PATH_SYNTAX and not path_text.lstrip().startswith(
                    ("lax", "strict")
                ):
                    logger.warning(f"Invalid JSON path syntax. {str(e)}")

        return path

    def parse(self, sql: str, **opts) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse(self.tokenize(sql), sql)

    def parse_into(
        self, expression_type: exp.IntoType, sql: str, **opts
    ) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse_into(expression_type, self.tokenize(sql), sql)

    def generate(self, expression: exp.Expression, copy: bool = True, **opts) -> str:
        return self.generator(**opts).generate(expression, copy=copy)

    def transpile(self, sql: str, **opts) -> t.List[str]:
        return [
            self.generate(expression, copy=False, **opts) if expression else ""
            for expression in self.parse(sql)
        ]

    def tokenize(self, sql: str, **opts) -> t.List[Token]:
        return self.tokenizer(**opts).tokenize(sql)

    def tokenizer(self, **opts) -> Tokenizer:
        return self.tokenizer_class(**{"dialect": self, **opts})

    def jsonpath_tokenizer(self, **opts) -> JSONPathTokenizer:
        return self.jsonpath_tokenizer_class(**{"dialect": self, **opts})

    def parser(self, **opts) -> Parser:
        return self.parser_class(**{"dialect": self, **opts})

    def generator(self, **opts) -> Generator:
        return self.generator_class(**{"dialect": self, **opts})

    def generate_values_aliases(self, expression: exp.Values) -> t.List[exp.Identifier]:
        return [
            exp.to_identifier(f"_col_{i}")
            for i, _ in enumerate(expression.expressions[0].expressions)
        ]


DialectType = t.Union[str, Dialect, t.Type[Dialect], None]


def rename_func(name: str) -> t.Callable[[Generator, exp.Expression], str]:
    return lambda self, expression: self.func(name, *flatten(expression.args.values()))


@unsupported_args("accuracy")
def approx_count_distinct_sql(self: Generator, expression: exp.ApproxDistinct) -> str:
    return self.func("APPROX_COUNT_DISTINCT", expression.this)


def if_sql(
    name: str = "IF", false_value: t.Optional[exp.Expression | str] = None
) -> t.Callable[[Generator, exp.If], str]:
    def _if_sql(self: Generator, expression: exp.If) -> str:
        return self.func(
            name,
            expression.this,
            expression.args.get("true"),
            expression.args.get("false") or false_value,
        )

    return _if_sql


def arrow_json_extract_sql(self: Generator, expression: JSON_EXTRACT_TYPE) -> str:
    this = expression.this
    if self.JSON_TYPE_REQUIRED_FOR_EXTRACTION and isinstance(this, exp.Literal) and this.is_string:
        this.replace(exp.cast(this, exp.DataType.Type.JSON))

    return self.binary(expression, "->" if isinstance(expression, exp.JSONExtract) else "->>")


def inline_array_sql(self: Generator, expression: exp.Expression) -> str:
    return f"[{self.expressions(expression, dynamic=True, new_line=True, skip_first=True, skip_last=True)}]"


def inline_array_unless_query(self: Generator, expression: exp.Expression) -> str:
    elem = seq_get(expression.expressions, 0)
    if isinstance(elem, exp.Expression) and elem.find(exp.Query):
        return self.func("ARRAY", elem)
    return inline_array_sql(self, expression)


def no_ilike_sql(self: Generator, expression: exp.ILike) -> str:
    return self.like_sql(
        exp.Like(
            this=exp.Lower(this=expression.this), expression=exp.Lower(this=expression.expression)
        )
    )


def no_paren_current_date_sql(self: Generator, expression: exp.CurrentDate) -> str:
    zone = self.sql(expression, "this")
    return f"CURRENT_DATE AT TIME ZONE {zone}" if zone else "CURRENT_DATE"


def no_recursive_cte_sql(self: Generator, expression: exp.With) -> str:
    if expression.args.get("recursive"):
        self.unsupported("Recursive CTEs are unsupported")
        expression.set("recursive", False)
    return self.with_sql(expression)


def no_tablesample_sql(self: Generator, expression: exp.TableSample) -> str:
    self.unsupported("TABLESAMPLE unsupported")
    return self.sql(expression.this)


def no_pivot_sql(self: Generator, expression: exp.Pivot) -> str:
    self.unsupported("PIVOT unsupported")
    return ""


def no_trycast_sql(self: Generator, expression: exp.TryCast) -> str:
    return self.cast_sql(expression)


def no_comment_column_constraint_sql(
    self: Generator, expression: exp.CommentColumnConstraint
) -> str:
    self.unsupported("CommentColumnConstraint unsupported")
    return ""


def no_map_from_entries_sql(self: Generator, expression: exp.MapFromEntries) -> str:
    self.unsupported("MAP_FROM_ENTRIES unsupported")
    return ""


def property_sql(self: Generator, expression: exp.Property) -> str:
    return f"{self.property_name(expression, string_key=True)}={self.sql(expression, 'value')}"


def strposition_sql(
    self: Generator,
    expression: exp.StrPosition,
    func_name: str = "STRPOS",
    supports_position: bool = False,
    supports_occurrence: bool = False,
    use_ansi_position: bool = True,
) -> str:
    string = expression.this
    substr = expression.args.get("substr")
    position = expression.args.get("position")
    occurrence = expression.args.get("occurrence")
    zero = exp.Literal.number(0)
    one = exp.Literal.number(1)

    if supports_occurrence and occurrence and supports_position and not position:
        position = one

    transpile_position = position and not supports_position
    if transpile_position:
        string = exp.Substring(this=string, start=position)

    if func_name == "POSITION" and use_ansi_position:
        func = exp.Anonymous(this=func_name, expressions=[exp.In(this=substr, field=string)])
    else:
        args = [substr, string] if func_name in ("LOCATE", "CHARINDEX") else [string, substr]
        if supports_position:
            args.append(position)
        if occurrence:
            if supports_occurrence:
                args.append(occurrence)
            else:
                self.unsupported(f"{func_name} does not support the occurrence parameter.")
        func = exp.Anonymous(this=func_name, expressions=args)

    if transpile_position:
        func_with_offset = exp.Sub(this=func + position, expression=one)
        func_wrapped = exp.If(this=func.eq(zero), true=zero, false=func_with_offset)
        return self.sql(func_wrapped)

    return self.sql(func)


def struct_extract_sql(self: Generator, expression: exp.StructExtract) -> str:
    return (
        f"{self.sql(expression, 'this')}.{self.sql(exp.to_identifier(expression.expression.name))}"
    )


def build_array_append_with_null_propagation(args: t.List) -> exp.ArrayAppend:
    """
    Builds ArrayAppend with null_propagation=True for Databricks/Spark/Snowflake semantics.

    ARRAY_APPEND returns NULL when array is NULL (NULL propagation).
    This differs from DuckDB/PostgreSQL which create a new array.
    """
    return exp.ArrayAppend(
        this=seq_get(args, 0),
        expression=seq_get(args, 1),
        null_propagation=True,
    )


def array_append_sql(self: Generator, expression: exp.ArrayAppend) -> str:
    """
    Transpile ARRAY_APPEND to dialects that propagate NULL values by default.
    When transpiling from a dialect that does not propagate NULLs like DuckDB/Postgres,
    explicitly handle the NULL case using COALESCE.
    """
    if expression.args.get("null_propagation"):
        return self.func("ARRAY_APPEND", expression.this, expression.expression)

    return self.func(
        "ARRAY_APPEND",
        exp.Coalesce(expressions=[expression.this, exp.Array(expressions=[])]),
        expression.expression,
    )


def var_map_sql(
    self: Generator, expression: exp.Map | exp.VarMap, map_func_name: str = "MAP"
) -> str:
    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return self.func(map_func_name, keys, values)

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(self.sql(key))
        args.append(self.sql(value))

    return self.func(map_func_name, *args)


def months_between_sql(self: Generator, expression: exp.MonthsBetween) -> str:
    """
    Transpile MONTHS_BETWEEN to dialects that don't have native support.

    Snowflake's MONTHS_BETWEEN returns whole months + fractional part where:
    - Fractional part = (DAY(date1) - DAY(date2)) / 31
    - Special case: If both dates are last day of month, fractional part = 0

    Formula: DATEDIFF('month', date2, date1) + (DAY(date1) - DAY(date2)) / 31.0
    """
    date1 = expression.this
    date2 = expression.expression

    # Cast to DATE to ensure consistent behavior
    date1_cast = exp.cast(date1, exp.DataType.Type.DATE, copy=False)
    date2_cast = exp.cast(date2, exp.DataType.Type.DATE, copy=False)

    # Whole months: DATEDIFF('month', date2, date1)
    whole_months = exp.DateDiff(this=date1_cast, expression=date2_cast, unit=exp.var("month"))

    # Day components
    day1 = exp.Day(this=date1_cast.copy())
    day2 = exp.Day(this=date2_cast.copy())

    # Last day of month components
    last_day_of_month1 = exp.LastDay(this=date1_cast.copy())
    last_day_of_month2 = exp.LastDay(this=date2_cast.copy())

    day_of_last_day1 = exp.Day(this=last_day_of_month1)
    day_of_last_day2 = exp.Day(this=last_day_of_month2)

    # Check if both are last day of month
    last_day1 = exp.EQ(this=day1.copy(), expression=day_of_last_day1)
    last_day2 = exp.EQ(this=day2.copy(), expression=day_of_last_day2)
    both_last_day = exp.And(this=last_day1, expression=last_day2)

    # Fractional part: (DAY(date1) - DAY(date2)) / 31.0
    fractional = exp.Div(
        this=exp.Paren(this=exp.Sub(this=day1.copy(), expression=day2.copy())),
        expression=exp.Literal.number("31.0"),
    )

    # If both are last day of month, fractional = 0, else calculate fractional
    fractional_with_check = exp.If(
        this=both_last_day, true=exp.Literal.number("0"), false=fractional
    )

    # Final result: whole_months + fractional
    result = exp.Add(this=whole_months, expression=fractional_with_check)

    return self.sql(result)


def build_formatted_time(
    exp_class: t.Type[E], dialect: str, default: t.Optional[bool | str] = None
) -> t.Callable[[t.List], E]:
    """Helper used for time expressions.

    Args:
        exp_class: the expression class to instantiate.
        dialect: target sql dialect.
        default: the default format, True being time.

    Returns:
        A callable that can be used to return the appropriately formatted time expression.
    """

    def _builder(args: t.List):
        return exp_class(
            this=seq_get(args, 0),
            format=Dialect[dialect].format_time(
                seq_get(args, 1)
                or (Dialect[dialect].TIME_FORMAT if default is True else default or None)
            ),
        )

    return _builder


def time_format(
    dialect: DialectType = None,
) -> t.Callable[[Generator, exp.UnixToStr | exp.StrToUnix], t.Optional[str]]:
    def _time_format(self: Generator, expression: exp.UnixToStr | exp.StrToUnix) -> t.Optional[str]:
        """
        Returns the time format for a given expression, unless it's equivalent
        to the default time format of the dialect of interest.
        """
        time_format = self.format_time(expression)
        return time_format if time_format != Dialect.get_or_raise(dialect).TIME_FORMAT else None

    return _time_format


def build_date_delta(
    exp_class: t.Type[E],
    unit_mapping: t.Optional[t.Dict[str, str]] = None,
    default_unit: t.Optional[str] = "DAY",
    supports_timezone: bool = False,
) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        unit_based = len(args) >= 3
        has_timezone = len(args) == 4
        this = args[2] if unit_based else seq_get(args, 0)
        unit = None
        if unit_based or default_unit:
            unit = args[0] if unit_based else exp.Literal.string(default_unit)
            unit = exp.var(unit_mapping.get(unit.name.lower(), unit.name)) if unit_mapping else unit
        expression = exp_class(this=this, expression=seq_get(args, 1), unit=unit)
        if supports_timezone and has_timezone:
            expression.set("zone", args[-1])
        return expression

    return _builder


def build_date_delta_with_interval(
    expression_class: t.Type[E],
) -> t.Callable[[t.List], t.Optional[E]]:
    def _builder(args: t.List) -> t.Optional[E]:
        if len(args) < 2:
            return None

        interval = args[1]

        if not isinstance(interval, exp.Interval):
            raise ParseError(f"INTERVAL expression expected but got '{interval}'")

        return expression_class(this=args[0], expression=interval.this, unit=unit_to_str(interval))

    return _builder


def date_trunc_to_time(args: t.List) -> exp.DateTrunc | exp.TimestampTrunc:
    unit = seq_get(args, 0)
    this = seq_get(args, 1)

    if isinstance(this, exp.Cast) and this.is_type("date"):
        return exp.DateTrunc(unit=unit, this=this)
    return exp.TimestampTrunc(this=this, unit=unit)


def date_add_interval_sql(
    data_type: str, kind: str
) -> t.Callable[[Generator, exp.Expression], str]:
    def func(self: Generator, expression: exp.Expression) -> str:
        this = self.sql(expression, "this")
        interval = exp.Interval(this=expression.expression, unit=unit_to_var(expression))
        return f"{data_type}_{kind}({this}, {self.sql(interval)})"

    return func


def timestamptrunc_sql(
    func: str = "DATE_TRUNC", zone: bool = False
) -> t.Callable[[Generator, exp.TimestampTrunc], str]:
    def _timestamptrunc_sql(self: Generator, expression: exp.TimestampTrunc) -> str:
        args = [unit_to_str(expression), expression.this]
        if zone:
            args.append(expression.args.get("zone"))
        return self.func(func, *args)

    return _timestamptrunc_sql


def no_timestamp_sql(self: Generator, expression: exp.Timestamp) -> str:
    zone = expression.args.get("zone")
    if not zone:
        from sqlglot.optimizer.annotate_types import annotate_types

        target_type = (
            annotate_types(expression, dialect=self.dialect).type or exp.DataType.Type.TIMESTAMP
        )
        return self.sql(exp.cast(expression.this, target_type))
    if zone.name.lower() in TIMEZONES:
        return self.sql(
            exp.AtTimeZone(
                this=exp.cast(expression.this, exp.DataType.Type.TIMESTAMP),
                zone=zone,
            )
        )
    return self.func("TIMESTAMP", expression.this, zone)


def no_time_sql(self: Generator, expression: exp.Time) -> str:
    # Transpile BQ's TIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIME)
    this = exp.cast(expression.this, exp.DataType.Type.TIMESTAMPTZ)
    expr = exp.cast(
        exp.AtTimeZone(this=this, zone=expression.args.get("zone")), exp.DataType.Type.TIME
    )
    return self.sql(expr)


def no_datetime_sql(self: Generator, expression: exp.Datetime) -> str:
    this = expression.this
    expr = expression.expression

    if expr.name.lower() in TIMEZONES:
        # Transpile BQ's DATETIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIMESTAMP)
        this = exp.cast(this, exp.DataType.Type.TIMESTAMPTZ)
        this = exp.cast(exp.AtTimeZone(this=this, zone=expr), exp.DataType.Type.TIMESTAMP)
        return self.sql(this)

    this = exp.cast(this, exp.DataType.Type.DATE)
    expr = exp.cast(expr, exp.DataType.Type.TIME)

    return self.sql(exp.cast(exp.Add(this=this, expression=expr), exp.DataType.Type.TIMESTAMP))


def left_to_substring_sql(self: Generator, expression: exp.Left) -> str:
    return self.sql(
        exp.Substring(
            this=expression.this, start=exp.Literal.number(1), length=expression.expression
        )
    )


def right_to_substring_sql(self: Generator, expression: exp.Left) -> str:
    return self.sql(
        exp.Substring(
            this=expression.this,
            start=exp.Length(this=expression.this) - exp.paren(expression.expression - 1),
        )
    )


def timestrtotime_sql(
    self: Generator,
    expression: exp.TimeStrToTime,
    include_precision: bool = False,
) -> str:
    datatype = exp.DataType.build(
        exp.DataType.Type.TIMESTAMPTZ
        if expression.args.get("zone")
        else exp.DataType.Type.TIMESTAMP
    )

    if isinstance(expression.this, exp.Literal) and include_precision:
        precision = subsecond_precision(expression.this.name)
        if precision > 0:
            datatype = exp.DataType.build(
                datatype.this, expressions=[exp.DataTypeParam(this=exp.Literal.number(precision))]
            )

    return self.sql(exp.cast(expression.this, datatype, dialect=self.dialect))


def datestrtodate_sql(self: Generator, expression: exp.DateStrToDate) -> str:
    return self.sql(exp.cast(expression.this, exp.DataType.Type.DATE))


# Used for Presto and Duckdb which use functions that don't support charset, and assume utf-8
def encode_decode_sql(
    self: Generator, expression: exp.Expression, name: str, replace: bool = True
) -> str:
    charset = expression.args.get("charset")
    if charset and charset.name.lower() != "utf-8":
        self.unsupported(f"Expected utf-8 character set, got {charset}.")

    return self.func(name, expression.this, expression.args.get("replace") if replace else None)


def min_or_least(self: Generator, expression: exp.Min) -> str:
    name = "LEAST" if expression.expressions else "MIN"
    return rename_func(name)(self, expression)


def max_or_greatest(self: Generator, expression: exp.Max) -> str:
    name = "GREATEST" if expression.expressions else "MAX"
    return rename_func(name)(self, expression)


def count_if_to_sum(self: Generator, expression: exp.CountIf) -> str:
    cond = expression.this

    if isinstance(expression.this, exp.Distinct):
        cond = expression.this.expressions[0]
        self.unsupported("DISTINCT is not supported when converting COUNT_IF to SUM")

    return self.func("sum", exp.func("if", cond, 1, 0))


def trim_sql(self: Generator, expression: exp.Trim, default_trim_type: str = "") -> str:
    target = self.sql(expression, "this")
    trim_type = self.sql(expression, "position") or default_trim_type
    remove_chars = self.sql(expression, "expression")
    collation = self.sql(expression, "collation")

    # Use TRIM/LTRIM/RTRIM syntax if the expression isn't database-specific
    if not remove_chars:
        return self.trim_sql(expression)

    trim_type = f"{trim_type} " if trim_type else ""
    remove_chars = f"{remove_chars} " if remove_chars else ""
    from_part = "FROM " if trim_type or remove_chars else ""
    collation = f" COLLATE {collation}" if collation else ""
    return f"TRIM({trim_type}{remove_chars}{from_part}{target}{collation})"


def str_to_time_sql(self: Generator, expression: exp.Expression) -> str:
    return self.func("STRPTIME", expression.this, self.format_time(expression))


def concat_to_dpipe_sql(self: Generator, expression: exp.Concat) -> str:
    return self.sql(reduce(lambda x, y: exp.DPipe(this=x, expression=y), expression.expressions))


def concat_ws_to_dpipe_sql(self: Generator, expression: exp.ConcatWs) -> str:
    delim, *rest_args = expression.expressions
    return self.sql(
        reduce(
            lambda x, y: exp.DPipe(this=x, expression=exp.DPipe(this=delim, expression=y)),
            rest_args,
        )
    )


@unsupported_args("position", "occurrence", "parameters")
def regexp_extract_sql(
    self: Generator, expression: exp.RegexpExtract | exp.RegexpExtractAll
) -> str:
    group = expression.args.get("group")

    # Do not render group if it's the default value for this dialect
    if group and group.name == str(self.dialect.REGEXP_EXTRACT_DEFAULT_GROUP):
        group = None

    return self.func(expression.sql_name(), expression.this, expression.expression, group)


@unsupported_args("position", "occurrence", "modifiers")
def regexp_replace_sql(self: Generator, expression: exp.RegexpReplace) -> str:
    return self.func(
        "REGEXP_REPLACE", expression.this, expression.expression, expression.args["replacement"]
    )


def pivot_column_names(aggregations: t.List[exp.Expression], dialect: DialectType) -> t.List[str]:
    names = []
    for agg in aggregations:
        if isinstance(agg, exp.Alias):
            names.append(agg.alias)
        else:
            """
            This case corresponds to aggregations without aliases being used as suffixes
            (e.g. col_avg(foo)). We need to unquote identifiers because they're going to
            be quoted in the base parser's `_parse_pivot` method, due to `to_identifier`.
            Otherwise, we'd end up with `col_avg(`foo`)` (notice the double quotes).
            """
            agg_all_unquoted = agg.transform(
                lambda node: (
                    exp.Identifier(this=node.name, quoted=False)
                    if isinstance(node, exp.Identifier)
                    else node
                )
            )
            names.append(agg_all_unquoted.sql(dialect=dialect, normalize_functions="lower"))

    return names


def binary_from_function(expr_type: t.Type[B]) -> t.Callable[[t.List], B]:
    return lambda args: expr_type(this=seq_get(args, 0), expression=seq_get(args, 1))


# Used to represent DATE_TRUNC in Doris, Postgres and Starrocks dialects
def build_timestamp_trunc(args: t.List) -> exp.TimestampTrunc:
    return exp.TimestampTrunc(this=seq_get(args, 1), unit=seq_get(args, 0))


def any_value_to_max_sql(self: Generator, expression: exp.AnyValue) -> str:
    return self.func("MAX", expression.this)


def bool_xor_sql(self: Generator, expression: exp.Xor) -> str:
    a = self.sql(expression.left)
    b = self.sql(expression.right)
    return f"({a} AND (NOT {b})) OR ((NOT {a}) AND {b})"


def is_parse_json(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.ParseJSON) or (
        isinstance(expression, exp.Cast) and expression.is_type("json")
    )


def isnull_to_is_null(args: t.List) -> exp.Expression:
    return exp.Paren(this=exp.Is(this=seq_get(args, 0), expression=exp.null()))


def generatedasidentitycolumnconstraint_sql(
    self: Generator, expression: exp.GeneratedAsIdentityColumnConstraint
) -> str:
    start = self.sql(expression, "start") or "1"
    increment = self.sql(expression, "increment") or "1"
    return f"IDENTITY({start}, {increment})"


def arg_max_or_min_no_count(name: str) -> t.Callable[[Generator, exp.ArgMax | exp.ArgMin], str]:
    @unsupported_args("count")
    def _arg_max_or_min_sql(self: Generator, expression: exp.ArgMax | exp.ArgMin) -> str:
        return self.func(name, expression.this, expression.expression)

    return _arg_max_or_min_sql


def ts_or_ds_add_cast(expression: exp.TsOrDsAdd) -> exp.TsOrDsAdd:
    this = expression.this.copy()

    return_type = expression.return_type
    if return_type.is_type(exp.DataType.Type.DATE):
        # If we need to cast to a DATE, we cast to TIMESTAMP first to make sure we
        # can truncate timestamp strings, because some dialects can't cast them to DATE
        this = exp.cast(this, exp.DataType.Type.TIMESTAMP)

    expression.this.replace(exp.cast(this, return_type))
    return expression


def date_delta_sql(name: str, cast: bool = False) -> t.Callable[[Generator, DATE_ADD_OR_DIFF], str]:
    def _delta_sql(self: Generator, expression: DATE_ADD_OR_DIFF) -> str:
        if cast and isinstance(expression, exp.TsOrDsAdd):
            expression = ts_or_ds_add_cast(expression)

        return self.func(
            name,
            unit_to_var(expression),
            expression.expression,
            expression.this,
        )

    return _delta_sql


def date_delta_to_binary_interval_op(
    cast: bool = True,
) -> t.Callable[[Generator, DATETIME_DELTA], str]:
    def date_delta_to_binary_interval_op_sql(self: Generator, expression: DATETIME_DELTA) -> str:
        this = expression.this
        unit = unit_to_var(expression)
        op = "+" if isinstance(expression, DATETIME_ADD) else "-"

        to_type: t.Optional[exp.DATA_TYPE] = None
        if cast:
            if isinstance(expression, exp.TsOrDsAdd):
                to_type = expression.return_type
            elif this.is_string:
                # Cast string literals (i.e function parameters) to the appropriate type for +/- interval to work
                to_type = (
                    exp.DataType.Type.DATETIME
                    if isinstance(expression, (exp.DatetimeAdd, exp.DatetimeSub))
                    else exp.DataType.Type.DATE
                )

        this = exp.cast(this, to_type) if to_type else this

        expr = expression.expression
        interval = expr if isinstance(expr, exp.Interval) else exp.Interval(this=expr, unit=unit)

        return f"{self.sql(this)} {op} {self.sql(interval)}"

    return date_delta_to_binary_interval_op_sql


def unit_to_str(expression: exp.Expression, default: str = "DAY") -> t.Optional[exp.Expression]:
    unit = expression.args.get("unit")
    if not unit:
        return exp.Literal.string(default) if default else None

    if isinstance(unit, exp.Placeholder) or type(unit) not in (exp.Var, exp.Literal):
        return unit

    return exp.Literal.string(unit.name)


def unit_to_var(expression: exp.Expression, default: str = "DAY") -> t.Optional[exp.Expression]:
    unit = expression.args.get("unit")

    if isinstance(unit, (exp.Var, exp.Placeholder, exp.WeekStart, exp.Column)):
        return unit

    value = unit.name if unit else default
    return exp.Var(this=value) if value else None


@t.overload
def map_date_part(part: exp.Expression, dialect: DialectType = Dialect) -> exp.Var:
    pass


@t.overload
def map_date_part(
    part: t.Optional[exp.Expression], dialect: DialectType = Dialect
) -> t.Optional[exp.Expression]:
    pass


def map_date_part(part, dialect: DialectType = Dialect):
    mapped = (
        Dialect.get_or_raise(dialect).DATE_PART_MAPPING.get(part.name.upper())
        if part and not (isinstance(part, exp.Column) and len(part.parts) != 1)
        else None
    )
    if mapped:
        return exp.Literal.string(mapped) if part.is_string else exp.var(mapped)

    return part


def no_last_day_sql(self: Generator, expression: exp.LastDay) -> str:
    trunc_curr_date = exp.func("date_trunc", "month", expression.this)
    plus_one_month = exp.func("date_add", trunc_curr_date, 1, "month")
    minus_one_day = exp.func("date_sub", plus_one_month, 1, "day")

    return self.sql(exp.cast(minus_one_day, exp.DataType.Type.DATE))


def merge_without_target_sql(self: Generator, expression: exp.Merge) -> str:
    """Remove table refs from columns in when statements."""
    alias = expression.this.args.get("alias")

    def normalize(identifier: t.Optional[exp.Identifier]) -> t.Optional[str]:
        return self.dialect.normalize_identifier(identifier).name if identifier else None

    targets = {normalize(expression.this.this)}

    if alias:
        targets.add(normalize(alias.this))

    for when in expression.args["whens"].expressions:
        # only remove the target table names from certain parts of WHEN MATCHED / WHEN NOT MATCHED
        # they are still valid in the <condition>, the right hand side of each UPDATE and the VALUES part
        # (not the column list) of the INSERT
        then: exp.Insert | exp.Update | None = when.args.get("then")
        if then:
            if isinstance(then, exp.Update):
                for equals in then.find_all(exp.EQ):
                    equal_lhs = equals.this
                    if (
                        isinstance(equal_lhs, exp.Column)
                        and normalize(equal_lhs.args.get("table")) in targets
                    ):
                        equal_lhs.replace(exp.column(equal_lhs.this))
            if isinstance(then, exp.Insert):
                column_list = then.this
                if isinstance(column_list, exp.Tuple):
                    for column in column_list.expressions:
                        if normalize(column.args.get("table")) in targets:
                            column.replace(exp.column(column.this))

    return self.merge_sql(expression)


def build_json_extract_path(
    expr_type: t.Type[F],
    zero_based_indexing: bool = True,
    arrow_req_json_type: bool = False,
    json_type: t.Optional[str] = None,
) -> t.Callable[[t.List], F]:
    def _builder(args: t.List) -> F:
        segments: t.List[exp.JSONPathPart] = [exp.JSONPathRoot()]
        for arg in args[1:]:
            if not isinstance(arg, exp.Literal):
                # We use the fallback parser because we can't really transpile non-literals safely
                return expr_type.from_arg_list(args)

            text = arg.name
            if is_int(text) and (not arrow_req_json_type or not arg.is_string):
                index = int(text)
                segments.append(
                    exp.JSONPathSubscript(this=index if zero_based_indexing else index - 1)
                )
            else:
                segments.append(exp.JSONPathKey(this=text))

        # This is done to avoid failing in the expression validator due to the arg count
        del args[2:]
        kwargs = {
            "this": seq_get(args, 0),
            "expression": exp.JSONPath(expressions=segments),
        }

        is_jsonb = issubclass(expr_type, (exp.JSONBExtract, exp.JSONBExtractScalar))
        if not is_jsonb:
            kwargs["only_json_types"] = arrow_req_json_type

        if json_type is not None:
            kwargs["json_type"] = json_type

        return expr_type(**kwargs)

    return _builder


def json_extract_segments(
    name: str, quoted_index: bool = True, op: t.Optional[str] = None
) -> t.Callable[[Generator, JSON_EXTRACT_TYPE], str]:
    def _json_extract_segments(self: Generator, expression: JSON_EXTRACT_TYPE) -> str:
        path = expression.expression
        if not isinstance(path, exp.JSONPath):
            return rename_func(name)(self, expression)

        escape = path.args.get("escape")

        segments = []
        for segment in path.expressions:
            path = self.sql(segment)
            if path:
                if isinstance(segment, exp.JSONPathPart) and (
                    quoted_index or not isinstance(segment, exp.JSONPathSubscript)
                ):
                    if escape:
                        path = self.escape_str(path)

                    path = f"{self.dialect.QUOTE_START}{path}{self.dialect.QUOTE_END}"

                segments.append(path)

        if op:
            return f" {op} ".join([self.sql(expression.this), *segments])
        return self.func(name, expression.this, *segments)

    return _json_extract_segments


def json_path_key_only_name(self: Generator, expression: exp.JSONPathKey) -> str:
    if isinstance(expression.this, exp.JSONPathWildcard):
        self.unsupported("Unsupported wildcard in JSONPathKey expression")

    return expression.name


def filter_array_using_unnest(
    self: Generator, expression: exp.ArrayFilter | exp.ArrayRemove
) -> str:
    cond = expression.expression
    if isinstance(cond, exp.Lambda) and len(cond.expressions) == 1:
        alias = cond.expressions[0]
        cond = cond.this
    elif isinstance(cond, exp.Predicate):
        alias = "_u"
    elif isinstance(expression, exp.ArrayRemove):
        alias = "_u"
        cond = exp.NEQ(this=alias, expression=expression.expression)
    else:
        self.unsupported("Unsupported filter condition")
        return ""

    unnest = exp.Unnest(expressions=[expression.this])
    filtered = exp.select(alias).from_(exp.alias_(unnest, None, table=[alias])).where(cond)
    return self.sql(exp.Array(expressions=[filtered]))


def remove_from_array_using_filter(self: Generator, expression: exp.ArrayRemove) -> str:
    lambda_id = exp.to_identifier("_u")
    cond = exp.NEQ(this=lambda_id, expression=expression.expression)
    return self.sql(
        exp.ArrayFilter(
            this=expression.this, expression=exp.Lambda(this=cond, expressions=[lambda_id])
        )
    )


def to_number_with_nls_param(self: Generator, expression: exp.ToNumber) -> str:
    return self.func(
        "TO_NUMBER",
        expression.this,
        expression.args.get("format"),
        expression.args.get("nlsparam"),
    )


def build_default_decimal_type(
    precision: t.Optional[int] = None, scale: t.Optional[int] = None
) -> t.Callable[[exp.DataType], exp.DataType]:
    def _builder(dtype: exp.DataType) -> exp.DataType:
        if dtype.expressions or precision is None:
            return dtype

        params = f"{precision}{f', {scale}' if scale is not None else ''}"
        return exp.DataType.build(f"DECIMAL({params})")

    return _builder


def build_timestamp_from_parts(args: t.List) -> exp.Func:
    if len(args) == 2:
        # Other dialects don't have the TIMESTAMP_FROM_PARTS(date, time) concept,
        # so we parse this into Anonymous for now instead of introducing complexity
        return exp.Anonymous(this="TIMESTAMP_FROM_PARTS", expressions=args)

    return exp.TimestampFromParts.from_arg_list(args)


def sha256_sql(self: Generator, expression: exp.SHA2) -> str:
    return self.func(f"SHA{expression.text('length') or '256'}", expression.this)


def sha2_digest_sql(self: Generator, expression: exp.SHA2Digest) -> str:
    return self.func(f"SHA{expression.text('length') or '256'}", expression.this)


def sequence_sql(self: Generator, expression: exp.GenerateSeries | exp.GenerateDateArray) -> str:
    start = expression.args.get("start")
    end = expression.args.get("end")
    step = expression.args.get("step")

    if isinstance(start, exp.Cast):
        target_type = start.to
    elif isinstance(end, exp.Cast):
        target_type = end.to
    else:
        target_type = None

    if start and end:
        if target_type and target_type.is_type("date", "timestamp"):
            if isinstance(start, exp.Cast) and target_type is start.to:
                end = exp.cast(end, target_type)
            else:
                start = exp.cast(start, target_type)

        if expression.args.get("is_end_exclusive"):
            step_value = step or exp.Literal.number(1)
            end = exp.paren(exp.Sub(this=end, expression=step_value), copy=False)

            sequence_call = exp.Anonymous(
                this="SEQUENCE", expressions=[e for e in (start, end, step) if e]
            )
            zero = exp.Literal.number(0)
            should_return_empty = exp.or_(
                exp.EQ(this=step_value.copy(), expression=zero.copy()),
                exp.and_(
                    exp.GT(this=step_value.copy(), expression=zero.copy()),
                    exp.GTE(this=start.copy(), expression=end.copy()),
                ),
                exp.and_(
                    exp.LT(this=step_value.copy(), expression=zero.copy()),
                    exp.LTE(this=start.copy(), expression=end.copy()),
                ),
            )
            empty_array_or_sequence = exp.If(
                this=should_return_empty,
                true=exp.Array(expressions=[]),
                false=sequence_call,
            )
            return self.sql(self._simplify_unless_literal(empty_array_or_sequence))

    return self.func("SEQUENCE", start, end, step)


def build_like(
    expr_type: t.Type[E], not_like: bool = False
) -> t.Callable[[t.List], exp.Expression]:
    def _builder(args: t.List) -> exp.Expression:
        like_expr: exp.Expression = expr_type(this=seq_get(args, 0), expression=seq_get(args, 1))

        if escape := seq_get(args, 2):
            like_expr = exp.Escape(this=like_expr, expression=escape)

        if not_like:
            like_expr = exp.Not(this=like_expr)

        return like_expr

    return _builder


def build_regexp_extract(expr_type: t.Type[E]) -> t.Callable[[t.List, Dialect], E]:
    def _builder(args: t.List, dialect: Dialect) -> E:
        # The "position" argument specifies the index of the string character to start matching from.
        # `null_if_pos_overflow` reflects the dialect's behavior when position is greater than the string
        # length. If true, returns NULL. If false, returns an empty string. `null_if_pos_overflow` is
        # only needed for exp.RegexpExtract - exp.RegexpExtractAll always returns an empty array if
        # position overflows.
        return expr_type(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            group=seq_get(args, 2) or exp.Literal.number(dialect.REGEXP_EXTRACT_DEFAULT_GROUP),
            parameters=seq_get(args, 3),
            **(
                {"null_if_pos_overflow": dialect.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL}
                if expr_type is exp.RegexpExtract
                else {}
            ),
        )

    return _builder


def explode_to_unnest_sql(self: Generator, expression: exp.Lateral) -> str:
    if isinstance(expression.this, exp.Explode):
        return self.sql(
            exp.Join(
                this=exp.Unnest(
                    expressions=[expression.this.this],
                    alias=expression.args.get("alias"),
                    offset=isinstance(expression.this, exp.Posexplode),
                ),
                kind="cross",
            )
        )
    return self.lateral_sql(expression)


def timestampdiff_sql(self: Generator, expression: exp.DatetimeDiff | exp.TimestampDiff) -> str:
    return self.func("TIMESTAMPDIFF", expression.unit, expression.expression, expression.this)


def no_make_interval_sql(self: Generator, expression: exp.MakeInterval, sep: str = ", ") -> str:
    args = []
    for unit, value in expression.args.items():
        if isinstance(value, exp.Kwarg):
            value = value.expression

        args.append(f"{value} {unit}")

    return f"INTERVAL '{self.format_args(*args, sep=sep)}'"


def length_or_char_length_sql(self: Generator, expression: exp.Length) -> str:
    length_func = "LENGTH" if expression.args.get("binary") else "CHAR_LENGTH"
    return self.func(length_func, expression.this)


def groupconcat_sql(
    self: Generator,
    expression: exp.GroupConcat,
    func_name="LISTAGG",
    sep: t.Optional[str] = ",",
    within_group: bool = True,
    on_overflow: bool = False,
) -> str:
    this = expression.this
    separator = self.sql(
        expression.args.get("separator") or (exp.Literal.string(sep) if sep else None)
    )

    on_overflow_sql = self.sql(expression, "on_overflow")
    on_overflow_sql = f" ON OVERFLOW {on_overflow_sql}" if (on_overflow and on_overflow_sql) else ""

    if isinstance(this, exp.Limit) and this.this:
        limit = this
        this = limit.this.pop()
    else:
        limit = None

    order = this.find(exp.Order)

    if order and order.this:
        this = order.this.pop()

    args = self.format_args(
        this, f"{separator}{on_overflow_sql}" if separator or on_overflow_sql else None
    )

    listagg: exp.Expression = exp.Anonymous(this=func_name, expressions=[args])

    modifiers = self.sql(limit)

    if order:
        if within_group:
            listagg = exp.WithinGroup(this=listagg, expression=order)
        else:
            modifiers = f"{self.sql(order)}{modifiers}"

    if modifiers:
        listagg.set("expressions", [f"{args}{modifiers}"])

    return self.sql(listagg)


def build_timetostr_or_tochar(args: t.List, dialect: DialectType) -> exp.TimeToStr | exp.ToChar:
    if len(args) == 2:
        this = args[0]
        if not this.type:
            from sqlglot.optimizer.annotate_types import annotate_types

            annotate_types(this, dialect=dialect)

        if this.is_type(*exp.DataType.TEMPORAL_TYPES):
            dialect_name = dialect.__class__.__name__.lower()
            return build_formatted_time(exp.TimeToStr, dialect_name, default=True)(args)

    return exp.ToChar.from_arg_list(args)


def build_replace_with_optional_replacement(args: t.List) -> exp.Replace:
    return exp.Replace(
        this=seq_get(args, 0),
        expression=seq_get(args, 1),
        replacement=seq_get(args, 2) or exp.Literal.string(""),
    )


def regexp_replace_global_modifier(expression: exp.RegexpReplace) -> exp.Expression | None:
    modifiers = expression.args.get("modifiers")
    single_replace = expression.args.get("single_replace")
    occurrence = expression.args.get("occurrence")

    if not single_replace and (not occurrence or (occurrence.is_int and occurrence.to_py() == 0)):
        if not modifiers or modifiers.is_string:
            # Append 'g' to the modifiers if they are not provided since
            # the semantics of REGEXP_REPLACE from the input dialect
            # is to replace all occurrences of the pattern.
            value = "" if not modifiers else modifiers.name
            modifiers = exp.Literal.string(value + "g")

    return modifiers


def getbit_sql(self: Generator, expression: exp.Getbit) -> str:
    """
    Generates SQL for Getbit according to DuckDB and Postgres, transpiling it if either:

    1. The zero index corresponds to the least-significant bit
    2. The input type is an integer value
    """
    value = expression.this
    position = expression.expression

    if not expression.args.get("zero_is_msb") and expression.is_type(
        *exp.DataType.SIGNED_INTEGER_TYPES, *exp.DataType.UNSIGNED_INTEGER_TYPES
    ):
        # Use bitwise operations: (value >> position) & 1
        shifted = exp.BitwiseRightShift(this=value, expression=position)
        masked = exp.BitwiseAnd(this=shifted, expression=exp.Literal.number(1))
        return self.sql(masked)

    return self.func("GET_BIT", value, position)
