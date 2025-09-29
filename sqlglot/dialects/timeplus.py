from __future__ import annotations
import typing as t
import datetime
from sqlglot import exp, generator, parser, tokens
from sqlglot._typing import E
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    arg_max_or_min_no_count,
    build_date_delta,
    build_formatted_time,
    inline_array_sql,
    json_extract_segments,
    json_path_key_only_name,
    length_or_char_length_sql,
    no_pivot_sql,
    build_json_extract_path,
    rename_func,
    remove_from_array_using_filter,
    sha256_sql,
    strposition_sql,
    var_map_sql,
    timestamptrunc_sql,
    unit_to_var,
    trim_sql,
)
from sqlglot.generator import Generator
from sqlglot.helper import is_int, seq_get
from sqlglot.tokens import Token, TokenType
from sqlglot.generator import unsupported_args

DATEΤΙΜΕ_DELTA = t.Union[exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd]


def _build_datetime_format(
    expr_type: t.Type[E],
) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        expr = build_formatted_time(expr_type, "timeplus")(args)

        timezone = seq_get(args, 2)
        if timezone:
            expr.set("zone", timezone)

        return expr

    return _builder


def _unix_to_time_sql(self: Timeplus.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("from_unix_timestamp", exp.cast(timestamp, exp.DataType.Type.BIGINT))
    if scale == exp.UnixToTime.MILLIS:
        return self.func(
            "from_unix_timestamp64_milli", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.MICROS:
        return self.func(
            "from_unix_timestamp64_micro", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.NANOS:
        return self.func(
            "from_unix_timestamp64_nano", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )

    return self.func(
        "from_unix_timestamp",
        exp.cast(
            exp.Div(this=timestamp, expression=exp.func("pow", 10, scale)),
            exp.DataType.Type.BIGINT,
        ),
    )


def _lower_func(sql: str) -> str:
    """Convert camelCase function names to snake_case for Timeplus."""
    index = sql.index("(")
    func_name = sql[:index]
    # Convert camelCase to snake_case
    import re

    func_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", func_name).lower()
    return func_name + sql[index:]


def _quantile_sql(self: Timeplus.Generator, expression: exp.Quantile) -> str:
    quantile = expression.args["quantile"]
    column = self.sql(expression, "this")

    if isinstance(quantile, exp.Array):
        # For multiple quantiles: quantiles(column, array)
        return self.func("quantiles", column, self.sql(quantile))
    else:
        # Standard Timeplus syntax: quantile(column, level)
        return self.func("quantile", column, quantile)


def _build_count_if(args: t.List) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="count_if", expressions=args)


def _build_str_to_date(args: t.List) -> exp.Cast | exp.Anonymous:
    if len(args) == 3:
        return exp.Anonymous(this="STR_TO_DATE", expressions=args)

    strtodate = exp.StrToDate.from_arg_list(args)
    return exp.cast(strtodate, exp.DataType.build(exp.DataType.Type.DATETIME))


def _datetime_delta_sql(name: str) -> t.Callable[[Generator, DATEΤΙΜΕ_DELTA], str]:
    def _delta_sql(self: Generator, expression: DATEΤΙΜΕ_DELTA) -> str:
        if not expression.unit:
            return rename_func(name)(self, expression)

        return self.func(
            name,
            unit_to_var(expression),
            expression.expression,
            expression.this,
            expression.args.get("zone"),
        )

    return _delta_sql


def _timestrtotime_sql(self: Timeplus.Generator, expression: exp.TimeStrToTime):
    ts = expression.this

    tz = expression.args.get("zone")
    if tz and isinstance(ts, exp.Literal):
        # Clickhouse will not accept timestamps that include a UTC offset, so we must remove them.
        # The first step to removing is parsing the string with `datetime.datetime.fromisoformat`.
        #
        # In python <3.11, `fromisoformat()` can only parse timestamps of millisecond (3 digit)
        # or microsecond (6 digit) precision. It will error if passed any other number of fractional
        # digits, so we extract the fractional seconds and pad to 6 digits before parsing.
        ts_string = ts.name.strip()

        # separate [date and time] from [fractional seconds and UTC offset]
        ts_parts = ts_string.split(".")
        if len(ts_parts) == 2:
            # separate fractional seconds and UTC offset
            offset_sep = "+" if "+" in ts_parts[1] else "-"
            ts_frac_parts = ts_parts[1].split(offset_sep)
            num_frac_parts = len(ts_frac_parts)

            # pad to 6 digits if fractional seconds present
            ts_frac_parts[0] = ts_frac_parts[0].ljust(6, "0")
            ts_string = "".join(
                [
                    ts_parts[0],  # date and time
                    ".",
                    ts_frac_parts[0],  # fractional seconds
                    offset_sep if num_frac_parts > 1 else "",
                    ts_frac_parts[1] if num_frac_parts > 1 else "",  # utc offset (if present)
                ]
            )

        # return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
        # this is because Clickhouse encodes the timezone as a data type parameter and throws an error if
        # it's part of the timestamp string
        ts_without_tz = (
            datetime.datetime.fromisoformat(ts_string).replace(tzinfo=None).isoformat(sep=" ")
        )
        ts = exp.Literal.string(ts_without_tz)

    # Non-nullable DateTime64 with microsecond precision
    expressions = [exp.DataTypeParam(this=tz)] if tz else []
    datatype = exp.DataType.build(
        exp.DataType.Type.DATETIME64,
        expressions=[exp.DataTypeParam(this=exp.Literal.number(6)), *expressions],
        nullable=False,
    )

    return self.sql(exp.cast(ts, datatype, dialect=self.dialect))


def _map_sql(self: Timeplus.Generator, expression: exp.Map | exp.VarMap) -> str:
    if not (expression.parent and expression.parent.arg_key == "settings"):
        return _lower_func(var_map_sql(self, expression))

    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return ""

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(f"{self.sql(key)}: {self.sql(value)}")

    csv_args = ", ".join(args)

    return f"{{{csv_args}}}"


class Timeplus(Dialect):
    # Timeplus streaming database - forked from ClickHouse but with isolated implementation
    INDEX_OFFSET = 1
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    LOG_BASE_FIRST: t.Optional[bool] = None
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    PRESERVE_ORIGINAL_NAMES = True
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    HEX_STRING_IS_INTEGER_TYPE = True
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    UNESCAPED_SEQUENCES = {
        "\\0": "\0",
    }

    # In Timeplus, DATABASE is DATABASE, not SCHEMA
    CREATABLE_KIND_MAPPING = {}

    SET_OP_DISTINCT_BY_DEFAULT: t.Dict[t.Type[exp.Expression], t.Optional[bool]] = {
        exp.Except: False,
        exp.Intersect: False,
        exp.Union: None,
    }

    def generate_values_aliases(self, expression: exp.Values) -> t.List[exp.Identifier]:
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
        # Copy from ClickHouse but with Timeplus-specific modifications
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
            "ATTACH": TokenType.COMMAND,
            "DATE32": TokenType.DATE32,
            "DATETIME64": TokenType.DATETIME64,
            "DICTIONARY": TokenType.DICTIONARY,
            "DYNAMIC": TokenType.DYNAMIC,
            "ENUM8": TokenType.ENUM8,
            "ENUM16": TokenType.ENUM16,
            "EXCHANGE": TokenType.COMMAND,
            "FINAL": TokenType.FINAL,
            "FIXED_STRING": TokenType.FIXEDSTRING,  # Timeplus native: fixed_string
            "STREAM": TokenType.TABLE,  # Timeplus uses STREAM instead of TABLE
            "MUTABLE": TokenType.TEMPORARY,  # For mutable streams
            "EXTERNAL": TokenType.FOREIGN_KEY,  # For external streams
            "RANDOM": TokenType.VOLATILE,  # For random streams
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "GLOBAL": TokenType.GLOBAL,
            "LOW_CARDINALITY": TokenType.LOWCARDINALITY,
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

        # Support $$ ... $$ for JavaScript/Python UDF body (PostgreSQL-style)
        HEREDOC_STRINGS = ["$"]
        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,  # Needed for heredoc string recognition
        }

    class Parser(parser.Parser):
        # In Timeplus, the following two queries do the same thing
        # * select x from t1 union all select x from t2 limit 1;
        # * select x from t1 union all (select x from t2 limit 1);
        MODIFIERS_ATTACHED_TO_SET_OP = False
        INTERVAL_SPANS = True
        OPTIONAL_ALIAS_TOKEN_CTE = False
        JOINS_HAVE_EQUAL_PRECEDENCE = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ANY": exp.AnyValue.from_arg_list,
            # Additional aggregate functions with mappings
            "TOP_K": lambda args: exp.Anonymous(this="top_k", expressions=args),
            "MIN_K": lambda args: exp.Anonymous(this="min_k", expressions=args),
            "MAX_K": lambda args: exp.Anonymous(this="max_k", expressions=args),
            "GROUP_CONCAT": lambda args: exp.Anonymous(this="group_concat", expressions=args),
            "GROUP_ARRAY_SORTED": lambda args: exp.Anonymous(
                this="group_array_sorted", expressions=args
            ),
            "MOVING_SUM": lambda args: exp.Anonymous(this="moving_sum", expressions=args),
            "AVG_TIME_WEIGHTED": lambda args: exp.Anonymous(
                this="avg_time_weighted", expressions=args
            ),
            "MEDIAN_TIME_WEIGHTED": lambda args: exp.Anonymous(
                this="median_time_weighted", expressions=args
            ),
            "HISTOGRAM": lambda args: exp.Anonymous(this="histogram", expressions=args),
            "KURT_POP": lambda args: exp.Anonymous(this="kurt_pop", expressions=args),
            "KURT_SAMP": lambda args: exp.Anonymous(this="kurt_samp", expressions=args),
            "LTTB": lambda args: exp.Anonymous(this="lttb", expressions=args),
            "LARGEST_TRIANGLE_THREE_BUCKETS": lambda args: exp.Anonymous(
                this="largest_triangle_three_buckets", expressions=args
            ),
            "P90": lambda args: exp.Anonymous(this="p90", expressions=args),
            "P95": lambda args: exp.Anonymous(this="p95", expressions=args),
            "P99": lambda args: exp.Anonymous(this="p99", expressions=args),
            "FIRST_VALUE": lambda args: exp.Anonymous(this="first_value", expressions=args),
            "LAST_VALUE": lambda args: exp.Anonymous(this="last_value", expressions=args),
            # Add snake_case versions for Timeplus
            "FROM_UNIXTIME": exp.UnixToTime.from_arg_list,
            "FROM_UNIX_TIMESTAMP": exp.UnixToTime.from_arg_list,
            "ARRAY_SUM": exp.ArraySum.from_arg_list,
            "ARRAY_REVERSE": exp.ArrayReverse.from_arg_list,
            "ARRAY_SLICE": exp.ArraySlice.from_arg_list,
            "ARRAY_JOIN": lambda args: exp.Explode(this=seq_get(args, 0)),
            "COUNT_IF": _build_count_if,
            "COUNT_DISTINCT": exp.Count.from_arg_list,  # Add count_distinct
            "COSINE_DISTANCE": exp.CosineDistance.from_arg_list,
            "DATE_ADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATEADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATE_DIFF": build_date_delta(exp.DateDiff, default_unit=None, supports_timezone=True),
            "DATEDIFF": build_date_delta(exp.DateDiff, default_unit=None, supports_timezone=True),
            "DATE_FORMAT": _build_datetime_format(exp.TimeToStr),
            "DATE_SUB": build_date_delta(exp.DateSub, default_unit=None),
            "DATESUB": build_date_delta(exp.DateSub, default_unit=None),
            "EARLIEST_TS": lambda args: exp.Anonymous(
                this="earliest_ts", expressions=args
            ),  # Add earliest_ts
            "EMIT_VERSION": lambda args: exp.Anonymous(
                this="emit_version", expressions=args
            ),  # Add emit_version
            "FORMAT_DATE_TIME": _build_datetime_format(exp.TimeToStr),
            "JSON_EXTRACT_STRING": build_json_extract_path(
                exp.JSONExtractScalar, zero_based_indexing=False
            ),
            "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
            "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
            "MAP": parser.build_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "PARSE_DATE_TIME": _build_datetime_format(exp.ParseDatetime),
            "RAND_CANONICAL": exp.Rand.from_arg_list,
            "STR_TO_DATE": _build_str_to_date,
            "TABLE": lambda args: exp.Anonymous(
                this="table", expressions=args
            ),  # Historical query function
            "TIMESTAMP_SUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMPSUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMP_ADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "TIMESTAMPADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "TUMBLE": lambda args: exp.Anonymous(
                this="tumble", expressions=args
            ),  # Window function for streaming
            "HOP": lambda args: exp.Anonymous(
                this="hop", expressions=args
            ),  # Window function for streaming
            "SESSION": lambda args: exp.Anonymous(
                this="session", expressions=args
            ),  # Session window function
            "DEDUP": lambda args: exp.Anonymous(
                this="dedup", expressions=args
            ),  # Deduplication function
            "LAG": exp.Lag.from_arg_list,
            "LAGS": lambda args: exp.Anonymous(
                this="lags", expressions=args
            ),  # Multiple lag values
            "DATE_DIFF_WITHIN": lambda args: exp.Anonymous(
                this="date_diff_within", expressions=args
            ),  # Range join function
            "LAG_BEHIND": lambda args: exp.Anonymous(
                this="lag_behind", expressions=args
            ),  # Streaming join function
            "LATEST": lambda args: exp.Anonymous(
                this="latest", expressions=args
            ),  # Latest value aggregation
            "EARLIEST": lambda args: exp.Anonymous(
                this="earliest", expressions=args
            ),  # Earliest value aggregation
            "CHANGELOG": lambda args: exp.Anonymous(
                this="changelog", expressions=args
            ),  # Convert to changelog stream
            "ROWIFY": lambda args: exp.Anonymous(
                this="rowify", expressions=args
            ),  # Split batch to rows
            "UNIQ": exp.ApproxDistinct.from_arg_list,
            "UNIQUE_EXACT": exp.Count.from_arg_list,  # Add unique_exact
            "UNIQUE_EXACT_IF": lambda args: exp.Anonymous(
                this="unique_exact_if", expressions=args
            ),  # Add unique_exact_if as anonymous
            "XOR": lambda args: exp.Xor(expressions=args),
            "MD5": exp.MD5Digest.from_arg_list,
            "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
            "SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
            "SUBSTRING_INDEX": exp.SubstringIndex.from_arg_list,
            "TO_TYPE_NAME": exp.Typeof.from_arg_list,
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "LEVENSHTEIN_DISTANCE": exp.Levenshtein.from_arg_list,
            # Additional date/time functions (FROM_UNIX_TIMESTAMP already defined above)
            "FROM_UNIX_TIMESTAMP64_MILLI": lambda args: exp.Anonymous(
                this="from_unix_timestamp64_milli", expressions=args
            ),
            "FROM_UNIX_TIMESTAMP64_MICRO": lambda args: exp.Anonymous(
                this="from_unix_timestamp64_micro", expressions=args
            ),
            "FROM_UNIX_TIMESTAMP64_NANO": lambda args: exp.Anonymous(
                this="from_unix_timestamp64_nano", expressions=args
            ),
            "TO_UNIX_TIMESTAMP": exp.TimeToUnix.from_arg_list,
            "TO_UNIX_TIMESTAMP64_MILLI": lambda args: exp.Anonymous(
                this="to_unix_timestamp64_milli", expressions=args
            ),
            "TO_UNIX_TIMESTAMP64_MICRO": lambda args: exp.Anonymous(
                this="to_unix_timestamp64_micro", expressions=args
            ),
            "TO_UNIX_TIMESTAMP64_NANO": lambda args: exp.Anonymous(
                this="to_unix_timestamp64_nano", expressions=args
            ),
            # Text functions
            "SPLIT_BY_STRING": lambda args: exp.Anonymous(this="split_by_string", expressions=args),
            "MULTI_SEARCH_ANY": lambda args: exp.Anonymous(
                this="multi_search_any", expressions=args
            ),
            "REPLACE_ONE": lambda args: exp.Anonymous(this="replace_one", expressions=args),
            "REPLACE_REGEX": exp.RegexpReplace.from_arg_list,
            "EXTRACT_ALL_GROUPS": lambda args: exp.Anonymous(
                this="extract_all_groups", expressions=args
            ),
            # JSON functions using snake_case
            "JSON_EXTRACT_INT": lambda args: exp.Anonymous(
                this="json_extract_int", expressions=args
            ),
            "JSON_EXTRACT_FLOAT": lambda args: exp.Anonymous(
                this="json_extract_float", expressions=args
            ),
            "JSON_EXTRACT_BOOL": lambda args: exp.Anonymous(
                this="json_extract_bool", expressions=args
            ),
            "JSON_EXTRACT_ARRAY": lambda args: exp.Anonymous(
                this="json_extract_array", expressions=args
            ),
            "JSON_EXTRACT_KEYS": lambda args: exp.Anonymous(
                this="json_extract_keys", expressions=args
            ),
            "IS_VALID_JSON": lambda args: exp.Anonymous(this="is_valid_json", expressions=args),
            "JSON_HAS": lambda args: exp.Anonymous(this="json_has", expressions=args),
        }
        FUNCTIONS.pop("TRANSFORM")
        FUNCTIONS.pop("APPROX_TOP_SUM")

        # Timeplus uses snake_case for all aggregate functions
        AGG_FUNCTIONS = {
            "count",
            "min",
            "max",
            "sum",
            "avg",
            "any",
            "stddev_pop",
            "stddev_samp",
            "var_pop",
            "var_samp",
            "corr",
            "covar_pop",
            "covar_samp",
            "entropy",
            "exponential_moving_average",
            "interval_length_sum",
            "kolmogorov_smirnov_test",
            "mann_whitney_u_test",
            "median",
            "rank_corr",
            "sum_kahan",
            "student_t_test",
            "welch_t_test",
            "any_heavy",
            "any_last",
            "bounding_ratio",
            "first_value",
            "last_value",
            "arg_min",
            "arg_max",
            "avg_weighted",
            "top_k",
            "approx_top_sum",
            "top_k_weighted",
            "delta_sum",
            "delta_sum_timestamp",
            "group_array",
            "group_array_last",
            "group_uniq_array",
            "group_array_insert_at",
            "group_array_moving_avg",
            "group_array_moving_sum",
            "group_array_sample",
            "group_bit_and",
            "group_bit_or",
            "group_bit_xor",
            "group_bitmap",
            "group_bitmap_and",
            "group_bitmap_or",
            "group_bitmap_xor",
            "sum_with_overflow",
            # Additional aggregation functions
            "count_if",
            "count_distinct",
            "unique",
            "unique_exact",
            "unique_exact_if",
            "top_k",
            "min_k",
            "max_k",
            "group_concat",
            "group_array_sorted",
            "moving_sum",
            "avg_time_weighted",
            "median_time_weighted",
            "histogram",
            "kurt_pop",
            "kurt_samp",
            "lttb",
            "largest_triangle_three_buckets",
            "p90",
            "p95",
            "p99",
            "sum_map",
            "min_map",
            "max_map",
            "skew_samp",
            "skew_pop",
            "kurt_samp",
            "kurt_pop",
            "uniq",
            "uniq_exact",
            "uniq_combined",
            "uniq_combined64",
            "uniq_hll12",
            "uniq_theta",
            "quantile",
            "quantiles",
            "quantile_exact",
            "quantiles_exact",
            "quantile_exact_low",
            "quantiles_exact_low",
            "quantile_exact_high",
            "quantiles_exact_high",
            "quantile_exact_weighted",
            "quantiles_exact_weighted",
            "quantile_timing",
            "quantiles_timing",
            "quantile_timing_weighted",
            "quantiles_timing_weighted",
            "quantile_deterministic",
            "quantiles_deterministic",
            "quantile_t_digest",
            "quantiles_t_digest",
            "quantile_t_digest_weighted",
            "quantiles_t_digest_weighted",
            "simple_linear_regression",
            "stochastic_linear_regression",
            "stochastic_logistic_regression",
            "categorical_information_value",
            "contingency",
            "cramers_v",
            "cramers_v_bias_corrected",
            "theils_u",
            "max_intersections",
            "max_intersections_position",
            "mean_z_test",
            "quantile_interpolated_weighted",
            "quantiles_interpolated_weighted",
            "quantile_gk",
            "quantiles_gk",
            "spark_bar",
            "sum_count",
            "largest_triangle_three_buckets",
            "histogram",
            "sequence_match",
            "sequence_count",
            "window_funnel",
            "retention",
            "uniq_up_to",
            "sequence_next_node",
            "exponential_time_decayed_avg",
        }

        # Timeplus uses snake_case for all suffixes
        AGG_FUNCTIONS_SUFFIXES = [
            "_if",
            "_array",
            "_array_if",
            "_map",
            "_simple_state",
            "_state",
            "_merge",
            "_merge_state",
            "_for_each",
            "_distinct",
            "_or_default",
            "_or_null",
            "_resample",
            "_arg_min",
            "_arg_max",
        ]

        FUNC_TOKENS = {
            *parser.Parser.FUNC_TOKENS,
            TokenType.AND,
            TokenType.OR,
            TokenType.SET,
        }

        RESERVED_TOKENS = parser.Parser.RESERVED_TOKENS - {TokenType.SELECT}

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,
            TokenType.LIKE,
        }

        AGG_FUNC_MAPPING = (
            lambda functions, suffixes: {
                f"{f}{sfx}": (f, sfx) for sfx in (suffixes + [""]) for f in functions
            }
        )(AGG_FUNCTIONS, AGG_FUNCTIONS_SUFFIXES)

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "QUANTILE": lambda self: self._parse_quantile(),
            "MEDIAN": lambda self: self._parse_quantile(),
            "COLUMNS": lambda self: self._parse_columns(),
            "TUPLE": lambda self: exp.Struct.from_arg_list(self._parse_function_args(alias=True)),
        }

        FUNCTION_PARSERS.pop("MATCH")

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "ENGINE": lambda self: self._parse_engine_property(),
        }
        PROPERTY_PARSERS.pop("DYNAMIC")

        NO_PAREN_FUNCTION_PARSERS = parser.Parser.NO_PAREN_FUNCTION_PARSERS.copy()
        NO_PAREN_FUNCTION_PARSERS.pop("ANY")

        NO_PAREN_FUNCTIONS = parser.Parser.NO_PAREN_FUNCTIONS.copy()
        NO_PAREN_FUNCTIONS.pop(TokenType.CURRENT_TIMESTAMP)

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.GLOBAL: lambda self, this: self._parse_global_in(this),
        }

        # The PLACEHOLDER entry is popped because 1) it doesn't affect Clickhouse (it corresponds to
        # the postgres-specific JSONBContains parser) and 2) it makes parsing the ternary op simpler.
        COLUMN_OPERATORS = parser.Parser.COLUMN_OPERATORS.copy()
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        JOIN_KINDS = {
            *parser.Parser.JOIN_KINDS,
            TokenType.ANY,
            TokenType.ASOF,
            TokenType.ARRAY,
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.ANY,
            TokenType.ARRAY,
            TokenType.FINAL,
            TokenType.FORMAT,
            TokenType.SETTINGS,
        }

        ALIAS_TOKENS = parser.Parser.ALIAS_TOKENS - {
            TokenType.FORMAT,
        }

        LOG_DEFAULTS_TO_LN = True

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            TokenType.SETTINGS: lambda self: (
                "settings",
                self._advance() or self._parse_csv(self._parse_assignment),
            ),
            TokenType.FORMAT: lambda self: (
                "format",
                self._advance() or self._parse_id_var(),
            ),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "INDEX": lambda self: self._parse_index_constraint(),
            "CODEC": lambda self: self._parse_compress(),
        }

        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,
            "REPLACE": lambda self: self._parse_alter_table_replace(),
        }

        SCHEMA_UNNAMED_CONSTRAINTS = {
            *parser.Parser.SCHEMA_UNNAMED_CONSTRAINTS,
            "INDEX",
        }

        PLACEHOLDER_PARSERS = {
            **parser.Parser.PLACEHOLDER_PARSERS,
            TokenType.L_BRACE: lambda self: self._parse_query_parameter(),
        }

        def _parse_engine_property(self) -> exp.EngineProperty:
            self._match(TokenType.EQ)
            return self.expression(
                exp.EngineProperty,
                this=self._parse_field(any_token=True, anonymous_func=True),
            )

        # https://clickhouse.com/docs/en/sql-reference/statements/create/function
        def _parse_user_defined_function_expression(self) -> t.Optional[exp.Expression]:
            return self._parse_lambda()

        def _parse_types(
            self,
            check_func: bool = False,
            schema: bool = False,
            allow_identifiers: bool = True,
        ) -> t.Optional[exp.Expression]:
            dtype = super()._parse_types(
                check_func=check_func,
                schema=schema,
                allow_identifiers=allow_identifiers,
            )
            # In Timeplus, nullable is lowercase but we keep the marking for proper conversion
            if isinstance(dtype, exp.DataType) and dtype.args.get("nullable") is not True:
                # Mark as non-nullable by default unless explicitly specified
                dtype.set("nullable", False)

            return dtype

        def _parse_extract(self) -> exp.Extract | exp.Anonymous:
            index = self._index
            this = self._parse_bitwise()
            if self._match(TokenType.FROM):
                self._retreat(index)
                return super()._parse_extract()

            # We return Anonymous here because extract and regexpExtract have different semantics,
            # so parsing extract(foo, bar) into RegexpExtract can potentially break queries. E.g.,
            # `extract('foobar', 'b')` works, but Timeplus crashes for `regexpExtract('foobar', 'b')`.
            #
            # TODO: can we somehow convert the former into an equivalent `regexpExtract` call?
            self._match(TokenType.COMMA)
            return self.expression(
                exp.Anonymous, this="extract", expressions=[this, self._parse_bitwise()]
            )

        def _parse_assignment(self) -> t.Optional[exp.Expression]:
            this = super()._parse_assignment()

            if self._match(TokenType.PLACEHOLDER):
                return self.expression(
                    exp.If,
                    this=this,
                    true=self._parse_assignment(),
                    false=self._match(TokenType.COLON) and self._parse_assignment(),
                )

            return this

        def _parse_query_parameter(self) -> t.Optional[exp.Expression]:
            """
            Parse a placeholder expression like SELECT {abc: UInt32} or FROM {table: Identifier}
            https://clickhouse.com/docs/en/sql-reference/syntax#defining-and-using-query-parameters
            """
            index = self._index

            this = self._parse_id_var()
            self._match(TokenType.COLON)
            kind = self._parse_types(check_func=False, allow_identifiers=False) or (
                self._match_text_seq("IDENTIFIER") and "Identifier"
            )

            if not kind:
                self._retreat(index)
                return None
            elif not self._match(TokenType.R_BRACE):
                self.raise_error("Expecting }")

            if isinstance(this, exp.Identifier) and not this.quoted:
                this = exp.var(this.name)

            return self.expression(exp.Placeholder, this=this, kind=kind)

        def _parse_bracket(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            l_brace = self._match(TokenType.L_BRACE, advance=False)
            bracket = super()._parse_bracket(this)

            if l_brace and isinstance(bracket, exp.Struct):
                varmap = exp.VarMap(keys=exp.Array(), values=exp.Array())
                for expression in bracket.expressions:
                    if not isinstance(expression, exp.PropertyEQ):
                        break

                    varmap.args["keys"].append("expressions", exp.Literal.string(expression.name))
                    varmap.args["values"].append("expressions", expression.expression)

                return varmap

            return bracket

        def _parse_in(self, this: t.Optional[exp.Expression], is_global: bool = False) -> exp.In:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_global_in(self, this: t.Optional[exp.Expression]) -> exp.Not | exp.In:
            is_negated = self._match(TokenType.NOT)
            this = self._match(TokenType.IN) and self._parse_in(this, is_global=True)
            return self.expression(exp.Not, this=this) if is_negated else this

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
            parse_partition: bool = False,
            consume_pipe: bool = False,
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_table(
                schema=schema,
                joins=joins,
                alias_tokens=alias_tokens,
                parse_bracket=parse_bracket,
                is_db_reference=is_db_reference,
            )

            if isinstance(this, exp.Table):
                inner = this.this
                alias = this.args.get("alias")

                if isinstance(inner, exp.GenerateSeries) and alias and not alias.columns:
                    alias.set("columns", [exp.to_identifier("generate_series")])

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
            return super()._parse_position(haystack_first=True)

        # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
        def _parse_cte(self) -> t.Optional[exp.CTE]:
            # WITH <identifier> AS <subquery expression>
            cte: t.Optional[exp.CTE] = self._try_parse(super()._parse_cte)

            if not cte:
                # WITH <expression> AS <identifier>
                cte = self.expression(
                    exp.CTE,
                    this=self._parse_assignment(),
                    alias=self._parse_table_alias(),
                    scalar=True,
                )

            return cte

        def _parse_join_parts(
            self,
        ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
            is_global = self._match(TokenType.GLOBAL) and self._prev
            kind_pre = self._match_set(self.JOIN_KINDS, advance=False) and self._prev

            if kind_pre:
                kind = self._match_set(self.JOIN_KINDS) and self._prev
                side = self._match_set(self.JOIN_SIDES) and self._prev
                return is_global, side, kind

            return (
                is_global,
                self._match_set(self.JOIN_SIDES) and self._prev,
                self._match_set(self.JOIN_KINDS) and self._prev,
            )

        def _parse_join(
            self, skip_join_token: bool = False, parse_bracket: bool = False
        ) -> t.Optional[exp.Join]:
            join = super()._parse_join(skip_join_token=skip_join_token, parse_bracket=True)
            if join:
                join.set("global", join.args.pop("method", None))

                # tbl ARRAY JOIN arr <-- this should be a `Column` reference, not a `Table`
                # https://clickhouse.com/docs/en/sql-reference/statements/select/array-join
                if join.kind == "ARRAY":
                    for table in join.find_all(exp.Table):
                        table.replace(table.to_column())

            return join

        def _parse_function(
            self,
            functions: t.Optional[t.Dict[str, t.Callable]] = None,
            anonymous: bool = False,
            optional_parens: bool = True,
            any_token: bool = False,
        ) -> t.Optional[exp.Expression]:
            expr = super()._parse_function(
                functions=functions,
                anonymous=anonymous,
                optional_parens=optional_parens,
                any_token=any_token,
            )

            func = expr.this if isinstance(expr, exp.Window) else expr

            # Aggregate functions can be split in 2 parts: <func_name><suffix>
            parts = (
                self.AGG_FUNC_MAPPING.get(func.this) if isinstance(func, exp.Anonymous) else None
            )

            if parts:
                anon_func: exp.Anonymous = t.cast(exp.Anonymous, func)
                params = self._parse_func_params(anon_func)

                kwargs = {
                    "this": anon_func.this,
                    "expressions": anon_func.expressions,
                }
                if parts[1]:
                    exp_class: t.Type[exp.Expression] = (
                        exp.CombinedParameterizedAgg if params else exp.CombinedAggFunc
                    )
                else:
                    exp_class = exp.ParameterizedAgg if params else exp.AnonymousAggFunc

                kwargs["exp_class"] = exp_class
                if params:
                    kwargs["params"] = params

                func = self.expression(**kwargs)

                if isinstance(expr, exp.Window):
                    # The window's func was parsed as Anonymous in base parser, fix its
                    # type to be Timeplus style CombinedAnonymousAggFunc / AnonymousAggFunc
                    expr.set("this", func)
                elif params:
                    # Params have blocked super()._parse_function() from parsing the following window
                    # (if that exists) as they're standing between the function call and the window spec
                    expr = self._parse_window(func)
                else:
                    expr = func

            return expr

        def _parse_func_params(
            self, this: t.Optional[exp.Func] = None
        ) -> t.Optional[t.List[exp.Expression]]:
            if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
                return self._parse_csv(self._parse_lambda)

            if self._match(TokenType.L_PAREN):
                params = self._parse_csv(self._parse_lambda)
                self._match_r_paren(this)
                return params

            return None

        def _parse_quantile(self) -> exp.Quantile:
            # Timeplus uses standard function syntax: quantile(column, level)
            # Not ClickHouse's syntax: quantile(level)(column)
            # When this function is called, we're already past "quantile("
            # and need to parse the arguments. The closing paren is handled by the framework.
            this = self._parse_expression()

            if self._match(TokenType.COMMA):
                quantile = self._parse_expression()
            else:
                # median() or quantile with default 0.5
                quantile = exp.Literal.number(0.5)

            # Don't consume the closing paren - the framework handles it
            return self.expression(exp.Quantile, this=this, quantile=quantile)

        def _parse_wrapped_id_vars(self, optional: bool = False) -> t.List[exp.Expression]:
            return super()._parse_wrapped_id_vars(optional=True)

        def _parse_primary_key(
            self, wrapped_optional: bool = False, in_props: bool = False
        ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
            return super()._parse_primary_key(
                wrapped_optional=wrapped_optional or in_props, in_props=in_props
            )

        def _parse_on_property(self) -> t.Optional[exp.Expression]:
            # Timeplus doesn't support ON CLUSTER
            return None

        def _parse_index_constraint(
            self, kind: t.Optional[str] = None
        ) -> exp.IndexColumnConstraint:
            # INDEX name1 expr TYPE type1(args) GRANULARITY value
            this = self._parse_id_var()
            expression = self._parse_assignment()

            index_type = self._match_text_seq("TYPE") and (
                self._parse_function() or self._parse_var()
            )

            granularity = self._match_text_seq("GRANULARITY") and self._parse_term()

            return self.expression(
                exp.IndexColumnConstraint,
                this=this,
                expression=expression,
                index_type=index_type,
                granularity=granularity,
            )

        def _parse_partition(self) -> t.Optional[exp.Partition]:
            # https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
            if not self._match(TokenType.PARTITION):
                return None

            if self._match_text_seq("ID"):
                # Corresponds to the PARTITION ID <string_value> syntax
                expressions: t.List[exp.Expression] = [
                    self.expression(exp.PartitionId, this=self._parse_string())
                ]
            else:
                expressions = self._parse_expressions()

            return self.expression(exp.Partition, expressions=expressions)

        def _parse_alter_table_replace(self) -> t.Optional[exp.Expression]:
            partition = self._parse_partition()

            if not partition or not self._match(TokenType.FROM):
                return None

            return self.expression(
                exp.ReplacePartition,
                expression=partition,
                source=self._parse_table_parts(),
            )

        def _parse_constraint(self) -> t.Optional[exp.Expression]:
            # Timeplus doesn't support PROJECTION
            return super()._parse_constraint()

        def _parse_alias(
            self, this: t.Optional[exp.Expression], explicit: bool = False
        ) -> t.Optional[exp.Expression]:
            # In clickhouse "SELECT <expr> APPLY(...)" is a query modifier,
            # so "APPLY" shouldn't be parsed as <expr>'s alias. However, "SELECT <expr> apply" is a valid alias
            if self._match_pair(TokenType.APPLY, TokenType.L_PAREN, advance=False):
                return this

            return super()._parse_alias(this=this, explicit=explicit)

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            this = super()._parse_expression()

            # Clickhouse allows "SELECT <expr> [APPLY(func)] [...]]" modifier
            while self._match_pair(TokenType.APPLY, TokenType.L_PAREN):
                this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
                self._match(TokenType.R_PAREN)

            return this

        def _parse_columns(self) -> exp.Expression:
            this: exp.Expression = self.expression(exp.Columns, this=self._parse_lambda())

            while self._next and self._match_text_seq(")", "APPLY", "("):
                self._match(TokenType.R_PAREN)
                this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
            return this

        def _parse_var_or_string(self, upper: bool = False) -> t.Optional[exp.Expression]:
            """Parse a variable or string literal."""
            if self._match(TokenType.STRING):
                return exp.Literal.string(self._prev.text)
            return self._parse_var(upper=upper)

        def _parse_heredoc(self) -> t.Optional[exp.Heredoc]:
            """Parse a heredoc string ($$...$$)."""
            # In ClickHouse/Timeplus, heredoc strings are parsed as HEREDOC_STRING tokens
            if self._curr and self._curr.token_type == TokenType.HEREDOC_STRING:
                text = self._curr.text
                self._advance()
                return self.expression(exp.Heredoc, this=text)
            # Alternative: parse between $$ markers
            if self._match(TokenType.HEREDOC_STRING):
                return self.expression(exp.Heredoc, this=self._prev.text)
            return None

        def _parse_value(self, values: bool = True) -> t.Optional[exp.Tuple]:
            value = super()._parse_value(values=values)
            if not value:
                return None

            # In Clickhouse "SELECT * FROM VALUES (1, 2, 3)" generates a table with a single column, in contrast
            # to other dialects. For this case, we canonicalize the values into a tuple-of-tuples AST if it's not already one.
            # In INSERT INTO statements the same clause actually references multiple columns (opposite semantics),
            # but the final result is not altered by the extra parentheses.
            # Note: Clickhouse allows VALUES([structure], value, ...) so the branch checks for the last expression
            expressions = value.expressions
            if values and not isinstance(expressions[-1], exp.Tuple):
                value.set(
                    "expressions",
                    [self.expression(exp.Tuple, expressions=[expr]) for expr in expressions],
                )

            return value

        def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
            # Timeplus allows custom expressions as partition key
            # https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key
            return self.expression(
                exp.PartitionedByProperty,
                this=self._parse_assignment(),
            )

        def _parse_create(self) -> exp.Create | exp.Command:
            """Parse CREATE statements, handling Timeplus-specific STREAM types."""
            # Save the start position
            start_index = self._index
            # Initialize replace flag
            replace = False

            # Check for EXTERNAL TABLE first
            if self._match_text_seq("EXTERNAL", "TABLE"):
                # This is CREATE EXTERNAL TABLE - treat as a regular CREATE with external flag
                exists = self._parse_exists(not_=True)
                this = self._parse_table(schema=True)

                # External tables don't have column definitions, just SETTINGS
                settings = None
                if self._match(TokenType.SETTINGS):
                    settings = self._parse_csv(self._parse_assignment)

                # Create a normal Create expression with external flag
                create = self.expression(
                    exp.Create,
                    this=this,
                    kind="EXTERNAL TABLE",
                    replace=False,
                    exists=exists,
                )

                if settings:
                    create.set("settings", settings)

                return create

            # Check for REMOTE FUNCTION
            if self._match_text_seq("REMOTE"):
                if self._match(TokenType.FUNCTION):
                    # For CREATE REMOTE FUNCTION, preserve it as a Create with special kind
                    # Parse function definition
                    exists = self._parse_exists(not_=True)
                    this = self._parse_user_defined_function()

                    # Parse RETURNS clause
                    returns = None
                    if self._match_text_seq("RETURNS"):
                        returns = self._parse_types()

                    # CREATE REMOTE FUNCTION specific properties
                    # URL, AUTH_METHOD, AUTH_HEADER, AUTH_KEY are handled as properties
                    remote_props = []
                    if self._match_text_seq("URL"):
                        url_value = self._parse_string()
                        remote_props.append(exp.Property(this=exp.var("URL"), value=url_value))

                    if self._match_text_seq("AUTH_METHOD"):
                        auth_method = self._parse_string()
                        remote_props.append(
                            exp.Property(this=exp.var("AUTH_METHOD"), value=auth_method)
                        )

                    if self._match_text_seq("AUTH_HEADER"):
                        auth_header = self._parse_string()
                        remote_props.append(
                            exp.Property(this=exp.var("AUTH_HEADER"), value=auth_header)
                        )

                    if self._match_text_seq("AUTH_KEY"):
                        auth_key = self._parse_string()
                        remote_props.append(exp.Property(this=exp.var("AUTH_KEY"), value=auth_key))

                    create = self.expression(
                        exp.Create,
                        this=this,
                        kind="REMOTE FUNCTION",
                        exists=exists,
                    )

                    if returns:
                        create.set("returns", returns)

                    if remote_props:
                        create.set("properties", exp.Properties(expressions=remote_props))

                    return create
                else:
                    # REMOTE but not FUNCTION - go back
                    self._retreat(self._index - 1)

            # Special handling for MATERIALIZED VIEW with INTO clause
            if self._match_text_seq("MATERIALIZED", "VIEW"):
                # Parse it manually to handle INTO clause and SETTINGS
                exists = self._parse_exists(not_=True)
                # Parse just the view name identifier
                view_name = self._parse_table_parts()

                # Check for INTO clause
                into_table = None
                if self._match(TokenType.INTO):
                    # Parse just the target table identifier
                    into_table = self._parse_table_parts()

                # Now parse AS SELECT
                if not self._match(TokenType.ALIAS):
                    self.raise_error("Expected AS after view definition")

                # Parse the SELECT expression
                expression = self._parse_ddl_select()

                # Parse SETTINGS if present after AS SELECT (only valid syntax)
                settings = None
                if self._match(TokenType.SETTINGS):
                    settings = self._parse_csv(self._parse_assignment)

                # Create the expression
                create = self.expression(
                    exp.Create,
                    this=view_name,
                    kind="MATERIALIZED VIEW",
                    expression=expression,
                    exists=exists,
                )

                if into_table:
                    create.set("into", into_table)

                if settings:
                    create.set("settings", settings)

                return create

            # Check for MUTABLE, EXTERNAL, or RANDOM keywords for streams
            stream_type = None
            if self._match(TokenType.TEMPORARY):  # We mapped MUTABLE to TEMPORARY
                stream_type = "MUTABLE"
            elif self._match(TokenType.FOREIGN_KEY):  # We mapped EXTERNAL to FOREIGN_KEY
                stream_type = "EXTERNAL"
            elif self._match(TokenType.VOLATILE):  # We mapped RANDOM to VOLATILE
                stream_type = "RANDOM"

            # If we have a stream type or next is STREAM/TABLE, continue parsing
            if stream_type or self._curr.token_type == TokenType.TABLE:
                # Match the TABLE token (which represents STREAM in Timeplus)
                if not self._match(TokenType.TABLE):
                    # No TABLE/STREAM keyword, go back and use parent
                    self._retreat(start_index)
                    return super()._parse_create()

                # Parse it like ClickHouse would, but we'll convert TABLE to STREAM
                exists = self._parse_exists(not_=True)
                this = self._parse_table(schema=True)

                # Parse schema or AS SELECT
                expression = self._parse_schema() if self._match(TokenType.L_PAREN) else None

                # Parse PRIMARY KEY if present
                primary_key = None
                if self._match(TokenType.PRIMARY_KEY):
                    primary_key = self._parse_wrapped_csv(self._parse_field)

                # Parse TTL if present
                ttl = None
                if self._match_text_seq("TTL"):
                    ttl = self._parse_bitwise()

                # Parse properties (including COMMENT)
                properties = self._parse_properties()

                # Build the Create expression
                create = self.expression(
                    exp.Create,
                    this=this,
                    kind="STREAM",
                    replace=False,
                    exists=exists,
                    expression=expression,
                    properties=properties,
                )

                # Store stream type info
                if stream_type:
                    create.set("stream_type", stream_type)

                if primary_key:
                    create.set(
                        "primary_key",
                        self.expression(exp.PrimaryKey, expressions=primary_key, options=[]),
                    )

                if ttl:
                    create.set("ttl", ttl)

                return create

            # Check for OR REPLACE followed by FORMAT SCHEMA
            if self._match_text_seq("OR", "REPLACE"):
                replace = True

            # Check for FORMAT SCHEMA
            if self._match_text_seq("FORMAT", "SCHEMA"):
                # FORMAT SCHEMA confirmed
                exists = self._parse_exists(not_=True)
                this = self._parse_table_parts(is_db_reference=False)  # schema name

                # Parse AS clause
                if not self._match(TokenType.ALIAS):
                    self.raise_error("Expected AS after FORMAT SCHEMA name")

                # Parse the schema content (string literal or heredoc)
                schema_content: t.Optional[exp.Expression] = None
                if self._match(TokenType.STRING):
                    schema_content = exp.Literal.string(self._prev.text)
                elif self._match(TokenType.HEREDOC_STRING):
                    schema_content = exp.Literal.string(self._prev.text)
                else:
                    # Try to parse as an expression
                    schema_content = self._parse_assignment()

                # Parse TYPE clause
                schema_type = None
                if self._match_text_seq("TYPE"):
                    schema_type = self._parse_var()

                # Build the Create expression
                create = self.expression(
                    exp.Create,
                    this=this,
                    kind="FORMAT SCHEMA",
                    replace=replace,
                    exists=exists,
                    expression=schema_content,
                )

                if schema_type:
                    create.set("schema_type", schema_type)

                return create

            # Check for DICTIONARY
            if self._match_text_seq("DICTIONARY"):
                # Parse CREATE DICTIONARY properly
                exists = self._parse_exists(not_=True)
                this = self._parse_table(schema=True)

                # Parse schema
                expression = self._parse_schema() if self._match(TokenType.L_PAREN) else None

                # Parse PRIMARY KEY
                primary_key = None
                if self._match(TokenType.PRIMARY_KEY):
                    # PRIMARY KEY can be either wrapped or unwrapped for dictionaries
                    if self._match(TokenType.L_PAREN):
                        primary_key = self._parse_csv(self._parse_column)
                        self._match(TokenType.R_PAREN)
                    else:
                        # Single column without parens
                        col = self._parse_column()
                        primary_key = [col] if col else []

                # Parse SOURCE, LAYOUT, LIFETIME
                source = None
                layout = None
                lifetime = None

                while self._curr:
                    if self._match_text_seq("SOURCE"):
                        # Parse SOURCE(...) clause
                        if self._match(TokenType.L_PAREN):
                            # Parse the source type (HTTP, MYSQL, TIMEPLUS, etc.)
                            source_type = self._parse_var()
                            # Parse source parameters
                            if self._match(TokenType.L_PAREN):
                                source_params = self._parse_csv(self._parse_assignment)
                                self._match(TokenType.R_PAREN)
                                source = self.expression(
                                    exp.Anonymous,
                                    this=source_type.name
                                    if isinstance(source_type, exp.Identifier)
                                    else str(source_type),
                                    expressions=source_params,
                                )
                            else:
                                source = source_type  # type: ignore
                            self._match(TokenType.R_PAREN)
                    elif self._match_text_seq("LAYOUT"):
                        # Parse LAYOUT(...) clause
                        if self._match(TokenType.L_PAREN):
                            # Parse layout type (HASHED, FLAT, IP_TRIE, etc.)
                            layout_type = self._parse_var()
                            # Parse layout parameters if they exist
                            if self._match(TokenType.L_PAREN):
                                layout_params = self._parse_csv(self._parse_assignment)
                                self._match(TokenType.R_PAREN)
                                layout = self.expression(
                                    exp.Anonymous,
                                    this=layout_type.name
                                    if isinstance(layout_type, exp.Identifier)
                                    else str(layout_type),
                                    expressions=layout_params,
                                )
                            else:
                                # Layout without parameters
                                layout = layout_type  # type: ignore
                            self._match(TokenType.R_PAREN)
                    elif self._match_text_seq("LIFETIME"):
                        # Parse LIFETIME(...) clause
                        if self._match(TokenType.L_PAREN):
                            # Parse MIN and MAX or just a single value
                            if self._match_text_seq("MIN"):
                                min_val = self._parse_number()
                                max_val = None
                                if self._match_text_seq("MAX"):
                                    max_val = self._parse_number()
                                if max_val:
                                    lifetime = self.expression(
                                        exp.Anonymous,
                                        this="LIFETIME",
                                        expressions=[
                                            exp.Property(this=exp.var("MIN"), value=min_val),
                                            exp.Property(this=exp.var("MAX"), value=max_val),
                                        ],
                                    )
                                else:
                                    lifetime = self.expression(
                                        exp.Anonymous,
                                        this="LIFETIME",
                                        expressions=[min_val] if min_val else [],
                                    )
                            else:
                                # Single value lifetime
                                lifetime_val = self._parse_number()
                                lifetime = lifetime_val  # type: ignore
                            self._match(TokenType.R_PAREN)
                    else:
                        break

                # Build the Create expression
                create = self.expression(
                    exp.Create,
                    this=this,
                    kind="DICTIONARY",
                    replace=False,
                    exists=exists,
                    expression=expression,
                )

                if primary_key:
                    create.set(
                        "primary_key",
                        self.expression(exp.PrimaryKey, expressions=primary_key, options=[]),
                    )

                if source:
                    create.set("source", source)

                if layout:
                    create.set("layout", layout)

                if lifetime:
                    create.set("lifetime", lifetime)

                return create

            # Check for FUNCTION (without REMOTE)
            if not replace and self._match_text_seq("OR", "REPLACE"):
                replace = True
            if self._match(TokenType.FUNCTION):
                # Parse CREATE [OR REPLACE] FUNCTION (SQL UDF, JavaScript UDF, Python UDF)
                exists = self._parse_exists(not_=True)

                # Check for AGGREGATE/AGGREGATION function
                is_aggregate = self._match_text_seq("AGGREGATE") or self._match_text_seq(
                    "AGGREGATION"
                )

                # Parse function definition
                this = self._parse_user_defined_function()

                # Check if it's a simple SQL UDF (AS (params) -> expression)
                returns = None
                language = None
                function_body = None
                execution_timeout = None

                if self._match(TokenType.ALIAS):
                    # SQL UDF: AS (params) -> expression
                    function_body = self._parse_lambda()
                else:
                    # JavaScript or Python UDF: RETURNS type LANGUAGE ... AS $$...$$
                    if self._match_text_seq("RETURNS"):
                        returns = self._parse_types()

                    if self._match_text_seq("LANGUAGE"):
                        language = self._parse_var_or_string()

                    if self._match(TokenType.ALIAS):
                        # Check for heredoc string (JavaScript/Python code)
                        if self._curr and self._curr.token_type == TokenType.HEREDOC_STRING:
                            # Parse the heredoc content as a literal string
                            function_body = exp.Literal.string(self._curr.text)
                            self._advance()
                        else:
                            # Regular expression
                            function_body = self._parse_assignment()

                    # Check for EXECUTION_TIMEOUT
                    if self._match_text_seq("EXECUTION_TIMEOUT"):
                        execution_timeout = self._parse_number()

                # Build the Create expression
                create = self.expression(
                    exp.Create,
                    this=this,
                    kind="AGGREGATE FUNCTION" if is_aggregate else "FUNCTION",
                    replace=replace,
                    exists=exists,
                )

                if returns:
                    create.set("returns", returns)

                if language:
                    create.set("language", language)

                if function_body:
                    create.set("expression", function_body)

                if execution_timeout:
                    create.set("execution_timeout", execution_timeout)

                return create
            elif replace:
                # OR REPLACE was matched but not followed by FUNCTION, go back
                self._retreat(start_index)

            # For other CREATE types, use parent implementation
            self._retreat(start_index)
            return super()._parse_create()

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_TO_NUMBER = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        GROUPINGS_SEP = ""
        SET_OP_MODIFIERS = False
        ARRAY_SIZE_NAME = "LENGTH"
        WRAP_DERIVED_VALUES = False

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "string",
            exp.DataType.Type.CHAR: "string",
            exp.DataType.Type.LONGBLOB: "string",
            exp.DataType.Type.LONGTEXT: "string",
            exp.DataType.Type.MEDIUMBLOB: "string",
            exp.DataType.Type.MEDIUMTEXT: "string",
            exp.DataType.Type.TINYBLOB: "string",
            exp.DataType.Type.TINYTEXT: "string",
            exp.DataType.Type.TEXT: "string",
            exp.DataType.Type.VARBINARY: "string",
            exp.DataType.Type.VARCHAR: "string",
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.ARRAY: "array",
            exp.DataType.Type.BOOLEAN: "bool",
            exp.DataType.Type.BIGINT: "int64",
            exp.DataType.Type.DATE: "date",
            exp.DataType.Type.DATE32: "date32",
            exp.DataType.Type.DATETIME: "datetime",
            exp.DataType.Type.DATETIME2: "datetime",
            exp.DataType.Type.SMALLDATETIME: "datetime",
            exp.DataType.Type.DATETIME64: "datetime64",
            exp.DataType.Type.DECIMAL: "decimal",
            exp.DataType.Type.DECIMAL32: "decimal32",
            exp.DataType.Type.DECIMAL64: "decimal64",
            exp.DataType.Type.DECIMAL128: "decimal128",
            exp.DataType.Type.DECIMAL256: "decimal256",
            exp.DataType.Type.TIMESTAMP: "datetime",
            exp.DataType.Type.TIMESTAMPNTZ: "datetime",
            exp.DataType.Type.TIMESTAMPTZ: "datetime",
            exp.DataType.Type.DOUBLE: "float64",
            exp.DataType.Type.ENUM: "enum",
            exp.DataType.Type.ENUM8: "enum8",
            exp.DataType.Type.ENUM16: "enum16",
            exp.DataType.Type.FIXEDSTRING: "fixed_string",
            exp.DataType.Type.FLOAT: "float32",
            exp.DataType.Type.INT: "int32",
            exp.DataType.Type.MEDIUMINT: "int32",
            exp.DataType.Type.INT128: "int128",
            exp.DataType.Type.INT256: "int256",
            exp.DataType.Type.LOWCARDINALITY: "low_cardinality",
            exp.DataType.Type.MAP: "map",
            exp.DataType.Type.NESTED: "nested",
            exp.DataType.Type.NOTHING: "nothing",
            exp.DataType.Type.SMALLINT: "int16",
            exp.DataType.Type.STRUCT: "tuple",
            exp.DataType.Type.TINYINT: "int8",
            exp.DataType.Type.UBIGINT: "uint64",
            exp.DataType.Type.UINT: "uint32",
            exp.DataType.Type.UINT128: "uint128",
            exp.DataType.Type.UINT256: "uint256",
            exp.DataType.Type.USMALLINT: "uint16",
            exp.DataType.Type.UTINYINT: "uint8",
            exp.DataType.Type.UUID: "uuid",  # Added UUID mapping
            exp.DataType.Type.JSON: "json",  # Added JSON mapping
            exp.DataType.Type.JSONB: "json",  # JSONB maps to json in Timeplus
            exp.DataType.Type.IPV4: "ipv4",
            exp.DataType.Type.IPV6: "ipv6",
            exp.DataType.Type.POINT: "point",
            exp.DataType.Type.RING: "ring",
            exp.DataType.Type.LINESTRING: "line_string",
            exp.DataType.Type.MULTILINESTRING: "multi_line_string",
            exp.DataType.Type.POLYGON: "polygon",
            exp.DataType.Type.MULTIPOLYGON: "multi_polygon",
            exp.DataType.Type.AGGREGATEFUNCTION: "AggregateFunction",
            exp.DataType.Type.SIMPLEAGGREGATEFUNCTION: "SimpleAggregateFunction",
            exp.DataType.Type.DYNAMIC: "dynamic",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("any"),
            exp.Interval: lambda self,
            e: f"INTERVAL {self.sql(e.this).strip(chr(39))} {self.sql(e, 'unit')}",
            exp.ApproxDistinct: rename_func("uniq"),
            exp.SettingsProperty: lambda self,
            e: f"SETTINGS {', '.join(f'{self.sql(s.left)} = {self.sql(s.right)}' if isinstance(s, exp.EQ) else self.sql(s) for s in e.expressions)}",
            exp.ArrayConcat: rename_func("array_concat"),
            exp.ArrayFilter: lambda self, e: self.func("array_filter", e.expression, e.this),
            exp.ArrayRemove: remove_from_array_using_filter,
            exp.ArrayReverse: rename_func("array_reverse"),
            exp.ArraySlice: rename_func("array_slice"),
            exp.ArraySum: rename_func("array_sum"),
            exp.ArgMax: arg_max_or_min_no_count("arg_max"),
            exp.ArgMin: arg_max_or_min_no_count("arg_min"),
            exp.Array: inline_array_sql,
            exp.CastToStrType: rename_func("CAST"),
            exp.CountIf: rename_func("count_if"),
            exp.CosineDistance: rename_func("cosine_distance"),
            exp.CompressColumnConstraint: lambda self,
            e: f"CODEC({self.expressions(e, key='this', flat=True)})",
            exp.ComputedColumnConstraint: lambda self,
            e: f"{'MATERIALIZED' if e.args.get('persisted') else 'ALIAS'} {self.sql(e, 'this')}",
            exp.CurrentDate: lambda self, e: self.func("CURRENT_DATE"),
            exp.DateAdd: _datetime_delta_sql("DATE_ADD"),
            exp.DateDiff: _datetime_delta_sql("DATE_DIFF"),
            exp.DateStrToDate: rename_func("to_date"),
            exp.DateSub: _datetime_delta_sql("DATE_SUB"),
            exp.Explode: rename_func("array_join"),
            exp.FarmFingerprint: rename_func("farm_fingerprint64"),
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.IsNan: rename_func("is_nan"),
            exp.JSONCast: lambda self, e: f"{self.sql(e, 'this')}.:{self.sql(e, 'to')}",
            exp.JSONExtract: json_extract_segments("json_extract_string", quoted_index=False),
            exp.JSONExtractScalar: json_extract_segments("json_extract_string", quoted_index=False),
            exp.JSONPathKey: json_path_key_only_name,
            exp.JSONPathRoot: lambda *_: "",
            exp.Length: length_or_char_length_sql,
            exp.Map: _map_sql,
            exp.Median: rename_func("median"),
            exp.Nullif: rename_func("null_if"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Pivot: no_pivot_sql,
            exp.Quantile: _quantile_sql,
            exp.RegexpLike: lambda self, e: self.func("match", e.this, e.expression),
            exp.Rand: rename_func("rand"),
            exp.StartsWith: rename_func("starts_with"),
            exp.Struct: rename_func("tuple"),
            exp.EndsWith: rename_func("ends_with"),
            exp.EuclideanDistance: rename_func("l2_distance"),
            exp.StrPosition: lambda self, e: strposition_sql(
                self,
                e,
                func_name="POSITION",
                supports_position=True,
                use_ansi_position=False,
            ),
            exp.TimeToStr: lambda self, e: self.func(
                "format_date_time", e.this, self.format_time(e), e.args.get("zone")
            ),
            exp.TimeStrToTime: _timestrtotime_sql,
            exp.TimestampAdd: _datetime_delta_sql("TIMESTAMP_ADD"),
            exp.TimestampSub: _datetime_delta_sql("TIMESTAMP_SUB"),
            exp.Typeof: rename_func("to_type_name"),
            exp.VarMap: _map_sql,
            exp.Xor: lambda self, e: self.func("xor", e.this, e.expression, *e.expressions),
            exp.MD5Digest: rename_func("MD5"),
            exp.MD5: lambda self, e: self.func("LOWER", self.func("HEX", self.func("MD5", e.this))),
            exp.SHA: rename_func("SHA1"),
            exp.SHA2: sha256_sql,
            exp.UnixToTime: _unix_to_time_sql,
            exp.TimestampTrunc: timestamptrunc_sql(zone=True),
            exp.Trim: lambda self, e: trim_sql(self, e, default_trim_type="BOTH"),
            exp.Variance: rename_func("var_samp"),
            exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
            exp.Stddev: rename_func("stddev_samp"),
            exp.Chr: rename_func("CHAR"),
            # LAG and LEAD functions in Timeplus use standard names, not '_in_frame' variants
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("edit_distance")
            ),
            exp.ParseDatetime: rename_func("parse_date_time"),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            # Timeplus doesn't support ON CLUSTER
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SettingsProperty: exp.Properties.Location.POST_SCHEMA,
        }

        # Timeplus doesn't support ON CLUSTER syntax

        # https://clickhouse.com/docs/en/sql-reference/data-types/nullable
        NON_NULLABLE_TYPES = {
            exp.DataType.Type.ARRAY,
            exp.DataType.Type.MAP,
            exp.DataType.Type.STRUCT,
            exp.DataType.Type.POINT,
            exp.DataType.Type.RING,
            exp.DataType.Type.LINESTRING,
            exp.DataType.Type.MULTILINESTRING,
            exp.DataType.Type.POLYGON,
            exp.DataType.Type.MULTIPOLYGON,
        }

        def offset_sql(self, expression: exp.Offset) -> str:
            offset = super().offset_sql(expression)

            # OFFSET ... FETCH syntax requires a "ROW" or "ROWS" keyword
            # https://clickhouse.com/docs/sql-reference/statements/select/offset
            parent = expression.parent
            if isinstance(parent, exp.Select) and isinstance(parent.args.get("limit"), exp.Fetch):
                offset = f"{offset} ROWS"

            return offset

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            strtodate_sql = self.function_fallback_sql(expression)

            if not isinstance(expression.parent, exp.Cast):
                # StrToDate returns DATEs in other dialects (eg. postgres), so
                # this branch aims to improve the transpilation to clickhouse
                return self.cast_sql(exp.cast(expression, "DATE"))

            return strtodate_sql

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            this = expression.this

            if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build("datetime"):
                return self.sql(this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            # Timeplus doesn't use Nullable wrapper
            return super().cast_sql(expression)

        def _jsonpathsubscript_sql(self, expression: exp.JSONPathSubscript) -> str:
            this = self.json_path_part(expression.this)
            return str(int(this) + 1) if is_int(this) else this

        def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
            return f"AS {self.sql(expression, 'this')}"

        def _any_to_has(
            self,
            expression: exp.EQ | exp.NEQ,
            default: t.Callable[[t.Any], str],
            prefix: str = "",
        ) -> str:
            if isinstance(expression.left, exp.Any):
                arr = expression.left
                this = expression.right
            elif isinstance(expression.right, exp.Any):
                arr = expression.right
                this = expression.left
            else:
                return default(expression)

            return prefix + self.func("has", arr.this.unnest(), this)

        def eq_sql(self, expression: exp.EQ) -> str:
            return self._any_to_has(expression, super().eq_sql)

        def neq_sql(self, expression: exp.NEQ) -> str:
            return self._any_to_has(expression, super().neq_sql, "NOT ")

        def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
            # Manually add a flag to make the search case-insensitive
            regex = self.func("CONCAT", "'(?i)'", expression.expression)
            return self.func("match", expression.this, regex)

        def datatype_sql(self, expression: exp.DataType) -> str:
            # string is the standard Timeplus type (lowercase), every other variant is just an alias.
            # Additionally, any supplied length parameter will be ignored.
            #
            # In Timeplus, string is lowercase
            if expression.this in self.STRING_TYPE_MAPPING:
                dtype = "string"
            else:
                dtype = super().datatype_sql(expression)

            # Timeplus uses lowercase nullable instead of Nullable
            # Check if this type should be wrapped in nullable
            parent = expression.parent
            nullable = expression.args.get("nullable")
            if nullable is True or (
                nullable is None
                and not (
                    isinstance(parent, exp.DataType)
                    and parent.is_type(exp.DataType.Type.MAP, check_nullable=True)
                    and expression.index in (None, 0)
                )
                and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
            ):
                # Use lowercase nullable for Timeplus
                dtype = f"nullable({dtype})"

            return dtype

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.args.get("scalar"):
                this = self.sql(expression, "this")
                alias = self.sql(expression, "alias")
                return f"{this} AS {alias}"

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
            return super().after_limit_modifiers(expression) + [
                (
                    self.seg("SETTINGS ") + self.expressions(expression, key="settings", flat=True)
                    if expression.args.get("settings")
                    else ""
                ),
                (
                    self.seg("FORMAT ") + self.sql(expression, "format")
                    if expression.args.get("format")
                    else ""
                ),
            ]

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

        # Timeplus doesn't support ON CLUSTER syntax

        # Timeplus doesn't need special createable_sql handling since ON CLUSTER is not supported

        def create_sql(self, expression: exp.Create) -> str:
            """Override to generate CREATE STREAM with proper SETTINGS.

            Following ClickHouse's approach for COMMENT handling in CTAS.
            """
            # The comment property comes last in CTAS statements, i.e. after the query
            query = expression.expression
            if isinstance(query, exp.Query):
                comment_prop = expression.find(exp.SchemaCommentProperty)
                if comment_prop:
                    comment_prop.pop()
                    query.replace(exp.paren(query))
            else:
                comment_prop = None

            # Call parent's create_sql but we need to customize for Timeplus
            # Since Timeplus uses STREAM instead of TABLE, we can't just call super()
            # We need to build it ourselves
            kind = expression.args.get("kind", "").upper()

            # Build CREATE statement parts
            create = "CREATE"
            replace = " OR REPLACE" if expression.args.get("replace") else ""

            # Handle special kinds
            if kind == "EXTERNAL TABLE":
                type_prefix = ""
                object_type = " EXTERNAL TABLE"
            elif kind == "REMOTE FUNCTION":
                # Handle CREATE REMOTE FUNCTION
                type_prefix = ""
                object_type = " REMOTE FUNCTION"
            elif kind == "FUNCTION":
                # Handle CREATE FUNCTION (SQL/JavaScript/Python UDF)
                type_prefix = ""
                object_type = " FUNCTION"
            elif kind == "AGGREGATE FUNCTION":
                # Handle CREATE AGGREGATE FUNCTION
                type_prefix = ""
                object_type = " AGGREGATE FUNCTION"
            elif kind == "DICTIONARY":
                # Handle CREATE DICTIONARY
                type_prefix = ""
                object_type = " DICTIONARY"
            elif kind == "FORMAT SCHEMA":
                # Handle CREATE FORMAT SCHEMA
                type_prefix = ""
                object_type = " FORMAT SCHEMA"
            elif kind in ("DATABASE", "SCHEMA"):
                # Database/Schema support
                type_prefix = ""
                object_type = " DATABASE"  # Always use DATABASE for Timeplus
            elif kind in ("VIEW", "MATERIALIZED VIEW"):
                # Views are views, not streams
                type_prefix = ""
                object_type = f" {kind}"
            else:
                # Handle stream type prefix
                stream_type = expression.args.get("stream_type")
                if stream_type:
                    type_prefix = f" {stream_type}"
                else:
                    type_prefix = ""
                # Always use STREAM for Timeplus (except EXTERNAL TABLE)
                object_type = " STREAM"

            exists = " IF NOT EXISTS" if expression.args.get("exists") else ""
            this = f" {self.sql(expression, 'this')}"

            # Handle INTO clause for materialized views
            into_sql = ""
            if expression.args.get("into"):
                into_sql = f" INTO {self.sql(expression.args.get('into'))}"

            # Handle schema (columns)
            expression_sql = ""
            if expression.expression:
                if isinstance(expression.expression, exp.Schema):
                    # Regular column definitions
                    expression_sql = f" ({self.expressions(expression.expression)})"
                else:
                    # For CREATE ... AS SELECT
                    expression_sql = f" AS {self.sql(expression.expression)}"

            # Handle RETURNS for functions
            returns_sql = ""
            if expression.args.get("returns"):
                returns_sql = f" RETURNS {self.sql(expression.args.get('returns'))}"

            # Handle primary key
            primary_key_sql = ""
            if expression.args.get("primary_key"):
                primary_key = expression.args.get("primary_key")
                if isinstance(primary_key, exp.PrimaryKey):
                    primary_key_sql = f" PRIMARY KEY ({self.expressions(primary_key)})"

            # Handle TTL - only for append streams (not MUTABLE streams)
            # MUTABLE streams use ttl_seconds in SETTINGS instead
            ttl_sql = ""
            stream_type = expression.args.get("stream_type")
            if expression.args.get("ttl") and stream_type != "MUTABLE":
                ttl_sql = f" TTL {self.sql(expression.args.get('ttl'))}"

            # Handle COMMENT for non-CTAS statements
            # (CTAS comment is handled separately at the end)
            comment_sql = ""
            if not comment_prop and not isinstance(query, exp.Query):
                comment_prop_inner = expression.find(exp.SchemaCommentProperty)
                if comment_prop_inner:
                    comment_prop_inner.pop()
                    comment_sql = f" {self.sql(comment_prop_inner)}"

            # Handle SETTINGS - they might be in 'settings' or in 'properties'
            # All Timeplus SETTINGS use 'key = value' format with spaces
            settings_sql = ""
            settings = expression.args.get("settings")

            # If no explicit settings, check if they're in properties (from ClickHouse parser)
            if not settings and expression.args.get("properties"):
                for prop in expression.args["properties"].expressions:
                    if isinstance(prop, exp.SettingsProperty):
                        settings = prop.expressions
                        break

            if settings:
                settings_parts = []
                for setting in settings:
                    if isinstance(setting, exp.EQ):
                        settings_parts.append(
                            f"{self.sql(setting.left)} = {self.sql(setting.right)}"
                        )
                    else:
                        settings_parts.append(self.sql(setting))
                if settings_parts:
                    settings_sql = f" SETTINGS {', '.join(settings_parts)}"

            # Handle properties for remote functions
            properties_sql = ""
            props = expression.args.get("properties")
            if props and kind == "REMOTE FUNCTION":
                for prop in props.expressions:
                    if isinstance(prop, exp.Property):
                        key = self.sql(prop.this)
                        # Get the value from the Property
                        value = prop.args.get("value")
                        if value:
                            value_sql = self.sql(value)
                        else:
                            value_sql = (
                                self.sql(prop.expression) if hasattr(prop, "expression") else ""
                            )
                        properties_sql += f" {key} {value_sql}"

            # Handle DICTIONARY specific clauses
            source_sql = ""
            layout_sql = ""
            lifetime_sql = ""
            language_sql = ""
            execution_timeout_sql = ""
            schema_type_sql = ""

            if kind == "DICTIONARY":
                if expression.args.get("source"):
                    source_sql = f" SOURCE({self.sql(expression.args.get('source'))})"

                if expression.args.get("layout"):
                    layout_sql = f" LAYOUT({self.sql(expression.args.get('layout'))})"

                if expression.args.get("lifetime"):
                    lifetime_sql = f" LIFETIME({self.sql(expression.args.get('lifetime'))})"

            # Handle FUNCTION specific clauses
            if kind in ("FUNCTION", "AGGREGATE FUNCTION"):
                if expression.args.get("language"):
                    language_sql = f" LANGUAGE {self.sql(expression.args.get('language'))}"

                if expression.args.get("execution_timeout"):
                    execution_timeout_sql = (
                        f" EXECUTION_TIMEOUT {self.sql(expression.args.get('execution_timeout'))}"
                    )

            # Handle FORMAT SCHEMA specific clauses
            if kind == "FORMAT SCHEMA":
                if expression.args.get("schema_type"):
                    schema_type_sql = f" TYPE {self.sql(expression.args.get('schema_type'))}"

            # Build the CREATE SQL
            create_sql = f"{create}{replace}{type_prefix}{object_type}{exists}{this}{into_sql}{expression_sql}{returns_sql}{language_sql}{primary_key_sql}{source_sql}{layout_sql}{lifetime_sql}{ttl_sql}{schema_type_sql}{comment_sql}{settings_sql}{properties_sql}{execution_timeout_sql}"

            # For CTAS, append comment at the very end (after the query)
            if comment_prop:
                comment_sql = self.sql(comment_prop)
                comment_sql = f" {comment_sql}" if comment_sql else ""
                create_sql = f"{create_sql}{comment_sql}"

            return create_sql

        def prewhere_sql(self, expression: exp.PreWhere) -> str:
            this = self.indent(self.sql(expression, "this"))
            return f"{self.seg('PREWHERE')}{self.sep()}{this}"

        def indexcolumnconstraint_sql(self, expression: exp.IndexColumnConstraint) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            expr = self.sql(expression, "expression")
            expr = f" {expr}" if expr else ""
            index_type = self.sql(expression, "index_type")
            index_type = f" TYPE {index_type}" if index_type else ""
            granularity = self.sql(expression, "granularity")
            granularity = f" GRANULARITY {granularity}" if granularity else ""

            return f"INDEX{this}{expr}{index_type}{granularity}"

        def partition_sql(self, expression: exp.Partition) -> str:
            return f"PARTITION {self.expressions(expression, flat=True)}"

        def partitionid_sql(self, expression: exp.PartitionId) -> str:
            return f"ID {self.sql(expression.this)}"

        def replacepartition_sql(self, expression: exp.ReplacePartition) -> str:
            return (
                f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"
            )

        def is_sql(self, expression: exp.Is) -> str:
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            return is_sql

        def in_sql(self, expression: exp.In) -> str:
            in_sql = super().in_sql(expression)

            if isinstance(expression.parent, exp.Not) and expression.args.get("is_global"):
                in_sql = in_sql.replace("GLOBAL IN", "GLOBAL NOT IN", 1)

            return in_sql

        def not_sql(self, expression: exp.Not) -> str:
            if isinstance(expression.this, exp.In) and expression.this.args.get("is_global"):
                # let `GLOBAL IN` child interpose `NOT`
                return self.sql(expression, "this")

            return super().not_sql(expression)

        def drop_sql(self, expression: exp.Drop) -> str:
            """Override to generate DROP STREAM for external streams/tables.

            In Timeplus, external streams and external tables are dropped using
            DROP STREAM, not DROP EXTERNAL STREAM/TABLE.
            """
            kind = expression.args.get("kind", "").upper()

            # For external tables/streams, we should use DROP STREAM
            # The "EXTERNAL" part is only used in CREATE, not in DROP
            if kind == "EXTERNAL TABLE" or kind == "EXTERNAL STREAM":
                # Replace the kind with just STREAM for the DROP statement
                expression = expression.copy()
                expression.set("kind", "STREAM")

            # Handle other object types
            elif not kind or kind == "TABLE":
                # Default tables should be treated as STREAM in Timeplus
                expression = expression.copy()
                expression.set("kind", "STREAM")

            return super().drop_sql(expression)

        def values_sql(self, expression: exp.Values, values_as_table: bool = True) -> str:
            # If the VALUES clause contains tuples of expressions, we need to treat it
            # as a table since Clickhouse will automatically alias it as such.
            alias = expression.args.get("alias")

            if alias and alias.args.get("columns") and expression.expressions:
                values = expression.expressions[0].expressions
                values_as_table = any(isinstance(value, exp.Tuple) for value in values)
            else:
                values_as_table = True

            return super().values_sql(expression, values_as_table=values_as_table)
