from __future__ import annotations

import typing as t

from collections import deque

from sqlglot import exp, parser
from sqlglot.dialects.dialect import (
    build_date_delta,
    build_formatted_time,
    build_json_extract_path,
    build_like,
)
from sqlglot.helper import seq_get
from sqlglot.trie import new_trie
from sqlglot.tokens import Token, TokenType
from builtins import type as Type

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from collections.abc import Mapping, Sequence, Collection


def _build_datetime_format(
    expr_type: Type[E],
) -> t.Callable:
    def _builder(args: list, dialect: t.Any) -> E:
        expr = build_formatted_time(expr_type)(args, dialect)

        timezone = seq_get(args, 2)
        if timezone:
            expr.set("zone", timezone)

        return expr

    return _builder


def _build_count_if(args: list) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="countIf", expressions=args)


def _build_str_to_date(args: list) -> exp.Cast | exp.Anonymous:
    if len(args) == 3:
        return exp.Anonymous(this="STR_TO_DATE", expressions=args)

    strtodate = exp.StrToDate.from_arg_list(args)
    return exp.cast(strtodate, exp.DType.DATETIME.into_expr())


def _build_timestamp_trunc(unit: str) -> t.Callable[[list], exp.TimestampTrunc]:
    return lambda args: exp.TimestampTrunc(
        this=seq_get(args, 0), unit=exp.var(unit), zone=seq_get(args, 1)
    )


def _build_split_by_char(args: list) -> exp.Split | exp.Anonymous:
    sep = seq_get(args, 0)
    if isinstance(sep, exp.Literal):
        sep_value = sep.to_py()
        if isinstance(sep_value, str) and len(sep_value.encode("utf-8")) == 1:
            return _build_split(exp.Split)(args)

    return exp.Anonymous(this="splitByChar", expressions=args)


def _build_split(exp_class: Type[E]) -> t.Callable[[list], E]:
    return lambda args: exp_class(
        this=seq_get(args, 1), expression=seq_get(args, 0), limit=seq_get(args, 2)
    )


def _show_parser(
    name: str, *args: t.Any, **kwargs: t.Any
) -> t.Callable[[ClickHouseParser], exp.Show | None]:
    def _parse(self: ClickHouseParser) -> exp.Show | None:
        return getattr(self, name)(*args, **kwargs)

    return _parse


# Skip the 'week' unit since ClickHouse's toStartOfWeek
# uses an extra mode argument to specify the first day of the week
TIMESTAMP_TRUNC_UNITS = {
    "MICROSECOND",
    "MILLISECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "MONTH",
    "QUARTER",
    "YEAR",
}


AGG_FUNCTIONS = {
    "count",
    "min",
    "max",
    "sum",
    "avg",
    "any",
    "stddevPop",
    "stddevSamp",
    "varPop",
    "varSamp",
    "corr",
    "covarPop",
    "covarSamp",
    "entropy",
    "exponentialMovingAverage",
    "intervalLengthSum",
    "kolmogorovSmirnovTest",
    "mannWhitneyUTest",
    "median",
    "rankCorr",
    "sumKahan",
    "studentTTest",
    "welchTTest",
    "anyHeavy",
    "anyLast",
    "boundingRatio",
    "first_value",
    "last_value",
    "argMin",
    "argMax",
    "avgWeighted",
    "topK",
    "approx_top_sum",
    "topKWeighted",
    "deltaSum",
    "deltaSumTimestamp",
    "groupArray",
    "groupArrayLast",
    "groupConcat",
    "groupUniqArray",
    "groupArrayInsertAt",
    "groupArrayMovingAvg",
    "groupArrayMovingSum",
    "groupArraySample",
    "groupBitAnd",
    "groupBitOr",
    "groupBitXor",
    "groupBitmap",
    "groupBitmapAnd",
    "groupBitmapOr",
    "groupBitmapXor",
    "sumWithOverflow",
    "sumMap",
    "minMap",
    "maxMap",
    "skewSamp",
    "skewPop",
    "kurtSamp",
    "kurtPop",
    "uniq",
    "uniqExact",
    "uniqCombined",
    "uniqCombined64",
    "uniqHLL12",
    "uniqTheta",
    "quantile",
    "quantiles",
    "quantileExact",
    "quantilesExact",
    "quantilesExactExclusive",
    "quantileExactLow",
    "quantilesExactLow",
    "quantileExactHigh",
    "quantilesExactHigh",
    "quantileExactWeighted",
    "quantilesExactWeighted",
    "quantileTiming",
    "quantilesTiming",
    "quantileTimingWeighted",
    "quantilesTimingWeighted",
    "quantileDeterministic",
    "quantilesDeterministic",
    "quantileTDigest",
    "quantilesTDigest",
    "quantileTDigestWeighted",
    "quantilesTDigestWeighted",
    "quantileBFloat16",
    "quantilesBFloat16",
    "quantileBFloat16Weighted",
    "quantilesBFloat16Weighted",
    "simpleLinearRegression",
    "stochasticLinearRegression",
    "stochasticLogisticRegression",
    "categoricalInformationValue",
    "contingency",
    "cramersV",
    "cramersVBiasCorrected",
    "theilsU",
    "maxIntersections",
    "maxIntersectionsPosition",
    "meanZTest",
    "quantileInterpolatedWeighted",
    "quantilesInterpolatedWeighted",
    "quantileGK",
    "quantilesGK",
    "sparkBar",
    "sumCount",
    "largestTriangleThreeBuckets",
    "histogram",
    "sequenceMatch",
    "sequenceCount",
    "windowFunnel",
    "retention",
    "uniqUpTo",
    "sequenceNextNode",
    "exponentialTimeDecayedAvg",
}

# Sorted longest-first so that compound suffixes (e.g. "SimpleState") are matched
# before their sub-suffixes (e.g. "State") when resolving multi-combinator functions.
AGG_FUNCTIONS_SUFFIXES: list[str] = sorted(
    [
        "If",
        "Array",
        "ArrayIf",
        "Map",
        "SimpleState",
        "State",
        "Merge",
        "MergeState",
        "ForEach",
        "Distinct",
        "OrDefault",
        "OrNull",
        "Resample",
        "ArgMin",
        "ArgMax",
    ],
    key=len,
    reverse=True,
)

# Memoized examples of all 0- and 1-suffix aggregate function names
AGG_FUNC_MAPPING: Mapping[str, tuple[str, str | None]] = {
    f"{f}{sfx}": (f, sfx) for sfx in AGG_FUNCTIONS_SUFFIXES for f in AGG_FUNCTIONS
} | {f: (f, None) for f in AGG_FUNCTIONS}


class ClickHouseParser(parser.Parser):
    # Tested in ClickHouse's playground, it seems that the following two queries do the same thing
    # * select x from t1 union all select x from t2 limit 1;
    # * select x from t1 union all (select x from t2 limit 1);
    MODIFIERS_ATTACHED_TO_SET_OP = False
    INTERVAL_SPANS = False
    OPTIONAL_ALIAS_TOKEN_CTE = False
    JOINS_HAVE_EQUAL_PRECEDENCE = True

    FUNCTIONS = {
        **{
            k: v
            for k, v in parser.Parser.FUNCTIONS.items()
            if k not in ("TRANSFORM", "APPROX_TOP_SUM")
        },
        **{f"TOSTARTOF{unit}": _build_timestamp_trunc(unit=unit) for unit in TIMESTAMP_TRUNC_UNITS},
        "ANY": exp.AnyValue.from_arg_list,
        "ARRAYCOMPACT": exp.ArrayCompact.from_arg_list,
        "ARRAYCONCAT": exp.ArrayConcat.from_arg_list,
        "ARRAYDISTINCT": exp.ArrayDistinct.from_arg_list,
        "ARRAYEXCEPT": exp.ArrayExcept.from_arg_list,
        "ARRAYSUM": exp.ArraySum.from_arg_list,
        "ARRAYMAX": exp.ArrayMax.from_arg_list,
        "ARRAYMIN": exp.ArrayMin.from_arg_list,
        "ARRAYREVERSE": exp.ArrayReverse.from_arg_list,
        "ARRAYSLICE": exp.ArraySlice.from_arg_list,
        "CURRENTDATABASE": exp.CurrentDatabase.from_arg_list,
        "CURRENTSCHEMAS": exp.CurrentSchemas.from_arg_list,
        "COUNTIF": _build_count_if,
        "CITYHASH64": exp.CityHash64.from_arg_list,
        "COSINEDISTANCE": exp.CosineDistance.from_arg_list,
        "VERSION": exp.CurrentVersion.from_arg_list,
        "DATE_ADD": build_date_delta(exp.DateAdd, default_unit=None),
        "DATEADD": build_date_delta(exp.DateAdd, default_unit=None),
        "DATE_DIFF": build_date_delta(exp.DateDiff, default_unit=None, supports_timezone=True),
        "DATEDIFF": build_date_delta(exp.DateDiff, default_unit=None, supports_timezone=True),
        "DATE_FORMAT": _build_datetime_format(exp.TimeToStr),
        "DATE_SUB": build_date_delta(exp.DateSub, default_unit=None),
        "DATESUB": build_date_delta(exp.DateSub, default_unit=None),
        "FORMATDATETIME": _build_datetime_format(exp.TimeToStr),
        "HAS": exp.ArrayContains.from_arg_list,
        "ILIKE": build_like(exp.ILike),
        "JSONEXTRACTSTRING": build_json_extract_path(
            exp.JSONExtractScalar, zero_based_indexing=False
        ),
        "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
        "LIKE": build_like(exp.Like),
        "L2Distance": exp.EuclideanDistance.from_arg_list,
        "MAP": parser.build_var_map,
        "MATCH": exp.RegexpLike.from_arg_list,
        "NOTLIKE": build_like(exp.Like, not_like=True),
        "PARSEDATETIME": _build_datetime_format(exp.ParseDatetime),
        "RANDCANONICAL": exp.Rand.from_arg_list,
        "STR_TO_DATE": _build_str_to_date,
        "TIMESTAMP_SUB": build_date_delta(exp.TimestampSub, default_unit=None),
        "TIMESTAMPSUB": build_date_delta(exp.TimestampSub, default_unit=None),
        "TIMESTAMP_ADD": build_date_delta(exp.TimestampAdd, default_unit=None),
        "TIMESTAMPADD": build_date_delta(exp.TimestampAdd, default_unit=None),
        "TOMONDAY": _build_timestamp_trunc("WEEK"),
        "UNIQ": exp.ApproxDistinct.from_arg_list,
        "XOR": lambda args: exp.Xor(expressions=args),
        "MD5": exp.MD5Digest.from_arg_list,
        "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
        "SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
        "SPLITBYCHAR": _build_split_by_char,
        "SPLITBYREGEXP": _build_split(exp.RegexpSplit),
        "SPLITBYSTRING": _build_split(exp.Split),
        "SUBSTRINGINDEX": exp.SubstringIndex.from_arg_list,
        "TOTYPENAME": exp.Typeof.from_arg_list,
        "EDITDISTANCE": exp.Levenshtein.from_arg_list,
        "JAROWINKLERSIMILARITY": exp.JarowinklerSimilarity.from_arg_list,
        "LEVENSHTEINDISTANCE": exp.Levenshtein.from_arg_list,
        "UTCTIMESTAMP": exp.UtcTimestamp.from_arg_list,
    }

    AGG_FUNCTIONS = AGG_FUNCTIONS
    AGG_FUNCTIONS_SUFFIXES = AGG_FUNCTIONS_SUFFIXES

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

    AGG_FUNC_MAPPING = AGG_FUNC_MAPPING

    @classmethod
    def _resolve_clickhouse_agg(cls, name: str) -> tuple[str, Sequence[str]] | None:
        # ClickHouse allows chaining multiple combinators on aggregate functions.
        # See https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators
        # N.B. this resolution allows any suffix stack, including ones that ClickHouse rejects
        # syntactically such as sumMergeMerge (due to repeated adjacent suffixes)

        # Until we are able to identify a 1- or 0-suffix aggregate function by name,
        # repeatedly strip and queue suffixes (checking longer suffixes first, see comment on
        # AGG_FUNCTIONS_SUFFIXES_SORTED). This loop only runs for 2 or more suffixes,
        # as AGG_FUNC_MAPPING memoizes all 0- and 1-suffix
        accumulated_suffixes: deque[str] = deque()
        while (parts := AGG_FUNC_MAPPING.get(name)) is None:
            for suffix in AGG_FUNCTIONS_SUFFIXES:
                if name.endswith(suffix) and len(name) != len(suffix):
                    accumulated_suffixes.appendleft(suffix)
                    name = name[: -len(suffix)]
                    break
            else:
                return None

        # We now have a 0- or 1-suffix aggregate
        agg_func_name, inner_suffix = parts
        if inner_suffix:
            # this is a 1-suffix aggregate (either naturally or via repeated suffix
            # stripping). prepend the innermost suffix.
            accumulated_suffixes.appendleft(inner_suffix)

        return (agg_func_name, accumulated_suffixes)

    FUNCTION_PARSERS = {
        **{k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "MATCH"},
        "ARRAYJOIN": lambda self: self.expression(exp.Explode(this=self._parse_expression())),
        "GROUPCONCAT": lambda self: self._parse_group_concat(),
        "QUANTILE": lambda self: self._parse_quantile(),
        "MEDIAN": lambda self: self._parse_quantile(),
        "COLUMNS": lambda self: self._parse_columns(),
        "TUPLE": lambda self: exp.Struct.from_arg_list(self._parse_function_args(alias=True)),
        "AND": lambda self: exp.and_(*self._parse_function_args(alias=False)),
        "OR": lambda self: exp.or_(*self._parse_function_args(alias=False)),
    }

    PROPERTY_PARSERS = {
        **{k: v for k, v in parser.Parser.PROPERTY_PARSERS.items() if k != "DYNAMIC"},
        "ENGINE": lambda self: self._parse_engine_property(),
        "UUID": lambda self: self.expression(exp.UuidProperty(this=self._parse_string())),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        k: v for k, v in parser.Parser.NO_PAREN_FUNCTION_PARSERS.items() if k != "ANY"
    }

    NO_PAREN_FUNCTIONS = {
        k: v
        for k, v in parser.Parser.NO_PAREN_FUNCTIONS.items()
        if k != TokenType.CURRENT_TIMESTAMP
    }

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.GLOBAL: lambda self, this: self._parse_global_in(this),
    }

    COLUMN_OPERATORS = {
        **{k: v for k, v in parser.Parser.COLUMN_OPERATORS.items() if k != TokenType.PLACEHOLDER},
        TokenType.DOTCARET: lambda self, this, field: self.expression(
            exp.NestedJSONSelect(this=this, expression=field)
        ),
    }

    JOIN_KINDS = {
        *parser.Parser.JOIN_KINDS,
        TokenType.ALL,
        TokenType.ANY,
        TokenType.ASOF,
        TokenType.ARRAY,
    }

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
        TokenType.ALL,
        TokenType.ANY,
        TokenType.ARRAY,
        TokenType.ASOF,
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
        TokenType.FORMAT: lambda self: ("format", self._advance() or self._parse_id_var()),
    }

    CONSTRAINT_PARSERS = {
        **parser.Parser.CONSTRAINT_PARSERS,
        "INDEX": lambda self: self._parse_index_constraint(),
        "CODEC": lambda self: self._parse_compress(),
        "ASSUME": lambda self: self._parse_assume_constraint(),
    }

    ALTER_PARSERS = {
        **parser.Parser.ALTER_PARSERS,
        "MODIFY": lambda self: self._parse_alter_table_modify(),
        "REPLACE": lambda self: self._parse_alter_table_replace(),
    }

    SCHEMA_UNNAMED_CONSTRAINTS = {
        *parser.Parser.SCHEMA_UNNAMED_CONSTRAINTS,
        "INDEX",
    } - {"CHECK"}

    PLACEHOLDER_PARSERS = {
        **parser.Parser.PLACEHOLDER_PARSERS,
        TokenType.L_BRACE: lambda self: self._parse_query_parameter(),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.DETACH: lambda self: self._parse_detach(),
        TokenType.SHOW: lambda self: self._parse_show(),
    }

    SHOW_PARSERS = {
        "CREATE TABLE": _show_parser(
            "_parse_show_target", "CREATE TABLE", allow_into_outfile=True, allow_format=True
        ),
        "CREATE TEMPORARY TABLE": _show_parser(
            "_parse_show_target",
            "CREATE TEMPORARY TABLE",
            allow_into_outfile=True,
            allow_format=True,
        ),
        "CREATE DICTIONARY": _show_parser(
            "_parse_show_target", "CREATE DICTIONARY", allow_into_outfile=True, allow_format=True
        ),
        "CREATE VIEW": _show_parser(
            "_parse_show_target", "CREATE VIEW", allow_into_outfile=True, allow_format=True
        ),
        "CREATE DATABASE": _show_parser(
            "_parse_show_target", "CREATE DATABASE", allow_into_outfile=True, allow_format=True
        ),
        "CREATE POLICY": _show_parser("_parse_show_on_expressions", "CREATE POLICY"),
        "CREATE ROW POLICY": _show_parser("_parse_show_on_expressions", "CREATE ROW POLICY"),
        "CREATE MASKING POLICY": _show_parser(
            "_parse_show_on_expressions", "CREATE MASKING POLICY"
        ),
        "CREATE USER": _show_parser("_parse_show_expressions", "CREATE USER"),
        "CREATE ROLE": _show_parser("_parse_show_expressions", "CREATE ROLE"),
        "CREATE QUOTA": _show_parser("_parse_show_expressions", "CREATE QUOTA"),
        "CREATE PROFILE": _show_parser("_parse_show_expressions", "CREATE PROFILE"),
        "CREATE SETTINGS PROFILE": _show_parser(
            "_parse_show_expressions", "CREATE SETTINGS PROFILE"
        ),
        "DATABASES": _show_parser("_parse_show_databases"),
        "FULL TEMPORARY TABLES": _show_parser("_parse_show_tables", "FULL TEMPORARY TABLES"),
        "FULL TABLES": _show_parser("_parse_show_tables", "FULL TABLES"),
        "TEMPORARY TABLES": _show_parser("_parse_show_tables", "TEMPORARY TABLES"),
        "TABLES": _show_parser("_parse_show_tables", "TABLES"),
        "EXTENDED FULL COLUMNS": _show_parser("_parse_show_columns", "EXTENDED FULL COLUMNS"),
        "EXTENDED COLUMNS": _show_parser("_parse_show_columns", "EXTENDED COLUMNS"),
        "FULL COLUMNS": _show_parser("_parse_show_columns", "FULL COLUMNS"),
        "COLUMNS": _show_parser("_parse_show_columns", "COLUMNS"),
        "DICTIONARIES": _show_parser("_parse_show_dictionaries"),
        "EXTENDED INDEX": _show_parser("_parse_show_indexes", "EXTENDED INDEX"),
        "EXTENDED INDEXES": _show_parser("_parse_show_indexes", "EXTENDED INDEXES"),
        "EXTENDED INDICES": _show_parser("_parse_show_indexes", "EXTENDED INDICES"),
        "EXTENDED KEYS": _show_parser("_parse_show_indexes", "EXTENDED KEYS"),
        "INDEX": _show_parser("_parse_show_indexes", "INDEX"),
        "INDEXES": _show_parser("_parse_show_indexes", "INDEXES"),
        "INDICES": _show_parser("_parse_show_indexes", "INDICES"),
        "KEYS": _show_parser("_parse_show_indexes", "KEYS"),
        "PROCESSLIST": _show_parser(
            "_parse_show_output_only", "PROCESSLIST", allow_into_outfile=True, allow_format=True
        ),
        "GRANTS": _show_parser("_parse_show_grants"),
        "TABLE": _show_parser(
            "_parse_show_target", "TABLE", allow_into_outfile=True, allow_format=True
        ),
        "TEMPORARY TABLE": _show_parser(
            "_parse_show_target", "TEMPORARY TABLE", allow_into_outfile=True, allow_format=True
        ),
        "DICTIONARY": _show_parser(
            "_parse_show_target", "DICTIONARY", allow_into_outfile=True, allow_format=True
        ),
        "VIEW": _show_parser(
            "_parse_show_target", "VIEW", allow_into_outfile=True, allow_format=True
        ),
        "DATABASE": _show_parser(
            "_parse_show_target", "DATABASE", allow_into_outfile=True, allow_format=True
        ),
        "USERS": _show_parser("_parse_show_simple", "USERS"),
        "CURRENT ROLES": _show_parser("_parse_show_simple", "CURRENT ROLES"),
        "ENABLED ROLES": _show_parser("_parse_show_simple", "ENABLED ROLES"),
        "ROLES": _show_parser("_parse_show_simple", "ROLES"),
        "PROFILES": _show_parser("_parse_show_simple", "PROFILES"),
        "ROW POLICIES": _show_parser("_parse_show_policies", "ROW POLICIES"),
        "POLICIES": _show_parser("_parse_show_policies", "POLICIES"),
        "QUOTAS": _show_parser("_parse_show_simple", "QUOTAS"),
        "CURRENT QUOTA": _show_parser("_parse_show_simple", "CURRENT QUOTA"),
        "QUOTA": _show_parser("_parse_show_simple", "QUOTA"),
        "ACCESS": _show_parser("_parse_show_simple", "ACCESS"),
        "CLUSTER": _show_parser("_parse_show_target", "CLUSTER", allow_format=True),
        "CLUSTERS": _show_parser("_parse_show_clusters"),
        "CHANGED SETTINGS": _show_parser("_parse_show_settings", "CHANGED SETTINGS"),
        "SETTINGS": _show_parser("_parse_show_settings_or_profiles"),
        "SETTING": _show_parser("_parse_show_target", "SETTING"),
        "FILESYSTEM CACHES": _show_parser("_parse_show_simple", "FILESYSTEM CACHES"),
        "ENGINES": _show_parser(
            "_parse_show_output_only", "ENGINES", allow_into_outfile=True, allow_format=True
        ),
        "FUNCTIONS": _show_parser("_parse_show_functions"),
        "MERGES": _show_parser("_parse_show_merges"),
    }

    SHOW_TRIE = new_trie(key.split(" ") for key in SHOW_PARSERS)

    def _show(self, this: str, **kwargs: t.Any) -> exp.Show:
        return self.expression(exp.Show(this=this, **kwargs))

    def _parse_show_expression(self) -> exp.Expr | None:
        return (
            self._parse_string()
            or self._parse_primary()
            or self._parse_table_parts()
            or self._parse_var(any_token=True)
        )

    def _parse_show_expression_list(self) -> list[exp.Expr]:
        return self._parse_csv(self._parse_show_expression)

    def _parse_show_like(
        self, allow_not: bool = True
    ) -> tuple[exp.Expr | None, bool | None, bool | None]:
        index = self._index
        not_ = self._match(TokenType.NOT) if allow_not else False

        if self._match_set((TokenType.LIKE, TokenType.ILIKE)):
            ilike = self._prev.token_type == TokenType.ILIKE
            like = self._parse_string() or self._parse_var(any_token=True)
            if like:
                return like, ilike, not_ or None

        self._retreat(index)
        return None, None, None

    def _parse_show_output(
        self, allow_into_outfile: bool = False, allow_format: bool = False
    ) -> tuple[exp.Expr | None, exp.Expr | None] | None:
        index = self._index
        into_outfile = None
        format = None

        if allow_into_outfile and self._match_text_seq("INTO", "OUTFILE"):
            into_outfile = self._parse_string() or self._parse_var(any_token=True)
            if not into_outfile:
                self._retreat(index)
                return None

        if allow_format and self._match(TokenType.FORMAT):
            format = self._parse_id_var(any_token=True) or self._parse_string()
            if not format:
                self._retreat(index)
                return None

        return into_outfile, format

    def _parse_show(self) -> exp.Show | exp.Command:
        start = self._prev
        parser = self._find_parser(self.SHOW_PARSERS, self.SHOW_TRIE)

        if not parser:
            return self._parse_as_command(start)

        expression = parser(self)
        return expression if expression and not self._curr else self._parse_as_command(start)

    def _parse_show_simple(self, this: str) -> exp.Show:
        return self._show(this)

    def _parse_show_target(
        self, this: str, allow_into_outfile: bool = False, allow_format: bool = False
    ) -> exp.Show | None:
        target = self._parse_show_expression()
        if not target:
            return None

        output = self._parse_show_output(
            allow_into_outfile=allow_into_outfile, allow_format=allow_format
        )
        if output is None:
            return None

        into_outfile, format = output
        return self._show(this, target=target, into_outfile=into_outfile, format=format)

    def _parse_show_expressions(self, this: str) -> exp.Show | None:
        expressions = self._parse_show_expression_list()
        return self._show(this, expressions=expressions) if expressions else None

    def _parse_show_on_expressions(self, this: str) -> exp.Show | None:
        target = self._parse_show_expression()
        if not target or not self._match(TokenType.ON):
            return None

        expressions = self._parse_show_expression_list()
        return self._show(this, target=target, expressions=expressions) if expressions else None

    def _parse_show_like_limit_output(
        self,
        this: str,
        allow_not: bool = True,
        allow_limit: bool = True,
        allow_into_outfile: bool = False,
        allow_format: bool = False,
        **kwargs: t.Any,
    ) -> exp.Show | None:
        like, ilike, not_ = self._parse_show_like(allow_not=allow_not)
        limit = self._parse_limit() if allow_limit else None
        output = self._parse_show_output(
            allow_into_outfile=allow_into_outfile, allow_format=allow_format
        )
        if output is None:
            return None

        into_outfile, format = output
        return self._show(
            this,
            like=like,
            ilike=ilike,
            not_=not_,
            limit=limit,
            into_outfile=into_outfile,
            format=format,
            **kwargs,
        )

    def _parse_show_output_only(
        self, this: str, allow_into_outfile: bool = False, allow_format: bool = False
    ) -> exp.Show | None:
        output = self._parse_show_output(
            allow_into_outfile=allow_into_outfile, allow_format=allow_format
        )
        if output is None:
            return None

        into_outfile, format = output
        return self._show(this, into_outfile=into_outfile, format=format)

    def _parse_show_databases(self) -> exp.Show | None:
        return self._parse_show_like_limit_output(
            "DATABASES", allow_into_outfile=True, allow_format=True
        )

    def _parse_show_tables(self, this: str) -> exp.Show | None:
        db = (
            self._parse_show_expression()
            if self._match_set((TokenType.FROM, TokenType.IN))
            else None
        )
        return self._parse_show_like_limit_output(
            this,
            allow_into_outfile=True,
            allow_format=True,
            db=db,
        )

    def _parse_show_columns(self, this: str) -> exp.Show | None:
        if not self._match_set((TokenType.FROM, TokenType.IN)):
            return None

        target = self._parse_show_expression()
        if not target:
            return None

        db = (
            self._parse_show_expression()
            if self._match_set((TokenType.FROM, TokenType.IN))
            else None
        )
        like, ilike, not_ = self._parse_show_like()
        where = None if like is not None else self._parse_where()
        limit = self._parse_limit()
        output = self._parse_show_output(allow_into_outfile=True, allow_format=True)
        if output is None:
            return None

        into_outfile, format = output
        return self._show(
            this,
            target=target,
            db=db,
            like=like,
            ilike=ilike,
            not_=not_,
            where=where,
            limit=limit,
            into_outfile=into_outfile,
            format=format,
        )

    def _parse_show_dictionaries(self) -> exp.Show | None:
        db = self._parse_show_expression() if self._match(TokenType.FROM) else None
        return self._parse_show_like_limit_output(
            "DICTIONARIES",
            allow_into_outfile=True,
            allow_format=True,
            db=db,
        )

    def _parse_show_indexes(self, this: str) -> exp.Show | None:
        if not self._match_set((TokenType.FROM, TokenType.IN)):
            return None

        target = self._parse_show_expression()
        if not target:
            return None

        db = (
            self._parse_show_expression()
            if self._match_set((TokenType.FROM, TokenType.IN))
            else None
        )
        where = self._parse_where()
        output = self._parse_show_output(allow_into_outfile=True, allow_format=True)
        if output is None:
            return None

        into_outfile, format = output
        return self._show(
            this,
            target=target,
            db=db,
            where=where,
            into_outfile=into_outfile,
            format=format,
        )

    def _parse_show_grants(self) -> exp.Show | None:
        expressions = None
        if self._match(TokenType.FOR):
            expressions = self._parse_show_expression_list()
            if not expressions:
                return None

        return self._show(
            "GRANTS",
            expressions=expressions,
            implicit=self._match_text_seq("WITH", "IMPLICIT"),
            final=self._match_text_seq("FINAL"),
        )

    def _parse_show_policies(self, this: str) -> exp.Show | None:
        target = None
        if self._match(TokenType.ON):
            target = self._parse_show_expression()
            if not target:
                return None

        return self._show(this, target=target)

    def _parse_show_clusters(self) -> exp.Show | None:
        return self._parse_show_like_limit_output("CLUSTERS")

    def _parse_show_settings(self, this: str) -> exp.Show | None:
        return self._parse_show_like_limit_output(this, allow_not=False, allow_limit=False)

    def _parse_show_settings_or_profiles(self) -> exp.Show | None:
        if self._match_text_seq("PROFILES"):
            return self._show("SETTINGS PROFILES")

        return self._parse_show_settings("SETTINGS")

    def _parse_show_functions(self) -> exp.Show | None:
        return self._parse_show_like_limit_output("FUNCTIONS", allow_not=False, allow_limit=False)

    def _parse_show_merges(self) -> exp.Show | None:
        return self._parse_show_like_limit_output("MERGES")

    def _parse_explain_table_override_expressions(self) -> list[exp.Expr]:
        expressions = []

        while True:
            if self._match_text_seq("COLUMNS"):
                expression = self._parse_schema(this=exp.var("COLUMNS"))
            elif self._match_texts(
                ("ORDER BY", "PARTITION BY", "PRIMARY KEY", "SAMPLE", "TTL"), advance=False
            ):
                expression = self._parse_property()
            else:
                break

            if not expression:
                break

            expressions.append(expression)

        return expressions

    def _parse_explain_setting(self) -> exp.Property | None:
        index = self._index
        key = self._parse_column() or self._parse_var(any_token=True)

        if not key or not self._match(TokenType.EQ):
            self._retreat(index)
            return None

        if isinstance(key, exp.Column):
            key = key.to_dot() if len(key.parts) > 1 else exp.var(key.name)

        value = self._parse_bitwise() or self._parse_var(any_token=True)
        if isinstance(value, exp.Column):
            value = exp.var(value.name)

        if value is None:
            self._retreat(index)
            return None

        return self.expression(exp.Property(this=key, value=value))

    def _parse_explain_settings(self) -> exp.Properties | None:
        properties = []

        while not self._match_set(
            {*self.STATEMENT_PARSERS, *self.SELECT_START_TOKENS}, advance=False
        ):
            property = self._parse_explain_setting()
            if not property:
                break

            properties.append(property)

            if not self._match(TokenType.COMMA):
                break

        return self.expression(exp.Properties(expressions=properties)) if properties else None

    def _parse_describe(self) -> exp.Describe | exp.Command:
        if self._prev.text.upper() != "EXPLAIN":
            return super()._parse_describe()

        start = self._prev
        style = None

        if self._match_text_seq("QUERY", "TREE"):
            style = "QUERY TREE"
        elif self._match_text_seq("TABLE", "OVERRIDE"):
            style = "TABLE OVERRIDE"
        elif self._match_texts(("AST", "SYNTAX", "PLAN", "PIPELINE", "ESTIMATE")):
            style = self._prev.text.upper()

        properties = None if style == "TABLE OVERRIDE" else self._parse_explain_settings()

        if style == "TABLE OVERRIDE":
            this = self._parse_table_parts()
            expressions = self._parse_explain_table_override_expressions()
        else:
            if not self._match_set(
                {*self.STATEMENT_PARSERS, *self.SELECT_START_TOKENS}, advance=False
            ):
                return self._parse_as_command(start)
            this = self._parse_statement()
            expressions = None

        if not this:
            return self._parse_as_command(start)

        format = None
        if isinstance(this, exp.Query):
            format = this.args.pop("format", None)

        if format is None and self._match(TokenType.FORMAT):
            format = self._parse_id_var(any_token=True) or self._parse_string()
            if format is None:
                return self._parse_as_command(start)

        expression = self.expression(
            exp.Describe(
                this=this,
                kind="EXPLAIN",
                style=style,
                properties=properties,
                expressions=expressions,
                format=format,
            )
        )
        return expression if not self._curr else self._parse_as_command(start)

    def _parse_wrapped_select_or_assignment(self) -> exp.Expr | None:
        return self._parse_wrapped(
            lambda: self._parse_select() or self._parse_assignment(), optional=True
        )

    def _parse_check_constraint(self) -> exp.CheckColumnConstraint | None:
        return self.expression(
            exp.CheckColumnConstraint(this=self._parse_wrapped_select_or_assignment())
        )

    def _parse_assume_constraint(self) -> exp.AssumeColumnConstraint | None:
        return self.expression(
            exp.AssumeColumnConstraint(this=self._parse_wrapped_select_or_assignment())
        )

    def _parse_engine_property(self) -> exp.EngineProperty:
        self._match(TokenType.EQ)
        return self.expression(
            exp.EngineProperty(this=self._parse_field(any_token=True, anonymous_func=True))
        )

    # https://clickhouse.com/docs/en/sql-reference/statements/create/function
    def _parse_user_defined_function_expression(self) -> exp.Expr | None:
        return self._parse_lambda()

    def _parse_types(
        self,
        check_func: bool = False,
        schema: bool = False,
        allow_identifiers: bool = True,
        with_collation: bool = False,
    ) -> exp.Expr | None:
        dtype = super()._parse_types(
            check_func=check_func,
            schema=schema,
            allow_identifiers=allow_identifiers,
            with_collation=with_collation,
        )
        if isinstance(dtype, exp.DataType) and dtype.args.get("nullable") is not True:
            # Mark every type as non-nullable which is ClickHouse's default, unless it's
            # already marked as nullable. This marker helps us transpile types from other
            # dialects to ClickHouse, so that we can e.g. produce `CAST(x AS Nullable(String))`
            # from `CAST(x AS TEXT)`. If there is a `NULL` value in `x`, the former would
            # fail in ClickHouse without the `Nullable` type constructor.
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
        # `extract('foobar', 'b')` works, but ClickHouse crashes for `regexpExtract('foobar', 'b')`.
        #
        # TODO: can we somehow convert the former into an equivalent `regexpExtract` call?
        self._match(TokenType.COMMA)
        return self.expression(
            exp.Anonymous(this="extract", expressions=[this, self._parse_bitwise()])
        )

    def _parse_assignment(self) -> exp.Expr | None:
        this = super()._parse_assignment()

        if self._match(TokenType.PLACEHOLDER):
            return self.expression(
                exp.If(
                    this=this,
                    true=self._parse_assignment(),
                    false=self._match(TokenType.COLON) and self._parse_assignment(),
                )
            )

        return this

    def _parse_query_parameter(self) -> exp.Expr | None:
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

        return self.expression(exp.Placeholder(this=this, kind=kind))

    def _parse_bracket(self, this: exp.Expr | None = None) -> exp.Expr | None:
        if this:
            bracket_json_type = None

            while self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
                bracket_json_type = exp.DataType(
                    this=exp.DType.ARRAY,
                    expressions=[
                        bracket_json_type
                        or exp.DType.JSON.into_expr(dialect=self.dialect, nullable=False)
                    ],
                    nested=True,
                )

            if bracket_json_type:
                return self.expression(exp.JSONCast(this=this, to=bracket_json_type))

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

    def _parse_global_in(self, this: exp.Expr | None) -> exp.Not | exp.In:
        is_negated = self._match(TokenType.NOT)
        in_expr: exp.In | None = None
        if self._match(TokenType.IN):
            in_expr = self._parse_in(this)
            in_expr.set("is_global", True)
        return self.expression(exp.Not(this=in_expr)) if is_negated else t.cast(exp.In, in_expr)

    def _parse_table(
        self,
        schema: bool = False,
        joins: bool = False,
        alias_tokens: Collection[TokenType] | None = None,
        parse_bracket: bool = False,
        is_db_reference: bool = False,
        parse_partition: bool = False,
        consume_pipe: bool = False,
    ) -> exp.Expr | None:
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
            this = self.expression(exp.Final(this=this))

        return this

    def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
        return super()._parse_position(haystack_first=True)

    # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
    def _parse_cte(self) -> exp.CTE | None:
        # WITH <identifier> AS <subquery expression>
        cte: exp.CTE | None = self._try_parse(super()._parse_cte)

        if not cte:
            # WITH <expression> AS <identifier>
            cte = self.expression(
                exp.CTE(this=self._parse_assignment(), alias=self._parse_table_alias(), scalar=True)
            )

        return cte

    def _parse_join_parts(
        self,
    ) -> tuple[Token | None, Token | None, Token | None]:
        is_global = self._prev if self._match(TokenType.GLOBAL) else None

        kind_pre = self._prev if self._match_set(self.JOIN_KINDS) else None
        side = self._prev if self._match_set(self.JOIN_SIDES) else None
        kind = self._prev if self._match_set(self.JOIN_KINDS) else None

        return is_global, side or kind, kind_pre or kind

    def _parse_join(
        self, skip_join_token: bool = False, parse_bracket: bool = False
    ) -> exp.Join | None:
        join = super()._parse_join(skip_join_token=skip_join_token, parse_bracket=True)
        if join:
            method = join.args.get("method")
            join.set("method", None)
            join.set("global_", method)

            # tbl ARRAY JOIN arr <-- this should be a `Column` reference, not a `Table`
            # https://clickhouse.com/docs/en/sql-reference/statements/select/array-join
            if join.kind == "ARRAY":
                for table in join.find_all(exp.Table):
                    table.replace(table.to_column())

        return join

    def _parse_function(
        self,
        functions: dict[str, t.Callable] | None = None,
        anonymous: bool = False,
        optional_parens: bool = True,
        any_token: bool = False,
    ) -> exp.Expr | None:
        expr = super()._parse_function(
            functions=functions,
            anonymous=anonymous,
            optional_parens=optional_parens,
            any_token=any_token,
        )

        func = expr.this if isinstance(expr, exp.Window) else expr

        # Aggregate functions can be split in 2 parts: <func_name><suffix[es]>
        parts = self._resolve_clickhouse_agg(func.this) if isinstance(func, exp.Anonymous) else None

        if parts:
            anon_func: exp.Anonymous = t.cast(exp.Anonymous, func)
            params = self._parse_func_params(anon_func)

            if len(parts[1]) > 0:
                exp_class: Type[exp.Expr] = (
                    exp.CombinedParameterizedAgg if params else exp.CombinedAggFunc
                )
            else:
                exp_class = exp.ParameterizedAgg if params else exp.AnonymousAggFunc

            instance = exp_class(this=anon_func.this, expressions=anon_func.expressions)
            if params:
                instance.set("params", params)
            func = self.expression(instance)

            if isinstance(expr, exp.Window):
                # The window's func was parsed as Anonymous in base parser, fix its
                # type to be ClickHouse style CombinedAnonymousAggFunc / AnonymousAggFunc
                expr.set("this", func)
            elif params:
                # Params have blocked super()._parse_function() from parsing the following window
                # (if that exists) as they're standing between the function call and the window spec
                expr = self._parse_window(func)
            else:
                expr = func

        return expr

    def _parse_func_params(self, this: exp.Func | None = None) -> list[exp.Expr] | None:
        if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
            return self._parse_csv(self._parse_lambda)

        if self._match(TokenType.L_PAREN):
            params = self._parse_csv(self._parse_lambda)
            self._match_r_paren(this)
            return params

        return None

    def _parse_group_concat(self) -> exp.GroupConcat:
        args = self._parse_csv(self._parse_lambda)
        params = self._parse_func_params()

        if params:
            # groupConcat(sep [, limit])(expr)
            separator = seq_get(args, 0)
            limit = seq_get(args, 1)
            this: exp.Expr | None = seq_get(params, 0)
            if limit is not None:
                this = exp.Limit(this=this, expression=limit)
            return self.expression(exp.GroupConcat(this=this, separator=separator))

        # groupConcat(expr)
        return self.expression(exp.GroupConcat(this=seq_get(args, 0)))

    def _parse_quantile(self) -> exp.Quantile:
        this = self._parse_lambda()
        params = self._parse_func_params()
        if params:
            return self.expression(exp.Quantile(this=params[0], quantile=this))
        return self.expression(exp.Quantile(this=this, quantile=exp.Literal.number(0.5)))

    def _parse_wrapped_id_vars(self, optional: bool = False) -> list[exp.Expr]:
        return super()._parse_wrapped_id_vars(optional=True)

    def _parse_column_def(
        self, this: exp.Expr | None, computed_column: bool = True
    ) -> exp.Expr | None:
        if self._match(TokenType.DOT):
            return exp.Dot(this=this, expression=self._parse_id_var())

        return super()._parse_column_def(this, computed_column=computed_column)

    def _parse_primary_key(
        self,
        wrapped_optional: bool = False,
        in_props: bool = False,
        named_primary_key: bool = False,
    ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
        return super()._parse_primary_key(
            wrapped_optional=wrapped_optional or in_props,
            in_props=in_props,
            named_primary_key=named_primary_key,
        )

    def _parse_on_property(self) -> exp.Expr | None:
        index = self._index
        if self._match_text_seq("CLUSTER"):
            this = self._parse_string() or self._parse_id_var()
            if this:
                return self.expression(exp.OnCluster(this=this))
            else:
                self._retreat(index)
        return None

    def _parse_index_constraint(self, kind: str | None = None) -> exp.IndexColumnConstraint:
        # INDEX name1 expr TYPE type1(args) GRANULARITY value
        this = self._parse_id_var()
        expression = self._parse_assignment()

        index_type = self._match_text_seq("TYPE") and (self._parse_function() or self._parse_var())

        granularity = self._match_text_seq("GRANULARITY") and self._parse_term()

        return self.expression(
            exp.IndexColumnConstraint(
                this=this, expression=expression, index_type=index_type, granularity=granularity
            )
        )

    def _parse_partition(self) -> exp.Partition | None:
        # https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
        if not self._match(TokenType.PARTITION):
            return None

        if self._match_text_seq("ID"):
            # Corresponds to the PARTITION ID <string_value> syntax
            expressions: list[exp.Expr] = [
                self.expression(exp.PartitionId(this=self._parse_string()))
            ]
        else:
            expressions = self._parse_expressions()

        return self.expression(exp.Partition(expressions=expressions))

    def _parse_alter_table_replace(self) -> exp.Expr | None:
        partition = self._parse_partition()

        if not partition or not self._match(TokenType.FROM):
            return None

        return self.expression(
            exp.ReplacePartition(expression=partition, source=self._parse_table_parts())
        )

    def _parse_alter_table_modify(self) -> exp.Expr | None:
        if properties := self._parse_properties():
            return self.expression(exp.AlterModifySqlSecurity(expressions=properties.expressions))
        return None

    def _parse_definer(self) -> exp.DefinerProperty | None:
        self._match(TokenType.EQ)
        if self._match(TokenType.CURRENT_USER):
            return exp.DefinerProperty(this=exp.Var(this=self._prev.text.upper()))
        return exp.DefinerProperty(this=self._parse_string())

    def _parse_projection_def(self) -> exp.ProjectionDef | None:
        if not self._match_text_seq("PROJECTION"):
            return None

        return self.expression(
            exp.ProjectionDef(
                this=self._parse_id_var(), expression=self._parse_wrapped(self._parse_statement)
            )
        )

    def _parse_constraint(self) -> exp.Expr | None:
        return super()._parse_constraint() or self._parse_projection_def()

    def _parse_alias(self, this: exp.Expr | None, explicit: bool = False) -> exp.Expr | None:
        # In clickhouse "SELECT <expr> APPLY(...)" is a query modifier,
        # so "APPLY" shouldn't be parsed as <expr>'s alias. However, "SELECT <expr> apply" is a valid alias
        if self._match_pair(TokenType.APPLY, TokenType.L_PAREN, advance=False):
            return this

        return super()._parse_alias(this=this, explicit=explicit)

    def _parse_expression(self) -> exp.Expr | None:
        this = super()._parse_expression()

        # Clickhouse allows "SELECT <expr> [APPLY(func)] [...]]" modifier
        while self._match_pair(TokenType.APPLY, TokenType.L_PAREN):
            this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
            self._match(TokenType.R_PAREN)

        return this

    def _parse_columns(self) -> exp.Expr:
        this: exp.Expr = self.expression(exp.Columns(this=self._parse_lambda()))

        while self._next and self._match_text_seq(")", "APPLY", "("):
            self._match(TokenType.R_PAREN)
            this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
        return this

    def _parse_value(self, values: bool = True) -> exp.Tuple | None:
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
                [self.expression(exp.Tuple(expressions=[expr])) for expr in expressions],
            )

        return value

    def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
        # ClickHouse allows custom expressions as partition key
        # https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key
        return self.expression(exp.PartitionedByProperty(this=self._parse_assignment()))

    def _parse_detach(self) -> exp.Detach:
        kind = self._match_set(self.DB_CREATABLES) and self._prev.text.upper()
        exists = self._parse_exists()
        this = self._parse_table_parts()

        return self.expression(
            exp.Detach(
                this=this,
                kind=kind,
                exists=exists,
                cluster=self._parse_on_property() if self._match(TokenType.ON) else None,
                permanent=self._match_text_seq("PERMANENTLY"),
                sync=self._match_text_seq("SYNC"),
            )
        )
