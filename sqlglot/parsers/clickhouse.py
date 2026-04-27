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
from sqlglot.tokens import Token, TokenType
from sqlglot.trie import new_trie
from builtins import type as Type

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from collections.abc import Mapping, Sequence, Collection


def _build_datetime_format(
    expr_type: Type[E],
) -> t.Callable[[list], E]:
    def _builder(args: list) -> E:
        expr = build_formatted_time(expr_type, "clickhouse")(args)

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
    *args: t.Any, **kwargs: t.Any
) -> t.Callable[[ClickHouseParser], exp.Show | exp.Command]:
    def _parse(self: ClickHouseParser) -> exp.Show | exp.Command:
        return self._parse_show_clickhouse(*args, **kwargs)

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

    EXPLAIN_STYLES = (
        "AST",
        "ESTIMATE",
        "PIPELINE",
        "PLAN",
        "SYNTAX",
    )

    SHOW_PARSERS = {
        "ACCESS": _show_parser("ACCESS"),
        "CHANGED SETTINGS": _show_parser("CHANGED SETTINGS"),
        "CLUSTER": _show_parser("CLUSTER"),
        "CLUSTERS": _show_parser("CLUSTERS"),
        "COLUMNS": _show_parser("COLUMNS"),
        "CREATE DATABASE": _show_parser("CREATE DATABASE"),
        "CREATE DICTIONARY": _show_parser("CREATE DICTIONARY"),
        "CREATE MASKING POLICY": _show_parser("CREATE MASKING POLICY"),
        "CREATE POLICY": _show_parser("CREATE POLICY"),
        "CREATE PROFILE": _show_parser("CREATE PROFILE"),
        "CREATE QUOTA": _show_parser("CREATE QUOTA"),
        "CREATE ROLE": _show_parser("CREATE ROLE"),
        "CREATE ROW POLICY": _show_parser("CREATE ROW POLICY"),
        "CREATE SETTINGS PROFILE": _show_parser("CREATE SETTINGS PROFILE"),
        "CREATE TABLE": _show_parser("CREATE TABLE"),
        "CREATE TEMPORARY TABLE": _show_parser("CREATE TEMPORARY TABLE"),
        "CREATE USER": _show_parser("CREATE USER"),
        "CREATE VIEW": _show_parser("CREATE VIEW"),
        "CURRENT QUOTA": _show_parser("CURRENT QUOTA"),
        "CURRENT ROLES": _show_parser("CURRENT ROLES"),
        "DATABASE": _show_parser("DATABASE"),
        "DATABASES": _show_parser("DATABASES"),
        "DICTIONARIES": _show_parser("DICTIONARIES"),
        "DICTIONARY": _show_parser("DICTIONARY"),
        "ENABLED ROLES": _show_parser("ENABLED ROLES"),
        "ENGINES": _show_parser("ENGINES"),
        "EXTENDED COLUMNS": _show_parser("EXTENDED COLUMNS"),
        "EXTENDED FULL COLUMNS": _show_parser("EXTENDED FULL COLUMNS"),
        "EXTENDED INDEX": _show_parser("EXTENDED INDEX"),
        "EXTENDED INDEXES": _show_parser("EXTENDED INDEXES"),
        "EXTENDED INDICES": _show_parser("EXTENDED INDICES"),
        "EXTENDED KEYS": _show_parser("EXTENDED KEYS"),
        "FILESYSTEM CACHES": _show_parser("FILESYSTEM CACHES"),
        "FULL COLUMNS": _show_parser("FULL COLUMNS"),
        "FULL EXTENDED COLUMNS": _show_parser("FULL EXTENDED COLUMNS"),
        "FULL TABLES": _show_parser("FULL TABLES"),
        "FULL TEMPORARY TABLES": _show_parser("FULL TEMPORARY TABLES"),
        "FUNCTIONS": _show_parser("FUNCTIONS"),
        "GRANTS": _show_parser("GRANTS"),
        "INDEX": _show_parser("INDEX"),
        "INDEXES": _show_parser("INDEXES"),
        "INDICES": _show_parser("INDICES"),
        "KEYS": _show_parser("KEYS"),
        "MERGES": _show_parser("MERGES"),
        "POLICIES": _show_parser("POLICIES"),
        "PROCESSLIST": _show_parser("PROCESSLIST"),
        "PROFILES": _show_parser("PROFILES"),
        "QUOTA": _show_parser("QUOTA"),
        "QUOTAS": _show_parser("QUOTAS"),
        "ROLES": _show_parser("ROLES"),
        "ROW POLICIES": _show_parser("ROW POLICIES"),
        "SETTING": _show_parser("SETTING"),
        "SETTINGS": _show_parser("SETTINGS"),
        "SETTINGS PROFILES": _show_parser("SETTINGS PROFILES"),
        "TABLE": _show_parser("TABLE"),
        "TABLES": _show_parser("TABLES"),
        "TEMPORARY FULL TABLES": _show_parser("TEMPORARY FULL TABLES"),
        "TEMPORARY TABLE": _show_parser("TEMPORARY TABLE"),
        "TEMPORARY TABLES": _show_parser("TEMPORARY TABLES"),
        "USERS": _show_parser("USERS"),
        "VIEW": _show_parser("VIEW"),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.DETACH: lambda self: self._parse_detach(),
        TokenType.SHOW: lambda self: self._parse_show(),
    }

    SHOW_TRIE = new_trie(key.split(" ") for key in SHOW_PARSERS)

    def _parse_show_clickhouse(self, this: str) -> exp.Show | exp.Command:
        query = None
        if self._curr:
            start = self._curr
            while self._curr:
                self._advance()
            query = self._find_sql(start, self._prev)

        return self.expression(exp.Show(this=this, query=query))

    def _parse_explain_settings(self) -> list[exp.EQ] | None:
        expressions = []

        while True:
            index = self._index
            setting = self._parse_id_var()

            if not setting or not self._match(TokenType.EQ):
                self._retreat(index)
                break

            expressions.append(
                self.expression(exp.EQ(this=setting, expression=self._parse_assignment()))
            )

            if not self._match(TokenType.COMMA):
                break

        return expressions or None

    def _parse_describe(self) -> exp.Describe | exp.Command:
        if self._prev.text.upper() != "EXPLAIN":
            return super()._parse_describe()

        start = self._prev
        style = None

        if self._match_text_seq("QUERY", "TREE"):
            style = "QUERY TREE"
        elif self._match_text_seq("TABLE", "OVERRIDE"):
            start = self._tokens[self._index - 2]
            while self._curr:
                self._advance()
            text = self._find_sql(start, self._prev)
            size = len(start.text)
            return self.expression(
                exp.Describe(
                    this=exp.Command(this=text[:size], expression=text[size:]),
                    kind="EXPLAIN",
                )
            )
        elif self._match_texts(self.EXPLAIN_STYLES):
            style = self._prev.text.upper()

        expressions = self._parse_explain_settings()
        if self._match_set(self.STATEMENT_PARSERS, advance=False):
            this = self._parse_statement()
        else:
            this = self._parse_select()

        if not this:
            return self._parse_as_command(start)

        if self._curr and self._curr.token_type != TokenType.SEMICOLON:
            return self._parse_as_command(start)

        return self.expression(
            exp.Describe(this=this, kind="EXPLAIN", style=style, expressions=expressions)
        )

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
        self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
    ) -> exp.Expr | None:
        dtype = super()._parse_types(
            check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
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
