from __future__ import annotations
import typing as t
import datetime
from sqlglot import exp, generator, parser, tokens
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

DATEΤΙΜΕ_DELTA = t.Union[
    exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd
]


class ExasolTokenType:
    WITH_LOCAL_TIME_ZONE = "WITH_LOCAL_TIME_ZONE"
    HASHTYPE = "HASHTYPE"
    BYTE = "BYTE"
    MONTH = "MONTH"
    DAY = "DAY"
    SECOND = "SECOND"
    TO = "TO"


def _build_date_format(args: t.List) -> exp.TimeToStr:
    expr = build_formatted_time(exp.TimeToStr, "exasol")(args)

    timezone = seq_get(args, 2)
    if timezone:
        expr.set("zone", timezone)

    return expr


def _unix_to_time_sql(self: Exasol.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func(
            "fromUnixTimestamp", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.MILLIS:
        return self.func(
            "fromUnixTimestamp64Milli", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.MICROS:
        return self.func(
            "fromUnixTimestamp64Micro", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.NANOS:
        return self.func(
            "fromUnixTimestamp64Nano", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )

    return self.func(
        "fromUnixTimestamp",
        exp.cast(
            exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)),
            exp.DataType.Type.BIGINT,
        ),
    )


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self: Exasol.Generator, expression: exp.Quantile) -> str:
    quantile = expression.args["quantile"]
    args = f"({self.sql(expression, 'this')})"

    if isinstance(quantile, exp.Array):
        func = self.func("quantiles", *quantile)
    else:
        func = self.func("quantile", quantile)

    return func + args


def _build_count_if(args: t.List) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="countIf", expressions=args)


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
        )

    return _delta_sql


def _timestrtotime_sql(self: Exasol.Generator, expression: exp.TimeStrToTime):
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
                    (
                        ts_frac_parts[1] if num_frac_parts > 1 else ""
                    ),  # utc offset (if present)
                ]
            )

        # return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
        # this is because Clickhouse encodes the timezone as a data type parameter and throws an error if
        # it's part of the timestamp string
        ts_without_tz = (
            datetime.datetime.fromisoformat(ts_string)
            .replace(tzinfo=None)
            .isoformat(sep=" ")
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


def _map_sql(self: Exasol.Generator, expression: exp.Map | exp.VarMap) -> str:
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


class Exasol(Dialect):
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

    # https://github.com/ClickHouse/ClickHouse/issues/33935#issue-1112165779
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    UNESCAPED_SEQUENCES = {
        "\\0": "\0",
    }

    CREATABLE_KIND_MAPPING = {"DATABASE": "SCHEMA"}

    SET_OP_DISTINCT_BY_DEFAULT: t.Dict[t.Type[exp.Expression], t.Optional[bool]] = {
        exp.Except: False,
        exp.Intersect: False,
        exp.Union: None,
    }

    class Tokenizer(tokens.Tokenizer):

        IDENTIFIER_ESCAPES = ['"']
        STRING_ESCAPES = ["'"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "YEAR": TokenType.YEAR,
            "WITH LOCAL TIME ZONE": ExasolTokenType.WITH_LOCAL_TIME_ZONE,
            "MONTH": ExasolTokenType.MONTH,
            "DAY": ExasolTokenType.DAY,
            "SECOND": ExasolTokenType.SECOND,
            "TO": ExasolTokenType.TO,
            "HASHTYPE": ExasolTokenType.HASHTYPE,
            "BYTE": ExasolTokenType.BYTE,
        }

    class Parser(parser.Parser):
        # Tested in ClickHouse's playground, it seems that the following two queries do the same thing
        # * select x from t1 union all select x from t2 limit 1;
        # * select x from t1 union all (select x from t2 limit 1);
        MODIFIERS_ATTACHED_TO_SET_OP = False
        INTERVAL_SPANS = False
        OPTIONAL_ALIAS_TOKEN_CTE = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ANY": exp.AnyValue.from_arg_list,
            "ARRAYSUM": exp.ArraySum.from_arg_list,
            "COUNTIF": _build_count_if,
            "DATE_ADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATEADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATE_DIFF": build_date_delta(exp.DateDiff, default_unit=None),
            "DATEDIFF": build_date_delta(exp.DateDiff, default_unit=None),
            "DATE_FORMAT": _build_date_format,
            "DATE_SUB": build_date_delta(exp.DateSub, default_unit=None),
            "DATESUB": build_date_delta(exp.DateSub, default_unit=None),
            "FORMATDATETIME": _build_date_format,
            "JSONEXTRACTSTRING": build_json_extract_path(
                exp.JSONExtractScalar, zero_based_indexing=False
            ),
            "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
            "MAP": parser.build_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "RANDCANONICAL": exp.Rand.from_arg_list,
            "STR_TO_DATE": _build_str_to_date,
            "TUPLE": exp.Struct.from_arg_list,
            "TIMESTAMP_SUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMPSUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMP_ADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "TIMESTAMPADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "UNIQ": exp.ApproxDistinct.from_arg_list,
            "XOR": lambda args: exp.Xor(expressions=args),
            "MD5": exp.MD5Digest.from_arg_list,
            "SHA256": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(256)
            ),
            "SHA512": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(512)
            ),
            "EDITDISTANCE": exp.Levenshtein.from_arg_list,
            "LEVENSHTEINDISTANCE": exp.Levenshtein.from_arg_list,
        }
        FUNCTIONS.pop("TRANSFORM")

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
            "topKWeighted",
            "deltaSum",
            "deltaSumTimestamp",
            "groupArray",
            "groupArrayLast",
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

        AGG_FUNCTIONS_SUFFIXES = [
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

        FUNCTIONS_WITH_ALIASED_ARGS = {
            *parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS,
            "TUPLE",
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "ARRAYJOIN": lambda self: self.expression(
                exp.Explode, this=self._parse_expression()
            ),
            "QUANTILE": lambda self: self._parse_quantile(),
            "MEDIAN": lambda self: self._parse_quantile(),
            "COLUMNS": lambda self: self._parse_columns(),
        }

        FUNCTION_PARSERS.pop("MATCH")

        PROPERTY_PARSERS = parser.Parser.PROPERTY_PARSERS.copy()
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
            if (
                isinstance(dtype, exp.DataType)
                and dtype.args.get("nullable") is not True
            ):
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

                    varmap.args["keys"].append(
                        "expressions", exp.Literal.string(expression.name)
                    )
                    varmap.args["values"].append("expressions", expression.expression)

                return varmap

            return bracket

        def _parse_in(
            self, this: t.Optional[exp.Expression], is_global: bool = False
        ) -> exp.In:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_global_in(
            self, this: t.Optional[exp.Expression]
        ) -> exp.Not | exp.In:
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

                if (
                    isinstance(inner, exp.GenerateSeries)
                    and alias
                    and not alias.columns
                ):
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
            join = super()._parse_join(
                skip_join_token=skip_join_token, parse_bracket=True
            )
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
                self.AGG_FUNC_MAPPING.get(func.this)
                if isinstance(func, exp.Anonymous)
                else None
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
                    # type to be ClickHouse style CombinedAnonymousAggFunc / AnonymousAggFunc
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
            this = self._parse_lambda()
            params = self._parse_func_params()
            if params:
                return self.expression(exp.Quantile, this=params[0], quantile=this)
            return self.expression(
                exp.Quantile, this=this, quantile=exp.Literal.number(0.5)
            )

        def _parse_wrapped_id_vars(
            self, optional: bool = False
        ) -> t.List[exp.Expression]:
            return super()._parse_wrapped_id_vars(optional=True)

        def _parse_primary_key(
            self, wrapped_optional: bool = False, in_props: bool = False
        ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
            return super()._parse_primary_key(
                wrapped_optional=wrapped_optional or in_props, in_props=in_props
            )

        def _parse_on_property(self) -> t.Optional[exp.Expression]:
            index = self._index
            if self._match_text_seq("CLUSTER"):
                this = self._parse_id_var()
                if this:
                    return self.expression(exp.OnCluster, this=this)
                else:
                    self._retreat(index)
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

        def _parse_projection_def(self) -> t.Optional[exp.ProjectionDef]:
            if not self._match_text_seq("PROJECTION"):
                return None

            return self.expression(
                exp.ProjectionDef,
                this=self._parse_id_var(),
                expression=self._parse_wrapped(self._parse_statement),
            )

        def _parse_constraint(self) -> t.Optional[exp.Expression]:
            return super()._parse_constraint() or self._parse_projection_def()

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
            this: exp.Expression = self.expression(
                exp.Columns, this=self._parse_lambda()
            )

            while self._next and self._match_text_seq(")", "APPLY", "("):
                self._match(TokenType.R_PAREN)
                this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
            return this

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_TO_NUMBER = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        GROUPINGS_SEP = ""
        SET_OP_MODIFIERS = False
        VALUES_AS_TABLE = False
        ARRAY_SIZE_NAME = "LENGTH"

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.CHAR: "CHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathRoot,  # $
            exp.JSONPathKey,  # .key or ['key']
            exp.JSONPathSubscript,  # ['key'] or [0]
            exp.JSONPathWildcard,  # [*]
            exp.JSONPathUnion,  # ['key1','key2']
            exp.JSONPathSlice,  # [start:end:step]
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.MEDIUMINT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.DATETIME2: "TIMESTAMP",
            exp.DataType.Type.SMALLDATETIME: "TIMESTAMP",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # exp.Abs : rename_func("ABS"),
            exp.Anonymous: lambda self, e: self._anonymous_func(e),
            exp.Acos: lambda self, e: f"ACOS({self.sql(e, 'this')})",
            exp.DateAdd: lambda self, e: f"ADD_DAYS({self._timestamp_literal(e.this, 'this')}, {self.sql(e, 'expression')})",
            exp.AddHours: lambda self, e: f"ADD_HOURS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddMinutes: lambda self, e: f"ADD_MINUTES({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddMonths: lambda self, e: f"ADD_MONTHS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddSeconds: lambda self, e: f"ADD_SECONDS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddWeeks: lambda self, e: f"ADD_WEEKS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddYears: lambda self, e: f"ADD_YEARS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AnyValue: lambda self, e: self.windowed_func("ANY", e),
            exp.ApproxDistinct: rename_func("APPROXIMATE_COUNT_DISTINCT"),
            exp.Ascii: lambda self, e: f"ASCII({self.sql(e, 'this')})",
            exp.Asin: lambda self, e: f"ASIN({self.sql(e, 'this')})",
            exp.Atan: lambda self, e: f"ATAN({self.sql(e, 'this')})",
            exp.Atan2: lambda self, e: f"ATAN2({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Avg: lambda self, e: self.windowed_func("AVG", e),
            exp.BitwiseAnd: rename_func("BIT_AND"),
            exp.BitCheck: lambda self, e: f"BIT_CHECK({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitLength: lambda self, e: f"BIT_LENGTH({self.sql(e, 'this')})",
            exp.BitLRotate: lambda self, e: f"BIT_LROTATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            exp.BitwiseNot: rename_func("BIT_NOT"),
            exp.BitwiseOr: rename_func("BIT_OR"),
            exp.BitRRotate: lambda self, e: f"BIT_RROTATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            exp.BitSet: lambda self, e: f"BIT_SET({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitToNum: lambda self, e: f"BIT_TO_NUM({', '.join(self.sql(arg) for arg in e.expressions)})",
            exp.BitwiseXor: rename_func("BIT_XOR"),
            # Case
            # Cast
            # exp.Ceil
            # exp.Chr: rename_func("CHR"),
            exp.CharacterLength: rename_func("CHARACTER_LENGTH"),
            # exp.Coalesce,
            exp.ColognePhonetic: lambda self, e: f"COLOGNE_PHONETIC({self.sql(e, 'this')})",
            exp.ConnectByIsCycle: lambda self, e: "CONNECT_BY_ISCYCLE",
            exp.ConnectByIsLeaf: lambda self, e: "CONNECT_BY_ISLEAF",
            exp.SysConnectByPath: lambda self, e: f"SYS_CONNECT_BY_PATH({self.sql(e, 'this')}, {', '.join(self.sql(x) for x in e.expressions)})",
            exp.Command: lambda self, e: " ".join(self.sql(x) for x in e.expressions),
            # exp.Concat
            # exp.Convert
            exp.ConvertTZ: lambda self, e: self.convert_tz_sql(e),
            # exp.Corr: lambda self, e: self.corr_sql(e), - already exist
            exp.Cos: lambda self, e: self.cos_sql(e),
            exp.CosH: lambda self, e: self.cosh_sql(e),
            exp.Cot: lambda self, e: self.cot_sql(e),
            exp.Count: lambda self, e: self.windowed_func("COUNT", e),
            exp.CovarPop: rename_func("COVAR_POP"),
            exp.CovarSamp: rename_func("COVAR_SAMP"),
            exp.CumeDist: lambda self, e: self.windowed_func("CUME_DIST"),
            exp.CurrentCluster: lambda self, e: "CURRENT_CLUSTER",
            exp.CurrentDate: rename_func("CURDATE"),
            exp.CurrentSchema: lambda self, e: "CURRENT_SCHEMA",
            exp.CurrentSession: lambda self, e: "CURRENT_SESSION",
            exp.CurrentStatement: lambda self, e: "CURRENT_STATEMENT",
            exp.CurrentUser: lambda self, e: "CURRENT_USER",
            # exp.CountDistinct: lambda self, e: self.windowed_func("COUNT_DISTINCT", e),
            # exp.CountStar: lambda self, e: "COUNT(*)",
            # exp.Covar: rename_func("COVAR
            exp.CurrentTimestamp: lambda self, e: (
                f"CURRENT_TIMESTAMP({self.sql(e, 'this')})"
                if e.args.get("this")
                else "CURRENT_TIMESTAMP"
            ),
            exp.DateTrunc: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, {self._timestamp_literal(e, 'expression')})",
            # exp.Day
            exp.DaysBetween: lambda self, e: f"DAYS_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Date: lambda self, e: f"DATE {self.sql(e, 'this')}",
            exp.Timestamp: lambda self, e: f"TIMESTAMP {self.sql(e, 'this')}",
            exp.DBTimezone: lambda self, e: "DBTIMEZONE",
            exp.Decode: lambda self, e: f"DECODE({', '.join(self.sql(arg) for arg in e.expressions)})",
            exp.Degrees: lambda self, e: f"DEGREES({self.sql(e, 'this')})",
            exp.DenseRank: lambda self, e: self.windowed_func("DENSE_RANK", e),
            # exp.Div
            exp.Dump: lambda self, e: f"DUMP({', '.join(filter(None, [
                self.sql(e, 'this'),
                self.sql(e, 'format'),
                self.sql(e, 'start_position'),
                self.sql(e, 'length')
            ]))})",
            exp.EditDistance: lambda self, e: f"EDIT_DISTANCE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Every: lambda self, e: f"EVERY({self.sql(e, 'this')})",
            exp.Extract: lambda self, e: f"EXTRACT({self.sql(e, 'expression')} FROM {self._timestamp_literal(e, 'this')})",
            # exp.Extract
            # exp.FirstValue: rename_func("FIRST_VALUE"),
            # exp.Exp: lambda self, e: f"EXP({self.sql(e, 'this')})",
            exp.FirstValue: lambda self, e: self.windowed_func("FIRST_VALUE", e),
            # exp.Floor: lambda self, e: f"FLOOR({self.sql(e, 'this')})",
            exp.FromPosixTime: lambda self, e: f"FROM_POSIX_TIME({self.sql(e, 'this')})",
            # exp.Greatest: lambda self, e: f"GREATEST({', '.join(self.sql(arg) for arg in e.expressions)})", //Todo:
            exp.GroupConcat: lambda self, e: self.group_concat_sql(e),
            exp.Grouping: lambda self, e: f"GROUPING({', '.join(self.sql(x) for x in e.expressions)})",
            # exp.GroupingId: lambda self, e: f"GROUPING_ID({self.sql(e, 'this')})",
            exp.MD5: lambda self, e: f"HASH_MD5({', '.join(self.sql(x) for x in e.expressions)})",
            exp.SHA: lambda self, e: f"HASH_SHA1({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashSha256: lambda self, e: f"HASH_SHA256({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashSha512: lambda self, e: f"HASH_SHA512({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTiger: lambda self, e: f"HASH_TIGER({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeMd5: lambda self, e: f"HASHTYPE_MD5({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha1: lambda self, e: f"HASHTYPE_SHA1({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha256: lambda self, e: f"HASHTYPE_SHA256({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha512: lambda self, e: f"HASHTYPE_SHA512({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeTiger: lambda self, e: f"HASHTYPE_TIGER({', '.join(self.sql(x) for x in e.expressions)})",
            exp.Hour: lambda self, e: f"HOUR({self._timestamp_literal(e, 'this')})",
            exp.HoursBetween: lambda self, e: f"HOURS_BETWEEN({self._timestamp_literal(e, 'this')}, {self._timestamp_literal(e, 'expression')})",
            exp.If: lambda self, e: (
                f"IF {self.sql(e, 'this')} THEN {self.sql(e, 'true')} ELSE {self.sql(e, 'false')} ENDIF"
            ),
            exp.InitCap: lambda self, e: f"INITCAP({self.sql(e, 'this')})",
            exp.Insert: lambda self, e: f"INSERT({self.sql(e, 'this')}, {self.sql(e, 'start')}, {self.sql(e, 'length')}, {self.sql(e, 'expression')})",
            exp.Instr: lambda self, e: (
                "INSTR("
                + ", ".join(
                    filter(
                        None,
                        [
                            self.sql(e, "this"),
                            self.sql(e, "substr"),
                            e.args.get("position") and self.sql(e, "position"),
                            e.args.get("occurrence") and self.sql(e, "occurrence"),
                        ],
                    )
                )
                + ")"
            ),
            exp.Iproc: lambda self, e: f"IPROC({self.sql(e, 'this')})",
            exp.Is: lambda self, e: f"IS_{self.sql(e, 'type')}({self.sql(e, 'this')}"
            + (f", {self.sql(e, 'expression')}" if e.args.get("expression") else "")
            + ")",
            exp.JSONBExtract: lambda self, e: self.jsonb_extract_sql(e),
            exp.JSONValue: lambda self, e: (
                f"JSON_VALUE({self.sql(e, 'this')}, {self.sql(e, 'path')}"
                + (
                    f" NULL ON EMPTY DEFAULT {self.sql(e, 'default')} ON ERROR"
                    if e.args.get("null_on_empty")
                    and e.args.get("default")
                    and e.args.get("on_error")
                    else ""
                )
                + ")"
            ),
            exp.Lag: lambda self, e: (
                f"LAG({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LAG({self.sql(e, 'this')})"
            ),
            # exp.LastValue
            exp.LCase: rename_func("LCASE"),
            exp.Lead: lambda self, e: (
                f"LEAD({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LEAD({self.sql(e, 'this')})"
            ),
            # exp.Least
            exp.Left: lambda self, e: f"LEFT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Length
            exp.ConnectBy: lambda self, e: f"CONNECT BY {'PRIOR ' if e.args.get('prior') else ''}{self.sql(e, 'this')}",
            exp.StartWith: lambda self, e: f"START WITH {self.sql(e, 'this')}",
            exp.Level: lambda self, e: "LEVEL",
            exp.Log: lambda self, e: f"LOG({self.sql(e, 'this')}, {self.sql(e, 'base')})",
            exp.Log2: lambda self, e: f"LOG2({self.sql(e, 'this')})",
            exp.Log10: lambda self, e: f"LOG10({self.sql(e, 'this')})",
            exp.ListAgg: lambda self, e: (
                f"LISTAGG({self.sql(e, 'this')}"
                f"{', ' + self.sql(e, 'separator') if e.args.get('separator') else ''})"
                f" WITHIN GROUP (ORDER BY {self.sql(e, 'order')})"
                if e.args.get("order")
                else ""
            ),
            # exp.Ln: lambda self, e: f"LN({self.sql(e, 'this')})",
            exp.LocalTimestamp: lambda self, e: (
                f"LOCALTIMESTAMP({self.sql(e, 'precision')})"
                if e.args.get("precision")
                else "LOCALTIMESTAMP"
            ),
            exp.Locate: lambda self, e: (
                f"LOCATE({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e, 'start')})"
                if e.args.get("start")
                else f"LOCATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
            ),
            # exp.Lower
            exp.LPad: lambda self, e: f"LPAD({self.sql(e, 'this')}, {self.sql(e, 'length')}, {self.sql(e, 'pad')})",
            exp.LTrim: lambda self, e: (
                f"LTRIM({self.sql(e, 'this')}, {self.sql(e, 'trim_chars')})"
                if e.args.get("trim_chars")
                else f"LTRIM({self.sql(e, 'this')})"
            ),
            # exp.Trim
            # exp.Max
            # exp.Median
            # exp.Min
            exp.Mid: lambda self, e: (
                f"MID({self.sql(e, 'this')}, {self.sql(e, 'start')}, {self.sql(e, 'length')})"
                if e.args.get("length")
                else f"MID({self.sql(e, 'this')}, {self.sql(e, 'start')})"
            ),
            exp.Mod: rename_func("MOD"),
            exp.MinScale: lambda self, e: f"MIN_SCALE({self.sql(e, 'this')})",
            # exp.MinutesBetween: lambda self, e: f"MINUTES_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Month: lambda self, e: f"MONTH({self.sql(e, 'this')})",
            # exp.MonthsBetween: lambda self, e: f"MONTHS_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Mul: lambda self, e: f"MUL({self.sql(e, 'this')})",
            # exp.Now: lambda self, e: "NOW()",
            # exp.NProc: lambda self, e: "NPROC()",
            # exp.NTile: lambda self, e: f"NTILE({self.sql(e, 'this')})",
            # exp.NullIf: lambda self, e: f"NULLIF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.NullIfZero: lambda self, e: f"NULLIFZERO({self.sql(e, 'this')})",
            # exp.NumToDSInterval: lambda self, e: f"NUMTODSINTERVAL({self.sql(e, 'this')}, {self.sql(e, 'unit')})",
            # exp.NumToYMInterval: lambda self, e: f"NUMTOYMINTERVAL({self.sql(e, 'this')}, {self.sql(e, 'unit')})",
            # exp.NVL: lambda self, e: f"NVL({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.NVL2: lambda self, e: f"NVL2({self.sql(e, 'this')}, {self.sql(e, 'true_value')}, {self.sql(e, 'false_value')})",
            # exp.OctetLength: lambda self, e: f"OCTET_LENGTH({self.sql(e, 'this')})",
            # exp.PercentRank: lambda self, e: "PERCENT_RANK()",
            # exp.Pi: lambda self, e: "PI()",
            # exp.PosixTime: lambda self, e: f"POSIX_TIME({self.sql(e, 'this')})",
            # exp.Month: lambda self, e: f"MONTH({self.sql(e, 'this')})",
            # exp.MonthsBetween: lambda self, e
            # exp.Nullif
            exp.NthValue: lambda self, e: (
                f"NTH_VALUE({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                + (" FROM LAST" if e.args.get("from_last") else " FROM FIRST")
                + (" RESPECT NULLS" if e.args.get("respect_nulls") else " IGNORE NULLS")
                + self.sql(e, "over")
            ),
            exp.PercentileCont: lambda self, e: (
                f"PERCENTILE_CONT({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.PercentileDisc: lambda self, e: (
                f"PERCENTILE_DISC({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.StrPosition: lambda self, e: f"POSITION({self.sql(e, 'this')} IN {self.sql(e, 'expression')})",
            exp.Pow: rename_func("POWER"),
            exp.Rand: rename_func("RANDOM"),
            
            exp.RegexpExtract: rename_func("REGEXP_SUBSTR"),
            exp.RegexpReplace: rename_func("REGEXP_REPLACE"),
            
            # exp.Repeat
            # exp.Right
            exp.Round: lambda self, e: self.round_sql(e),
            # exp.Round: lambda self, e: f"ADD_DAYS({self._timestamp_literal(e.this, 'this')}, {self.sql(e, 'expression')})",
            
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        # There's no list in docs, but it can be found in Clickhouse code
        # see `ClickHouse/src/Parsers/ParserCreate*.cpp`
        ON_CLUSTER_TARGETS = {
            "SCHEMA",  # Transpiled CREATE SCHEMA may have OnCluster property set
            "DATABASE",
            "TABLE",
            "VIEW",
            "DICTIONARY",
            "INDEX",
            "FUNCTION",
            "NAMED COLLECTION",
        }

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

        def _anonymous_func(self, e):
            func_name = e.this.upper()
            handlers = {
                "MIN_SCALE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "MONTH": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "MINUTES_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "MONTHS_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "MUL": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NOW": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NPROC": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NTILE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NULLIF": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NULLIFZERO": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NUMTODSINTERVAL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NUMTOYMINTERVAL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NVL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NVL2": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                "OCTET_LENGTH": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "PERCENT_RANK": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "PI": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "POSIX_TIME": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "RADIANS": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "RADIANS": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "RANK": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "RATIO_TO_REPORT": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "REGEXP_INSTR": lambda e: self._render_func_with_args(e,  min_arg_count=2, max_arg_count=5),
                "REPLACE": lambda e: self._render_func_with_args(e,  expected_arg_count=3),
                "REVERSE": lambda e: self._render_func_with_args(e,  expected_arg_count=3),
                "ROWNUM": lambda e: "ROWNUM",
                "ROW_NUMBER": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "ROWID": lambda e: "ROWID",
                "RPAD": lambda e: self._render_func_with_args(e,  expected_arg_count=3),
                "RTRIM": lambda e: self._render_func_with_args(e,  expected_arg_count=2),
                
            }

            regr_funcs = [
                "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2",
                "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY"
            ]
            for name in regr_funcs:
                handlers[name] = lambda e: self._render_func_with_args(e, expected_arg_count=2)

            if func_name in handlers:
                return handlers[func_name](e)
            return f"{func_name}({', '.join(self.sql(arg) for arg in e.expressions)})"

     

        def _render_func_with_args(self, e, expected_arg_count=None, min_arg_count=None, max_arg_count=None):
            arg_count = len(e.expressions)

            if expected_arg_count is not None and arg_count != expected_arg_count:
                raise ValueError(f"{e.this} expects exactly {expected_arg_count} arguments, got {arg_count}")

            if min_arg_count is not None and arg_count < min_arg_count:
                raise ValueError(f"{e.this} expects at least {min_arg_count} arguments, got {arg_count}")

            if max_arg_count is not None and arg_count > max_arg_count:
                raise ValueError(f"{e.this} expects at most {max_arg_count} arguments, got {arg_count}")

            def format_arg(arg):
                if isinstance(arg, exp.Literal) and isinstance(arg.this, str) and arg.this.startswith("TIMESTAMP "):
                    return arg.this
                return self.sql(arg)

            formatted_args = ", ".join(format_arg(arg) for arg in e.expressions)
            return f"{e.this.upper()}({formatted_args})"

 

        def round_sql(self, expression):
            value = expression.this
            arg = expression.args.get("decimals")

            datetime_units = {
                "CC", "SCC", "YYYY", "SYYY", "YEAR", "SYEAR", "YYY", "YY", "Y",
                "IYYY", "IYY", "IY", "I", "Q", "MONTH", "MON", "MM", "RM",
                "WW", "IW", "W", "DDD", "DD", "J", "DAY", "DY", "D",
                "HH", "HH24", "HH12", "MI", "SS",
            }

            def is_datetime_literal(lit):
                return (
                    isinstance(lit, exp.Literal)
                    and lit.is_string
                    and (lit.this.startswith("DATE ") or lit.this.startswith("TIMESTAMP "))
                )

            # Render the first argument properly
            value_sql = value.this if is_datetime_literal(value) else self.sql(value)

            if arg is None:
                return f"ROUND({value_sql})"

            # If rounding a datetime, the second arg should be a valid datetime unit
            if is_datetime_literal(value):
                unit = arg.this.strip("'").upper() if isinstance(arg, exp.Literal) else self.sql(arg)
                if unit not in datetime_units:
                    raise ValueError(f"Invalid ROUND datetime unit for Exasol: {unit}")
                return f"ROUND({value_sql}, '{unit}')"

            # Else, treat second argument as numeric (e.g., ROUND(42.123, 2))
            return f"ROUND({value_sql}, {self.sql(arg)})"

        def _timestamp_literal(self, e, key):
            arg = e.args.get(key)
            if isinstance(arg, exp.Literal) and arg.args.get("prefix"):
                # print(arg.args.get("prefix"))
                # print("The fame I waited for")
                return f"{arg.args['prefix']} '{arg.this}'"
            return self.sql(e, key)

        def alias_sql(self, expression):
            return f"{self.sql(expression, 'this')} {self.sql(expression, 'alias')}"

        def windowed_func(self, name, e):
            sql = f"{name}({self.sql(e, 'this')})"
            if e.args.get("over"):
                sql += f"{self.sql(e, 'over')}"
            return sql

        def convert_tz_sql(self, expression):
            this = self.sql(expression, "this")
            from_tz = self.sql(expression, "from_tz")
            to_tz = self.sql(expression, "to_tz")
            options = self.sql(expression, "options")
            if options:
                return f"CONVERT_TZ({this}, {from_tz}, {to_tz}, {options})"
            return f"CONVERT_TZ({this}, {from_tz}, {to_tz})"

        def convert_tz(self, e):
            args = [
                self.sql(e, "this"),
                self.sql(e, "timezone"),
                self.sql(e, "to_timezone"),
            ]
            if e.args.get("options"):
                args.append(self.sql(e, "options"))
            return f"CONVERT_TZ({', '.join(args)})"

        def cos_sql(self, expression):
            return f"COS({self.sql(expression, 'this')})"

        def cosh_sql(self, expression):
            return f"COSH({self.sql(expression, 'this')})"

        def cot_sql(self, expression):
            return f"COT({self.sql(expression, 'this')})"

        # def connect_by_sql(self, expression):
        #     prior = expression.args.get("prior")
        #     prefix = "CONNECT BY PRIOR " if prior else "CONNECT BY "
        #     return prefix + self.sql(expression, "this")

        # def start_with_sql(self, expression):
        #     return "START WITH " + self.sql(expression, "this")

        # def corr_sql(self, expression):
        #     return f"CORR({self.sql(expression, 'this')}, {self.sql(expression, 'expression')})"

        def group_concat_sql(self, e):
            order = e.args.get("order")
            order_sql = ""
            if order:
                # order is a list of Ordered expressions
                order_sql = " ORDER BY " + ", ".join(self.sql(o) for o in order)

            return (
                "GROUP_CONCAT("
                + ("DISTINCT " if e.args.get("distinct") else "")
                + self.sql(e, "this")
                + order_sql
                + (
                    f" SEPARATOR {self.sql(e, 'separator')}"
                    if e.args.get("separator")
                    else ""
                )
                + ")"
                + self.sql(e, "over")
            )

        def jsonb_extract_sql(self, e):
            args_sql = ", ".join(self.sql(arg) for arg in e.expressions)
            emits = e.args.get("emits")
            emits_sql = ""

            if emits:
                emits_sql = (
                    " EMITS("
                    + ", ".join(f"{name} {datatype}" for name, datatype in emits)
                    + ")"
                )

            return f"JSON_EXTRACT({self.sql(e, 'this')}, {args_sql}){emits_sql}"

        def convert_sql(self, expression):
            return f"CONVERT({self.sql(expression, 'this')}, {self.sql(expression, 'expression')})"

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            strtodate_sql = self.function_fallback_sql(expression)

            if not isinstance(expression.parent, exp.Cast):
                # StrToDate returns DATEs in other dialects (eg. postgres), so
                # this branch aims to improve the transpilation to clickhouse
                return self.cast_sql(exp.cast(expression, "DATE"))

            return strtodate_sql

        def cast_sql(
            self, expression: exp.Cast, safe_prefix: t.Optional[str] = None
        ) -> str:
            this = expression.this

            if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build(
                "datetime"
            ):
                return self.sql(this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        # Important method
        def trycast_sql(self, expression: exp.TryCast) -> str:
            dtype = expression.to
            if not dtype.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True):
                # Casting x into Nullable(T) appears to behave similarly to TRY_CAST(x AS T)
                dtype.set("nullable", True)

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
            # String is the standard ClickHouse type, every other variant is just an alias.
            # Additionally, any supplied length parameter will be ignored.
            #
            # https://clickhouse.com/docs/en/sql-reference/data-types/string
            if expression.this in self.STRING_TYPE_MAPPING:
                # print(self.STRING_TYPE_MAPPING[expression.this])
                dtype = self.STRING_TYPE_MAPPING[expression.this]
            else:
                dtype = super().datatype_sql(expression)

            # This section changes the type to `Nullable(...)` if the following conditions hold:
            # - It's marked as nullable - this ensures we won't wrap ClickHouse types with `Nullable`
            #   and change their semantics
            # - It's not the key type of a `Map`. This is because ClickHouse enforces the following
            #   constraint: "Type of Map key must be a type, that can be represented by integer or
            #   String or FixedString (possibly LowCardinality) or UUID or IPv6"
            # - It's not a composite type, e.g. `Nullable(Array(...))` is not a valid type
            # parent = expression.parent

            # nullable = expression.args.get("nullable")
            # if nullable is True or (
            #     nullable is None
            #     and not (
            #         isinstance(parent, exp.DataType)
            #         and parent.is_type(exp.DataType.Type.MAP, check_nullable=True)
            #         and expression.index in (None, 0)
            #     )
            #     and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
            # ):
            #     dtype = f"Nullable({dtype})"

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
                    self.seg("SETTINGS ")
                    + self.expressions(expression, key="settings", flat=True)
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

        def oncluster_sql(self, expression: exp.OnCluster) -> str:
            return f"ON CLUSTER {self.sql(expression, 'this')}"

        def createable_sql(
            self, expression: exp.Create, locations: t.DefaultDict
        ) -> str:
            if expression.kind in self.ON_CLUSTER_TARGETS and locations.get(
                exp.Properties.Location.POST_NAME
            ):
                this_name = self.sql(
                    (
                        expression.this
                        if isinstance(expression.this, exp.Schema)
                        else expression
                    ),
                    "this",
                )
                this_properties = " ".join(
                    [
                        self.sql(prop)
                        for prop in locations[exp.Properties.Location.POST_NAME]
                    ]
                )
                this_schema = self.schema_columns_sql(expression.this)
                this_schema = f"{self.sep()}{this_schema}" if this_schema else ""

                return f"{this_name}{self.sep()}{this_properties}{this_schema}"

            return super().createable_sql(expression, locations)

        def create_sql(self, expression: exp.Create) -> str:
            # The comment property comes last in CTAS statements, i.e. after the query
            query = expression.expression
            if isinstance(query, exp.Query):
                comment_prop = expression.find(exp.SchemaCommentProperty)
                if comment_prop:
                    comment_prop.pop()
                    query.replace(exp.paren(query))
            else:
                comment_prop = None

            create_sql = super().create_sql(expression)

            comment_sql = self.sql(comment_prop)
            comment_sql = f" {comment_sql}" if comment_sql else ""

            return f"{create_sql}{comment_sql}"

        def prewhere_sql(self, expression: exp.PreWhere) -> str:
            this = self.indent(self.sql(expression, "this"))
            return f"{self.seg('PREWHERE')}{self.sep()}{this}"

        def indexcolumnconstraint_sql(
            self, expression: exp.IndexColumnConstraint
        ) -> str:
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
            return f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"

        def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
            return f"PROJECTION {self.sql(expression.this)} {self.wrap(expression.expression)}"

        def is_sql(self, expression: exp.Is) -> str:
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            return is_sql

        def in_sql(self, expression: exp.In) -> str:
            in_sql = super().in_sql(expression)

            if isinstance(expression.parent, exp.Not) and expression.args.get(
                "is_global"
            ):
                in_sql = in_sql.replace("GLOBAL IN", "GLOBAL NOT IN", 1)

            return in_sql

        def not_sql(self, expression: exp.Not) -> str:
            if isinstance(expression.this, exp.In) and expression.this.args.get(
                "is_global"
            ):
                # let `GLOBAL IN` child interpose `NOT`
                return self.sql(expression, "this")

            return super().not_sql(expression)
