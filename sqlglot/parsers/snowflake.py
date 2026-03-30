from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.trie import new_trie
from sqlglot.dialects.dialect import (
    Dialect,
    build_default_decimal_type,
    build_formatted_time,
    build_like,
    build_replace_with_optional_replacement,
    build_timetostr_or_tochar,
    build_trunc,
    binary_from_function,
    date_trunc_to_time,
    map_date_part,
)
from sqlglot.helper import is_date_unit, is_int, seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import B, E
    from collections.abc import Collection


def _build_approx_top_k(args: t.List) -> exp.ApproxTopK:
    """
    Normalizes APPROX_TOP_K arguments to match Snowflake semantics.

    Snowflake APPROX_TOP_K signature: APPROX_TOP_K(column [, k] [, counters])
    - k defaults to 1 if omitted (per Snowflake documentation)
    - counters is optional precision parameter
    """
    # Add default k=1 if only column is provided
    if len(args) == 1:
        args.append(exp.Literal.number(1))

    return exp.ApproxTopK.from_arg_list(args)


def _build_date_from_parts(args: t.List) -> exp.DateFromParts:
    return exp.DateFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        allow_overflow=True,
    )


# Timestamp types used in _build_datetime
TIMESTAMP_TYPES = {
    exp.DType.TIMESTAMP: "TO_TIMESTAMP",
    exp.DType.TIMESTAMPLTZ: "TO_TIMESTAMP_LTZ",
    exp.DType.TIMESTAMPNTZ: "TO_TIMESTAMP_NTZ",
    exp.DType.TIMESTAMPTZ: "TO_TIMESTAMP_TZ",
}


def _build_datetime(
    name: str, kind: exp.DType, safe: bool = False
) -> t.Callable[[t.List], exp.Func]:
    def _builder(args: t.List) -> exp.Func:
        value = seq_get(args, 0)
        scale_or_fmt = seq_get(args, 1)

        int_value = value is not None and is_int(value.name)
        int_scale_or_fmt = scale_or_fmt is not None and scale_or_fmt.is_int

        if isinstance(value, (exp.Literal, exp.Neg)) or (value and scale_or_fmt):
            # Converts calls like `TO_TIME('01:02:03')` into casts
            if len(args) == 1 and value.is_string and not int_value:
                return (
                    exp.TryCast(this=value, to=exp.DataType.build(kind), requires_string=True)
                    if safe
                    else exp.cast(value, kind)
                )

            # Handles `TO_TIMESTAMP(str, fmt)` and `TO_TIMESTAMP(num, scale)` as special
            # cases so we can transpile them, since they're relatively common
            if kind in TIMESTAMP_TYPES:
                if not safe and (int_scale_or_fmt or (int_value and scale_or_fmt is None)):
                    # TRY_TO_TIMESTAMP('integer') is not parsed into exp.UnixToTime as
                    # it's not easily transpilable. Also, numeric-looking strings with
                    # format strings (e.g., TO_TIMESTAMP('20240115', 'YYYYMMDD')) should
                    # use StrToTime, not UnixToTime.
                    unix_expr = exp.UnixToTime(this=value, scale=scale_or_fmt)
                    unix_expr.set("target_type", exp.DataType.build(kind, dialect="snowflake"))
                    return unix_expr
                if scale_or_fmt and not int_scale_or_fmt:
                    # Format string provided (e.g., 'YYYY-MM-DD'), use StrToTime
                    strtotime_expr = build_formatted_time(exp.StrToTime, "snowflake")(args)
                    strtotime_expr.set("safe", safe)
                    strtotime_expr.set("target_type", exp.DataType.build(kind, dialect="snowflake"))
                    return strtotime_expr

        # Handle DATE/TIME with format strings - allow int_value if a format string is provided
        has_format_string = scale_or_fmt and not int_scale_or_fmt
        if kind in (exp.DType.DATE, exp.DType.TIME) and (not int_value or has_format_string):
            klass = exp.TsOrDsToDate if kind == exp.DType.DATE else exp.TsOrDsToTime
            formatted_exp = build_formatted_time(klass, "snowflake")(args)
            formatted_exp.set("safe", safe)
            return formatted_exp

        return exp.Anonymous(this=name, expressions=args)

    return _builder


def _build_bitwise(expr_type: t.Type[B], name: str) -> t.Callable[[t.List], B | exp.Anonymous]:
    def _builder(args: t.List) -> B | exp.Anonymous:
        if len(args) == 3:
            # Special handling for bitwise operations with padside argument
            if expr_type in (exp.BitwiseAnd, exp.BitwiseOr, exp.BitwiseXor):
                return expr_type(
                    this=seq_get(args, 0), expression=seq_get(args, 1), padside=seq_get(args, 2)
                )
            return exp.Anonymous(this=name, expressions=args)

        result = binary_from_function(expr_type)(args)

        # Snowflake specifies INT128 for bitwise shifts
        if expr_type in (exp.BitwiseLeftShift, exp.BitwiseRightShift):
            result.set("requires_int128", True)

        return result

    return _builder


# https://docs.snowflake.com/en/sql-reference/functions/div0
def _build_if_from_div0(args: t.List) -> exp.If:
    lhs = exp._wrap(seq_get(args, 0), exp.Binary)
    rhs = exp._wrap(seq_get(args, 1), exp.Binary)

    cond = exp.EQ(this=rhs, expression=exp.Literal.number(0)).and_(
        exp.Is(this=lhs, expression=exp.null()).not_()
    )
    true = exp.Literal.number(0)
    false = exp.Div(this=lhs, expression=rhs)
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/div0null
def _build_if_from_div0null(args: t.List) -> exp.If:
    lhs = exp._wrap(seq_get(args, 0), exp.Binary)
    rhs = exp._wrap(seq_get(args, 1), exp.Binary)

    # Returns 0 when divisor is 0 OR NULL
    cond = exp.EQ(this=rhs, expression=exp.Literal.number(0)).or_(
        exp.Is(this=rhs, expression=exp.null())
    )
    true = exp.Literal.number(0)
    false = exp.Div(this=lhs, expression=rhs)
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


def _build_search(args: t.List) -> exp.Search:
    kwargs = {
        "this": seq_get(args, 0),
        "expression": seq_get(args, 1),
        **{arg.name.lower(): arg for arg in args[2:] if isinstance(arg, exp.Kwarg)},
    }
    return exp.Search(**kwargs)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


def _build_regexp_replace(args: t.List) -> exp.RegexpReplace:
    regexp_replace = exp.RegexpReplace.from_arg_list(args)

    if not regexp_replace.args.get("replacement"):
        regexp_replace.set("replacement", exp.Literal.string(""))

    return regexp_replace


def _build_regexp_like(args: t.List) -> exp.RegexpLike:
    return exp.RegexpLike(
        this=seq_get(args, 0),
        expression=seq_get(args, 1),
        flag=seq_get(args, 2),
        full_match=True,
    )


def _date_trunc_to_time(args: t.List) -> exp.DateTrunc | exp.TimestampTrunc:
    trunc = date_trunc_to_time(args)
    unit = map_date_part(trunc.args["unit"])
    trunc.set("unit", unit)
    is_time_input = trunc.this.is_type(exp.DType.TIME, exp.DType.TIMETZ)
    if (isinstance(trunc, exp.TimestampTrunc) and is_date_unit(unit) or is_time_input) or (
        isinstance(trunc, exp.DateTrunc) and not is_date_unit(unit)
    ):
        trunc.set("input_type_preserved", True)
    return trunc


def _build_regexp_extract(expr_type: t.Type[E]) -> t.Callable[[t.List, Dialect], E]:
    def _builder(args: t.List, dialect: Dialect) -> E:
        return expr_type(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            position=seq_get(args, 2),
            occurrence=seq_get(args, 3),
            parameters=seq_get(args, 4),
            group=seq_get(args, 5) or exp.Literal.number(0),
            **(
                {"null_if_pos_overflow": dialect.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL}
                if expr_type is exp.RegexpExtract
                else {}
            ),
        )

    return _builder


def _build_timestamp_from_parts(args: t.List) -> exp.Func:
    """Build TimestampFromParts with support for both syntaxes:
    1. TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second [, nanosecond] [, time_zone])
    2. TIMESTAMP_FROM_PARTS(date_expr, time_expr) - Snowflake specific
    """
    if len(args) == 2:
        return exp.TimestampFromParts(this=seq_get(args, 0), expression=seq_get(args, 1))

    return exp.TimestampFromParts.from_arg_list(args)


def _build_round(args: t.List) -> exp.Round:
    """
    Build Round expression, unwrapping Snowflake's named parameters.

    Maps EXPR => this, SCALE => decimals, ROUNDING_MODE => truncate.

    Note: Snowflake does not support mixing named and positional arguments.
    Arguments are either all named or all positional.
    """
    kwarg_map = {"EXPR": "this", "SCALE": "decimals", "ROUNDING_MODE": "truncate"}
    round_args = {}
    positional_keys = ["this", "decimals", "truncate"]
    positional_idx = 0

    for arg in args:
        if isinstance(arg, exp.Kwarg):
            key = arg.this.name.upper()
            round_key = kwarg_map.get(key)
            if round_key:
                round_args[round_key] = arg.expression
        else:
            if positional_idx < len(positional_keys):
                round_args[positional_keys[positional_idx]] = arg
                positional_idx += 1

    expression = exp.Round(**round_args)
    expression.set("casts_non_integer_decimals", True)
    return expression


def _build_array_sort(args: t.List) -> exp.SortArray:
    asc = seq_get(args, 1)
    nulls_first = seq_get(args, 2)
    if nulls_first is None and isinstance(asc, exp.Boolean):
        nulls_first = exp.Boolean(this=not asc.this)
    return exp.SortArray(this=seq_get(args, 0), asc=asc, nulls_first=nulls_first)


def _build_generator(args: t.List) -> exp.Generator:
    """
    Build Generator expression, unwrapping Snowflake's named parameters.

    Maps ROWCOUNT => rowcount, TIMELIMIT => timelimit.
    """
    kwarg_map = {"ROWCOUNT": "rowcount", "TIMELIMIT": "timelimit"}
    gen_args = {}

    positional_keys = ("rowcount", "timelimit")

    for i, arg in enumerate(args):
        if isinstance(arg, exp.Kwarg):
            key = arg.this.name.upper()
            gen_key = kwarg_map.get(key)
            if gen_key:
                gen_args[gen_key] = arg.expression
        elif i < len(positional_keys):
            gen_args[positional_keys[i]] = arg

    return exp.Generator(**gen_args)


def _build_try_to_number(args: t.List[exp.Expr]) -> exp.Expr:
    return exp.ToNumber(
        this=seq_get(args, 0),
        format=seq_get(args, 1),
        precision=seq_get(args, 2),
        scale=seq_get(args, 3),
        safe=True,
    )


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[SnowflakeParser], exp.Show]:
    def _parse(self: SnowflakeParser) -> exp.Show:
        return self._parse_show_snowflake(*args, **kwargs)

    return _parse


class SnowflakeParser(parser.Parser):
    IDENTIFY_PIVOT_STRINGS = True
    TYPED_LAMBDA_ARGS = True
    DEFAULT_SAMPLING_METHOD = "BERNOULLI"
    COLON_IS_VARIANT_EXTRACT = True
    JSON_EXTRACT_REQUIRES_JSON_EXPRESSION = True

    TYPE_TOKENS = {*parser.Parser.TYPE_TOKENS, TokenType.FILE}
    STRUCT_TYPE_TOKENS = {*parser.Parser.STRUCT_TYPE_TOKENS, TokenType.FILE}
    NESTED_TYPE_TOKENS = {*parser.Parser.NESTED_TYPE_TOKENS, TokenType.FILE}

    ID_VAR_TOKENS = {
        *parser.Parser.ID_VAR_TOKENS,
        TokenType.EXCEPT,
        TokenType.INTEGRATION,
        TokenType.MATCH_CONDITION,
        TokenType.PACKAGE,
        TokenType.POLICY,
        TokenType.POOL,
        TokenType.ROLE,
        TokenType.RULE,
        TokenType.VOLUME,
    }

    ALIAS_TOKENS = parser.Parser.ALIAS_TOKENS | {
        TokenType.INTEGRATION,
        TokenType.PACKAGE,
        TokenType.POLICY,
        TokenType.POOL,
        TokenType.ROLE,
        TokenType.RULE,
        TokenType.VOLUME,
    }

    TABLE_ALIAS_TOKENS = (
        parser.Parser.TABLE_ALIAS_TOKENS
        | {
            TokenType.ANTI,
            TokenType.INTEGRATION,
            TokenType.PACKAGE,
            TokenType.POLICY,
            TokenType.POOL,
            TokenType.ROLE,
            TokenType.RULE,
            TokenType.SEMI,
            TokenType.VOLUME,
            TokenType.WINDOW,
        }
    ) - {TokenType.MATCH_CONDITION}

    COLON_PLACEHOLDER_TOKENS = ID_VAR_TOKENS | {TokenType.NUMBER}

    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.LOCALTIME: exp.Localtime,
        TokenType.LOCALTIMESTAMP: exp.Localtimestamp,
        TokenType.CURRENT_TIME: exp.Localtime,
    }

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.RLIKE: lambda self, this: self.expression(
            exp.RegexpLike(this=this, expression=self._parse_bitwise(), full_match=True)
        ),
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "CHARINDEX": lambda args: exp.StrPosition(
            this=seq_get(args, 1),
            substr=seq_get(args, 0),
            position=seq_get(args, 2),
            clamp_position=True,
        ),
        "ADD_MONTHS": lambda args: exp.AddMonths(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            preserve_end_of_month=True,
        ),
        "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
        "CURRENT_TIME": lambda args: exp.Localtime(this=seq_get(args, 0)),
        "APPROX_TOP_K": _build_approx_top_k,
        "ARRAY_CONSTRUCT": lambda args: exp.Array(expressions=args),
        "ARRAY_CONTAINS": lambda args: exp.ArrayContains(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            ensure_variant=False,
            check_null=True,
        ),
        "ARRAY_DISTINCT": lambda args: exp.ArrayDistinct(
            this=seq_get(args, 0),
            check_null=True,
        ),
        "ARRAY_GENERATE_RANGE": lambda args: exp.GenerateSeries(
            # Snowflake has exclusive end semantics
            start=seq_get(args, 0),
            end=seq_get(args, 1),
            step=seq_get(args, 2),
            is_end_exclusive=True,
        ),
        "ARRAY_EXCEPT": lambda args: exp.ArrayExcept(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            is_multiset=True,
        ),
        "ARRAY_INTERSECTION": lambda args: exp.ArrayIntersect(
            expressions=args,
            is_multiset=True,
        ),
        "ARRAY_POSITION": lambda args: exp.ArrayPosition(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            zero_based=True,
        ),
        "ARRAY_SLICE": lambda args: exp.ArraySlice(
            this=seq_get(args, 0),
            start=seq_get(args, 1),
            end=seq_get(args, 2),
            zero_based=True,
        ),
        "ARRAY_SORT": _build_array_sort,
        "ARRAY_FLATTEN": exp.Flatten.from_arg_list,
        "ARRAY_TO_STRING": lambda args: exp.ArrayToString(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            null_is_empty=True,
            null_delim_is_null=True,
        ),
        "ARRAYS_OVERLAP": lambda args: exp.ArrayOverlaps(
            this=seq_get(args, 0), expression=seq_get(args, 1), null_safe=True
        ),
        "BITAND": _build_bitwise(exp.BitwiseAnd, "BITAND"),
        "BIT_AND": _build_bitwise(exp.BitwiseAnd, "BITAND"),
        "BITNOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
        "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
        "BITXOR": _build_bitwise(exp.BitwiseXor, "BITXOR"),
        "BIT_XOR": _build_bitwise(exp.BitwiseXor, "BITXOR"),
        "BITOR": _build_bitwise(exp.BitwiseOr, "BITOR"),
        "BIT_OR": _build_bitwise(exp.BitwiseOr, "BITOR"),
        "BITSHIFTLEFT": _build_bitwise(exp.BitwiseLeftShift, "BITSHIFTLEFT"),
        "BIT_SHIFTLEFT": _build_bitwise(exp.BitwiseLeftShift, "BIT_SHIFTLEFT"),
        "BITSHIFTRIGHT": _build_bitwise(exp.BitwiseRightShift, "BITSHIFTRIGHT"),
        "BIT_SHIFTRIGHT": _build_bitwise(exp.BitwiseRightShift, "BIT_SHIFTRIGHT"),
        "BITANDAGG": exp.BitwiseAndAgg.from_arg_list,
        "BITAND_AGG": exp.BitwiseAndAgg.from_arg_list,
        "BIT_AND_AGG": exp.BitwiseAndAgg.from_arg_list,
        "BIT_ANDAGG": exp.BitwiseAndAgg.from_arg_list,
        "BITORAGG": exp.BitwiseOrAgg.from_arg_list,
        "BITOR_AGG": exp.BitwiseOrAgg.from_arg_list,
        "BIT_OR_AGG": exp.BitwiseOrAgg.from_arg_list,
        "BIT_ORAGG": exp.BitwiseOrAgg.from_arg_list,
        "BITXORAGG": exp.BitwiseXorAgg.from_arg_list,
        "BITXOR_AGG": exp.BitwiseXorAgg.from_arg_list,
        "BIT_XOR_AGG": exp.BitwiseXorAgg.from_arg_list,
        "BIT_XORAGG": exp.BitwiseXorAgg.from_arg_list,
        "BITMAP_OR_AGG": exp.BitmapOrAgg.from_arg_list,
        "BOOLAND": lambda args: exp.Booland(
            this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
        ),
        "BOOLOR": lambda args: exp.Boolor(
            this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
        ),
        "BOOLNOT": lambda args: exp.Boolnot(this=seq_get(args, 0), round_input=True),
        "BOOLXOR": lambda args: exp.Xor(
            this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
        ),
        "CORR": lambda args: exp.Corr(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            null_on_zero_variance=True,
        ),
        "DATE": _build_datetime("DATE", exp.DType.DATE),
        "DATEFROMPARTS": _build_date_from_parts,
        "DATE_FROM_PARTS": _build_date_from_parts,
        "DATE_TRUNC": _date_trunc_to_time,
        "DATEADD": lambda args: exp.DateAdd(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
        ),
        "DATEDIFF": lambda args: exp.DateDiff(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
            date_part_boundary=True,
        ),
        "DAYNAME": lambda args: exp.Dayname(this=seq_get(args, 0), abbreviated=True),
        "DAYOFWEEKISO": exp.DayOfWeekIso.from_arg_list,
        "DIV0": _build_if_from_div0,
        "DIV0NULL": _build_if_from_div0null,
        "EDITDISTANCE": lambda args: exp.Levenshtein(
            this=seq_get(args, 0), expression=seq_get(args, 1), max_dist=seq_get(args, 2)
        ),
        "FLATTEN": exp.Explode.from_arg_list,
        "GENERATOR": _build_generator,
        "GET": exp.GetExtract.from_arg_list,
        "GETDATE": exp.CurrentTimestamp.from_arg_list,
        "GET_PATH": lambda args, dialect: exp.JSONExtract(
            this=seq_get(args, 0),
            expression=dialect.to_json_path(seq_get(args, 1)),
            requires_json=True,
        ),
        "GREATEST_IGNORE_NULLS": lambda args: exp.Greatest(
            this=seq_get(args, 0), expressions=args[1:], ignore_nulls=True
        ),
        "LEAST_IGNORE_NULLS": lambda args: exp.Least(
            this=seq_get(args, 0), expressions=args[1:], ignore_nulls=True
        ),
        "HEX_DECODE_BINARY": exp.Unhex.from_arg_list,
        "IFF": exp.If.from_arg_list,
        "JAROWINKLER_SIMILARITY": lambda args: exp.JarowinklerSimilarity(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            case_insensitive=True,
        ),
        "MD5_HEX": exp.MD5.from_arg_list,
        "MD5_BINARY": exp.MD5Digest.from_arg_list,
        "MD5_NUMBER_LOWER64": exp.MD5NumberLower64.from_arg_list,
        "MD5_NUMBER_UPPER64": exp.MD5NumberUpper64.from_arg_list,
        "MONTHNAME": lambda args: exp.Monthname(this=seq_get(args, 0), abbreviated=True),
        "LAST_DAY": lambda args: exp.LastDay(
            this=seq_get(args, 0), unit=map_date_part(seq_get(args, 1))
        ),
        "LEN": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
        "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
        "LOCALTIMESTAMP": exp.CurrentTimestamp.from_arg_list,
        "NULLIFZERO": _build_if_from_nullifzero,
        "OBJECT_CONSTRUCT": lambda args: build_object_construct(args),
        "OBJECT_KEYS": exp.JSONKeys.from_arg_list,
        "OCTET_LENGTH": exp.ByteLength.from_arg_list,
        "PARSE_URL": lambda args: exp.ParseUrl(this=seq_get(args, 0), permissive=seq_get(args, 1)),
        "REGEXP_EXTRACT_ALL": _build_regexp_extract(exp.RegexpExtractAll),
        "REGEXP_LIKE": _build_regexp_like,
        "REGEXP_REPLACE": _build_regexp_replace,
        "REGEXP_SUBSTR": _build_regexp_extract(exp.RegexpExtract),
        "REGEXP_SUBSTR_ALL": _build_regexp_extract(exp.RegexpExtractAll),
        "RANDOM": lambda args: exp.Rand(
            this=seq_get(args, 0),
            lower=exp.Literal.number(-9223372036854775808.0),  # -2^63 as float to avoid overflow
            upper=exp.Literal.number(9223372036854775807.0),  # 2^63-1 as float
        ),
        "REPLACE": build_replace_with_optional_replacement,
        "RLIKE": _build_regexp_like,
        "ROUND": _build_round,
        "SHA1_BINARY": exp.SHA1Digest.from_arg_list,
        "SHA1_HEX": exp.SHA.from_arg_list,
        "SHA2_BINARY": exp.SHA2Digest.from_arg_list,
        "SHA2_HEX": exp.SHA2.from_arg_list,
        "SPLIT": lambda args: exp.Split(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            null_returns_null=True,
            empty_delimiter_returns_whole=True,
        ),
        "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
        "STDDEV_SAMP": exp.Stddev.from_arg_list,
        "SYSDATE": lambda args: exp.CurrentTimestamp(this=seq_get(args, 0), sysdate=True),
        "TABLE": lambda args: exp.TableFromRows(this=seq_get(args, 0)),
        "TIMEADD": lambda args: exp.TimeAdd(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
        ),
        "TIMEDIFF": lambda args: exp.DateDiff(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
            date_part_boundary=True,
        ),
        "TIME_FROM_PARTS": lambda args: exp.TimeFromParts(
            hour=seq_get(args, 0),
            min=seq_get(args, 1),
            sec=seq_get(args, 2),
            nano=seq_get(args, 3),
            overflow=True,
        ),
        "TIMESTAMPADD": lambda args: exp.DateAdd(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
        ),
        "TIMESTAMPDIFF": lambda args: exp.DateDiff(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
            date_part_boundary=True,
        ),
        "TIMESTAMPFROMPARTS": _build_timestamp_from_parts,
        "TIMESTAMP_FROM_PARTS": _build_timestamp_from_parts,
        "TIMESTAMPNTZFROMPARTS": _build_timestamp_from_parts,
        "TIMESTAMP_NTZ_FROM_PARTS": _build_timestamp_from_parts,
        "TRUNC": lambda args, dialect: build_trunc(args, dialect, date_trunc_requires_part=False),
        "TRUNCATE": lambda args, dialect: build_trunc(
            args, dialect, date_trunc_requires_part=False
        ),
        "TRY_DECRYPT": lambda args: exp.Decrypt(
            this=seq_get(args, 0),
            passphrase=seq_get(args, 1),
            aad=seq_get(args, 2),
            encryption_method=seq_get(args, 3),
            safe=True,
        ),
        "TRY_DECRYPT_RAW": lambda args: exp.DecryptRaw(
            this=seq_get(args, 0),
            key=seq_get(args, 1),
            iv=seq_get(args, 2),
            aad=seq_get(args, 3),
            encryption_method=seq_get(args, 4),
            aead=seq_get(args, 5),
            safe=True,
        ),
        "TRY_PARSE_JSON": lambda args: exp.ParseJSON(this=seq_get(args, 0), safe=True),
        "TRY_TO_BINARY": lambda args: exp.ToBinary(
            this=seq_get(args, 0), format=seq_get(args, 1), safe=True
        ),
        "TRY_TO_BOOLEAN": lambda args: exp.ToBoolean(this=seq_get(args, 0), safe=True),
        "TRY_TO_DATE": _build_datetime("TRY_TO_DATE", exp.DType.DATE, safe=True),
        **dict.fromkeys(
            ("TRY_TO_DECIMAL", "TRY_TO_NUMBER", "TRY_TO_NUMERIC"), _build_try_to_number
        ),
        "TRY_TO_DOUBLE": lambda args: exp.ToDouble(
            this=seq_get(args, 0), format=seq_get(args, 1), safe=True
        ),
        "TRY_TO_FILE": lambda args: exp.ToFile(
            this=seq_get(args, 0), path=seq_get(args, 1), safe=True
        ),
        "TRY_TO_TIME": _build_datetime("TRY_TO_TIME", exp.DType.TIME, safe=True),
        "TRY_TO_TIMESTAMP": _build_datetime("TRY_TO_TIMESTAMP", exp.DType.TIMESTAMP, safe=True),
        "TRY_TO_TIMESTAMP_LTZ": _build_datetime(
            "TRY_TO_TIMESTAMP_LTZ", exp.DType.TIMESTAMPLTZ, safe=True
        ),
        "TRY_TO_TIMESTAMP_NTZ": _build_datetime(
            "TRY_TO_TIMESTAMP_NTZ", exp.DType.TIMESTAMPNTZ, safe=True
        ),
        "TRY_TO_TIMESTAMP_TZ": _build_datetime(
            "TRY_TO_TIMESTAMP_TZ", exp.DType.TIMESTAMPTZ, safe=True
        ),
        "TO_CHAR": build_timetostr_or_tochar,
        "TO_DATE": _build_datetime("TO_DATE", exp.DType.DATE),
        **dict.fromkeys(
            ("TO_DECIMAL", "TO_NUMBER", "TO_NUMERIC"),
            lambda args: exp.ToNumber(
                this=seq_get(args, 0),
                format=seq_get(args, 1),
                precision=seq_get(args, 2),
                scale=seq_get(args, 3),
            ),
        ),
        "TO_TIME": _build_datetime("TO_TIME", exp.DType.TIME),
        "TO_TIMESTAMP": _build_datetime("TO_TIMESTAMP", exp.DType.TIMESTAMP),
        "TO_TIMESTAMP_LTZ": _build_datetime("TO_TIMESTAMP_LTZ", exp.DType.TIMESTAMPLTZ),
        "TO_TIMESTAMP_NTZ": _build_datetime("TO_TIMESTAMP_NTZ", exp.DType.TIMESTAMPNTZ),
        "TO_TIMESTAMP_TZ": _build_datetime("TO_TIMESTAMP_TZ", exp.DType.TIMESTAMPTZ),
        "TO_GEOGRAPHY": lambda args: (
            exp.cast(args[0], exp.DType.GEOGRAPHY)
            if len(args) == 1
            else exp.Anonymous(this="TO_GEOGRAPHY", expressions=args)
        ),
        "TO_GEOMETRY": lambda args: (
            exp.cast(args[0], exp.DType.GEOMETRY)
            if len(args) == 1
            else exp.Anonymous(this="TO_GEOMETRY", expressions=args)
        ),
        "TO_VARCHAR": build_timetostr_or_tochar,
        "TO_JSON": exp.JSONFormat.from_arg_list,
        "VECTOR_COSINE_SIMILARITY": exp.CosineDistance.from_arg_list,
        "VECTOR_INNER_PRODUCT": exp.DotProduct.from_arg_list,
        "VECTOR_L1_DISTANCE": exp.ManhattanDistance.from_arg_list,
        "VECTOR_L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "ZEROIFNULL": _build_if_from_zeroifnull,
        "LIKE": build_like(exp.Like),
        "ILIKE": build_like(exp.ILike),
        "SEARCH": _build_search,
        "SKEW": exp.Skewness.from_arg_list,
        "SPLIT_PART": lambda args: exp.SplitPart(
            this=seq_get(args, 0),
            delimiter=seq_get(args, 1),
            part_index=seq_get(args, 2),
            part_index_zero_as_one=True,
            empty_delimiter_returns_whole=True,
        ),
        "STRTOK": lambda args: exp.Strtok(
            this=seq_get(args, 0),
            delimiter=seq_get(args, 1) or exp.Literal.string(" "),
            part_index=seq_get(args, 2) or exp.Literal.number("1"),
        ),
        "SYSTIMESTAMP": exp.CurrentTimestamp.from_arg_list,
        "UNICODE": lambda args: exp.Unicode(this=seq_get(args, 0), empty_is_zero=True),
        "WEEKISO": exp.WeekOfYear.from_arg_list,
        "WEEKOFYEAR": exp.Week.from_arg_list,
    }
    FUNCTIONS = {k: v for k, v in FUNCTIONS.items() if k != "PREDICT"}

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        "DATE_PART": lambda self: self._parse_date_part(),
        "DIRECTORY": lambda self: self._parse_directory(),
        "OBJECT_CONSTRUCT_KEEP_NULL": lambda self: self._parse_json_object(),
        "LISTAGG": lambda self: self._parse_string_agg(),
        "SEMANTIC_VIEW": lambda self: self._parse_semantic_view(),
    }
    FUNCTION_PARSERS = {k: v for k, v in FUNCTION_PARSERS.items() if k != "TRIM"}

    TIMESTAMPS = parser.Parser.TIMESTAMPS - {TokenType.TIME}

    ALTER_PARSERS = {
        **parser.Parser.ALTER_PARSERS,
        "MODIFY": lambda self: self._parse_alter_table_alter(),
        "SESSION": lambda self: self._parse_alter_session(),
        "UNSET": lambda self: self.expression(
            exp.Set(
                tag=self._match_text_seq("TAG"),
                expressions=self._parse_csv(self._parse_id_var),
                unset=True,
            )
        ),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.GET: lambda self: self._parse_get(),
        TokenType.PUT: lambda self: self._parse_put(),
        TokenType.SHOW: lambda self: self._parse_show(),
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "CREDENTIALS": lambda self: self._parse_credentials_property(),
        "FILE_FORMAT": lambda self: self._parse_file_format_property(),
        "LOCATION": lambda self: self._parse_location_property(),
        "TAG": lambda self: self._parse_tag(),
        "USING": lambda self: (
            self._match_text_seq("TEMPLATE")
            and self.expression(exp.UsingTemplateProperty(this=self._parse_statement()))
        ),
    }

    DESCRIBE_QUALIFIER_PARSERS: t.ClassVar[t.Dict[str, t.Callable]] = {
        "API": lambda self: self.expression(exp.ApiProperty()),
        "APPLICATION": lambda self: self.expression(exp.ApplicationProperty()),
        "CATALOG": lambda self: self.expression(exp.CatalogProperty()),
        "COMPUTE": lambda self: self.expression(exp.ComputeProperty()),
        "DATABASE": lambda self: (
            self.expression(exp.DatabaseProperty())
            if self._curr and self._curr.text.upper() == "ROLE"
            else None
        ),
        "DYNAMIC": lambda self: self.expression(exp.DynamicProperty()),
        "EXTERNAL": lambda self: self.expression(exp.ExternalProperty()),
        "HYBRID": lambda self: self.expression(exp.HybridProperty()),
        "ICEBERG": lambda self: self.expression(exp.IcebergProperty()),
        "MASKING": lambda self: self.expression(exp.MaskingProperty()),
        "MATERIALIZED": lambda self: self.expression(exp.MaterializedProperty()),
        "NETWORK": lambda self: self.expression(exp.NetworkProperty()),
        "ROW": lambda self: (
            self.expression(exp.RowAccessProperty()) if self._match_text_seq("ACCESS") else None
        ),
        "SECURITY": lambda self: (
            self.expression(exp.SecurityIntegrationProperty())
            if self._curr and self._curr.text.upper() == "INTEGRATION"
            else None
        ),
    }

    TYPE_CONVERTERS = {
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric#number
        exp.DType.DECIMAL: build_default_decimal_type(precision=38, scale=0),
    }

    SHOW_PARSERS = {
        "DATABASES": _show_parser("DATABASES"),
        "SCHEMAS": _show_parser("SCHEMAS"),
        "OBJECTS": _show_parser("OBJECTS"),
        "TABLES": _show_parser("TABLES"),
        "VIEWS": _show_parser("VIEWS"),
        "PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
        "IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
        "UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
        "SEQUENCES": _show_parser("SEQUENCES"),
        "STAGES": _show_parser("STAGES"),
        "COLUMNS": _show_parser("COLUMNS"),
        "USERS": _show_parser("USERS"),
        "FILE FORMATS": _show_parser("FILE FORMATS"),
        "FUNCTIONS": _show_parser("FUNCTIONS"),
        "PROCEDURES": _show_parser("PROCEDURES"),
        "WAREHOUSES": _show_parser("WAREHOUSES"),
        "ICEBERG TABLES": _show_parser("TABLES", iceberg=True),
        "TERSE ICEBERG TABLES": _show_parser("TABLES", terse=True, iceberg=True),
        "TERSE DATABASES": _show_parser("DATABASES", terse=True),
        "TERSE SCHEMAS": _show_parser("SCHEMAS", terse=True),
        "TERSE OBJECTS": _show_parser("OBJECTS", terse=True),
        "TERSE TABLES": _show_parser("TABLES", terse=True),
        "TERSE VIEWS": _show_parser("VIEWS", terse=True),
        "TERSE SEQUENCES": _show_parser("SEQUENCES", terse=True),
        "TERSE USERS": _show_parser("USERS", terse=True),
        # TERSE has no semantic effect for KEYS, so we do not set the terse AST arg
        "TERSE PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
        "TERSE IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
        "TERSE UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
    }

    SHOW_TRIE = new_trie(key.split(" ") for key in SHOW_PARSERS)

    CONSTRAINT_PARSERS = {
        **parser.Parser.CONSTRAINT_PARSERS,
        "WITH": lambda self: self._parse_with_constraint(),
        "MASKING": lambda self: self._parse_with_constraint(),
        "PROJECTION": lambda self: self._parse_with_constraint(),
        "TAG": lambda self: self._parse_with_constraint(),
    }

    STAGED_FILE_SINGLE_TOKENS = {
        TokenType.DOT,
        TokenType.MOD,
        TokenType.SLASH,
    }

    FLATTEN_COLUMNS = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]

    SCHEMA_KINDS = {"OBJECTS", "TABLES", "VIEWS", "SEQUENCES", "UNIQUE KEYS", "IMPORTED KEYS"}

    NON_TABLE_CREATABLES = {"STORAGE INTEGRATION", "TAG", "WAREHOUSE", "STREAMLIT"}

    CREATABLES = {
        *parser.Parser.CREATABLES,
        TokenType.INTEGRATION,
        TokenType.PACKAGE,
        TokenType.POLICY,
        TokenType.POOL,
        TokenType.ROLE,
        TokenType.RULE,
        TokenType.VOLUME,
    }

    LAMBDAS = {
        **parser.Parser.LAMBDAS,
        TokenType.ARROW: lambda self, expressions: self.expression(
            exp.Lambda(
                this=self._replace_lambda(
                    self._parse_assignment(),
                    expressions,
                ),
                expressions=[e.this if isinstance(e, exp.Cast) else e for e in expressions],
            )
        ),
    }

    COLUMN_OPERATORS = {
        **parser.Parser.COLUMN_OPERATORS,
        TokenType.EXCLAMATION: lambda self, this, attr: self.expression(
            exp.ModelAttribute(this=this, expression=attr)
        ),
    }

    def _parse_directory(self) -> exp.DirectoryStage:
        table = self._parse_table_parts()
        this = table.this if isinstance(table, exp.Table) else table
        return self.expression(exp.DirectoryStage(this=this))

    def _parse_describe(self) -> exp.Describe:
        index = self._index

        if self._match_texts(self.DESCRIBE_QUALIFIER_PARSERS):
            qualifier = self.DESCRIBE_QUALIFIER_PARSERS[self._prev.text.upper()](self)

            if qualifier:
                kind = self._match_set(self.CREATABLES) and self._prev.text.upper()

                if kind:
                    this = self._parse_table(schema=True)
                    properties = self.expression(exp.Properties(expressions=[qualifier]))
                    post_props = self._parse_properties()
                    expressions = post_props.expressions if post_props else None
                    return self.expression(
                        exp.Describe(
                            this=this,
                            kind=kind,
                            properties=properties,
                            expressions=expressions,
                        )
                    )

        self._retreat(index)
        return super()._parse_describe()

    def _parse_use(self) -> exp.Use:
        if self._match_text_seq("SECONDARY", "ROLES"):
            this = self._match_texts(("ALL", "NONE")) and exp.var(self._prev.text.upper())
            roles = None if this else self._parse_csv(lambda: self._parse_table(schema=False))
            return self.expression(exp.Use(kind="SECONDARY ROLES", this=this, expressions=roles))

        return super()._parse_use()

    def _negate_range(self, this: t.Optional[exp.Expr] = None) -> t.Optional[exp.Expr]:
        if not this:
            return this

        query = this.args.get("query")
        if isinstance(this, exp.In) and isinstance(query, exp.Query):
            # Snowflake treats `value NOT IN (subquery)` as `VALUE <> ALL (subquery)`, so
            # we do this conversion here to avoid parsing it into `NOT value IN (subquery)`
            # which can produce different results (most likely a SnowFlake bug).
            #
            # https://docs.snowflake.com/en/sql-reference/functions/in
            # Context: https://github.com/tobymao/sqlglot/issues/3890
            return self.expression(exp.NEQ(this=this.this, expression=exp.All(this=query.unnest())))

        return self.expression(exp.Not(this=this))

    def _parse_tag(self) -> exp.Tags:
        return self.expression(exp.Tags(expressions=self._parse_wrapped_csv(self._parse_property)))

    def _parse_with_constraint(self) -> t.Optional[exp.Expr]:
        if self._prev.token_type != TokenType.WITH:
            self._retreat(self._index - 1)

        if self._match_text_seq("MASKING", "POLICY"):
            policy = self._parse_column()
            return self.expression(
                exp.MaskingPolicyColumnConstraint(
                    this=policy.to_dot() if isinstance(policy, exp.Column) else policy,
                    expressions=self._match(TokenType.USING)
                    and self._parse_wrapped_csv(self._parse_id_var),
                )
            )
        if self._match_text_seq("PROJECTION", "POLICY"):
            policy = self._parse_column()
            return self.expression(
                exp.ProjectionPolicyColumnConstraint(
                    this=policy.to_dot() if isinstance(policy, exp.Column) else policy
                )
            )
        if self._match(TokenType.TAG):
            return self._parse_tag()

        return None

    def _parse_with_property(self) -> t.Optional[exp.Expr] | t.List[exp.Expr]:
        if self._match(TokenType.TAG):
            return self._parse_tag()

        return super()._parse_with_property()

    def _parse_create(self) -> exp.Create | exp.Command:
        expression = super()._parse_create()
        if isinstance(expression, exp.Create) and expression.kind in self.NON_TABLE_CREATABLES:
            # Replace the Table node with the enclosed Identifier
            expression.this.replace(expression.this.this)

        return expression

    # https://docs.snowflake.com/en/sql-reference/functions/date_part.html
    # https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
    def _parse_date_part(self) -> t.Optional[exp.Expr]:
        this = self._parse_var() or self._parse_type()

        if not this:
            return None

        # Handle both syntaxes: DATE_PART(part, expr) and DATE_PART(part FROM expr)
        expression = self._match_set((TokenType.FROM, TokenType.COMMA)) and self._parse_bitwise()
        return self.expression(
            exp.Extract(this=map_date_part(this, self.dialect), expression=expression)
        )

    def _parse_bracket_key_value(self, is_map: bool = False) -> t.Optional[exp.Expr]:
        if is_map:
            # Keys are strings in Snowflake's objects, see also:
            # - https://docs.snowflake.com/en/sql-reference/data-types-semistructured
            # - https://docs.snowflake.com/en/sql-reference/functions/object_construct
            return self._parse_slice(self._parse_string()) or self._parse_assignment()

        return self._parse_slice(self._parse_alias(self._parse_assignment(), explicit=True))

    def _parse_lateral(self) -> t.Optional[exp.Lateral]:
        lateral = super()._parse_lateral()
        if not lateral:
            return lateral

        if isinstance(lateral.this, exp.Explode):
            table_alias = lateral.args.get("alias")
            columns = [exp.to_identifier(col) for col in self.FLATTEN_COLUMNS]
            if table_alias and not table_alias.args.get("columns"):
                table_alias.set("columns", columns)
            elif not table_alias:
                exp.alias_(lateral, "_flattened", table=columns, copy=False)

        return lateral

    def _parse_table_parts(
        self,
        schema: bool = False,
        is_db_reference: bool = False,
        wildcard: bool = False,
        fast: bool = False,
    ) -> t.Optional[exp.Table | exp.Dot]:
        # https://docs.snowflake.com/en/user-guide/querying-stage
        if self._match(TokenType.STRING, advance=False):
            table = self._parse_string()
        elif self._match_text_seq("@", advance=False):
            table = self._parse_location_path()
        else:
            table = None

        if table:
            file_format = None
            pattern = None

            wrapped = self._match(TokenType.L_PAREN)
            while self._curr and wrapped and not self._match(TokenType.R_PAREN):
                if self._match_text_seq("FILE_FORMAT", "=>"):
                    file_format = self._parse_string() or super()._parse_table_parts(
                        is_db_reference=is_db_reference
                    )
                elif self._match_text_seq("PATTERN", "=>"):
                    pattern = self._parse_string()
                else:
                    break

                self._match(TokenType.COMMA)

            table = self.expression(exp.Table(this=table, format=file_format, pattern=pattern))
        else:
            table = super()._parse_table_parts(
                schema=schema,
                is_db_reference=is_db_reference,
                fast=fast,
            )

        return table

    def _parse_table(
        self,
        schema: bool = False,
        joins: bool = False,
        alias_tokens: t.Optional[Collection[TokenType]] = None,
        parse_bracket: bool = False,
        is_db_reference: bool = False,
        parse_partition: bool = False,
        consume_pipe: bool = False,
    ) -> t.Optional[exp.Expr]:
        table = super()._parse_table(
            schema=schema,
            joins=joins,
            alias_tokens=alias_tokens,
            parse_bracket=parse_bracket,
            is_db_reference=is_db_reference,
            parse_partition=parse_partition,
        )
        if isinstance(table, exp.Table) and isinstance(table.this, exp.TableFromRows):
            table_from_rows = table.this
            for arg in exp.TableFromRows.arg_types:
                if arg != "this":
                    table_from_rows.set(arg, table.args.get(arg))

            table = table_from_rows

        return table

    def _parse_id_var(
        self,
        any_token: bool = True,
        tokens: t.Optional[Collection[TokenType]] = None,
    ) -> t.Optional[exp.Expr]:
        if self._match_text_seq("IDENTIFIER", "("):
            identifier = (
                super()._parse_id_var(any_token=any_token, tokens=tokens) or self._parse_string()
            )
            self._match_r_paren()
            return self.expression(exp.Anonymous(this="IDENTIFIER", expressions=[identifier]))

        return super()._parse_id_var(any_token=any_token, tokens=tokens)

    def _parse_show_snowflake(
        self, this: str, terse: bool = False, iceberg: bool = False
    ) -> exp.Show:
        scope = None
        scope_kind = None

        history = self._match_text_seq("HISTORY")

        like = self._parse_string() if self._match(TokenType.LIKE) else None

        if self._match(TokenType.IN):
            if self._match_text_seq("ACCOUNT"):
                scope_kind = "ACCOUNT"
            elif self._match_text_seq("CLASS"):
                scope_kind = "CLASS"
                scope = self._parse_table_parts()
            elif self._match_text_seq("APPLICATION"):
                scope_kind = "APPLICATION"
                if self._match_text_seq("PACKAGE"):
                    scope_kind += " PACKAGE"
                scope = self._parse_table_parts()
            elif self._match_set(self.DB_CREATABLES):
                scope_kind = self._prev.text.upper()
                if self._curr:
                    scope = self._parse_table_parts()
            elif self._curr:
                scope_kind = "SCHEMA" if this in self.SCHEMA_KINDS else "TABLE"
                scope = self._parse_table_parts()

        return self.expression(
            exp.Show(
                terse=terse,
                iceberg=iceberg,
                this=this,
                history=history,
                like=like,
                scope=scope,
                scope_kind=scope_kind,
                starts_with=self._match_text_seq("STARTS", "WITH") and self._parse_string(),
                limit=self._parse_limit(),
                from_=self._parse_string() if self._match(TokenType.FROM) else None,
                privileges=self._match_text_seq("WITH", "PRIVILEGES")
                and self._parse_csv(lambda: self._parse_var(any_token=True, upper=True)),
            )
        )

    def _parse_put(self) -> exp.Put | exp.Command:
        if self._curr.token_type != TokenType.STRING:
            return self._parse_as_command(self._prev)

        return self.expression(
            exp.Put(
                this=self._parse_string(),
                target=self._parse_location_path(),
                properties=self._parse_properties(),
            )
        )

    def _parse_get(self) -> t.Optional[exp.Expr]:
        start = self._prev

        # If we detect GET( then we need to parse a function, not a statement
        if self._match(TokenType.L_PAREN):
            self._retreat(self._index - 2)
            return self._parse_expression()

        target = self._parse_location_path()

        # Parse as command if unquoted file path
        if self._curr.token_type == TokenType.URI_START:
            return self._parse_as_command(start)

        return self.expression(
            exp.Get(this=self._parse_string(), target=target, properties=self._parse_properties())
        )

    def _parse_location_property(self) -> exp.LocationProperty:
        self._match(TokenType.EQ)
        return self.expression(exp.LocationProperty(this=self._parse_location_path()))

    def _parse_file_location(self) -> t.Optional[exp.Expr]:
        # Parse either a subquery or a staged file
        return (
            self._parse_select(table=True, parse_subquery_alias=False)
            if self._match(TokenType.L_PAREN, advance=False)
            else self._parse_table_parts()
        )

    def _parse_location_path(self) -> exp.Var:
        start = self._curr
        self._advance_any(ignore_reserved=True)

        # We avoid consuming a comma token because external tables like @foo and @bar
        # can be joined in a query with a comma separator, as well as closing paren
        # in case of subqueries
        while self._is_connected() and not self._match_set(
            (TokenType.COMMA, TokenType.L_PAREN, TokenType.R_PAREN), advance=False
        ):
            self._advance_any(ignore_reserved=True)

        return exp.var(self._find_sql(start, self._prev))

    def _parse_lambda_arg(self) -> t.Optional[exp.Expr]:
        this = super()._parse_lambda_arg()

        if not this:
            return this

        typ = self._parse_types()

        if typ:
            return self.expression(exp.Cast(this=this, to=typ))

        return this

    def _parse_foreign_key(self) -> exp.ForeignKey:
        # inlineFK, the REFERENCES columns are implied
        if self._match(TokenType.REFERENCES, advance=False):
            return self.expression(exp.ForeignKey())

        # outoflineFK, explicitly names the columns
        return super()._parse_foreign_key()

    def _parse_file_format_property(self) -> exp.FileFormatProperty:
        self._match(TokenType.EQ)
        if self._match(TokenType.L_PAREN, advance=False):
            expressions = self._parse_wrapped_options()
        else:
            expressions = [self._parse_format_name()]

        return self.expression(exp.FileFormatProperty(expressions=expressions))

    def _parse_credentials_property(self) -> exp.CredentialsProperty:
        return self.expression(exp.CredentialsProperty(expressions=self._parse_wrapped_options()))

    def _parse_semantic_view(self) -> exp.SemanticView:
        kwargs: t.Dict[str, t.Any] = {"this": self._parse_table_parts()}

        while self._curr and not self._match(TokenType.R_PAREN, advance=False):
            if self._match_texts(("DIMENSIONS", "METRICS", "FACTS")):
                keyword = self._prev.text.lower()
                kwargs[keyword] = self._parse_csv(
                    lambda: self._parse_alias(self._parse_disjunction(), explicit=True)
                )
            elif self._match_text_seq("WHERE"):
                kwargs["where"] = self._parse_expression()
            else:
                self.raise_error("Expecting ) or encountered unexpected keyword")
                break

        return self.expression(exp.SemanticView(**kwargs))

    def _parse_set(self, unset: bool = False, tag: bool = False) -> exp.Set | exp.Command:
        set = super()._parse_set(unset=unset, tag=tag)

        if isinstance(set, exp.Set):
            for expr in set.expressions:
                if isinstance(expr, exp.SetItem):
                    expr.set("kind", "VARIABLE")
        return set

    def _parse_window(
        self, this: t.Optional[exp.Expr], alias: bool = False
    ) -> t.Optional[exp.Expr]:
        if isinstance(this, exp.NthValue):
            if self._match_text_seq("FROM"):
                if self._match_texts(("FIRST", "LAST")):
                    from_first = self._prev.text.upper() == "FIRST"
                    this.set("from_first", from_first)

        result = super()._parse_window(this, alias)

        # Set default window frame for ranking functions if not present
        if (
            isinstance(result, exp.Window)
            and isinstance(this, RANKING_WINDOW_FUNCTIONS_WITH_FRAME)
            and not result.args.get("spec")
        ):
            frame = exp.WindowSpec(
                kind="ROWS",
                start="UNBOUNDED",
                start_side="PRECEDING",
                end="UNBOUNDED",
                end_side="FOLLOWING",
            )
            result.set("spec", frame)
        return result


# This is imported and used by both the parser (above) and the generator in the dialect file
RANKING_WINDOW_FUNCTIONS_WITH_FRAME = (
    exp.FirstValue,
    exp.LastValue,
    exp.NthValue,
)


def build_object_construct(args: t.List) -> t.Union[exp.StarMap, exp.Struct]:
    expression = parser.build_var_map(args)

    if isinstance(expression, exp.StarMap):
        return expression

    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=k, expression=v) for k, v in zip(expression.keys, expression.values)
        ]
    )
