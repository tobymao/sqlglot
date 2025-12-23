from __future__ import annotations

from decimal import Decimal
from itertools import groupby
import re
import typing as t

from sqlglot import exp, generator, parser, tokens, transforms

from sqlglot.dialects.dialect import (
    DATETIME_DELTA,
    Dialect,
    JSON_EXTRACT_TYPE,
    NormalizationStrategy,
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    binary_from_function,
    bool_xor_sql,
    build_default_decimal_type,
    count_if_to_sum,
    date_delta_to_binary_interval_op,
    date_trunc_to_time,
    datestrtodate_sql,
    no_datetime_sql,
    encode_decode_sql,
    build_formatted_time,
    months_between_sql,
    no_comment_column_constraint_sql,
    no_time_sql,
    no_timestamp_sql,
    pivot_column_names,
    rename_func,
    remove_from_array_using_filter,
    strposition_sql,
    str_to_time_sql,
    timestrtotime_sql,
    unit_to_str,
    sha256_sql,
    build_regexp_extract,
    explode_to_unnest_sql,
    no_make_interval_sql,
    groupconcat_sql,
    inline_array_unless_query,
    regexp_replace_global_modifier,
    sha2_digest_sql,
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import is_date_unit, seq_get
from sqlglot.tokens import TokenType
from sqlglot.parser import binary_range_parser

# Regex to detect time zones in timestamps of the form [+|-]TT[:tt]
# The pattern matches timezone offsets that appear after the time portion
TIMEZONE_PATTERN = re.compile(r":\d{2}.*?[+\-]\d{2}(?::\d{2})?")

# Characters that must be escaped when building regex expressions in INITCAP
REGEX_ESCAPE_REPLACEMENTS = {
    "\\": "\\\\",
    "-": r"\-",
    "^": r"\^",
    "[": r"\[",
    "]": r"\]",
}

# Used to in RANDSTR transpilation
RANDSTR_CHAR_POOL = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
RANDSTR_SEED = 123456

# Whitespace control characters that DuckDB must process with `CHR({val})` calls
WS_CONTROL_CHARS_TO_DUCK = {
    "\u000b": 11,
    "\u001c": 28,
    "\u001d": 29,
    "\u001e": 30,
    "\u001f": 31,
}

# Days of week to ISO 8601 day-of-week numbers
# ISO 8601 standard: Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6, Sunday=7
WEEK_START_DAY_TO_DOW = {
    "MONDAY": 1,
    "TUESDAY": 2,
    "WEDNESDAY": 3,
    "THURSDAY": 4,
    "FRIDAY": 5,
    "SATURDAY": 6,
    "SUNDAY": 7,
}

MAX_BIT_POSITION = exp.Literal.number(32768)


def _last_day_sql(self: DuckDB.Generator, expression: exp.LastDay) -> str:
    """
    DuckDB's LAST_DAY only supports finding the last day of a month.
    For other date parts (year, quarter, week), we need to implement equivalent logic.
    """
    date_expr = expression.this
    unit = expression.text("unit")

    if not unit or unit.upper() == "MONTH":
        # Default behavior - use DuckDB's native LAST_DAY
        return self.func("LAST_DAY", date_expr)

    if unit.upper() == "YEAR":
        # Last day of year: December 31st of the same year
        year_expr = exp.func("EXTRACT", "YEAR", date_expr)
        make_date_expr = exp.func(
            "MAKE_DATE", year_expr, exp.Literal.number(12), exp.Literal.number(31)
        )
        return self.sql(make_date_expr)

    if unit.upper() == "QUARTER":
        # Last day of quarter
        year_expr = exp.func("EXTRACT", "YEAR", date_expr)
        quarter_expr = exp.func("EXTRACT", "QUARTER", date_expr)

        # Calculate last month of quarter: quarter * 3. Quarter can be 1 to 4
        last_month_expr = exp.Mul(this=quarter_expr, expression=exp.Literal.number(3))
        first_day_last_month_expr = exp.func(
            "MAKE_DATE", year_expr, last_month_expr, exp.Literal.number(1)
        )

        # Last day of the last month of the quarter
        last_day_expr = exp.func("LAST_DAY", first_day_last_month_expr)
        return self.sql(last_day_expr)

    if unit.upper() == "WEEK":
        # DuckDB DAYOFWEEK: Sunday=0, Monday=1, ..., Saturday=6
        dow = exp.func("EXTRACT", "DAYOFWEEK", date_expr)
        # Days to the last day of week: (7 - dayofweek) % 7, assuming the last day of week is Sunday (Snowflake)
        # Wrap in parentheses to ensure correct precedence
        days_to_sunday_expr = exp.Mod(
            this=exp.Paren(this=exp.Sub(this=exp.Literal.number(7), expression=dow)),
            expression=exp.Literal.number(7),
        )
        interval_expr = exp.Interval(this=days_to_sunday_expr, unit=exp.var("DAY"))
        add_expr = exp.Add(this=date_expr, expression=interval_expr)
        cast_expr = exp.cast(add_expr, exp.DataType.Type.DATE)
        return self.sql(cast_expr)

    self.unsupported(f"Unsupported date part '{unit}' in LAST_DAY function")
    return self.function_fallback_sql(expression)


def _is_nanosecond_unit(unit: t.Optional[exp.Expression]) -> bool:
    return isinstance(unit, (exp.Var, exp.Literal)) and unit.name.upper() == "NANOSECOND"


def _handle_nanosecond_diff(
    self: DuckDB.Generator,
    end_time: exp.Expression,
    start_time: exp.Expression,
) -> str:
    """Generate NANOSECOND diff using EPOCH_NS since DATE_DIFF doesn't support it."""
    end_ns = exp.cast(end_time, exp.DataType.Type.TIMESTAMP_NS)
    start_ns = exp.cast(start_time, exp.DataType.Type.TIMESTAMP_NS)

    # Build expression tree: EPOCH_NS(end) - EPOCH_NS(start)
    return self.sql(
        exp.Sub(this=exp.func("EPOCH_NS", end_ns), expression=exp.func("EPOCH_NS", start_ns))
    )


def _to_boolean_sql(self: DuckDB.Generator, expression: exp.ToBoolean) -> str:
    """
    Transpile TO_BOOLEAN and TRY_TO_BOOLEAN functions from Snowflake to DuckDB equivalent.

    DuckDB's CAST to BOOLEAN supports most of Snowflake's TO_BOOLEAN strings except 'on'/'off'.
    We need to handle the 'on'/'off' cases explicitly.

    For TO_BOOLEAN (safe=False): NaN and INF values cause errors. We use DuckDB's native ERROR()
    function to replicate this behavior with a clear error message.

    For TRY_TO_BOOLEAN (safe=True): Use DuckDB's TRY_CAST for conversion, which returns NULL
    for invalid inputs instead of throwing errors.
    """
    arg = expression.this
    is_safe = expression.args.get("safe", False)

    base_case_expr = (
        exp.case()
        .when(
            # Handle 'on' -> TRUE (case insensitive)
            exp.Upper(this=exp.cast(arg, exp.DataType.Type.VARCHAR)).eq(exp.Literal.string("ON")),
            exp.true(),
        )
        .when(
            # Handle 'off' -> FALSE (case insensitive)
            exp.Upper(this=exp.cast(arg, exp.DataType.Type.VARCHAR)).eq(exp.Literal.string("OFF")),
            exp.false(),
        )
    )

    if is_safe:
        # TRY_TO_BOOLEAN: handle 'on'/'off' and use TRY_CAST for everything else
        case_expr = base_case_expr.else_(exp.func("TRY_CAST", arg, exp.DataType.build("BOOLEAN")))
    else:
        # TO_BOOLEAN: handle NaN/INF errors, 'on'/'off', and use regular CAST
        cast_to_real = exp.func("TRY_CAST", arg, exp.DataType.build("REAL"))

        # Check for NaN and INF values
        nan_inf_check = exp.Or(
            this=exp.func("ISNAN", cast_to_real), expression=exp.func("ISINF", cast_to_real)
        )

        case_expr = base_case_expr.when(
            nan_inf_check,
            exp.func(
                "ERROR",
                exp.Literal.string("TO_BOOLEAN: Non-numeric values NaN and INF are not supported"),
            ),
        ).else_(exp.cast(arg, exp.DataType.Type.BOOLEAN))

    return self.sql(case_expr)


# BigQuery -> DuckDB conversion for the DATE function
def _date_sql(self: DuckDB.Generator, expression: exp.Date) -> str:
    this = expression.this
    zone = self.sql(expression, "zone")

    if zone:
        # BigQuery considers "this" at UTC, converts it to the specified
        # time zone and then keeps only the DATE part
        # To micmic that, we:
        #   (1) Cast to TIMESTAMP to remove DuckDB's local tz
        #   (2) Apply consecutive AtTimeZone calls for UTC -> zone conversion
        this = exp.cast(this, exp.DataType.Type.TIMESTAMP)
        at_utc = exp.AtTimeZone(this=this, zone=exp.Literal.string("UTC"))
        this = exp.AtTimeZone(this=at_utc, zone=zone)

    return self.sql(exp.cast(expression=this, to=exp.DataType.Type.DATE))


# BigQuery -> DuckDB conversion for the TIME_DIFF function
def _timediff_sql(self: DuckDB.Generator, expression: exp.TimeDiff) -> str:
    unit = expression.unit

    if _is_nanosecond_unit(unit):
        return _handle_nanosecond_diff(self, expression.expression, expression.this)

    this = exp.cast(expression.this, exp.DataType.Type.TIME)
    expr = exp.cast(expression.expression, exp.DataType.Type.TIME)

    # Although the 2 dialects share similar signatures, BQ seems to inverse
    # the sign of the result so the start/end time operands are flipped
    return self.func("DATE_DIFF", unit_to_str(expression), expr, this)


def _date_delta_to_binary_interval_op(
    cast: bool = True,
) -> t.Callable[[DuckDB.Generator, DATETIME_DELTA], str]:
    """DuckDB override to handle NANOSECOND operations; delegates other units to base."""
    base_impl = date_delta_to_binary_interval_op(cast=cast)

    def _duckdb_date_delta_sql(self: DuckDB.Generator, expression: DATETIME_DELTA) -> str:
        unit = expression.unit

        # Handle NANOSECOND unit (DuckDB doesn't support INTERVAL ... NANOSECOND)
        if _is_nanosecond_unit(unit):
            interval_value = expression.expression
            if isinstance(interval_value, exp.Interval):
                interval_value = interval_value.this

            timestamp_ns = exp.cast(expression.this, exp.DataType.Type.TIMESTAMP_NS)

            return self.sql(
                exp.func(
                    "MAKE_TIMESTAMP_NS",
                    exp.Add(this=exp.func("EPOCH_NS", timestamp_ns), expression=interval_value),
                )
            )

        return base_impl(self, expression)

    return _duckdb_date_delta_sql


@unsupported_args(("expression", "DuckDB's ARRAY_SORT does not support a comparator."))
def _array_sort_sql(self: DuckDB.Generator, expression: exp.ArraySort) -> str:
    return self.func("ARRAY_SORT", expression.this)


def _sort_array_sql(self: DuckDB.Generator, expression: exp.SortArray) -> str:
    name = "ARRAY_REVERSE_SORT" if expression.args.get("asc") == exp.false() else "ARRAY_SORT"
    return self.func(name, expression.this)


def _build_sort_array_desc(args: t.List) -> exp.Expression:
    return exp.SortArray(this=seq_get(args, 0), asc=exp.false())


def _build_array_prepend(args: t.List) -> exp.Expression:
    return exp.ArrayPrepend(this=seq_get(args, 1), expression=seq_get(args, 0))


def _build_date_diff(args: t.List) -> exp.Expression:
    return exp.DateDiff(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))


def _build_generate_series(end_exclusive: bool = False) -> t.Callable[[t.List], exp.GenerateSeries]:
    def _builder(args: t.List) -> exp.GenerateSeries:
        # Check https://duckdb.org/docs/sql/functions/nested.html#range-functions
        if len(args) == 1:
            # DuckDB uses 0 as a default for the series' start when it's omitted
            args.insert(0, exp.Literal.number("0"))

        gen_series = exp.GenerateSeries.from_arg_list(args)
        gen_series.set("is_end_exclusive", end_exclusive)

        return gen_series

    return _builder


def _build_make_timestamp(args: t.List) -> exp.Expression:
    if len(args) == 1:
        return exp.UnixToTime(this=seq_get(args, 0), scale=exp.UnixToTime.MICROS)

    return exp.TimestampFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        hour=seq_get(args, 3),
        min=seq_get(args, 4),
        sec=seq_get(args, 5),
    )


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[DuckDB.Parser], exp.Show]:
    def _parse(self: DuckDB.Parser) -> exp.Show:
        return self._parse_show_duckdb(*args, **kwargs)

    return _parse


def _struct_sql(self: DuckDB.Generator, expression: exp.Struct) -> str:
    args: t.List[str] = []

    # BigQuery allows inline construction such as "STRUCT<a STRING, b INTEGER>('str', 1)" which is
    # canonicalized to "ROW('str', 1) AS STRUCT(a TEXT, b INT)" in DuckDB
    # The transformation to ROW will take place if:
    #  1. The STRUCT itself does not have proper fields (key := value) as a "proper" STRUCT would
    #  2. A cast to STRUCT / ARRAY of STRUCTs is found
    ancestor_cast = expression.find_ancestor(exp.Cast)
    is_bq_inline_struct = (
        (expression.find(exp.PropertyEQ) is None)
        and ancestor_cast
        and any(
            casted_type.is_type(exp.DataType.Type.STRUCT)
            for casted_type in ancestor_cast.find_all(exp.DataType)
        )
    )

    for i, expr in enumerate(expression.expressions):
        is_property_eq = isinstance(expr, exp.PropertyEQ)
        value = expr.expression if is_property_eq else expr

        if is_bq_inline_struct:
            args.append(self.sql(value))
        else:
            if is_property_eq:
                if isinstance(expr.this, exp.Identifier):
                    key = self.sql(exp.Literal.string(expr.name))
                else:
                    key = self.sql(expr.this)
            else:
                key = self.sql(exp.Literal.string(f"_{i}"))

            args.append(f"{key}: {self.sql(value)}")

    csv_args = ", ".join(args)

    return f"ROW({csv_args})" if is_bq_inline_struct else f"{{{csv_args}}}"


def _datatype_sql(self: DuckDB.Generator, expression: exp.DataType) -> str:
    if expression.is_type("array"):
        return f"{self.expressions(expression, flat=True)}[{self.expressions(expression, key='values', flat=True)}]"

    # Modifiers are not supported for TIME, [TIME | TIMESTAMP] WITH TIME ZONE
    if expression.is_type(
        exp.DataType.Type.TIME, exp.DataType.Type.TIMETZ, exp.DataType.Type.TIMESTAMPTZ
    ):
        return expression.this.value

    return self.datatype_sql(expression)


def _json_format_sql(self: DuckDB.Generator, expression: exp.JSONFormat) -> str:
    sql = self.func("TO_JSON", expression.this, expression.args.get("options"))
    return f"CAST({sql} AS TEXT)"


def _unix_to_time_sql(self: DuckDB.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this
    target_type = expression.args.get("target_type")

    # Check if we need NTZ (naive timestamp in UTC)
    is_ntz = target_type and target_type.this in (
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPNTZ,
    )

    if scale == exp.UnixToTime.MILLIS:
        # EPOCH_MS already returns TIMESTAMP (naive, UTC)
        return self.func("EPOCH_MS", timestamp)
    if scale == exp.UnixToTime.MICROS:
        # MAKE_TIMESTAMP already returns TIMESTAMP (naive, UTC)
        return self.func("MAKE_TIMESTAMP", timestamp)

    # Other scales: divide and use TO_TIMESTAMP
    if scale not in (None, exp.UnixToTime.SECONDS):
        timestamp = exp.Div(this=timestamp, expression=exp.func("POW", 10, scale))

    to_timestamp: exp.Expression = exp.Anonymous(this="TO_TIMESTAMP", expressions=[timestamp])

    if is_ntz:
        to_timestamp = exp.AtTimeZone(this=to_timestamp, zone=exp.Literal.string("UTC"))

    return self.sql(to_timestamp)


WRAPPED_JSON_EXTRACT_EXPRESSIONS = (exp.Binary, exp.Bracket, exp.In, exp.Not)


def _arrow_json_extract_sql(self: DuckDB.Generator, expression: JSON_EXTRACT_TYPE) -> str:
    arrow_sql = arrow_json_extract_sql(self, expression)
    if not expression.same_parent and isinstance(
        expression.parent, WRAPPED_JSON_EXTRACT_EXPRESSIONS
    ):
        arrow_sql = self.wrap(arrow_sql)
    return arrow_sql


def _implicit_datetime_cast(
    arg: t.Optional[exp.Expression], type: exp.DataType.Type = exp.DataType.Type.DATE
) -> t.Optional[exp.Expression]:
    if isinstance(arg, exp.Literal) and arg.is_string:
        ts = arg.name
        if type == exp.DataType.Type.DATE and ":" in ts:
            type = (
                exp.DataType.Type.TIMESTAMPTZ
                if TIMEZONE_PATTERN.search(ts)
                else exp.DataType.Type.TIMESTAMP
            )

        arg = exp.cast(arg, type)

    return arg


def _week_unit_to_dow(unit: t.Optional[exp.Expression]) -> t.Optional[int]:
    """
    Compute the Monday-based day shift to align DATE_DIFF('WEEK', ...) coming
    from other dialects, e.g BigQuery's WEEK(<day>) or ISOWEEK unit parts.

    Args:
        unit: The unit expression (Var for ISOWEEK or WeekStart)

    Returns:
        The ISO 8601 day number (Monday=1, Sunday=7 etc) or None if not a week unit or if day is dynamic (not a constant).

        Examples:
            "WEEK(SUNDAY)" -> 7
            "WEEK(MONDAY)" -> 1
            "ISOWEEK" -> 1
    """
    # Handle plain Var expressions for ISOWEEK only
    if isinstance(unit, exp.Var) and unit.name.upper() in "ISOWEEK":
        return 1

    # Handle WeekStart expressions with explicit day
    if isinstance(unit, exp.WeekStart):
        return WEEK_START_DAY_TO_DOW.get(unit.name.upper())

    return None


def _build_week_trunc_expression(date_expr: exp.Expression, start_dow: int) -> exp.Expression:
    """
    Build DATE_TRUNC expression for week boundaries with custom start day.

    Args:
        date_expr: The date expression to truncate
        shift_days: ISO 8601 day-of-week number (Monday=0, ..., Sunday=6)

    DuckDB's DATE_TRUNC('WEEK', ...) aligns weeks to Monday (ISO standard).
    To align to a different start day, we shift the date before truncating.

    Shift formula: Sunday (7) gets +1, others get (1 - start_dow)
    Examples:
        Monday (1): shift = 0 (no shift needed)
        Tuesday (2): shift = -1 (shift back 1 day) ...
        Sunday (7): shift = +1 (shift forward 1 day, wraps to next Monday-based week)
    """
    shift_days = 1 if start_dow == 7 else 1 - start_dow

    # Shift date to align week boundaries with the desired start day
    # No shift needed for Monday-based weeks (shift_days == 0)
    shifted_date = (
        exp.DateAdd(
            this=date_expr,
            expression=exp.Interval(this=exp.Literal.string(str(shift_days)), unit=exp.var("DAY")),
        )
        if shift_days != 0
        else date_expr
    )

    return exp.DateTrunc(unit=exp.var("WEEK"), this=shifted_date)


def _date_diff_sql(self: DuckDB.Generator, expression: exp.DateDiff) -> str:
    unit = expression.unit

    if _is_nanosecond_unit(unit):
        return _handle_nanosecond_diff(self, expression.this, expression.expression)

    this = _implicit_datetime_cast(expression.this)
    expr = _implicit_datetime_cast(expression.expression)

    # DuckDB's WEEK diff does not respect Monday crossing (week boundaries), it checks (end_day - start_day) / 7:
    #  SELECT DATE_DIFF('WEEK', CAST('2024-12-13' AS DATE), CAST('2024-12-17' AS DATE)) --> 0 (Monday crossed)
    #  SELECT DATE_DIFF('WEEK', CAST('2024-12-13' AS DATE), CAST('2024-12-20' AS DATE)) --> 1 (7 days difference)
    # Whereas for other units such as MONTH it does respect month boundaries:
    #  SELECT DATE_DIFF('MONTH', CAST('2024-11-30' AS DATE), CAST('2024-12-01' AS DATE)) --> 1 (Month crossed)
    date_part_boundary = expression.args.get("date_part_boundary")

    # Extract week start day; returns None if day is dynamic (column/placeholder)
    week_start = _week_unit_to_dow(unit)
    if date_part_boundary and week_start and this and expr:
        expression.set("unit", exp.Literal.string("WEEK"))

        # Truncate both dates to week boundaries to respect input dialect semantics
        this = _build_week_trunc_expression(this, week_start)
        expr = _build_week_trunc_expression(expr, week_start)

    return self.func("DATE_DIFF", unit_to_str(expression), expr, this)


def _generate_datetime_array_sql(
    self: DuckDB.Generator, expression: t.Union[exp.GenerateDateArray, exp.GenerateTimestampArray]
) -> str:
    is_generate_date_array = isinstance(expression, exp.GenerateDateArray)

    type = exp.DataType.Type.DATE if is_generate_date_array else exp.DataType.Type.TIMESTAMP
    start = _implicit_datetime_cast(expression.args.get("start"), type=type)
    end = _implicit_datetime_cast(expression.args.get("end"), type=type)

    # BQ's GENERATE_DATE_ARRAY & GENERATE_TIMESTAMP_ARRAY are transformed to DuckDB'S GENERATE_SERIES
    gen_series: t.Union[exp.GenerateSeries, exp.Cast] = exp.GenerateSeries(
        start=start, end=end, step=expression.args.get("step")
    )

    if is_generate_date_array:
        # The GENERATE_SERIES result type is TIMESTAMP array, so to match BQ's semantics for
        # GENERATE_DATE_ARRAY we must cast it back to DATE array
        gen_series = exp.cast(gen_series, exp.DataType.build("ARRAY<DATE>"))

    return self.sql(gen_series)


def _json_extract_value_array_sql(
    self: DuckDB.Generator, expression: exp.JSONValueArray | exp.JSONExtractArray
) -> str:
    json_extract = exp.JSONExtract(this=expression.this, expression=expression.expression)
    data_type = "ARRAY<STRING>" if isinstance(expression, exp.JSONValueArray) else "ARRAY<JSON>"
    return self.sql(exp.cast(json_extract, to=exp.DataType.build(data_type)))


def _cast_to_varchar(arg: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
    if arg and arg.type and not arg.is_type(exp.DataType.Type.VARCHAR, exp.DataType.Type.UNKNOWN):
        return exp.cast(arg, exp.DataType.Type.VARCHAR)
    return arg


def _cast_to_boolean(arg: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
    if arg and not arg.is_type(exp.DataType.Type.BOOLEAN):
        return exp.cast(arg, exp.DataType.Type.BOOLEAN)
    return arg


def _is_binary(arg: exp.Expression) -> bool:
    return arg.is_type(
        exp.DataType.Type.BINARY,
        exp.DataType.Type.VARBINARY,
        exp.DataType.Type.BLOB,
    )


def _gen_with_cast_to_blob(
    self: DuckDB.Generator, expression: exp.Expression, result_sql: str
) -> str:
    if _is_binary(expression):
        blob = exp.DataType.build("BLOB", dialect="duckdb")
        result_sql = self.sql(exp.Cast(this=result_sql, to=blob))
    return result_sql


def _cast_to_bit(arg: exp.Expression) -> exp.Expression:
    if not _is_binary(arg):
        return arg

    if isinstance(arg, exp.HexString):
        arg = exp.Unhex(this=exp.Literal.string(arg.this))
    elif isinstance(arg, exp.Cast) and isinstance(arg.this, exp.HexString):
        arg = exp.Unhex(this=exp.Literal.string(arg.this.this))

    return exp.cast(arg, exp.DataType.Type.BIT)


def _prepare_binary_bitwise_args(expression: exp.Binary) -> None:
    if _is_binary(expression.this):
        expression.set("this", _cast_to_bit(expression.this))
    if _is_binary(expression.expression):
        expression.set("expression", _cast_to_bit(expression.expression))


def _anyvalue_sql(self: DuckDB.Generator, expression: exp.AnyValue) -> str:
    # Transform ANY_VALUE(expr HAVING MAX/MIN having_expr) to ARG_MAX_NULL/ARG_MIN_NULL
    having = expression.this
    if isinstance(having, exp.HavingMax):
        func_name = "ARG_MAX_NULL" if having.args.get("max") else "ARG_MIN_NULL"
        return self.func(func_name, having.this, having.expression)
    return self.function_fallback_sql(expression)


def _bitwise_agg_sql(
    self: DuckDB.Generator,
    expression: t.Union[exp.BitwiseOrAgg, exp.BitwiseAndAgg, exp.BitwiseXorAgg],
) -> str:
    """
    DuckDB's bitwise aggregate functions only accept integer types. For other types:
    - DECIMAL/STRING: Use CAST(arg AS INT) to convert directly, will round to nearest int
    - FLOAT/DOUBLE: Use ROUND(arg)::INT to round to nearest integer, required due to float precision loss
    """
    if isinstance(expression, exp.BitwiseOrAgg):
        func_name = "BIT_OR"
    elif isinstance(expression, exp.BitwiseAndAgg):
        func_name = "BIT_AND"
    else:  # exp.BitwiseXorAgg
        func_name = "BIT_XOR"

    arg = expression.this

    if not arg.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        arg = annotate_types(arg, dialect=self.dialect)

    if arg.is_type(*exp.DataType.REAL_TYPES, *exp.DataType.TEXT_TYPES):
        if arg.is_type(*exp.DataType.FLOAT_TYPES):
            # float types need to be rounded first due to precision loss
            arg = exp.func("ROUND", arg)

        arg = exp.cast(arg, exp.DataType.Type.INT)

    return self.func(func_name, arg)


def _literal_sql_with_ws_chr(self: DuckDB.Generator, literal: str) -> str:
    # DuckDB does not support \uXXXX escapes, so we must use CHR() instead of replacing them directly
    if not any(ch in WS_CONTROL_CHARS_TO_DUCK for ch in literal):
        return self.sql(exp.Literal.string(literal))

    sql_segments: t.List[str] = []
    for is_ws_control, group in groupby(literal, key=lambda ch: ch in WS_CONTROL_CHARS_TO_DUCK):
        if is_ws_control:
            for ch in group:
                duckdb_char_code = WS_CONTROL_CHARS_TO_DUCK[ch]
                sql_segments.append(self.func("CHR", exp.Literal.number(str(duckdb_char_code))))
        else:
            sql_segments.append(self.sql(exp.Literal.string("".join(group))))

    sql = " || ".join(sql_segments)
    return sql if len(sql_segments) == 1 else f"({sql})"


def _escape_regex_metachars(
    self: DuckDB.Generator, delimiters: t.Optional[exp.Expression], delimiters_sql: str
) -> str:
    r"""
    Escapes regex metacharacters \ - ^ [ ] for use in character classes regex expressions.

    Literal strings are escaped at transpile time, expressions handled with REPLACE() calls.
    """
    if not delimiters:
        return delimiters_sql

    if delimiters.is_string:
        literal_value = delimiters.this
        escaped_literal = "".join(REGEX_ESCAPE_REPLACEMENTS.get(ch, ch) for ch in literal_value)
        return _literal_sql_with_ws_chr(self, escaped_literal)

    escaped_sql = delimiters_sql
    for raw, escaped in REGEX_ESCAPE_REPLACEMENTS.items():
        escaped_sql = self.func(
            "REPLACE",
            escaped_sql,
            self.sql(exp.Literal.string(raw)),
            self.sql(exp.Literal.string(escaped)),
        )

    return escaped_sql


def _build_capitalization_sql(
    self: DuckDB.Generator,
    value_to_split: str,
    delimiters_sql: str,
) -> str:
    # empty string delimiter --> treat value as one word, no need to split
    if delimiters_sql == "''":
        return f"UPPER(LEFT({value_to_split}, 1)) || LOWER(SUBSTRING({value_to_split}, 2))"

    delim_regex_sql = f"CONCAT('[', {delimiters_sql}, ']')"
    split_regex_sql = f"CONCAT('([', {delimiters_sql}, ']+|[^', {delimiters_sql}, ']+)')"

    # REGEXP_EXTRACT_ALL produces a list of string segments, alternating between delimiter and non-delimiter segments.
    # We do not know whether the first segment is a delimiter or not, so we check the first character of the string
    # with REGEXP_MATCHES. If the first char is a delimiter, we capitalize even list indexes, otherwise capitalize odd.
    return self.func(
        "ARRAY_TO_STRING",
        exp.case()
        .when(
            f"REGEXP_MATCHES(LEFT({value_to_split}, 1), {delim_regex_sql})",
            self.func(
                "LIST_TRANSFORM",
                self.func("REGEXP_EXTRACT_ALL", value_to_split, split_regex_sql),
                "(seg, idx) -> CASE WHEN idx % 2 = 0 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END",
            ),
        )
        .else_(
            self.func(
                "LIST_TRANSFORM",
                self.func("REGEXP_EXTRACT_ALL", value_to_split, split_regex_sql),
                "(seg, idx) -> CASE WHEN idx % 2 = 1 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END",
            ),
        ),
        "''",
    )


def _initcap_sql(self: DuckDB.Generator, expression: exp.Initcap) -> str:
    this_sql = self.sql(expression, "this")
    delimiters = expression.args.get("expression")
    if delimiters is None:
        # fallback for manually created exp.Initcap w/o delimiters arg
        delimiters = exp.Literal.string(self.dialect.INITCAP_DEFAULT_DELIMITER_CHARS)
    delimiters_sql = self.sql(delimiters)

    escaped_delimiters_sql = _escape_regex_metachars(self, delimiters, delimiters_sql)

    return _build_capitalization_sql(self, this_sql, escaped_delimiters_sql)


def _boolxor_agg_sql(self: DuckDB.Generator, expression: exp.BoolxorAgg) -> str:
    """
    Snowflake's `BOOLXOR_AGG(col)` returns TRUE if exactly one input in `col` is TRUE, FALSE otherwise;
    Since DuckDB does not have a mapping function, we mimic the behavior by generating `COUNT_IF(col) = 1`.

    DuckDB's COUNT_IF strictly requires boolean inputs, so cast if not already boolean.
    """
    return self.sql(
        exp.EQ(
            this=exp.CountIf(this=_cast_to_boolean(expression.this)),
            expression=exp.Literal.number(1),
        )
    )


def _bitshift_sql(
    self: DuckDB.Generator, expression: exp.BitwiseLeftShift | exp.BitwiseRightShift
) -> str:
    """
    Transform bitshift expressions for DuckDB by injecting BIT/INT128 casts.

    DuckDB's bitwise shift operators don't work with BLOB/BINARY types, so we cast
    them to BIT for the operation, then cast the result back to the original type.

    Note: Assumes type annotation has been applied with the source dialect.
    """
    operator = "<<" if isinstance(expression, exp.BitwiseLeftShift) else ">>"
    original_type = None
    this = expression.this

    # Check if input is binary:
    # 1. Direct binary type annotation on expression.this
    # 2. Chained bitshift where inner operation's input is binary
    is_binary_input = _is_binary(this) or (
        isinstance(this.this, exp.Expression) and _is_binary(this.this)
    )

    # Deal with binary separately, remember the original type, cast back later
    if is_binary_input:
        original_type = this.to if isinstance(this, exp.Cast) else exp.DataType.build("BLOB")

        # for chained binary operators
        if isinstance(this, exp.Binary):
            expression.set("this", exp.cast(this, exp.DataType.Type.BIT))
        else:
            expression.set("this", _cast_to_bit(this))

        # Remove the flag for binary otherwise the final cast will get wrapped in an extra INT128 cast
        expression.args.pop("requires_int128")

    # cast to INT128 if required (e.g. coming from Snowflake)
    elif expression.args.get("requires_int128"):
        this.replace(exp.cast(this, exp.DataType.Type.INT128))

    # Cast shift amount to int in case it's something else
    shift_amount = expression.expression
    if isinstance(shift_amount, exp.Literal) and not shift_amount.is_int:
        expression.set("expression", exp.cast(shift_amount, exp.DataType.Type.INT))

    result_sql = self.binary(expression, operator)

    # Wrap in parentheses if parent is a bitwise operator to "fix" DuckDB precedence issue
    # DuckDB parses: a << b | c << d  as  (a << b | c) << d
    if isinstance(expression.parent, exp.Binary):
        result_sql = self.sql(exp.Paren(this=result_sql))

    # Cast the result back to the original type
    if original_type:
        result_sql = self.sql(exp.Cast(this=result_sql, to=original_type))

    return result_sql


def _scale_rounding_sql(
    self: DuckDB.Generator,
    expression: exp.Expression,
    rounding_func: type[exp.Expression],
) -> str | None:
    """
    Handle scale parameter transformation for rounding functions.

    DuckDB doesn't support the scale parameter for certain functions (e.g., FLOOR, CEIL),
    so we transform: FUNC(x, n) to ROUND(FUNC(x * 10^n) / 10^n, n)

    Args:
        self: The DuckDB generator instance
        expression: The expression to transform (must have 'this', 'decimals', and 'to' args)
        rounding_func: The rounding function class to use in the transformation

    Returns:
        The transformed SQL string if decimals parameter exists, None otherwise
    """
    decimals = expression.args.get("decimals")

    if decimals is None or expression.args.get("to") is not None:
        return None

    this = expression.this
    if isinstance(this, exp.Binary):
        this = exp.Paren(this=this)

    n_int = decimals
    if not (decimals.is_int or decimals.is_type(*exp.DataType.INTEGER_TYPES)):
        n_int = exp.cast(decimals, exp.DataType.Type.INT)

    pow_ = exp.Pow(this=exp.Literal.number("10"), expression=n_int)
    rounded = rounding_func(this=exp.Mul(this=this, expression=pow_))
    result = exp.Div(this=rounded, expression=pow_.copy())

    return self.round_sql(
        exp.Round(this=result, decimals=decimals, casts_non_integer_decimals=True)
    )


def _ceil_floor(self: DuckDB.Generator, expression: exp.Floor | exp.Ceil) -> str:
    scaled_sql = _scale_rounding_sql(self, expression, type(expression))
    if scaled_sql is not None:
        return scaled_sql
    return self.ceil_floor(expression)


def _regr_val_sql(
    self: DuckDB.Generator,
    expression: exp.RegrValx | exp.RegrValy,
) -> str:
    """
    Transpile Snowflake's REGR_VALX/REGR_VALY to DuckDB equivalent.

    REGR_VALX(y, x) returns NULL if y is NULL; otherwise returns x.
    REGR_VALY(y, x) returns NULL if x is NULL; otherwise returns y.
    """
    from sqlglot.optimizer.annotate_types import annotate_types

    y = expression.this
    x = expression.expression

    # Determine which argument to check for NULL and which to return based on expression type
    if isinstance(expression, exp.RegrValx):
        # REGR_VALX: check y for NULL, return x
        check_for_null = y
        return_value = x
        return_value_attr = "expression"
    else:
        # REGR_VALY: check x for NULL, return y
        check_for_null = x
        return_value = y
        return_value_attr = "this"

    # Get the type from the return argument
    result_type = return_value.type

    # If no type info, annotate the expression to infer types
    if not result_type or result_type.this == exp.DataType.Type.UNKNOWN:
        try:
            annotated = annotate_types(expression.copy(), dialect=self.dialect)
            result_type = getattr(annotated, return_value_attr).type
        except Exception:
            pass

    # Default to DOUBLE for regression functions if type still unknown
    if not result_type or result_type.this == exp.DataType.Type.UNKNOWN:
        result_type = exp.DataType.build("DOUBLE")

    # Cast NULL to the same type as return_value to avoid DuckDB type inference issues
    typed_null = exp.Cast(this=exp.Null(), to=result_type)

    return self.sql(
        exp.If(
            this=exp.Is(this=check_for_null.copy(), expression=exp.Null()),
            true=typed_null,
            false=return_value.copy(),
        )
    )


def _maybe_corr_null_to_false(
    expression: t.Union[exp.Filter, exp.Window, exp.Corr],
) -> t.Optional[t.Union[exp.Filter, exp.Window, exp.Corr]]:
    corr = expression
    while isinstance(corr, (exp.Window, exp.Filter)):
        corr = corr.this

    if not isinstance(corr, exp.Corr) or not corr.args.get("null_on_zero_variance"):
        return None

    corr.set("null_on_zero_variance", False)
    return expression


def _date_from_parts_sql(self, expression: exp.DateFromParts) -> str:
    """
    Snowflake's DATE_FROM_PARTS allows out-of-range values for the month and day input.
    E.g., larger values (month=13, day=100), zero-values (month=0, day=0), negative values (month=-13, day=-100).

    DuckDB's MAKE_DATE does not support out-of-range values, but DuckDB's INTERVAL type does.

    We convert to date arithmetic:
    DATE_FROM_PARTS(year, month, day)
    - MAKE_DATE(year, 1, 1) + INTERVAL (month-1) MONTH + INTERVAL (day-1) DAY
    """
    year_expr = expression.args.get("year")
    month_expr = expression.args.get("month")
    day_expr = expression.args.get("day")

    if expression.args.get("allow_overflow"):
        base_date: exp.Expression = exp.func(
            "MAKE_DATE", year_expr, exp.Literal.number(1), exp.Literal.number(1)
        )

        if month_expr:
            base_date = base_date + exp.Interval(this=month_expr - 1, unit=exp.var("MONTH"))

        if day_expr:
            base_date = base_date + exp.Interval(this=day_expr - 1, unit=exp.var("DAY"))

        return self.sql(exp.cast(expression=base_date, to=exp.DataType.Type.DATE))

    return self.func("MAKE_DATE", year_expr, month_expr, day_expr)


class DuckDB(Dialect):
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = True
    SAFE_DIVISION = True
    INDEX_OFFSET = 1
    CONCAT_COALESCE = True
    SUPPORTS_ORDER_BY_ALL = True
    SUPPORTS_FIXED_SIZE_ARRAYS = True
    STRICT_JSON_PATH_SYNTAX = False
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True

    # https://duckdb.org/docs/sql/introduction.html#creating-a-new-table
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "DAYOFWEEKISO": "ISODOW",
    }

    DATE_PART_MAPPING.pop("WEEKDAY")

    INVERSE_TIME_MAPPING = {
        "%e": "%-d",  # BigQuery's space-padded day (%e) -> DuckDB's no-padding day (%-d)
    }

    def to_json_path(self, path: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if isinstance(path, exp.Literal):
            # DuckDB also supports the JSON pointer syntax, where every path starts with a `/`.
            # Additionally, it allows accessing the back of lists using the `[#-i]` syntax.
            # This check ensures we'll avoid trying to parse these as JSON paths, which can
            # either result in a noisy warning or in an invalid representation of the path.
            path_text = path.name
            if path_text.startswith("/") or "[#" in path_text:
                return path

        return super().to_json_path(path)

    class Tokenizer(tokens.Tokenizer):
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]
        HEREDOC_STRINGS = ["$"]

        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "//": TokenType.DIV,
            "**": TokenType.DSTAR,
            "^@": TokenType.CARET_AT,
            "@>": TokenType.AT_GT,
            "<@": TokenType.LT_AT,
            "ATTACH": TokenType.ATTACH,
            "BINARY": TokenType.VARBINARY,
            "BITSTRING": TokenType.BIT,
            "BPCHAR": TokenType.TEXT,
            "CHAR": TokenType.TEXT,
            "DATETIME": TokenType.TIMESTAMPNTZ,
            "DETACH": TokenType.DETACH,
            "FORCE": TokenType.FORCE,
            "INSTALL": TokenType.INSTALL,
            "INT8": TokenType.BIGINT,
            "LOGICAL": TokenType.BOOLEAN,
            "MACRO": TokenType.FUNCTION,
            "ONLY": TokenType.ONLY,
            "PIVOT_WIDER": TokenType.PIVOT,
            "POSITIONAL": TokenType.POSITIONAL,
            "RESET": TokenType.COMMAND,
            "ROW": TokenType.STRUCT,
            "SIGNED": TokenType.INT,
            "STRING": TokenType.TEXT,
            "SUMMARIZE": TokenType.SUMMARIZE,
            "TIMESTAMP": TokenType.TIMESTAMPNTZ,
            "TIMESTAMP_S": TokenType.TIMESTAMP_S,
            "TIMESTAMP_MS": TokenType.TIMESTAMP_MS,
            "TIMESTAMP_NS": TokenType.TIMESTAMP_NS,
            "TIMESTAMP_US": TokenType.TIMESTAMP,
            "UBIGINT": TokenType.UBIGINT,
            "UINTEGER": TokenType.UINT,
            "USMALLINT": TokenType.USMALLINT,
            "UTINYINT": TokenType.UTINYINT,
            "VARCHAR": TokenType.TEXT,
        }
        KEYWORDS.pop("/*+")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(parser.Parser):
        MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS = True

        BITWISE = parser.Parser.BITWISE.copy()
        BITWISE.pop(TokenType.CARET)

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.DAMP: binary_range_parser(exp.ArrayOverlaps),
            TokenType.CARET_AT: binary_range_parser(exp.StartsWith),
            TokenType.TILDA: binary_range_parser(exp.RegexpFullMatch),
        }

        EXPONENT = {
            **parser.Parser.EXPONENT,
            TokenType.CARET: exp.Pow,
            TokenType.DSTAR: exp.Pow,
        }

        FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "STRUCT_PACK"}

        SHOW_PARSERS = {
            "TABLES": _show_parser("TABLES"),
            "ALL TABLES": _show_parser("ALL TABLES"),
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ANY_VALUE": lambda args: exp.IgnoreNulls(this=exp.AnyValue.from_arg_list(args)),
            "ARRAY_PREPEND": _build_array_prepend,
            "ARRAY_REVERSE_SORT": _build_sort_array_desc,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
            "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
            "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
            "DATEDIFF": _build_date_diff,
            "DATE_DIFF": _build_date_diff,
            "DATE_TRUNC": date_trunc_to_time,
            "DATETRUNC": date_trunc_to_time,
            "DECODE": lambda args: exp.Decode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "EDITDIST3": exp.Levenshtein.from_arg_list,
            "ENCODE": lambda args: exp.Encode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "EPOCH": exp.TimeToUnix.from_arg_list,
            "EPOCH_MS": lambda args: exp.UnixToTime(
                this=seq_get(args, 0), scale=exp.UnixToTime.MILLIS
            ),
            "GENERATE_SERIES": _build_generate_series(),
            "JSON": exp.ParseJSON.from_arg_list,
            "JSON_EXTRACT_PATH": parser.build_extract_json_with_path(exp.JSONExtract),
            "JSON_EXTRACT_STRING": parser.build_extract_json_with_path(exp.JSONExtractScalar),
            "LIST_APPEND": exp.ArrayAppend.from_arg_list,
            "LIST_CONTAINS": exp.ArrayContains.from_arg_list,
            "LIST_COSINE_DISTANCE": exp.CosineDistance.from_arg_list,
            "LIST_DISTANCE": exp.EuclideanDistance.from_arg_list,
            "LIST_FILTER": exp.ArrayFilter.from_arg_list,
            "LIST_HAS": exp.ArrayContains.from_arg_list,
            "LIST_HAS_ANY": exp.ArrayOverlaps.from_arg_list,
            "LIST_PREPEND": _build_array_prepend,
            "LIST_REVERSE_SORT": _build_sort_array_desc,
            "LIST_SORT": exp.SortArray.from_arg_list,
            "LIST_TRANSFORM": exp.Transform.from_arg_list,
            "LIST_VALUE": lambda args: exp.Array(expressions=args),
            "MAKE_DATE": exp.DateFromParts.from_arg_list,
            "MAKE_TIME": exp.TimeFromParts.from_arg_list,
            "MAKE_TIMESTAMP": _build_make_timestamp,
            "QUANTILE_CONT": exp.PercentileCont.from_arg_list,
            "QUANTILE_DISC": exp.PercentileDisc.from_arg_list,
            "RANGE": _build_generate_series(end_exclusive=True),
            "REGEXP_EXTRACT": build_regexp_extract(exp.RegexpExtract),
            "REGEXP_EXTRACT_ALL": build_regexp_extract(exp.RegexpExtractAll),
            "REGEXP_MATCHES": exp.RegexpLike.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                modifiers=seq_get(args, 3),
                single_replace=True,
            ),
            "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
            "STRFTIME": build_formatted_time(exp.TimeToStr, "duckdb"),
            "STRING_SPLIT": exp.Split.from_arg_list,
            "STRING_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "STRING_TO_ARRAY": exp.Split.from_arg_list,
            "STRPTIME": build_formatted_time(exp.StrToTime, "duckdb"),
            "STRUCT_PACK": exp.Struct.from_arg_list,
            "STR_SPLIT": exp.Split.from_arg_list,
            "STR_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "TIME_BUCKET": exp.DateBin.from_arg_list,
            "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
            "XOR": binary_from_function(exp.BitwiseXor),
        }

        FUNCTIONS.pop("DATE_SUB")
        FUNCTIONS.pop("GLOB")

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            **dict.fromkeys(
                ("GROUP_CONCAT", "LISTAGG", "STRINGAGG"), lambda self: self._parse_string_agg()
            ),
        }
        FUNCTION_PARSERS.pop("DECODE")

        NO_PAREN_FUNCTION_PARSERS = {
            **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
            "MAP": lambda self: self._parse_map(),
            "@": lambda self: exp.Abs(this=self._parse_bitwise()),
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.SEMI,
            TokenType.ANTI,
        }

        PLACEHOLDER_PARSERS = {
            **parser.Parser.PLACEHOLDER_PARSERS,
            TokenType.PARAMETER: lambda self: (
                self.expression(exp.Placeholder, this=self._prev.text)
                if self._match(TokenType.NUMBER) or self._match_set(self.ID_VAR_TOKENS)
                else None
            ),
        }

        TYPE_CONVERTERS = {
            # https://duckdb.org/docs/sql/data_types/numeric
            exp.DataType.Type.DECIMAL: build_default_decimal_type(precision=18, scale=3),
            # https://duckdb.org/docs/sql/data_types/text
            exp.DataType.Type.TEXT: lambda dtype: exp.DataType.build("TEXT"),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.ATTACH: lambda self: self._parse_attach_detach(),
            TokenType.DETACH: lambda self: self._parse_attach_detach(is_attach=False),
            TokenType.FORCE: lambda self: self._parse_force(),
            TokenType.INSTALL: lambda self: self._parse_install(),
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        SET_PARSERS = {
            **parser.Parser.SET_PARSERS,
            "VARIABLE": lambda self: self._parse_set_item_assignment("VARIABLE"),
        }

        def _parse_lambda(self, alias: bool = False) -> t.Optional[exp.Expression]:
            index = self._index
            if not self._match_text_seq("LAMBDA"):
                return super()._parse_lambda(alias=alias)

            expressions = self._parse_csv(self._parse_lambda_arg)
            if not self._match(TokenType.COLON):
                self._retreat(index)
                return None

            this = self._replace_lambda(self._parse_assignment(), expressions)
            return self.expression(exp.Lambda, this=this, expressions=expressions, colon=True)

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            # DuckDB supports prefix aliases, e.g. foo: 1
            if self._next and self._next.token_type == TokenType.COLON:
                alias = self._parse_id_var(tokens=self.ALIAS_TOKENS)
                self._match(TokenType.COLON)
                comments = self._prev_comments or []

                this = self._parse_assignment()
                if isinstance(this, exp.Expression):
                    # Moves the comment next to the alias in `alias: expr /* comment */`
                    comments += this.pop_comments() or []

                return self.expression(exp.Alias, comments=comments, this=this, alias=alias)

            return super()._parse_expression()

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
            # DuckDB supports prefix aliases, e.g. FROM foo: bar
            if self._next and self._next.token_type == TokenType.COLON:
                alias = self._parse_table_alias(
                    alias_tokens=alias_tokens or self.TABLE_ALIAS_TOKENS
                )
                self._match(TokenType.COLON)
                comments = self._prev_comments or []
            else:
                alias = None
                comments = []

            table = super()._parse_table(
                schema=schema,
                joins=joins,
                alias_tokens=alias_tokens,
                parse_bracket=parse_bracket,
                is_db_reference=is_db_reference,
                parse_partition=parse_partition,
            )
            if isinstance(table, exp.Expression) and isinstance(alias, exp.TableAlias):
                # Moves the comment next to the alias in `alias: table /* comment */`
                comments += table.pop_comments() or []
                alias.comments = alias.pop_comments() + comments
                table.set("alias", alias)

            return table

        def _parse_table_sample(self, as_modifier: bool = False) -> t.Optional[exp.TableSample]:
            # https://duckdb.org/docs/sql/samples.html
            sample = super()._parse_table_sample(as_modifier=as_modifier)
            if sample and not sample.args.get("method"):
                if sample.args.get("size"):
                    sample.set("method", exp.var("RESERVOIR"))
                else:
                    sample.set("method", exp.var("SYSTEM"))

            return sample

        def _parse_bracket(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            bracket = super()._parse_bracket(this)

            if self.dialect.version < (1, 2) and isinstance(bracket, exp.Bracket):
                # https://duckdb.org/2025/02/05/announcing-duckdb-120.html#breaking-changes
                bracket.set("returns_list_for_maps", True)

            return bracket

        def _parse_map(self) -> exp.ToMap | exp.Map:
            if self._match(TokenType.L_BRACE, advance=False):
                return self.expression(exp.ToMap, this=self._parse_bracket())

            args = self._parse_wrapped_csv(self._parse_assignment)
            return self.expression(exp.Map, keys=seq_get(args, 0), values=seq_get(args, 1))

        def _parse_struct_types(self, type_required: bool = False) -> t.Optional[exp.Expression]:
            return self._parse_field_def()

        def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
            if len(aggregations) == 1:
                return super()._pivot_column_names(aggregations)
            return pivot_column_names(aggregations, dialect="duckdb")

        def _parse_attach_detach(self, is_attach=True) -> exp.Attach | exp.Detach:
            def _parse_attach_option() -> exp.AttachOption:
                return self.expression(
                    exp.AttachOption,
                    this=self._parse_var(any_token=True),
                    expression=self._parse_field(any_token=True),
                )

            self._match(TokenType.DATABASE)
            exists = self._parse_exists(not_=is_attach)
            this = self._parse_alias(self._parse_primary_or_var(), explicit=True)

            if self._match(TokenType.L_PAREN, advance=False):
                expressions = self._parse_wrapped_csv(_parse_attach_option)
            else:
                expressions = None

            return (
                self.expression(exp.Attach, this=this, exists=exists, expressions=expressions)
                if is_attach
                else self.expression(exp.Detach, this=this, exists=exists)
            )

        def _parse_show_duckdb(self, this: str) -> exp.Show:
            return self.expression(exp.Show, this=this)

        def _parse_force(self) -> exp.Install | exp.Command:
            # FORCE can only be followed by INSTALL or CHECKPOINT
            # In the case of CHECKPOINT, we fallback
            if not self._match(TokenType.INSTALL):
                return self._parse_as_command(self._prev)

            return self._parse_install(force=True)

        def _parse_install(self, force: bool = False) -> exp.Install:
            return self.expression(
                exp.Install,
                this=self._parse_id_var(),
                from_=self._parse_var_or_string() if self._match(TokenType.FROM) else None,
                force=force,
            )

        def _parse_primary(self) -> t.Optional[exp.Expression]:
            if self._match_pair(TokenType.HASH, TokenType.NUMBER):
                return exp.PositionalColumn(this=exp.Literal.number(self._prev.text))

            return super()._parse_primary()

    class Generator(generator.Generator):
        PARAMETER_TOKEN = "$"
        NAMED_PLACEHOLDER_TOKEN = "$"
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        LIMIT_FETCH = "LIMIT"
        STRUCT_DELIMITER = ("(", ")")
        RENAME_TABLE_WITH_DB = False
        NVL2_SUPPORTED = False
        SEMI_ANTI_JOIN_WITH_SIDE = False
        TABLESAMPLE_KEYWORDS = "USING SAMPLE"
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        IGNORE_NULLS_IN_FUNC = True
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False
        MULTI_ARG_DISTINCT = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_TO_NUMBER = False
        SUPPORTS_WINDOW_EXCLUDE = True
        COPY_HAS_INTO_KEYWORD = False
        STAR_EXCEPT = "EXCLUDE"
        PAD_FILL_PATTERN_IS_REQUIRED = True
        ARRAY_CONCAT_IS_VAR_LEN = False
        ARRAY_SIZE_DIM_REQUIRED = False
        NORMALIZE_EXTRACT_DATE_PARTS = True
        SUPPORTS_LIKE_QUANTIFIERS = False
        SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = True

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: _anyvalue_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.Boolnot: lambda self, e: f"NOT ({self.sql(e, 'this')})",
            exp.Array: transforms.preprocess(
                [transforms.inherit_struct_field_names],
                generator=inline_array_unless_query,
            ),
            exp.ArrayAppend: rename_func("LIST_APPEND"),
            exp.ArrayFilter: rename_func("LIST_FILTER"),
            exp.ArrayRemove: remove_from_array_using_filter,
            exp.ArraySort: _array_sort_sql,
            exp.ArrayPrepend: lambda self, e: self.func("LIST_PREPEND", e.expression, e.this),
            exp.ArraySum: rename_func("LIST_SUM"),
            exp.ArrayUniqueAgg: lambda self, e: self.func(
                "LIST", exp.Distinct(expressions=[e.this])
            ),
            exp.BitwiseAnd: lambda self, e: self._bitwise_op(e, "&"),
            exp.BitwiseAndAgg: _bitwise_agg_sql,
            exp.BitwiseLeftShift: _bitshift_sql,
            exp.BitwiseOr: lambda self, e: self._bitwise_op(e, "|"),
            exp.BitwiseOrAgg: _bitwise_agg_sql,
            exp.BitwiseRightShift: _bitshift_sql,
            exp.BitwiseXorAgg: _bitwise_agg_sql,
            exp.CommentColumnConstraint: no_comment_column_constraint_sql,
            exp.Corr: lambda self, e: self._corr_sql(e),
            exp.CosineDistance: rename_func("LIST_COSINE_DISTANCE"),
            exp.CurrentTime: lambda *_: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfWeekIso: rename_func("ISODOW"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.Dayname: lambda self, e: (
                self.func("STRFTIME", e.this, exp.Literal.string("%a"))
                if e.args.get("abbreviated")
                else self.func("DAYNAME", e.this)
            ),
            exp.Monthname: lambda self, e: (
                self.func("STRFTIME", e.this, exp.Literal.string("%b"))
                if e.args.get("abbreviated")
                else self.func("MONTHNAME", e.this)
            ),
            exp.DataType: _datatype_sql,
            exp.Date: _date_sql,
            exp.DateAdd: _date_delta_to_binary_interval_op(),
            exp.DateFromParts: _date_from_parts_sql,
            exp.DateSub: _date_delta_to_binary_interval_op(),
            exp.DateDiff: _date_diff_sql,
            exp.DateStrToDate: datestrtodate_sql,
            exp.Datetime: no_datetime_sql,
            exp.DatetimeDiff: _date_diff_sql,
            exp.DatetimeSub: _date_delta_to_binary_interval_op(),
            exp.DatetimeAdd: _date_delta_to_binary_interval_op(),
            exp.DateToDi: lambda self,
            e: f"CAST(STRFTIME({self.sql(e, 'this')}, {DuckDB.DATEINT_FORMAT}) AS INT)",
            exp.Decode: lambda self, e: encode_decode_sql(self, e, "DECODE", replace=False),
            exp.DiToDate: lambda self,
            e: f"CAST(STRPTIME(CAST({self.sql(e, 'this')} AS TEXT), {DuckDB.DATEINT_FORMAT}) AS DATE)",
            exp.Encode: lambda self, e: encode_decode_sql(self, e, "ENCODE", replace=False),
            exp.EuclideanDistance: rename_func("LIST_DISTANCE"),
            exp.GenerateDateArray: _generate_datetime_array_sql,
            exp.GenerateTimestampArray: _generate_datetime_array_sql,
            exp.GroupConcat: lambda self, e: groupconcat_sql(self, e, within_group=False),
            exp.Explode: rename_func("UNNEST"),
            exp.IntDiv: lambda self, e: self.binary(e, "//"),
            exp.IsInf: rename_func("ISINF"),
            exp.IsNan: rename_func("ISNAN"),
            exp.Ceil: _ceil_floor,
            exp.Floor: _ceil_floor,
            exp.JSONBExists: rename_func("JSON_EXISTS"),
            exp.JSONExtract: _arrow_json_extract_sql,
            exp.JSONExtractArray: _json_extract_value_array_sql,
            exp.JSONFormat: _json_format_sql,
            exp.JSONValueArray: _json_extract_value_array_sql,
            exp.Lateral: explode_to_unnest_sql,
            exp.LogicalOr: lambda self, e: self.func("BOOL_OR", _cast_to_boolean(e.this)),
            exp.LogicalAnd: lambda self, e: self.func("BOOL_AND", _cast_to_boolean(e.this)),
            exp.BoolxorAgg: _boolxor_agg_sql,
            exp.MakeInterval: lambda self, e: no_make_interval_sql(self, e, sep=" "),
            exp.Initcap: _initcap_sql,
            exp.MD5Digest: lambda self, e: self.func("UNHEX", self.func("MD5", e.this)),
            exp.SHA1Digest: lambda self, e: self.func("UNHEX", self.func("SHA1", e.this)),
            exp.SHA2Digest: lambda self, e: self.func("UNHEX", sha2_digest_sql(self, e)),
            exp.MonthsBetween: months_between_sql,
            exp.PercentileCont: rename_func("QUANTILE_CONT"),
            exp.PercentileDisc: rename_func("QUANTILE_DISC"),
            # DuckDB doesn't allow qualified columns inside of PIVOT expressions.
            # See: https://github.com/duckdb/duckdb/blob/671faf92411182f81dce42ac43de8bfb05d9909e/src/planner/binder/tableref/bind_pivot.cpp#L61-L62
            exp.Pivot: transforms.preprocess([transforms.unqualify_columns]),
            exp.RegexpReplace: lambda self, e: self.func(
                "REGEXP_REPLACE",
                e.this,
                e.expression,
                e.args.get("replacement"),
                regexp_replace_global_modifier(e),
            ),
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.RegexpILike: lambda self, e: self.func(
                "REGEXP_MATCHES", e.this, e.expression, exp.Literal.string("i")
            ),
            exp.RegexpSplit: rename_func("STR_SPLIT_REGEX"),
            exp.RegrValx: _regr_val_sql,
            exp.RegrValy: _regr_val_sql,
            exp.Return: lambda self, e: self.sql(e, "this"),
            exp.ReturnsProperty: lambda self, e: "TABLE" if isinstance(e.this, exp.Schema) else "",
            exp.Rand: rename_func("RANDOM"),
            exp.SHA: rename_func("SHA1"),
            exp.SHA2: sha256_sql,
            exp.Split: rename_func("STR_SPLIT"),
            exp.SortArray: _sort_array_sql,
            exp.StrPosition: strposition_sql,
            exp.StrToUnix: lambda self, e: self.func(
                "EPOCH", self.func("STRPTIME", e.this, self.format_time(e))
            ),
            exp.Struct: _struct_sql,
            exp.Transform: rename_func("LIST_TRANSFORM"),
            exp.TimeAdd: _date_delta_to_binary_interval_op(),
            exp.TimeSub: _date_delta_to_binary_interval_op(),
            exp.Time: no_time_sql,
            exp.TimeDiff: _timediff_sql,
            exp.Timestamp: no_timestamp_sql,
            exp.TimestampAdd: _date_delta_to_binary_interval_op(),
            exp.TimestampDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.unit), e.expression, e.this
            ),
            exp.TimestampSub: _date_delta_to_binary_interval_op(),
            exp.TimeStrToDate: lambda self, e: self.sql(exp.cast(e.this, exp.DataType.Type.DATE)),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: self.func(
                "EPOCH", exp.cast(e.this, exp.DataType.Type.TIMESTAMP)
            ),
            exp.TimeToStr: lambda self, e: self.func("STRFTIME", e.this, self.format_time(e)),
            exp.ToBoolean: _to_boolean_sql,
            exp.TimeToUnix: rename_func("EPOCH"),
            exp.TsOrDiToDi: lambda self,
            e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS TEXT), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _date_delta_to_binary_interval_op(),
            exp.TsOrDsDiff: lambda self, e: self.func(
                "DATE_DIFF",
                f"'{e.args.get('unit') or 'DAY'}'",
                exp.cast(e.expression, exp.DataType.Type.TIMESTAMP),
                exp.cast(e.this, exp.DataType.Type.TIMESTAMP),
            ),
            exp.UnixMicros: lambda self, e: self.func("EPOCH_US", _implicit_datetime_cast(e.this)),
            exp.UnixMillis: lambda self, e: self.func("EPOCH_MS", _implicit_datetime_cast(e.this)),
            exp.UnixSeconds: lambda self, e: self.sql(
                exp.cast(
                    self.func("EPOCH", _implicit_datetime_cast(e.this)), exp.DataType.Type.BIGINT
                )
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "STRFTIME", self.func("TO_TIMESTAMP", e.this), self.format_time(e)
            ),
            exp.DatetimeTrunc: lambda self, e: self.func(
                "DATE_TRUNC", unit_to_str(e), exp.cast(e.this, exp.DataType.Type.DATETIME)
            ),
            exp.UnixToTime: _unix_to_time_sql,
            exp.UnixToTimeStr: lambda self, e: f"CAST(TO_TIMESTAMP({self.sql(e, 'this')}) AS TEXT)",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.YearOfWeek: lambda self, e: self.sql(
                exp.Extract(
                    this=exp.Var(this="ISOYEAR"),
                    expression=e.this,
                )
            ),
            exp.YearOfWeekIso: lambda self, e: self.sql(
                exp.Extract(
                    this=exp.Var(this="ISOYEAR"),
                    expression=e.this,
                )
            ),
            exp.Xor: bool_xor_sql,
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("LEVENSHTEIN")
            ),
            exp.JSONObjectAgg: rename_func("JSON_GROUP_OBJECT"),
            exp.JSONBObjectAgg: rename_func("JSON_GROUP_OBJECT"),
            exp.DateBin: rename_func("TIME_BUCKET"),
            exp.LastDay: _last_day_sql,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
            exp.JSONPathWildcard,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.BPCHAR: "TEXT",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.DECFLOAT: "DECIMAL(38, 5)",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.JSONB: "JSON",
            exp.DataType.Type.NCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
            exp.DataType.Type.UINT: "UINTEGER",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.ROWVERSION: "BLOB",
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMPTZ",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_S: "TIMESTAMP_S",
            exp.DataType.Type.TIMESTAMP_MS: "TIMESTAMP_MS",
            exp.DataType.Type.TIMESTAMP_NS: "TIMESTAMP_NS",
            exp.DataType.Type.BIGDECIMAL: "DECIMAL(38, 5)",
        }

        # https://github.com/duckdb/duckdb/blob/ff7f24fd8e3128d94371827523dae85ebaf58713/third_party/libpg_query/grammar/keywords/reserved_keywords.list#L1-L77
        RESERVED_KEYWORDS = {
            "array",
            "analyse",
            "union",
            "all",
            "when",
            "in_p",
            "default",
            "create_p",
            "window",
            "asymmetric",
            "to",
            "else",
            "localtime",
            "from",
            "end_p",
            "select",
            "current_date",
            "foreign",
            "with",
            "grant",
            "session_user",
            "or",
            "except",
            "references",
            "fetch",
            "limit",
            "group_p",
            "leading",
            "into",
            "collate",
            "offset",
            "do",
            "then",
            "localtimestamp",
            "check_p",
            "lateral_p",
            "current_role",
            "where",
            "asc_p",
            "placing",
            "desc_p",
            "user",
            "unique",
            "initially",
            "column",
            "both",
            "some",
            "as",
            "any",
            "only",
            "deferrable",
            "null_p",
            "current_time",
            "true_p",
            "table",
            "case",
            "trailing",
            "variadic",
            "for",
            "on",
            "distinct",
            "false_p",
            "not",
            "constraint",
            "current_timestamp",
            "returning",
            "primary",
            "intersect",
            "having",
            "analyze",
            "current_user",
            "and",
            "cast",
            "symmetric",
            "using",
            "order",
            "current_catalog",
        }

        UNWRAPPED_INTERVAL_VALUES = (exp.Literal, exp.Paren)

        # DuckDB doesn't generally support CREATE TABLE .. properties
        # https://duckdb.org/docs/sql/statements/create_table.html
        PROPERTIES_LOCATION = {
            prop: exp.Properties.Location.UNSUPPORTED
            for prop in generator.Generator.PROPERTIES_LOCATION
        }

        # There are a few exceptions (e.g. temporary tables) which are supported or
        # can be transpiled to DuckDB, so we explicitly override them accordingly
        PROPERTIES_LOCATION[exp.LikeProperty] = exp.Properties.Location.POST_SCHEMA
        PROPERTIES_LOCATION[exp.TemporaryProperty] = exp.Properties.Location.POST_CREATE
        PROPERTIES_LOCATION[exp.ReturnsProperty] = exp.Properties.Location.POST_ALIAS
        PROPERTIES_LOCATION[exp.SequenceProperties] = exp.Properties.Location.POST_EXPRESSION

        IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS = (
            exp.FirstValue,
            exp.Lag,
            exp.LastValue,
            exp.Lead,
            exp.NthValue,
        )

        # Template for ZIPF transpilation - placeholders get replaced with actual parameters
        ZIPF_TEMPLATE: exp.Expression = exp.maybe_parse(
            """
            WITH rand AS (SELECT :random_expr AS r),
            weights AS (
                SELECT i, 1.0 / POWER(i, :s) AS w
                FROM RANGE(1, :n + 1) AS t(i)
            ),
            cdf AS (
                SELECT i, SUM(w) OVER (ORDER BY i) / SUM(w) OVER () AS p
                FROM weights
            )
            SELECT MIN(i)
            FROM cdf
            WHERE p >= (SELECT r FROM rand)
            """
        )

        # Template for NORMAL transpilation using Box-Muller transform
        # mean + (stddev * sqrt(-2 * ln(u1)) * cos(2 * pi * u2))
        NORMAL_TEMPLATE: exp.Expression = exp.maybe_parse(
            ":mean + (:stddev * SQRT(-2 * LN(GREATEST(:u1, 1e-10))) * COS(2 * PI() * :u2))"
        )

        # Template for generating a seeded pseudo-random value in [0, 1) from a hash
        SEEDED_RANDOM_TEMPLATE: exp.Expression = exp.maybe_parse(
            "(ABS(HASH(:seed)) % 1000000) / 1000000.0"
        )

        # Template for RANDSTR transpilation - placeholders get replaced with actual parameters
        RANDSTR_TEMPLATE: exp.Expression = exp.maybe_parse(
            f"""
            SELECT LISTAGG(
                SUBSTRING(
                    '{RANDSTR_CHAR_POOL}',
                    1 + CAST(FLOOR(random_value * 62) AS INT),
                    1
                ),
                ''
            )
            FROM (
                SELECT (ABS(HASH(i + :seed)) % 1000) / 1000.0 AS random_value
                FROM RANGE(:length) AS t(i)
            )
            """,
        )

        def bitmapbucketnumber_sql(
            self: DuckDB.Generator, expression: exp.BitmapBucketNumber
        ) -> str:
            """
            Transpile BITMAP_BUCKET_NUMBER function from Snowflake to DuckDB equivalent.

            Snowflake's BITMAP_BUCKET_NUMBER returns a 1-based bucket identifier where:
            - Each bucket covers 32,768 values
            - Bucket numbering starts at 1
            - Formula: ((value - 1) // 32768) + 1 for positive values

            For non-positive values (0 and negative), we use value // 32768 to avoid
            producing bucket 0 or positive bucket IDs for negative inputs.
            """
            value = expression.this

            positive_formula = ((value - 1) // 32768) + 1
            non_positive_formula = value // 32768

            # CASE WHEN value > 0 THEN ((value - 1) // 32768) + 1 ELSE value // 32768 END
            case_expr = (
                exp.case()
                .when(exp.GT(this=value, expression=exp.Literal.number(0)), positive_formula)
                .else_(non_positive_formula)
            )
            return self.sql(case_expr)

        def bitmapbitposition_sql(self: DuckDB.Generator, expression: exp.BitmapBitPosition) -> str:
            """
            Transpile Snowflake's BITMAP_BIT_POSITION to DuckDB CASE expression.

            Snowflake's BITMAP_BIT_POSITION behavior:
            - For n <= 0: returns ABS(n) % 32768
            - For n > 0: returns (n - 1) % 32768 (maximum return value is 32767)
            """
            this = expression.this

            return self.sql(
                exp.Mod(
                    this=exp.Paren(
                        this=exp.If(
                            this=exp.GT(this=this, expression=exp.Literal.number(0)),
                            true=this - exp.Literal.number(1),
                            false=exp.Abs(this=this),
                        )
                    ),
                    expression=MAX_BIT_POSITION,
                )
            )

        def randstr_sql(self: DuckDB.Generator, expression: exp.Randstr) -> str:
            """
            Transpile Snowflake's RANDSTR to DuckDB equivalent using deterministic hash-based random.
            Uses a pre-parsed template with placeholders replaced by expression nodes.

            RANDSTR(length, generator) generates a random string of specified length.
            - With numeric seed: Use HASH(i + seed) for deterministic output (same seed = same result)
            - With RANDOM(): Use RANDOM() in the hash for non-deterministic output
            - No generator: Use default seed value
            """
            length = expression.this
            generator = expression.args.get("generator")

            if generator:
                if isinstance(generator, exp.Rand):
                    # If it's RANDOM(), use its seed if available, otherwise use RANDOM() itself
                    seed_value = generator.this or generator
                else:
                    # Const/int or other expression - use as seed directly
                    seed_value = generator
            else:
                # No generator specified, use default seed (arbitrary but deterministic)
                seed_value = exp.Literal.number(RANDSTR_SEED)

            replacements = {"seed": seed_value, "length": length}
            return f"({self.sql(exp.replace_placeholders(self.RANDSTR_TEMPLATE, **replacements))})"

        def zipf_sql(self: DuckDB.Generator, expression: exp.Zipf) -> str:
            """
            Transpile Snowflake's ZIPF to DuckDB using CDF-based inverse sampling.
            Uses a pre-parsed template with placeholders replaced by expression nodes.
            """
            s = expression.this
            n = expression.args["elementcount"]
            gen = expression.args["gen"]

            if not isinstance(gen, exp.Rand):
                # (ABS(HASH(seed)) % 1000000) / 1000000.0
                random_expr: exp.Expression = exp.Div(
                    this=exp.Paren(
                        this=exp.Mod(
                            this=exp.Abs(this=exp.Anonymous(this="HASH", expressions=[gen.copy()])),
                            expression=exp.Literal.number(1000000),
                        )
                    ),
                    expression=exp.Literal.number(1000000.0),
                )
            else:
                # Use RANDOM() for non-deterministic output
                random_expr = exp.Rand()

            replacements = {"s": s, "n": n, "random_expr": random_expr}
            return f"({self.sql(exp.replace_placeholders(self.ZIPF_TEMPLATE, **replacements))})"

        def tobinary_sql(self: DuckDB.Generator, expression: exp.ToBinary) -> str:
            """
            TO_BINARY and TRY_TO_BINARY transpilation:
            - 'HEX': TO_BINARY('48454C50', 'HEX') → UNHEX('48454C50')
            - 'UTF-8': TO_BINARY('TEST', 'UTF-8') → ENCODE('TEST')
            - 'BASE64': TO_BINARY('SEVMUA==', 'BASE64') → FROM_BASE64('SEVMUA==')

            For TRY_TO_BINARY (safe=True), wrap with TRY():
            - 'HEX': TRY_TO_BINARY('invalid', 'HEX') → TRY(UNHEX('invalid'))
            """
            value = expression.this
            format_arg = expression.args.get("format")
            is_safe = expression.args.get("safe")

            fmt = "HEX"
            if format_arg:
                fmt = format_arg.name.upper()

            if expression.is_type(exp.DataType.Type.BINARY):
                if fmt == "UTF-8":
                    result = self.func("ENCODE", value)
                elif fmt == "BASE64":
                    result = self.func("FROM_BASE64", value)
                elif fmt == "HEX":
                    result = self.func("UNHEX", value)
                else:
                    if is_safe:
                        return self.sql(exp.null())
                    else:
                        self.unsupported(f"format {fmt} is not supported")
                        result = self.func("TO_BINARY", value)

                # Wrap with TRY() for TRY_TO_BINARY
                if is_safe:
                    result = self.func("TRY", result)

                return result

            # Fallback, which needs to be updated if want to support transpilation from other dialects than Snowflake
            return self.func("TO_BINARY", value)

        def _greatest_least_sql(
            self: DuckDB.Generator, expression: exp.Greatest | exp.Least
        ) -> str:
            """
            Handle GREATEST/LEAST functions with dialect-aware NULL behavior.

            - If ignore_nulls=False (BigQuery-style): return NULL if any argument is NULL
            - If ignore_nulls=True (DuckDB/PostgreSQL-style): ignore NULLs, return greatest/least non-NULL value
            """
            # Get all arguments
            all_args = [expression.this, *expression.expressions]
            fallback_sql = self.function_fallback_sql(expression)

            if expression.args.get("ignore_nulls"):
                # DuckDB/PostgreSQL behavior: use native GREATEST/LEAST (ignores NULLs)
                return self.sql(fallback_sql)

            # return NULL if any argument is NULL
            case_expr = exp.case().when(
                exp.or_(*[arg.is_(exp.null()) for arg in all_args], copy=False),
                exp.null(),
                copy=False,
            )
            case_expr.set("default", fallback_sql)
            return self.sql(case_expr)

        def greatest_sql(self: DuckDB.Generator, expression: exp.Greatest) -> str:
            return self._greatest_least_sql(expression)

        def least_sql(self: DuckDB.Generator, expression: exp.Least) -> str:
            return self._greatest_least_sql(expression)

        def lambda_sql(
            self, expression: exp.Lambda, arrow_sep: str = "->", wrap: bool = True
        ) -> str:
            if expression.args.get("colon"):
                prefix = "LAMBDA "
                arrow_sep = ":"
                wrap = False
            else:
                prefix = ""

            lambda_sql = super().lambda_sql(expression, arrow_sep=arrow_sep, wrap=wrap)
            return f"{prefix}{lambda_sql}"

        def show_sql(self, expression: exp.Show) -> str:
            return f"SHOW {expression.name}"

        def install_sql(self, expression: exp.Install) -> str:
            force = "FORCE " if expression.args.get("force") else ""
            this = self.sql(expression, "this")
            from_clause = expression.args.get("from_")
            from_clause = f" FROM {from_clause}" if from_clause else ""
            return f"{force}INSTALL {this}{from_clause}"

        def approxtopk_sql(self, expression: exp.ApproxTopK) -> str:
            self.unsupported(
                "APPROX_TOP_K cannot be transpiled to DuckDB due to incompatible return types. "
            )
            return self.function_fallback_sql(expression)

        def fromiso8601timestamp_sql(self, expression: exp.FromISO8601Timestamp) -> str:
            return self.sql(exp.cast(expression.this, exp.DataType.Type.TIMESTAMPTZ))

        def strtotime_sql(self, expression: exp.StrToTime) -> str:
            # Check if target_type requires TIMESTAMPTZ (for LTZ/TZ variants)
            target_type = expression.args.get("target_type")
            needs_tz = target_type and target_type.this in (
                exp.DataType.Type.TIMESTAMPLTZ,
                exp.DataType.Type.TIMESTAMPTZ,
            )

            if expression.args.get("safe"):
                formatted_time = self.format_time(expression)
                cast_type = (
                    exp.DataType.Type.TIMESTAMPTZ if needs_tz else exp.DataType.Type.TIMESTAMP
                )
                return self.sql(
                    exp.cast(self.func("TRY_STRPTIME", expression.this, formatted_time), cast_type)
                )

            base_sql = str_to_time_sql(self, expression)
            if needs_tz:
                return self.sql(
                    exp.cast(
                        base_sql,
                        exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ),
                    )
                )
            return base_sql

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            formatted_time = self.format_time(expression)
            function_name = "STRPTIME" if not expression.args.get("safe") else "TRY_STRPTIME"
            return self.sql(
                exp.cast(
                    self.func(function_name, expression.this, formatted_time),
                    exp.DataType(this=exp.DataType.Type.DATE),
                )
            )

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            if not expression.this:
                return "CURRENT_DATE"

            expr = exp.Cast(
                this=exp.AtTimeZone(this=exp.CurrentTimestamp(), zone=expression.this),
                to=exp.DataType(this=exp.DataType.Type.DATE),
            )
            return self.sql(expr)

        def parsejson_sql(self, expression: exp.ParseJSON) -> str:
            arg = expression.this
            if expression.args.get("safe"):
                return self.sql(exp.case().when(exp.func("json_valid", arg), arg).else_(exp.null()))
            return self.func("JSON", arg)

        def normal_sql(self, expression: exp.Normal) -> str:
            """
            Transpile Snowflake's NORMAL(mean, stddev, gen) to DuckDB.

            Uses the Box-Muller transform via NORMAL_TEMPLATE.
            """
            mean = expression.this
            stddev = expression.args["stddev"]
            gen: exp.Expression = expression.args["gen"]

            # Build two uniform random values [0, 1) for Box-Muller transform
            if isinstance(gen, exp.Rand) and gen.this is None:
                u1: exp.Expression = exp.Rand()
                u2: exp.Expression = exp.Rand()
            else:
                # Seeded: derive two values using HASH with different inputs
                seed = gen.this if isinstance(gen, exp.Rand) else gen
                u1 = exp.replace_placeholders(self.SEEDED_RANDOM_TEMPLATE, seed=seed)
                u2 = exp.replace_placeholders(
                    self.SEEDED_RANDOM_TEMPLATE,
                    seed=exp.Add(this=seed.copy(), expression=exp.Literal.number(1)),
                )

            replacements = {"mean": mean, "stddev": stddev, "u1": u1, "u2": u2}
            return self.sql(exp.replace_placeholders(self.NORMAL_TEMPLATE, **replacements))

        def uniform_sql(self, expression: exp.Uniform) -> str:
            """
            Transpile Snowflake's UNIFORM(min, max, gen) to DuckDB.

            UNIFORM returns a random value in [min, max]:
            - Integer result if both min and max are integers
            - Float result if either min or max is a float
            """
            min_val = expression.this
            max_val = expression.expression
            gen = expression.args.get("gen")

            # Determine if result should be integer (both bounds are integers).
            # We do this to emulate Snowflake's behavior, INT -> INT, FLOAT -> FLOAT
            is_int_result = min_val.is_int and max_val.is_int

            # Build the random value expression [0, 1)
            if not isinstance(gen, exp.Rand):
                # Seed value: (ABS(HASH(seed)) % 1000000) / 1000000.0
                random_expr: exp.Expression = exp.Div(
                    this=exp.Paren(
                        this=exp.Mod(
                            this=exp.Abs(this=exp.Anonymous(this="HASH", expressions=[gen])),
                            expression=exp.Literal.number(1000000),
                        )
                    ),
                    expression=exp.Literal.number(1000000.0),
                )
            else:
                random_expr = exp.Rand()

            # Build: min + random * (max - min [+ 1 for int])
            range_expr: exp.Expression = exp.Sub(this=max_val, expression=min_val)
            if is_int_result:
                range_expr = exp.Add(this=range_expr, expression=exp.Literal.number(1))

            result: exp.Expression = exp.Add(
                this=min_val,
                expression=exp.Mul(this=random_expr, expression=exp.Paren(this=range_expr)),
            )

            if is_int_result:
                result = exp.Cast(
                    this=exp.Floor(this=result),
                    to=exp.DataType.build("BIGINT"),
                )

            return self.sql(result)

        def timefromparts_sql(self, expression: exp.TimeFromParts) -> str:
            nano = expression.args.get("nano")
            if nano is not None:
                expression.set(
                    "sec", expression.args["sec"] + nano.pop() / exp.Literal.number(1000000000.0)
                )

            return rename_func("MAKE_TIME")(self, expression)

        def timestampfromparts_sql(self, expression: exp.TimestampFromParts) -> str:
            sec = expression.args["sec"]

            milli = expression.args.get("milli")
            if milli is not None:
                sec += milli.pop() / exp.Literal.number(1000.0)

            nano = expression.args.get("nano")
            if nano is not None:
                sec += nano.pop() / exp.Literal.number(1000000000.0)

            if milli or nano:
                expression.set("sec", sec)

            return rename_func("MAKE_TIMESTAMP")(self, expression)

        def tablesample_sql(
            self,
            expression: exp.TableSample,
            tablesample_keyword: t.Optional[str] = None,
        ) -> str:
            if not isinstance(expression.parent, exp.Select):
                # This sample clause only applies to a single source, not the entire resulting relation
                tablesample_keyword = "TABLESAMPLE"

            if expression.args.get("size"):
                method = expression.args.get("method")
                if method and method.name.upper() != "RESERVOIR":
                    self.unsupported(
                        f"Sampling method {method} is not supported with a discrete sample count, "
                        "defaulting to reservoir sampling"
                    )
                    expression.set("method", exp.var("RESERVOIR"))

            return super().tablesample_sql(expression, tablesample_keyword=tablesample_keyword)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            if isinstance(expression.parent, exp.UserDefinedFunction):
                return self.sql(expression, "this")
            return super().columndef_sql(expression, sep)

        def join_sql(self, expression: exp.Join) -> str:
            if (
                not expression.args.get("using")
                and not expression.args.get("on")
                and not expression.method
                and (expression.kind in ("", "INNER", "OUTER"))
            ):
                # Some dialects support `LEFT/INNER JOIN UNNEST(...)` without an explicit ON clause
                # DuckDB doesn't, but we can just add a dummy ON clause that is always true
                if isinstance(expression.this, exp.Unnest):
                    return super().join_sql(expression.on(exp.true()))

                expression.set("side", None)
                expression.set("kind", None)

            return super().join_sql(expression)

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            # GENERATE_SERIES(a, b) -> [a, b], RANGE(a, b) -> [a, b)
            if expression.args.get("is_end_exclusive"):
                return rename_func("RANGE")(self, expression)

            return self.function_fallback_sql(expression)

        def countif_sql(self, expression: exp.CountIf) -> str:
            if self.dialect.version >= (1, 2):
                return self.function_fallback_sql(expression)

            # https://github.com/tobymao/sqlglot/pull/4749
            return count_if_to_sum(self, expression)

        def bracket_sql(self, expression: exp.Bracket) -> str:
            if self.dialect.version >= (1, 2):
                return super().bracket_sql(expression)

            # https://duckdb.org/2025/02/05/announcing-duckdb-120.html#breaking-changes
            this = expression.this
            if isinstance(this, exp.Array):
                this.replace(exp.paren(this))

            bracket = super().bracket_sql(expression)

            if not expression.args.get("returns_list_for_maps"):
                if not this.type:
                    from sqlglot.optimizer.annotate_types import annotate_types

                    this = annotate_types(this, dialect=self.dialect)

                if this.is_type(exp.DataType.Type.MAP):
                    bracket = f"({bracket})[1]"

            return bracket

        def withingroup_sql(self, expression: exp.WithinGroup) -> str:
            expression_sql = self.sql(expression, "expression")

            func = expression.this
            if isinstance(func, exp.PERCENTILES):
                # Make the order key the first arg and slide the fraction to the right
                # https://duckdb.org/docs/sql/aggregates#ordered-set-aggregate-functions
                order_col = expression.find(exp.Ordered)
                if order_col:
                    func.set("expression", func.this)
                    func.set("this", order_col.this)

            this = self.sql(expression, "this").rstrip(")")

            return f"{this}{expression_sql})"

        def length_sql(self, expression: exp.Length) -> str:
            arg = expression.this

            # Dialects like BQ and Snowflake also accept binary values as args, so
            # DDB will attempt to infer the type or resort to case/when resolution
            if not expression.args.get("binary") or arg.is_string:
                return self.func("LENGTH", arg)

            if not arg.type:
                from sqlglot.optimizer.annotate_types import annotate_types

                arg = annotate_types(arg, dialect=self.dialect)

            if arg.is_type(*exp.DataType.TEXT_TYPES):
                return self.func("LENGTH", arg)

            # We need these casts to make duckdb's static type checker happy
            blob = exp.cast(arg, exp.DataType.Type.VARBINARY)
            varchar = exp.cast(arg, exp.DataType.Type.VARCHAR)

            case = (
                exp.case(self.func("TYPEOF", arg))
                .when("'BLOB'", self.func("OCTET_LENGTH", blob))
                .else_(
                    exp.Anonymous(this="LENGTH", expressions=[varchar])
                )  # anonymous to break length_sql recursion
            )

            return self.sql(case)

        def lower_sql(self, expression: exp.Lower) -> str:
            result_sql = self.func("LOWER", _cast_to_varchar(expression.this))
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def upper_sql(self, expression: exp.Upper) -> str:
            result_sql = self.func("UPPER", _cast_to_varchar(expression.this))
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def replace_sql(self, expression: exp.Replace) -> str:
            result_sql = self.func(
                "REPLACE",
                _cast_to_varchar(expression.this),
                _cast_to_varchar(expression.expression),
                _cast_to_varchar(expression.args.get("replacement")),
            )
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def _bitwise_op(self, expression: exp.Binary, op: str) -> str:
            _prepare_binary_bitwise_args(expression)
            result_sql = self.binary(expression, op)
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def bitwisexor_sql(self, expression: exp.BitwiseXor) -> str:
            _prepare_binary_bitwise_args(expression)
            result_sql = self.func("XOR", expression.this, expression.expression)
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def objectinsert_sql(self, expression: exp.ObjectInsert) -> str:
            this = expression.this
            key = expression.args.get("key")
            key_sql = key.name if isinstance(key, exp.Expression) else ""
            value_sql = self.sql(expression, "value")

            kv_sql = f"{key_sql} := {value_sql}"

            # If the input struct is empty e.g. transpiling OBJECT_INSERT(OBJECT_CONSTRUCT(), key, value) from Snowflake
            # then we can generate STRUCT_PACK which will build it since STRUCT_INSERT({}, key := value) is not valid DuckDB
            if isinstance(this, exp.Struct) and not this.expressions:
                return self.func("STRUCT_PACK", kv_sql)

            return self.func("STRUCT_INSERT", this, kv_sql)

        def startswith_sql(self, expression: exp.StartsWith) -> str:
            return self.func(
                "STARTS_WITH",
                _cast_to_varchar(expression.this),
                _cast_to_varchar(expression.expression),
            )

        def unnest_sql(self, expression: exp.Unnest) -> str:
            explode_array = expression.args.get("explode_array")
            if explode_array:
                # In BigQuery, UNNESTing a nested array leads to explosion of the top-level array & struct
                # This is transpiled to DDB by transforming "FROM UNNEST(...)" to "FROM (SELECT UNNEST(..., max_depth => 2))"
                expression.expressions.append(
                    exp.Kwarg(this=exp.var("max_depth"), expression=exp.Literal.number(2))
                )

                # If BQ's UNNEST is aliased, we transform it from a column alias to a table alias in DDB
                alias = expression.args.get("alias")
                if isinstance(alias, exp.TableAlias):
                    expression.set("alias", None)
                    if alias.columns:
                        alias = exp.TableAlias(this=seq_get(alias.columns, 0))

                unnest_sql = super().unnest_sql(expression)
                select = exp.Select(expressions=[unnest_sql]).subquery(alias)
                return self.sql(select)

            return super().unnest_sql(expression)

        def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
            this = expression.this

            if isinstance(this, self.IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS):
                # DuckDB should render IGNORE NULLS only for the general-purpose
                # window functions that accept it e.g. FIRST_VALUE(... IGNORE NULLS) OVER (...)
                return super().ignorenulls_sql(expression)

            if isinstance(this, exp.First):
                this = exp.AnyValue(this=this.this)

            if not isinstance(this, (exp.AnyValue, exp.ApproxQuantiles)):
                self.unsupported("IGNORE NULLS is not supported for non-window functions.")

            return self.sql(this)

        def respectnulls_sql(self, expression: exp.RespectNulls) -> str:
            if isinstance(expression.this, self.IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS):
                # DuckDB should render RESPECT NULLS only for the general-purpose
                # window functions that accept it e.g. FIRST_VALUE(... RESPECT NULLS) OVER (...)
                return super().respectnulls_sql(expression)

            self.unsupported("RESPECT NULLS is not supported for non-window functions.")
            return self.sql(expression, "this")

        def arraytostring_sql(self, expression: exp.ArrayToString) -> str:
            this = self.sql(expression, "this")
            null_text = self.sql(expression, "null")

            if null_text:
                this = f"LIST_TRANSFORM({this}, x -> COALESCE(x, {null_text}))"

            return self.func("ARRAY_TO_STRING", this, expression.expression)

        def regexpextract_sql(self, expression: exp.RegexpExtract) -> str:
            this = expression.this
            group = expression.args.get("group")
            params = expression.args.get("parameters")
            position = expression.args.get("position")
            occurrence = expression.args.get("occurrence")
            null_if_pos_overflow = expression.args.get("null_if_pos_overflow")

            if position and (not position.is_int or position.to_py() > 1):
                this = exp.Substring(this=this, start=position)

                if null_if_pos_overflow:
                    this = exp.Nullif(this=this, expression=exp.Literal.string(""))

            # Do not render group if there is no following argument,
            # and it's the default value for this dialect
            if (
                not params
                and group
                and group.name == str(self.dialect.REGEXP_EXTRACT_DEFAULT_GROUP)
            ):
                group = None

            if occurrence and (not occurrence.is_int or occurrence.to_py() > 1):
                return self.func(
                    "ARRAY_EXTRACT",
                    self.func("REGEXP_EXTRACT_ALL", this, expression.expression, group, params),
                    exp.Literal.number(occurrence),
                )

            return self.func("REGEXP_EXTRACT", this, expression.expression, group, params)

        @unsupported_args("culture")
        def numbertostr_sql(self, expression: exp.NumberToStr) -> str:
            fmt = expression.args.get("format")
            if fmt and fmt.is_int:
                return self.func("FORMAT", f"'{{:,.{fmt.name}f}}'", expression.this)

            self.unsupported("Only integer formats are supported by NumberToStr")
            return self.function_fallback_sql(expression)

        def autoincrementcolumnconstraint_sql(self, _) -> str:
            self.unsupported("The AUTOINCREMENT column constraint is not supported by DuckDB")
            return ""

        def aliases_sql(self, expression: exp.Aliases) -> str:
            this = expression.this
            if isinstance(this, exp.Posexplode):
                return self.posexplode_sql(this)

            return super().aliases_sql(expression)

        def posexplode_sql(self, expression: exp.Posexplode) -> str:
            this = expression.this
            parent = expression.parent

            # The default Spark aliases are "pos" and "col", unless specified otherwise
            pos, col = exp.to_identifier("pos"), exp.to_identifier("col")

            if isinstance(parent, exp.Aliases):
                # Column case: SELECT POSEXPLODE(col) [AS (a, b)]
                pos, col = parent.expressions
            elif isinstance(parent, exp.Table):
                # Table case: SELECT * FROM POSEXPLODE(col) [AS (a, b)]
                alias = parent.args.get("alias")
                if alias:
                    pos, col = alias.columns or [pos, col]
                    alias.pop()

            # Translate POSEXPLODE to UNNEST + GENERATE_SUBSCRIPTS
            # Note: In Spark pos is 0-indexed, but in DuckDB it's 1-indexed, so we subtract 1 from GENERATE_SUBSCRIPTS
            unnest_sql = self.sql(exp.Unnest(expressions=[this], alias=col))
            gen_subscripts = self.sql(
                exp.Alias(
                    this=exp.Anonymous(
                        this="GENERATE_SUBSCRIPTS", expressions=[this, exp.Literal.number(1)]
                    )
                    - exp.Literal.number(1),
                    alias=pos,
                )
            )

            posexplode_sql = self.format_args(gen_subscripts, unnest_sql)

            if isinstance(parent, exp.From) or (parent and isinstance(parent.parent, exp.From)):
                # SELECT * FROM POSEXPLODE(col) -> SELECT * FROM (SELECT GENERATE_SUBSCRIPTS(...), UNNEST(...))
                return self.sql(exp.Subquery(this=exp.Select(expressions=[posexplode_sql])))

            return posexplode_sql

        def addmonths_sql(self, expression: exp.AddMonths) -> str:
            """
            Handles three key issues:
            1. Float/decimal months: e.g., Snowflake rounds, whereas DuckDB INTERVAL requires integers
            2. End-of-month preservation: If input is last day of month, result is last day of result month
            3. Type preservation: Maintains DATE/TIMESTAMPTZ types (DuckDB defaults to TIMESTAMP)
            """
            from sqlglot.optimizer.annotate_types import annotate_types

            this = expression.this
            if not this.type:
                this = annotate_types(this, dialect=self.dialect)

            if this.is_type(*exp.DataType.TEXT_TYPES):
                this = exp.Cast(this=this, to=exp.DataType(this=exp.DataType.Type.TIMESTAMP))

            # Detect float/decimal months to apply rounding (Snowflake behavior)
            # DuckDB INTERVAL syntax doesn't support non-integer expressions, so use TO_MONTHS
            months_expr = expression.expression
            if not months_expr.type:
                months_expr = annotate_types(months_expr, dialect=self.dialect)

            # Build interval or to_months expression based on type
            # Float/decimal case: Round and use TO_MONTHS(CAST(ROUND(value) AS INT))
            interval_or_to_months = (
                exp.func("TO_MONTHS", exp.cast(exp.func("ROUND", months_expr), "INT"))
                if months_expr.is_type(
                    exp.DataType.Type.FLOAT,
                    exp.DataType.Type.DOUBLE,
                    exp.DataType.Type.DECIMAL,
                )
                # Integer case: standard INTERVAL N MONTH syntax
                else exp.Interval(this=months_expr, unit=exp.var("MONTH"))
            )

            date_add_expr = exp.Add(this=this, expression=interval_or_to_months)

            # Apply end-of-month preservation if Snowflake flag is set
            # CASE WHEN LAST_DAY(date) = date THEN LAST_DAY(result) ELSE result END
            preserve_eom = expression.args.get("preserve_end_of_month")
            result_expr = (
                exp.case()
                .when(
                    exp.EQ(this=exp.func("LAST_DAY", this), expression=this),
                    exp.func("LAST_DAY", date_add_expr),
                )
                .else_(date_add_expr)
                if preserve_eom
                else date_add_expr
            )

            # DuckDB's DATE_ADD function returns TIMESTAMP/DATETIME by default, even when the input is DATE
            # To match for example Snowflake's ADD_MONTHS behavior (which preserves the input type)
            # We need to cast the result back to the original type when the input is DATE or TIMESTAMPTZ
            # Example: ADD_MONTHS('2023-01-31'::date, 1) should return DATE, not TIMESTAMP
            if this.is_type(exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMPTZ):
                return self.sql(exp.Cast(this=result_expr, to=this.type))
            return self.sql(result_expr)

        def format_sql(self, expression: exp.Format) -> str:
            if expression.name.lower() == "%s" and len(expression.expressions) == 1:
                return self.func("FORMAT", "'{}'", expression.expressions[0])

            return self.function_fallback_sql(expression)

        def hexstring_sql(
            self, expression: exp.HexString, binary_function_repr: t.Optional[str] = None
        ) -> str:
            from_hex = super().hexstring_sql(expression, binary_function_repr="FROM_HEX")

            if expression.args.get("is_integer"):
                return from_hex

            # `from_hex` has transpiled x'ABCD' (BINARY) to DuckDB's '\xAB\xCD' (BINARY)
            # `to_hex` & CASTing transforms it to "ABCD" (BINARY) to match representation
            to_hex = exp.cast(self.func("TO_HEX", from_hex), exp.DataType.Type.BLOB)

            return self.sql(to_hex)

        def datetrunc_sql(self, expression: exp.DateTrunc) -> str:
            unit = unit_to_str(expression)
            date = expression.this
            result = self.func("DATE_TRUNC", unit, date)

            if expression.args.get("input_type_preserved"):
                if not date.type:
                    from sqlglot.optimizer.annotate_types import annotate_types

                    date = annotate_types(date, dialect=self.dialect)

                if date.type and date.is_type(*exp.DataType.TEMPORAL_TYPES):
                    return self.sql(exp.Cast(this=result, to=date.type))
            return result

        def timestamptrunc_sql(self, expression: exp.TimestampTrunc) -> str:
            unit = unit_to_str(expression)
            zone = expression.args.get("zone")
            timestamp = expression.this

            if is_date_unit(unit) and zone:
                # BigQuery's TIMESTAMP_TRUNC with timezone truncates in the target timezone and returns as UTC.
                # Double AT TIME ZONE needed for BigQuery compatibility:
                # 1. First AT TIME ZONE: ensures truncation happens in the target timezone
                # 2. Second AT TIME ZONE: converts the DATE result back to TIMESTAMPTZ (preserving time component)
                timestamp = exp.AtTimeZone(this=timestamp, zone=zone)
                result_sql = self.func("DATE_TRUNC", unit, timestamp)
                return self.sql(exp.AtTimeZone(this=result_sql, zone=zone))

            result = self.func("DATE_TRUNC", unit, timestamp)
            if expression.args.get("input_type_preserved"):
                if not timestamp.type:
                    from sqlglot.optimizer.annotate_types import annotate_types

                    timestamp = annotate_types(timestamp, dialect=self.dialect)

                if timestamp.type and timestamp.is_type(
                    exp.DataType.Type.TIME, exp.DataType.Type.TIMETZ
                ):
                    dummy_date = exp.Cast(
                        this=exp.Literal.string("1970-01-01"),
                        to=exp.DataType(this=exp.DataType.Type.DATE),
                    )
                    date_time = exp.Add(this=dummy_date, expression=timestamp)
                    result = self.func("DATE_TRUNC", unit, date_time)
                    return self.sql(exp.Cast(this=result, to=timestamp.type))

                if timestamp.type and timestamp.is_type(*exp.DataType.TEMPORAL_TYPES):
                    return self.sql(exp.Cast(this=result, to=timestamp.type))
            return result

        def trim_sql(self, expression: exp.Trim) -> str:
            expression.this.replace(_cast_to_varchar(expression.this))
            if expression.expression:
                expression.expression.replace(_cast_to_varchar(expression.expression))

            result_sql = super().trim_sql(expression)
            return _gen_with_cast_to_blob(self, expression, result_sql)

        def round_sql(self, expression: exp.Round) -> str:
            this = expression.this
            decimals = expression.args.get("decimals")
            truncate = expression.args.get("truncate")

            # DuckDB requires the scale (decimals) argument to be an INT
            # Some dialects (e.g., Snowflake) allow non-integer scales and cast to an integer internally
            if decimals is not None and expression.args.get("casts_non_integer_decimals"):
                if not (decimals.is_int or decimals.is_type(*exp.DataType.INTEGER_TYPES)):
                    decimals = exp.cast(decimals, exp.DataType.Type.INT)

            func = "ROUND"
            if truncate:
                # BigQuery uses ROUND_HALF_EVEN; Snowflake uses HALF_TO_EVEN
                if truncate.this in ("ROUND_HALF_EVEN", "HALF_TO_EVEN"):
                    func = "ROUND_EVEN"
                    truncate = None
                # BigQuery uses ROUND_HALF_AWAY_FROM_ZERO; Snowflake uses HALF_AWAY_FROM_ZERO
                elif truncate.this in ("ROUND_HALF_AWAY_FROM_ZERO", "HALF_AWAY_FROM_ZERO"):
                    truncate = None

            return self.func(func, this, decimals, truncate)

        def approxquantiles_sql(self, expression: exp.ApproxQuantiles) -> str:
            """
            BigQuery's APPROX_QUANTILES(expr, n) returns an array of n+1 approximate quantile values
            dividing the input distribution into n equal-sized buckets.

            Both BigQuery and DuckDB use approximate algorithms for quantile estimation, but BigQuery
            does not document the specific algorithm used so results may differ. DuckDB does not
            support RESPECT NULLS.
            """
            this = expression.this
            if isinstance(this, exp.Distinct):
                # APPROX_QUANTILES requires 2 args and DISTINCT node grabs both
                if len(this.expressions) < 2:
                    self.unsupported("APPROX_QUANTILES requires a bucket count argument")
                    return self.function_fallback_sql(expression)
                num_quantiles_expr = this.expressions[1].pop()
            else:
                num_quantiles_expr = expression.expression

            if not isinstance(num_quantiles_expr, exp.Literal) or not num_quantiles_expr.is_int:
                self.unsupported("APPROX_QUANTILES bucket count must be a positive integer")
                return self.function_fallback_sql(expression)

            num_quantiles = t.cast(int, num_quantiles_expr.to_py())
            if num_quantiles <= 0:
                self.unsupported("APPROX_QUANTILES bucket count must be a positive integer")
                return self.function_fallback_sql(expression)

            quantiles = [
                exp.Literal.number(Decimal(i) / Decimal(num_quantiles))
                for i in range(num_quantiles + 1)
            ]

            return self.sql(
                exp.ApproxQuantile(this=this, quantile=exp.Array(expressions=quantiles))
            )

        def jsonextractscalar_sql(self, expression: exp.JSONExtractScalar) -> str:
            if expression.args.get("scalar_only"):
                expression = exp.JSONExtractScalar(
                    this=rename_func("JSON_VALUE")(self, expression), expression="'$'"
                )
            return _arrow_json_extract_sql(self, expression)

        def bitwisenot_sql(self, expression: exp.BitwiseNot) -> str:
            this = expression.this

            if _is_binary(this):
                expression.type = exp.DataType.build("BINARY")

            arg = _cast_to_bit(this)

            if isinstance(this, exp.Neg):
                arg = exp.Paren(this=arg)

            expression.set("this", arg)

            result_sql = f"~{self.sql(expression, 'this')}"

            return _gen_with_cast_to_blob(self, expression, result_sql)

        def window_sql(self, expression: exp.Window) -> str:
            this = expression.this
            if isinstance(this, exp.Corr) or (
                isinstance(this, exp.Filter) and isinstance(this.this, exp.Corr)
            ):
                return self._corr_sql(expression)

            return super().window_sql(expression)

        def filter_sql(self, expression: exp.Filter) -> str:
            if isinstance(expression.this, exp.Corr):
                return self._corr_sql(expression)

            return super().filter_sql(expression)

        def _corr_sql(
            self,
            expression: t.Union[exp.Filter, exp.Window, exp.Corr],
        ) -> str:
            if isinstance(expression, exp.Corr) and not expression.args.get(
                "null_on_zero_variance"
            ):
                return self.func("CORR", expression.this, expression.expression)

            corr_expr = _maybe_corr_null_to_false(expression)
            if corr_expr is None:
                if isinstance(expression, exp.Window):
                    return super().window_sql(expression)
                if isinstance(expression, exp.Filter):
                    return super().filter_sql(expression)
                corr_expr = expression  # make mypy happy

            return self.sql(exp.case().when(exp.IsNan(this=corr_expr), exp.null()).else_(corr_expr))
