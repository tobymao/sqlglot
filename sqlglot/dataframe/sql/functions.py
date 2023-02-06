from __future__ import annotations

import typing as t

from sqlglot import expressions as glotexp
from sqlglot.dataframe.sql.column import Column
from sqlglot.helper import ensure_list
from sqlglot.helper import flatten as _flatten

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import ColumnOrLiteral, ColumnOrName
    from sqlglot.dataframe.sql.dataframe import DataFrame


def col(column_name: t.Union[ColumnOrName, t.Any]) -> Column:
    return Column(column_name)


def lit(value: t.Optional[t.Any] = None) -> Column:
    if isinstance(value, str):
        return Column(glotexp.Literal.string(str(value)))
    return Column(value)


def greatest(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(cols[0], glotexp.Greatest, expressions=cols[1:])
    return Column.invoke_expression_over_column(cols[0], glotexp.Greatest)


def least(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(cols[0], glotexp.Least, expressions=cols[1:])
    return Column.invoke_expression_over_column(cols[0], glotexp.Least)


def count_distinct(col: ColumnOrName, *cols: ColumnOrName) -> Column:
    columns = [Column.ensure_col(x) for x in [col] + list(cols)]
    return Column(glotexp.Count(this=glotexp.Distinct(expressions=[x.expression for x in columns])))


def countDistinct(col: ColumnOrName, *cols: ColumnOrName) -> Column:
    return count_distinct(col, *cols)


def when(condition: Column, value: t.Any) -> Column:
    true_value = value if isinstance(value, Column) else lit(value)
    return Column(
        glotexp.Case(
            ifs=[glotexp.If(this=condition.column_expression, true=true_value.column_expression)]
        )
    )


def asc(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc()


def desc(col: ColumnOrName):
    return Column.ensure_col(col).desc()


def broadcast(df: DataFrame) -> DataFrame:
    return df.hint("broadcast")


def sqrt(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Sqrt)


def abs(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Abs)


def max(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Max)


def min(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Min)


def max_by(col: ColumnOrName, ord: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAX_BY", ord)


def min_by(col: ColumnOrName, ord: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MIN_BY", ord)


def count(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Count)


def sum(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Sum)


def avg(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Avg)


def mean(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MEAN")


def sumDistinct(col: ColumnOrName) -> Column:
    return sum_distinct(col)


def sum_distinct(col: ColumnOrName) -> Column:
    raise NotImplementedError("Sum distinct is not currently implemented")


def product(col: ColumnOrName) -> Column:
    raise NotImplementedError("Product is not currently implemented")


def acos(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ACOS")


def acosh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ACOSH")


def asin(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ASIN")


def asinh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ASINH")


def atan(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ATAN")


def atan2(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    return Column.invoke_anonymous_function(col1, "ATAN2", col2)


def atanh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ATANH")


def cbrt(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "CBRT")


def ceil(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Ceil)


def cos(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COS")


def cosh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COSH")


def cot(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COT")


def csc(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "CSC")


def exp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Exp)


def expm1(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "EXPM1")


def floor(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Floor)


def log10(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Log10)


def log1p(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "LOG1P")


def log2(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Log2)


def log(arg1: t.Union[ColumnOrName, float], arg2: t.Optional[ColumnOrName] = None) -> Column:
    if arg2 is None:
        return Column.invoke_expression_over_column(arg1, glotexp.Ln)
    return Column.invoke_expression_over_column(arg1, glotexp.Log, expression=arg2)


def rint(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "RINT")


def sec(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SEC")


def signum(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SIGNUM")


def sin(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SIN")


def sinh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SINH")


def tan(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TAN")


def tanh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TANH")


def toDegrees(col: ColumnOrName) -> Column:
    return degrees(col)


def degrees(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "DEGREES")


def toRadians(col: ColumnOrName) -> Column:
    return radians(col)


def radians(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "RADIANS")


def bitwiseNOT(col: ColumnOrName) -> Column:
    return bitwise_not(col)


def bitwise_not(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.BitwiseNot)


def asc_nulls_first(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc_nulls_first()


def asc_nulls_last(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc_nulls_last()


def desc_nulls_first(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).desc_nulls_first()


def desc_nulls_last(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).desc_nulls_last()


def stddev(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Stddev)


def stddev_samp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.StddevSamp)


def stddev_pop(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.StddevPop)


def variance(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Variance)


def var_samp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Variance)


def var_pop(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.VariancePop)


def skewness(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SKEWNESS")


def kurtosis(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "KURTOSIS")


def collect_list(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.ArrayAgg)


def collect_set(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.SetAgg)


def hypot(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    return Column.invoke_anonymous_function(col1, "HYPOT", col2)


def pow(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    return Column.invoke_expression_over_column(col1, glotexp.Pow, expression=col2)


def row_number() -> Column:
    return Column(glotexp.Anonymous(this="ROW_NUMBER"))


def dense_rank() -> Column:
    return Column(glotexp.Anonymous(this="DENSE_RANK"))


def rank() -> Column:
    return Column(glotexp.Anonymous(this="RANK"))


def cume_dist() -> Column:
    return Column(glotexp.Anonymous(this="CUME_DIST"))


def percent_rank() -> Column:
    return Column(glotexp.Anonymous(this="PERCENT_RANK"))


def approxCountDistinct(col: ColumnOrName, rsd: t.Optional[float] = None) -> Column:
    return approx_count_distinct(col, rsd)


def approx_count_distinct(col: ColumnOrName, rsd: t.Optional[float] = None) -> Column:
    if rsd is None:
        return Column.invoke_expression_over_column(col, glotexp.ApproxDistinct)
    return Column.invoke_expression_over_column(col, glotexp.ApproxDistinct, accuracy=rsd)


def coalesce(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(cols[0], glotexp.Coalesce, expressions=cols[1:])
    return Column.invoke_expression_over_column(cols[0], glotexp.Coalesce)


def corr(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "CORR", col2)


def covar_pop(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "COVAR_POP", col2)


def covar_samp(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "COVAR_SAMP", col2)


def first(col: ColumnOrName, ignorenulls: t.Optional[bool] = None) -> Column:
    if ignorenulls is not None:
        return Column.invoke_anonymous_function(col, "FIRST", ignorenulls)
    return Column.invoke_anonymous_function(col, "FIRST")


def grouping_id(*cols: ColumnOrName) -> Column:
    if not cols:
        return Column.invoke_anonymous_function(None, "GROUPING_ID")
    if len(cols) == 1:
        return Column.invoke_anonymous_function(cols[0], "GROUPING_ID")
    return Column.invoke_anonymous_function(cols[0], "GROUPING_ID", *cols[1:])


def input_file_name() -> Column:
    return Column.invoke_anonymous_function(None, "INPUT_FILE_NAME")


def isnan(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ISNAN")


def isnull(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ISNULL")


def last(col: ColumnOrName, ignorenulls: t.Optional[bool] = None) -> Column:
    if ignorenulls is not None:
        return Column.invoke_anonymous_function(col, "LAST", ignorenulls)
    return Column.invoke_anonymous_function(col, "LAST")


def monotonically_increasing_id() -> Column:
    return Column.invoke_anonymous_function(None, "MONOTONICALLY_INCREASING_ID")


def nanvl(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "NANVL", col2)


def percentile_approx(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    accuracy: t.Optional[t.Union[ColumnOrLiteral, int]] = None,
) -> Column:
    if accuracy:
        return Column.invoke_expression_over_column(
            col, glotexp.ApproxQuantile, quantile=lit(percentage), accuracy=accuracy
        )
    return Column.invoke_expression_over_column(
        col, glotexp.ApproxQuantile, quantile=lit(percentage)
    )


def rand(seed: t.Optional[ColumnOrLiteral] = None) -> Column:
    return Column.invoke_anonymous_function(seed, "RAND")


def randn(seed: t.Optional[ColumnOrLiteral] = None) -> Column:
    return Column.invoke_anonymous_function(seed, "RANDN")


def round(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    if scale is not None:
        return Column.invoke_expression_over_column(col, glotexp.Round, decimals=scale)
    return Column.invoke_expression_over_column(col, glotexp.Round)


def bround(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    if scale is not None:
        return Column.invoke_anonymous_function(col, "BROUND", scale)
    return Column.invoke_anonymous_function(col, "BROUND")


def shiftleft(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.BitwiseLeftShift, expression=numBits)


def shiftLeft(col: ColumnOrName, numBits: int) -> Column:
    return shiftleft(col, numBits)


def shiftright(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.BitwiseRightShift, expression=numBits)


def shiftRight(col: ColumnOrName, numBits: int) -> Column:
    return shiftright(col, numBits)


def shiftrightunsigned(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_anonymous_function(col, "SHIFTRIGHTUNSIGNED", numBits)


def shiftRightUnsigned(col: ColumnOrName, numBits: int) -> Column:
    return shiftrightunsigned(col, numBits)


def expr(str: str) -> Column:
    return Column(str)


def struct(col: t.Union[ColumnOrName, t.Iterable[ColumnOrName]], *cols: ColumnOrName) -> Column:
    columns = ensure_list(col) + list(cols)
    return Column.invoke_expression_over_column(None, glotexp.Struct, expressions=columns)


def conv(col: ColumnOrName, fromBase: int, toBase: int) -> Column:
    return Column.invoke_anonymous_function(col, "CONV", fromBase, toBase)


def factorial(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "FACTORIAL")


def lag(
    col: ColumnOrName, offset: t.Optional[int] = 1, default: t.Optional[ColumnOrLiteral] = None
) -> Column:
    if default is not None:
        return Column.invoke_anonymous_function(col, "LAG", offset, default)
    if offset != 1:
        return Column.invoke_anonymous_function(col, "LAG", offset)
    return Column.invoke_anonymous_function(col, "LAG")


def lead(
    col: ColumnOrName, offset: t.Optional[int] = 1, default: t.Optional[t.Any] = None
) -> Column:
    if default is not None:
        return Column.invoke_anonymous_function(col, "LEAD", offset, default)
    if offset != 1:
        return Column.invoke_anonymous_function(col, "LEAD", offset)
    return Column.invoke_anonymous_function(col, "LEAD")


def nth_value(
    col: ColumnOrName, offset: t.Optional[int] = 1, ignoreNulls: t.Optional[bool] = None
) -> Column:
    if ignoreNulls is not None:
        raise NotImplementedError("There is currently not support for `ignoreNulls` parameter")
    if offset != 1:
        return Column.invoke_anonymous_function(col, "NTH_VALUE", offset)
    return Column.invoke_anonymous_function(col, "NTH_VALUE")


def ntile(n: int) -> Column:
    return Column.invoke_anonymous_function(None, "NTILE", n)


def current_date() -> Column:
    return Column.invoke_expression_over_column(None, glotexp.CurrentDate)


def current_timestamp() -> Column:
    return Column.invoke_expression_over_column(None, glotexp.CurrentTimestamp)


def date_format(col: ColumnOrName, format: str) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.TimeToStr, format=lit(format))


def year(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Year)


def quarter(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "QUARTER")


def month(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Month)


def dayofweek(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DayOfWeek)


def dayofmonth(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DayOfMonth)


def dayofyear(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DayOfYear)


def hour(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "HOUR")


def minute(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MINUTE")


def second(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SECOND")


def weekofyear(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.WeekOfYear)


def make_date(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(year, "MAKE_DATE", month, day)


def date_add(col: ColumnOrName, days: t.Union[ColumnOrName, int]) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DateAdd, expression=days)


def date_sub(col: ColumnOrName, days: t.Union[ColumnOrName, int]) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DateSub, expression=days)


def date_diff(end: ColumnOrName, start: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(end, glotexp.DateDiff, expression=start)


def add_months(start: ColumnOrName, months: t.Union[ColumnOrName, int]) -> Column:
    return Column.invoke_anonymous_function(start, "ADD_MONTHS", months)


def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    if roundOff is None:
        return Column.invoke_anonymous_function(date1, "MONTHS_BETWEEN", date2)
    return Column.invoke_anonymous_function(date1, "MONTHS_BETWEEN", date2, roundOff)


def to_date(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    if format is not None:
        return Column.invoke_expression_over_column(col, glotexp.TsOrDsToDate, format=lit(format))
    return Column.invoke_expression_over_column(col, glotexp.TsOrDsToDate)


def to_timestamp(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    if format is not None:
        return Column.invoke_anonymous_function(col, "TO_TIMESTAMP", lit(format))
    return Column.invoke_anonymous_function(col, "TO_TIMESTAMP")


def trunc(col: ColumnOrName, format: str) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.DateTrunc, unit=lit(format))


def date_trunc(format: str, timestamp: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(timestamp, glotexp.TimestampTrunc, unit=lit(format))


def next_day(col: ColumnOrName, dayOfWeek: str) -> Column:
    return Column.invoke_anonymous_function(col, "NEXT_DAY", lit(dayOfWeek))


def last_day(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "LAST_DAY")


def from_unixtime(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    if format is not None:
        return Column.invoke_expression_over_column(col, glotexp.UnixToStr, format=lit(format))
    return Column.invoke_expression_over_column(col, glotexp.UnixToStr)


def unix_timestamp(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    if format is not None:
        return Column.invoke_expression_over_column(
            timestamp, glotexp.StrToUnix, format=lit(format)
        )
    return Column.invoke_expression_over_column(timestamp, glotexp.StrToUnix)


def from_utc_timestamp(timestamp: ColumnOrName, tz: ColumnOrName) -> Column:
    tz_column = tz if isinstance(tz, Column) else lit(tz)
    return Column.invoke_anonymous_function(timestamp, "FROM_UTC_TIMESTAMP", tz_column)


def to_utc_timestamp(timestamp: ColumnOrName, tz: ColumnOrName) -> Column:
    tz_column = tz if isinstance(tz, Column) else lit(tz)
    return Column.invoke_anonymous_function(timestamp, "TO_UTC_TIMESTAMP", tz_column)


def timestamp_seconds(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TIMESTAMP_SECONDS")


def window(
    timeColumn: ColumnOrName,
    windowDuration: str,
    slideDuration: t.Optional[str] = None,
    startTime: t.Optional[str] = None,
) -> Column:
    if slideDuration is not None and startTime is not None:
        return Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration), lit(startTime)
        )
    if slideDuration is not None:
        return Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration)
        )
    if startTime is not None:
        return Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(windowDuration), lit(startTime)
        )
    return Column.invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration))


def session_window(timeColumn: ColumnOrName, gapDuration: ColumnOrName) -> Column:
    gap_duration_column = gapDuration if isinstance(gapDuration, Column) else lit(gapDuration)
    return Column.invoke_anonymous_function(timeColumn, "SESSION_WINDOW", gap_duration_column)


def crc32(col: ColumnOrName) -> Column:
    column = col if isinstance(col, Column) else lit(col)
    return Column.invoke_anonymous_function(column, "CRC32")


def md5(col: ColumnOrName) -> Column:
    column = col if isinstance(col, Column) else lit(col)
    return Column.invoke_anonymous_function(column, "MD5")


def sha1(col: ColumnOrName) -> Column:
    column = col if isinstance(col, Column) else lit(col)
    return Column.invoke_anonymous_function(column, "SHA1")


def sha2(col: ColumnOrName, numBits: int) -> Column:
    column = col if isinstance(col, Column) else lit(col)
    num_bits = lit(numBits)
    return Column.invoke_anonymous_function(column, "SHA2", num_bits)


def hash(*cols: ColumnOrName) -> Column:
    args = cols[1:] if len(cols) > 1 else []
    return Column.invoke_anonymous_function(cols[0], "HASH", *args)


def xxhash64(*cols: ColumnOrName) -> Column:
    args = cols[1:] if len(cols) > 1 else []
    return Column.invoke_anonymous_function(cols[0], "XXHASH64", *args)


def assert_true(col: ColumnOrName, errorMsg: t.Optional[ColumnOrName] = None) -> Column:
    if errorMsg is not None:
        error_msg_col = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
        return Column.invoke_anonymous_function(col, "ASSERT_TRUE", error_msg_col)
    return Column.invoke_anonymous_function(col, "ASSERT_TRUE")


def raise_error(errorMsg: ColumnOrName) -> Column:
    error_msg_col = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
    return Column.invoke_anonymous_function(error_msg_col, "RAISE_ERROR")


def upper(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Upper)


def lower(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Lower)


def ascii(col: ColumnOrLiteral) -> Column:
    return Column.invoke_anonymous_function(col, "ASCII")


def base64(col: ColumnOrLiteral) -> Column:
    return Column.invoke_anonymous_function(col, "BASE64")


def unbase64(col: ColumnOrLiteral) -> Column:
    return Column.invoke_anonymous_function(col, "UNBASE64")


def ltrim(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "LTRIM")


def rtrim(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "RTRIM")


def trim(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Trim)


def concat_ws(sep: str, *cols: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        None, glotexp.ConcatWs, expressions=[lit(sep)] + list(cols)
    )


def decode(col: ColumnOrName, charset: str) -> Column:
    return Column.invoke_expression_over_column(
        col, glotexp.Decode, charset=glotexp.Literal.string(charset)
    )


def encode(col: ColumnOrName, charset: str) -> Column:
    return Column.invoke_expression_over_column(
        col, glotexp.Encode, charset=glotexp.Literal.string(charset)
    )


def format_number(col: ColumnOrName, d: int) -> Column:
    return Column.invoke_anonymous_function(col, "FORMAT_NUMBER", lit(d))


def format_string(format: str, *cols: ColumnOrName) -> Column:
    format_col = lit(format)
    columns = [Column.ensure_col(x) for x in cols]
    return Column.invoke_anonymous_function(format_col, "FORMAT_STRING", *columns)


def instr(col: ColumnOrName, substr: str) -> Column:
    return Column.invoke_anonymous_function(col, "INSTR", lit(substr))


def overlay(
    src: ColumnOrName,
    replace: ColumnOrName,
    pos: t.Union[ColumnOrName, int],
    len: t.Optional[t.Union[ColumnOrName, int]] = None,
) -> Column:
    if len is not None:
        return Column.invoke_anonymous_function(src, "OVERLAY", replace, pos, len)
    return Column.invoke_anonymous_function(src, "OVERLAY", replace, pos)


def sentences(
    string: ColumnOrName,
    language: t.Optional[ColumnOrName] = None,
    country: t.Optional[ColumnOrName] = None,
) -> Column:
    if language is not None and country is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", language, country)
    if language is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", language)
    if country is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", lit("en"), country)
    return Column.invoke_anonymous_function(string, "SENTENCES")


def substring(str: ColumnOrName, pos: int, len: int) -> Column:
    return Column.ensure_col(str).substr(pos, len)


def substring_index(str: ColumnOrName, delim: str, count: int) -> Column:
    return Column.invoke_anonymous_function(str, "SUBSTRING_INDEX", lit(delim), lit(count))


def levenshtein(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(left, glotexp.Levenshtein, expression=right)


def locate(substr: str, str: ColumnOrName, pos: t.Optional[int] = None) -> Column:
    substr_col = lit(substr)
    if pos is not None:
        return Column.invoke_expression_over_column(
            str, glotexp.StrPosition, substr=substr_col, position=pos
        )
    return Column.invoke_expression_over_column(str, glotexp.StrPosition, substr=substr_col)


def lpad(col: ColumnOrName, len: int, pad: str) -> Column:
    return Column.invoke_anonymous_function(col, "LPAD", lit(len), lit(pad))


def rpad(col: ColumnOrName, len: int, pad: str) -> Column:
    return Column.invoke_anonymous_function(col, "RPAD", lit(len), lit(pad))


def repeat(col: ColumnOrName, n: int) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Repeat, times=lit(n))


def split(str: ColumnOrName, pattern: str, limit: t.Optional[int] = None) -> Column:
    if limit is not None:
        return Column.invoke_expression_over_column(
            str, glotexp.RegexpSplit, expression=lit(pattern).expression, limit=limit
        )
    return Column.invoke_expression_over_column(str, glotexp.RegexpSplit, expression=lit(pattern))


def regexp_extract(str: ColumnOrName, pattern: str, idx: t.Optional[int] = None) -> Column:
    if idx is not None:
        return Column.invoke_anonymous_function(str, "REGEXP_EXTRACT", lit(pattern), idx)
    return Column.invoke_anonymous_function(str, "REGEXP_EXTRACT", lit(pattern))


def regexp_replace(str: ColumnOrName, pattern: str, replacement: str) -> Column:
    return Column.invoke_anonymous_function(str, "REGEXP_REPLACE", lit(pattern), lit(replacement))


def initcap(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Initcap)


def soundex(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SOUNDEX")


def bin(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIN")


def hex(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Hex)


def unhex(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Unhex)


def length(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Length)


def octet_length(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "OCTET_LENGTH")


def bit_length(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_LENGTH")


def translate(srcCol: ColumnOrName, matching: str, replace: str) -> Column:
    return Column.invoke_anonymous_function(srcCol, "TRANSLATE", lit(matching), lit(replace))


def array(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    columns = _flatten(cols) if not isinstance(cols[0], (str, Column)) else cols
    return Column.invoke_expression_over_column(None, glotexp.Array, expressions=columns)


def create_map(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    cols = list(_flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    return Column.invoke_expression_over_column(
        None,
        glotexp.VarMap,
        keys=array(*cols[::2]).expression,
        values=array(*cols[1::2]).expression,
    )


def map_from_arrays(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(None, glotexp.Map, keys=col1, values=col2)


def array_contains(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_expression_over_column(
        col, glotexp.ArrayContains, expression=value_col.expression
    )


def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAYS_OVERLAP", Column.ensure_col(col2))


def slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)
    return Column.invoke_anonymous_function(x, "SLICE", start_col, length_col)


def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: t.Optional[str] = None
) -> Column:
    if null_replacement is not None:
        return Column.invoke_anonymous_function(
            col, "ARRAY_JOIN", lit(delimiter), lit(null_replacement)
        )
    return Column.invoke_anonymous_function(col, "ARRAY_JOIN", lit(delimiter))


def concat(*cols: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(None, glotexp.Concat, expressions=cols)


def array_position(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_POSITION", value_col)


def element_at(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ELEMENT_AT", value_col)


def array_remove(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_REMOVE", value_col)


def array_distinct(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_DISTINCT")


def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_INTERSECT", Column.ensure_col(col2))


def array_union(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_UNION", Column.ensure_col(col2))


def array_except(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_EXCEPT", Column.ensure_col(col2))


def explode(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Explode)


def posexplode(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.Posexplode)


def explode_outer(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "EXPLODE_OUTER")


def posexplode_outer(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "POSEXPLODE_OUTER")


def get_json_object(col: ColumnOrName, path: str) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.JSONExtract, path=lit(path))


def json_tuple(col: ColumnOrName, *fields: str) -> Column:
    return Column.invoke_anonymous_function(col, "JSON_TUPLE", *[lit(field) for field in fields])


def from_json(
    col: ColumnOrName,
    schema: t.Union[Column, str],
    options: t.Optional[t.Dict[str, str]] = None,
) -> Column:
    schema = schema if isinstance(schema, Column) else lit(schema)
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "FROM_JSON", schema, options_col)
    return Column.invoke_anonymous_function(col, "FROM_JSON", schema)


def to_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "TO_JSON", options_col)
    return Column.invoke_anonymous_function(col, "TO_JSON")


def schema_of_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON")


def schema_of_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV")


def to_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "TO_CSV", options_col)
    return Column.invoke_anonymous_function(col, "TO_CSV")


def size(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, glotexp.ArraySize)


def array_min(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_MIN")


def array_max(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_MAX")


def sort_array(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    if asc is not None:
        return Column.invoke_expression_over_column(col, glotexp.SortArray, asc=asc)
    return Column.invoke_expression_over_column(col, glotexp.SortArray)


def array_sort(
    col: ColumnOrName,
    comparator: t.Optional[t.Union[t.Callable[[Column, Column], Column]]] = None,
) -> Column:
    if comparator is not None:
        f_expression = _get_lambda_from_func(comparator)
        return Column.invoke_expression_over_column(col, glotexp.ArraySort, expression=f_expression)
    return Column.invoke_expression_over_column(col, glotexp.ArraySort)


def shuffle(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SHUFFLE")


def reverse(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "REVERSE")


def flatten(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "FLATTEN")


def map_keys(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_KEYS")


def map_values(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_VALUES")


def map_entries(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_ENTRIES")


def map_from_entries(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_FROM_ENTRIES")


def array_repeat(col: ColumnOrName, count: t.Union[ColumnOrName, int]) -> Column:
    count_col = count if isinstance(count, Column) else lit(count)
    return Column.invoke_anonymous_function(col, "ARRAY_REPEAT", count_col)


def array_zip(*cols: ColumnOrName) -> Column:
    if len(cols) == 1:
        return Column.invoke_anonymous_function(cols[0], "ARRAY_ZIP")
    return Column.invoke_anonymous_function(cols[0], "ARRAY_ZIP", *cols[1:])


def map_concat(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    columns = list(flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    if len(columns) == 1:
        return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT")
    return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT", *columns[1:])


def sequence(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    if step is not None:
        return Column.invoke_anonymous_function(start, "SEQUENCE", stop, step)
    return Column.invoke_anonymous_function(start, "SEQUENCE", stop)


def from_csv(
    col: ColumnOrName,
    schema: t.Union[Column, str],
    options: t.Optional[t.Dict[str, str]] = None,
) -> Column:
    schema = schema if isinstance(schema, Column) else lit(schema)
    if options is not None:
        option_cols = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "FROM_CSV", schema, option_cols)
    return Column.invoke_anonymous_function(col, "FROM_CSV", schema)


def aggregate(
    col: ColumnOrName,
    initialValue: ColumnOrName,
    merge: t.Callable[[Column, Column], Column],
    finish: t.Optional[t.Callable[[Column], Column]] = None,
) -> Column:
    merge_exp = _get_lambda_from_func(merge)
    if finish is not None:
        finish_exp = _get_lambda_from_func(finish)
        return Column.invoke_expression_over_column(
            col,
            glotexp.Reduce,
            initial=initialValue,
            merge=Column(merge_exp),
            finish=Column(finish_exp),
        )
    return Column.invoke_expression_over_column(
        col, glotexp.Reduce, initial=initialValue, merge=Column(merge_exp)
    )


def transform(
    col: ColumnOrName,
    f: t.Union[t.Callable[[Column], Column], t.Callable[[Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "TRANSFORM", Column(f_expression))


def exists(col: ColumnOrName, f: t.Callable[[Column], Column]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "EXISTS", Column(f_expression))


def forall(col: ColumnOrName, f: t.Callable[[Column], Column]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "FORALL", Column(f_expression))


def filter(
    col: ColumnOrName,
    f: t.Union[t.Callable[[Column], Column], t.Callable[[Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_expression_over_column(col, glotexp.ArrayFilter, expression=f_expression)


def zip_with(
    left: ColumnOrName, right: ColumnOrName, f: t.Callable[[Column, Column], Column]
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(left, "ZIP_WITH", right, Column(f_expression))


def transform_keys(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "TRANSFORM_KEYS", Column(f_expression))


def transform_values(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "TRANSFORM_VALUES", Column(f_expression))


def map_filter(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "MAP_FILTER", Column(f_expression))


def map_zip_with(
    col1: ColumnOrName,
    col2: ColumnOrName,
    f: t.Union[t.Callable[[Column, Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col1, "MAP_ZIP_WITH", col2, Column(f_expression))


def _lambda_quoted(value: str) -> t.Optional[bool]:
    return False if value == "_" else None


def _get_lambda_from_func(lambda_expression: t.Callable):
    variables = [
        glotexp.to_identifier(x, quoted=_lambda_quoted(x))
        for x in lambda_expression.__code__.co_varnames
    ]
    return glotexp.Lambda(
        this=lambda_expression(*[Column(x) for x in variables]).expression,
        expressions=variables,
    )
