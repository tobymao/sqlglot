import typing as t

from sqlglot import expressions as glotexp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe.util import ensure_strings, ensure_sqlglot_column

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame

from pyspark.sql import functions

ColumnOrName = t.TypeVar("ColumnOrName", bound=t.Union["Column", str])
ColumnOrPrimitive = t.TypeVar("ColumnOrPrimitive", bound=t.Union["Column", str, float, int, bool])


def ensure_col(value: t.Union["ColumnOrName", int, float]):
    return Column(value)


def _invoke_column_function(column: "ColumnOrName", func_name: str, *args, **kwargs) -> "Column":
    column = ensure_col(column)
    func = getattr(column, func_name)
    return func(*args, **kwargs)


def _invoke_expression_over_column(column: t.Union["ColumnOrName", None], callable_expression: t.Callable, **kwargs) -> "Column":
    column = ensure_col(column) if column is not None else None
    new_expression = (
        callable_expression(this=column.column_expression, **kwargs)
        if column is not None
        else callable_expression(**kwargs)
    )
    return Column(new_expression)


def _invoke_anonymous_function(column: t.Optional["ColumnOrName"], func_name: str, *args) -> "Column":
    column = [ensure_col(column)] if column is not None else []
    args = [ensure_col(arg) for arg in args]
    expressions = [x.expression for x in column + args]
    new_expression = glotexp.Anonymous(this=func_name.upper(), expressions=expressions)
    return Column(new_expression)


def col(column_name: t.Union[ColumnOrName, t.Any]) -> "Column":
    return Column(column_name)


def lit(value: t.Optional[t.Any] = None) -> "Column":
    if value is None:
        return Column(glotexp.Null())
    return Column(glotexp.Literal(this=str(value), is_string=isinstance(value, str)))


def greatest(*cols: "ColumnOrName") -> "Column":
    cols = [ensure_col(col) for col in cols]
    return _invoke_expression_over_column(cols[0], glotexp.Greatest,
                                          expressions=[col.expression for col in cols[1:]] if len(cols) > 1 else None)


def least(*cols: "ColumnOrName") -> "Column":
    cols = [ensure_col(col) for col in cols]
    return _invoke_expression_over_column(cols[0], glotexp.Least,
                                          expressions=[col.expression for col in cols[1:]] if len(cols) > 1 else None)

def count_distinct(col: "ColumnOrName", *cols: "ColumnOrName") -> "Column":
    cols = [ensure_col(x) for x in [col] + list(cols)]
    if len(cols) > 1:
        raise NotImplementedError("Multiple columns in a count distinct is not supported")
    return Column(glotexp.Count(this=glotexp.Distinct(this=cols[0].expression)))


def countDistinct(col: "Column", *cols: "ColumnOrName") -> "Column":
    return count_distinct(col, *cols)


def when(condition: "Column", value: t.Any) -> "Column":
    true_value = value if isinstance(value, Column) else lit(value)
    return Column(glotexp.Case(ifs=[glotexp.If(this=condition.column_expression, true=true_value.column_expression)]))


def asc(col: "ColumnOrName") -> "Column":
    return _invoke_column_function(col, "asc")


def desc(col: "ColumnOrName"):
    return _invoke_column_function(col, "desc")


def broadcast(df: "DataFrame") -> "DataFrame":
    return df.hint("broadcast")


def sqrt(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Sqrt)


def abs(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Abs)


def max(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Max)


def min(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Min)


def max_by(col: "ColumnOrName", ord: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "MAX_BY", ord)


def min_by(col: "ColumnOrName", ord: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "MIN_BY", ord)


def count(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Count)


def sum(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Sum)


def avg(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Avg)


def mean(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "MEAN")


def sumDistinct(col: "ColumnOrName") -> "Column":
    return sum_distinct(col)


def sum_distinct(col: "ColumnOrName") -> "Column":
    raise NotImplementedError("Sum distinct is not currently implemented")


def product(col: "ColumnOrName") -> "Column":
    raise NotImplementedError("Product is not currently implemented")


def acos(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ACOS")


def acosh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ACOSH")


def asin(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ASIN")


def asinh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ASINH")


def atan(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ATAN")


def atan2(col1: t.Union["ColumnOrName", float], col2: t.Union["ColumnOrName", float]) -> "Column":
    return _invoke_anonymous_function(col1, "ATAN2", col2)


def atanh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ATANH")


def cbrt(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "CBRT")


def ceil(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Ceil)

def cos(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "COS")


def cosh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "COSH")


def cot(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "COT")


def csc(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "CSC")


def exp(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "EXP")


def expm1(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "EXPM1")


def floor(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Floor)


def log10(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Log10)


def log1p(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "LOG1P")


def log2(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Log2)


def log(arg1: t.Union["ColumnOrName", float], arg2: t.Optional["ColumnOrName"] = None) -> "Column":
    if arg2 is None:
        return _invoke_expression_over_column(arg1, glotexp.Ln)
    return _invoke_expression_over_column(arg1, glotexp.Log, expression=ensure_col(arg2).expression)


def rint(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "RINT")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def signum(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SIGNUM")


def sin(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SIN")


def sinh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SINH")


def tan(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "TAN")


def tanh(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "TANH")


def toDegrees(col: "ColumnOrName") -> "Column":
    return degrees(col)


def degrees(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "DEGREES")


def toRadians(col: "ColumnOrName") -> "Column":
    return radians(col)


def radians(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "RADIANS")


def bitwiseNOT(col: "ColumnOrName") -> "Column":
    return bitwise_not(col)


def bitwise_not(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.BitwiseNot)


def asc_nulls_first(col: "ColumnOrName") -> "Column":
    return _invoke_column_function(col, "asc_nulls_first")


def asc_nulls_last(col: "ColumnOrName") -> "Column":
    return _invoke_column_function(col, "asc_nulls_last")


def desc_nulls_first(col: "ColumnOrName") -> "Column":
    return _invoke_column_function(col, "desc_nulls_first")


def desc_nulls_last(col: "ColumnOrName") -> "Column":
    return _invoke_column_function(col, "desc_nulls_last")


def stddev(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Stddev)


def stddev_samp(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.StddevSamp)


def stddev_pop(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.StddevPop)


def variance(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Variance)


def var_samp(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.VarianceSamp)


def var_pop(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.VariancePop)


def skewness(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SKEWNESS")


def kurtosis(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "KURTOSIS")


def collect_list(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "COLLECT_LIST")


def collect_set(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "COLLECT_SET")


def hypot(col1: t.Union["ColumnOrName", float], col2: t.Union["ColumnOrName", float]) -> "Column":
    return _invoke_anonymous_function(col1, "HYPOT", col2)


def pow(col1: t.Union["ColumnOrName", float], col2: t.Union["ColumnOrName", float]) -> "Column":
    return _invoke_anonymous_function(col1, "POW", col2)


def row_number() -> "Column":
    return Column(glotexp.Anonymous(this="ROW_NUMBER"))


def dense_rank() -> "Column":
    return Column(glotexp.Anonymous(this="DENSE_RANK"))


def rank() -> "Column":
    return Column(glotexp.Anonymous(this="RANK"))


def cume_dist() -> "Column":
    return Column(glotexp.Anonymous(this="CUME_DIST"))


def percent_rank() -> "Column":
    return Column(glotexp.Anonymous(this="PERCENT_RANK"))


def approxCountDistinct(col: "ColumnOrName", rsd: t.Optional[float] = None) -> "Column":
    return approx_count_distinct(col, rsd)


def approx_count_distinct(col: "ColumnOrName", rsd: t.Optional[float] = None) -> "Column":
    return _invoke_expression_over_column(col, glotexp.ApproxDistinct, accuracy=ensure_col(rsd).expression)


def coalesce(*cols: "ColumnOrName") -> "Column":
    cols = [ensure_col(col) for col in cols]
    return _invoke_expression_over_column(cols[0], glotexp.Coalesce, expressions=[col.expression for col in cols[1:]] if len(cols) > 1 else None)


def corr(col1: "ColumnOrName", col2: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col1, "CORR", col2)


def covar_pop(col1: "ColumnOrName", col2: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col1, "COVAR_POP", col2)


def covar_samp(col1: "ColumnOrName", col2: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col1, "COVAR_SAMP", col2)


def first(col: "ColumnOrName", ignorenulls: bool = None) -> "Column":
    if ignorenulls is not None:
        return _invoke_anonymous_function(col, "FIRST", ignorenulls)
    return _invoke_anonymous_function(col, "FIRST")


def grouping_id(*cols: "ColumnOrName") -> "Column":
    if len(cols) == 0:
        return _invoke_anonymous_function(None, "GROUPING_ID")
    if len(cols) == 1:
        return _invoke_anonymous_function(cols[0], "GROUPING_ID")
    return _invoke_anonymous_function(cols[0], "GROUPING_ID", *cols[1:])


def input_file_name() -> "Column":
    return _invoke_anonymous_function(None, "INPUT_FILE_NAME")


def isnan(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ISNAN")


def isnull(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "ISNULL")


def last(col: "ColumnOrName", ignorenulls: bool = None) -> "Column":
    if ignorenulls is not None:
        return _invoke_anonymous_function(col, "LAST", ignorenulls)
    return _invoke_anonymous_function(col, "LAST")


def monotonically_increasing_id() -> "Column":
    return _invoke_anonymous_function(None, "MONOTONICALLY_INCREASING_ID")


def nanvl(col1: "ColumnOrName", col2: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col1, "NANVL", col2)


def percentile_approx(
    col: "ColumnOrName",
    percentage: t.Union["ColumnOrName", float, t.List[float], t.Tuple[float]],
    accuracy: t.Union["ColumnOrName", float] = None,
) -> "Column":
    if accuracy:
        return _invoke_anonymous_function(col, "PERCENTILE_APPROX", percentage, accuracy)
    return _invoke_anonymous_function(col, "PERCENTILE_APPROX", percentage)


def rand(seed: "ColumnOrPrimitive" = None) -> "Column":
    return _invoke_anonymous_function(seed, "RAND")


def randn(seed: "ColumnOrPrimitive" = None) -> "Column":
    return _invoke_anonymous_function(seed, "RANDN")


def round(col: "ColumnOrName", scale: int = None) -> "Column":
    if scale is not None:
        return _invoke_anonymous_function(col, "ROUND", scale)
    return _invoke_anonymous_function(col, "ROUND")


def bround(col: "ColumnOrName", scale: int = None) -> "Column":
    if scale is not None:
        return _invoke_anonymous_function(col, "BROUND", scale)
    return _invoke_anonymous_function(col, "BROUND")


def shiftleft(col: "ColumnOrName", numBits: int) -> "Column":
    return _invoke_expression_over_column(
        col,
        glotexp.BitwiseLeftShift,
        expression=ensure_col(numBits).expression
    )


def shiftLeft(col: "ColumnOrName", numBits: int) -> "Column":
    return shiftleft(col, numBits)


def shiftright(col: "ColumnOrName", numBits: int) -> "Column":
    return _invoke_expression_over_column(
        col,
        glotexp.BitwiseRightShift,
        expression=ensure_col(numBits).expression
    )


def shiftRight(col: "ColumnOrName", numBits: int) -> "Column":
    return shiftright(col, numBits)


def shiftrightunsigned(col: "ColumnOrName", numBits: int) -> "Column":
    return _invoke_anonymous_function(
        col,
        "SHIFTRIGHTUNSIGNED",
        numBits
    )


def shiftRightUnsigned(col: "ColumnOrName", numBits: int) -> "Column":
    return shiftrightunsigned(col, numBits)


def expr(str: str) -> "Column":
    return Column(str)


def struct(col: t.Union["ColumnOrName", t.Iterable["ColumnOrName"]], *cols: "ColumnOrName") -> "Column":
    col = [col] if isinstance(col, (str, Column)) else col
    columns = col + list(cols)
    expressions = [ensure_col(column).expression for column in columns]
    return Column(glotexp.Struct(expressions=expressions))


def conv(col: "ColumnOrName", fromBase: int, toBase: int) -> "Column":
    return _invoke_anonymous_function(col, "CONV", fromBase, toBase)


def factorial(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "FACTORIAL")


def lag(col: "ColumnOrName", offset: t.Optional[int] = 1, default: t.Optional[t.Any] = None) -> "Column":
    if default is not None:
        return _invoke_anonymous_function(col, "LAG", offset, default)
    if offset != 1:
        return _invoke_anonymous_function(col, "LAG", offset)
    return _invoke_anonymous_function(col, "LAG")


def lead(col: "ColumnOrName", offset: t.Optional[int] = 1, default: t.Optional[t.Any] = None) -> "Column":
    if default is not None:
        return _invoke_anonymous_function(col, "LEAD", offset, default)
    if offset != 1:
        return _invoke_anonymous_function(col, "LEAD", offset)
    return _invoke_anonymous_function(col, "LEAD")


def nth_value(col: "ColumnOrName", offset: t.Optional[int] = 1, ignoreNulls: t.Optional[bool] = None) -> "Column":
    if ignoreNulls is not None:
        raise NotImplementedError("There is currently not support for `ignoreNulls` parameter")
    if offset != 1:
        return _invoke_anonymous_function(col, "NTH_VALUE", offset)
    return _invoke_anonymous_function(col, "NTH_VALUE")


def ntile(n: int) -> "Column":
    return _invoke_anonymous_function(None, "NTILE", n)


def current_date() -> "Column":
    return _invoke_expression_over_column(None, glotexp.CurrentDate)


def current_timestamp() -> "Column":
    return _invoke_expression_over_column(None, glotexp.CurrentTimestamp)


def date_format(col: "ColumnOrName", format: str) -> "Column":
    return _invoke_anonymous_function(col, "DATE_FORMAT", lit(format))


def year(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Year)


def quarter(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "QUARTER")


def month(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Month)


def dayofweek(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "DAYOFWEEK")


def dayofmonth(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "DAYOFMONTH")


def dayofyear(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "DAYOFYEAR")


def hour(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "HOUR")


def minute(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "MINUTE")


def second(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SECOND")


def weekofyear(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "WEEKOFYEAR")


def make_date(year: "ColumnOrName", month: "ColumnOrName", day: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(year, "MAKE_DATE", month, day)


def date_add(col: "ColumnOrName", days: t.Union["ColumnOrName", int]) -> "Column":
    return _invoke_expression_over_column(col, glotexp.DateAdd, expression=ensure_col(days).expression)


def date_sub(col: "ColumnOrName", days: t.Union["ColumnOrName", int]) -> "Column":
    return _invoke_expression_over_column(col, glotexp.DateSub, expression=ensure_col(days).expression)


def date_diff(end: "ColumnOrName", start: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(end, glotexp.DateDiff, expression=ensure_col(start).expression)


def add_months(start: "ColumnOrName", months: t.Union["ColumnOrName", int]) -> "Column":
    return _invoke_anonymous_function(col, "ADD_MONTHS", months)


def months_between(date1: "ColumnOrName", date2: "ColumnOrName", roundOff: t.Optional[bool] = None) -> "Column":
    if roundOff is None:
        return _invoke_anonymous_function(date1, "MONTHS_BETWEEN", date2)
    return _invoke_anonymous_function(date1, "MONTHS_BETWEEN", date2, roundOff)


def to_date(col: "ColumnOrName", format: t.Optional[str] = None) -> "Column":
    if format is not None:
        return _invoke_anonymous_function(col, "TO_DATE", lit(format))
    return _invoke_anonymous_function(col, "TO_DATE")


def to_timestamp(col: "ColumnOrName", format: t.Optional[str] = None) -> "Column":
    if format is not None:
        return _invoke_anonymous_function(col, "TO_TIMESTAMP", lit(format))
    return _invoke_anonymous_function(col, "TO_TIMESTAMP")


def trunc(col: "ColumnOrName", format: str) -> "Column":
    return _invoke_expression_over_column(col, glotexp.DateTrunc, unit=lit(format).expression)


def date_trunc(format: str, timestamp: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(timestamp, glotexp.TimestampTrunc, unit=lit(format).expression)


def next_day(col: "ColumnOrName", dayOfWeek: str) -> "Column":
    return _invoke_anonymous_function(col, "NEXT_DAY", lit(dayOfWeek))


def last_day(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "LAST_DAY")


def from_unixtime(col: "ColumnOrName", format: str = None) -> "Column":
    if format is not None:
        return _invoke_anonymous_function(col, "FROM_UNIXTIME", lit(format))
    return _invoke_anonymous_function(col, "FROM_UNIXTIME")


def unix_timestamp(timestamp: t.Optional["ColumnOrName"] = None, format: str = None) -> "Column":
    if format is not None:
        return _invoke_anonymous_function(timestamp, "UNIX_TIMESTAMP", lit(format))
    return _invoke_anonymous_function(timestamp, "UNIX_TIMESTAMP")


def from_utc_timestamp(timestamp: "ColumnOrName", tz: "ColumnOrName") -> "Column":
    tz = tz if isinstance(tz, Column) else lit(tz)
    return _invoke_anonymous_function(timestamp, "FROM_UTC_TIMESTAMP", tz)


def to_utc_timestamp(timestamp: "ColumnOrName", tz: "ColumnOrName") -> "Column":
    tz = tz if isinstance(tz, Column) else lit(tz)
    return _invoke_anonymous_function(timestamp, "TO_UTC_TIMESTAMP", tz)


def timestamp_seconds(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "TIMESTAMP_SECONDS")


def window(timeColumn: "ColumnOrName", windowDuration: str,
           slideDuration: t.Optional[str] = None, startTime: t.Optional[str] = None) -> "Column":
    if slideDuration is not None and startTime is not None:
        return _invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration), lit(startTime))
    if slideDuration is not None:
        return _invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration))
    if startTime is not None:
        return _invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration), lit(windowDuration), lit(startTime))
    return _invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration))


def session_window(timeColumn: "ColumnOrName", gapDuration: "ColumnOrName") -> "Column":
    gapDuration = gapDuration if isinstance(gapDuration, Column) else lit(gapDuration)
    return _invoke_anonymous_function(timeColumn, "SESSION_WINDOW", gapDuration)


def crc32(col: "ColumnOrName") -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "CRC32")


def md5(col: "ColumnOrName") -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "MD5")


def sha1(col: "ColumnOrName") -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA1")


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def hash(*cols: "ColumnOrName") -> "Column":
    args = cols[1:] if len(cols) > 1 else []
    return _invoke_anonymous_function(cols[0], "HASH", *args)


def xxhash64(*cols: "ColumnOrName") -> "Column":
    args = cols[1:] if len(cols) > 1 else []
    return _invoke_anonymous_function(cols[0], "XXHASH64", *args)


def assert_true(col: "ColumnOrName", errorMsg: t.Optional["ColumnOrName"] = None) -> "Column":
    if errorMsg is not None:
        errorMsg = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
        return _invoke_anonymous_function(col, "ASSERT_TRUE", errorMsg)
    return _invoke_anonymous_function(col, "ASSERT_TRUE")


def raise_error(errorMsg: "ColumnOrName") -> "Column":
    errorMsg = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
    return _invoke_anonymous_function(errorMsg, "RAISE_ERROR")


def upper(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Upper)


def lower(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Lower)


def ascii(col: "ColumnOrPrimitive") -> "Column":
    return _invoke_anonymous_function(col, "ASCII")


def base64(col: "ColumnOrPrimitive") -> "Column":
    return _invoke_anonymous_function(col, "BASE64")


def unbase64(col: "ColumnOrPrimitive") -> "Column":
    return _invoke_anonymous_function(col, "UNBASE64")


def ltrim(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "LTRIM")


def rtrim(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "RTRIM")


def trim(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "TRIM")


def concat_ws(sep: str, *cols: "ColumnOrName") -> "Column":
    cols = [Column(col) for col in cols]
    return _invoke_expression_over_column(None, glotexp.ConcatWs, expressions=[x.expression for x in [lit(sep)] + list(cols)])


def decode(col: "ColumnOrName", charset: str) -> "Column":
    return _invoke_anonymous_function(col, "DECODE", lit(charset))


def encode(col: "ColumnOrName", charset: str) -> "Column":
    return _invoke_anonymous_function(col, "ENCODE", lit(charset))


def format_number(col: "ColumnOrName", d: int) -> "Column":
    return _invoke_anonymous_function(col, "FORMAT_NUMBER", lit(d))


def format_string(format: str, *cols: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(lit(format), "FORMAT_STRING", *cols)


def instr(col: "ColumnOrName", substr: str) -> "Column":
    return _invoke_anonymous_function(col, "SHA2", lit(substr))


def overlay(src: "ColumnOrName",
            replace: "ColumnOrName",
            pos: t.Union["ColumnOrName", int],
            len: t.Union["ColumnOrName", int] = None) -> "Column":
    if len is not None:
        return _invoke_anonymous_function(src, "OVERLAY", replace, pos, len)
    return _invoke_anonymous_function(src, "OVERLAY", replace, pos)


def sentences(string: "ColumnOrName",
              language: t.Optional["ColumnOrName"] = None,
              country: t.Optional["ColumnOrName"] = None) -> "Column":
    if language is not None and country is not None:
        return _invoke_anonymous_function(string, "SENTENCES", language, country)
    if language is not None:
        return _invoke_anonymous_function(string, "SENTENCES", language)
    if country is not None:
        return _invoke_anonymous_function(string, "SENTENCES", lit("en"), country)
    return _invoke_anonymous_function(string, "SENTENCES")


def substring(str: "ColumnOrName", pos: int, len: int) -> "Column":
    return _invoke_expression_over_column(str, glotexp.Substring, start=lit(pos).expression, length=lit(len).expression)


def substring_index(str: "ColumnOrName", delim: str, count: int) -> "Column":
    return _invoke_anonymous_function(str, "SUBSTRING_INDEX", lit(delim), lit(count))


def levenshtein(left: "ColumnOrName", right: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(left, glotexp.Levenshtein, expression=ensure_col(right).expression)


def locate(substr: str, str: "ColumnOrName", pos: int = None) -> "Column":
    if pos is not None:
        return _invoke_anonymous_function(lit(substr), "LOCATE", str, lit(pos))
    return _invoke_anonymous_function(lit(substr), "LOCATE", str)


def lpad(col: "ColumnOrName", len: int, pad: str) -> "Column":
    return _invoke_anonymous_function(col, "LPAD", lit(len), lit(pad))


def rpad(col: "ColumnOrName", len: int, pad: str) -> "Column":
    return _invoke_anonymous_function(col, "RPAD", lit(len), lit(pad))


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)


def sha2(col: "ColumnOrName", numBits: int) -> "Column":
    col = col if isinstance(col, Column) else lit(col)
    return _invoke_anonymous_function(col, "SHA2", numBits)