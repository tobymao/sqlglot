import typing as t

from sqlglot import expressions as glotexp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe.util import ensure_strings, ensure_sqlglot_column

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame

from pyspark.sql import functions

ColumnOrName = t.TypeVar("ColumnOrName", bound=t.Union["Column", str])


def ensure_col(value: t.Union["ColumnOrName", int, float]):
    return Column(value)


def _invoke_column_function(column: "ColumnOrName", func_name: str, *args, **kwargs) -> "Column":
    column = ensure_col(column)
    func = getattr(column, func_name)
    return func(*args, **kwargs)


def _invoke_expression_over_column(column: "ColumnOrName", expression: t.Callable, **kwargs) -> "Column":
    column = ensure_col(column)
    new_expression = expression(this=column.column_expression, **kwargs)
    return Column(new_expression)


def _invoke_anonymous_function(column: "ColumnOrName", func_name: str, *args) -> "Column":
    column = ensure_col(column)
    args = [ensure_col(arg) for arg in args]
    expressions = [x.expression for x in [column] + args]
    new_expression = glotexp.Anonymous(this=func_name.upper(), expressions=expressions)
    return Column(new_expression)


def col(column_name: t.Union[ColumnOrName, int]) -> "Column":
    return Column(column_name)


def lit(value: t.Optional[t.Any] = None) -> "Column":
    if value is None:
        return Column(glotexp.Null())
    return Column(glotexp.Literal(this=str(value), is_string=isinstance(value, str)))


def greatest(*cols: "ColumnOrName") -> "Column":
    cols = ensure_strings(cols)
    return Column(glotexp.Greatest(this=cols[0], expressions=cols[1:]))


def count_distinct(col: "ColumnOrName") -> "Column":
    col = ensure_strings([col])[0]
    return Column(glotexp.Count(this=glotexp.Distinct(this=col)))


def countDistinct(col: "Column") -> "Column":
    return count_distinct(col)


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


def log(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Log)


def log10(col: "ColumnOrName") -> "Column":
    return _invoke_expression_over_column(col, glotexp.Log10)


def log1p(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "LOG1P")


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
    return Column(glotexp.Anonymous(this="PRECENT_RANK"))


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


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")


def sec(col: "ColumnOrName") -> "Column":
    return _invoke_anonymous_function(col, "SEC")