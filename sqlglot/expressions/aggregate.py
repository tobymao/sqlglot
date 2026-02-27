"""sqlglot expressions - aggregate, window, and statistical functions."""

from __future__ import annotations

from sqlglot.expressions.core import Expression, Func, AggFunc, Binary


class AIAgg(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["AI_AGG"]


class AISummarizeAgg(Expression, AggFunc):
    _sql_names = ["AI_SUMMARIZE_AGG"]


class AnyValue(Expression, AggFunc):
    pass


class ApproximateSimilarity(Expression, AggFunc):
    _sql_names = ["APPROXIMATE_SIMILARITY", "APPROXIMATE_JACCARD_INDEX"]


class ApproxPercentileAccumulate(Expression, AggFunc):
    pass


class ApproxPercentileCombine(Expression, AggFunc):
    pass


class ApproxPercentileEstimate(Expression, Func):
    arg_types = {"this": True, "percentile": True}


class ApproxQuantiles(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopK(Expression, AggFunc):
    arg_types = {"this": True, "expression": False, "counters": False}


class ApproxTopKAccumulate(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKCombine(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKEstimate(Expression, Func):
    arg_types = {"this": True, "expression": False}


class ApproxTopSum(Expression, AggFunc):
    arg_types = {"this": True, "expression": True, "count": True}


class ArgMax(Expression, AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MAX", "ARGMAX", "MAX_BY"]


class ArgMin(Expression, AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MIN", "ARGMIN", "MIN_BY"]


class ArrayAgg(Expression, AggFunc):
    arg_types = {"this": True, "nulls_excluded": False}


class ArrayConcatAgg(Expression, AggFunc):
    pass


class ArrayUnionAgg(Expression, AggFunc):
    pass


class ArrayUniqueAgg(Expression, AggFunc):
    pass


class Avg(Expression, AggFunc):
    pass


class Corr(Expression, AggFunc, Binary):
    # Correlation divides by variance(column). If a column has 0 variance, the denominator
    # is 0 - some dialects return NaN (DuckDB) while others return NULL (Snowflake).
    # `null_on_zero_variance` is set to True at parse time for dialects that return NULL.
    arg_types = {"this": True, "expression": True, "null_on_zero_variance": False}


class Count(Expression, AggFunc):
    arg_types = {"this": False, "expressions": False, "big_int": False}
    is_var_len_args = True


class CountIf(Expression, AggFunc):
    _sql_names = ["COUNT_IF", "COUNTIF"]


class CovarPop(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class CovarSamp(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class CumeDist(Expression, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class DenseRank(Expression, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class First(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class FirstValue(Expression, AggFunc):
    pass


class GroupConcat(Expression, AggFunc):
    arg_types = {"this": True, "separator": False, "on_overflow": False}


class Grouping(Expression, AggFunc):
    arg_types = {"expressions": True}
    is_var_len_args = True


class GroupingId(Expression, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Kurtosis(Expression, AggFunc):
    pass


class Lag(Expression, AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class Last(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class LastValue(Expression, AggFunc):
    pass


class Lead(Expression, AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class LogicalAnd(Expression, AggFunc):
    _sql_names = ["LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"]


class LogicalOr(Expression, AggFunc):
    _sql_names = ["LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"]


class Max(Expression, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Median(Expression, AggFunc):
    pass


class Min(Expression, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Minhash(Expression, AggFunc):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MinhashCombine(Expression, AggFunc):
    pass


class Mode(Expression, AggFunc):
    arg_types = {"this": False, "deterministic": False}


class Ntile(Expression, AggFunc):
    arg_types = {"this": False}


class NthValue(Expression, AggFunc):
    arg_types = {"this": True, "offset": True, "from_first": False}


class ObjectAgg(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class PercentileCont(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentileDisc(Expression, AggFunc):
    arg_types = {"this": True, "expression": False}


PERCENTILES = (PercentileCont, PercentileDisc)


class PercentRank(Expression, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Quantile(Expression, AggFunc):
    arg_types = {"this": True, "quantile": True}


class ApproxQuantile(Quantile):
    arg_types = {
        "this": True,
        "quantile": True,
        "accuracy": False,
        "weight": False,
        "error_tolerance": False,
    }


class Rank(Expression, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class RegrAvgx(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrAvgy(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrCount(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrIntercept(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrR2(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSlope(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxx(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxy(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSyy(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValx(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValy(Expression, AggFunc):
    arg_types = {"this": True, "expression": True}


class RowNumber(Expression, Func):
    arg_types = {"this": False}


class Skewness(Expression, AggFunc):
    pass


class Stddev(Expression, AggFunc):
    _sql_names = ["STDDEV", "STDEV"]


class StddevPop(Expression, AggFunc):
    pass


class StddevSamp(Expression, AggFunc):
    pass


class Sum(Expression, AggFunc):
    pass


class Variance(Expression, AggFunc):
    _sql_names = ["VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"]


class VariancePop(Expression, AggFunc):
    _sql_names = ["VARIANCE_POP", "VAR_POP"]
