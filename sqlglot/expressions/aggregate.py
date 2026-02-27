"""sqlglot expressions - aggregate, window, and statistical functions."""

from __future__ import annotations

from sqlglot.expressions.core import Func, AggFunc, Binary


class AIAgg(AggFunc):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["AI_AGG"]


class AISummarizeAgg(AggFunc):
    _sql_names = ["AI_SUMMARIZE_AGG"]


class AnyValue(AggFunc):
    pass


class ApproximateSimilarity(AggFunc):
    _sql_names = ["APPROXIMATE_SIMILARITY", "APPROXIMATE_JACCARD_INDEX"]


class ApproxPercentileAccumulate(AggFunc):
    pass


class ApproxPercentileCombine(AggFunc):
    pass


class ApproxPercentileEstimate(Func):
    arg_types = {"this": True, "percentile": True}


class ApproxQuantiles(AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopK(AggFunc):
    arg_types = {"this": True, "expression": False, "counters": False}


class ApproxTopKAccumulate(AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKCombine(AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKEstimate(Func):
    arg_types = {"this": True, "expression": False}


class ApproxTopSum(AggFunc):
    arg_types = {"this": True, "expression": True, "count": True}


class ArgMax(AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MAX", "ARGMAX", "MAX_BY"]


class ArgMin(AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MIN", "ARGMIN", "MIN_BY"]


class ArrayAgg(AggFunc):
    arg_types = {"this": True, "nulls_excluded": False}


class ArrayConcatAgg(AggFunc):
    pass


class ArrayUnionAgg(AggFunc):
    pass


class ArrayUniqueAgg(AggFunc):
    pass


class Avg(AggFunc):
    pass


class Corr(AggFunc, Binary):
    # Correlation divides by variance(column). If a column has 0 variance, the denominator
    # is 0 - some dialects return NaN (DuckDB) while others return NULL (Snowflake).
    # `null_on_zero_variance` is set to True at parse time for dialects that return NULL.
    arg_types = {"this": True, "expression": True, "null_on_zero_variance": False}


class Count(AggFunc):
    arg_types = {"this": False, "expressions": False, "big_int": False}
    is_var_len_args = True


class CountIf(AggFunc):
    _sql_names = ["COUNT_IF", "COUNTIF"]


class CovarPop(AggFunc):
    arg_types = {"this": True, "expression": True}


class CovarSamp(AggFunc):
    arg_types = {"this": True, "expression": True}


class CumeDist(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class DenseRank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class First(AggFunc):
    arg_types = {"this": True, "expression": False}


class FirstValue(AggFunc):
    pass


class GroupConcat(AggFunc):
    arg_types = {"this": True, "separator": False, "on_overflow": False}


class Grouping(AggFunc):
    arg_types = {"expressions": True}
    is_var_len_args = True


class GroupingId(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Kurtosis(AggFunc):
    pass


class Lag(AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class Last(AggFunc):
    arg_types = {"this": True, "expression": False}


class LastValue(AggFunc):
    pass


class Lead(AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class LogicalAnd(AggFunc):
    _sql_names = ["LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"]


class LogicalOr(AggFunc):
    _sql_names = ["LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"]


class Max(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Median(AggFunc):
    pass


class Min(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Minhash(AggFunc):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MinhashCombine(AggFunc):
    pass


class Mode(AggFunc):
    arg_types = {"this": False, "deterministic": False}


class Ntile(AggFunc):
    arg_types = {"this": False}


class NthValue(AggFunc):
    arg_types = {"this": True, "offset": True, "from_first": False}


class ObjectAgg(AggFunc):
    arg_types = {"this": True, "expression": True}


class PercentileCont(AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentileDisc(AggFunc):
    arg_types = {"this": True, "expression": False}


PERCENTILES = (PercentileCont, PercentileDisc)


class PercentRank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class ApproxQuantile(Quantile):
    arg_types = {
        "this": True,
        "quantile": True,
        "accuracy": False,
        "weight": False,
        "error_tolerance": False,
    }


class Rank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class RegrAvgx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrAvgy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrCount(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrIntercept(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrR2(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSlope(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSyy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RowNumber(Func):
    arg_types = {"this": False}


class Skewness(AggFunc):
    pass


class Stddev(AggFunc):
    _sql_names = ["STDDEV", "STDEV"]


class StddevPop(AggFunc):
    pass


class StddevSamp(AggFunc):
    pass


class Sum(AggFunc):
    pass


class Variance(AggFunc):
    _sql_names = ["VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"]


class VariancePop(AggFunc):
    _sql_names = ["VARIANCE_POP", "VAR_POP"]
