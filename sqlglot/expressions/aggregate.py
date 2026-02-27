"""sqlglot expressions - aggregate, window, and statistical functions."""

from __future__ import annotations

from sqlglot.expressions.core import ExpressionBase, Func, AggFunc, Binary


class AIAgg(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["AI_AGG"]


class AISummarizeAgg(ExpressionBase, AggFunc):
    _sql_names = ["AI_SUMMARIZE_AGG"]


class AnyValue(ExpressionBase, AggFunc):
    pass


class ApproximateSimilarity(ExpressionBase, AggFunc):
    _sql_names = ["APPROXIMATE_SIMILARITY", "APPROXIMATE_JACCARD_INDEX"]


class ApproxPercentileAccumulate(ExpressionBase, AggFunc):
    pass


class ApproxPercentileCombine(ExpressionBase, AggFunc):
    pass


class ApproxPercentileEstimate(ExpressionBase, Func):
    arg_types = {"this": True, "percentile": True}


class ApproxQuantiles(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopK(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False, "counters": False}


class ApproxTopKAccumulate(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKCombine(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKEstimate(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class ApproxTopSum(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True, "count": True}


class ArgMax(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MAX", "ARGMAX", "MAX_BY"]


class ArgMin(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MIN", "ARGMIN", "MIN_BY"]


class ArrayAgg(ExpressionBase, AggFunc):
    arg_types = {"this": True, "nulls_excluded": False}


class ArrayConcatAgg(ExpressionBase, AggFunc):
    pass


class ArrayUnionAgg(ExpressionBase, AggFunc):
    pass


class ArrayUniqueAgg(ExpressionBase, AggFunc):
    pass


class Avg(ExpressionBase, AggFunc):
    pass


class Corr(ExpressionBase, AggFunc, Binary):
    # Correlation divides by variance(column). If a column has 0 variance, the denominator
    # is 0 - some dialects return NaN (DuckDB) while others return NULL (Snowflake).
    # `null_on_zero_variance` is set to True at parse time for dialects that return NULL.
    arg_types = {"this": True, "expression": True, "null_on_zero_variance": False}


class Count(ExpressionBase, AggFunc):
    arg_types = {"this": False, "expressions": False, "big_int": False}
    is_var_len_args = True


class CountIf(ExpressionBase, AggFunc):
    _sql_names = ["COUNT_IF", "COUNTIF"]


class CovarPop(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class CovarSamp(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class CumeDist(ExpressionBase, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class DenseRank(ExpressionBase, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class First(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class FirstValue(ExpressionBase, AggFunc):
    pass


class GroupConcat(ExpressionBase, AggFunc):
    arg_types = {"this": True, "separator": False, "on_overflow": False}


class Grouping(ExpressionBase, AggFunc):
    arg_types = {"expressions": True}
    is_var_len_args = True


class GroupingId(ExpressionBase, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Kurtosis(ExpressionBase, AggFunc):
    pass


class Lag(ExpressionBase, AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class Last(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class LastValue(ExpressionBase, AggFunc):
    pass


class Lead(ExpressionBase, AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class LogicalAnd(ExpressionBase, AggFunc):
    _sql_names = ["LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"]


class LogicalOr(ExpressionBase, AggFunc):
    _sql_names = ["LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"]


class Max(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Median(ExpressionBase, AggFunc):
    pass


class Min(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Minhash(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MinhashCombine(ExpressionBase, AggFunc):
    pass


class Mode(ExpressionBase, AggFunc):
    arg_types = {"this": False, "deterministic": False}


class Ntile(ExpressionBase, AggFunc):
    arg_types = {"this": False}


class NthValue(ExpressionBase, AggFunc):
    arg_types = {"this": True, "offset": True, "from_first": False}


class ObjectAgg(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class PercentileCont(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentileDisc(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": False}


PERCENTILES = (PercentileCont, PercentileDisc)


class PercentRank(ExpressionBase, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Quantile(ExpressionBase, AggFunc):
    arg_types = {"this": True, "quantile": True}


class ApproxQuantile(Quantile):
    arg_types = {
        "this": True,
        "quantile": True,
        "accuracy": False,
        "weight": False,
        "error_tolerance": False,
    }


class Rank(ExpressionBase, AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class RegrAvgx(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrAvgy(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrCount(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrIntercept(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrR2(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSlope(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxx(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxy(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSyy(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValx(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValy(ExpressionBase, AggFunc):
    arg_types = {"this": True, "expression": True}


class RowNumber(ExpressionBase, Func):
    arg_types = {"this": False}


class Skewness(ExpressionBase, AggFunc):
    pass


class Stddev(ExpressionBase, AggFunc):
    _sql_names = ["STDDEV", "STDEV"]


class StddevPop(ExpressionBase, AggFunc):
    pass


class StddevSamp(ExpressionBase, AggFunc):
    pass


class Sum(ExpressionBase, AggFunc):
    pass


class Variance(ExpressionBase, AggFunc):
    _sql_names = ["VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"]


class VariancePop(ExpressionBase, AggFunc):
    _sql_names = ["VARIANCE_POP", "VAR_POP"]
