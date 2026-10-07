from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_floor_ceil(self: TypeAnnotator, expression: exp.Expr) -> exp.Expr:
    # Decimal results and numeric literals need precision inference that we don't model here.
    if (
        expression.args.get("decimals") is not None
        or expression.this.is_type(exp.DType.DECIMAL, exp.DType.UNKNOWN)
        or expression.this.unnest().is_number
    ):
        return self._set_type(expression, exp.DType.UNKNOWN)
    return self._set_type(expression, exp.DType.BIGINT)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{expr_type: {"annotator": _annotate_floor_ceil} for expr_type in {exp.Ceil, exp.Floor}},
    **{
        expr_type: {"returns": exp.DType.BINARY}
        for expr_type in {
            exp.Encode,
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Corr,
            exp.MonthsBetween,
            exp.Sign,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.AddMonths,
            exp.CurrentDatabase,
            exp.Hex,
            exp.JSONExtractScalar,
            exp.JSONFormat,
            exp.NextDay,
            exp.RegexpExtract,
            exp.RegexpReplace,
            exp.Replace,
            exp.Soundex,
        }
    },
    **{
        expr_type: {"returns": exp.DType.BIGINT}
        for expr_type in {
            exp.Factorial,
            exp.IntDiv,
            exp.StrToUnix,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.ArraySize,
            exp.DenseRank,
            exp.Month,
            exp.Ntile,
            exp.Rank,
            exp.RowNumber,
            exp.Second,
            exp.Minute,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.ArrayDistinct,
            exp.ArrayExcept,
            exp.First,
            exp.Last,
            exp.Negative,
            exp.Reverse,
        }
    },
    exp.ArrayIntersect: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.ApproxQuantile: {"annotator": lambda self, e: self._annotate_by_args(e, "quantile")},
    exp.Coalesce: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True)
    },
    exp.Grouping: {"returns": exp.DType.BIGINT},
    exp.If: {"annotator": lambda self, e: self._annotate_by_args(e, "true", "false", promote=True)},
    exp.PercentileDisc: {"returns": exp.DType.DOUBLE},
    exp.Quantile: {"annotator": lambda self, e: self._annotate_by_args(e, "quantile")},
    exp.RegexpSplit: {"returns": exp.DataType.from_str("ARRAY<STRING>")},
    exp.StrToMap: {"returns": exp.DataType.from_str("MAP<STRING, STRING>")},
    exp.WithinGroup: {"annotator": lambda self, e: self._annotate_by_args(e, "this")},
}
