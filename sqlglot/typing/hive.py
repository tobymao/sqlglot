from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
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
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.AddMonths,
            exp.CurrentDatabase,
            exp.CurrentUser,
            exp.CurrentSchema,
            exp.Hex,
            exp.NextDay,
            exp.Repeat,
            exp.Replace,
            exp.Soundex,
        }
    },
    **{
        expr_type: {"returns": exp.DType.BIGINT}
        for expr_type in {
            exp.StrToUnix,
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.Month,
            exp.Second,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.ArrayDistinct,
            exp.ArrayExcept,
            exp.Reverse,
        }
    },
    exp.ApproxQuantile: {"annotator": lambda self, e: self._annotate_by_args(e, "quantile")},
    exp.ArrayIntersect: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.Coalesce: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True)
    },
    exp.If: {"annotator": lambda self, e: self._annotate_by_args(e, "true", "false", promote=True)},
    exp.Quantile: {"annotator": lambda self, e: self._annotate_by_args(e, "quantile")},
    exp.RegexpSplit: {"returns": exp.DataType.build("ARRAY<STRING>")},
}
