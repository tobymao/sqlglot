from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.BIGINT}
        for expr_type in {
            exp.BitLength,
            exp.DateDiff,
            exp.Day,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfWeekIso,
            exp.DayOfYear,
            exp.Extract,
            exp.Hour,
            exp.Length,
            exp.Minute,
            exp.Month,
            exp.Quarter,
            exp.Second,
            exp.Week,
            exp.Year,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT128}
        for expr_type in {
            exp.CountIf,
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.JarowinklerSimilarity,
            exp.TimeToUnix,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.Format,
            exp.Reverse,
        }
    },
    exp.DateBin: {"annotator": lambda self, e: self._annotate_by_args(e, "expression")},
    exp.Localtimestamp: {"returns": exp.DType.TIMESTAMP},
    exp.ToDays: {"returns": exp.DType.INTERVAL},
    exp.TimeFromParts: {"returns": exp.DType.TIME},
}
