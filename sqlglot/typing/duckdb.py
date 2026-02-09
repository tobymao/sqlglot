from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.BitLength,
            exp.Day,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfWeekIso,
            exp.DayOfYear,
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
        expr_type: {"returns": exp.DataType.Type.INT128}
        for expr_type in {
            exp.CountIf,
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.JarowinklerSimilarity,
            exp.Rand,
            exp.TimeToUnix,
        }
    },
      **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.Format,
            exp.Reverse,
        }
    },
    exp.ToDays: {"returns": exp.DataType.Type.INTERVAL},
    exp.TimeFromParts: {"returns": exp.DataType.Type.TIME},
}
