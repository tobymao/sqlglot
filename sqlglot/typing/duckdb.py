from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.Day,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfYear,
            exp.Hour,
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
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Acosh,
            exp.Asinh,
            exp.Atanh,
            exp.Atan2,
            exp.Acos,
            exp.Asin,
            exp.Atan,
            exp.Cos,
            exp.Cot,
            exp.Rand,
            exp.Sin,
            exp.Tan,
        }
    },
}
