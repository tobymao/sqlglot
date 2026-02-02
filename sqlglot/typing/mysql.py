from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Acos,
            exp.Asin,
            exp.Atan,
            exp.Atan2,
            exp.Cos,
            exp.Cot,
            exp.Degrees,
            exp.Sin,
            exp.Tan,
        }
    },
    **{expr_type: {"returns": exp.DataType.Type.VARCHAR} for expr_type in (exp.Elt,)},
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.DayOfWeek,
            exp.Month,
            exp.Quarter,
            exp.Second,
        }
    },
    exp.Localtime: {"returns": exp.DataType.Type.DATETIME},
}
