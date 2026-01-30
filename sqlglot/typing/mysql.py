from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{expr_type: {"returns": exp.DataType.Type.VARCHAR} for expr_type in (exp.Elt,)},
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.DayOfWeek,
            exp.Month,
        }
    },
    exp.Localtime: {"returns": exp.DataType.Type.DATETIME},
}
