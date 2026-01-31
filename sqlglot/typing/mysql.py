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
        }
    },
    **{expr_type: {"returns": exp.DataType.Type.VARCHAR} for expr_type in (exp.Elt,)},
    exp.Localtime: {"returns": exp.DataType.Type.DATETIME},
    exp.DayOfWeek: {"returns": exp.DataType.Type.INT},
}
