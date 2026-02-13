from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Atan2,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DATETIME}
        for expr_type in {
            exp.CurrentTimestamp,
            exp.Localtime,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.Elt,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.Month,
            exp.Second,
            exp.Week,
        }
    },
}
