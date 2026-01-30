from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.INT128}
        for expr_type in {
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Cos,
            exp.Cot,
            exp.Rand,
            exp.Sin,
            exp.Tan,
        }
    },
}
