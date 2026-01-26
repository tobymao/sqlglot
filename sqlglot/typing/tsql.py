from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.FLOAT}
        for expr_type in {
            exp.Cos,
            exp.Cot,
            exp.Sin,
            exp.Tan,
        }
    },
    exp.Radians: {"annotator": lambda self, e: self._annotate_by_args(e, "this")},
}
