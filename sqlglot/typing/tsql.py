from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.FLOAT}
        for expr_type in {
            exp.Acos,
            exp.Asin,
            exp.Atan,
            exp.Atan2,
            exp.Cos,
            exp.Cot,
            exp.Sin,
            exp.Tan,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.Soundex,
            exp.Stuff,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Degrees,
            exp.Radians,
        }
    },
    exp.CurrentTimezone: {"returns": exp.DataType.Type.NVARCHAR},
}
