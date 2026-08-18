from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.Ntile,
            exp.WidthBucket,
        }
    },
    **{
        expr_type: {"returns": exp.DType.TEXT}
        for expr_type in {
            exp.Encode,
            exp.Left,
            exp.Right,
            exp.Overlay,
            exp.Reverse,
            exp.Pad,
            exp.Format,
            exp.Hex,
            exp.SplitPart,
            exp.Normalize,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Decode,
        }
    },
    exp.ToNumber: {"returns": exp.DType.DECIMAL},
}
