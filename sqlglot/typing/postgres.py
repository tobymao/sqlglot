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
            exp.RegexpReplace,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Decode,
        }
    },
    **{
        expr_type: {"returns": exp.DType.TIME}
        for expr_type in {
            exp.TimeFromParts,
        }
    },
    **{
        expr_type: {"returns": exp.DType.TIMESTAMP}
        for expr_type in {
            exp.TimestampFromParts,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.BitwiseAndAgg,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
        }
    },
    exp.ToNumber: {"returns": exp.DType.DECIMAL},
}
