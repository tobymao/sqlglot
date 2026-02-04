from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.BitwiseAnd,
            exp.BitwiseNot,
            exp.BitwiseOr,
            exp.BitwiseXor,
            exp.Length,
            exp.Levenshtein,
            exp.StrPosition,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Ceil,
            exp.Floor,
            exp.Round,
            exp.Sign,
        }
    },
    exp.Mod: {"annotator": lambda self, e: self._annotate_by_args(e, "this", "expression")},
    exp.Rand: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this")
        if e.this
        else self._set_type(e, exp.DataType.Type.DOUBLE)
    },
    exp.MD5Digest: {"returns": exp.DataType.Type.VARBINARY},
}
