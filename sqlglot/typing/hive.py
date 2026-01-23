from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.BINARY}
        for expr_type in {
            exp.Encode,
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Asin,
            exp.Acos,
            exp.Atan,
            exp.Corr,
            exp.Cos,
            exp.Cosh,
            exp.MonthsBetween,
            exp.Sin,
            exp.Sinh,
            exp.Tan,
            exp.Tanh,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.CurrentCatalog,
            exp.CurrentDatabase,
            exp.CurrentSchema,
            exp.CurrentUser,
            exp.Hex,
            exp.Soundex,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.StrToUnix,
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.Month,
            exp.Quarter,
        }
    },
    exp.Coalesce: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True)
    },
    exp.If: {"annotator": lambda self, e: self._annotate_by_args(e, "true", "false", promote=True)},
}
