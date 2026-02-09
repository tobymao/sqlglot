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
            exp.Corr,
            exp.MonthsBetween,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.AddMonths,
            exp.CurrentDatabase,
            exp.CurrentSchema,
            exp.Hex,
            exp.Repeat,
            exp.Replace,
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
            exp.Second,
        }
    },
    exp.Coalesce: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True)
    },
    exp.If: {"annotator": lambda self, e: self._annotate_by_args(e, "true", "false", promote=True)},
    exp.RegexpSplit: {"returns": exp.DataType.build("ARRAY<STRING>")},
    exp.Reverse: {"annotator": lambda self, e: self._annotate_by_args(e, "this")},
}
