from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DType.INT}
        for exp_type in {
            exp.RegexpCount,
        }
    },
    **{
        exp_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this", array=True)}
        for exp_type in {
            exp.RegexpExtractAll,
        }
    },
}
