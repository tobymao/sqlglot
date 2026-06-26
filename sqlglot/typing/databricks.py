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
    exp.RegexpExtractAll: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.from_str("ARRAY<STRING>", dialect="databricks")
        )
    },
}
