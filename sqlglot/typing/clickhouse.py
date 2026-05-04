from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.UBIGINT}
        for expr_type in {
            exp.CountIf,
        }
    },
    exp.MD5Digest: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("FixedString(16)", dialect="clickhouse")
        )
    },
}
