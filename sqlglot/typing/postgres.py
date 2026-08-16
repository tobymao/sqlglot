from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    # https://www.postgresql.org/docs/current/functions-window.html
    # NTILE returns integer; other ranking functions return bigint (base default).
    exp.Ntile: {"returns": exp.DType.INT},
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
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Decode,
        }
    },
}
