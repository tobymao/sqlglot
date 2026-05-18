from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.postgres import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.DenseRank,
            exp.Rank,
            exp.RowNumber,
        }
    },
}
