from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    exp.Rand: {"returns": exp.DType.BIGINT},
}
