from __future__ import annotations

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    # https://www.postgresql.org/docs/current/functions-window.html
    # NTILE returns integer; other ranking functions return bigint (base default).
    exp.Ntile: {"returns": exp.DType.INT},
}
