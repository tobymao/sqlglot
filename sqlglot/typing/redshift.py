from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing.postgres import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_sha2(self: TypeAnnotator, expression: exp.SHA2) -> exp.Expr:
    size = 128
    length = expression.args.get("length")
    if length and length.is_int:
        bits = length.to_py()
        size = (bits or 256) // 4 if bits in (0, 224, 256, 384, 512) else 1

    return self._set_type(expression, exp.DataType.build(f"VARCHAR({size})"))


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    exp.SHA2: {"annotator": _annotate_sha2},
    # Redshift's TO_TIMESTAMP returns TIMESTAMPTZ, not TIMESTAMP
    # https://docs.aws.amazon.com/redshift/latest/dg/r_TO_TIMESTAMP.html
    exp.StrToTime: {"returns": exp.DataType.Type.TIMESTAMPTZ},
    # Redshift's RANK returns INTEGER; DENSE_RANK/NTILE/ROW_NUMBER return BIGINT (base default).
    # https://docs.aws.amazon.com/redshift/latest/dg/r_WF_RANK.html
    exp.Rank: {"returns": exp.DType.INT},
    # Postgres NTILE is INT, but Redshift's is BIGINT — restore the base default.
    # https://docs.aws.amazon.com/redshift/latest/dg/r_WF_NTILE.html
    exp.Ntile: {"returns": exp.DType.BIGINT},
}
