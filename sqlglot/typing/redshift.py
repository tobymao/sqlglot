from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.postgres import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
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
