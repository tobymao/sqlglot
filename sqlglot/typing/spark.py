from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DataType.Type.DOUBLE}
        for exp_type in {
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DataType.Type.INT}
        for exp_type in {
            exp.ArraySize,
        }
    },
    **{
        exp_type: {"returns": exp.DataType.Type.VARCHAR}
        for exp_type in {
            exp.Collation,
            exp.CurrentTimezone,
            exp.Monthname,
            exp.Randstr,
            exp.SessionUser,
        }
    },
    **{
        exp_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for exp_type in {
            exp.ArrayCompact,
            exp.Overlay,
        }
    },
    exp.BitmapCount: {"returns": exp.DataType.Type.BIGINT},
    exp.Localtimestamp: {"returns": exp.DataType.Type.TIMESTAMPNTZ},
    exp.ToBinary: {"returns": exp.DataType.Type.BINARY},
    exp.DateFromUnixDate: {"returns": exp.DataType.Type.DATE},
}
