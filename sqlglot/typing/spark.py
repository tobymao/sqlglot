from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DType.DOUBLE}
        for exp_type in {
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DType.INT}
        for exp_type in {
            exp.ArraySize,
        }
    },
    **{
        exp_type: {"returns": exp.DType.VARCHAR}
        for exp_type in {
            exp.Collation,
            exp.CurrentTimezone,
            exp.Randstr,
        }
    },
    **{
        exp_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for exp_type in {
            exp.ArrayCompact,
            exp.ArrayInsert,
            exp.BitwiseAndAgg,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
            exp.Overlay,
        }
    },
    exp.BitmapCount: {"returns": exp.DType.BIGINT},
    exp.Localtimestamp: {"returns": exp.DType.TIMESTAMPNTZ},
    exp.ToBinary: {"returns": exp.DType.BINARY},
    exp.DateFromUnixDate: {"returns": exp.DType.DATE},
}
