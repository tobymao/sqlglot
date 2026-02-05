from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA


def _annotate_overlay(self, expression):
    if expression.this.is_type(exp.DataType.Type.BINARY):
        self._set_type(expression, exp.DataType.Type.BINARY)
    else:
        self._set_type(expression, exp.DataType.Type.VARCHAR)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DataType.Type.DOUBLE}
        for exp_type in {
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DataType.Type.VARCHAR}
        for exp_type in {
            exp.Collation,
            exp.CurrentTimezone,
            exp.Monthname,
            exp.SessionUser,
        }
    },
    exp.BitmapCount: {"returns": exp.DataType.Type.BIGINT},
    exp.Localtimestamp: {"returns": exp.DataType.Type.TIMESTAMPNTZ},
    exp.ToBinary: {"returns": exp.DataType.Type.BINARY},
    exp.DateFromUnixDate: {"returns": exp.DataType.Type.DATE},
    exp.ArraySize: {"returns": exp.DataType.Type.INT},
    exp.Overlay: {"annotator": _annotate_overlay},
}
