from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA

EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DataType.Type.DOUBLE}
        for exp_type in {
            exp.Acosh,
            exp.Asinh,
            exp.Atanh,
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DataType.Type.VARCHAR}
        for exp_type in {
            exp.CurrentTimezone,
            exp.Monthname,
            exp.SessionUser,
        }
    },
    exp.Localtimestamp: {"returns": exp.DataType.Type.TIMESTAMPNTZ},
    exp.ToBinary: {"returns": exp.DataType.Type.BINARY},
    exp.DateFromUnixDate: {"returns": exp.DataType.Type.DATE},
}
