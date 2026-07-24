from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_reverse(self: TypeAnnotator, expression: exp.Reverse) -> exp.Reverse:
    if expression.this.is_type(exp.DType.BINARY, exp.DType.VARBINARY, exp.DType.UNKNOWN):
        self._annotate_by_args(expression, "this")
    else:
        self._set_type(expression, exp.DType.VARCHAR)

    return expression


def _annotate_truncate(self: TypeAnnotator, expression: exp.Trunc) -> exp.Expr:
    if expression.this.is_type(*exp.DataType.TEXT_TYPES):
        return self._set_type(expression, exp.DType.DOUBLE)

    return self._annotate_by_args(expression, "this")


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Atan2,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DATETIME}
        for expr_type in {
            exp.CurrentTimestamp,
            exp.ConvertTimezone,
            exp.Localtime,
            exp.Localtimestamp,
            exp.UtcTimestamp,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DATE}
        for expr_type in {
            exp.UtcDate,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.Elt,
            exp.Hex,
            exp.NumberToStr,  # format()
            exp.Replace,
            exp.Stuff,  # insert function
            exp.SubstringIndex,
            exp.RegexpSubstr,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.Month,
            exp.Second,
            exp.Week,
            exp.Minute,
        }
    },
    **{
        expr_type: {"returns": exp.DType.TIME}
        for expr_type in {
            exp.TimeFromParts,
            exp.UtcTime,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DType.LONGTEXT}
        for expr_type in {
            exp.RegexpReplace,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Pad,
            exp.Left,
            exp.Right,
        }
    },
    exp.Reverse: {"annotator": _annotate_reverse},
    exp.Trunc: {"annotator": _annotate_truncate},
}
