from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.typing import EXPRESSION_METADATA, annotate_by_numeric_arg

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_math_function(self: TypeAnnotator, expression: exp.Expr) -> exp.Expr:
    this = expression.this
    if this.is_type(exp.DType.UTINYINT, exp.DType.SMALLINT):
        return self._set_type(expression, exp.DType.INT)
    if this.is_type(exp.DType.BIT, *exp.DataType.FLOAT_TYPES, *exp.DataType.TEXT_TYPES):
        return self._set_type(expression, exp.DType.DOUBLE)
    if this.is_type(exp.DType.SMALLMONEY):
        return self._set_type(expression, exp.DType.MONEY)
    dtype = this.type
    if dtype and dtype.is_type(exp.DType.DECIMAL) and not isinstance(expression, exp.Sign):
        precision = seq_get(dtype.expressions, 0)
        return self._set_type(
            expression,
            exp.DataType.build(f"DECIMAL({precision.this.to_py() if precision else 18}, 0)"),
        )
    return annotate_by_numeric_arg(self, expression)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.FLOAT}
        for expr_type in {
            exp.Acos,
            exp.Asin,
            exp.Atan,
            exp.Atan2,
            exp.Cos,
            exp.Cot,
            exp.Sin,
            exp.Tan,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.Soundex,
            exp.Stuff,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Degrees,
            exp.Radians,
        }
    },
    **{
        expr_type: {"annotator": _annotate_math_function}
        for expr_type in {
            exp.Ceil,
            exp.Floor,
            exp.Sign,
        }
    },
    exp.CurrentTimezone: {"returns": exp.DType.NVARCHAR},
    exp.CurrentTimestamp: {"returns": exp.DType.DATETIME},
}
