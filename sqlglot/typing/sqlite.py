from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA, annotate_signed_integer_literal

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_neg(self: TypeAnnotator, expression: exp.Neg) -> exp.Expr:
    operand = expression.this.unnest()
    while isinstance(operand, exp.Pos):
        operand = operand.this.unnest()

    if isinstance(operand, exp.Neg):
        # Only -9223372036854775808 changes REAL to INTEGER. Negating it overflows back to REAL.
        if operand.is_type(exp.DType.INT) and operand.this.is_type(exp.DType.DOUBLE):
            expression.meta["nonnull"] = True
            return self._set_type(expression, exp.DType.DOUBLE)
        return self._annotate_unary(expression)

    return annotate_signed_integer_literal(self, expression, negated_operand=operand)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    exp.Literal: {"annotator": annotate_signed_integer_literal},
    exp.Neg: {"annotator": _annotate_neg},
    exp.Rand: {"returns": exp.DType.BIGINT},
}
