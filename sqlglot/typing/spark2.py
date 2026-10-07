from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list, seq_get
from sqlglot.typing.hive import EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.annotate_types import TypeAnnotator
    from sqlglot.typing import ExprMetadataType


def _annotate_floor_ceil(self: TypeAnnotator, expression: exp.Expr) -> exp.Expr:
    this = expression.this
    dtype = this.type
    if not dtype or dtype.is_type(exp.DType.UNKNOWN):
        return self._set_type(expression, exp.DType.UNKNOWN)

    decimals = expression.args.get("decimals")
    literal = this.unnest()
    if literal.is_int and not -(2**31) <= literal.to_py() < 2**31:
        value = literal.to_py()
        dtype = (
            exp.DType.BIGINT.into_expr()
            if -(2**63) <= value < 2**63
            else exp.DataType.build(f"DECIMAL({len(str(abs(value)))}, 0)")
        )
    if literal.is_number and not literal.is_int and "E" not in literal.name.upper():
        number = literal.to_py().as_tuple()
        scale = -number.exponent
        precision = max(len(number.digits), scale)
    elif dtype.is_type(exp.DType.DECIMAL):
        precision_expr = seq_get(dtype.expressions, 0)
        scale_expr = seq_get(dtype.expressions, 1)
        precision = precision_expr.this.to_py() if precision_expr else 10
        scale = scale_expr.this.to_py() if scale_expr else 0
    elif decimals is None:
        return self._set_type(expression, exp.DType.BIGINT)
    else:
        # Spark coerces the two-argument overload to DECIMAL before rounding.
        decimal_type = {
            exp.DType.TINYINT: (3, 0),
            exp.DType.SMALLINT: (5, 0),
            exp.DType.INT: (10, 0),
            exp.DType.BIGINT: (20, 0),
            exp.DType.FLOAT: (14, 7),
            exp.DType.DOUBLE: (30, 15),
        }.get(dtype.this)
        if decimal_type is None:
            return self._set_type(expression, exp.DType.UNKNOWN)
        precision, scale = decimal_type

    if decimals is None:
        precision = precision - scale + (scale != 0)
        scale = 0
    elif decimals.is_int:
        target_scale = decimals.to_py()
        integral_digits = precision - scale + 1
        scale = min(scale, max(target_scale, 0))
        precision = max(integral_digits, -target_scale + 1) + scale
    else:
        return self._set_type(expression, exp.DType.UNKNOWN)

    return self._set_type(expression, exp.DataType.build(f"DECIMAL({min(precision, 38)}, {scale})"))


def _annotate_by_similar_args(self: TypeAnnotator, expression: E, *arg_keys: str) -> E:
    """
    Type inference for CONCAT-family expressions (CONCAT, LPAD, RPAD).

    - All-BINARY → BINARY (the binary overload).
    - Otherwise, if any arg has a known, non-array, non-binary type → STRING.
      Spark coerces scalars (dates, ints, etc.) to string when mixed with a
      string-resolving arg. The binary exclusion preserves the binary+unknown
      case as UNKNOWN: Spark can't disambiguate the string vs. binary overload
      there.
    - Else → UNKNOWN. Covers all-unknown, binary+unknown, and anything
      involving arrays (array handling is intentionally out of scope here).
    """
    arg_exprs: list[exp.Expression] = []
    for key in arg_keys:
        arg_exprs.extend(e for e in ensure_list(expression.args.get(key)) if e)

    if arg_exprs and all(e.is_type(exp.DType.BINARY) for e in arg_exprs):
        result: exp.DataType | exp.DType = exp.DType.BINARY
    elif any(
        e.type is not None and not e.is_type(exp.DType.UNKNOWN, exp.DType.ARRAY, exp.DType.BINARY)
        for e in arg_exprs
    ):
        result = exp.DType.TEXT
    else:
        result = exp.DType.UNKNOWN

    self._set_type(expression, result)
    return expression


EXPRESSION_METADATA: ExprMetadataType = {
    **HIVE_EXPRESSION_METADATA,
    **{expr_type: {"annotator": _annotate_floor_ceil} for expr_type in {exp.Ceil, exp.Floor}},
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.Randn,
        }
    },
    **{
        exp_type: {"returns": exp.DType.VARCHAR}
        for exp_type in {
            exp.Format,
            exp.Right,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.ArrayFilter,
            exp.Shuffle,
            exp.Substring,
        }
    },
    **{
        exp_type: {"returns": exp.DType.DOUBLE}
        for exp_type in {
            exp.Nanvl,
        }
    },
    exp.AddMonths: {"returns": exp.DType.DATE},
    exp.ApproxQuantile: {
        "annotator": lambda self, e: self._annotate_by_args(
            e, "this", array=e.args["quantile"].is_type(exp.DType.ARRAY)
        )
    },
    exp.AtTimeZone: {"returns": exp.DType.TIMESTAMP},
    exp.Concat: {"annotator": lambda self, e: _annotate_by_similar_args(self, e, "expressions")},
    exp.NextDay: {"returns": exp.DType.DATE},
    exp.Pad: {
        "annotator": lambda self, e: _annotate_by_similar_args(self, e, "this", "fill_pattern")
    },
}
