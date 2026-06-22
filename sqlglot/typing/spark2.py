from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.typing.hive import EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.annotate_types import TypeAnnotator
    from sqlglot.typing import ExprMetadataType


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
            exp.Substring,
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
