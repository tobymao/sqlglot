from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.typing.hive import EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.annotate_types import TypeAnnotator
    from sqlglot.typing import ExprMetadataType


def _common_array_element_type(types: list[exp.DataType]) -> exp.DataType | exp.DType:
    """
    Recursively narrows a list of CONCAT-arg DataTypes to their common type.

    - Returns UNKNOWN for incompatible types: scalar mismatches (INT + DATE),
      ARRAY mixed with non-ARRAY (e.g. CONCAT(ARRAY<INT>, INT)), and unmatched
      nesting depths (which yield ARRAY<UNKNOWN> at the appropriate level).
      UNKNOWN means "no common type", which the caller relies on.
    - Return type is exp.DataType | exp.DType: bare DType for simple cases,
      DataType for the recursive ARRAY case where nesting is required.
    """
    normalized = [
        exp.DataType(this=exp.DType.TEXT) if t.this in exp.DataType.TEXT_TYPES else t for t in types
    ]
    if len({t.sql() for t in normalized}) == 1:
        return normalized[0]
    if all(t.this == exp.DType.ARRAY for t in normalized):
        elem_types = [
            t.expressions[0] if t.expressions else exp.DataType(this=exp.DType.UNKNOWN)
            for t in normalized
        ]
        common_elem = _common_array_element_type(elem_types)
        elem_dt = (
            common_elem if isinstance(common_elem, exp.DataType) else exp.DataType(this=common_elem)
        )
        return exp.DataType(this=exp.DType.ARRAY, expressions=[elem_dt], nested=True)
    if any(t.this == exp.DType.TEXT for t in normalized):
        return exp.DType.TEXT
    return exp.DType.UNKNOWN


def _annotate_by_similar_args(self: TypeAnnotator, expression: E, *arg_keys: str) -> E:
    """
    Type inference for CONCAT-family expressions (CONCAT, LPAD, RPAD).

    - TEXT-before-UNKNOWN is load-bearing: a known text arg forces a text
      result, since the query either coerces the unknown to string or fails
      entirely — no valid execution produces a non-text result.
    - TEXT_TYPES on input narrows to DType.TEXT on output: CONCAT/LPAD
      accept any TEXT_TYPES member (VARCHAR/CHAR/NCHAR/NVARCHAR/NAME) as
      input, but Spark always emits DType.TEXT.
    """
    arg_exprs: list[exp.Expression] = []
    for key in arg_keys:
        arg_exprs.extend(e for e in ensure_list(expression.args.get(key)) if e)

    result: exp.DataType | exp.DType
    if any(e.is_type(*exp.DataType.TEXT_TYPES) for e in arg_exprs):
        result = exp.DType.TEXT
    elif any(e.is_type(exp.DType.UNKNOWN) for e in arg_exprs):
        result = exp.DType.UNKNOWN
    elif all(e.is_type(exp.DType.BINARY) for e in arg_exprs):
        result = exp.DType.BINARY
    elif any(e.is_type(exp.DType.ARRAY) for e in arg_exprs):
        result = _common_array_element_type([e.type for e in arg_exprs])
    else:
        result = exp.DType.TEXT

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
