from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.typing.hive import EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.annotate_types import TypeAnnotator
    from sqlglot.typing import ExprMetadataType


def _annotate_by_similar_args(
    self: TypeAnnotator,
    expression: E,
    *args: str,
    target_type: exp.DataType | exp.DType | tuple[exp.DataType | exp.DType, ...],
) -> E:
    """
    Infers the type of the expression according to the following rules:
    - If any arg matches a target_type, the expr is inferred as the first target_type
    - If any arg is of UNKNOWN type and none match target_type, the expr is inferred as UNKNOWN.
    - Otherwise the expr is inferred as the type of the last non-matching arg.
    """
    target_types = target_type if isinstance(target_type, tuple) else (target_type,)
    result_type = target_types[0]

    expressions: list[exp.Expr] = []
    for arg in args:
        arg_expr = expression.args.get(arg)
        expressions.extend(expr for expr in ensure_list(arg_expr) if expr)

    last_datatype = None

    has_unknown = False
    for expr in expressions:
        if expr.is_type(exp.DType.UNKNOWN):
            has_unknown = True
        elif expr.is_type(*target_types):
            has_unknown = False
            last_datatype = result_type
            break
        else:
            last_datatype = expr.type

    self._set_type(expression, exp.DType.UNKNOWN if has_unknown else last_datatype)
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
    exp.Concat: {
        "annotator": lambda self, e: _annotate_by_similar_args(
            self, e, "expressions", target_type=(exp.DType.TEXT, *exp.DataType.TEXT_TYPES)
        )
    },
    exp.NextDay: {"returns": exp.DType.DATE},
    exp.Pad: {
        "annotator": lambda self, e: _annotate_by_similar_args(
            self, e, "this", "fill_pattern", target_type=(exp.DType.TEXT, *exp.DataType.TEXT_TYPES)
        )
    },
}
