from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.typing.hive import EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.annotate_types import TypeAnnotator
    from sqlglot.typing import ExpressionMetadataType


def _annotate_by_similar_args(
    self: TypeAnnotator, expression: E, *args: str, target_type: exp.DataType | exp.DataType.Type
) -> E:
    """
    Infers the type of the expression according to the following rules:
    - If all args are of the same type OR any arg is of target_type, the expr is inferred as such
    - If any arg is of UNKNOWN type and none of target_type, the expr is inferred as UNKNOWN
    """
    expressions: t.List[exp.Expression] = []
    for arg in args:
        arg_expr = expression.args.get(arg)
        expressions.extend(expr for expr in ensure_list(arg_expr) if expr)

    last_datatype = None

    has_unknown = False
    for expr in expressions:
        if expr.is_type(exp.DataType.Type.UNKNOWN):
            has_unknown = True
        elif expr.is_type(target_type):
            has_unknown = False
            last_datatype = target_type
            break
        else:
            last_datatype = expr.type

    self._set_type(expression, exp.DataType.Type.UNKNOWN if has_unknown else last_datatype)
    return expression


EXPRESSION_METADATA: ExpressionMetadataType = {
    **HIVE_EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.Randn,
        }
    },
    **{
        exp_type: {"returns": exp.DataType.Type.VARCHAR}
        for exp_type in {
            exp.Format,
            exp.Right,
        }
    },
    exp.AddMonths: {"returns": exp.DataType.Type.DATE},
    exp.Concat: {
        "annotator": lambda self, e: _annotate_by_similar_args(
            self, e, "expressions", target_type=exp.DataType.Type.TEXT
        )
    },
    exp.Pad: {
        "annotator": lambda self, e: _annotate_by_similar_args(
            self, e, "this", "fill_pattern", target_type=exp.DataType.Type.TEXT
        )
    },
    exp.Substring: {"annotator": lambda self, e: self._annotate_by_args(e, "this")},
}
