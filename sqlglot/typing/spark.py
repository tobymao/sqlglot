from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _decimal(p: int, s: int) -> exp.DataType:
    return exp.DataType(
        this=exp.DType.DECIMAL,
        expressions=[
            exp.DataTypeParam(this=exp.Literal.number(p)),
            exp.DataTypeParam(this=exp.Literal.number(s)),
        ],
    )


def _decimal_ps(datatype: exp.DataType) -> tuple[int, int] | None:
    if datatype and datatype.is_type(exp.DType.DECIMAL) and datatype.expressions:
        return int(datatype.expressions[0].name), int(datatype.expressions[1].name)
    return None


def _round_literal_int(expr: exp.Expression) -> int | None:
    if isinstance(expr, exp.Literal) and expr.is_int:
        return int(expr.to_py())
    if isinstance(expr, exp.Neg) and isinstance(expr.this, exp.Literal) and expr.this.is_int:
        return -int(expr.this.to_py())
    return None


def _annotate_round(self: TypeAnnotator, expression: exp.Round) -> exp.Round:
    ps = _decimal_ps(expression.this.type)
    if ps is None:
        return self._annotate_by_args(expression, "this")
    p, s = ps
    d_expr = expression.args.get("decimals")
    d = _round_literal_int(d_expr) if d_expr is not None else 0
    if d is None or d < 0:
        return self._set_type(expression, _decimal(min(38, max(p - s + 1, -(d or 0) + 1)), 0))
    d_eff = min(s, d)
    return self._set_type(expression, _decimal(min(38, p - s + 1 + d_eff), d_eff))


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DType.DOUBLE}
        for exp_type in {
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DType.INT}
        for exp_type in {
            exp.ArraySize,
        }
    },
    **{
        exp_type: {"returns": exp.DType.VARCHAR}
        for exp_type in {
            exp.Collation,
            exp.CurrentTimezone,
            exp.Randstr,
        }
    },
    **{
        exp_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for exp_type in {
            exp.ArrayCompact,
            exp.ArrayInsert,
            exp.BitwiseAndAgg,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
            exp.Overlay,
        }
    },
    exp.BitmapCount: {"returns": exp.DType.BIGINT},
    exp.Localtimestamp: {"returns": exp.DType.TIMESTAMPNTZ},
    exp.ToBinary: {"returns": exp.DType.BINARY},
    exp.DateFromUnixDate: {"returns": exp.DType.DATE},
    # 2-arg `date_add(startDate, numDays)` / `date_sub` are routed to
    # TsOrDsAdd by Hive/Spark parsers; both return DATE per the Spark
    # and Databricks contracts.
    exp.TsOrDsAdd: {"returns": exp.DType.DATE},
    exp.Round: {"annotator": _annotate_round},
}
