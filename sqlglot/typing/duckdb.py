from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


DATETIME_WITHOUT_TZ = {
    exp.DType.DATE,
    exp.DType.TIMESTAMP,
    exp.DType.TIMESTAMPNTZ,
    exp.DType.TIMESTAMP_S,
    exp.DType.TIMESTAMP_MS,
    exp.DType.TIMESTAMP_NS,
}


def _annotate_at_time_zone(self: TypeAnnotator, expression: exp.AtTimeZone) -> exp.Expr:
    this = expression.this

    if this.is_type(*DATETIME_WITHOUT_TZ):
        return self._set_type(expression, exp.DType.TIMESTAMPTZ)
    if this.is_type(exp.DType.TIMESTAMPTZ):
        return self._set_type(expression, exp.DType.TIMESTAMP)
    if this.is_type(exp.DType.TIMETZ):
        return self._set_type(expression, exp.DType.TIMETZ)

    return self._set_type(expression, exp.DType.UNKNOWN)


def _annotate_date_trunc(
    self: TypeAnnotator, expression: exp.DateTrunc | exp.TimestampTrunc
) -> exp.Expr:
    if expression.this.is_type(
        exp.DType.DATE,
        exp.DType.TIMESTAMP_S,
        exp.DType.TIMESTAMP_MS,
        exp.DType.TIMESTAMP_NS,
    ):
        return self._set_type(expression, exp.DType.TIMESTAMP)

    return self._set_type(expression, expression.this.type)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.BIGINT}
        for expr_type in {
            exp.BitLength,
            exp.DateDiff,
            exp.Day,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfWeekIso,
            exp.DayOfYear,
            exp.Extract,
            exp.Hour,
            exp.Length,
            exp.Minute,
            exp.Month,
            exp.Quarter,
            exp.Second,
            exp.Week,
            exp.Year,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT128}
        for expr_type in {
            exp.CountIf,
            exp.Factorial,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.JarowinklerSimilarity,
            exp.TimeToUnix,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.Format,
            exp.Reverse,
            exp.Decode,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Encode,
            exp.Unhex,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.BitwiseAndAgg,
        }
    },
    exp.AtTimeZone: {"annotator": _annotate_at_time_zone},
    exp.DateBin: {"annotator": lambda self, e: self._annotate_by_args(e, "expression")},
    exp.DateTrunc: {"annotator": _annotate_date_trunc},
    exp.PercentileDisc: {"annotator": lambda self, e: self._annotate_by_args(e, "this")},
    exp.TimestampTrunc: {"annotator": _annotate_date_trunc},
    exp.Localtimestamp: {"returns": exp.DType.TIMESTAMP},
    exp.ToDays: {"returns": exp.DType.INTERVAL},
    exp.TimeFromParts: {"returns": exp.DType.TIME},
    exp.TimestampFromParts: {"returns": exp.DType.TIMESTAMP},
}
