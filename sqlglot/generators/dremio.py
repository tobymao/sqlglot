from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot import generator
from sqlglot.dialects.dialect import (
    rename_func,
    no_trycast_sql,
)

DATE_DELTA = t.Union[exp.DateAdd, exp.DateSub]


def _date_delta_sql(name: str) -> t.Callable[[DremioGenerator, DATE_DELTA], str]:
    def _delta_sql(self: DremioGenerator, expression: DATE_DELTA) -> str:
        unit = expression.text("unit").upper()

        # Fallback to default behavior if unit is missing or 'DAY'
        if not unit or unit == "DAY":
            return self.func(name, expression.this, expression.expression)

        this_sql = self.sql(expression, "this")
        expr_sql = self.sql(expression, "expression")

        interval_sql = f"CAST({expr_sql} AS INTERVAL {unit})"
        return f"{name}({this_sql}, {interval_sql})"

    return _delta_sql


class DremioGenerator(generator.Generator):
    NVL2_SUPPORTED = False
    SUPPORTS_CONVERT_TIMEZONE = True
    INTERVAL_ALLOWS_PLURAL_FORM = False
    JOIN_HINTS = False
    LIMIT_ONLY_LITERALS = True
    MULTI_ARG_DISTINCT = False
    SUPPORTS_BETWEEN_FLAGS = True

    # https://docs.dremio.com/current/reference/sql/data-types/
    TYPE_MAPPING = {
        **generator.Generator.TYPE_MAPPING,
        exp.DType.SMALLINT: "INT",
        exp.DType.TINYINT: "INT",
        exp.DType.BINARY: "VARBINARY",
        exp.DType.TEXT: "VARCHAR",
        exp.DType.NCHAR: "VARCHAR",
        exp.DType.CHAR: "VARCHAR",
        exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
        exp.DType.DATETIME: "TIMESTAMP",
        exp.DType.ARRAY: "LIST",
        exp.DType.BIT: "BOOLEAN",
    }

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.BitwiseAndAgg: rename_func("BIT_AND"),
        exp.BitwiseOrAgg: rename_func("BIT_OR"),
        exp.ToChar: rename_func("TO_CHAR"),
        exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
        exp.TryCast: no_trycast_sql,
        exp.DateAdd: _date_delta_sql("DATE_ADD"),
        exp.DateSub: _date_delta_sql("DATE_SUB"),
        exp.GenerateSeries: rename_func("ARRAY_GENERATE_RANGE"),
    }

    def datatype_sql(self, expression: exp.DataType) -> str:
        """
        Reject time-zone-aware TIMESTAMPs, which Dremio does not accept
        """
        if expression.is_type(
            exp.DType.TIMESTAMPTZ,
            exp.DType.TIMESTAMPLTZ,
        ):
            self.unsupported("Dremio does not support time-zone-aware TIMESTAMP")

        return super().datatype_sql(expression)

    def cast_sql(self, expression: exp.Cast, safe_prefix: str | None = None) -> str:
        # Match: CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)
        if expression.is_type(exp.DType.DATE):
            at_time_zone = expression.this

            if (
                isinstance(at_time_zone, exp.AtTimeZone)
                and isinstance(at_time_zone.this, exp.CurrentTimestamp)
                and isinstance(at_time_zone.args["zone"], exp.Literal)
                and at_time_zone.text("zone").upper() == "UTC"
            ):
                return "CURRENT_DATE_UTC"

        return super().cast_sql(expression, safe_prefix)
