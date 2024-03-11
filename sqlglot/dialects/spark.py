from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.hive import _build_with_ignore_nulls
from sqlglot.dialects.spark2 import Spark2, temporary_storage_provider
from sqlglot.helper import seq_get
from sqlglot.transforms import (
    ctas_with_tmp_tables_to_create_tmp_view,
    remove_unique_constraints,
    preprocess,
    move_partitioned_by_to_schema_columns,
)


def _build_datediff(args: t.List) -> exp.Expression:
    """
    Although Spark docs don't mention the "unit" argument, Spark3 added support for
    it at some point. Databricks also supports this variant (see below).

    For example, in spark-sql (v3.3.1):
    - SELECT DATEDIFF('2020-01-01', '2020-01-05') results in -4
    - SELECT DATEDIFF(day, '2020-01-01', '2020-01-05') results in 4

    See also:
    - https://docs.databricks.com/sql/language-manual/functions/datediff3.html
    - https://docs.databricks.com/sql/language-manual/functions/datediff.html
    """
    unit = None
    this = seq_get(args, 0)
    expression = seq_get(args, 1)

    if len(args) == 3:
        unit = this
        this = args[2]

    return exp.DateDiff(
        this=exp.TsOrDsToDate(this=this), expression=exp.TsOrDsToDate(this=expression), unit=unit
    )


def _normalize_partition(e: exp.Expression) -> exp.Expression:
    """Normalize the expressions in PARTITION BY (<expression>, <expression>, ...)"""
    if isinstance(e, str):
        return exp.to_identifier(e)
    if isinstance(e, exp.Literal):
        return exp.to_identifier(e.name)
    return e


class Spark(Spark2):
    class Tokenizer(Spark2.Tokenizer):
        RAW_STRINGS = [
            (prefix + q, q)
            for q in t.cast(t.List[str], Spark2.Tokenizer.QUOTES)
            for prefix in ("r", "R")
        ]

    class Parser(Spark2.Parser):
        FUNCTIONS = {
            **Spark2.Parser.FUNCTIONS,
            "ANY_VALUE": _build_with_ignore_nulls(exp.AnyValue),
            "DATEDIFF": _build_datediff,
        }

        def _parse_generated_as_identity(
            self,
        ) -> (
            exp.GeneratedAsIdentityColumnConstraint
            | exp.ComputedColumnConstraint
            | exp.GeneratedAsRowColumnConstraint
        ):
            this = super()._parse_generated_as_identity()
            if this.expression:
                return self.expression(exp.ComputedColumnConstraint, this=this.expression)
            return this

    class Generator(Spark2.Generator):
        SUPPORTS_TO_NUMBER = True

        TYPE_MAPPING = {
            **Spark2.Generator.TYPE_MAPPING,
            exp.DataType.Type.MONEY: "DECIMAL(15, 4)",
            exp.DataType.Type.SMALLMONEY: "DECIMAL(6, 4)",
            exp.DataType.Type.UNIQUEIDENTIFIER: "STRING",
        }

        TRANSFORMS = {
            **Spark2.Generator.TRANSFORMS,
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    lambda e: ctas_with_tmp_tables_to_create_tmp_view(
                        e, temporary_storage_provider
                    ),
                    move_partitioned_by_to_schema_columns,
                ]
            ),
            exp.PartitionedByProperty: lambda self,
            e: f"PARTITIONED BY {self.wrap(self.expressions(sqls=[_normalize_partition(e) for e in e.this.expressions], skip_first=True))}",
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.TimestampAdd: lambda self, e: self.func(
                "DATEADD", e.args.get("unit") or "DAY", e.expression, e.this
            ),
            exp.TryCast: lambda self, e: (
                self.trycast_sql(e) if e.args.get("safe") else self.cast_sql(e)
            ),
        }
        TRANSFORMS.pop(exp.AnyValue)
        TRANSFORMS.pop(exp.DateDiff)
        TRANSFORMS.pop(exp.Group)

        def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
            return f"GENERATED ALWAYS AS ({self.sql(expression, 'this')})"

        def anyvalue_sql(self, expression: exp.AnyValue) -> str:
            return self.function_fallback_sql(expression)

        def datediff_sql(self, expression: exp.DateDiff) -> str:
            unit = self.sql(expression, "unit")
            end = self.sql(expression, "this")
            start = self.sql(expression, "expression")

            if unit:
                return self.func("DATEDIFF", unit, start, end)

            return self.func("DATEDIFF", end, start)
