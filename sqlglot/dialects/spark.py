from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.spark2 import Spark2
from sqlglot.helper import seq_get


def _parse_datediff(args: t.List) -> exp.Expression:
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
            "ANY_VALUE": lambda args: exp.AnyValue(
                this=seq_get(args, 0), ignore_nulls=seq_get(args, 1)
            ),
            "DATEDIFF": _parse_datediff,
        }

        FUNCTION_PARSERS = Spark2.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("ANY_VALUE")

    class Generator(Spark2.Generator):
        TYPE_MAPPING = {
            **Spark2.Generator.TYPE_MAPPING,
            exp.DataType.Type.MONEY: "DECIMAL(15, 4)",
            exp.DataType.Type.SMALLMONEY: "DECIMAL(6, 4)",
            exp.DataType.Type.UNIQUEIDENTIFIER: "STRING",
        }

        TRANSFORMS = {
            **Spark2.Generator.TRANSFORMS,
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.TimestampAdd: lambda self, e: self.func(
                "DATEADD", e.args.get("unit") or "DAY", e.expression, e.this
            ),
        }
        TRANSFORMS.pop(exp.AnyValue)
        TRANSFORMS.pop(exp.DateDiff)
        TRANSFORMS.pop(exp.Group)

        def anyvalue_sql(self, expression: exp.AnyValue) -> str:
            return self.function_fallback_sql(expression)

        def datediff_sql(self, expression: exp.DateDiff) -> str:
            unit = self.sql(expression, "unit")
            end = self.sql(expression, "this")
            start = self.sql(expression, "expression")

            if unit:
                return self.func("DATEDIFF", unit, start, end)

            return self.func("DATEDIFF", end, start)
