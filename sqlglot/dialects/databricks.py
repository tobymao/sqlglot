from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    date_delta_sql,
    build_date_delta,
    timestamptrunc_sql,
)
from sqlglot.dialects.spark import Spark
from sqlglot.tokens import TokenType


def _timestamp_diff(
    self: Databricks.Generator, expression: exp.DatetimeDiff | exp.TimestampDiff
) -> str:
    return self.func("TIMESTAMPDIFF", expression.unit, expression.expression, expression.this)


class Databricks(Spark):
    SAFE_DIVISION = False

    class Parser(Spark.Parser):
        LOG_DEFAULTS_TO_LN = True
        STRICT_CAST = True

        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": build_date_delta(exp.DateAdd),
            "DATE_ADD": build_date_delta(exp.DateAdd),
            "DATEDIFF": build_date_delta(exp.DateDiff),
            "TIMESTAMPDIFF": build_date_delta(exp.TimestampDiff),
        }

        FACTOR = {
            **Spark.Parser.FACTOR,
            TokenType.COLON: exp.JSONExtract,
        }

    class Generator(Spark.Generator):
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"

        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DatetimeAdd: lambda self, e: self.func(
                "TIMESTAMPADD", e.unit, e.expression, e.this
            ),
            exp.DatetimeSub: lambda self, e: self.func(
                "TIMESTAMPADD",
                e.unit,
                exp.Mul(this=e.expression, expression=exp.Literal.number(-1)),
                e.this,
            ),
            exp.DatetimeDiff: _timestamp_diff,
            exp.TimestampDiff: _timestamp_diff,
            exp.DatetimeTrunc: timestamptrunc_sql,
            exp.JSONExtract: lambda self, e: self.binary(e, ":"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.unnest_to_explode,
                ]
            ),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        }

        TRANSFORMS.pop(exp.TryCast)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            constraint = expression.find(exp.GeneratedAsIdentityColumnConstraint)
            kind = expression.kind
            if (
                constraint
                and isinstance(kind, exp.DataType)
                and kind.this in exp.DataType.INTEGER_TYPES
            ):
                # only BIGINT generated identity constraints are supported
                expression.set("kind", exp.DataType.build("bigint"))

            return super().columndef_sql(expression, sep)

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            expression.set("this", True)  # trigger ALWAYS in super class
            return super().generatedasidentitycolumnconstraint_sql(expression)

    class Tokenizer(Spark.Tokenizer):
        HEX_STRINGS = []
