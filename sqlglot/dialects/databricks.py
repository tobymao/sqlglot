from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import parse_date_delta, timestamptrunc_sql
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.tsql import generate_date_delta_with_unit_sql
from sqlglot.tokens import TokenType


class Databricks(Spark):
    SAFE_DIVISION = False

    class Parser(Spark.Parser):
        LOG_DEFAULTS_TO_LN = True
        STRICT_CAST = True

        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": parse_date_delta(exp.DateAdd),
            "DATE_ADD": parse_date_delta(exp.DateAdd),
            "DATEDIFF": parse_date_delta(exp.DateDiff),
        }

        FACTOR = {
            **Spark.Parser.FACTOR,
            TokenType.COLON: exp.JSONExtract,
        }

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: generate_date_delta_with_unit_sql,
            exp.DateDiff: generate_date_delta_with_unit_sql,
            exp.DatetimeAdd: lambda self, e: self.func(
                "TIMESTAMPADD", e.text("unit"), e.expression, e.this
            ),
            exp.DatetimeSub: lambda self, e: self.func(
                "TIMESTAMPADD",
                e.text("unit"),
                exp.Mul(this=e.expression, expression=exp.Literal.number(-1)),
                e.this,
            ),
            exp.DatetimeDiff: lambda self, e: self.func(
                "TIMESTAMPDIFF", e.text("unit"), e.expression, e.this
            ),
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
            kind = expression.args.get("kind")
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
