from __future__ import annotations

import typing as t

from sqlglot import exp, parser, transforms
from sqlglot.dialects.dialect import parse_date_delta, timestamptrunc_sql
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.tsql import generate_date_delta_with_unit_sql
from sqlglot.tokens import TokenType


class Databricks(Spark):
    class Parser(Spark.Parser):
        LOG_DEFAULTS_TO_LN = True

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

        def _parse_generated(self, kind: t.Optional[str] = None) -> exp.ComputedColumnConstraint:
            self._match_text_seq("ALWAYS")
            self._match(TokenType.ALIAS)
            this = self._parse_expression()

            return self.expression(
                exp.ComputedColumnConstraint,
                this=this,
            )

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "GENERATED": lambda self: self._parse_generated(),
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
                exp.Mul(this=e.expression.copy(), expression=exp.Literal.number(-1)),
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

        def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
            this = self.sql(expression, "this")
            return f"GENERATED ALWAYS AS {this}"

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            c = expression.find(exp.GeneratedAsIdentityColumnConstraint)
            kind = self.sql(expression, "kind")
            if c and isinstance(expression.this, exp.Identifier) and kind == "INT":
                # only BIGINT GENERATED ALWAYS AS IDENTITY constraints are supported, upcast to BIGINT
                expression = expression.copy()
                expression.set("kind", "BIGINT")
            return super().columndef_sql(expression, sep)

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            expression = expression.copy()
            expression.set("this", True)  # trigger ALWAYS in super class
            return super().generatedasidentitycolumnconstraint_sql(expression)

    class Tokenizer(Spark.Tokenizer):
        HEX_STRINGS = []

        SINGLE_TOKENS = {
            **Spark.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }
