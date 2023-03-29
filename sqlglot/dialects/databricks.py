from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import parse_date_delta
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.tsql import generate_date_delta_with_unit_sql
from sqlglot.tokens import TokenType


class Databricks(Spark):
    class Parser(Spark.Parser):
        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": parse_date_delta(exp.DateAdd),
            "DATE_ADD": parse_date_delta(exp.DateAdd),
            "DATEDIFF": parse_date_delta(exp.DateDiff),
        }

        LOG_DEFAULTS_TO_LN = True

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,  # type: ignore
            exp.DateAdd: generate_date_delta_with_unit_sql,
            exp.DateDiff: generate_date_delta_with_unit_sql,
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        }
        TRANSFORMS.pop(exp.Select)  # Remove the ELIMINATE_QUALIFY transformation

        PARAMETER_TOKEN = "$"

    class Tokenizer(Spark.Tokenizer):
        SINGLE_TOKENS = {
            **Spark.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }
