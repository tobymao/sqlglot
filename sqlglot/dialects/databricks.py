from sqlglot import exp
from sqlglot.dialects.dialect import generate_tsql_date_delta, parse_date_delta
from sqlglot.dialects.spark import Spark


class Databricks(Spark):
    class Parser(Spark.Parser):
        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": parse_date_delta(exp.DateAdd),
            "DATE_ADD": parse_date_delta(exp.DateAdd),
            "DATEDIFF": parse_date_delta(exp.DateDiff),
        }

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: lambda self, e: generate_tsql_date_delta(self, e),
            exp.DateDiff: lambda self, e: generate_tsql_date_delta(self, e),
        }
