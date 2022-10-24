from sqlglot import exp
from sqlglot.dialects.spark import Spark


class Databricks(Spark):
    class Tokenizer(Spark.Tokenizer):
        pass

    class Parser(Spark.Parser):
        FUNCTIONS = {**Spark.Parser.FUNCTIONS}

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        }
