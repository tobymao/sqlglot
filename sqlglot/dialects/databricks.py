from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.helper import list_get


class Databricks(Spark):
    class Tokenizer(Spark.Tokenizer):
        pass

    class Parser(Spark.Parser):
        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": lambda args: exp.DateAdd(
                this=list_get(args, 2), expression=list_get(args, 1), unit=list_get(args, 0)
            ),
            "DATE_ADD": lambda args: exp.DateAdd(this=list_get(args, 2), expression=list_get(args, 1), unit="DAY"),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=list_get(args, 2), expression=list_get(args, 1), unit=list_get(args, 0)
            ),
        }

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: lambda self, e: f"DATEADD({self.sql(e,'unit')}, {self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateDiff: lambda self, e: f"DATEDIFF({self.sql(e,'unit')}, {self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        }
