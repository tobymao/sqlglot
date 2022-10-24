from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.helper import list_get


def generate_date_delta(self, e):
    func = "DATEADD" if isinstance(e, exp.DateAdd) else "DATEDIFF"
    return f"{func}({self.format_args(e.text('unit'), e.expression, e.this)})"


def parse_date_delta(exp_class):
    def inner_func(args):
        spark_func = len(args) == 2
        this = list_get(args, 0) if spark_func else list_get(args, 2)
        expression = list_get(args, 1) if spark_func else list_get(args, 1)
        unit = exp.Literal.string("DAY") if spark_func else list_get(args, 0)
        return exp_class(this=this, expression=expression, unit=unit)

    return inner_func


class Databricks(Spark):
    class Tokenizer(Spark.Tokenizer):
        pass

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
            exp.DateAdd: lambda self, e: generate_date_delta(self, e),
            exp.DateDiff: lambda self, e: generate_date_delta(self, e),
        }
