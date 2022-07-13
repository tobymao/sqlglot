from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.helper import list_get
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


def _date_add(expression_class):
    def func(args):
        interval = list_get(args, 1)
        return expression_class(
            this=list_get(args, 0),
            expression=interval.this,
            unit=interval.args.get("unit"),
        )

    return func


def _date_add_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = self.sql(expression, "unit")
        expression = self.sql(expression, "expression")
        return f"DATE_{kind}({this}, INTERVAL {expression} {unit})"

    return func


class BigQuery(Dialect):
    identifier = "`"

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"', '"""']

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "UNKNOWN": TokenType.NULL,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "DATE_ADD": _date_add(exp.DateAdd),
            "DATE_SUB": _date_add(exp.DateSub),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=list_get(args, 0),
                expression=list_get(args, 1),
                unit=exp.DateTimePart.build(str(list_get(args, 2))),
            ),
            "DATETIME_DIFF": lambda args: exp.DateTimeDiff(
                this=list_get(args, 0),
                expression=list_get(args, 1),
                unit=exp.DateTimePart.build(str(list_get(args, 2))),
            ),
        }

    class Generator(Generator):
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateDiff: lambda self, e: f"""DATE_DIFF({self.sql(e, 'expression')}, {self.sql(e, 'this')}, {exp.DateTimePart.build(str(self.sql(e, 'unit')))})""",
            exp.DateTimeDiff: lambda self, e: f"""DATETIME_DIFF({self.sql(e, 'expression')}, {self.sql(e, 'this')}, {exp.DateTimePart.build(str(self.sql(e, 'unit')))})""",
        }

        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.TEXT: "STRING",
        }
