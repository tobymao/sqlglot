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


def _datetime_trunc(expression_class):
    def func(args):
        return expression_class(
            this=list_get(args, 0),
            unit=exp.DateTimePart.build(str(list_get(args, 1))),
            zone=list_get(args, 2),
        )

    return func


def _datetime_trunc_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = exp.DateTimePart.build(str(self.sql(expression, "unit")))
        zone = self.sql(expression, "zone")
        zone = ", " + zone if zone else ""
        return f"{kind}_TRUNC({this}, {unit}{zone})"

    return func


def _datetime_diff(expression_class):
    def func(args):
        return expression_class(
            this=list_get(args, 0),
            expression=list_get(args, 1),
            unit=exp.DateTimePart.build(str(list_get(args, 2))),
        )

    return func


def _datetime_diff_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        e = self.sql(expression, "expression")
        unit = self.sql(expression, "unit")
        unit = exp.DateTimePart.build(str(unit))
        return f"{kind}_DIFF({this}, {e}, {unit})"

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
            "DATE_DIFF": _datetime_diff(exp.DateDiff),
            "DATETIME_DIFF": _datetime_diff(exp.DateTimeDiff),
            "TIMESTAMP_DIFF": _datetime_diff(exp.TimestampDiff),
            "TIMESTAMP_TRUNC": _datetime_trunc(exp.TimestampTrunc),
            "DATE_TRUNC": _datetime_trunc(exp.DateTrunc),
            "DATETIME_TRUNC": _datetime_trunc(exp.DateTimeTrunc),
            "TIME_TRUNC": _datetime_trunc(exp.TimeTrunc),
        }

    class Generator(Generator):
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateDiff: _datetime_diff_sql("DATE"),
            exp.DateTimeDiff: _datetime_diff_sql("DATETIME"),
            exp.TimestampDiff: _datetime_diff_sql("TIMESTAMP"),
            exp.TimestampTrunc: _datetime_trunc_sql("TIMESTAMP"),
            exp.DateTrunc: _datetime_trunc_sql("DATE"),
            exp.DateTimeTrunc: _datetime_trunc_sql("DATETIME"),
            exp.TimeTrunc: _datetime_trunc_sql("TIME"),
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
