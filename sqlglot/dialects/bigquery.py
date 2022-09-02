from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    inline_array_sql,
    no_ilike_sql,
    rename_func,
)
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


def _date_add_sql(data_type, kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = self.sql(expression, "unit") or "'day'"
        expression = self.sql(expression, "expression")
        return f"{data_type}_{kind}({this}, INTERVAL {expression} {unit})"

    return func


class BigQuery(Dialect):
    identifiers = ["`"]
    escape = "\\"
    unnest_column_only = True

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"', '"""']

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "CURRENT_TIME": TokenType.CURRENT_TIME,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "QUALIFY": TokenType.QUALIFY,
            "UNKNOWN": TokenType.NULL,
            "WINDOW": TokenType.WINDOW,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "DATE_ADD": _date_add(exp.DateAdd),
            "DATETIME_ADD": _date_add(exp.DatetimeAdd),
            "TIME_ADD": _date_add(exp.TimeAdd),
            "TIMESTAMP_ADD": _date_add(exp.TimestampAdd),
            "DATE_SUB": _date_add(exp.DateSub),
            "DATETIME_SUB": _date_add(exp.DatetimeSub),
            "TIME_SUB": _date_add(exp.TimeSub),
            "TIMESTAMP_SUB": _date_add(exp.TimestampSub),
        }

        NO_PAREN_FUNCTIONS = {
            **Parser.NO_PAREN_FUNCTIONS,
            TokenType.CURRENT_DATETIME: exp.CurrentDatetime,
            TokenType.CURRENT_TIME: exp.CurrentTime,
        }

    class Generator(Generator):
        TRANSFORMS = {
            exp.Array: inline_array_sql,
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.DateAdd: _date_add_sql("DATE", "ADD"),
            exp.DateSub: _date_add_sql("DATE", "SUB"),
            exp.DatetimeAdd: _date_add_sql("DATETIME", "ADD"),
            exp.DatetimeSub: _date_add_sql("DATETIME", "SUB"),
            exp.ILike: no_ilike_sql,
            exp.TimeAdd: _date_add_sql("TIME", "ADD"),
            exp.TimeSub: _date_add_sql("TIME", "SUB"),
            exp.TimestampAdd: _date_add_sql("TIMESTAMP", "ADD"),
            exp.TimestampSub: _date_add_sql("TIMESTAMP", "SUB"),
        }

        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
        }

        def in_unnest_op(self, unnest):
            return self.sql(unnest)

        def union_op(self, expression):
            return f"UNION{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def except_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT without DISTINCT is not supported in BigQuery")
            return f"EXCEPT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def intersect_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported(
                    "INTERSECT without DISTINCT is not supported in BigQuery"
                )
            return (
                f"INTERSECT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"
            )
