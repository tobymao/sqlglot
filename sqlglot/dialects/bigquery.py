from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    inline_array_sql,
    no_ilike_sql,
    rename_func,
)
from sqlglot.generator import Generator
from sqlglot.helper import list_get
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


def _derived_table_values_to_unnest(self, expression):
    if not isinstance(expression.unnest().parent, exp.From):
        return self.values_sql(expression)
    rows = [list(tuple_exp.find_all(exp.Literal)) for tuple_exp in expression.find_all(exp.Tuple)]
    structs = []
    for row in rows:
        aliases = [
            exp.alias_(value, column_name) for value, column_name in zip(row, expression.args["alias"].args["columns"])
        ]
        structs.append(exp.Struct(expressions=aliases))
    unnest_exp = exp.Unnest(expressions=[exp.Array(expressions=structs)])
    return self.unnest_sql(unnest_exp)


def _returnsproperty_sql(self, expression):
    value = expression.args.get("value")
    if isinstance(value, exp.Schema):
        value = f"{value.this} <{self.expressions(value)}>"
    else:
        value = self.sql(value)
    return f"RETURNS {value}"


def _create_sql(self, expression):
    kind = expression.args.get("kind")
    returns = expression.find(exp.ReturnsProperty)
    if kind.upper() == "FUNCTION" and returns and returns.args.get("is_table"):
        expression = expression.copy()
        expression.set("kind", "TABLE FUNCTION")
        if isinstance(
            expression.expression,
            (
                exp.Subquery,
                exp.Literal,
            ),
        ):
            expression.set("expression", expression.expression.this)

        return self.create_sql(expression)

    return self.create_sql(expression)


class BigQuery(Dialect):
    unnest_column_only = True

    class Tokenizer(Tokenizer):
        QUOTES = [
            (prefix + quote, quote) if prefix else quote
            for quote in ["'", '"', '"""', "'''"]
            for prefix in ["", "r", "R"]
        ]
        IDENTIFIERS = ["`"]
        ESCAPE = "\\"
        HEX_STRINGS = [("0x", ""), ("0X", "")]

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
            "NOT DETERMINISTIC": TokenType.VOLATILE,
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

        NESTED_TYPE_TOKENS = {
            *Parser.NESTED_TYPE_TOKENS,
            TokenType.TABLE,
        }

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.DateAdd: _date_add_sql("DATE", "ADD"),
            exp.DateSub: _date_add_sql("DATE", "SUB"),
            exp.DatetimeAdd: _date_add_sql("DATETIME", "ADD"),
            exp.DatetimeSub: _date_add_sql("DATETIME", "SUB"),
            exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e.args.get('unit', 'DAY'))})",
            exp.ILike: no_ilike_sql,
            exp.TimeAdd: _date_add_sql("TIME", "ADD"),
            exp.TimeSub: _date_add_sql("TIME", "SUB"),
            exp.TimestampAdd: _date_add_sql("TIMESTAMP", "ADD"),
            exp.TimestampSub: _date_add_sql("TIMESTAMP", "SUB"),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Values: _derived_table_values_to_unnest,
            exp.ReturnsProperty: _returnsproperty_sql,
            exp.Create: _create_sql,
            exp.VolatilityProperty: lambda self, e: f"DETERMINISTIC" if e.name == "IMMUTABLE" else "NOT DETERMINISTIC",
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

        ROOT_PROPERTIES = {
            exp.LanguageProperty,
            exp.ReturnsProperty,
            exp.VolatilityProperty,
        }

        WITH_PROPERTIES = {
            exp.AnonymousProperty,
        }

        EXPLICIT_UNION = True

        def in_unnest_op(self, unnest):
            return self.sql(unnest)

        def except_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT without DISTINCT is not supported in BigQuery")
            return f"EXCEPT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def intersect_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("INTERSECT without DISTINCT is not supported in BigQuery")
            return f"INTERSECT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"
