"""Supports BigQuery Standard SQL."""

from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    datestrtodate_sql,
    inline_array_sql,
    no_ilike_sql,
    rename_func,
    timestrtotime_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

E = t.TypeVar("E", bound=exp.Expression)


def _date_add(expression_class: t.Type[E]) -> t.Callable[[t.Sequence], E]:
    def func(args):
        interval = seq_get(args, 1)
        return expression_class(
            this=seq_get(args, 0),
            expression=interval.this,
            unit=interval.args.get("unit"),
        )

    return func


def _date_trunc(args: t.Sequence) -> exp.Expression:
    unit = seq_get(args, 1)
    if isinstance(unit, exp.Column):
        unit = exp.Var(this=unit.name)
    return exp.DateTrunc(this=seq_get(args, 0), expression=unit)


def _date_add_sql(
    data_type: str, kind: str
) -> t.Callable[[generator.Generator, exp.Expression], str]:
    def func(self, expression):
        this = self.sql(expression, "this")
        return f"{data_type}_{kind}({this}, {self.sql(exp.Interval(this=expression.expression, unit=expression.args.get('unit') or exp.Literal.string('day')))})"

    return func


def _derived_table_values_to_unnest(self: generator.Generator, expression: exp.Values) -> str:
    if not isinstance(expression.unnest().parent, exp.From):
        expression = t.cast(exp.Values, transforms.remove_precision_parameterized_types(expression))
        return self.values_sql(expression)
    rows = [tuple_exp.expressions for tuple_exp in expression.find_all(exp.Tuple)]
    structs = []
    for row in rows:
        aliases = [
            exp.alias_(value, column_name)
            for value, column_name in zip(row, expression.args["alias"].args["columns"])
        ]
        structs.append(exp.Struct(expressions=aliases))
    unnest_exp = exp.Unnest(expressions=[exp.Array(expressions=structs)])
    return self.unnest_sql(unnest_exp)


def _returnsproperty_sql(self: generator.Generator, expression: exp.ReturnsProperty) -> str:
    this = expression.this
    if isinstance(this, exp.Schema):
        this = f"{this.this} <{self.expressions(this)}>"
    else:
        this = self.sql(this)
    return f"RETURNS {this}"


def _create_sql(self: generator.Generator, expression: exp.Create) -> str:
    kind = expression.args["kind"]
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
    time_mapping = {
        "%M": "%-M",
        "%d": "%-d",
        "%m": "%-m",
        "%y": "%-y",
        "%H": "%-H",
        "%I": "%-I",
        "%S": "%-S",
        "%j": "%-j",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = [
            (prefix + quote, quote) if prefix else quote
            for quote in ["'", '"', '"""', "'''"]
            for prefix in ["", "r", "R"]
        ]
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]
        HEX_STRINGS = [("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BEGIN": TokenType.COMMAND,
            "BEGIN TRANSACTION": TokenType.BEGIN,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "CURRENT_TIME": TokenType.CURRENT_TIME,
            "DECLARE": TokenType.COMMAND,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "FLOAT64": TokenType.DOUBLE,
            "INT64": TokenType.BIGINT,
            "NOT DETERMINISTIC": TokenType.VOLATILE,
            "UNKNOWN": TokenType.NULL,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "DATE_TRUNC": _date_trunc,
            "DATE_ADD": _date_add(exp.DateAdd),
            "DATETIME_ADD": _date_add(exp.DatetimeAdd),
            "DIV": lambda args: exp.IntDiv(this=seq_get(args, 0), expression=seq_get(args, 1)),
            "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
            "TIME_ADD": _date_add(exp.TimeAdd),
            "TIMESTAMP_ADD": _date_add(exp.TimestampAdd),
            "DATE_SUB": _date_add(exp.DateSub),
            "DATETIME_SUB": _date_add(exp.DatetimeSub),
            "TIME_SUB": _date_add(exp.TimeSub),
            "TIMESTAMP_SUB": _date_add(exp.TimestampSub),
            "PARSE_TIMESTAMP": lambda args: exp.StrToTime(
                this=seq_get(args, 1), format=seq_get(args, 0)
            ),
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,  # type: ignore
            "ARRAY": lambda self: self.expression(exp.Array, expressions=[self._parse_statement()]),
        }
        FUNCTION_PARSERS.pop("TRIM")

        NO_PAREN_FUNCTIONS = {
            **parser.Parser.NO_PAREN_FUNCTIONS,  # type: ignore
            TokenType.CURRENT_DATETIME: exp.CurrentDatetime,
            TokenType.CURRENT_TIME: exp.CurrentTime,
        }

        NESTED_TYPE_TOKENS = {
            *parser.Parser.NESTED_TYPE_TOKENS,  # type: ignore
            TokenType.TABLE,
        }

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,  # type: ignore
            TokenType.VALUES,
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,  # type: ignore
            "NOT DETERMINISTIC": lambda self: self.expression(
                exp.VolatilityProperty, this=exp.Literal.string("VOLATILE")
            ),
        }

    class Generator(generator.Generator):
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            **transforms.REMOVE_PRECISION_PARAMETERIZED_TYPES,  # type: ignore
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.DateAdd: _date_add_sql("DATE", "ADD"),
            exp.DateSub: _date_add_sql("DATE", "SUB"),
            exp.DatetimeAdd: _date_add_sql("DATETIME", "ADD"),
            exp.DatetimeSub: _date_add_sql("DATETIME", "SUB"),
            exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e.args.get('unit', 'DAY'))})",
            exp.DateStrToDate: datestrtodate_sql,
            exp.GroupConcat: rename_func("STRING_AGG"),
            exp.ILike: no_ilike_sql,
            exp.IntDiv: rename_func("DIV"),
            exp.StrToTime: lambda self, e: f"PARSE_TIMESTAMP({self.format_time(e)}, {self.sql(e, 'this')})",
            exp.TimeAdd: _date_add_sql("TIME", "ADD"),
            exp.TimeSub: _date_add_sql("TIME", "SUB"),
            exp.TimestampAdd: _date_add_sql("TIMESTAMP", "ADD"),
            exp.TimestampSub: _date_add_sql("TIMESTAMP", "SUB"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Values: _derived_table_values_to_unnest,
            exp.ReturnsProperty: _returnsproperty_sql,
            exp.Create: _create_sql,
            exp.Trim: lambda self, e: f"TRIM({self.format_args(e.this, e.expression)})",
            exp.VolatilityProperty: lambda self, e: f"DETERMINISTIC"
            if e.name == "IMMUTABLE"
            else "NOT DETERMINISTIC",
            exp.RegexpLike: rename_func("REGEXP_CONTAINS"),
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
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

        EXPLICIT_UNION = True

        def array_sql(self, expression: exp.Array) -> str:
            first_arg = seq_get(expression.expressions, 0)
            if isinstance(first_arg, exp.Subqueryable):
                return f"ARRAY{self.wrap(self.sql(first_arg))}"

            return inline_array_sql(self, expression)

        def transaction_sql(self, *_) -> str:
            return "BEGIN TRANSACTION"

        def commit_sql(self, *_) -> str:
            return "COMMIT TRANSACTION"

        def rollback_sql(self, *_) -> str:
            return "ROLLBACK TRANSACTION"

        def in_unnest_op(self, expression: exp.Unnest) -> str:
            return self.sql(expression)

        def except_op(self, expression: exp.Except) -> str:
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT without DISTINCT is not supported in BigQuery")
            return f"EXCEPT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def intersect_op(self, expression: exp.Intersect) -> str:
            if not expression.args.get("distinct", False):
                self.unsupported("INTERSECT without DISTINCT is not supported in BigQuery")
            return f"INTERSECT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"
