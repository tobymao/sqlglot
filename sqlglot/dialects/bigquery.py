"""Supports BigQuery Standard SQL."""

from __future__ import annotations

import re
import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    datestrtodate_sql,
    inline_array_sql,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    parse_date_delta_with_interval,
    rename_func,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

E = t.TypeVar("E", bound=exp.Expression)


def _date_add_sql(
    data_type: str, kind: str
) -> t.Callable[[generator.Generator, exp.Expression], str]:
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = expression.args.get("unit")
        unit = exp.var(unit.name.upper() if unit else "DAY")
        interval = exp.Interval(this=expression.expression, unit=unit)
        return f"{data_type}_{kind}({this}, {self.sql(interval)})"

    return func


def _derived_table_values_to_unnest(self: generator.Generator, expression: exp.Values) -> str:
    if not isinstance(expression.unnest().parent, exp.From):
        return self.values_sql(expression)

    alias = expression.args.get("alias")

    structs = [
        exp.Struct(
            expressions=[
                exp.alias_(value, column_name)
                for value, column_name in zip(
                    t.expressions,
                    alias.columns
                    if alias and alias.columns
                    else (f"_c{i}" for i in range(len(t.expressions))),
                )
            ]
        )
        for t in expression.find_all(exp.Tuple)
    ]

    return self.unnest_sql(exp.Unnest(expressions=[exp.Array(expressions=structs)]))


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


def _unqualify_unnest(expression: exp.Expression) -> exp.Expression:
    """Remove references to unnest table aliases since bigquery doesn't allow them.

    These are added by the optimizer's qualify_column step.
    """
    if isinstance(expression, exp.Select):
        for unnest in expression.find_all(exp.Unnest):
            if isinstance(unnest.parent, (exp.From, exp.Join)) and unnest.alias:
                for select in expression.selects:
                    for column in select.find_all(exp.Column):
                        if column.table == unnest.alias:
                            column.set("table", None)

    return expression


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
        BYTE_STRINGS = [("b'", "'"), ("B'", "'")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ANY TYPE": TokenType.VARIANT,
            "BEGIN": TokenType.COMMAND,
            "BEGIN TRANSACTION": TokenType.BEGIN,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "DECLARE": TokenType.COMMAND,
            "FLOAT64": TokenType.DOUBLE,
            "INT64": TokenType.BIGINT,
            "BYTES": TokenType.BINARY,
            "NOT DETERMINISTIC": TokenType.VOLATILE,
            "UNKNOWN": TokenType.NULL,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        PREFIXED_PIVOT_COLUMNS = True

        LOG_BASE_FIRST = False
        LOG_DEFAULTS_TO_LN = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "DATE_TRUNC": lambda args: exp.DateTrunc(
                unit=exp.Literal.string(str(seq_get(args, 1))),
                this=seq_get(args, 0),
            ),
            "DATE_ADD": parse_date_delta_with_interval(exp.DateAdd),
            "DATETIME_ADD": parse_date_delta_with_interval(exp.DatetimeAdd),
            "DIV": lambda args: exp.IntDiv(this=seq_get(args, 0), expression=seq_get(args, 1)),
            "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
            "REGEXP_EXTRACT": lambda args: exp.RegexpExtract(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                position=seq_get(args, 2),
                occurrence=seq_get(args, 3),
                group=exp.Literal.number(1)
                if re.compile(str(seq_get(args, 1))).groups == 1
                else None,
            ),
            "TIME_ADD": parse_date_delta_with_interval(exp.TimeAdd),
            "TIMESTAMP_ADD": parse_date_delta_with_interval(exp.TimestampAdd),
            "DATE_SUB": parse_date_delta_with_interval(exp.DateSub),
            "DATETIME_SUB": parse_date_delta_with_interval(exp.DatetimeSub),
            "TIME_SUB": parse_date_delta_with_interval(exp.TimeSub),
            "TIMESTAMP_SUB": parse_date_delta_with_interval(exp.TimestampSub),
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
                exp.StabilityProperty, this=exp.Literal.string("VOLATILE")
            ),
            "OPTIONS": lambda self: self._parse_with_property(),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,  # type: ignore
            "OPTIONS": lambda self: exp.Properties(expressions=self._parse_with_property()),
        }

        def _parse_table_part(self, schema: bool = False) -> t.Optional[exp.Expression]:
            this = super()._parse_table_part(schema=schema)

            # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
            if isinstance(this, exp.Identifier):
                table_name = this.name
                while self._match(TokenType.DASH, advance=False) and self._next:
                    self._advance(2)
                    table_name += f"-{self._prev.text}"

                this = exp.Identifier(this=table_name, quoted=this.args.get("quoted"))

            return this

        def _parse_table_parts(self, schema: bool = False) -> exp.Expression:
            table = super()._parse_table_parts(schema=schema)
            if isinstance(table.this, exp.Identifier) and "." in table.name:
                table = exp.to_table(table.name, dialect="bigquery")
            return table

    class Generator(generator.Generator):
        EXPLICIT_UNION = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        LIMIT_FETCH = "LIMIT"
        RENAME_TABLE_WITH_DB = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.AtTimeZone: lambda self, e: self.func(
                "TIMESTAMP", self.func("DATETIME", e.this, e.args.get("zone"))
            ),
            exp.Cast: transforms.preprocess([transforms.remove_precision_parameterized_types]),
            exp.DateAdd: _date_add_sql("DATE", "ADD"),
            exp.DateSub: _date_add_sql("DATE", "SUB"),
            exp.DatetimeAdd: _date_add_sql("DATETIME", "ADD"),
            exp.DatetimeSub: _date_add_sql("DATETIME", "SUB"),
            exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e.args.get('unit', 'DAY'))})",
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", e.this, e.text("unit")),
            exp.GroupConcat: rename_func("STRING_AGG"),
            exp.ILike: no_ilike_sql,
            exp.IntDiv: rename_func("DIV"),
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.Select: transforms.preprocess(
                [_unqualify_unnest, transforms.eliminate_distinct_on]
            ),
            exp.StrToTime: lambda self, e: f"PARSE_TIMESTAMP({self.format_time(e)}, {self.sql(e, 'this')})",
            exp.TimeAdd: _date_add_sql("TIME", "ADD"),
            exp.TimeSub: _date_add_sql("TIME", "SUB"),
            exp.TimestampAdd: _date_add_sql("TIMESTAMP", "ADD"),
            exp.TimestampSub: _date_add_sql("TIMESTAMP", "SUB"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("bigquery"),
            exp.TsOrDsAdd: _date_add_sql("DATE", "ADD"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Values: _derived_table_values_to_unnest,
            exp.ReturnsProperty: _returnsproperty_sql,
            exp.Create: _create_sql,
            exp.Trim: lambda self, e: self.func(f"TRIM", e.this, e.expression),
            exp.StabilityProperty: lambda self, e: f"DETERMINISTIC"
            if e.name == "IMMUTABLE"
            else "NOT DETERMINISTIC",
            exp.RegexpLike: rename_func("REGEXP_CONTAINS"),
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BIGDECIMAL: "BIGNUMERIC",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.BINARY: "BYTES",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.VARBINARY: "BYTES",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.VARIANT: "ANY TYPE",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

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

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(properties, prefix=self.seg("OPTIONS"))
