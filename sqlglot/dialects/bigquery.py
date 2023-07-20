from __future__ import annotations

import logging
import re
import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot._typing import E
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    datestrtodate_sql,
    format_time_lambda,
    inline_array_sql,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    parse_date_delta_with_interval,
    regexp_replace_sql,
    rename_func,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
)
from sqlglot.helper import seq_get, split_num_words
from sqlglot.tokens import TokenType

logger = logging.getLogger("sqlglot")


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
    if not expression.find_ancestor(exp.From, exp.Join):
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
    from sqlglot.optimizer.scope import Scope

    if isinstance(expression, exp.Select):
        for unnest in expression.find_all(exp.Unnest):
            if isinstance(unnest.parent, (exp.From, exp.Join)) and unnest.alias:
                for column in Scope(expression).find_all(exp.Column):
                    if column.table == unnest.alias:
                        column.set("table", None)

    return expression


# https://issuetracker.google.com/issues/162294746
# workaround for bigquery bug when grouping by an expression and then ordering
# WITH x AS (SELECT 1 y)
# SELECT y + 1 z
# FROM x
# GROUP BY x + 1
# ORDER by z
def _alias_ordered_group(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        group = expression.args.get("group")
        order = expression.args.get("order")

        if group and order:
            aliases = {
                select.this: select.args["alias"]
                for select in expression.selects
                if isinstance(select, exp.Alias)
            }

            for e in group.expressions:
                alias = aliases.get(e)

                if alias:
                    e.replace(exp.column(alias))

    return expression


def _pushdown_cte_column_names(expression: exp.Expression) -> exp.Expression:
    """BigQuery doesn't allow column names when defining a CTE, so we try to push them down."""
    if isinstance(expression, exp.CTE) and expression.alias_column_names:
        cte_query = expression.this

        if cte_query.is_star:
            logger.warning(
                "Can't push down CTE column names for star queries. Run the query through"
                " the optimizer or use 'qualify' to expand the star projections first."
            )
            return expression

        column_names = expression.alias_column_names
        expression.args["alias"].set("columns", None)

        for name, select in zip(column_names, cte_query.selects):
            to_replace = select

            if isinstance(select, exp.Alias):
                select = select.this

            # Inner aliases are shadowed by the CTE column names
            to_replace.replace(exp.alias_(select, name))

    return expression


def _parse_timestamp(args: t.List) -> exp.StrToTime:
    this = format_time_lambda(exp.StrToTime, "bigquery")([seq_get(args, 1), seq_get(args, 0)])
    this.set("zone", seq_get(args, 2))
    return this


def _parse_date(args: t.List) -> exp.Date | exp.DateFromParts:
    expr_type = exp.DateFromParts if len(args) == 3 else exp.Date
    return expr_type.from_arg_list(args)


def _parse_to_hex(args: t.List) -> exp.Hex | exp.MD5:
    # TO_HEX(MD5(..)) is common in BigQuery, so it's parsed into MD5 to simplify its transpilation
    arg = seq_get(args, 0)
    return exp.MD5(this=arg.this) if isinstance(arg, exp.MD5Digest) else exp.Hex(this=arg)


class BigQuery(Dialect):
    UNNEST_COLUMN_ONLY = True

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = None

    # bigquery udfs are case sensitive
    NORMALIZE_FUNCTIONS = False

    TIME_MAPPING = {
        "%D": "%m/%d/%y",
    }

    FORMAT_MAPPING = {
        "DD": "%d",
        "MM": "%m",
        "MON": "%b",
        "MONTH": "%B",
        "YYYY": "%Y",
        "YY": "%y",
        "HH": "%I",
        "HH12": "%I",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
        "SSSSS": "%f",
        "TZH": "%z",
    }

    @classmethod
    def normalize_identifier(cls, expression: E) -> E:
        # In BigQuery, CTEs aren't case-sensitive, but table names are (by default, at least).
        # The following check is essentially a heuristic to detect tables based on whether or
        # not they're qualified.
        if isinstance(expression, exp.Identifier):
            parent = expression.parent

            while isinstance(parent, exp.Dot):
                parent = parent.parent

            if (
                not isinstance(parent, exp.UserDefinedFunction)
                and not (isinstance(parent, exp.Table) and parent.db)
                and not expression.meta.get("is_table")
            ):
                expression.set("this", expression.this.lower())

        return expression

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"', '"""', "'''"]
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        HEX_STRINGS = [("0x", ""), ("0X", "")]

        BYTE_STRINGS = [
            (prefix + q, q) for q in t.cast(t.List[str], QUOTES) for prefix in ("b", "B")
        ]

        RAW_STRINGS = [
            (prefix + q, q) for q in t.cast(t.List[str], QUOTES) for prefix in ("r", "R")
        ]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ANY TYPE": TokenType.VARIANT,
            "BEGIN": TokenType.COMMAND,
            "BEGIN TRANSACTION": TokenType.BEGIN,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "BYTES": TokenType.BINARY,
            "DECLARE": TokenType.COMMAND,
            "FLOAT64": TokenType.DOUBLE,
            "INT64": TokenType.BIGINT,
            "RECORD": TokenType.STRUCT,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
            "NOT DETERMINISTIC": TokenType.VOLATILE,
            "UNKNOWN": TokenType.NULL,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        PREFIXED_PIVOT_COLUMNS = True

        LOG_BASE_FIRST = False
        LOG_DEFAULTS_TO_LN = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE": _parse_date,
            "DATE_ADD": parse_date_delta_with_interval(exp.DateAdd),
            "DATE_SUB": parse_date_delta_with_interval(exp.DateSub),
            "DATE_TRUNC": lambda args: exp.DateTrunc(
                unit=exp.Literal.string(str(seq_get(args, 1))),
                this=seq_get(args, 0),
            ),
            "DATETIME_ADD": parse_date_delta_with_interval(exp.DatetimeAdd),
            "DATETIME_SUB": parse_date_delta_with_interval(exp.DatetimeSub),
            "DIV": binary_from_function(exp.IntDiv),
            "GENERATE_ARRAY": exp.GenerateSeries.from_arg_list,
            "MD5": exp.MD5Digest.from_arg_list,
            "TO_HEX": _parse_to_hex,
            "PARSE_DATE": lambda args: format_time_lambda(exp.StrToDate, "bigquery")(
                [seq_get(args, 1), seq_get(args, 0)]
            ),
            "PARSE_TIMESTAMP": _parse_timestamp,
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
            "SPLIT": lambda args: exp.Split(
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
                this=seq_get(args, 0),
                expression=seq_get(args, 1) or exp.Literal.string(","),
            ),
            "TIME_ADD": parse_date_delta_with_interval(exp.TimeAdd),
            "TIME_SUB": parse_date_delta_with_interval(exp.TimeSub),
            "TIMESTAMP_ADD": parse_date_delta_with_interval(exp.TimestampAdd),
            "TIMESTAMP_SUB": parse_date_delta_with_interval(exp.TimestampSub),
            "TO_JSON_STRING": exp.JSONFormat.from_arg_list,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "ARRAY": lambda self: self.expression(exp.Array, expressions=[self._parse_statement()]),
        }
        FUNCTION_PARSERS.pop("TRIM")

        NO_PAREN_FUNCTIONS = {
            **parser.Parser.NO_PAREN_FUNCTIONS,
            TokenType.CURRENT_DATETIME: exp.CurrentDatetime,
        }

        NESTED_TYPE_TOKENS = {
            *parser.Parser.NESTED_TYPE_TOKENS,
            TokenType.TABLE,
        }

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,
            TokenType.VALUES,
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "NOT DETERMINISTIC": lambda self: self.expression(
                exp.StabilityProperty, this=exp.Literal.string("VOLATILE")
            ),
            "OPTIONS": lambda self: self._parse_with_property(),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
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

        def _parse_table_parts(self, schema: bool = False) -> exp.Table:
            table = super()._parse_table_parts(schema=schema)
            if isinstance(table.this, exp.Identifier) and "." in table.name:
                catalog, db, this, *rest = (
                    t.cast(t.Optional[exp.Expression], exp.to_identifier(x))
                    for x in split_num_words(table.name, ".", 3)
                )

                if rest and this:
                    this = exp.Dot.build(t.cast(t.List[exp.Expression], [this, *rest]))

                table = exp.Table(this=this, db=db, catalog=catalog)

            return table

    class Generator(generator.Generator):
        EXPLICIT_UNION = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        QUERY_HINTS = False
        TABLE_HINTS = False
        LIMIT_FETCH = "LIMIT"
        RENAME_TABLE_WITH_DB = False
        ESCAPE_LINE_BREAK = True

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.Cast: transforms.preprocess([transforms.remove_precision_parameterized_types]),
            exp.Create: _create_sql,
            exp.CTE: transforms.preprocess([_pushdown_cte_column_names]),
            exp.DateAdd: _date_add_sql("DATE", "ADD"),
            exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e.args.get('unit', 'DAY'))})",
            exp.DateFromParts: rename_func("DATE"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("DATE", "SUB"),
            exp.DatetimeAdd: _date_add_sql("DATETIME", "ADD"),
            exp.DatetimeSub: _date_add_sql("DATETIME", "SUB"),
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", e.this, e.text("unit")),
            exp.GenerateSeries: rename_func("GENERATE_ARRAY"),
            exp.GroupConcat: rename_func("STRING_AGG"),
            exp.Hex: rename_func("TO_HEX"),
            exp.ILike: no_ilike_sql,
            exp.IntDiv: rename_func("DIV"),
            exp.JSONFormat: rename_func("TO_JSON_STRING"),
            exp.Max: max_or_greatest,
            exp.MD5: lambda self, e: self.func("TO_HEX", self.func("MD5", e.this)),
            exp.MD5Digest: rename_func("MD5"),
            exp.Min: min_or_least,
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.RegexpExtract: lambda self, e: self.func(
                "REGEXP_EXTRACT",
                e.this,
                e.expression,
                e.args.get("position"),
                e.args.get("occurrence"),
            ),
            exp.RegexpReplace: regexp_replace_sql,
            exp.RegexpLike: rename_func("REGEXP_CONTAINS"),
            exp.ReturnsProperty: _returnsproperty_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.explode_to_unnest,
                    _unqualify_unnest,
                    transforms.eliminate_distinct_on,
                    _alias_ordered_group,
                ]
            ),
            exp.StabilityProperty: lambda self, e: f"DETERMINISTIC"
            if e.name == "IMMUTABLE"
            else "NOT DETERMINISTIC",
            exp.StrToDate: lambda self, e: f"PARSE_DATE({self.format_time(e)}, {self.sql(e, 'this')})",
            exp.StrToTime: lambda self, e: self.func(
                "PARSE_TIMESTAMP", self.format_time(e), e.this, e.args.get("zone")
            ),
            exp.TimeAdd: _date_add_sql("TIME", "ADD"),
            exp.TimeSub: _date_add_sql("TIME", "SUB"),
            exp.TimestampAdd: _date_add_sql("TIMESTAMP", "ADD"),
            exp.TimestampSub: _date_add_sql("TIMESTAMP", "SUB"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.Trim: lambda self, e: self.func(f"TRIM", e.this, e.expression),
            exp.TsOrDsAdd: _date_add_sql("DATE", "ADD"),
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("bigquery"),
            exp.Unhex: rename_func("FROM_HEX"),
            exp.Values: _derived_table_values_to_unnest,
            exp.VariancePop: rename_func("VAR_POP"),
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
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
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP",
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.VARBINARY: "BYTES",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.VARIANT: "ANY TYPE",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        # from: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords
        RESERVED_KEYWORDS = {
            *generator.Generator.RESERVED_KEYWORDS,
            "all",
            "and",
            "any",
            "array",
            "as",
            "asc",
            "assert_rows_modified",
            "at",
            "between",
            "by",
            "case",
            "cast",
            "collate",
            "contains",
            "create",
            "cross",
            "cube",
            "current",
            "default",
            "define",
            "desc",
            "distinct",
            "else",
            "end",
            "enum",
            "escape",
            "except",
            "exclude",
            "exists",
            "extract",
            "false",
            "fetch",
            "following",
            "for",
            "from",
            "full",
            "group",
            "grouping",
            "groups",
            "hash",
            "having",
            "if",
            "ignore",
            "in",
            "inner",
            "intersect",
            "interval",
            "into",
            "is",
            "join",
            "lateral",
            "left",
            "like",
            "limit",
            "lookup",
            "merge",
            "natural",
            "new",
            "no",
            "not",
            "null",
            "nulls",
            "of",
            "on",
            "or",
            "order",
            "outer",
            "over",
            "partition",
            "preceding",
            "proto",
            "qualify",
            "range",
            "recursive",
            "respect",
            "right",
            "rollup",
            "rows",
            "select",
            "set",
            "some",
            "struct",
            "tablesample",
            "then",
            "to",
            "treat",
            "true",
            "unbounded",
            "union",
            "unnest",
            "using",
            "when",
            "where",
            "window",
            "with",
            "within",
        }

        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            parent = expression.parent

            # BigQuery allows CAST(.. AS {STRING|TIMESTAMP} [FORMAT <fmt> [AT TIME ZONE <tz>]]).
            # Only the TIMESTAMP one should use the below conversion, when AT TIME ZONE is included.
            if not isinstance(parent, exp.Cast) or not parent.to.is_type("text"):
                return self.func(
                    "TIMESTAMP", self.func("DATETIME", expression.this, expression.args.get("zone"))
                )

            return super().attimezone_sql(expression)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            return self.cast_sql(expression, safe_prefix="SAFE_")

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.alias_column_names:
                self.unsupported("Column names in CTE definition are not supported.")
            return super().cte_sql(expression)

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
