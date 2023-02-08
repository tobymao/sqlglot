from __future__ import annotations

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    datestrtodate_sql,
    format_time_lambda,
    inline_array_sql,
    rename_func,
    timestrtotime_sql,
    var_map_sql,
)
from sqlglot.expressions import Literal
from sqlglot.helper import flatten, seq_get
from sqlglot.tokens import TokenType


def _check_int(s):
    if s[0] in ("-", "+"):
        return s[1:].isdigit()
    return s.isdigit()


# from https://docs.snowflake.com/en/sql-reference/functions/to_timestamp.html
def _snowflake_to_timestamp(args):
    if len(args) == 2:
        first_arg, second_arg = args
        if second_arg.is_string:
            # case: <string_expr> [ , <format> ]
            return format_time_lambda(exp.StrToTime, "snowflake")(args)

        # case: <numeric_expr> [ , <scale> ]
        if second_arg.name not in ["0", "3", "9"]:
            raise ValueError(
                f"Scale for snowflake numeric timestamp is {second_arg}, but should be 0, 3, or 9"
            )

        if second_arg.name == "0":
            timescale = exp.UnixToTime.SECONDS
        elif second_arg.name == "3":
            timescale = exp.UnixToTime.MILLIS
        elif second_arg.name == "9":
            timescale = exp.UnixToTime.MICROS

        return exp.UnixToTime(this=first_arg, scale=timescale)

    first_arg = seq_get(args, 0)
    if not isinstance(first_arg, Literal):
        # case: <variant_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)(args)

    if first_arg.is_string:
        if _check_int(first_arg.this):
            # case: <integer>
            return exp.UnixToTime.from_arg_list(args)

        # case: <date_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)(args)

    # case: <numeric_expr>
    return exp.UnixToTime.from_arg_list(args)


def _unix_to_time_sql(self, expression):
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale in [None, exp.UnixToTime.SECONDS]:
        return f"TO_TIMESTAMP({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TO_TIMESTAMP({timestamp}, 3)"
    if scale == exp.UnixToTime.MICROS:
        return f"TO_TIMESTAMP({timestamp}, 9)"

    raise ValueError("Improper scale for timestamp")


# https://docs.snowflake.com/en/sql-reference/functions/date_part.html
# https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
def _parse_date_part(self):
    this = self._parse_var() or self._parse_type()
    self._match(TokenType.COMMA)
    expression = self._parse_bitwise()

    name = this.name.upper()
    if name.startswith("EPOCH"):
        if name.startswith("EPOCH_MILLISECOND"):
            scale = 10**3
        elif name.startswith("EPOCH_MICROSECOND"):
            scale = 10**6
        elif name.startswith("EPOCH_NANOSECOND"):
            scale = 10**9
        else:
            scale = None

        ts = self.expression(exp.Cast, this=expression, to=exp.DataType.build("TIMESTAMP"))
        to_unix = self.expression(exp.TimeToUnix, this=ts)

        if scale:
            to_unix = exp.Mul(this=to_unix, expression=exp.Literal.number(scale))

        return to_unix

    return self.expression(exp.Extract, this=this, expression=expression)


def _datatype_sql(self, expression):
    if expression.this == exp.DataType.Type.ARRAY:
        return "ARRAY"
    elif expression.this == exp.DataType.Type.MAP:
        return "OBJECT"
    return self.datatype_sql(expression)


class Snowflake(Dialect):
    null_ordering = "nulls_are_large"
    time_format = "'yyyy-mm-dd hh24:mi:ss'"

    time_mapping = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "mmmm": "%B",
        "MON": "%b",
        "mon": "%b",
        "MM": "%m",
        "mm": "%m",
        "DD": "%d",
        "dd": "%d",
        "d": "%-d",
        "DY": "%w",
        "dy": "%w",
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FF": "%f",
        "ff": "%f",
        "FF6": "%f",
        "ff6": "%f",
    }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAYAGG": exp.ArrayAgg.from_arg_list,
            "IFF": exp.If.from_arg_list,
            "TO_TIMESTAMP": _snowflake_to_timestamp,
            "ARRAY_CONSTRUCT": exp.Array.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "DECODE": exp.Matches.from_arg_list,
            "OBJECT_CONSTRUCT": parser.parse_var_map,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "DATE_PART": _parse_date_part,
        }
        FUNCTION_PARSERS.pop("TRIM")

        FUNC_TOKENS = {
            *parser.Parser.FUNC_TOKENS,
            TokenType.RLIKE,
            TokenType.TABLE,
        }

        COLUMN_OPERATORS = {
            **parser.Parser.COLUMN_OPERATORS,  # type: ignore
            TokenType.COLON: lambda self, this, path: self.expression(
                exp.Bracket,
                this=this,
                expressions=[path],
            ),
        }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", "$$"]
        STRING_ESCAPES = ["\\", "'"]

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "EXCLUDE": TokenType.EXCEPT,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "RENAME": TokenType.REPLACE,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP_NTZ": TokenType.TIMESTAMP,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TIMESTAMPNTZ": TokenType.TIMESTAMP,
            "MINUS": TokenType.EXCEPT,
            "SAMPLE": TokenType.TABLE_SAMPLE,
        }

    class Generator(generator.Generator):
        CREATE_TRANSIENT = True

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.Array: inline_array_sql,
            exp.ArrayConcat: rename_func("ARRAY_CAT"),
            exp.DateAdd: rename_func("DATEADD"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DataType: _datatype_sql,
            exp.If: rename_func("IFF"),
            exp.Map: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.VarMap: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.Parameter: lambda self, e: f"${self.sql(e, 'this')}",
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Matches: rename_func("DECODE"),
            exp.StrPosition: lambda self, e: f"{self.normalize_func('POSITION')}({self.format_args(e.args.get('substr'), e.this, e.args.get('position'))})",
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.Trim: lambda self, e: f"TRIM({self.format_args(e.this, e.expression)})",
            exp.UnixToTime: _unix_to_time_sql,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TIMESTAMP: "TIMESTAMPNTZ",
        }

        STAR_MAPPING = {
            "except": "EXCLUDE",
            "replace": "RENAME",
        }

        def except_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT with All is not supported in Snowflake")
            return super().except_op(expression)

        def intersect_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("INTERSECT with All is not supported in Snowflake")
            return super().intersect_op(expression)

        def values_sql(self, expression: exp.Values) -> str:
            """Due to a bug in Snowflake we want to make sure that all columns in a VALUES table alias are unquoted.

            We also want to make sure that after we find matches where we need to unquote a column that we prevent users
            from adding quotes to the column by using the `identify` argument when generating the SQL.
            """
            alias = expression.args.get("alias")
            if alias and alias.args.get("columns"):
                expression = expression.transform(
                    lambda node: exp.Identifier(**{**node.args, "quoted": False})
                    if isinstance(node, exp.Identifier)
                    and isinstance(node.parent, exp.TableAlias)
                    and node.arg_key == "columns"
                    else node,
                )
                return self.no_identify(lambda: super(self.__class__, self).values_sql(expression))
            return super().values_sql(expression)

        def select_sql(self, expression: exp.Select) -> str:
            """Due to a bug in Snowflake we want to make sure that all columns in a VALUES table alias are unquoted and also
            that all columns in a SELECT are unquoted. We also want to make sure that after we find matches where we need
            to unquote a column that we prevent users from adding quotes to the column by using the `identify` argument when
            generating the SQL.

            Note: We make an assumption that any columns referenced in a VALUES expression should be unquoted throughout the
            expression. This might not be true in a case where the same column name can be sourced from another table that can
            properly quote but should be true in most cases.
            """
            values_expressions = expression.find_all(exp.Values)
            values_identifiers = set(
                flatten(
                    v.args.get("alias", exp.Alias()).args.get("columns", [])
                    for v in values_expressions
                )
            )
            if values_identifiers:
                expression = expression.transform(
                    lambda node: exp.Identifier(**{**node.args, "quoted": False})
                    if isinstance(node, exp.Identifier) and node in values_identifiers
                    else node,
                )
                return self.no_identify(lambda: super(self.__class__, self).select_sql(expression))
            return super().select_sql(expression)

        def describe_sql(self, expression: exp.Describe) -> str:
            # Default to table if kind is unknown
            kind_value = expression.args.get("kind") or "TABLE"
            kind = f" {kind_value}" if kind_value else ""
            this = f" {self.sql(expression, 'this')}"
            return f"DESCRIBE{kind}{this}"
