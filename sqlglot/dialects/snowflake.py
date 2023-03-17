from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    date_trunc_to_time,
    datestrtodate_sql,
    format_time_lambda,
    inline_array_sql,
    min_or_least,
    rename_func,
    timestamptrunc_sql,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
    var_map_sql,
)
from sqlglot.expressions import Literal
from sqlglot.helper import flatten, seq_get
from sqlglot.parser import binary_range_parser
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


# https://docs.snowflake.com/en/sql-reference/functions/div0
def _div0_to_if(args):
    cond = exp.EQ(this=seq_get(args, 1), expression=exp.Literal.number(0))
    true = exp.Literal.number(0)
    false = exp.Div(this=seq_get(args, 0), expression=seq_get(args, 1))
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _zeroifnull_to_if(args):
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _nullifzero_to_if(args):
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


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
            "ARRAY_CONSTRUCT": exp.Array.from_arg_list,
            "ARRAY_TO_STRING": exp.ArrayJoin.from_arg_list,
            "DATE_TRUNC": date_trunc_to_time,
            "DATEADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DECODE": exp.Matches.from_arg_list,
            "DIV0": _div0_to_if,
            "IFF": exp.If.from_arg_list,
            "NULLIFZERO": _nullifzero_to_if,
            "OBJECT_CONSTRUCT": parser.parse_var_map,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "TO_ARRAY": exp.Array.from_arg_list,
            "TO_VARCHAR": exp.ToChar.from_arg_list,
            "TO_TIMESTAMP": _snowflake_to_timestamp,
            "ZEROIFNULL": _zeroifnull_to_if,
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

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,  # type: ignore
            TokenType.LIKE_ANY: binary_range_parser(exp.LikeAny),
            TokenType.ILIKE_ANY: binary_range_parser(exp.ILikeAny),
        }

        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,  # type: ignore
            "UNSET": lambda self: self._parse_alter_table_set_tag(unset=True),
            "SET": lambda self: self._parse_alter_table_set_tag(),
        }

        def _parse_alter_table_set_tag(self, unset: bool = False) -> exp.Expression:
            self._match_text_seq("TAG")
            parser = t.cast(t.Callable, self._parse_id_var if unset else self._parse_conjunction)
            return self.expression(exp.SetTag, expressions=self._parse_csv(parser), unset=unset)

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", "$$"]
        STRING_ESCAPES = ["\\", "'"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "EXCLUDE": TokenType.EXCEPT,
            "ILIKE ANY": TokenType.ILIKE_ANY,
            "LIKE ANY": TokenType.LIKE_ANY,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "PUT": TokenType.COMMAND,
            "RENAME": TokenType.REPLACE,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP_NTZ": TokenType.TIMESTAMP,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TIMESTAMPNTZ": TokenType.TIMESTAMP,
            "MINUS": TokenType.EXCEPT,
            "SAMPLE": TokenType.TABLE_SAMPLE,
        }

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

    class Generator(generator.Generator):
        PARAMETER_TOKEN = "$"
        MATCHED_BY_SOURCE = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.Array: inline_array_sql,
            exp.ArrayConcat: rename_func("ARRAY_CAT"),
            exp.ArrayJoin: rename_func("ARRAY_TO_STRING"),
            exp.DateAdd: lambda self, e: self.func("DATEADD", e.text("unit"), e.expression, e.this),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DataType: _datatype_sql,
            exp.If: rename_func("IFF"),
            exp.Map: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.LogicalOr: rename_func("BOOLOR_AGG"),
            exp.LogicalAnd: rename_func("BOOLAND_AGG"),
            exp.VarMap: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Matches: rename_func("DECODE"),
            exp.StrPosition: lambda self, e: self.func(
                "POSITION", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.Trim: lambda self, e: self.func("TRIM", e.this, e.expression),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("snowflake"),
            exp.UnixToTime: _unix_to_time_sql,
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.Min: min_or_least,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TIMESTAMP: "TIMESTAMPNTZ",
        }

        STAR_MAPPING = {
            "except": "EXCLUDE",
            "replace": "RENAME",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.SetProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def ilikeany_sql(self, expression: exp.ILikeAny) -> str:
            return self.binary(expression, "ILIKE ANY")

        def likeany_sql(self, expression: exp.LikeAny) -> str:
            return self.binary(expression, "LIKE ANY")

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

        def settag_sql(self, expression: exp.SetTag) -> str:
            action = "UNSET" if expression.args.get("unset") else "SET"
            return f"{action} TAG {self.expressions(expression)}"

        def select_sql(self, expression: exp.Select) -> str:
            """Due to a bug in Snowflake we want to make sure that all columns in a VALUES table alias are unquoted and also
            that all columns in a SELECT are unquoted. We also want to make sure that after we find matches where we need
            to unquote a column that we prevent users from adding quotes to the column by using the `identify` argument when
            generating the SQL.

            Note: We make an assumption that any columns referenced in a VALUES expression should be unquoted throughout the
            expression. This might not be true in a case where the same column name can be sourced from another table that can
            properly quote but should be true in most cases.
            """
            values_identifiers = set(
                flatten(
                    (v.args.get("alias") or exp.Alias()).args.get("columns", [])
                    for v in expression.find_all(exp.Values)
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

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            start = expression.args.get("start")
            start = f" START {start}" if start else ""
            increment = expression.args.get("increment")
            increment = f" INCREMENT {increment}" if increment else ""
            return f"AUTOINCREMENT{start}{increment}"
