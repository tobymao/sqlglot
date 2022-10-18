from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    format_time_lambda,
    inline_array_sql,
    rename_func,
)
from sqlglot.expressions import Literal
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


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
            raise ValueError(f"Scale for snowflake numeric timestamp is {second_arg}, but should be 0, 3, or 9")

        if second_arg.name == "0":
            timescale = exp.UnixToTime.SECONDS
        elif second_arg.name == "3":
            timescale = exp.UnixToTime.MILLIS
        elif second_arg.name == "9":
            timescale = exp.UnixToTime.MICROS

        return exp.UnixToTime(this=first_arg, scale=timescale)

    first_arg = list_get(args, 0)
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


def _unix_to_time(self, expression):
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

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "ARRAYAGG": exp.ArrayAgg.from_arg_list,
            "IFF": exp.If.from_arg_list,
            "TO_TIMESTAMP": _snowflake_to_timestamp,
            "ARRAY_CONSTRUCT": exp.Array.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
        }

        FUNCTION_PARSERS = {
            **Parser.FUNCTION_PARSERS,
            "DATE_PART": _parse_date_part,
        }

        FUNC_TOKENS = {
            *Parser.FUNC_TOKENS,
            TokenType.RLIKE,
            TokenType.TABLE,
        }

        COLUMN_OPERATORS = {
            **Parser.COLUMN_OPERATORS,
            TokenType.COLON: lambda self, this, path: self.expression(
                exp.Bracket,
                this=this,
                expressions=[path],
            ),
        }

        PROPERTY_PARSERS = {
            **Parser.PROPERTY_PARSERS,
            TokenType.PARTITION_BY: lambda self: self._parse_partitioned_by(),
        }

    class Tokenizer(Tokenizer):
        QUOTES = ["'", "$$"]
        ESCAPE = "\\"

        SINGLE_TOKENS = {
            **Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "QUALIFY": TokenType.QUALIFY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP_NTZ": TokenType.TIMESTAMP,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TIMESTAMPNTZ": TokenType.TIMESTAMP,
            "SAMPLE": TokenType.TABLE_SAMPLE,
        }

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.ArrayConcat: rename_func("ARRAY_CAT"),
            exp.If: rename_func("IFF"),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: _unix_to_time,
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.Array: inline_array_sql,
            exp.StrPosition: rename_func("POSITION"),
            exp.Parameter: lambda self, e: f"${self.sql(e, 'this')}",
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'value')}",
        }

        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.TIMESTAMP: "TIMESTAMPNTZ",
        }

        ROOT_PROPERTIES = {
            exp.PartitionedByProperty,
            exp.ReturnsProperty,
            exp.LanguageProperty,
            exp.SchemaCommentProperty,
            exp.ExecuteAsProperty,
            exp.VolatilityProperty,
        }

        def except_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT with All is not supported in Snowflake")
            return super().except_op(expression)

        def intersect_op(self, expression):
            if not expression.args.get("distinct", False):
                self.unsupported("INTERSECT with All is not supported in Snowflake")
            return super().intersect_op(expression)
