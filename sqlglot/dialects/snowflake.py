from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, format_time_lambda, rename_func
from sqlglot.expressions import Literal
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.generator import Generator


def _check_int(s):
    if s[0] in ("-", "+"):
        return s[1:].isdigit()
    return s.isdigit()


# from https://docs.snowflake.com/en/sql-reference/functions/to_timestamp.html
def _snowflake_to_timestamp(args):
    if len(args) == 2:
        first_arg, second_arg = args
        if first_arg.is_string:
            # case: <string_expr> [ , <format> ]
            return format_time_lambda(exp.StrToTime, "snowflake")(args)

        # case: <numeric_expr> [ , <scale> ]
        if second_arg.this not in ["0", "3", "9"]:
            raise ValueError(
                f"Scale for snowflake numeric timestamp is {second_arg}, but should be 0, 3, or 9"
            )

        timestamp = int(first_arg.name)
        scale = int(second_arg.name)
        return exp.UnixToTime(this=str(timestamp // (10**scale)))

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


class Snowflake(Dialect):
    time_format = "'yyyy-mm-dd hh24:mi:ss'"

    # pylint: disable=duplicate-code
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
    # pylint: enable=duplicate-code

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "IFF": exp.If.from_arg_list,
            "TO_TIMESTAMP": _snowflake_to_timestamp,
        }

        COLUMN_OPERATORS = {
            **Parser.COLUMN_OPERATORS,
            TokenType.COLON: lambda self, this, path: self.expression(
                exp.Bracket,
                this=this,
                expressions=[path],
            ),
        }

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "QUALIFY": TokenType.QUALIFY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
        }

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.If: rename_func("IFF"),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: rename_func("TO_TIMESTAMP"),
        }
