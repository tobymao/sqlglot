from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, format_time_lambda
from sqlglot.expressions import Literal
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType

def _check_int(s):
    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()

# from https://docs.snowflake.com/en/sql-reference/functions/to_timestamp.html
def _snowflake_to_timestamp(args):
    args_num = len(args)
    if args_num == 2:
        first_arg = list_get(args, 0)
        second_arg = list_get(args, 1)
        if first_arg.is_string:
            # case: <string_expr> [ , <format> ]
            return format_time_lambda(exp.StrToTime, "snowflake")

        # case: <numeric_expr> [ , <scale> ]
        if second_arg.this not in ['0', '3', '9']:
            raise ValueError(f"Scale for snowflake numeric timestamp is {second_arg}, but should be 0, 3, or 9")

        def _convert_time_scale_and_run(args):
            conv_args = []
            exponent = int(list_get(args, 1).this)
            retval = int(list_get(args, 0).this) / (10 ** exponent)
            conv_args.append(str(retval))
            args[:] = args[:1]
            return exp.UnixToTime.from_arg_list(conv_args)

        return _convert_time_scale_and_run

    arg = list_get(args, 0)
    if not isinstance(arg, Literal):
        # case: <variant_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)

    if arg.is_string:
        if _check_int(arg.this):
            # case: <integer>
            return exp.UnixToTime.from_arg_list

        # case: <date_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)

    # case: <numeric_expr>
    return exp.UnixToTime.from_arg_list

class Snowflake(Dialect):
    time_format = "'%Y-%m-%d %H:%I:%S.%f'"

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
        "%mm": "%m",
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
        "ff6": "%f"
        # TODO: Snowflake also supports variable precisions FF0...FF9, but unclear how to support in Python
    }

    class Parser(Parser):
        COLUMN_OPERATORS = Parser.COLUMN_OPERATORS | {TokenType.COLON}

        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "TO_TIMESTAMP": lambda args: _snowflake_to_timestamp(args)(args)
        }

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "QUALIFY": TokenType.QUALIFY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
        }
