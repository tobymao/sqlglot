from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer


def _date_trunc_sql(self, expression):
    unit = expression.text("unit").lower()

    this = self.sql(expression.this)

    if unit == "day":
        return f"DATE({this})"

    if unit == "week":
        concat = f"CONCAT(YEAR({this}), ' ', WEEK({this}, 1), ' 1')"
        date_format = "%Y %u %w"
    elif unit == "month":
        concat = f"CONCAT(YEAR({this}), ' ', MONTH({this}), ' 1')"
        date_format = "%Y %c %e"
    elif unit == "quarter":
        concat = f"CONCAT(YEAR({this}), ' ', QUARTER({this}) * 3 - 2, ' 1')"
        date_format = "%Y %c %e"
    elif unit == "year":
        concat = f"CONCAT(YEAR({this}), ' 1 1')"
        date_format = "%Y %c %e"
    else:
        self.unsupported("Unexpected interval unit: {unit}")
        return f"DATE({this})"

    return f"STR_TO_DATE({concat}, '{date_format}')"


def _str_to_date(args):
    date_format = MySQL.format_time(list_get(args, 1))
    return exp.StrToDate(this=list_get(args, 0), format=date_format)


def _str_to_date_sql(self, expression):
    date_format = self.format_time(expression)
    return f"STR_TO_DATE({self.sql(expression.this)}, {date_format})"


def _date_add(expression_class):
    def func(args):
        interval = list_get(args, 1)
        return expression_class(
            this=list_get(args, 0),
            expression=interval.this,
            unit=exp.Literal.string(interval.text("unit").lower()),
        )

    return func


def _date_add_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = expression.text("unit").upper() or "DAY"
        expression = self.sql(expression, "expression")
        return f"DATE_{kind}({this}, INTERVAL {expression} {unit})"

    return func


class MySQL(Dialect):
    identifiers = ["`"]

    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    time_mapping = {
        "%M": "%B",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%S": "%S",
        "%u": "%W",
    }

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']

    class Parser(Parser):
        STRICT_CAST = False

        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "DATE_ADD": _date_add(exp.DateAdd),
            "DATE_SUB": _date_add(exp.DateSub),
            "STR_TO_DATE": _str_to_date,
        }

    class Generator(Generator):
        NULL_ORDERING_SUPPORTED = False

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateTrunc: _date_trunc_sql,
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_date_sql,
        }
