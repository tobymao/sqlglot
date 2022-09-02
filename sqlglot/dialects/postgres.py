from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_sql,
    arrow_json_extract_scalar_sql,
    format_time_lambda,
    no_paren_current_date_sql,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


def _date_add_sql(kind):
    def func(self, expression):
        from sqlglot.optimizer.simplify import simplify

        this = self.sql(expression, "this")
        unit = self.sql(expression, "unit")
        expression = simplify(expression.args["expression"])

        if not isinstance(expression, exp.Literal):
            self.unsupported("Cannot add non literal")

        expression = expression.copy()
        expression.args["is_string"] = True
        expression = self.sql(expression)
        return f"{this} {kind} INTERVAL {expression} {unit}"

    return func


class Postgres(Dialect):
    null_ordering = "nulls_are_large"
    time_format = "'YYYY-MM-DD HH24:MI:SS'"
    time_mapping = {
        "AM": "%p",  # AM or PM
        "D": "%w",  # 1-based day of week
        "DD": "%d",  # day of month
        "DDD": "%j",  # zero padded day of year
        "FMDD": "%-d",  # - is no leading zero for Python; same for FM in postgres
        "FMDDD": "%-j",  # day of year
        "FMHH12": "%-I",  # 9
        "FMHH24": "%-H",  # 9
        "FMMI": "%-M",  # Minute
        "FMMM": "%-m",  # 1
        "FMSS": "%-S",  # Second
        "HH12": "%I",  # 09
        "HH24": "%H",  # 09
        "MI": "%M",  # zero padded minute
        "MM": "%m",  # 01
        "OF": "%z",  # utc offset
        "SS": "%S",  # zero padded second
        "TMDay": "%A",  # TM is locale dependent
        "TMDy": "%a",
        "TMMon": "%b",  # Sep
        "TMMonth": "%B",  # September
        "TZ": "%Z",  # uppercase timezone name
        "US": "%f",  # zero padded microsecond
        "WW": "%U",  # 1-based week of year
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
    }

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "SERIAL": TokenType.AUTO_INCREMENT,
            "UUID": TokenType.UUID,
        }

    class Parser(Parser):
        STRICT_CAST = False
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "TO_TIMESTAMP": format_time_lambda(exp.StrToTime, "postgres"),
            "TO_CHAR": format_time_lambda(exp.TimeToStr, "postgres"),
        }

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.BINARY: "BYTEA",
        }

        TOKEN_MAPPING = {
            TokenType.AUTO_INCREMENT: "SERIAL",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: lambda self, e: f"{self.sql(e, 'this')}#>{self.sql(e, 'path')}",
            exp.JSONBExtractScalar: lambda self, e: f"{self.sql(e, 'this')}#>>{self.sql(e, 'path')}",
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DateAdd: _date_add_sql("+"),
            exp.DateSub: _date_add_sql("-"),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
