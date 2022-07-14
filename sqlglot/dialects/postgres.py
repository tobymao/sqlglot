from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
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
    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "UUID": TokenType.UUID,
        }

    class Parser(Parser):
        STRICT_CAST = False
        FUNCTIONS = {**Parser.FUNCTIONS, "TO_TIMESTAMP": exp.StrToTime.from_arg_list}

    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.BINARY: "BYTEA",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DateAdd: _date_add_sql("+"),
            exp.DateSub: _date_add_sql("-"),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
