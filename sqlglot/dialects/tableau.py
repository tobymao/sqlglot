from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser


def _if_sql(self, expression):
    return f"IF {self.sql(expression, 'this')} THEN {self.sql(expression, 'true')} ELSE {self.sql(expression, 'false')} END"


def _coalesce_sql(self, expression):
    return f"IFNULL({self.sql(expression, 'this')}, {self.expressions(expression)})"


def _count_sql(self, expression):
    this = expression.this
    if isinstance(this, exp.Distinct):
        return f"COUNTD({self.sql(this, 'this')})"
    return f"COUNT({self.sql(expression, 'this')})"


class Tableau(Dialect):
    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.If: _if_sql,
            exp.Coalesce: _coalesce_sql,
            exp.Count: _count_sql,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "IFNULL": exp.Coalesce.from_arg_list,
            "COUNTD": lambda args: exp.Count(this=exp.Distinct(this=list_get(args, 0))),
        }
