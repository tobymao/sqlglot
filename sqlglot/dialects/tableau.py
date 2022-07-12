from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator


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
