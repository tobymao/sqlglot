from __future__ import annotations

from sqlglot import exp, generator, parser, transforms
from sqlglot.dialects.dialect import Dialect


def _if_sql(self, expression):
    return f"IF {self.sql(expression, 'this')} THEN {self.sql(expression, 'true')} ELSE {self.sql(expression, 'false')} END"


def _coalesce_sql(self, expression):
    return f"IFNULL({self.sql(expression, 'this')}, {self.expressions(expression)})"


def _count_sql(self, expression):
    this = expression.this
    if isinstance(this, exp.Distinct):
        return f"COUNTD({self.expressions(this, flat=True)})"
    return f"COUNT({self.sql(expression, 'this')})"


class Tableau(Dialect):
    class Generator(generator.Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.If: _if_sql,
            exp.Coalesce: _coalesce_sql,
            exp.Count: _count_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "COUNTD": lambda args: exp.Count(this=exp.Distinct(expressions=args)),
        }
