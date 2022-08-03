import typing as t

import sqlglot
from sqlglot import expressions as exp


class Column:
    def __init__(self, item: t.Union[str, exp.Expression]):
        if isinstance(item, str):
            item = sqlglot.parse_one(item)
        self.expression = item

    def __eq__(self, other: "Column") -> "Column":
        return Column(exp.EQ(this=self.expression, expression=other.expression))

    def __ne__(self, other: "Column") -> "Column":
        return Column(exp.NEQ(this=self.expression, expression=other.expression))

    def __gt__(self, other):
        return Column(exp.GT(this=self.expression, expression=other.expression))

    def __and__(self, other):
        return Column(exp.And(this=self.expression, expression=other.expression))

    def __or__(self, other):
        return Column(exp.Or(this=self.expression, expression=other.expression))

    def alias(self, name: str):
        self.expression = exp.Alias(alias=name, this=self.expression)
        return self.expression
