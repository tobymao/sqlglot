import typing as t

import sqlglot
from sqlglot import expressions as exp


class Column:
    def __init__(self, item: t.Union[str, exp.Expression]):
        if isinstance(item, str):
            item = sqlglot.parse_one(item)
        self.expression = item

    def __hash__(self):
        return hash(self.expression)

    def __eq__(self, other: "Column") -> "Column":
        return Column(exp.EQ(this=self.expression, expression=other.expression))

    def __ne__(self, other: "Column") -> "Column":
        return Column(exp.NEQ(this=self.expression, expression=other.expression))

    def __gt__(self, other) -> "Column":
        return Column(exp.GT(this=self.expression, expression=other.expression))

    def __and__(self, other) -> "Column":
        return Column(exp.And(this=self.expression, expression=other.expression))

    def __or__(self, other) -> "Column":
        return Column(exp.Or(this=self.expression, expression=other.expression))

    def copy(self):
        return Column(self.expression.copy())

    def set_table_name(self, table_name: str):
        self.expression.set("table", exp.Identifier(this=table_name))
        return self

    def sql(self, **kwargs):
        return self.expression.sql(dialect="spark", **kwargs)

    def alias(self, name: str):
        self.expression = exp.Alias(alias=exp.Identifier(this=name), this=self.expression)
        return Column(self.expression)
