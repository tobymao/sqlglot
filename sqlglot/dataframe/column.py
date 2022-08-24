import typing as t

import sqlglot
from sqlglot import expressions as exp


class Column:
    def __init__(self, expression: t.Union[str, exp.Expression]):
        if isinstance(expression, str):
            expression = sqlglot.parse_one(expression)
        self.expression = expression
        if isinstance(self.expression, exp.Literal):
            self.expression = self.alias(self.expression.args["this"]).expression
        elif isinstance(self.expression, exp.Null):
            self.expression = self.alias("NULL").expression

    def __hash__(self):
        return hash(self.expression)

    def __eq__(self, other: "Column") -> "Column":
        return self.binary_op(exp.EQ, other)

    def __ne__(self, other: "Column") -> "Column":
        return self.binary_op(exp.NEQ, other)

    def __gt__(self, other: "Column") -> "Column":
        return self.binary_op(exp.GT, other)

    def __ge__(self, other):
        return self.binary_op(exp.GTE, other)

    def __lt__(self, other):
        return self.binary_op(exp.LT, other)

    def __le__(self, other):
        return self.binary_op(exp.LTE, other)

    def __and__(self, other: "Column") -> "Column":
        return self.binary_op(exp.And, other)

    def __or__(self, other: "Column") -> "Column":
        return self.binary_op(exp.Or, other)

    def __mod__(self, other: "Column") -> "Column":
        return self.binary_op(exp.Mod, other)

    def __add__(self, other):
        return self.binary_op(exp.Add, other)

    def __sub__(self, other):
        return self.binary_op(exp.Sub, other)

    def __mul__(self, other):
        return self.binary_op(exp.Mul, other)

    def __truediv__(self, other):
        return self.binary_op(exp.Div, other)

    def binary_op(self, clazz: t.Callable, other: "Column", **kwargs) -> "Column":
        return Column(clazz(this=self.column_expression, expression=other.column_expression, **kwargs))

    @property
    def is_alias(self):
        return isinstance(self.expression, exp.Alias)

    @property
    def is_column(self):
        return isinstance(self.expression, exp.Column)

    @property
    def column_expression(self):
        if self.is_alias:
            return self.expression.args["this"]
        return self.expression

    def copy(self) -> "Column":
        return Column(self.expression.copy())

    def set_table_name(self, table_name: str) -> "Column":
        self.expression.set("table", exp.Identifier(this=table_name))
        return Column(self.expression)

    def sql(self, **kwargs) -> "Column":
        return self.expression.sql(dialect="spark", **kwargs)

    def alias(self, name: str) -> "Column":
        new_expression = exp.Alias(alias=exp.Identifier(this=name, quoted=True), this=self.column_expression)
        return Column(new_expression)

    def asc(self) -> "Column":
        new_expression = exp.Ordered(this=self.column_expression, desc=False, nulls_first=True)
        return Column(new_expression)

    def desc(self) -> "Column":
        new_expression = exp.Ordered(this=self.column_expression, desc=True, nulls_first=False)
        return Column(new_expression)

    asc_nulls_first = asc

    def asc_nulls_last(self) -> "Column":
        new_expression = exp.Ordered(this=self.column_expression, desc=False, nulls_first=False)
        return Column(new_expression)

    def desc_nulls_first(self) -> "Column":
        new_expression = exp.Ordered(this=self.column_expression, desc=True, nulls_first=True)
        return Column(new_expression)

    desc_null_last = desc

    def when(self, condition: "Column", value: t.Any) -> "Column":
        from sqlglot.dataframe.functions import when
        column_with_if = when(condition, value)
        new_column = self.copy()
        new_column.expression.args["ifs"].extend(column_with_if.expression.args["ifs"])
        return new_column

    def otherwise(self, value: t.Any) -> "Column":
        from sqlglot.dataframe.functions import lit
        true_value = value if isinstance(value, Column) else lit(value)
        new_column = self.copy()
        new_column.expression.args["default"] = true_value.column_expression
        return new_column

    def isNull(self) -> "Column":
        new_expression = exp.Is(this=self.column_expression, expression=exp.Null())
        return Column(new_expression)

    def isNotNull(self) -> "Column":
        new_expression = exp.Not(this=exp.Is(this=self.column_expression, expression=exp.Null()))
        return Column(new_expression)
