from collections.abc import Iterable
import typing as t

import sqlglot
from sqlglot import expressions as exp


class Column:
    def __init__(self, expression: t.Union[t.Any, exp.Expression]):
        if isinstance(expression, str):
            expression = sqlglot.parse_one(expression)
        elif isinstance(expression, Column):
            expression = expression.expression
        self.expression = expression
        if isinstance(self.expression, exp.Null):
            self.expression = self.alias("NULL").expression
        elif isinstance(self.expression, (int, float, bool)):
            self.expression = exp.Column(this=exp.Literal(this=str(expression).lower(), is_string=False))
        elif isinstance(self.expression, Iterable):
            expressions = [exp.Literal(this=str(x).lower(), is_string=False) for x in self.expression]
            self.expression = exp.Column(this=exp.Array(expressions=expressions))

    def __repr__(self):
        return repr(self.expression)

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
    def column_expression(self) -> exp.Expression:
        if self.is_alias:
            return self.expression.args["this"]
        return self.expression

    @property
    def alias_or_name(self):
        if isinstance(self.expression.args.get("this"), exp.Star):
            return self.expression.args["this"].alias_or_name
        return self.expression.alias_or_name

    @classmethod
    def ensure_literal(cls, value) -> "Column":
        from sqlglot.dataframe.functions import lit
        if isinstance(value, cls):
            value = value.expression
        if not isinstance(value, exp.Literal):
            return lit(value)
        return Column(value)

    def copy(self) -> "Column":
        return Column(self.expression.copy())

    def set_table_name(self, table_name: str, copy=False) -> "Column":
        expression = self.expression.copy() if copy else self.expression
        expression.set("table", exp.Identifier(this=table_name))
        return Column(expression)

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

    desc_nulls_last = desc

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

    def cast(self, dataType: str):
        """
        Functionality Difference: PySpark cast accepts a datatype instance of the datatype class
        Sqlglot doesn't currently replicate this class so it only accepts a string
        """
        new_expression = exp.Cast(this=self.column_expression, to=dataType)
        return Column(new_expression)

    def startswith(self, value: t.Union[str, "Column"]) -> "Column":
        value = self.ensure_literal(value)
        new_expression = exp.Anonymous(this="startswith", expressions=[self.column_expression, value.column_expression])
        return Column(new_expression)

