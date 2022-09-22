from collections.abc import Iterable
import datetime
from itertools import chain
import typing as t

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.window import WindowSpec

if t.TYPE_CHECKING:
    from sqlglot.dataframe._typing import ColumnOrName, ColumnOrPrimitive, DateTimeLiteral, DecimalLiteral, Literals, Primitives


flatten = chain.from_iterable


class Column:
    def __init__(self, expression: t.Union[t.Any, exp.Expression]):
        if isinstance(expression, str):
            expression = sqlglot.parse_one(expression)
        elif isinstance(expression, Column):
            expression = expression.expression
        self.expression = expression
        if self.expression is None or isinstance(self.expression, (int, float, bool, dict, Iterable, datetime.date)):
            self.expression = self._lit(expression).expression

    def __repr__(self):
        return repr(self.expression)

    def __hash__(self):
        return hash(self.expression)

    def __eq__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.EQ, other)

    def __ne__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.NEQ, other)

    def __gt__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.GT, other)

    def __ge__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.GTE, other)

    def __lt__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.LT, other)

    def __le__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.LTE, other)

    def __and__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.And, other)

    def __or__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Or, other)

    def __mod__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Mod, other)

    def __add__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Add, other)

    def __sub__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Sub, other)

    def __mul__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Mul, other)

    def __truediv__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Div, other)

    def __div__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.binary_op(exp.Div, other)

    def __neg__(self) -> "Column":
        return self.unary_op(exp.Neg)

    def __radd__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Add, other)

    def __rsub__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Sub, other)

    def __rmul__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Mul, other)

    def __rdiv__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Div, other)

    def __rtruediv__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Div, other)

    def __rmod__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Mod, other)

    def __pow__(self, power: "ColumnOrPrimitive", modulo=None):
        return Column(exp.Pow(this=self.expression, power=Column(power).expression))

    def __rpow__(self, power: "ColumnOrPrimitive"):
        return Column(exp.Pow(this=Column(power).expression, power=self.expression))

    def __invert__(self):
        return self.unary_op(exp.Not)

    def __rand__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.And, other)

    def __ror__(self, other: "ColumnOrPrimitive") -> "Column":
        return self.inverse_binary_op(exp.Or, other)

    @classmethod
    def ensure_col(cls, value: t.Union["ColumnOrName", int, float]):
        return cls(value)

    @classmethod
    def _lit(cls, value: t.Optional[t.Any] = None) -> "Column":
        if value is None:
            return cls(exp.Null())
        if isinstance(value, (str, int, bool, float)):
            value_formatted = str(value).lower() if isinstance(value, bool) else str(value)
            return cls(exp.Literal(this=value_formatted, is_string=isinstance(value, str)))
        if isinstance(value, dict):
            columns = [
                cls._lit(v).alias(k).expression
                for k, v in value.items()
            ]
            return cls(exp.Struct(expressions=columns))
        if isinstance(value, Iterable):
            expressions = [cls._lit(x).expression for x in value]
            return cls(exp.Column(this=exp.Array(expressions=expressions)))
        if isinstance(value, datetime.datetime):
            datetime_literal = exp.Literal(this=value.strftime("%Y-%m-%d %H:%M:%S"), is_string=True)
            return cls(exp.Column(this=exp.StrToTime(this=datetime_literal,
                                                     format=exp.Literal(this='YYYY-MM-DD HH:MM:SS', is_string=True))))
        if isinstance(value, datetime.date):
            date_literal = exp.Literal(this=value.strftime("%Y-%m-%d"), is_string=True)
            return cls(exp.Column(this=exp.StrToDate(this=date_literal,
                                                     format=exp.Literal(this="YYYY-MM-DD", is_string=True))))
        return cls(value)

    @classmethod
    def invoke_anonymous_function(cls, column: t.Union["ColumnOrName", None], func_name: str, *args) -> "Column":
        column = [cls.ensure_col(column)] if column is not None else []
        args = [cls.ensure_col(arg) for arg in args]
        expressions = [x.expression for x in column + args]
        new_expression = exp.Anonymous(this=func_name.upper(), expressions=expressions)
        return Column(new_expression)

    @classmethod
    def invoke_expression_over_column(cls, column: t.Union["ColumnOrName", None], callable_expression: t.Callable,
                                      **kwargs) -> "Column":
        column = cls.ensure_col(column) if column is not None else None
        new_expression = (
            callable_expression(this=column.column_expression, **kwargs)
            if column is not None
            else callable_expression(**kwargs)
        )
        return Column(new_expression)

    def binary_op(self, clazz: t.Callable, other: "Column", **kwargs) -> "Column":
        return Column(clazz(this=self.column_expression, expression=Column(other).column_expression, **kwargs))

    def inverse_binary_op(self, clazz: t.Callable, other: "Column", **kwargs) -> "Column":
        return Column(clazz(this=Column(other).column_expression, expression=self.column_expression, **kwargs))


    def unary_op(self, clazz: t.Callable, **kwargs) -> "Column":
        return Column(clazz(this=self.column_expression, **kwargs))

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
        if isinstance(self.expression, exp.Null):
            return "NULL"
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
        if not isinstance(self.expression, exp.Case):
            return column_with_if
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
        value = self._lit(value) if not isinstance(value, Column) else value
        return self.invoke_anonymous_function(self, "STARTSWITH", value)

    def endswith(self, value: t.Union[str, "Column"]) -> "Column":
        value = self._lit(value) if not isinstance(value, Column) else value
        return self.invoke_anonymous_function(self, "ENDSWITH", value)

    def rlike(self, regexp: str) -> "Column":
        return self.invoke_expression_over_column(self, exp.RegexpLike, expression=self._lit(regexp).expression)

    def like(self, other: str):
        return self.invoke_expression_over_column(self, exp.Like, expression=self._lit(other).expression)

    def ilike(self, other: str):
        return self.invoke_expression_over_column(self, exp.ILike, expression=self._lit(other).expression)

    def substr(self, startPos: t.Union[int, "Column"], length: t.Union[int, "Column"]) -> "Column":
        startPos = self._lit(startPos) if not isinstance(startPos, Column) else startPos
        length = self._lit(length) if not isinstance(length, Column) else length
        return Column.invoke_expression_over_column(self, exp.Substring, start=startPos.expression, length=length.expression)

    def isin(self, *cols: t.Union["Primitives", t.Iterable["Primitives"]]):
        cols = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols
        expressions = [self._lit(x).expression for x in cols]
        return Column.invoke_expression_over_column(self, exp.In, expressions=expressions)

    def between(
        self,
        lowerBound: t.Union["Column", "Literals", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: t.Union["Column", "Literals", "DateTimeLiteral", "DecimalLiteral"],
    ) -> "Column":
        lower_bound_exp = self._lit(lowerBound) if not isinstance(lowerBound, Column) else lowerBound
        upper_bound_exp = self._lit(upperBound) if not isinstance(upperBound, Column) else upperBound
        return Column(exp.Between(this=self.column_expression,
                                  low=lower_bound_exp.expression,
                                  high=upper_bound_exp.expression))

    def over(self, window: "WindowSpec") -> "Column":
        window_args = window.expression.args
        window_args["this"] = self.column_expression
        return Column(exp.Window(**window_args))
