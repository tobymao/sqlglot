import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe.util import ensure_strings, ensure_sqlglot_column

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame


def col(column_name: str) -> "Column":
    return Column(column_name)


def lit(value: t.Optional[t.Any] = None) -> "Column":
    if value is None:
        return Column(exp.Null())
    return Column(exp.Literal(this=str(value), is_string=isinstance(value, str)))


def greatest(*cols: t.List[t.Union[str, "Column"]]) -> "Column":
    cols = ensure_strings(cols)
    return Column(exp.Greatest(this=cols[0], expressions=cols[1:]))


def count_distinct(col: "Column") -> "Column":
    col = ensure_strings([col])[0]
    return Column(exp.Count(this=exp.Distinct(this=col)))


def countDistinct(col: "Column") -> "Column":
    return count_distinct(col)


def min(col: "Column") -> "Column":
    col = ensure_sqlglot_column(col)
    return Column(exp.Min(this=col))


def when(condition: "Column", value: t.Any) -> "Column":
    true_value = value if isinstance(value, Column) else lit(value)
    return Column(exp.Case(ifs=[exp.If(this=condition.column_expression, true=true_value.column_expression)]))


def asc(col: "Column") -> "Column":
    return col.asc()


def desc(col: "Column"):
    return col.desc()


def broadcast(df: "DataFrame") -> "DataFrame":
    return df.hint("broadcast")
