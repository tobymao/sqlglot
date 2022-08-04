import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe.util import ensure_strings, ensure_sqlglot_column


def col(column_name: str):
    return Column(column_name)


def lit(value: t.Any):
    return Column(exp.Literal(this=value, is_string=isinstance(value, str)))


def greatest(*cols: t.List[t.Union[str, "Column"]]):
    cols = ensure_strings(cols)
    return Column(exp.Greatest(this=cols[0], expressions=cols[1:]))


def count_distinct(col):
    col = ensure_strings([col])[0]
    return Column(exp.Count(this=exp.Distinct(this=col)))


def countDistinct(col):
    return count_distinct(col)


def min(col):
    col = ensure_sqlglot_column(col)
    return Column(exp.Min(this=col))
