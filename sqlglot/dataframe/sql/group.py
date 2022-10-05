import typing as t

from sqlglot.dataframe.sql.column import Column
from sqlglot.dataframe.sql.operations import Operation, operation

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.dataframe import DataFrame

from pyspark.sql.dataframe import DataFrame


class GroupedData:
    def __init__(self, df: "DataFrame", group_by_cols: t.List["Column"]):
        self._df = df.copy()
        self.spark = df.spark
        self.group_by_cols = group_by_cols

    @operation(Operation.SELECT)
    def agg(self, *exprs: t.Union[Column, t.Dict[str, str]]) -> "DataFrame":
        if isinstance(exprs[0], dict):
            cols = [Column(f"{agg_func}({column_name})")
                       for column_name, agg_func in exprs[0].items()]
        else:
            cols = Column.ensure_cols(exprs)

        expression = (
            self
            ._df
            .expression
            .group_by(*[x.expression for x in self.group_by_cols])
            .select(
                *[x.expression for x in self.group_by_cols + cols],
                append=False
            )
        )
        return self._df.copy(expression=expression)
