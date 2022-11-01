from __future__ import annotations

import typing as t

from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.column import Column
from sqlglot.dataframe.sql.operations import Operation, operation

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.dataframe import DataFrame


class GroupedData:
    def __init__(self, df: DataFrame, group_by_cols: t.List[Column], last_op: Operation):
        self._df = df.copy()
        self.spark = df.spark
        self.last_op = last_op
        self.group_by_cols = group_by_cols

    def _get_function_applied_columns(
        self, func_name: str, cols: t.Tuple[str, ...]
    ) -> t.List[Column]:
        func_name = func_name.lower()
        return [getattr(F, func_name)(name).alias(f"{func_name}({name})") for name in cols]

    @operation(Operation.SELECT)
    def agg(self, *exprs: t.Union[Column, t.Dict[str, str]]) -> DataFrame:
        columns = (
            [Column(f"{agg_func}({column_name})") for column_name, agg_func in exprs[0].items()]
            if isinstance(exprs[0], dict)
            else exprs
        )
        cols = self._df._ensure_and_normalize_cols(columns)

        expression = self._df.expression.group_by(
            *[x.expression for x in self.group_by_cols]
        ).select(*[x.expression for x in self.group_by_cols + cols], append=False)
        return self._df.copy(expression=expression)

    def count(self) -> DataFrame:
        return self.agg(F.count("*").alias("count"))

    def mean(self, *cols: str) -> DataFrame:
        return self.avg(*cols)

    def avg(self, *cols: str) -> DataFrame:
        return self.agg(*self._get_function_applied_columns("avg", cols))

    def max(self, *cols: str) -> DataFrame:
        return self.agg(*self._get_function_applied_columns("max", cols))

    def min(self, *cols: str) -> DataFrame:
        return self.agg(*self._get_function_applied_columns("min", cols))

    def sum(self, *cols: str) -> DataFrame:
        return self.agg(*self._get_function_applied_columns("sum", cols))

    def pivot(self, *cols: str) -> DataFrame:
        raise NotImplementedError("Sum distinct is not currently implemented")
