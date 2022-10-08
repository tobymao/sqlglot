from __future__ import annotations

import typing as t
from copy import copy

import sqlglot
from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.dataframe import DataFrame
    from sqlglot.dataframe.sql.session import SparkSession


class DataFrameReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def table(self, tableName: str) -> DataFrame:
        from sqlglot.dataframe.sql.dataframe import DataFrame

        sqlglot.schema.add_table(tableName)
        return DataFrame(self.spark, exp.Select().from_(tableName).select(*sqlglot.schema.column_names(tableName)))


class DataFrameWriter:
    def __init__(self, df: DataFrame, spark: SparkSession = None, mode: str = None, by_name: bool = False):
        self._df = df
        self._spark = spark or df.spark
        self._mode = mode
        self._by_name = by_name

    def copy(self, **kwargs) -> DataFrameWriter:
        kwargs = {
            **{k: copy(v) for k, v in vars(self).copy().items()},
            **kwargs
        }
        return DataFrameWriter(**{k[1:] if k.startswith("_") else k: v for k, v in kwargs.items()})

    def sql(self, **kwargs) -> str:
        return self._df.sql(**kwargs)

    def mode(self, saveMode: t.Optional[str]) -> DataFrameWriter:
        return self.copy(_mode=saveMode)

    @property
    def byName(self):
        return self.copy(by_name=True)

    def insertInto(self, tableName: str, overwrite: t.Optional[bool] = None) -> DataFrameWriter:
        df = self._df.copy()
        if self._by_name:
            columns = sqlglot.schema.column_names(tableName, only_visible=True)
            df = df._convert_leaf_to_cte().select(*columns)
        expression_without_cte = df.expression.copy()
        expression_without_cte.set("with", None)
        insert_expression = exp.Insert(**{
            "this": exp.to_table(tableName),
            "expression": expression_without_cte,
            "overwrite": overwrite,
            "with": df.expression.args.get("with")
        })
        return self.copy(_df=df.copy(expression=insert_expression))

    def saveAsTable(self,
                    name: str,
                    format: t.Optional[str] = None,
                    mode: t.Optional[str] = None):
        if format is not None:
            raise NotImplementedError("Providing Format in the save as table is not supported")
        exists, replace, mode = None, None, mode or str(self._mode)
        if mode == "append":
            return self.insertInto(name)
        if mode == "ignore":
            exists = True
        if mode == "overwrite":
            replace = True
        ctas_expression = exp.Create(
            this=exp.to_table(name),
            kind="TABLE",
            expression=self._df.expression,
            exists=exists,
            replace=replace,
        )
        return self.copy(_df=self._df.copy(expression=ctas_expression))
