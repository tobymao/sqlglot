import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe.dataframe import DataFrame

if t.TYPE_CHECKING:
    from sqlglot.dataframe.session import SparkSession


class DataFrameReader:
    def __init__(self, spark: "SparkSession"):
        self.spark = spark

    def table(self, table_name: str) -> "DataFrame":
        return DataFrame(self.spark, exp.Select().from_(table_name).select("*"), branch_id=DataFrame.random_name)
