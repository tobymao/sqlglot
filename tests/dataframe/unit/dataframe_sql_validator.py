import typing as t
import unittest

from sqlglot.dataframe.sql import types
from sqlglot.dataframe.sql.dataframe import DataFrame
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.helper import ensure_list


class DataFrameSQLValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession()
        self.employee_schema = types.StructType(
            [
                types.StructField("employee_id", types.IntegerType(), False),
                types.StructField("fname", types.StringType(), False),
                types.StructField("lname", types.StringType(), False),
                types.StructField("age", types.IntegerType(), False),
                types.StructField("store_id", types.IntegerType(), False),
            ]
        )
        employee_data = [
            (1, "Jack", "Shephard", 37, 1),
            (2, "John", "Locke", 65, 1),
            (3, "Kate", "Austen", 37, 2),
            (4, "Claire", "Littleton", 27, 2),
            (5, "Hugo", "Reyes", 29, 100),
        ]
        self.df_employee = self.spark.createDataFrame(
            data=employee_data, schema=self.employee_schema
        )

    def compare_sql(
        self, df: DataFrame, expected_statements: t.Union[str, t.List[str]], pretty=False
    ):
        actual_sqls = df.sql(pretty=pretty)
        expected_statements = ensure_list(expected_statements)
        self.assertEqual(len(expected_statements), len(actual_sqls))
        for expected, actual in zip(expected_statements, actual_sqls):
            self.assertEqual(expected, actual)
