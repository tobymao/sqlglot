import unittest

from sqlglot import expressions as exp
from sqlglot.dataframe.sql import types
from sqlglot.dataframe.sql.dataframe import DataFrame
from sqlglot.dataframe.sql.session import SparkSession


class TestDataframeColumn(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession()
        cls.employee_schema = types.StructType(
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
        cls.df_employee = cls.spark.createDataFrame(data=employee_data, schema=cls.employee_schema)

    def test_hash_select_expression(self):
        expression = exp.select("cola").from_("table")
        self.assertEqual("t17051", DataFrame._create_hash_from_expression(expression))

    def test_columns(self):
        self.assertEqual(self.df_employee.columns, ["employee_id", "fname", "lname", "age", "store_id"])
