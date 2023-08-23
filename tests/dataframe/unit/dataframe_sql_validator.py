from sqlglot.dataframe.sql import types
from sqlglot.dataframe.sql.session import SparkSession
from tests.dataframe.unit.dataframe_test_base import DataFrameTestBase


class DataFrameSQLValidator(DataFrameTestBase):
    def setUp(self) -> None:
        super().setUp()
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
