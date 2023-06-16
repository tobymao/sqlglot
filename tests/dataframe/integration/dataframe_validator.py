import typing as t
import unittest
import warnings

import sqlglot
from tests.helpers import SKIP_INTEGRATION

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame


@unittest.skipIf(SKIP_INTEGRATION, "Skipping Integration Tests since `SKIP_INTEGRATION` is set")
class DataFrameValidator(unittest.TestCase):
    spark = None
    sqlglot = None
    df_employee = None
    df_store = None
    df_district = None
    spark_employee_schema = None
    sqlglot_employee_schema = None
    spark_store_schema = None
    sqlglot_store_schema = None
    spark_district_schema = None
    sqlglot_district_schema = None

    @classmethod
    def setUpClass(cls):
        from pyspark import SparkConf
        from pyspark.sql import SparkSession, types

        from sqlglot.dataframe.sql import types as sqlglotSparkTypes
        from sqlglot.dataframe.sql.session import SparkSession as SqlglotSparkSession

        # This is for test `test_branching_root_dataframes`
        config = SparkConf().setAll([("spark.sql.analyzer.failAmbiguousSelfJoin", "false")])
        cls.spark = (
            SparkSession.builder.master("local[*]")
            .appName("Unit-tests")
            .config(conf=config)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.sqlglot = SqlglotSparkSession()
        cls.spark_employee_schema = types.StructType(
            [
                types.StructField("employee_id", types.IntegerType(), False),
                types.StructField("fname", types.StringType(), False),
                types.StructField("lname", types.StringType(), False),
                types.StructField("age", types.IntegerType(), False),
                types.StructField("store_id", types.IntegerType(), False),
            ]
        )
        cls.sqlglot_employee_schema = sqlglotSparkTypes.StructType(
            [
                sqlglotSparkTypes.StructField(
                    "employee_id", sqlglotSparkTypes.IntegerType(), False
                ),
                sqlglotSparkTypes.StructField("fname", sqlglotSparkTypes.StringType(), False),
                sqlglotSparkTypes.StructField("lname", sqlglotSparkTypes.StringType(), False),
                sqlglotSparkTypes.StructField("age", sqlglotSparkTypes.IntegerType(), False),
                sqlglotSparkTypes.StructField("store_id", sqlglotSparkTypes.IntegerType(), False),
            ]
        )
        employee_data = [
            (1, "Jack", "Shephard", 37, 1),
            (2, "John", "Locke", 65, 1),
            (3, "Kate", "Austen", 37, 2),
            (4, "Claire", "Littleton", 27, 2),
            (5, "Hugo", "Reyes", 29, 100),
        ]
        cls.df_employee = cls.spark.createDataFrame(
            data=employee_data, schema=cls.spark_employee_schema
        )
        cls.dfs_employee = cls.sqlglot.createDataFrame(
            data=employee_data, schema=cls.sqlglot_employee_schema
        )
        cls.df_employee.createOrReplaceTempView("employee")

        cls.spark_store_schema = types.StructType(
            [
                types.StructField("store_id", types.IntegerType(), False),
                types.StructField("store_name", types.StringType(), False),
                types.StructField("district_id", types.IntegerType(), False),
                types.StructField("num_sales", types.IntegerType(), False),
            ]
        )
        cls.sqlglot_store_schema = sqlglotSparkTypes.StructType(
            [
                sqlglotSparkTypes.StructField("store_id", sqlglotSparkTypes.IntegerType(), False),
                sqlglotSparkTypes.StructField("store_name", sqlglotSparkTypes.StringType(), False),
                sqlglotSparkTypes.StructField(
                    "district_id", sqlglotSparkTypes.IntegerType(), False
                ),
                sqlglotSparkTypes.StructField("num_sales", sqlglotSparkTypes.IntegerType(), False),
            ]
        )
        store_data = [
            (1, "Hydra", 1, 37),
            (2, "Arrow", 2, 2000),
        ]
        cls.df_store = cls.spark.createDataFrame(data=store_data, schema=cls.spark_store_schema)
        cls.dfs_store = cls.sqlglot.createDataFrame(
            data=store_data, schema=cls.sqlglot_store_schema
        )
        cls.df_store.createOrReplaceTempView("store")

        cls.spark_district_schema = types.StructType(
            [
                types.StructField("district_id", types.IntegerType(), False),
                types.StructField("district_name", types.StringType(), False),
                types.StructField("manager_name", types.StringType(), False),
            ]
        )
        cls.sqlglot_district_schema = sqlglotSparkTypes.StructType(
            [
                sqlglotSparkTypes.StructField(
                    "district_id", sqlglotSparkTypes.IntegerType(), False
                ),
                sqlglotSparkTypes.StructField(
                    "district_name", sqlglotSparkTypes.StringType(), False
                ),
                sqlglotSparkTypes.StructField(
                    "manager_name", sqlglotSparkTypes.StringType(), False
                ),
            ]
        )
        district_data = [
            (1, "Temple", "Dogen"),
            (2, "Lighthouse", "Jacob"),
        ]
        cls.df_district = cls.spark.createDataFrame(
            data=district_data, schema=cls.spark_district_schema
        )
        cls.dfs_district = cls.sqlglot.createDataFrame(
            data=district_data, schema=cls.sqlglot_district_schema
        )
        cls.df_district.createOrReplaceTempView("district")
        sqlglot.schema.add_table("employee", cls.sqlglot_employee_schema, dialect="spark")
        sqlglot.schema.add_table("store", cls.sqlglot_store_schema, dialect="spark")
        sqlglot.schema.add_table("district", cls.sqlglot_district_schema, dialect="spark")

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)
        self.df_spark_store = self.df_store.alias("df_store")  # type: ignore
        self.df_spark_employee = self.df_employee.alias("df_employee")  # type: ignore
        self.df_spark_district = self.df_district.alias("df_district")  # type: ignore
        self.df_sqlglot_store = self.dfs_store.alias("store")  # type: ignore
        self.df_sqlglot_employee = self.dfs_employee.alias("employee")  # type: ignore
        self.df_sqlglot_district = self.dfs_district.alias("district")  # type: ignore

    def compare_spark_with_sqlglot(
        self, df_spark, df_sqlglot, no_empty=True, skip_schema_compare=False
    ) -> t.Tuple["SparkDataFrame", "SparkDataFrame"]:
        def compare_schemas(schema_1, schema_2):
            for schema in [schema_1, schema_2]:
                for struct_field in schema.fields:
                    struct_field.metadata = {}
            self.assertEqual(schema_1, schema_2)

        for statement in df_sqlglot.sql():
            actual_df_sqlglot = self.spark.sql(statement)  # type: ignore
        df_sqlglot_results = actual_df_sqlglot.collect()
        df_spark_results = df_spark.collect()
        if not skip_schema_compare:
            compare_schemas(df_spark.schema, actual_df_sqlglot.schema)
        self.assertEqual(df_spark_results, df_sqlglot_results)
        if no_empty:
            self.assertNotEqual(len(df_spark_results), 0)
            self.assertNotEqual(len(df_sqlglot_results), 0)
        return df_spark, actual_df_sqlglot

    @classmethod
    def get_explain_plan(cls, df: "SparkDataFrame", mode: str = "extended") -> str:
        return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)  # type: ignore
