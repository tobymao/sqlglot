import unittest

from pyspark.sql import functions as F
from sqlglot.dataframe import functions as SF


class TestDataframe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        from pyspark.sql import types
        from sqlglot.dataframe.session import SparkSession as SqlglotSparkSession
        cls.spark = (
            SparkSession
            .builder
            .master("local[*]")
            .appName("Unit-tests")
            .getOrCreate()
        )
        cls.sqlglot = SqlglotSparkSession()
        employee_schema = types.StructType([
            types.StructField('employee_id', types.IntegerType(), False),
            types.StructField('fname', types.StringType(), False),
            types.StructField('lname', types.StringType(), False),
            types.StructField('age', types.IntegerType(), False),
        ])
        employee_data = [
            (1, "Jack", "Shephard", 37),
            (2, "John", "Locke", 65),
            (3, "Kate", "Austen", 34),
            (4, "Claire", "Littleton", 27),
        ]
        df_employee = cls.spark.createDataFrame(data=employee_data, schema=employee_schema)
        df_employee.createOrReplaceTempView("employee")

        store_schema = types.StructType([
            types.StructField("store_id", types.IntegerType(), False),
            types.StructField("store_name", types.StringType(), False),
            types.StructField("num_sales", types.IntegerType(), False),
        ])

        store_data = [
            (1, "Hydra", 1000),
            (2, "Arrow", 2000),
        ]
        df_store = cls.spark.createDataFrame(data=store_data, schema=store_schema)
        df_store.createOrReplaceTempView("store")
        cls.df_spark_store = df_store
        cls.df_spark_employee = df_employee
        cls.df_sqlglot_store = cls.sqlglot.read.table("store")
        cls.df_sqlglot_employee = cls.sqlglot.read.table("employee")

    @classmethod
    def compare_spark_with_sqlglot(cls, df_spark, df_sqlglot, no_empty=True):
        df_sqlglot = cls.spark.sql(df_sqlglot.sql())
        if df_spark.schema != df_sqlglot.schema:
            return False
        df_spark_results = df_spark.collect()
        df_sqlglot_results = df_sqlglot.collect()
        if df_spark_results != df_sqlglot_results:
            return False
        if no_empty and (len(df_spark_results) == 0 or len(df_sqlglot_results) == 0):
            return False
        return True

    def test_simple_select(self):
        df_employee = self.df_spark_employee.select(F.col("employee_id"))
        dfs_employee = self.df_sqlglot_employee.select(SF.col("employee_id"))
        assert(self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_simple_select_df_attribute(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee.employee_id)
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee.employee_id)
        assert(self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_simple_select_df_dict(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee['employee_id'])
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee['employee_id'])
        assert(self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_multiple_selects(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee['employee_id'], F.col('fname'), self.df_spark_employee.lname)
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee['employee_id'], SF.col('fname'), self.df_sqlglot_employee.lname)
        assert(self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_alias_no_op(self):
        df_employee = self.df_spark_employee.alias("df_employee")
        dfs_employee = self.df_sqlglot_employee.alias("dfs_employee")
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_alias_with_select(self):
        df_employee = (
            self
            .df_spark_employee
            .alias("df_employee")
            .select(
                self.df_spark_employee['employee_id'],
                F.col('df_employee.fname'),
                self.df_spark_employee.lname
            )
        )
        dfs_employee = (
            self
            .df_sqlglot_employee
            .alias("dfs_employee")
            .select(
                self.df_sqlglot_employee['employee_id'],
                SF.col('dfs_employee.fname'),
                self.df_sqlglot_employee.lname
            )
        )
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_clause_single(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37))
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_clause_multiple_and(self):
        df_employee = self.df_spark_employee.where((F.col("age") == F.lit(37)) & (F.col("fname") == F.lit("Jack")))
        dfs_employee = self.df_sqlglot_employee.where((SF.col("age") == SF.lit(37)) & (SF.col("fname") == SF.lit("Jack")))
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))
        
    def test_where_many_and(self):
        df_employee = self.df_spark_employee.where(
            (F.col("age") == F.lit(37))
            & (F.col("fname") == F.lit("Jack"))
            & (F.col("lname") == F.lit("Shephard"))
            & (F.col("employee_id") == F.lit(1))
        )
        dfs_employee = self.df_sqlglot_employee.where(
            (SF.col("age") == SF.lit(37))
            & (SF.col("fname") == SF.lit("Jack"))
            & (SF.col("lname") == SF.lit("Shephard"))
            & (SF.col("employee_id") == SF.lit(1))
        )
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_clause_multiple_or(self):
        df_employee = self.df_spark_employee.where((F.col("age") == F.lit(37)) | (F.col("fname") == F.lit("Kate")))
        dfs_employee = self.df_sqlglot_employee.where((SF.col("age") == SF.lit(37)) | (SF.col("fname") == SF.lit("Kate")))
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_many_or(self):
        df_employee = self.df_spark_employee.where(
            (F.col("age") == F.lit(37))
            | (F.col("fname") == F.lit("Kate"))
            | (F.col("lname") == F.lit("Littleton"))
            | (F.col("employee_id") == F.lit(2))
        )
        dfs_employee = self.df_sqlglot_employee.where(
            (SF.col("age") == SF.lit(37))
            | (SF.col("fname") == SF.lit("Kate"))
            | (SF.col("lname") == SF.lit("Littleton"))
            | (SF.col("employee_id") == SF.lit(2))
        )
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_mixed_and_or(self):
        df_employee = self.df_spark_employee.where(
            (
                    (F.col("age") == F.lit(65))
                    & (F.col("fname") == F.lit("John"))
            )
            |
            (
                    (F.col("lname") == F.lit("Shephard"))
                    & (F.col("age") == F.lit(37))
            )
        )
        dfs_employee = self.df_sqlglot_employee.where(
            (
                    (SF.col("age") == SF.lit(65))
                    & (SF.col("fname") == SF.lit("John"))
            )
            |
            (
                    (SF.col("lname") == SF.lit("Shephard"))
                    & (SF.col("age") == SF.lit(37))
            )
        )
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))

    def test_where_multiple_chained(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37)).where(self.df_spark_employee.fname == F.lit("Jack"))
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37)).where(self.df_sqlglot_employee.fname == SF.lit("Jack"))
        assert (self.compare_spark_with_sqlglot(df_employee, dfs_employee))
