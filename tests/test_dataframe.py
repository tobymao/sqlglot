import unittest

from pyspark.sql import functions as F
from sqlglot.dataframe import functions as SF


class TestDataframe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql import types
        from sqlglot.dataframe.session import SparkSession as SqlglotSparkSession
        # This is for test `test_branching_root_dataframes`
        config = SparkConf().setAll([('spark.sql.analyzer.failAmbiguousSelfJoin', 'false')])
        cls.spark = (
            SparkSession
            .builder
            .master("local[*]")
            .appName("Unit-tests")
            .config(conf=config)
            .getOrCreate()
        )
        cls.sqlglot = SqlglotSparkSession()
        employee_schema = types.StructType([
            types.StructField('employee_id', types.IntegerType(), False),
            types.StructField('fname', types.StringType(), False),
            types.StructField('lname', types.StringType(), False),
            types.StructField('age', types.IntegerType(), False),
            types.StructField('store_id', types.IntegerType(), False),
        ])
        employee_data = [
            (1, "Jack", "Shephard", 37, 1),
            (2, "John", "Locke", 65, 1),
            (3, "Kate", "Austen", 37, 2),
            (4, "Claire", "Littleton", 27, 2),
            (5, "Hugo", "Reyes", 29, 100),
        ]
        cls.df_employee = cls.spark.createDataFrame(data=employee_data, schema=employee_schema)
        cls.df_employee.createOrReplaceTempView("employee")

        store_schema = types.StructType([
            types.StructField("store_id", types.IntegerType(), False),
            types.StructField("store_name", types.StringType(), False),
            types.StructField("district_id", types.IntegerType(), False),
            types.StructField("num_sales", types.IntegerType(), False),
        ])

        store_data = [
            (1, "Hydra", 1, 37),
            (2, "Arrow", 2, 2000),
        ]
        cls.df_store = cls.spark.createDataFrame(data=store_data, schema=store_schema)
        cls.df_store.createOrReplaceTempView("store")

        district_schema = types.StructType([
            types.StructField("district_id", types.IntegerType(), False),
            types.StructField("district_name", types.StringType(), False),
            types.StructField("manager_name", types.StringType(), False),
        ])

        district_data = [
            (1, "Temple", "Dogen"),
            (2, "Lighthouse", "Jacob"),
        ]
        cls.df_district = cls.spark.createDataFrame(data=district_data, schema=district_schema)
        cls.df_district.createOrReplaceTempView("district")

    def setUp(self) -> None:
        self.df_spark_store = self.df_store.alias('df_store')
        self.df_spark_employee = self.df_employee.alias('df_employee')
        self.df_spark_district = self.df_district.alias('df_district')
        self.df_sqlglot_store = self.sqlglot.read.table('store')
        self.df_sqlglot_employee = self.sqlglot.read.table('employee')
        self.df_sqlglot_district = self.sqlglot.read.table('district')

    @classmethod
    def compare_spark_with_sqlglot(cls, df_spark, df_sqlglot, no_empty=True):
        def compare_schemas(schema_1, schema_2):
            for schema in [schema_1, schema_2]:
                for struct_field in schema.fields:
                    struct_field.metadata = {}
            assert schema_1 == schema_2
        df_sqlglot = cls.spark.sql(df_sqlglot.sql())
        df_spark_results = df_spark.collect()
        df_sqlglot_results = df_sqlglot.collect()
        compare_schemas(df_spark.schema, df_sqlglot.schema)
        assert df_spark_results == df_sqlglot_results
        if no_empty:
            assert len(df_spark_results) != 0
            assert len(df_sqlglot_results) != 0

    def test_simple_select(self):
        df_employee = self.df_spark_employee.select(F.col("employee_id"))
        dfs_employee = self.df_sqlglot_employee.select(SF.col("employee_id"))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_simple_select_df_attribute(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee.employee_id)
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee.employee_id)
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_simple_select_df_dict(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee['employee_id'])
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee['employee_id'])
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_multiple_selects(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee['employee_id'], F.col('fname'), self.df_spark_employee.lname)
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee['employee_id'], SF.col('fname'), self.df_sqlglot_employee.lname)
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_alias_no_op(self):
        df_employee = self.df_spark_employee.alias("df_employee")
        dfs_employee = self.df_sqlglot_employee.alias("dfs_employee")
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

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
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_clause_single(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_clause_multiple_and(self):
        df_employee = self.df_spark_employee.where((F.col("age") == F.lit(37)) & (F.col("fname") == F.lit("Jack")))
        dfs_employee = self.df_sqlglot_employee.where((SF.col("age") == SF.lit(37)) & (SF.col("fname") == SF.lit("Jack")))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)
        
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
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_clause_multiple_or(self):
        df_employee = self.df_spark_employee.where((F.col("age") == F.lit(37)) | (F.col("fname") == F.lit("Kate")))
        dfs_employee = self.df_sqlglot_employee.where((SF.col("age") == SF.lit(37)) | (SF.col("fname") == SF.lit("Kate")))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

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
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

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
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_multiple_chained(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37)).where(self.df_spark_employee.fname == F.lit("Jack"))
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37)).where(self.df_sqlglot_employee.fname == SF.lit("Jack"))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_operators(self):
        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] < F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] < SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] > F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] > SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] != F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] != SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] == SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_group_by(self):
        df_employee = self.df_spark_employee.groupBy(self.df_spark_employee.age).agg(F.min(self.df_spark_employee.employee_id))
        dfs_employee = self.df_sqlglot_employee.groupBy(self.df_sqlglot_employee.age).agg(SF.min(self.df_sqlglot_employee.employee_id))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_group_by_where_non_aggregate(self):
        df_employee = (
            self
            .df_spark_employee
            .groupBy(self.df_spark_employee.age)
            .agg(F.min(self.df_spark_employee.employee_id).alias("min_employee_id"))
            .where(F.col("age") > F.lit(50))
        )
        dfs_employee = (
            self
            .df_sqlglot_employee
            .groupBy(self.df_sqlglot_employee.age)
            .agg(SF.min(self.df_sqlglot_employee.employee_id).alias("min_employee_id"))
            .where(SF.col("age") > SF.lit(50))
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_group_by_where_aggregate_like_having(self):
        df_employee = (
            self
            .df_spark_employee
            .groupBy(self.df_spark_employee.age)
            .agg(F.min(self.df_spark_employee.employee_id).alias("min_employee_id"))
            .where(F.col("min_employee_id") > F.lit(1))
        )
        dfs_employee = (
            self
            .df_sqlglot_employee
            .groupBy(self.df_sqlglot_employee.age)
            .agg(SF.min(self.df_sqlglot_employee.employee_id).alias("min_employee_id"))
            .where(SF.col("min_employee_id") > SF.lit(1))
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_join_inner(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=["store_id"],
                how="inner"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                F.col('store_id'),
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=["store_id"],
                how="inner"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                SF.col('store_id'),
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_single(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=self.df_spark_employee.store_id == self.df_spark_store.store_id,
                how="inner"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                self.df_spark_employee.store_id,
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
                how="inner"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                self.df_sqlglot_employee.store_id,
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_multiple(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=[
                    self.df_spark_employee.store_id == self.df_spark_store.store_id,
                    self.df_spark_employee.age == self.df_spark_store.num_sales,
                ],
                how="inner"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                self.df_spark_employee.store_id,
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=[
                    self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
                    self.df_sqlglot_employee.age == self.df_sqlglot_store.num_sales,
                ],
                how="inner"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                self.df_sqlglot_employee.store_id,
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_multiple_bitwise_and(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=(self.df_spark_employee.store_id == self.df_spark_store.store_id)
                    & (self.df_spark_employee.age == self.df_spark_store.num_sales),
                how="inner"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                self.df_spark_employee.store_id,
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=(self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id)
                   & (self.df_sqlglot_employee.age == self.df_sqlglot_store.num_sales),
                how="inner"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                self.df_sqlglot_employee.store_id,
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_left_outer(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=["store_id"],
                how="left_outer"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                F.col('store_id'),
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=["store_id"],
                how="left_outer"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                SF.col('store_id'),
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_full_outer(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on=["store_id"],
                how="full_outer"
            )
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee['fname'],
                F.col('lname'),
                F.col('age'),
                F.col('store_id'),
                self.df_spark_store.store_name,
                self.df_spark_store['num_sales']
            )
        )
        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on=["store_id"],
                how="full_outer"
            )
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee['fname'],
                SF.col('lname'),
                SF.col('age'),
                SF.col('store_id'),
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store['num_sales']
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_branching_root_dataframes(self):
        """
        Test a pattern that has non-intuitive behavior in spark

        Scenario: You do a self-join in a dataframe using an original dataframe and then a modified version
        of it. You then reference the columns by the dataframe name instead of the column function.
        Spark will use the root dataframe's column in the result.
        """
        df_hydra_employees_only = self.df_spark_employee.where(F.col("store_id") == F.lit(1))
        df_joined = (
            self.df_spark_employee.where(F.col("store_id") == F.lit(2)).alias("df_arrow_employees_only")
            .join(
                df_hydra_employees_only.alias("df_hydra_employees_only"),
                on=["store_id"],
                how="full_outer",
            )
            .select(
                self.df_spark_employee.fname,
                F.col("df_arrow_employees_only.fname"),
                df_hydra_employees_only.fname,
                F.col("df_hydra_employees_only.fname")
            )
        )

        dfs_hydra_employees_only = self.df_sqlglot_employee.where(SF.col("store_id") == SF.lit(1))
        dfs_joined = (
            self.df_sqlglot_employee.where(SF.col("store_id") == SF.lit(2)).alias("dfs_arrow_employees_only")
            .join(
                dfs_hydra_employees_only.alias("dfs_hydra_employees_only"),
                on=["store_id"],
                how="full_outer",
            )
            .select(
                self.df_sqlglot_employee.fname,
                SF.col("dfs_arrow_employees_only.fname"),
                dfs_hydra_employees_only.fname,
                SF.col("dfs_hydra_employees_only.fname")
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_basic_union(self):
        df_unioned = (
            self.df_spark_employee.select(F.col("employee_id"), F.col("age"))
            .union(
                self.df_spark_store.select(F.col("store_id"), F.col("num_sales"))
            )
        )

        dfs_unioned = (
            self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("age"))
            .union(
                self.df_sqlglot_store.select(SF.col("store_id"), SF.col("num_sales"))
            )
        )
        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_union_with_join(self):
        df_joined = (
            self.df_spark_employee
            .join(
                self.df_spark_store,
                on="store_id",
                how="inner",
            )
        )
        df_unioned = (
            df_joined.select(F.col("store_id"), F.col("store_name"))
            .union(
                self.df_spark_district.select(F.col("district_id"), F.col("district_name"))
            )
        )

        dfs_joined = (
            self.df_sqlglot_employee
            .join(
                self.df_sqlglot_store,
                on="store_id",
                how="inner",
            )
        )
        dfs_unioned = (
            dfs_joined.select(SF.col("store_id"), SF.col("store_name"))
            .union(
                self.df_sqlglot_district.select(SF.col("district_id"), SF.col("district_name"))
            )
        )

        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_double_union(self):
        df_unioned = (
            self.df_spark_employee.select(F.col("employee_id"), F.col("fname"))
            .union(
                self.df_spark_store.select(F.col("store_id"), F.col("store_name"))
            )
            .union(
                self.df_spark_district.select(F.col("district_id"), F.col("district_name"))
            )
        )

        dfs_unioned = (
            self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("fname"))
            .union(
                self.df_sqlglot_store.select(SF.col("store_id"), SF.col("store_name"))
            )
            .union(
                self.df_sqlglot_district.select(SF.col("district_id"), SF.col("district_name"))
            )
        )

        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

