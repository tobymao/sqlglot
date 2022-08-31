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
    def compare_spark_with_sqlglot(cls, df_spark, df_sqlglot, no_empty=True, skip_schema_compare=False):
        def compare_schemas(schema_1, schema_2):
            for schema in [schema_1, schema_2]:
                for struct_field in schema.fields:
                    struct_field.metadata = {}
            assert schema_1 == schema_2
        df_sqlglot = cls.spark.sql(df_sqlglot.sql())
        df_spark_results = df_spark.collect()
        df_sqlglot_results = df_sqlglot.collect()
        if not skip_schema_compare:
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

    def test_case_when_otherwise(self):
        df = (
            self.df_spark_employee
            .select(
                F
                .when((F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)), F.lit("between 40 and 60"))
                .when(F.col("age") < F.lit(40), "less than 40")
                .otherwise("greater than 60")
            )
        )

        dfs = (
            self.df_sqlglot_employee
            .select(
                SF
                .when((SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)), SF.lit("between 40 and 60"))
                .when(SF.col("age") < SF.lit(40), "less than 40")
                .otherwise("greater than 60")
            )
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_case_when_no_otherwise(self):
        df = (
            self.df_spark_employee
            .select(
                F
                .when((F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)), F.lit("between 40 and 60"))
                .when(F.col("age") < F.lit(40), "less than 40")
            )
        )

        dfs = (
            self.df_sqlglot_employee
            .select(
                SF
                .when((SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)), SF.lit("between 40 and 60"))
                .when(SF.col("age") < SF.lit(40), "less than 40")
            )
        )

        self.compare_spark_with_sqlglot(df, dfs)

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

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] <= F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] <= SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] > F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] > SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] >= F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] >= SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] != F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] != SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] == SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] % F.lit(5) == F.lit(0))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] % SF.lit(5) == SF.lit(0))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] + F.lit(5) > F.lit(28))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] + SF.lit(5) > SF.lit(28))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] - F.lit(5) > F.lit(28))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] - SF.lit(5) > SF.lit(28))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee['age'] * F.lit(.5) == self.df_spark_employee['age'] / F.lit(2))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee['age'] * SF.lit(.5) == self.df_sqlglot_employee['age'] / SF.lit(2))
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

    def test_double_union_all(self):
        df_unioned = (
            self.df_spark_employee.select(F.col("employee_id"), F.col("fname"))
            .unionAll(
                self.df_spark_store.select(F.col("store_id"), F.col("store_name"))
            )
            .unionAll(
                self.df_spark_district.select(F.col("district_id"), F.col("district_name"))
            )
        )

        dfs_unioned = (
            self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("fname"))
            .unionAll(
                self.df_sqlglot_store.select(SF.col("store_id"), SF.col("store_name"))
            )
            .unionAll(
                self.df_sqlglot_district.select(SF.col("district_id"), SF.col("district_name"))
            )
        )

        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_order_by_default(self):
        df = (
            self.df_spark_store
            .groupBy(F.col("district_id"))
            .agg(F.min("num_sales"))
            .orderBy(F.col("district_id") )

        )

        dfs = (
            self.df_sqlglot_store
            .groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales"))
            .orderBy(SF.col("district_id"))
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_array_bool(self):
        df = (
            self.df_spark_store
            .groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.col("total_sales"), F.col("district_id"), ascending=[1, 0])
        )

        dfs = (
            self.df_sqlglot_store
            .groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=[1, 0])
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_single_bool(self):
        df = (
            self.df_spark_store
            .groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.col("total_sales"), F.col("district_id"), ascending=False)
        )

        dfs = (
            self.df_sqlglot_store
            .groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=False)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method(self):
        df = (
            self.df_spark_store
                .groupBy(F.col("district_id"))
                .agg(F.min("num_sales").alias("total_sales"))
                .orderBy(F.col("total_sales").asc(), F.col("district_id").desc())
        )

        dfs = (
            self.df_sqlglot_store
                .groupBy(SF.col("district_id"))
                .agg(SF.min("num_sales").alias("total_sales"))
                .orderBy(SF.col("total_sales").asc(), SF.col("district_id").desc())
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method_nulls(self):
        df = (
            self.df_spark_store
            .groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.when(F.col("district_id") == F.lit(2), F.col("district_id")).asc_nulls_last())
        )

        dfs = (
            self.df_sqlglot_store
            .groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.when(SF.col("district_id") == SF.lit(2), SF.col("district_id")).asc_nulls_last())
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method_nulls(self):
        df = (
            self.df_spark_store
            .groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.when(F.col("district_id") == F.lit(1), F.col("district_id")).desc_nulls_first())
        )

        dfs = (
            self.df_sqlglot_store
            .groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.when(SF.col("district_id") == SF.lit(1), SF.col("district_id")).desc_nulls_first())
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_intersect(self):
        df_employee_duplicate = (
            self.df_spark_employee
            .select(F.col("employee_id"), F.col("store_id"))
            .union(
                self.df_spark_employee
                .select(F.col("employee_id"), F.col("store_id"))
            )
        )

        df_store_duplicate = (
            self.df_spark_store
            .select(F.col("store_id"), F.col("district_id"))
            .union(
                self.df_spark_store
                .select(F.col("store_id"), F.col("district_id"))
            )
        )

        df = (
            df_employee_duplicate
            .intersect(df_store_duplicate)
        )

        dfs_employee_duplicate = (
            self.df_sqlglot_employee
            .select(SF.col("employee_id"), SF.col("store_id"))
            .union(
                self.df_sqlglot_employee
                .select(SF.col("employee_id"), SF.col("store_id"))
            )
        )

        dfs_store_duplicate = (
            self.df_sqlglot_store
            .select(SF.col("store_id"), SF.col("district_id"))
            .union(
                self.df_sqlglot_store
                .select(SF.col("store_id"), SF.col("district_id"))
            )
        )

        dfs = (
            dfs_employee_duplicate
            .intersect(dfs_store_duplicate)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_intersect_all(self):
        df_employee_duplicate = (
            self.df_spark_employee
            .select(F.col("employee_id"), F.col("store_id"))
            .union(
                self.df_spark_employee
                .select(F.col("employee_id"), F.col("store_id"))
            )
        )

        df_store_duplicate = (
            self.df_spark_store
            .select(F.col("store_id"), F.col("district_id"))
            .union(
                self.df_spark_store
                .select(F.col("store_id"), F.col("district_id"))
            )
        )

        df = (
            df_employee_duplicate
            .intersectAll(df_store_duplicate)
        )

        dfs_employee_duplicate = (
            self.df_sqlglot_employee
            .select(SF.col("employee_id"), SF.col("store_id"))
            .union(
                self.df_sqlglot_employee
                .select(SF.col("employee_id"), SF.col("store_id"))
            )
        )

        dfs_store_duplicate = (
            self.df_sqlglot_store
            .select(SF.col("store_id"), SF.col("district_id"))
            .union(
                self.df_sqlglot_store
                .select(SF.col("store_id"), SF.col("district_id"))
            )
        )

        dfs = (
            dfs_employee_duplicate
            .intersectAll(dfs_store_duplicate)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_except_all(self):
        df_employee_duplicate = (
            self.df_spark_employee
            .select(F.col("employee_id"), F.col("store_id"))
            .union(
                self.df_spark_employee
                .select(F.col("employee_id"), F.col("store_id"))
            )
        )

        df_store_duplicate = (
            self.df_spark_store
            .select(F.col("store_id"), F.col("district_id"))
            .union(
                self.df_spark_store
                .select(F.col("store_id"), F.col("district_id"))
            )
        )

        df = (
            df_employee_duplicate
            .exceptAll(df_store_duplicate)
        )

        dfs_employee_duplicate = (
            self.df_sqlglot_employee
            .select(SF.col("employee_id"), SF.col("store_id"))
            .union(
                self.df_sqlglot_employee
                .select(SF.col("employee_id"), SF.col("store_id"))
            )
        )

        dfs_store_duplicate = (
            self.df_sqlglot_store
            .select(SF.col("store_id"), SF.col("district_id"))
            .union(
                self.df_sqlglot_store
                .select(SF.col("store_id"), SF.col("district_id"))
            )
        )

        dfs = (
            dfs_employee_duplicate
            .exceptAll(dfs_store_duplicate)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_distinct(self):
        df = (
            self.df_spark_employee
            .select(F.col("age")).distinct()
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age")).distinct()
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_na_default(self):
        df = (
            self.df_spark_employee
            .select(F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .dropna()
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .dropna()
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_dropna_how(self):
        df = (
            self.df_spark_employee
            .select(F.lit(None), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .dropna(how="all")
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.lit(None), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .dropna(how="all")
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_dropna_thresh(self):
        df = (
            self.df_spark_employee
            .select(F.lit(None), F.lit(1), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .dropna(how="any", thresh=2)
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.lit(None), SF.lit(1), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .dropna(how="any", thresh=2)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_dropna_subset(self):
        df = (
            self.df_spark_employee
            .select(F.lit(None), F.lit(1), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .dropna(thresh=1, subset="the_age")
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.lit(None), SF.lit(1), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .dropna(thresh=1, subset="the_age")
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_dropna_na_function(self):
        df = (
            self.df_spark_employee
            .select(F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .na
            .drop()
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .na
            .drop()
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_fillna_default(self):
        df = (
            self.df_spark_employee
            .select(F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .fillna(100)
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .fillna(100)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_fillna_dict_replacement(self):
        df = (
            self.df_spark_employee
            .select(F.col("fname"), F.when(F.col("lname").startswith("L"), F.col("lname")).alias("l_lname"), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("fname"), SF.when(SF.col("lname").startswith("L"), SF.col("lname")).alias("l_lname"), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})
        )

        # For some reason the sqlglot results sets a column as nullable when it doesn't need to
        # This seems to be a nuance in how spark dataframe from sql works so we can ignore
        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_fillna_na_func(self):
        df = (
            self.df_spark_employee
            .select(F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"))
            .na
            .fill(100)
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"))
            .na
            .fill(100)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_replace_basic(self):
        df = (
            self.df_spark_employee
            .select(F.col("age"), F.lit(37).alias("test_col"))
            .replace(to_replace=37, value=100)
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age"), SF.lit(37).alias("test_col"))
            .replace(to_replace=37, value=100)
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_basic_subset(self):
        df = (
            self.df_spark_employee
            .select(F.col("age"), F.lit(37).alias("test_col"))
            .replace(to_replace=37, value=100, subset="age")
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age"), SF.lit(37).alias("test_col"))
            .replace(to_replace=37, value=100, subset="age")
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_mapping(self):
        df = (
            self.df_spark_employee
            .select(F.col("age"), F.lit(37).alias("test_col"))
            .replace({37: 100})
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age"), SF.lit(37).alias("test_col"))
            .replace({37: 100})
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_mapping_subset(self):
        df = (
            self.df_spark_employee
            .select(F.col("age"), F.lit(37).alias("test_col"), F.lit(50).alias("test_col_2"))
            .replace({37: 100, 50: 1}, subset=["age", "test_col_2"])
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age"), SF.lit(37).alias("test_col"), SF.lit(50).alias("test_col_2"))
            .replace({37: 100, 50: 1}, subset=["age", "test_col_2"])
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_na_func_basic(self):
        df = (
            self.df_spark_employee
            .select(F.col("age"), F.lit(37).alias("test_col"))
            .na
            .replace(to_replace=37, value=100)
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("age"), SF.lit(37).alias("test_col"))
            .na
            .replace(to_replace=37, value=100)
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_with_column(self):
        df = (
            self.df_spark_employee
            .withColumn("test", F.col("age"))
        )

        dfs = (
            self.df_sqlglot_employee
            .withColumn("test", SF.col("age"))
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_column_single(self):
        df = (
            self.df_spark_employee
            .select(F.col("fname"), F.col("lname"), F.col("age"))
            .drop("age")
        )

        dfs = (
            self.df_sqlglot_employee
            .select(SF.col("fname"), SF.col("lname"), SF.col("age"))
            .drop("age")
        )

        self.compare_spark_with_sqlglot(df, dfs)
