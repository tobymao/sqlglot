from pyspark.sql import functions as F

from sqlglot.dataframe.sql import functions as SF
from tests.dataframe.integration.dataframe_validator import DataFrameValidator


class TestDataframeFunc(DataFrameValidator):
    def test_simple_select(self):
        df_employee = self.df_spark_employee.select(F.col("employee_id"))
        dfs_employee = self.df_sqlglot_employee.select(SF.col("employee_id"))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_simple_select_from_table(self):
        df = self.df_spark_employee
        dfs = self.sqlglot.read.table("employee")
        self.compare_spark_with_sqlglot(df, dfs)

    def test_simple_select_df_attribute(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee.employee_id)
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee.employee_id)
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_simple_select_df_dict(self):
        df_employee = self.df_spark_employee.select(self.df_spark_employee["employee_id"])
        dfs_employee = self.df_sqlglot_employee.select(self.df_sqlglot_employee["employee_id"])
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_multiple_selects(self):
        df_employee = self.df_spark_employee.select(
            self.df_spark_employee["employee_id"], F.col("fname"), self.df_spark_employee.lname
        )
        dfs_employee = self.df_sqlglot_employee.select(
            self.df_sqlglot_employee["employee_id"], SF.col("fname"), self.df_sqlglot_employee.lname
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_alias_no_op(self):
        df_employee = self.df_spark_employee.alias("df_employee")
        dfs_employee = self.df_sqlglot_employee.alias("dfs_employee")
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_alias_with_select(self):
        df_employee = self.df_spark_employee.alias("df_employee").select(
            self.df_spark_employee["employee_id"],
            F.col("df_employee.fname"),
            self.df_spark_employee.lname,
        )
        dfs_employee = self.df_sqlglot_employee.alias("dfs_employee").select(
            self.df_sqlglot_employee["employee_id"],
            SF.col("dfs_employee.fname"),
            self.df_sqlglot_employee.lname,
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_case_when_otherwise(self):
        df = self.df_spark_employee.select(
            F.when(
                (F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)),
                F.lit("between 40 and 60"),
            )
            .when(F.col("age") < F.lit(40), "less than 40")
            .otherwise("greater than 60")
        )

        dfs = self.df_sqlglot_employee.select(
            SF.when(
                (SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)),
                SF.lit("between 40 and 60"),
            )
            .when(SF.col("age") < SF.lit(40), "less than 40")
            .otherwise("greater than 60")
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_case_when_no_otherwise(self):
        df = self.df_spark_employee.select(
            F.when(
                (F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)),
                F.lit("between 40 and 60"),
            ).when(F.col("age") < F.lit(40), "less than 40")
        )

        dfs = self.df_sqlglot_employee.select(
            SF.when(
                (SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)),
                SF.lit("between 40 and 60"),
            ).when(SF.col("age") < SF.lit(40), "less than 40")
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_where_clause_single(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_clause_multiple_and(self):
        df_employee = self.df_spark_employee.where(
            (F.col("age") == F.lit(37)) & (F.col("fname") == F.lit("Jack"))
        )
        dfs_employee = self.df_sqlglot_employee.where(
            (SF.col("age") == SF.lit(37)) & (SF.col("fname") == SF.lit("Jack"))
        )
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
        df_employee = self.df_spark_employee.where(
            (F.col("age") == F.lit(37)) | (F.col("fname") == F.lit("Kate"))
        )
        dfs_employee = self.df_sqlglot_employee.where(
            (SF.col("age") == SF.lit(37)) | (SF.col("fname") == SF.lit("Kate"))
        )
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
            ((F.col("age") == F.lit(65)) & (F.col("fname") == F.lit("John")))
            | ((F.col("lname") == F.lit("Shephard")) & (F.col("age") == F.lit(37)))
        )
        dfs_employee = self.df_sqlglot_employee.where(
            ((SF.col("age") == SF.lit(65)) & (SF.col("fname") == SF.lit("John")))
            | ((SF.col("lname") == SF.lit("Shephard")) & (SF.col("age") == SF.lit(37)))
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_where_multiple_chained(self):
        df_employee = self.df_spark_employee.where(F.col("age") == F.lit(37)).where(
            self.df_spark_employee.fname == F.lit("Jack")
        )
        dfs_employee = self.df_sqlglot_employee.where(SF.col("age") == SF.lit(37)).where(
            self.df_sqlglot_employee.fname == SF.lit("Jack")
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_operators(self):
        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] < F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] < SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] <= F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] <= SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] > F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] > SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] >= F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] >= SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] != F.lit(50))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] != SF.lit(50))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(self.df_spark_employee["age"] == F.lit(37))
        dfs_employee = self.df_sqlglot_employee.where(self.df_sqlglot_employee["age"] == SF.lit(37))
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(
            self.df_spark_employee["age"] % F.lit(5) == F.lit(0)
        )
        dfs_employee = self.df_sqlglot_employee.where(
            self.df_sqlglot_employee["age"] % SF.lit(5) == SF.lit(0)
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(
            self.df_spark_employee["age"] + F.lit(5) > F.lit(28)
        )
        dfs_employee = self.df_sqlglot_employee.where(
            self.df_sqlglot_employee["age"] + SF.lit(5) > SF.lit(28)
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(
            self.df_spark_employee["age"] - F.lit(5) > F.lit(28)
        )
        dfs_employee = self.df_sqlglot_employee.where(
            self.df_sqlglot_employee["age"] - SF.lit(5) > SF.lit(28)
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

        df_employee = self.df_spark_employee.where(
            self.df_spark_employee["age"] * F.lit(0.5) == self.df_spark_employee["age"] / F.lit(2)
        )
        dfs_employee = self.df_sqlglot_employee.where(
            self.df_sqlglot_employee["age"] * SF.lit(0.5)
            == self.df_sqlglot_employee["age"] / SF.lit(2)
        )
        self.compare_spark_with_sqlglot(df_employee, dfs_employee)

    def test_join_inner(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store, on=["store_id"], how="inner"
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            F.col("store_id"),
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store, on=["store_id"], how="inner"
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            SF.col("store_id"),
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_no_select(self):
        df_joined = self.df_spark_employee.select(
            F.col("store_id"), F.col("fname"), F.col("lname")
        ).join(
            self.df_spark_store.select(F.col("store_id"), F.col("store_name")),
            on=["store_id"],
            how="inner",
        )
        dfs_joined = self.df_sqlglot_employee.select(
            SF.col("store_id"), SF.col("fname"), SF.col("lname")
        ).join(
            self.df_sqlglot_store.select(SF.col("store_id"), SF.col("store_name")),
            on=["store_id"],
            how="inner",
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_single(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store,
            on=self.df_spark_employee.store_id == self.df_spark_store.store_id,
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
            F.lit("literal_value"),
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store,
            on=self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
            SF.lit("literal_value"),
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_multiple(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store,
            on=[
                self.df_spark_employee.store_id == self.df_spark_store.store_id,
                self.df_spark_employee.age == self.df_spark_store.num_sales,
            ],
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store,
            on=[
                self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
                self.df_sqlglot_employee.age == self.df_sqlglot_store.num_sales,
            ],
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_inner_equality_multiple_bitwise_and(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store,
            on=(self.df_spark_store.store_id == self.df_spark_employee.store_id)
            & (self.df_spark_store.num_sales == self.df_spark_employee.age),
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store,
            on=(self.df_sqlglot_store.store_id == self.df_sqlglot_employee.store_id)
            & (self.df_sqlglot_store.num_sales == self.df_sqlglot_employee.age),
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_left_outer(self):
        df_joined = (
            self.df_spark_employee.join(self.df_spark_store, on=["store_id"], how="left_outer")
            .select(
                self.df_spark_employee.employee_id,
                self.df_spark_employee["fname"],
                F.col("lname"),
                F.col("age"),
                F.col("store_id"),
                self.df_spark_store.store_name,
                self.df_spark_store["num_sales"],
            )
            .orderBy(F.col("employee_id"))
        )
        dfs_joined = (
            self.df_sqlglot_employee.join(self.df_sqlglot_store, on=["store_id"], how="left_outer")
            .select(
                self.df_sqlglot_employee.employee_id,
                self.df_sqlglot_employee["fname"],
                SF.col("lname"),
                SF.col("age"),
                SF.col("store_id"),
                self.df_sqlglot_store.store_name,
                self.df_sqlglot_store["num_sales"],
            )
            .orderBy(SF.col("employee_id"))
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_join_full_outer(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store, on=["store_id"], how="full_outer"
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            F.col("store_id"),
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store, on=["store_id"], how="full_outer"
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            SF.col("store_id"),
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_triple_join(self):
        df = (
            self.df_employee.join(
                self.df_store, on=self.df_employee.employee_id == self.df_store.store_id
            )
            .join(self.df_district, on=self.df_store.store_id == self.df_district.district_id)
            .select(
                self.df_employee.employee_id,
                self.df_store.store_id,
                self.df_district.district_id,
                self.df_employee.fname,
                self.df_store.store_name,
                self.df_district.district_name,
            )
        )
        dfs = (
            self.dfs_employee.join(
                self.dfs_store, on=self.dfs_employee.employee_id == self.dfs_store.store_id
            )
            .join(self.dfs_district, on=self.dfs_store.store_id == self.dfs_district.district_id)
            .select(
                self.dfs_employee.employee_id,
                self.dfs_store.store_id,
                self.dfs_district.district_id,
                self.dfs_employee.fname,
                self.dfs_store.store_name,
                self.dfs_district.district_name,
            )
        )
        self.compare_spark_with_sqlglot(df, dfs)

    def test_triple_join_no_select(self):
        df = (
            self.df_employee.join(
                self.df_store,
                on=self.df_employee["employee_id"] == self.df_store["store_id"],
                how="left",
            )
            .join(
                self.df_district,
                on=self.df_store["store_id"] == self.df_district["district_id"],
                how="left",
            )
            .orderBy(F.col("employee_id"))
        )
        dfs = (
            self.dfs_employee.join(
                self.dfs_store,
                on=self.dfs_employee["employee_id"] == self.dfs_store["store_id"],
                how="left",
            )
            .join(
                self.dfs_district,
                on=self.dfs_store["store_id"] == self.dfs_district["district_id"],
                how="left",
            )
            .orderBy(SF.col("employee_id"))
        )
        self.compare_spark_with_sqlglot(df, dfs)

    def test_triple_joins_filter(self):
        df = (
            self.df_employee.join(
                self.df_store,
                on=self.df_employee["employee_id"] == self.df_store["store_id"],
                how="left",
            ).join(
                self.df_district,
                on=self.df_store["store_id"] == self.df_district["district_id"],
                how="left",
            )
        ).filter(F.coalesce(self.df_store["num_sales"], F.lit(0)) > 100)
        dfs = (
            self.dfs_employee.join(
                self.dfs_store,
                on=self.dfs_employee["employee_id"] == self.dfs_store["store_id"],
                how="left",
            ).join(
                self.dfs_district,
                on=self.dfs_store["store_id"] == self.dfs_district["district_id"],
                how="left",
            )
        ).filter(SF.coalesce(self.dfs_store["num_sales"], SF.lit(0)) > 100)
        self.compare_spark_with_sqlglot(df, dfs)

    def test_triple_join_column_name_only(self):
        df = (
            self.df_employee.join(
                self.df_store,
                on=self.df_employee["employee_id"] == self.df_store["store_id"],
                how="left",
            )
            .join(self.df_district, on="district_id", how="left")
            .orderBy(F.col("employee_id"))
        )
        dfs = (
            self.dfs_employee.join(
                self.dfs_store,
                on=self.dfs_employee["employee_id"] == self.dfs_store["store_id"],
                how="left",
            )
            .join(self.dfs_district, on="district_id", how="left")
            .orderBy(SF.col("employee_id"))
        )
        self.compare_spark_with_sqlglot(df, dfs)

    def test_join_select_and_select_start(self):
        df = self.df_spark_employee.select(
            F.col("fname"), F.col("lname"), F.col("age"), F.col("store_id")
        ).join(self.df_spark_store, "store_id", "inner")

        dfs = self.df_sqlglot_employee.select(
            SF.col("fname"), SF.col("lname"), SF.col("age"), SF.col("store_id")
        ).join(self.df_sqlglot_store, "store_id", "inner")

        self.compare_spark_with_sqlglot(df, dfs)

    def test_branching_root_dataframes(self):
        """
        Test a pattern that has non-intuitive behavior in spark

        Scenario: You do a self-join in a dataframe using an original dataframe and then a modified version
        of it. You then reference the columns by the dataframe name instead of the column function.
        Spark will use the root dataframe's column in the result.
        """
        df_hydra_employees_only = self.df_spark_employee.where(F.col("store_id") == F.lit(1))
        df_joined = (
            self.df_spark_employee.where(F.col("store_id") == F.lit(2))
            .alias("df_arrow_employees_only")
            .join(
                df_hydra_employees_only.alias("df_hydra_employees_only"),
                on=["store_id"],
                how="full_outer",
            )
            .select(
                self.df_spark_employee.fname,
                F.col("df_arrow_employees_only.fname"),
                df_hydra_employees_only.fname,
                F.col("df_hydra_employees_only.fname"),
            )
        )

        dfs_hydra_employees_only = self.df_sqlglot_employee.where(SF.col("store_id") == SF.lit(1))
        dfs_joined = (
            self.df_sqlglot_employee.where(SF.col("store_id") == SF.lit(2))
            .alias("dfs_arrow_employees_only")
            .join(
                dfs_hydra_employees_only.alias("dfs_hydra_employees_only"),
                on=["store_id"],
                how="full_outer",
            )
            .select(
                self.df_sqlglot_employee.fname,
                SF.col("dfs_arrow_employees_only.fname"),
                dfs_hydra_employees_only.fname,
                SF.col("dfs_hydra_employees_only.fname"),
            )
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_basic_union(self):
        df_unioned = self.df_spark_employee.select(F.col("employee_id"), F.col("age")).union(
            self.df_spark_store.select(F.col("store_id"), F.col("num_sales"))
        )

        dfs_unioned = self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("age")).union(
            self.df_sqlglot_store.select(SF.col("store_id"), SF.col("num_sales"))
        )
        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_union_with_join(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store,
            on="store_id",
            how="inner",
        )
        df_unioned = df_joined.select(F.col("store_id"), F.col("store_name")).union(
            self.df_spark_district.select(F.col("district_id"), F.col("district_name"))
        )

        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store,
            on="store_id",
            how="inner",
        )
        dfs_unioned = dfs_joined.select(SF.col("store_id"), SF.col("store_name")).union(
            self.df_sqlglot_district.select(SF.col("district_id"), SF.col("district_name"))
        )

        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_double_union_all(self):
        df_unioned = (
            self.df_spark_employee.select(F.col("employee_id"), F.col("fname"))
            .unionAll(self.df_spark_store.select(F.col("store_id"), F.col("store_name")))
            .unionAll(self.df_spark_district.select(F.col("district_id"), F.col("district_name")))
        )

        dfs_unioned = (
            self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("fname"))
            .unionAll(self.df_sqlglot_store.select(SF.col("store_id"), SF.col("store_name")))
            .unionAll(
                self.df_sqlglot_district.select(SF.col("district_id"), SF.col("district_name"))
            )
        )

        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_union_by_name(self):
        df = self.df_spark_employee.select(
            F.col("employee_id"), F.col("fname"), F.col("lname")
        ).unionByName(
            self.df_spark_store.select(
                F.col("store_name").alias("lname"),
                F.col("store_id").alias("employee_id"),
                F.col("store_name").alias("fname"),
            )
        )

        dfs = self.df_sqlglot_employee.select(
            SF.col("employee_id"), SF.col("fname"), SF.col("lname")
        ).unionByName(
            self.df_sqlglot_store.select(
                SF.col("store_name").alias("lname"),
                SF.col("store_id").alias("employee_id"),
                SF.col("store_name").alias("fname"),
            )
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_union_by_name_allow_missing(self):
        df = self.df_spark_employee.select(
            F.col("age"), F.col("employee_id"), F.col("fname"), F.col("lname")
        ).unionByName(
            self.df_spark_store.select(
                F.col("store_name").alias("lname"),
                F.col("store_id").alias("employee_id"),
                F.col("store_name").alias("fname"),
                F.col("num_sales"),
            ),
            allowMissingColumns=True,
        )

        dfs = self.df_sqlglot_employee.select(
            SF.col("age"), SF.col("employee_id"), SF.col("fname"), SF.col("lname")
        ).unionByName(
            self.df_sqlglot_store.select(
                SF.col("store_name").alias("lname"),
                SF.col("store_id").alias("employee_id"),
                SF.col("store_name").alias("fname"),
                SF.col("num_sales"),
            ),
            allowMissingColumns=True,
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_default(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales"))
            .orderBy(F.col("district_id"))
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales"))
            .orderBy(SF.col("district_id"))
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_order_by_array_bool(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.col("total_sales"), F.col("district_id"), ascending=[1, 0])
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=[1, 0])
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_single_bool(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.col("total_sales"), F.col("district_id"), ascending=False)
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=False)
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(F.col("total_sales").asc(), F.col("district_id").desc())
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(SF.col("total_sales").asc(), SF.col("district_id").desc())
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method_nulls_last(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(
                F.when(F.col("district_id") == F.lit(2), F.col("district_id")).asc_nulls_last()
            )
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(
                SF.when(SF.col("district_id") == SF.lit(2), SF.col("district_id")).asc_nulls_last()
            )
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_order_by_column_sort_method_nulls_first(self):
        df = (
            self.df_spark_store.groupBy(F.col("district_id"))
            .agg(F.min("num_sales").alias("total_sales"))
            .orderBy(
                F.when(F.col("district_id") == F.lit(1), F.col("district_id")).desc_nulls_first()
            )
        )

        dfs = (
            self.df_sqlglot_store.groupBy(SF.col("district_id"))
            .agg(SF.min("num_sales").alias("total_sales"))
            .orderBy(
                SF.when(
                    SF.col("district_id") == SF.lit(1), SF.col("district_id")
                ).desc_nulls_first()
            )
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_intersect(self):
        df_employee_duplicate = self.df_spark_employee.select(
            F.col("employee_id"), F.col("store_id")
        ).union(self.df_spark_employee.select(F.col("employee_id"), F.col("store_id")))

        df_store_duplicate = self.df_spark_store.select(
            F.col("store_id"), F.col("district_id")
        ).union(self.df_spark_store.select(F.col("store_id"), F.col("district_id")))

        df = df_employee_duplicate.intersect(df_store_duplicate)

        dfs_employee_duplicate = self.df_sqlglot_employee.select(
            SF.col("employee_id"), SF.col("store_id")
        ).union(self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("store_id")))

        dfs_store_duplicate = self.df_sqlglot_store.select(
            SF.col("store_id"), SF.col("district_id")
        ).union(self.df_sqlglot_store.select(SF.col("store_id"), SF.col("district_id")))

        dfs = dfs_employee_duplicate.intersect(dfs_store_duplicate)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_intersect_all(self):
        df_employee_duplicate = self.df_spark_employee.select(
            F.col("employee_id"), F.col("store_id")
        ).union(self.df_spark_employee.select(F.col("employee_id"), F.col("store_id")))

        df_store_duplicate = self.df_spark_store.select(
            F.col("store_id"), F.col("district_id")
        ).union(self.df_spark_store.select(F.col("store_id"), F.col("district_id")))

        df = df_employee_duplicate.intersectAll(df_store_duplicate)

        dfs_employee_duplicate = self.df_sqlglot_employee.select(
            SF.col("employee_id"), SF.col("store_id")
        ).union(self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("store_id")))

        dfs_store_duplicate = self.df_sqlglot_store.select(
            SF.col("store_id"), SF.col("district_id")
        ).union(self.df_sqlglot_store.select(SF.col("store_id"), SF.col("district_id")))

        dfs = dfs_employee_duplicate.intersectAll(dfs_store_duplicate)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_except_all(self):
        df_employee_duplicate = self.df_spark_employee.select(
            F.col("employee_id"), F.col("store_id")
        ).union(self.df_spark_employee.select(F.col("employee_id"), F.col("store_id")))

        df_store_duplicate = self.df_spark_store.select(
            F.col("store_id"), F.col("district_id")
        ).union(self.df_spark_store.select(F.col("store_id"), F.col("district_id")))

        df = df_employee_duplicate.exceptAll(df_store_duplicate)

        dfs_employee_duplicate = self.df_sqlglot_employee.select(
            SF.col("employee_id"), SF.col("store_id")
        ).union(self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("store_id")))

        dfs_store_duplicate = self.df_sqlglot_store.select(
            SF.col("store_id"), SF.col("district_id")
        ).union(self.df_sqlglot_store.select(SF.col("store_id"), SF.col("district_id")))

        dfs = dfs_employee_duplicate.exceptAll(dfs_store_duplicate)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_distinct(self):
        df = self.df_spark_employee.select(F.col("age")).distinct()

        dfs = self.df_sqlglot_employee.select(SF.col("age")).distinct()

        self.compare_spark_with_sqlglot(df, dfs)

    def test_union_distinct(self):
        df_unioned = (
            self.df_spark_employee.select(F.col("employee_id"), F.col("age"))
            .union(self.df_spark_employee.select(F.col("employee_id"), F.col("age")))
            .distinct()
        )

        dfs_unioned = (
            self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("age"))
            .union(self.df_sqlglot_employee.select(SF.col("employee_id"), SF.col("age")))
            .distinct()
        )
        self.compare_spark_with_sqlglot(df_unioned, dfs_unioned)

    def test_drop_duplicates_no_subset(self):
        df = self.df_spark_employee.select("age").dropDuplicates()
        dfs = self.df_sqlglot_employee.select("age").dropDuplicates()
        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_duplicates_subset(self):
        df = self.df_spark_employee.dropDuplicates(["age"])
        dfs = self.df_sqlglot_employee.dropDuplicates(["age"])
        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_na_default(self):
        df = self.df_spark_employee.select(
            F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).dropna()

        dfs = self.df_sqlglot_employee.select(
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
        ).dropna()

        self.compare_spark_with_sqlglot(df, dfs)

    def test_dropna_how(self):
        df = self.df_spark_employee.select(
            F.lit(None), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).dropna(how="all")

        dfs = self.df_sqlglot_employee.select(
            SF.lit(None), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
        ).dropna(how="all")

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_dropna_thresh(self):
        df = self.df_spark_employee.select(
            F.lit(None), F.lit(1), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).dropna(how="any", thresh=2)

        dfs = self.df_sqlglot_employee.select(
            SF.lit(None),
            SF.lit(1),
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
        ).dropna(how="any", thresh=2)

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_dropna_subset(self):
        df = self.df_spark_employee.select(
            F.lit(None), F.lit(1), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).dropna(thresh=1, subset="the_age")

        dfs = self.df_sqlglot_employee.select(
            SF.lit(None),
            SF.lit(1),
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
        ).dropna(thresh=1, subset="the_age")

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_dropna_na_function(self):
        df = self.df_spark_employee.select(
            F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).na.drop()

        dfs = self.df_sqlglot_employee.select(
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
        ).na.drop()

        self.compare_spark_with_sqlglot(df, dfs)

    def test_fillna_default(self):
        df = self.df_spark_employee.select(
            F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).fillna(100)

        dfs = self.df_sqlglot_employee.select(
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
        ).fillna(100)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_fillna_dict_replacement(self):
        df = self.df_spark_employee.select(
            F.col("fname"),
            F.when(F.col("lname").startswith("L"), F.col("lname")).alias("l_lname"),
            F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"),
        ).fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})

        dfs = self.df_sqlglot_employee.select(
            SF.col("fname"),
            SF.when(SF.col("lname").startswith("L"), SF.col("lname")).alias("l_lname"),
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
        ).fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})

        # For some reason the sqlglot results sets a column as nullable when it doesn't need to
        # This seems to be a nuance in how spark dataframe from sql works so we can ignore
        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_fillna_na_func(self):
        df = self.df_spark_employee.select(
            F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
        ).na.fill(100)

        dfs = self.df_sqlglot_employee.select(
            SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
        ).na.fill(100)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_replace_basic(self):
        df = self.df_spark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace(
            to_replace=37, value=100
        )

        dfs = self.df_sqlglot_employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace(
            to_replace=37, value=100
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_basic_subset(self):
        df = self.df_spark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace(
            to_replace=37, value=100, subset="age"
        )

        dfs = self.df_sqlglot_employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace(
            to_replace=37, value=100, subset="age"
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_mapping(self):
        df = self.df_spark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace(
            {37: 100}
        )

        dfs = self.df_sqlglot_employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace(
            {37: 100}
        )

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_mapping_subset(self):
        df = self.df_spark_employee.select(
            F.col("age"), F.lit(37).alias("test_col"), F.lit(50).alias("test_col_2")
        ).replace({37: 100, 50: 1}, subset=["age", "test_col_2"])

        dfs = self.df_sqlglot_employee.select(
            SF.col("age"), SF.lit(37).alias("test_col"), SF.lit(50).alias("test_col_2")
        ).replace({37: 100, 50: 1}, subset=["age", "test_col_2"])

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_replace_na_func_basic(self):
        df = self.df_spark_employee.select(F.col("age"), F.lit(37).alias("test_col")).na.replace(
            to_replace=37, value=100
        )

        dfs = self.df_sqlglot_employee.select(
            SF.col("age"), SF.lit(37).alias("test_col")
        ).na.replace(to_replace=37, value=100)

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_with_column(self):
        df = self.df_spark_employee.withColumn("test", F.col("age"))

        dfs = self.df_sqlglot_employee.withColumn("test", SF.col("age"))

        self.compare_spark_with_sqlglot(df, dfs)

    def test_with_column_existing_name(self):
        df = self.df_spark_employee.withColumn("fname", F.lit("blah"))

        dfs = self.df_sqlglot_employee.withColumn("fname", SF.lit("blah"))

        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)

    def test_with_column_renamed(self):
        df = self.df_spark_employee.withColumnRenamed("fname", "first_name")

        dfs = self.df_sqlglot_employee.withColumnRenamed("fname", "first_name")

        self.compare_spark_with_sqlglot(df, dfs)

    def test_with_column_renamed_double(self):
        df = self.df_spark_employee.select(F.col("fname").alias("first_name")).withColumnRenamed(
            "first_name", "first_name_again"
        )

        dfs = self.df_sqlglot_employee.select(
            SF.col("fname").alias("first_name")
        ).withColumnRenamed("first_name", "first_name_again")

        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_column_single(self):
        df = self.df_spark_employee.select(F.col("fname"), F.col("lname"), F.col("age")).drop("age")

        dfs = self.df_sqlglot_employee.select(SF.col("fname"), SF.col("lname"), SF.col("age")).drop(
            "age"
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_drop_column_reference_join(self):
        df_spark_employee_cols = self.df_spark_employee.select(
            F.col("fname"), F.col("lname"), F.col("age"), F.col("store_id")
        )
        df_spark_store_cols = self.df_spark_store.select(F.col("store_id"), F.col("store_name"))
        df = df_spark_employee_cols.join(df_spark_store_cols, on="store_id", how="inner").drop(
            df_spark_employee_cols.age,
        )

        df_sqlglot_employee_cols = self.df_sqlglot_employee.select(
            SF.col("fname"), SF.col("lname"), SF.col("age"), SF.col("store_id")
        )
        df_sqlglot_store_cols = self.df_sqlglot_store.select(
            SF.col("store_id"), SF.col("store_name")
        )
        dfs = df_sqlglot_employee_cols.join(df_sqlglot_store_cols, on="store_id", how="inner").drop(
            df_sqlglot_employee_cols.age,
        )

        self.compare_spark_with_sqlglot(df, dfs)

    def test_limit(self):
        df = self.df_spark_employee.limit(1)

        dfs = self.df_sqlglot_employee.limit(1)

        self.compare_spark_with_sqlglot(df, dfs)

    def test_hint_broadcast_alias(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store.alias("store").hint("broadcast", "store"),
            on=self.df_spark_employee.store_id == self.df_spark_store.store_id,
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store.alias("store").hint("broadcast", "store"),
            on=self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        df, dfs = self.compare_spark_with_sqlglot(df_joined, dfs_joined)
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(df))
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(dfs))

    def test_hint_broadcast_no_alias(self):
        df_joined = self.df_spark_employee.join(
            self.df_spark_store.hint("broadcast"),
            on=self.df_spark_employee.store_id == self.df_spark_store.store_id,
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            self.df_sqlglot_store.hint("broadcast"),
            on=self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        df, dfs = self.compare_spark_with_sqlglot(df_joined, dfs_joined)
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(df))
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(dfs))
        self.assertEqual(
            "'UnresolvedHint BROADCAST, ['a2]", self.get_explain_plan(dfs).split("\n")[1]
        )

    def test_broadcast_func(self):
        df_joined = self.df_spark_employee.join(
            F.broadcast(self.df_spark_store),
            on=self.df_spark_employee.store_id == self.df_spark_store.store_id,
            how="inner",
        ).select(
            self.df_spark_employee.employee_id,
            self.df_spark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            self.df_spark_employee.store_id,
            self.df_spark_store.store_name,
            self.df_spark_store["num_sales"],
        )
        dfs_joined = self.df_sqlglot_employee.join(
            SF.broadcast(self.df_sqlglot_store),
            on=self.df_sqlglot_employee.store_id == self.df_sqlglot_store.store_id,
            how="inner",
        ).select(
            self.df_sqlglot_employee.employee_id,
            self.df_sqlglot_employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            self.df_sqlglot_employee.store_id,
            self.df_sqlglot_store.store_name,
            self.df_sqlglot_store["num_sales"],
        )
        df, dfs = self.compare_spark_with_sqlglot(df_joined, dfs_joined)
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(df))
        self.assertIn("ResolvedHint (strategy=broadcast)", self.get_explain_plan(dfs))
        self.assertEqual(
            "'UnresolvedHint BROADCAST, ['a2]", self.get_explain_plan(dfs).split("\n")[1]
        )

    def test_repartition_by_num(self):
        """
        The results are different when doing the repartition on a table created using VALUES in SQL.
        So I just use the views instead for these tests
        """
        df = self.df_spark_employee.repartition(63)

        dfs = self.sqlglot.read.table("employee").repartition(63)
        df, dfs = self.compare_spark_with_sqlglot(df, dfs)
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlglot_num_partitions = dfs.rdd.getNumPartitions()
        self.assertEqual(spark_num_partitions, 63)
        self.assertEqual(spark_num_partitions, sqlglot_num_partitions)

    def test_repartition_name_only(self):
        """
        We use the view here to help ensure the explain plans are similar enough to compare
        """
        df = self.df_spark_employee.repartition("age")

        dfs = self.sqlglot.read.table("employee").repartition("age")
        df, dfs = self.compare_spark_with_sqlglot(df, dfs)
        self.assertIn("RepartitionByExpression [age", self.get_explain_plan(df))
        self.assertIn("RepartitionByExpression [age", self.get_explain_plan(dfs))

    def test_repartition_num_and_multiple_names(self):
        """
        We use the view here to help ensure the explain plans are similar enough to compare
        """
        df = self.df_spark_employee.repartition(53, "age", "fname")

        dfs = self.sqlglot.read.table("employee").repartition(53, "age", "fname")
        df, dfs = self.compare_spark_with_sqlglot(df, dfs)
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlglot_num_partitions = dfs.rdd.getNumPartitions()
        self.assertEqual(spark_num_partitions, 53)
        self.assertEqual(spark_num_partitions, sqlglot_num_partitions)
        self.assertIn("RepartitionByExpression [age#3, fname#1], 53", self.get_explain_plan(df))
        self.assertIn("RepartitionByExpression [age#3, fname#1], 53", self.get_explain_plan(dfs))

    def test_coalesce(self):
        df = self.df_spark_employee.coalesce(1)
        dfs = self.df_sqlglot_employee.coalesce(1)
        df, dfs = self.compare_spark_with_sqlglot(df, dfs)
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlglot_num_partitions = dfs.rdd.getNumPartitions()
        self.assertEqual(spark_num_partitions, 1)
        self.assertEqual(spark_num_partitions, sqlglot_num_partitions)

    def test_cache_select(self):
        df_employee = (
            self.df_spark_employee.groupBy("store_id")
            .agg(F.countDistinct("employee_id").alias("num_employees"))
            .cache()
        )
        df_joined = df_employee.join(self.df_spark_store, on="store_id").select(
            self.df_spark_store.store_id, df_employee.num_employees
        )
        dfs_employee = (
            self.df_sqlglot_employee.groupBy("store_id")
            .agg(SF.countDistinct("employee_id").alias("num_employees"))
            .cache()
        )
        dfs_joined = dfs_employee.join(self.df_sqlglot_store, on="store_id").select(
            self.df_sqlglot_store.store_id, dfs_employee.num_employees
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)

    def test_persist_select(self):
        df_employee = (
            self.df_spark_employee.groupBy("store_id")
            .agg(F.countDistinct("employee_id").alias("num_employees"))
            .persist()
        )
        df_joined = df_employee.join(self.df_spark_store, on="store_id").select(
            self.df_spark_store.store_id, df_employee.num_employees
        )
        dfs_employee = (
            self.df_sqlglot_employee.groupBy("store_id")
            .agg(SF.countDistinct("employee_id").alias("num_employees"))
            .persist()
        )
        dfs_joined = dfs_employee.join(self.df_sqlglot_store, on="store_id").select(
            self.df_sqlglot_store.store_id, dfs_employee.num_employees
        )
        self.compare_spark_with_sqlglot(df_joined, dfs_joined)
