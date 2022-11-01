from pyspark.sql import functions as F

from sqlglot.dataframe.sql import functions as SF
from tests.dataframe.integration.dataframe_validator import DataFrameValidator


class TestSessionFunc(DataFrameValidator):
    def test_sql_simple_select(self):
        query = "SELECT fname, lname FROM employee"
        df = self.spark.sql(query)
        dfs = self.sqlglot.sql(query)
        self.compare_spark_with_sqlglot(df, dfs)

    def test_sql_with_join(self):
        query = """
        SELECT
            e.employee_id
            , s.store_id    
        FROM
            employee e
            INNER JOIN
            store s
            ON
                e.store_id = s.store_id
        """
        df = (
            self.spark.sql(query)
            .groupBy(F.col("store_id"))
            .agg(F.countDistinct(F.col("employee_id")))
        )
        dfs = (
            self.sqlglot.sql(query)
            .groupBy(SF.col("store_id"))
            .agg(SF.countDistinct(SF.col("employee_id")))
        )
        self.compare_spark_with_sqlglot(df, dfs, skip_schema_compare=True)
