from sqlglot import expressions as exp
from sqlglot.dataframe.sql.dataframe import DataFrame
from tests.dataframe.unit.dataframe_sql_validator import DataFrameSQLValidator


class TestDataframe(DataFrameSQLValidator):
    def test_hash_select_expression(self):
        expression = exp.select("cola").from_("table")
        self.assertEqual("t17051", DataFrame._create_hash_from_expression(expression))

    def test_columns(self):
        self.assertEqual(
            ["employee_id", "fname", "lname", "age", "store_id"], self.df_employee.columns
        )

    def test_cache(self):
        df = self.df_employee.select("fname").cache()
        expected_statements = [
            "DROP VIEW IF EXISTS t11623",
            "CACHE LAZY TABLE t11623 OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT CAST(`a1`.`fname` AS string) AS `fname` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
            "SELECT `t11623`.`fname` AS `fname` FROM `t11623` AS `t11623`",
        ]
        self.compare_sql(df, expected_statements)

    def test_persist_default(self):
        df = self.df_employee.select("fname").persist()
        expected_statements = [
            "DROP VIEW IF EXISTS t11623",
            "CACHE LAZY TABLE t11623 OPTIONS('storageLevel' = 'MEMORY_AND_DISK_SER') AS SELECT CAST(`a1`.`fname` AS string) AS `fname` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
            "SELECT `t11623`.`fname` AS `fname` FROM `t11623` AS `t11623`",
        ]
        self.compare_sql(df, expected_statements)

    def test_persist_storagelevel(self):
        df = self.df_employee.select("fname").persist("DISK_ONLY_2")
        expected_statements = [
            "DROP VIEW IF EXISTS t11623",
            "CACHE LAZY TABLE t11623 OPTIONS('storageLevel' = 'DISK_ONLY_2') AS SELECT CAST(`a1`.`fname` AS string) AS `fname` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
            "SELECT `t11623`.`fname` AS `fname` FROM `t11623` AS `t11623`",
        ]
        self.compare_sql(df, expected_statements)
