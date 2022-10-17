from unittest import mock

import sqlglot
from sqlglot.schema import MappingSchema
from tests.dataframe.unit.dataframe_sql_validator import DataFrameSQLValidator


class TestDataFrameWriter(DataFrameSQLValidator):
    def test_insertInto_full_path(self):
        df = self.df_employee.write.insertInto("catalog.db.table_name")
        expected = "INSERT INTO catalog.db.table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_insertInto_db_table(self):
        df = self.df_employee.write.insertInto("db.table_name")
        expected = "INSERT INTO db.table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_insertInto_table(self):
        df = self.df_employee.write.insertInto("table_name")
        expected = "INSERT INTO table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_insertInto_overwrite(self):
        df = self.df_employee.write.insertInto("table_name", overwrite=True)
        expected = "INSERT OVERWRITE TABLE table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    @mock.patch("sqlglot.schema", MappingSchema())
    def test_insertInto_byName(self):
        sqlglot.schema.add_table("table_name", {"employee_id": "INT"})
        df = self.df_employee.write.byName.insertInto("table_name")
        expected = "INSERT INTO table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_insertInto_cache(self):
        df = self.df_employee.cache().write.insertInto("table_name")
        expected_statements = [
            "DROP VIEW IF EXISTS t35612",
            "CACHE LAZY TABLE t35612 OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
            "INSERT INTO table_name SELECT `t35612`.`employee_id` AS `employee_id`, `t35612`.`fname` AS `fname`, `t35612`.`lname` AS `lname`, `t35612`.`age` AS `age`, `t35612`.`store_id` AS `store_id` FROM `t35612` AS `t35612`",
        ]
        self.compare_sql(df, expected_statements)

    def test_saveAsTable_format(self):
        with self.assertRaises(NotImplementedError):
            self.df_employee.write.saveAsTable("table_name", format="parquet").sql(pretty=False)[0]

    def test_saveAsTable_append(self):
        df = self.df_employee.write.saveAsTable("table_name", mode="append")
        expected = "INSERT INTO table_name SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_saveAsTable_overwrite(self):
        df = self.df_employee.write.saveAsTable("table_name", mode="overwrite")
        expected = "CREATE OR REPLACE TABLE table_name AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_saveAsTable_error(self):
        df = self.df_employee.write.saveAsTable("table_name", mode="error")
        expected = "CREATE TABLE table_name AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_saveAsTable_ignore(self):
        df = self.df_employee.write.saveAsTable("table_name", mode="ignore")
        expected = "CREATE TABLE IF NOT EXISTS table_name AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_mode_standalone(self):
        df = self.df_employee.write.mode("ignore").saveAsTable("table_name")
        expected = "CREATE TABLE IF NOT EXISTS table_name AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_mode_override(self):
        df = self.df_employee.write.mode("ignore").saveAsTable("table_name", mode="overwrite")
        expected = "CREATE OR REPLACE TABLE table_name AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
        self.compare_sql(df, expected)

    def test_saveAsTable_cache(self):
        df = self.df_employee.cache().write.saveAsTable("table_name")
        expected_statements = [
            "DROP VIEW IF EXISTS t35612",
            "CACHE LAZY TABLE t35612 OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT CAST(`a1`.`employee_id` AS int) AS `employee_id`, CAST(`a1`.`fname` AS string) AS `fname`, CAST(`a1`.`lname` AS string) AS `lname`, CAST(`a1`.`age` AS int) AS `age`, CAST(`a1`.`store_id` AS int) AS `store_id` FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
            "CREATE TABLE table_name AS SELECT `t35612`.`employee_id` AS `employee_id`, `t35612`.`fname` AS `fname`, `t35612`.`lname` AS `lname`, `t35612`.`age` AS `age`, `t35612`.`store_id` AS `store_id` FROM `t35612` AS `t35612`",
        ]
        self.compare_sql(df, expected_statements)
