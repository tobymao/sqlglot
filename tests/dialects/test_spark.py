from tests.dialects.test_dialect import Validator


class TestSpark(Validator):
    dialect = "spark"

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            write={
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )

    def test_spark(self):
        self.validate_all(
            "ARRAY_SORT(x, (left, right) -> -1)",
            write={
                "duckdb": "ARRAY_SORT(x)",
                "presto": "ARRAY_SORT(x, (left, right) -> -1)",
                "hive": "SORT_ARRAY(x)",
                "spark": "ARRAY_SORT(x, (left, right) -> -1)",
            },
        )

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            },
        )
