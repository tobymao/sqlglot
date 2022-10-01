from sqlglot import ErrorLevel, ParseError, UnsupportedError, transpile
from tests.dialects.test_dialect import Validator


class TestBigQuery(Validator):
    dialect = "bigquery"

    def test_bigquery(self):
        self.validate_all(
            '"""x"""',
            write={
                "bigquery": "'x'",
                "duckdb": "'x'",
                "presto": "'x'",
                "hive": "'x'",
                "spark": "'x'",
            },
        )
        self.validate_all(
            '"""x\'"""',
            write={
                "bigquery": "'x\\''",
                "duckdb": "'x'''",
                "presto": "'x'''",
                "hive": "'x\\''",
                "spark": "'x\\''",
            },
        )
        self.validate_all(
            r'r"""/\*.*\*/"""',
            write={
                "bigquery": r"'/\\*.*\\*/'",
                "duckdb": r"'/\*.*\*/'",
                "presto": r"'/\*.*\*/'",
                "hive": r"'/\\*.*\\*/'",
                "spark": r"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            R'R"""/\*.*\*/"""',
            write={
                "bigquery": R"'/\\*.*\\*/'",
                "duckdb": R"'/\*.*\*/'",
                "presto": R"'/\*.*\*/'",
                "hive": R"'/\\*.*\\*/'",
                "spark": R"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            "CAST(a AS INT64)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "duckdb": "CAST(a AS BIGINT)",
                "presto": "CAST(a AS BIGINT)",
                "hive": "CAST(a AS BIGINT)",
                "spark": "CAST(a AS LONG)",
            },
        )
        self.validate_all(
            "CAST(a AS NUMERIC)",
            write={
                "bigquery": "CAST(a AS NUMERIC)",
                "duckdb": "CAST(a AS DECIMAL)",
                "presto": "CAST(a AS DECIMAL)",
                "hive": "CAST(a AS DECIMAL)",
                "spark": "CAST(a AS DECIMAL)",
            },
        )
        self.validate_all(
            "[1, 2, 3]",
            read={
                "duckdb": "LIST_VALUE(1, 2, 3)",
                "presto": "ARRAY[1, 2, 3]",
                "hive": "ARRAY(1, 2, 3)",
                "spark": "ARRAY(1, 2, 3)",
            },
            write={
                "bigquery": "[1, 2, 3]",
                "duckdb": "LIST_VALUE(1, 2, 3)",
                "presto": "ARRAY[1, 2, 3]",
                "hive": "ARRAY(1, 2, 3)",
                "spark": "ARRAY(1, 2, 3)",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(['7', '14']) AS x",
            read={
                "spark": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS (x)",
            },
            write={
                "bigquery": "SELECT * FROM UNNEST(['7', '14']) AS x",
                "presto": "SELECT * FROM UNNEST(ARRAY['7', '14']) AS (x)",
                "hive": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS (x)",
                "spark": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS (x)",
            },
        )

        self.validate_all(
            "x IS unknown",
            write={
                "bigquery": "x IS NULL",
                "duckdb": "x IS NULL",
                "presto": "x IS NULL",
                "hive": "x IS NULL",
                "spark": "x IS NULL",
            },
        )
        self.validate_all(
            "current_datetime",
            write={
                "bigquery": "CURRENT_DATETIME()",
                "duckdb": "CURRENT_DATETIME()",
                "presto": "CURRENT_DATETIME()",
                "hive": "CURRENT_DATETIME()",
                "spark": "CURRENT_DATETIME()",
            },
        )
        self.validate_all(
            "current_time",
            write={
                "bigquery": "CURRENT_TIME()",
                "duckdb": "CURRENT_TIME()",
                "presto": "CURRENT_TIME()",
                "hive": "CURRENT_TIME()",
                "spark": "CURRENT_TIME()",
            },
        )
        self.validate_all(
            "current_timestamp",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP()",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP()",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "current_timestamp()",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP()",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP()",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )

        self.validate_identity(
            "SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)"
        )

        self.validate_identity(
            "SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)",
        )

        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b TEXT>)",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a BIGINT, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a BIGINT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: LONG, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )
        self.validate_all(
            "SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])",
            write={
                "bigquery": "SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])",
                "mysql": "SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY(1, 2, 3)))",
                "presto": "SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY[1, 2, 3]))",
                "hive": "SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY(1, 2, 3)))",
                "spark": "SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY(1, 2, 3)))",
            },
        )

        # Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        with self.assertRaises(ParseError):
            transpile("SELECT * FROM UNNEST(x) AS x(y)", read="bigquery")

        self.validate_all(
            "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            write={
                "postgres": "CURRENT_DATE - INTERVAL '1' DAY",
            },
        )
        self.validate_all(
            "DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)",
            write={
                "bigquery": "DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
                "duckdb": "CURRENT_DATE + INTERVAL 1 DAY",
                "mysql": "DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
                "postgres": "CURRENT_DATE + INTERVAL '1' DAY",
                "presto": "DATE_ADD(DAY, 1, CURRENT_DATE)",
                "hive": "DATE_ADD(CURRENT_DATE, 1)",
                "spark": "DATE_ADD(CURRENT_DATE, 1)",
            },
        )
        self.validate_all(
            "CURRENT_DATE('UTC')",
            write={
                "mysql": "CURRENT_DATE AT TIME ZONE 'UTC'",
                "postgres": "CURRENT_DATE AT TIME ZONE 'UTC'",
            },
        )
        self.validate_all(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            write={
                "bigquery": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
                "snowflake": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS FIRST LIMIT 10",
            },
        )
        self.validate_all(
            "SELECT cola, colb FROM (VALUES (1, 'test')) AS tab(cola, colb)",
            write={
                "spark": "SELECT cola, colb FROM VALUES (1, 'test') AS tab(cola, colb)",
                "bigquery": "SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb)])",
                "snowflake": "SELECT cola, colb FROM (VALUES (1, 'test')) AS tab(cola, colb)",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) d, COUNT(*) e FOR c IN ('x', 'y'))",
            write={
                "bigquery": "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) AS d, COUNT(*) AS e FOR c IN ('x', 'y'))",
            },
        )

    def test_user_defined_functions(self):
        self.validate_identity(
            "CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) RETURNS FLOAT64 NOT DETERMINISTIC LANGUAGE js AS 'return x*y;'"
        )
        self.validate_identity("CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) AS ((x + 4) / y)")
        self.validate_identity("CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE <q STRING, r INT64> AS SELECT s, t")
