from unittest import mock

from sqlglot import ErrorLevel, ParseError, TokenError, UnsupportedError, transpile
from tests.dialects.test_dialect import Validator


class TestBigQuery(Validator):
    dialect = "bigquery"

    def test_bigquery(self):
        with self.assertRaises(TokenError):
            transpile("'\\'", read="bigquery")

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

        self.validate_identity("SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008', 'UTC')")
        self.validate_identity("SELECT ANY_VALUE(fruit HAVING MAX sold) FROM fruits")
        self.validate_identity("SELECT ANY_VALUE(fruit HAVING MIN sold) FROM fruits")
        self.validate_identity("SELECT `project-id`.udfs.func(call.dir)")
        self.validate_identity("SELECT CAST(CURRENT_DATE AS STRING FORMAT 'DAY') AS current_day")
        self.validate_identity("SAFE_CAST(encrypted_value AS STRING FORMAT 'BASE64')")
        self.validate_identity("CAST(encrypted_value AS STRING FORMAT 'BASE64')")
        self.validate_identity("CAST(STRUCT<a INT64>(1) AS STRUCT<a INT64>)")
        self.validate_identity("STRING_AGG(a)")
        self.validate_identity("STRING_AGG(a, ' & ')")
        self.validate_identity("STRING_AGG(DISTINCT a, ' & ')")
        self.validate_identity("STRING_AGG(a, ' & ' ORDER BY LENGTH(a))")
        self.validate_identity("DATE(2016, 12, 25)")
        self.validate_identity("DATE(CAST('2016-12-25 23:59:59' AS DATETIME))")
        self.validate_identity("SELECT foo IN UNNEST(bar) AS bla")
        self.validate_identity("SELECT * FROM x-0.a")
        self.validate_identity("SELECT * FROM pivot CROSS JOIN foo")
        self.validate_identity("SAFE_CAST(x AS STRING)")
        self.validate_identity("SELECT * FROM a-b-c.mydataset.mytable")
        self.validate_identity("SELECT * FROM abc-def-ghi")
        self.validate_identity("SELECT * FROM a-b-c")
        self.validate_identity("SELECT * FROM my-table")
        self.validate_identity("SELECT * FROM my-project.mydataset.mytable")
        self.validate_identity("SELECT * FROM pro-ject_id.c.d CROSS JOIN foo-bar")
        self.validate_identity("x <> ''")
        self.validate_identity("DATE_TRUNC(col, WEEK(MONDAY))")
        self.validate_identity("SELECT b'abc'")
        self.validate_identity("""SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])""")
        self.validate_identity("SELECT AS STRUCT 1 AS a, 2 AS b")
        self.validate_identity("SELECT DISTINCT AS STRUCT 1 AS a, 2 AS b")
        self.validate_identity("SELECT AS VALUE STRUCT(1 AS a, 2 AS b)")
        self.validate_identity("SELECT STRUCT<ARRAY<STRING>>(['2023-01-17'])")
        self.validate_identity("SELECT STRUCT<STRING>((SELECT a FROM b.c LIMIT 1)).*")
        self.validate_identity("SELECT * FROM q UNPIVOT(values FOR quarter IN (b, c))")
        self.validate_identity("""CREATE TABLE x (a STRUCT<values ARRAY<INT64>>)""")
        self.validate_identity("""CREATE TABLE x (a STRUCT<b STRING OPTIONS (description='b')>)""")
        self.validate_identity("CAST(x AS TIMESTAMP)")
        self.validate_identity("REGEXP_EXTRACT(`foo`, 'bar: (.+?)', 1, 1)")
        self.validate_identity("BEGIN A B C D E F")
        self.validate_identity("BEGIN TRANSACTION")
        self.validate_identity("COMMIT TRANSACTION")
        self.validate_identity("ROLLBACK TRANSACTION")
        self.validate_identity("CAST(x AS BIGNUMERIC)")
        self.validate_identity(
            "DATE(CAST('2016-12-25 05:30:00+07' AS DATETIME), 'America/Los_Angeles')"
        )
        self.validate_identity(
            """CREATE TABLE x (a STRING OPTIONS (description='x')) OPTIONS (table_expiration_days=1)"""
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM `t`) AS a UNPIVOT((c) FOR c_name IN (v1, v2))"
        )
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM bla EXCEPT DISTINCT (SELECT * FROM bar) LIMIT 0"
        )
        self.validate_identity(
            "SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)"
        )
        self.validate_identity(
            "SELECT item, purchases, LAST_VALUE(item) OVER (item_window ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce WINDOW item_window AS (ORDER BY purchases)"
        )
        self.validate_identity(
            "SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)",
        )

        self.validate_all("SELECT SPLIT(foo)", write={"bigquery": "SELECT SPLIT(foo, ',')"})
        self.validate_all("SELECT 1 AS hash", write={"bigquery": "SELECT 1 AS `hash`"})
        self.validate_all("SELECT 1 AS at", write={"bigquery": "SELECT 1 AS `at`"})
        self.validate_all('x <> ""', write={"bigquery": "x <> ''"})
        self.validate_all('x <> """"""', write={"bigquery": "x <> ''"})
        self.validate_all("x <> ''''''", write={"bigquery": "x <> ''"})
        self.validate_all("CAST(x AS DATETIME)", read={"": "x::timestamp"})
        self.validate_all("LEAST(x, y)", read={"sqlite": "MIN(x, y)"})
        self.validate_all("CAST(x AS CHAR)", write={"bigquery": "CAST(x AS STRING)"})
        self.validate_all("CAST(x AS NCHAR)", write={"bigquery": "CAST(x AS STRING)"})
        self.validate_all("CAST(x AS NVARCHAR)", write={"bigquery": "CAST(x AS STRING)"})
        self.validate_all("CAST(x AS TIMESTAMPTZ)", write={"bigquery": "CAST(x AS TIMESTAMP)"})
        self.validate_all("CAST(x AS RECORD)", write={"bigquery": "CAST(x AS STRUCT)"})
        self.validate_all(
            "MD5(x)",
            write={
                "": "MD5_DIGEST(x)",
                "bigquery": "MD5(x)",
                "hive": "UNHEX(MD5(x))",
                "spark": "UNHEX(MD5(x))",
            },
        )
        self.validate_all(
            "SELECT TO_HEX(MD5(some_string))",
            read={
                "duckdb": "SELECT MD5(some_string)",
                "spark": "SELECT MD5(some_string)",
            },
            write={
                "": "SELECT MD5(some_string)",
                "bigquery": "SELECT TO_HEX(MD5(some_string))",
                "duckdb": "SELECT MD5(some_string)",
            },
        )
        self.validate_all(
            "SELECT CAST('20201225' AS TIMESTAMP FORMAT 'YYYYMMDD' AT TIME ZONE 'America/New_York')",
            write={"bigquery": "SELECT PARSE_TIMESTAMP('%Y%m%d', '20201225', 'America/New_York')"},
        )
        self.validate_all(
            "SELECT CAST('20201225' AS TIMESTAMP FORMAT 'YYYYMMDD')",
            write={"bigquery": "SELECT PARSE_TIMESTAMP('%Y%m%d', '20201225')"},
        )
        self.validate_all(
            "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string",
            write={
                "bigquery": "SELECT CAST(CAST('2008-12-25 00:00:00+00:00' AS TIMESTAMP) AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string",
            },
        )
        self.validate_all(
            "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string",
            write={
                "bigquery": "SELECT CAST(CAST('2008-12-25 00:00:00+00:00' AS TIMESTAMP) AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string",
            },
        )
        self.validate_all(
            "WITH cte AS (SELECT [1, 2, 3] AS arr) SELECT col FROM cte CROSS JOIN UNNEST(arr) AS col",
            read={
                "spark": "WITH cte AS (SELECT ARRAY(1, 2, 3) AS arr) SELECT EXPLODE(arr) FROM cte"
            },
        )
        self.validate_all(
            "SELECT AS STRUCT ARRAY(SELECT AS STRUCT b FROM x) AS y FROM z",
            write={
                "": "SELECT AS STRUCT ARRAY(SELECT AS STRUCT b FROM x) AS y FROM z",
                "bigquery": "SELECT AS STRUCT ARRAY(SELECT AS STRUCT b FROM x) AS y FROM z",
                "duckdb": "SELECT {'y': ARRAY(SELECT {'b': b} FROM x)} FROM z",
            },
        )
        self.validate_all(
            "cast(x as date format 'MM/DD/YYYY')",
            write={
                "bigquery": "PARSE_DATE('%m/%d/%Y', x)",
            },
        )
        self.validate_all(
            "cast(x as time format 'YYYY.MM.DD HH:MI:SSTZH')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%Y.%m.%d %I:%M:%S%z', x)",
            },
        )
        self.validate_all(
            "CREATE TEMP TABLE foo AS SELECT 1",
            write={"bigquery": "CREATE TEMPORARY TABLE foo AS SELECT 1"},
        )
        self.validate_all(
            "SELECT * FROM `SOME_PROJECT_ID.SOME_DATASET_ID.INFORMATION_SCHEMA.SOME_VIEW`",
            write={
                "bigquery": "SELECT * FROM SOME_PROJECT_ID.SOME_DATASET_ID.INFORMATION_SCHEMA.SOME_VIEW",
            },
        )
        self.validate_all(
            "SELECT * FROM `my-project.my-dataset.my-table`",
            write={"bigquery": "SELECT * FROM `my-project`.`my-dataset`.`my-table`"},
        )
        self.validate_all(
            "SELECT ARRAY(SELECT AS STRUCT 1 a, 2 b)",
            write={
                "bigquery": "SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b)",
            },
        )
        self.validate_all(
            "REGEXP_CONTAINS('foo', '.*')",
            read={"bigquery": "REGEXP_CONTAINS('foo', '.*')"},
            write={"mysql": "REGEXP_LIKE('foo', '.*')"},
        ),
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
            "r'x\\''",
            write={
                "bigquery": "'x\\''",
                "hive": "'x\\''",
            },
        )

        self.validate_all(
            "r'x\\y'",
            write={
                "bigquery": "'x\\\y'",
                "hive": "'x\\\\y'",
            },
        )
        self.validate_all(
            "'\\\\'",
            write={
                "bigquery": r"'\\'",
                "duckdb": r"'\\'",
                "presto": r"'\\'",
                "hive": r"'\\'",
            },
        )
        self.validate_all(
            r'r"""/\*.*\*/"""',
            write={
                "bigquery": r"'/\\*.*\\*/'",
                "duckdb": r"'/\\*.*\\*/'",
                "presto": r"'/\\*.*\\*/'",
                "hive": r"'/\\*.*\\*/'",
                "spark": r"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            r'R"""/\*.*\*/"""',
            write={
                "bigquery": r"'/\\*.*\\*/'",
                "duckdb": r"'/\\*.*\\*/'",
                "presto": r"'/\\*.*\\*/'",
                "hive": r"'/\\*.*\\*/'",
                "spark": r"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            'r"""a\n"""',
            write={
                "bigquery": "'a\\n'",
                "duckdb": "'a\n'",
            },
        )
        self.validate_all(
            '"""a\n"""',
            write={
                "bigquery": "'a\\n'",
                "duckdb": "'a\n'",
            },
        )
        self.validate_all(
            "CAST(a AS INT64)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "duckdb": "CAST(a AS BIGINT)",
                "presto": "CAST(a AS BIGINT)",
                "hive": "CAST(a AS BIGINT)",
                "spark": "CAST(a AS BIGINT)",
            },
        )
        self.validate_all(
            "CAST(a AS BYTES)",
            write={
                "bigquery": "CAST(a AS BYTES)",
                "duckdb": "CAST(a AS BLOB)",
                "presto": "CAST(a AS VARBINARY)",
                "hive": "CAST(a AS BINARY)",
                "spark": "CAST(a AS BINARY)",
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
            "SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)",
            write={"bigquery": "SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)"},
        )
        self.validate_all(
            "SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers",
            write={
                "bigquery": "SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers"
            },
        )
        self.validate_all(
            "SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)",
            write={"bigquery": "SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)"},
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
            "CURRENT_TIMESTAMP()",
            read={
                "tsql": "GETDATE()",
            },
            write={
                "tsql": "GETDATE()",
            },
        )
        self.validate_all(
            "current_datetime",
            write={
                "bigquery": "CURRENT_DATETIME()",
                "presto": "CURRENT_DATETIME()",
                "hive": "CURRENT_DATETIME()",
                "spark": "CURRENT_DATETIME()",
            },
        )
        self.validate_all(
            "current_time",
            write={
                "bigquery": "CURRENT_TIME()",
                "duckdb": "CURRENT_TIME",
                "presto": "CURRENT_TIME()",
                "hive": "CURRENT_TIME()",
                "spark": "CURRENT_TIME()",
            },
        )
        self.validate_all(
            "current_timestamp",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "current_timestamp()",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "DIV(x, y)",
            write={
                "bigquery": "DIV(x, y)",
                "duckdb": "x // y",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a BIGINT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a BIGINT, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a BIGINT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: BIGINT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (x int) PARTITION BY x cluster by x",
            write={
                "bigquery": "CREATE TABLE db.example_table (x INT64) PARTITION BY x CLUSTER BY x",
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
        self.validate_all(
            "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            write={
                "postgres": "CURRENT_DATE - INTERVAL '1 DAY'",
                "bigquery": "DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)",
            write={
                "bigquery": "DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
                "duckdb": "CURRENT_DATE + INTERVAL 1 DAY",
                "mysql": "DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
                "postgres": "CURRENT_DATE + INTERVAL '1 DAY'",
                "presto": "DATE_ADD('DAY', 1, CURRENT_DATE)",
                "hive": "DATE_ADD(CURRENT_DATE, 1)",
                "spark": "DATE_ADD(CURRENT_DATE, 1)",
            },
        )
        self.validate_all(
            "DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY)",
            write={
                "bigquery": "DATE_DIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE), DAY)",
                "mysql": "DATEDIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', MINUTE)",
            write={
                "bigquery": "DATE_DIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE), MINUTE)",
                "mysql": "DATEDIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
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
            "SELECT cola, colb FROM (VALUES (1, 'test')) AS tab",
            write={
                "bigquery": "SELECT cola, colb FROM UNNEST([STRUCT(1 AS _c0, 'test' AS _c1)])",
            },
        )
        self.validate_all(
            "SELECT cola, colb FROM (VALUES (1, 'test'))",
            write={
                "bigquery": "SELECT cola, colb FROM UNNEST([STRUCT(1 AS _c0, 'test' AS _c1)])",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST([STRUCT(1 AS id)]) CROSS JOIN UNNEST([STRUCT(1 AS id)])",
            read={
                "postgres": "SELECT * FROM (VALUES (1)) AS t1(id) CROSS JOIN (VALUES (1)) AS t2(id)",
            },
        )
        self.validate_all(
            "SELECT cola, colb, colc FROM (VALUES (1, 'test', NULL)) AS tab(cola, colb, colc)",
            write={
                "spark": "SELECT cola, colb, colc FROM VALUES (1, 'test', NULL) AS tab(cola, colb, colc)",
                "bigquery": "SELECT cola, colb, colc FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb, NULL AS colc)])",
                "snowflake": "SELECT cola, colb, colc FROM (VALUES (1, 'test', NULL)) AS tab(cola, colb, colc)",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) d, COUNT(*) e FOR c IN ('x', 'y'))",
            write={
                "bigquery": "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) AS d, COUNT(*) AS e FOR c IN ('x', 'y'))",
            },
        )
        self.validate_all(
            "SELECT REGEXP_EXTRACT(abc, 'pattern(group)') FROM table",
            write={
                "bigquery": "SELECT REGEXP_EXTRACT(abc, 'pattern(group)') FROM table",
                "duckdb": "SELECT REGEXP_EXTRACT(abc, 'pattern(group)', 1) FROM table",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST([1]) WITH OFFSET",
            write={"bigquery": "SELECT * FROM UNNEST([1]) WITH OFFSET AS offset"},
        )
        self.validate_all(
            "SELECT * FROM UNNEST([1]) WITH OFFSET y",
            write={"bigquery": "SELECT * FROM UNNEST([1]) WITH OFFSET AS y"},
        )
        self.validate_all(
            "GENERATE_ARRAY(1, 4)",
            read={"bigquery": "GENERATE_ARRAY(1, 4)"},
            write={"duckdb": "GENERATE_SERIES(1, 4)"},
        )
        self.validate_all(
            "TO_JSON_STRING(x)",
            read={"bigquery": "TO_JSON_STRING(x)"},
            write={
                "bigquery": "TO_JSON_STRING(x)",
                "duckdb": "CAST(TO_JSON(x) AS TEXT)",
                "presto": "JSON_FORMAT(x)",
                "spark": "TO_JSON(x)",
            },
        )

        self.validate_identity(
            "SELECT y + 1 z FROM x GROUP BY y + 1 ORDER BY z",
            "SELECT y + 1 AS z FROM x GROUP BY z ORDER BY z",
        )
        self.validate_identity(
            "SELECT y + 1 z FROM x GROUP BY y + 1",
            "SELECT y + 1 AS z FROM x GROUP BY y + 1",
        )
        self.validate_identity("SELECT y + 1 FROM x GROUP BY y + 1 ORDER BY 1")

    def test_user_defined_functions(self):
        self.validate_identity(
            "CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) RETURNS FLOAT64 NOT DETERMINISTIC LANGUAGE js AS 'return x*y;'"
        )
        self.validate_identity("CREATE TEMPORARY FUNCTION udf(x ANY TYPE) AS (x)")
        self.validate_identity("CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) AS ((x + 4) / y)")
        self.validate_identity(
            "CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE <q STRING, r INT64> AS SELECT s, t"
        )

    def test_group_concat(self):
        self.validate_all(
            "SELECT a, GROUP_CONCAT(b) FROM table GROUP BY a",
            write={"bigquery": "SELECT a, STRING_AGG(b) FROM table GROUP BY a"},
        )

    def test_remove_precision_parameterized_types(self):
        self.validate_all(
            "SELECT CAST(1 AS NUMERIC(10, 2))",
            write={
                "bigquery": "SELECT CAST(1 AS NUMERIC)",
            },
        )
        self.validate_all(
            "CREATE TABLE test (a NUMERIC(10, 2))",
            write={
                "bigquery": "CREATE TABLE test (a NUMERIC(10, 2))",
            },
        )
        self.validate_all(
            "SELECT CAST('1' AS STRING(10)) UNION ALL SELECT CAST('2' AS STRING(10))",
            write={
                "bigquery": "SELECT CAST('1' AS STRING) UNION ALL SELECT CAST('2' AS STRING)",
            },
        )
        self.validate_all(
            "SELECT cola FROM (SELECT CAST('1' AS STRING(10)) AS cola UNION ALL SELECT CAST('2' AS STRING(10)) AS cola)",
            write={
                "bigquery": "SELECT cola FROM (SELECT CAST('1' AS STRING) AS cola UNION ALL SELECT CAST('2' AS STRING) AS cola)",
            },
        )
        self.validate_all(
            "INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING(10)), CAST(14 AS STRING(10)))",
            write={
                "bigquery": "INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING), CAST(14 AS STRING))",
            },
        )

    def test_merge(self):
        self.validate_all(
            """
            MERGE dataset.Inventory T
            USING dataset.NewArrivals S ON FALSE
            WHEN NOT MATCHED BY TARGET AND product LIKE '%a%'
            THEN DELETE
            WHEN NOT MATCHED BY SOURCE AND product LIKE '%b%'
            THEN DELETE""",
            write={
                "bigquery": "MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE '%a%' THEN DELETE WHEN NOT MATCHED BY SOURCE AND product LIKE '%b%' THEN DELETE",
                "snowflake": "MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE '%a%' THEN DELETE WHEN NOT MATCHED AND product LIKE '%b%' THEN DELETE",
            },
        )

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "snowflake": "ALTER TABLE db.t1 RENAME TO db.t2",
                "bigquery": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )

    @mock.patch("sqlglot.dialects.bigquery.logger")
    def test_pushdown_cte_column_names(self, logger):
        with self.assertRaises(UnsupportedError):
            transpile(
                "WITH cte(foo) AS (SELECT * FROM tbl) SELECT foo FROM cte",
                read="spark",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte",
            read={"spark": "WITH cte(foo) AS (SELECT 1) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte",
            read={"spark": "WITH cte(foo) AS (SELECT 1 AS bar) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte",
            read={"spark": "WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo, 2) SELECT foo FROM cte",
            read={"postgres": "WITH cte(foo) AS (SELECT 1, 2) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo UNION ALL SELECT 2) SELECT foo FROM cte",
            read={"postgres": "WITH cte(foo) AS (SELECT 1 UNION ALL SELECT 2) SELECT foo FROM cte"},
        )
