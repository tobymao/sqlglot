from unittest import mock

from sqlglot import UnsupportedError, exp, parse_one
from tests.dialects.test_dialect import Validator


class TestPresto(Validator):
    dialect = "presto"

    def test_cast(self):
        self.validate_identity("CAST(x AS IPADDRESS)")
        self.validate_identity("CAST(x AS IPPREFIX)")

        self.validate_all(
            "CAST(x AS INTERVAL YEAR TO MONTH)",
            write={
                "oracle": "CAST(x AS INTERVAL YEAR TO MONTH)",
                "presto": "CAST(x AS INTERVAL YEAR TO MONTH)",
            },
        )
        self.validate_all(
            "CAST(x AS INTERVAL DAY TO SECOND)",
            write={
                "oracle": "CAST(x AS INTERVAL DAY TO SECOND)",
                "presto": "CAST(x AS INTERVAL DAY TO SECOND)",
            },
        )
        self.validate_all(
            "SELECT CAST('10C' AS INTEGER)",
            read={
                "postgres": "SELECT CAST('10C' AS INTEGER)",
                "presto": "SELECT CAST('10C' AS INTEGER)",
                "redshift": "SELECT CAST('10C' AS INTEGER)",
            },
        )
        self.validate_all(
            "SELECT DATE_DIFF('week', CAST(CAST('2009-01-01' AS TIMESTAMP) AS DATE), CAST(CAST('2009-12-31' AS TIMESTAMP) AS DATE))",
            read={"redshift": "SELECT DATEDIFF(week, '2009-01-01', '2009-12-31')"},
        )
        self.validate_all(
            "SELECT DATE_ADD('month', 18, CAST(CAST('2008-02-28' AS TIMESTAMP) AS DATE))",
            read={"redshift": "SELECT DATEADD(month, 18, '2008-02-28')"},
        )
        self.validate_all(
            "SELECT CAST('1970-01-01 00:00:00' AS TIMESTAMP)",
            read={"postgres": "SELECT 'epoch'::TIMESTAMP"},
        )
        self.validate_all(
            "FROM_BASE64(x)",
            read={
                "hive": "UNBASE64(x)",
            },
            write={
                "hive": "UNBASE64(x)",
                "presto": "FROM_BASE64(x)",
            },
        )
        self.validate_all(
            "TO_BASE64(x)",
            read={
                "hive": "BASE64(x)",
            },
            write={
                "hive": "BASE64(x)",
                "presto": "TO_BASE64(x)",
            },
        )
        self.validate_all(
            "CAST(a AS ARRAY(INT))",
            write={
                "bigquery": "CAST(a AS ARRAY<INT64>)",
                "duckdb": "CAST(a AS INT[])",
                "presto": "CAST(a AS ARRAY(INTEGER))",
                "spark": "CAST(a AS ARRAY<INT>)",
                "snowflake": "CAST(a AS ARRAY)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "spark": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
            write={
                "bigquery": "CAST([1, 2] AS ARRAY<INT64>)",
                "duckdb": "CAST([1, 2] AS BIGINT[])",
                "presto": "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
                "spark": "CAST(ARRAY(1, 2) AS ARRAY<BIGINT>)",
                "snowflake": "CAST([1, 2] AS ARRAY)",
            },
        )
        self.validate_all(
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INT,INT))",
            write={
                "bigquery": "CAST(MAP([1], [1]) AS MAP<INT64, INT64>)",
                "duckdb": "CAST(MAP([1], [1]) AS MAP(INT, INT))",
                "presto": "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INTEGER, INTEGER))",
                "hive": "CAST(MAP(1, 1) AS MAP<INT, INT>)",
                "spark": "CAST(MAP_FROM_ARRAYS(ARRAY(1), ARRAY(1)) AS MAP<INT, INT>)",
                "snowflake": "CAST(OBJECT_CONSTRUCT(1, 1) AS OBJECT)",
            },
        )
        self.validate_all(
            "CAST(MAP(ARRAY['a','b','c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INT)))",
            write={
                "bigquery": "CAST(MAP(['a', 'b', 'c'], [[1], [2], [3]]) AS MAP<STRING, ARRAY<INT64>>)",
                "duckdb": "CAST(MAP(['a', 'b', 'c'], [[1], [2], [3]]) AS MAP(TEXT, INT[]))",
                "presto": "CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INTEGER)))",
                "hive": "CAST(MAP('a', ARRAY(1), 'b', ARRAY(2), 'c', ARRAY(3)) AS MAP<STRING, ARRAY<INT>>)",
                "spark": "CAST(MAP_FROM_ARRAYS(ARRAY('a', 'b', 'c'), ARRAY(ARRAY(1), ARRAY(2), ARRAY(3))) AS MAP<STRING, ARRAY<INT>>)",
                "snowflake": "CAST(OBJECT_CONSTRUCT('a', [1], 'b', [2], 'c', [3]) AS OBJECT)",
            },
        )
        self.validate_all(
            "CAST(x AS TIME(5) WITH TIME ZONE)",
            write={
                "duckdb": "CAST(x AS TIMETZ)",
                "postgres": "CAST(x AS TIMETZ(5))",
                "presto": "CAST(x AS TIME(5) WITH TIME ZONE)",
                "redshift": "CAST(x AS TIME(5) WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
            write={
                "bigquery": "CAST(x AS TIMESTAMP)",
                "duckdb": "CAST(x AS TIMESTAMPTZ)",
                "presto": "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
                "hive": "CAST(x AS TIMESTAMP)",
                "spark": "CAST(x AS TIMESTAMP)",
            },
        )

    def test_regex(self):
        self.validate_all(
            "REGEXP_REPLACE('abcd', '[ab]')",
            write={
                "presto": "REGEXP_REPLACE('abcd', '[ab]', '')",
                "spark": "REGEXP_REPLACE('abcd', '[ab]', '')",
            },
        )
        self.validate_all(
            "REGEXP_LIKE(a, 'x')",
            write={
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "hive": "a RLIKE 'x'",
                "spark": "a RLIKE 'x'",
            },
        )
        self.validate_all(
            "SPLIT(x, 'a.')",
            write={
                "duckdb": "STR_SPLIT(x, 'a.')",
                "presto": "SPLIT(x, 'a.')",
                "hive": "SPLIT(x, CONCAT('\\\\Q', 'a.'))",
                "spark": "SPLIT(x, CONCAT('\\\\Q', 'a.'))",
            },
        )
        self.validate_all(
            "REGEXP_SPLIT(x, 'a.')",
            write={
                "duckdb": "STR_SPLIT_REGEX(x, 'a.')",
                "presto": "REGEXP_SPLIT(x, 'a.')",
                "hive": "SPLIT(x, 'a.')",
                "spark": "SPLIT(x, 'a.')",
            },
        )
        self.validate_all(
            "CARDINALITY(x)",
            write={
                "duckdb": "ARRAY_LENGTH(x)",
                "presto": "CARDINALITY(x)",
                "hive": "SIZE(x)",
                "spark": "SIZE(x)",
            },
        )
        self.validate_all(
            "ARRAY_JOIN(x, '-', 'a')",
            write={
                "hive": "CONCAT_WS('-', x)",
                "spark": "ARRAY_JOIN(x, '-', 'a')",
            },
        )

    def test_interval_plural_to_singular(self):
        # Microseconds, weeks and quarters are not supported in Presto/Trino INTERVAL literals
        unit_to_expected = {
            "SeCoNds": "second",
            "minutes": "minute",
            "hours": "hour",
            "days": "day",
            "months": "month",
            "years": "year",
        }

        for unit, expected in unit_to_expected.items():
            self.validate_all(
                f"SELECT INTERVAL '1' {unit}",
                write={
                    "bigquery": f"SELECT INTERVAL '1' {expected}",
                    "presto": f"SELECT INTERVAL '1' {expected}",
                    "trino": f"SELECT INTERVAL '1' {expected}",
                },
            )

    def test_time(self):
        expr = parse_one("TIME(7) WITH TIME ZONE", into=exp.DataType, read="presto")
        self.assertEqual(expr.this, exp.DataType.Type.TIMETZ)

        self.validate_identity("FROM_UNIXTIME(a, b)")
        self.validate_identity("FROM_UNIXTIME(a, b, c)")
        self.validate_identity("TRIM(a, b)")
        self.validate_identity("VAR_POP(a)")

        self.validate_all(
            "SELECT FROM_UNIXTIME(col) FROM tbl",
            write={
                "presto": "SELECT FROM_UNIXTIME(col) FROM tbl",
                "spark": "SELECT CAST(FROM_UNIXTIME(col) AS TIMESTAMP) FROM tbl",
                "trino": "SELECT FROM_UNIXTIME(col) FROM tbl",
            },
        )
        self.validate_all(
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
            write={
                "duckdb": "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d %T')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
                "spark": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%S')",
            write={
                "duckdb": "STRPTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_PARSE(x, '%Y-%m-%d %T')",
                "hive": "CAST(x AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "DATE_PARSE(x, '%Y-%m-%d')",
            write={
                "duckdb": "STRPTIME(x, '%Y-%m-%d')",
                "presto": "DATE_PARSE(x, '%Y-%m-%d')",
                "hive": "CAST(x AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "DATE_FORMAT(x, '%T')",
            write={
                "hive": "DATE_FORMAT(x, 'HH:mm:ss')",
            },
        )
        self.validate_all(
            "DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d')",
            write={
                "duckdb": "STRPTIME(SUBSTR(x, 1, 10), '%Y-%m-%d')",
                "presto": "DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d')",
                "hive": "CAST(SUBSTR(x, 1, 10) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(SUBSTR(x, 1, 10), 'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "FROM_UNIXTIME(x)",
            write={
                "duckdb": "TO_TIMESTAMP(x)",
                "presto": "FROM_UNIXTIME(x)",
                "hive": "FROM_UNIXTIME(x)",
                "spark": "CAST(FROM_UNIXTIME(x) AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "TO_UNIXTIME(x)",
            write={
                "duckdb": "EPOCH(x)",
                "presto": "TO_UNIXTIME(x)",
                "hive": "UNIX_TIMESTAMP(x)",
                "spark": "UNIX_TIMESTAMP(x)",
            },
        )
        self.validate_all(
            "DATE_ADD('day', 1, x)",
            write={
                "duckdb": "x + INTERVAL 1 day",
                "presto": "DATE_ADD('day', 1, x)",
                "hive": "DATE_ADD(x, 1)",
                "spark": "DATE_ADD(x, 1)",
            },
        )
        self.validate_all(
            "DATE_ADD('DAY', 1 * -1, x)",
            write={
                "presto": "DATE_ADD('DAY', 1 * -1, x)",
            },
            read={"mysql": "DATE_SUB(x, INTERVAL 1 DAY)"},
        )
        self.validate_all(
            "NOW()",
            write={
                "presto": "CURRENT_TIMESTAMP",
                "hive": "CURRENT_TIMESTAMP()",
            },
        )

        self.validate_all(
            "DAY_OF_WEEK(timestamp '2012-08-08 01:00:00')",
            write={
                "spark": "DAYOFWEEK(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "presto": "DAY_OF_WEEK(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "duckdb": "DAYOFWEEK(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
            },
        )

        self.validate_all(
            "DAY_OF_MONTH(timestamp '2012-08-08 01:00:00')",
            write={
                "spark": "DAYOFMONTH(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "presto": "DAY_OF_MONTH(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "duckdb": "DAYOFMONTH(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
            },
        )

        self.validate_all(
            "DAY_OF_YEAR(timestamp '2012-08-08 01:00:00')",
            write={
                "spark": "DAYOFYEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "presto": "DAY_OF_YEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "duckdb": "DAYOFYEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
            },
        )

        self.validate_all(
            "WEEK_OF_YEAR(timestamp '2012-08-08 01:00:00')",
            write={
                "spark": "WEEKOFYEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "presto": "WEEK_OF_YEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
                "duckdb": "WEEKOFYEAR(CAST('2012-08-08 01:00:00' AS TIMESTAMP))",
            },
        )

        self.validate_all(
            "SELECT timestamp '2012-10-31 00:00' AT TIME ZONE 'America/Sao_Paulo'",
            write={
                "spark": "SELECT FROM_UTC_TIMESTAMP(CAST('2012-10-31 00:00' AS TIMESTAMP), 'America/Sao_Paulo')",
                "presto": "SELECT CAST('2012-10-31 00:00' AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo'",
            },
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            write={"presto": "CAST(x AS TIMESTAMP)"},
            read={"mysql": "CAST(x AS DATETIME)", "clickhouse": "CAST(x AS DATETIME64)"},
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            read={"mysql": "TIMESTAMP(x)"},
        )

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            write={
                "duckdb": "CREATE TABLE test AS SELECT 1",
                "presto": "CREATE TABLE test WITH (FORMAT='PARQUET') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
                "spark": "CREATE TABLE test USING PARQUET AS SELECT 1",
            },
        )
        self.validate_all(
            "CREATE TABLE test STORED AS 'PARQUET' AS SELECT 1",
            write={
                "duckdb": "CREATE TABLE test AS SELECT 1",
                "presto": "CREATE TABLE test WITH (FORMAT='PARQUET') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
                "spark": "CREATE TABLE test USING PARQUET AS SELECT 1",
            },
        )
        self.validate_all(
            "CREATE TABLE test WITH (FORMAT = 'PARQUET', X = '1', Z = '2') AS SELECT 1",
            write={
                "duckdb": "CREATE TABLE test AS SELECT 1",
                "presto": "CREATE TABLE test WITH (FORMAT='PARQUET', X='1', Z='2') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET TBLPROPERTIES ('X'='1', 'Z'='2') AS SELECT 1",
                "spark": "CREATE TABLE test USING PARQUET TBLPROPERTIES ('X'='1', 'Z'='2') AS SELECT 1",
            },
        )
        self.validate_all(
            "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY=ARRAY['y', 'z'])",
            write={
                "duckdb": "CREATE TABLE x (w TEXT, y INT, z INT)",
                "presto": "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY=ARRAY['y', 'z'])",
                "hive": "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
                "spark": "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            },
        )
        self.validate_all(
            "CREATE TABLE x WITH (bucket_by = ARRAY['y'], bucket_count = 64) AS SELECT 1 AS y",
            write={
                "duckdb": "CREATE TABLE x AS SELECT 1 AS y",
                "presto": "CREATE TABLE x WITH (bucket_by=ARRAY['y'], bucket_count=64) AS SELECT 1 AS y",
                "hive": "CREATE TABLE x TBLPROPERTIES ('bucket_by'=ARRAY('y'), 'bucket_count'=64) AS SELECT 1 AS y",
                "spark": "CREATE TABLE x TBLPROPERTIES ('bucket_by'=ARRAY('y'), 'bucket_count'=64) AS SELECT 1 AS y",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
            write={
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
            write={
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
            },
        )

    def test_quotes(self):
        self.validate_all(
            "''''",
            write={
                "duckdb": "''''",
                "presto": "''''",
                "hive": "'\\''",
                "spark": "'\\''",
            },
        )
        self.validate_all(
            "'x'",
            write={
                "duckdb": "'x'",
                "presto": "'x'",
                "hive": "'x'",
                "spark": "'x'",
            },
        )
        self.validate_all(
            "'''x'''",
            write={
                "duckdb": "'''x'''",
                "presto": "'''x'''",
                "hive": "'\\'x\\''",
                "spark": "'\\'x\\''",
            },
        )
        self.validate_all(
            "'''x'",
            write={
                "duckdb": "'''x'",
                "presto": "'''x'",
                "hive": "'\\'x'",
                "spark": "'\\'x'",
            },
        )
        self.validate_all(
            "x IN ('a', 'a''b')",
            write={
                "duckdb": "x IN ('a', 'a''b')",
                "presto": "x IN ('a', 'a''b')",
                "hive": "x IN ('a', 'a\\'b')",
                "spark": "x IN ('a', 'a\\'b')",
            },
        )

    def test_unnest(self):
        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(ARRAY(y)) AS t (a)",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t(a)",
                "hive": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            },
        )

        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(ARRAY(y)) AS t (a) CROSS JOIN b",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t(a) CROSS JOIN b",
                "hive": "SELECT a FROM x CROSS JOIN b LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            },
        )

    @mock.patch("sqlglot.helper.logger")
    def test_presto(self, logger):
        self.validate_identity(
            "SELECT * FROM example.testdb.customer_orders FOR VERSION AS OF 8954597067493422955"
        )
        self.validate_identity(
            "SELECT * FROM example.testdb.customer_orders FOR TIMESTAMP AS OF CAST('2022-03-23 09:59:29.803 Europe/Vienna' AS TIMESTAMP)"
        )

        self.validate_identity("SELECT * FROM x OFFSET 1 LIMIT 1")
        self.validate_identity("SELECT * FROM x OFFSET 1 FETCH FIRST 1 ROWS ONLY")
        self.validate_identity("SELECT BOOL_OR(a > 10) FROM asd AS T(a)")
        self.validate_identity("SELECT * FROM (VALUES (1))")
        self.validate_identity("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE")
        self.validate_identity("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self.validate_identity("APPROX_PERCENTILE(a, b, c, d)")
        self.validate_identity(
            "SELECT SPLIT_TO_MAP('a:1;b:2;a:3', ';', ':', (k, v1, v2) -> CONCAT(v1, v2))"
        )

        self.validate_all(
            """JSON '"foo"'""",
            write={
                "bigquery": """PARSE_JSON('"foo"')""",
                "presto": """JSON_PARSE('"foo"')""",
                "snowflake": """PARSE_JSON('"foo"')""",
            },
        )
        self.validate_all(
            "SELECT ROW(1, 2)",
            read={
                "spark": "SELECT STRUCT(1, 2)",
            },
            write={
                "presto": "SELECT ROW(1, 2)",
                "spark": "SELECT STRUCT(1, 2)",
            },
        )
        self.validate_all(
            "ARBITRARY(x)",
            read={
                "bigquery": "ANY_VALUE(x)",
                "clickhouse": "any(x)",
                "databricks": "ANY_VALUE(x)",
                "doris": "ANY_VALUE(x)",
                "drill": "ANY_VALUE(x)",
                "duckdb": "ANY_VALUE(x)",
                "hive": "FIRST(x)",
                "mysql": "ANY_VALUE(x)",
                "oracle": "ANY_VALUE(x)",
                "redshift": "ANY_VALUE(x)",
                "snowflake": "ANY_VALUE(x)",
                "spark": "ANY_VALUE(x)",
                "spark2": "FIRST(x)",
            },
            write={
                "bigquery": "ANY_VALUE(x)",
                "clickhouse": "any(x)",
                "databricks": "ANY_VALUE(x)",
                "doris": "ANY_VALUE(x)",
                "drill": "ANY_VALUE(x)",
                "duckdb": "ANY_VALUE(x)",
                "hive": "FIRST(x)",
                "mysql": "ANY_VALUE(x)",
                "oracle": "ANY_VALUE(x)",
                "postgres": "MAX(x)",
                "presto": "ARBITRARY(x)",
                "redshift": "ANY_VALUE(x)",
                "snowflake": "ANY_VALUE(x)",
                "spark": "ANY_VALUE(x)",
                "spark2": "FIRST(x)",
                "sqlite": "MAX(x)",
                "tsql": "MAX(x)",
            },
        )
        self.validate_all(
            "STARTS_WITH('abc', 'a')",
            read={"spark": "STARTSWITH('abc', 'a')"},
            write={
                "presto": "STARTS_WITH('abc', 'a')",
                "snowflake": "STARTSWITH('abc', 'a')",
                "spark": "STARTSWITH('abc', 'a')",
            },
        )
        self.validate_all(
            "IS_NAN(x)",
            read={
                "spark": "ISNAN(x)",
            },
            write={
                "presto": "IS_NAN(x)",
                "spark": "ISNAN(x)",
                "spark2": "ISNAN(x)",
            },
        )
        self.validate_all("VALUES 1, 2, 3", write={"presto": "VALUES (1), (2), (3)"})
        self.validate_all("INTERVAL '1 day'", write={"trino": "INTERVAL '1' day"})
        self.validate_all("(5 * INTERVAL '7' day)", read={"": "INTERVAL '5' week"})
        self.validate_all("(5 * INTERVAL '7' day)", read={"": "INTERVAL '5' WEEKS"})
        self.validate_all(
            "SELECT COALESCE(ELEMENT_AT(MAP_FROM_ENTRIES(ARRAY[(51, '1')]), id), quantity) FROM my_table",
            write={
                "postgres": UnsupportedError,
                "presto": "SELECT COALESCE(ELEMENT_AT(MAP_FROM_ENTRIES(ARRAY[(51, '1')]), id), quantity) FROM my_table",
            },
        )
        self.validate_all(
            "SELECT ELEMENT_AT(ARRAY[1, 2, 3], 4)",
            write={
                "": "SELECT ARRAY(1, 2, 3)[3]",
                "postgres": "SELECT (ARRAY[1, 2, 3])[4]",
                "presto": "SELECT ELEMENT_AT(ARRAY[1, 2, 3], 4)",
            },
        )
        self.validate_all(
            "SELECT SUBSTRING(a, 1, 3), SUBSTRING(a, LENGTH(a) - (3 - 1))",
            read={
                "redshift": "SELECT LEFT(a, 3), RIGHT(a, 3)",
            },
        )
        self.validate_all(
            "WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT SUM(n) FROM t",
            read={
                "postgres": "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT SUM(n) FROM t",
            },
        )
        self.validate_all(
            "WITH RECURSIVE t(n, k) AS (SELECT 1 AS n, 2 AS k) SELECT SUM(n) FROM t",
            read={
                "postgres": "WITH RECURSIVE t AS (SELECT 1 AS n, 2 as k) SELECT SUM(n) FROM t",
            },
        )
        self.validate_all(
            "WITH RECURSIVE t1(n) AS (SELECT 1 AS n), t2(n) AS (SELECT 2 AS n) SELECT SUM(t1.n), SUM(t2.n) FROM t1, t2",
            read={
                "postgres": "WITH RECURSIVE t1 AS (SELECT 1 AS n), t2 AS (SELECT 2 AS n) SELECT SUM(t1.n), SUM(t2.n) FROM t1, t2",
            },
        )
        self.validate_all(
            "WITH RECURSIVE t(n, _c_0) AS (SELECT 1 AS n, (1 + 2)) SELECT * FROM t",
            read={
                "postgres": "WITH RECURSIVE t AS (SELECT 1 AS n, (1 + 2)) SELECT * FROM t",
            },
        )
        self.validate_all(
            'WITH RECURSIVE t(n, "1") AS (SELECT n, 1 FROM tbl) SELECT * FROM t',
            read={
                "postgres": "WITH RECURSIVE t AS (SELECT n, 1 FROM tbl) SELECT * FROM t",
            },
        )
        self.validate_all(
            "SELECT JSON_OBJECT(KEY 'key1' VALUE 1, KEY 'key2' VALUE TRUE)",
            write={
                "presto": "SELECT JSON_OBJECT('key1': 1, 'key2': TRUE)",
            },
        )
        self.validate_all(
            "ARRAY_AGG(x ORDER BY y DESC)",
            write={
                "hive": "COLLECT_LIST(x)",
                "presto": "ARRAY_AGG(x ORDER BY y DESC)",
                "spark": "COLLECT_LIST(x)",
                "trino": "ARRAY_AGG(x ORDER BY y DESC)",
            },
        )
        self.validate_all(
            "SELECT a FROM t GROUP BY a, ROLLUP(b), ROLLUP(c), ROLLUP(d)",
            write={
                "presto": "SELECT a FROM t GROUP BY a, ROLLUP (b, c, d)",
            },
        )
        self.validate_all(
            'SELECT a."b" FROM "foo"',
            write={
                "duckdb": 'SELECT a."b" FROM "foo"',
                "presto": 'SELECT a."b" FROM "foo"',
                "spark": "SELECT a.`b` FROM `foo`",
            },
        )
        self.validate_all(
            "SELECT ARRAY[1, 2]",
            write={
                "bigquery": "SELECT [1, 2]",
                "duckdb": "SELECT [1, 2]",
                "presto": "SELECT ARRAY[1, 2]",
                "spark": "SELECT ARRAY(1, 2)",
            },
        )
        self.validate_all(
            "SELECT APPROX_DISTINCT(a) FROM foo",
            write={
                "duckdb": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "presto": "SELECT APPROX_DISTINCT(a) FROM foo",
                "hive": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "spark": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            },
        )
        self.validate_all(
            "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
            write={
                "duckdb": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "presto": "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
                "hive": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "spark": "SELECT APPROX_COUNT_DISTINCT(a, 0.1) FROM foo",
            },
        )
        self.validate_all(
            "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
            write={
                "presto": "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
                "hive": UnsupportedError,
                "spark": "SELECT APPROX_COUNT_DISTINCT(a, 0.1) FROM foo",
            },
        )
        self.validate_all(
            "SELECT JSON_EXTRACT(x, '$.name')",
            write={
                "presto": "SELECT JSON_EXTRACT(x, '$.name')",
                "hive": "SELECT GET_JSON_OBJECT(x, '$.name')",
                "spark": "SELECT GET_JSON_OBJECT(x, '$.name')",
            },
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_SCALAR(x, '$.name')",
            write={
                "presto": "SELECT JSON_EXTRACT_SCALAR(x, '$.name')",
                "hive": "SELECT GET_JSON_OBJECT(x, '$.name')",
                "spark": "SELECT GET_JSON_OBJECT(x, '$.name')",
            },
        )
        self.validate_all(
            "'\u6bdb'",
            write={
                "presto": "'\u6bdb'",
                "hive": "'\u6bdb'",
                "spark": "'\u6bdb'",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            write={
                "duckdb": "SELECT ARRAY_SORT(x)",
                "presto": "SELECT ARRAY_SORT(x, (left, right) -> -1)",
                "hive": "SELECT SORT_ARRAY(x)",
                "spark": "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SORT(x)",
            write={
                "presto": "SELECT ARRAY_SORT(x)",
                "hive": "SELECT SORT_ARRAY(x)",
                "spark": "SELECT ARRAY_SORT(x)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            write={
                "hive": UnsupportedError,
            },
        )
        self.validate_all(
            "MAP(a, b)",
            write={
                "hive": UnsupportedError,
                "spark": "MAP_FROM_ARRAYS(a, b)",
                "snowflake": UnsupportedError,
            },
        )
        self.validate_all(
            "MAP(ARRAY(a, b), ARRAY(c, d))",
            write={
                "hive": "MAP(a, c, b, d)",
                "presto": "MAP(ARRAY[a, b], ARRAY[c, d])",
                "spark": "MAP_FROM_ARRAYS(ARRAY(a, b), ARRAY(c, d))",
                "snowflake": "OBJECT_CONSTRUCT(a, c, b, d)",
            },
        )
        self.validate_all(
            "MAP(ARRAY('a'), ARRAY('b'))",
            write={
                "hive": "MAP('a', 'b')",
                "presto": "MAP(ARRAY['a'], ARRAY['b'])",
                "spark": "MAP_FROM_ARRAYS(ARRAY('a'), ARRAY('b'))",
                "snowflake": "OBJECT_CONSTRUCT('a', 'b')",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x",
            write={
                "bigquery": "SELECT * FROM UNNEST(['7', '14'])",
                "presto": "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x",
                "hive": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS x",
                "spark": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS x",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x(y)",
            write={
                "bigquery": "SELECT * FROM UNNEST(['7', '14']) AS y",
                "presto": "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x(y)",
                "hive": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS x(y)",
                "spark": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS x(y)",
            },
        )
        self.validate_all(
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100 ) SELECT sum(n) FROM t",
            write={
                "presto": "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 100) SELECT SUM(n) FROM t",
                "spark": UnsupportedError,
            },
        )

        self.validate_all(
            "SELECT a, b, c, d, sum(y) FROM z GROUP BY CUBE(a) ROLLUP(a), GROUPING SETS((b, c)), d",
            write={
                "presto": "SELECT a, b, c, d, SUM(y) FROM z GROUP BY d, GROUPING SETS ((b, c)), CUBE (a), ROLLUP (a)",
                "hive": "SELECT a, b, c, d, SUM(y) FROM z GROUP BY d, GROUPING SETS ((b, c)), CUBE (a), ROLLUP (a)",
            },
        )
        self.validate_all(
            "JSON_FORMAT(x)",
            read={
                "spark": "TO_JSON(x)",
            },
            write={
                "bigquery": "TO_JSON_STRING(x)",
                "duckdb": "CAST(TO_JSON(x) AS TEXT)",
                "presto": "JSON_FORMAT(x)",
                "spark": "TO_JSON(x)",
            },
        )
        self.validate_all(
            """JSON_FORMAT(JSON '"x"')""",
            write={
                "bigquery": """TO_JSON_STRING(PARSE_JSON('"x"'))""",
                "duckdb": """CAST(TO_JSON(JSON('"x"')) AS TEXT)""",
                "presto": """JSON_FORMAT(JSON_PARSE('"x"'))""",
                "spark": """REGEXP_EXTRACT(TO_JSON(FROM_JSON('["x"]', SCHEMA_OF_JSON('["x"]'))), '^.(.*).$', 1)""",
            },
        )
        self.validate_all(
            """SELECT JSON_FORMAT(JSON '{"a": 1, "b": "c"}')""",
            write={
                "spark": """SELECT REGEXP_EXTRACT(TO_JSON(FROM_JSON('[{"a": 1, "b": "c"}]', SCHEMA_OF_JSON('[{"a": 1, "b": "c"}]'))), '^.(.*).$', 1)""",
            },
        )
        self.validate_all(
            """SELECT JSON_FORMAT(JSON '[1, 2, 3]')""",
            write={
                "spark": "SELECT REGEXP_EXTRACT(TO_JSON(FROM_JSON('[[1, 2, 3]]', SCHEMA_OF_JSON('[[1, 2, 3]]'))), '^.(.*).$', 1)",
            },
        )

    def test_encode_decode(self):
        self.validate_identity("FROM_UTF8(x, y)")

        self.validate_all(
            "TO_UTF8(x)",
            read={
                "duckdb": "ENCODE(x)",
                "spark": "ENCODE(x, 'utf-8')",
            },
            write={
                "duckdb": "ENCODE(x)",
                "presto": "TO_UTF8(x)",
                "spark": "ENCODE(x, 'utf-8')",
            },
        )
        self.validate_all(
            "FROM_UTF8(x)",
            read={
                "duckdb": "DECODE(x)",
                "spark": "DECODE(x, 'utf-8')",
            },
            write={
                "duckdb": "DECODE(x)",
                "presto": "FROM_UTF8(x)",
                "spark": "DECODE(x, 'utf-8')",
            },
        )
        self.validate_all(
            "ENCODE(x, 'invalid')",
            write={
                "presto": UnsupportedError,
                "duckdb": UnsupportedError,
            },
        )
        self.validate_all(
            "DECODE(x, 'invalid')",
            write={
                "presto": UnsupportedError,
                "duckdb": UnsupportedError,
            },
        )

    def test_hex_unhex(self):
        self.validate_all(
            "TO_HEX(x)",
            write={
                "spark": "HEX(x)",
            },
        )
        self.validate_all(
            "FROM_HEX(x)",
            write={
                "spark": "UNHEX(x)",
            },
        )
        self.validate_all(
            "HEX(x)",
            write={
                "presto": "TO_HEX(x)",
            },
        )
        self.validate_all(
            "UNHEX(x)",
            write={
                "presto": "FROM_HEX(x)",
            },
        )

    def test_json(self):
        self.validate_all(
            "SELECT CAST(JSON '[1,23,456]' AS ARRAY(INTEGER))",
            write={
                "spark": "SELECT FROM_JSON('[1,23,456]', 'ARRAY<INT>')",
                "presto": "SELECT CAST(JSON_PARSE('[1,23,456]') AS ARRAY(INTEGER))",
            },
        )
        self.validate_all(
            """SELECT CAST(JSON '{"k1":1,"k2":23,"k3":456}' AS MAP(VARCHAR, INTEGER))""",
            write={
                "spark": 'SELECT FROM_JSON(\'{"k1":1,"k2":23,"k3":456}\', \'MAP<STRING, INT>\')',
                "presto": 'SELECT CAST(JSON_PARSE(\'{"k1":1,"k2":23,"k3":456}\') AS MAP(VARCHAR, INTEGER))',
            },
        )

        self.validate_all(
            "SELECT CAST(ARRAY [1, 23, 456] AS JSON)",
            write={
                "spark": "SELECT TO_JSON(ARRAY(1, 23, 456))",
                "presto": "SELECT CAST(ARRAY[1, 23, 456] AS JSON)",
            },
        )

    def test_match_recognize(self):
        self.validate_identity(
            """SELECT
  *
FROM orders
MATCH_RECOGNIZE (
  PARTITION BY custkey
  ORDER BY
    orderdate
  MEASURES
    A.totalprice AS starting_price,
    LAST(B.totalprice) AS bottom_price,
    LAST(C.totalprice) AS top_price
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B+ C+ D+)
  DEFINE
    B AS totalprice < PREV(totalprice),
    C AS totalprice > PREV(totalprice) AND totalprice <= A.totalprice,
    D AS totalprice > PREV(totalprice)
)""",
            pretty=True,
        )
