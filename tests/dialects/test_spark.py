from unittest import mock

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import Dialects
from tests.dialects.test_dialect import Validator


class TestSpark(Validator):
    dialect = "spark"

    def test_ddl(self):
        self.validate_identity("CREATE TABLE foo AS WITH t AS (SELECT 1 AS col) SELECT col FROM t")
        self.validate_identity("CREATE TEMPORARY VIEW test AS SELECT 1")
        self.validate_identity("CREATE TABLE foo (col VARCHAR(50))")
        self.validate_identity("CREATE TABLE foo (col STRUCT<struct_col_a: VARCHAR((50))>)")
        self.validate_identity("CREATE TABLE foo (col STRING) CLUSTERED BY (col) INTO 10 BUCKETS")
        self.validate_identity(
            "CREATE TABLE foo (col STRING) CLUSTERED BY (col) SORTED BY (col) INTO 10 BUCKETS"
        )
        self.validate_identity("TRUNCATE TABLE t1 PARTITION(age = 10, name = 'test', address)")

        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            write={
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a array<int>, col_b array<array<int>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a ARRAY<INT64>, col_b ARRAY<ARRAY<INT64>>)",
                "duckdb": "CREATE TABLE db.example_table (col_a INT[], col_b INT[][])",
                "presto": "CREATE TABLE db.example_table (col_a ARRAY(INTEGER), col_b ARRAY(ARRAY(INTEGER)))",
                "hive": "CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)",
                "spark": "CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)",
                "snowflake": "CREATE TABLE db.example_table (col_a ARRAY, col_b ARRAY)",
            },
        )
        self.validate_all(
            "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            write={
                "duckdb": "CREATE TABLE x",
                "presto": "CREATE TABLE x WITH (FORMAT='ICEBERG', PARTITIONED_BY=ARRAY['MONTHS'])",
                "hive": "CREATE TABLE x STORED AS ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
                "spark": "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            },
        )
        self.validate_all(
            "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
            write={
                "duckdb": "CREATE TABLE test AS SELECT 1",
                "presto": "CREATE TABLE test WITH (FORMAT='PARQUET') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
                "spark": "CREATE TABLE test USING PARQUET AS SELECT 1",
            },
        )
        self.validate_all(
            """CREATE TABLE blah (col_a INT) COMMENT "Test comment: blah" PARTITIONED BY (date STRING) STORED AS ICEBERG TBLPROPERTIES('x' = '1')""",
            write={
                "duckdb": """CREATE TABLE blah (
  col_a INT
)""",  # Partition columns should exist in table
                "presto": """CREATE TABLE blah (
  col_a INTEGER,
  date VARCHAR
)
COMMENT 'Test comment: blah'
WITH (
  PARTITIONED_BY=ARRAY['date'],
  FORMAT='ICEBERG',
  x='1'
)""",
                "hive": """CREATE TABLE blah (
  col_a INT
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date STRING
)
STORED AS ICEBERG
TBLPROPERTIES (
  'x'='1'
)""",
                "spark": """CREATE TABLE blah (
  col_a INT,
  date STRING
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date
)
USING ICEBERG
TBLPROPERTIES (
  'x'='1'
)""",
            },
            pretty=True,
        )

        self.validate_all(
            "CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData",
            write={
                "spark": "CACHE TABLE testCache OPTIONS('storageLevel' = 'DISK_ONLY') AS SELECT * FROM testData"
            },
        )
        self.validate_all(
            "ALTER TABLE StudentInfo ADD COLUMNS (LastName STRING, DOB TIMESTAMP)",
            write={
                "spark": "ALTER TABLE StudentInfo ADD COLUMNS (LastName STRING, DOB TIMESTAMP)",
            },
        )
        self.validate_all(
            "ALTER TABLE StudentInfo DROP COLUMNS (LastName, DOB)",
            write={
                "spark": "ALTER TABLE StudentInfo DROP COLUMNS (LastName, DOB)",
            },
        )

    def test_to_date(self):
        self.validate_all(
            "TO_DATE(x, 'yyyy-MM-dd')",
            write={
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(CAST(x AS TIMESTAMP) AS DATE)",
                "spark": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "TO_DATE(x, 'yyyy')",
            write={
                "duckdb": "CAST(STRPTIME(x, '%Y') AS DATE)",
                "hive": "TO_DATE(x, 'yyyy')",
                "presto": "CAST(DATE_PARSE(x, '%Y') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy')",
            },
        )

    @mock.patch("sqlglot.generator.logger")
    def test_hint(self, logger):
        self.validate_all(
            "SELECT /*+ COALESCE(3) */ * FROM x",
            write={
                "spark": "SELECT /*+ COALESCE(3) */ * FROM x",
                "bigquery": "SELECT * FROM x",
            },
        )
        self.validate_all(
            "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
            write={
                "spark": "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
                "bigquery": "SELECT * FROM x",
            },
        )
        self.validate_all(
            "SELECT /*+ BROADCAST(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ BROADCAST(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ BROADCASTJOIN(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ BROADCASTJOIN(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ MAPJOIN(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ MAPJOIN(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ MERGE(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ MERGE(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ SHUFFLEMERGE(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ SHUFFLEMERGE(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ MERGEJOIN(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ MERGEJOIN(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ SHUFFLE_HASH(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ SHUFFLE_HASH(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )
        self.validate_all(
            "SELECT /*+ SHUFFLE_REPLICATE_NL(table) */ cola FROM table",
            write={
                "spark": "SELECT /*+ SHUFFLE_REPLICATE_NL(table) */ cola FROM table",
                "bigquery": "SELECT cola FROM table",
            },
        )

    def test_spark(self):
        self.validate_identity("any_value(col, true)", "ANY_VALUE(col) IGNORE NULLS")
        self.validate_identity("first(col, true)", "FIRST(col) IGNORE NULLS")
        self.validate_identity("first_value(col, true)", "FIRST_VALUE(col) IGNORE NULLS")
        self.validate_identity("last(col, true)", "LAST(col) IGNORE NULLS")
        self.validate_identity("last_value(col, true)", "LAST_VALUE(col) IGNORE NULLS")

        self.assertEqual(
            parse_one("REFRESH TABLE t", read="spark").assert_is(exp.Refresh).sql(dialect="spark"),
            "REFRESH TABLE t",
        )

        self.validate_identity("DESCRIBE EXTENDED db.table")
        self.validate_identity("SELECT * FROM test TABLESAMPLE (50 PERCENT)")
        self.validate_identity("SELECT * FROM test TABLESAMPLE (5 ROWS)")
        self.validate_identity("SELECT * FROM test TABLESAMPLE (BUCKET 4 OUT OF 10)")
        self.validate_identity("REFRESH 'hdfs://path/to/table'")
        self.validate_identity("REFRESH TABLE tempDB.view1")
        self.validate_identity("SELECT CASE WHEN a = NULL THEN 1 ELSE 2 END")
        self.validate_identity("SELECT * FROM t1 SEMI JOIN t2 ON t1.x = t2.x")
        self.validate_identity("SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x + 1)")
        self.validate_identity("SELECT TRANSFORM(ARRAY(1, 2, 3), (x, i) -> x + i)")
        self.validate_identity("REFRESH TABLE a.b.c")
        self.validate_identity("INTERVAL -86 DAYS")
        self.validate_identity("SELECT UNIX_TIMESTAMP()")
        self.validate_identity("TRIM('    SparkSQL   ')")
        self.validate_identity("TRIM(BOTH 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("TRIM(LEADING 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("TRIM(TRAILING 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("SPLIT(str, pattern, lim)")
        self.validate_identity(
            "SELECT CAST('2023-01-01' AS TIMESTAMP) + INTERVAL 23 HOUR + 59 MINUTE + 59 SECONDS",
            "SELECT CAST('2023-01-01' AS TIMESTAMP) + INTERVAL '23' HOUR + INTERVAL '59' MINUTE + INTERVAL '59' SECONDS",
        )
        self.validate_identity(
            "SELECT CAST('2023-01-01' AS TIMESTAMP) + INTERVAL '23' HOUR + '59' MINUTE + '59' SECONDS",
            "SELECT CAST('2023-01-01' AS TIMESTAMP) + INTERVAL '23' HOUR + INTERVAL '59' MINUTE + INTERVAL '59' SECONDS",
        )
        self.validate_identity(
            "SELECT INTERVAL '5' HOURS '30' MINUTES '5' SECONDS '6' MILLISECONDS '7' MICROSECONDS",
            "SELECT INTERVAL '5' HOURS + INTERVAL '30' MINUTES + INTERVAL '5' SECONDS + INTERVAL '6' MILLISECONDS + INTERVAL '7' MICROSECONDS",
        )
        self.validate_identity(
            "SELECT INTERVAL 5 HOURS 30 MINUTES 5 SECONDS 6 MILLISECONDS 7 MICROSECONDS",
            "SELECT INTERVAL '5' HOURS + INTERVAL '30' MINUTES + INTERVAL '5' SECONDS + INTERVAL '6' MILLISECONDS + INTERVAL '7' MICROSECONDS",
        )
        self.validate_identity(
            "SELECT REGEXP_REPLACE('100-200', r'([^0-9])', '')",
            "SELECT REGEXP_REPLACE('100-200', '([^0-9])', '')",
        )
        self.validate_identity(
            "SELECT REGEXP_REPLACE('100-200', R'([^0-9])', '')",
            "SELECT REGEXP_REPLACE('100-200', '([^0-9])', '')",
        )
        self.validate_identity(
            "SELECT STR_TO_MAP('a:1,b:2,c:3')",
            "SELECT STR_TO_MAP('a:1,b:2,c:3', ',', ':')",
        )

        self.validate_all(
            "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
            write={
                "clickhouse": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "databricks": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "doris": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "duckdb": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl",
                "hive": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "mysql": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "postgres": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl",
                "presto": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl",
                "snowflake": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
                "spark": "WITH tbl AS (SELECT 1 AS id, 'eggy' AS name UNION ALL SELECT NULL AS id, 'jake' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl",
            },
        )
        self.validate_all(
            "SELECT TO_UTC_TIMESTAMP('2016-08-31', 'Asia/Seoul')",
            write={
                "bigquery": "SELECT DATETIME(TIMESTAMP(CAST('2016-08-31' AS DATETIME), 'Asia/Seoul'), 'UTC')",
                "duckdb": "SELECT CAST('2016-08-31' AS TIMESTAMP) AT TIME ZONE 'Asia/Seoul' AT TIME ZONE 'UTC'",
                "postgres": "SELECT CAST('2016-08-31' AS TIMESTAMP) AT TIME ZONE 'Asia/Seoul' AT TIME ZONE 'UTC'",
                "presto": "SELECT WITH_TIMEZONE(CAST('2016-08-31' AS TIMESTAMP), 'Asia/Seoul') AT TIME ZONE 'UTC'",
                "redshift": "SELECT CAST('2016-08-31' AS TIMESTAMP) AT TIME ZONE 'Asia/Seoul' AT TIME ZONE 'UTC'",
                "snowflake": "SELECT CONVERT_TIMEZONE('Asia/Seoul', 'UTC', CAST('2016-08-31' AS TIMESTAMPNTZ))",
                "spark": "SELECT TO_UTC_TIMESTAMP(CAST('2016-08-31' AS TIMESTAMP), 'Asia/Seoul')",
            },
        )
        self.validate_all(
            "SELECT FROM_UTC_TIMESTAMP('2016-08-31', 'Asia/Seoul')",
            write={
                "presto": "SELECT AT_TIMEZONE(CAST('2016-08-31' AS TIMESTAMP), 'Asia/Seoul')",
                "spark": "SELECT FROM_UTC_TIMESTAMP(CAST('2016-08-31' AS TIMESTAMP), 'Asia/Seoul')",
            },
        )
        self.validate_all(
            "foo.bar",
            read={
                "": "STRUCT_EXTRACT(foo, bar)",
            },
        )
        self.validate_all(
            "MAP(1, 2, 3, 4)",
            write={
                "spark": "MAP(1, 2, 3, 4)",
                "trino": "MAP(ARRAY[1, 3], ARRAY[2, 4])",
            },
        )
        self.validate_all(
            "MAP()",
            read={
                "spark": "MAP()",
                "trino": "MAP()",
            },
            write={
                "trino": "MAP(ARRAY[], ARRAY[])",
            },
        )
        self.validate_all(
            "SELECT STR_TO_MAP('a:1,b:2,c:3', ',', ':')",
            read={
                "presto": "SELECT SPLIT_TO_MAP('a:1,b:2,c:3', ',', ':')",
                "spark": "SELECT STR_TO_MAP('a:1,b:2,c:3', ',', ':')",
            },
            write={
                "presto": "SELECT SPLIT_TO_MAP('a:1,b:2,c:3', ',', ':')",
                "spark": "SELECT STR_TO_MAP('a:1,b:2,c:3', ',', ':')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(MONTH, CAST('1996-10-30' AS TIMESTAMP), CAST('1997-02-28 10:30:00' AS TIMESTAMP))",
            read={
                "duckdb": "SELECT DATEDIFF('month', CAST('1996-10-30' AS TIMESTAMP), CAST('1997-02-28 10:30:00' AS TIMESTAMP))",
            },
            write={
                "spark": "SELECT DATEDIFF(MONTH, TO_DATE(CAST('1996-10-30' AS TIMESTAMP)), TO_DATE(CAST('1997-02-28 10:30:00' AS TIMESTAMP)))",
                "spark2": "SELECT CAST(MONTHS_BETWEEN(TO_DATE(CAST('1997-02-28 10:30:00' AS TIMESTAMP)), TO_DATE(CAST('1996-10-30' AS TIMESTAMP))) AS INT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(week, '2020-01-01', '2020-12-31')",
            write={
                "bigquery": "SELECT DATE_DIFF(CAST('2020-12-31' AS DATE), CAST('2020-01-01' AS DATE), WEEK)",
                "duckdb": "SELECT DATE_DIFF('WEEK', CAST('2020-01-01' AS DATE), CAST('2020-12-31' AS DATE))",
                "hive": "SELECT CAST(DATEDIFF(TO_DATE('2020-12-31'), TO_DATE('2020-01-01')) / 7 AS INT)",
                "postgres": "SELECT CAST(EXTRACT(days FROM (CAST(CAST('2020-12-31' AS DATE) AS TIMESTAMP) - CAST(CAST('2020-01-01' AS DATE) AS TIMESTAMP))) / 7 AS BIGINT)",
                "redshift": "SELECT DATEDIFF(WEEK, CAST('2020-01-01' AS DATE), CAST('2020-12-31' AS DATE))",
                "snowflake": "SELECT DATEDIFF(WEEK, CAST('2020-01-01' AS DATE), CAST('2020-12-31' AS DATE))",
                "spark": "SELECT DATEDIFF(WEEK, TO_DATE('2020-01-01'), TO_DATE('2020-12-31'))",
            },
        )
        self.validate_all(
            "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30')",
            write={
                "duckdb": "SELECT DATEDIFF('month', CAST('1996-10-30' AS TIMESTAMP), CAST('1997-02-28 10:30:00' AS TIMESTAMP))",
                "hive": "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30')",
                "spark": "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30')",
            },
        )
        self.validate_all(
            "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30', FALSE)",
            write={
                "duckdb": "SELECT DATEDIFF('month', CAST('1996-10-30' AS TIMESTAMP), CAST('1997-02-28 10:30:00' AS TIMESTAMP))",
                "hive": "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30')",
                "spark": "SELECT MONTHS_BETWEEN('1997-02-28 10:30:00', '1996-10-30', FALSE)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2016-12-31 00:12:00')",
            write={
                "": "SELECT CAST('2016-12-31 00:12:00' AS TIMESTAMP)",
                "duckdb": "SELECT CAST('2016-12-31 00:12:00' AS TIMESTAMP)",
                "spark": "SELECT CAST('2016-12-31 00:12:00' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2016-12-31', 'yyyy-MM-dd')",
            read={
                "duckdb": "SELECT STRPTIME('2016-12-31', '%Y-%m-%d')",
            },
            write={
                "": "SELECT STR_TO_TIME('2016-12-31', '%Y-%m-%d')",
                "duckdb": "SELECT STRPTIME('2016-12-31', '%Y-%m-%d')",
                "spark": "SELECT TO_TIMESTAMP('2016-12-31', 'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "SELECT RLIKE('John Doe', 'John.*')",
            write={
                "bigquery": "SELECT REGEXP_CONTAINS('John Doe', 'John.*')",
                "hive": "SELECT 'John Doe' RLIKE 'John.*'",
                "postgres": "SELECT 'John Doe' ~ 'John.*'",
                "snowflake": "SELECT REGEXP_LIKE('John Doe', 'John.*')",
                "spark": "SELECT 'John Doe' RLIKE 'John.*'",
            },
        )
        self.validate_all(
            "UNHEX(MD5(x))",
            write={
                "bigquery": "FROM_HEX(TO_HEX(MD5(x)))",
                "spark": "UNHEX(MD5(x))",
            },
        )
        self.validate_all(
            "SELECT * FROM ((VALUES 1))", write={"spark": "SELECT * FROM (VALUES (1))"}
        )
        self.validate_all(
            "SELECT CAST(STRUCT('fooo') AS STRUCT<a: VARCHAR(2)>)",
            write={"spark": "SELECT CAST(STRUCT('fooo') AS STRUCT<a: STRING>)"},
        )
        self.validate_all(
            "SELECT CAST(123456 AS VARCHAR(3))",
            write={
                "": "SELECT TRY_CAST(123456 AS TEXT)",
                "databricks": "SELECT TRY_CAST(123456 AS STRING)",
                "spark": "SELECT CAST(123456 AS STRING)",
                "spark2": "SELECT CAST(123456 AS STRING)",
            },
        )
        self.validate_all(
            "SELECT TRY_CAST('a' AS INT)",
            write={
                "": "SELECT TRY_CAST('a' AS INT)",
                "databricks": "SELECT TRY_CAST('a' AS INT)",
                "spark": "SELECT TRY_CAST('a' AS INT)",
                "spark2": "SELECT CAST('a' AS INT)",
            },
        )
        self.validate_all(
            "SELECT piv.Q1 FROM (SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2'))) AS piv",
            read={
                "snowflake": "SELECT piv.Q1 FROM produce PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2')) piv",
            },
        )
        self.validate_all(
            "SELECT piv.Q1 FROM (SELECT * FROM (SELECT * FROM produce) PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2'))) AS piv",
            read={
                "snowflake": "SELECT piv.Q1 FROM (SELECT * FROM produce) PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2')) piv",
            },
        )
        self.validate_all(
            "SELECT * FROM produce PIVOT(SUM(produce.sales) FOR quarter IN ('Q1', 'Q2'))",
            read={
                "snowflake": "SELECT * FROM produce PIVOT (SUM(produce.sales) FOR produce.quarter IN ('Q1', 'Q2'))",
            },
        )
        self.validate_all(
            "SELECT * FROM produce AS p PIVOT(SUM(p.sales) AS sales FOR quarter IN ('Q1' AS Q1, 'Q2' AS Q1))",
            read={
                "bigquery": "SELECT * FROM produce AS p PIVOT(SUM(p.sales) AS sales FOR p.quarter IN ('Q1' AS Q1, 'Q2' AS Q1))",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(MONTH, '2020-01-01', '2020-03-05')",
            write={
                "databricks": "SELECT DATEDIFF(MONTH, TO_DATE('2020-01-01'), TO_DATE('2020-03-05'))",
                "hive": "SELECT CAST(MONTHS_BETWEEN(TO_DATE('2020-03-05'), TO_DATE('2020-01-01')) AS INT)",
                "presto": "SELECT DATE_DIFF('MONTH', CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE), CAST(CAST('2020-03-05' AS TIMESTAMP) AS DATE))",
                "spark": "SELECT DATEDIFF(MONTH, TO_DATE('2020-01-01'), TO_DATE('2020-03-05'))",
                "spark2": "SELECT CAST(MONTHS_BETWEEN(TO_DATE('2020-03-05'), TO_DATE('2020-01-01')) AS INT)",
                "trino": "SELECT DATE_DIFF('MONTH', CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE), CAST(CAST('2020-03-05' AS TIMESTAMP) AS DATE))",
            },
        )

        for data_type in ("BOOLEAN", "DATE", "DOUBLE", "FLOAT", "INT", "TIMESTAMP"):
            self.validate_all(
                f"{data_type}(x)",
                write={
                    "": f"CAST(x AS {data_type})",
                    "spark": f"CAST(x AS {data_type})",
                },
            )
        self.validate_all(
            "STRING(x)",
            write={
                "": "CAST(x AS TEXT)",
                "spark": "CAST(x AS STRING)",
            },
        )

        self.validate_all(
            "CAST(x AS TIMESTAMP)", read={"trino": "CAST(x AS TIMESTAMP(6) WITH TIME ZONE)"}
        )
        self.validate_all(
            "SELECT DATE_ADD(my_date_column, 1)",
            write={
                "spark": "SELECT DATE_ADD(my_date_column, 1)",
                "bigquery": "SELECT DATE_ADD(CAST(CAST(my_date_column AS DATETIME) AS DATE), INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "AGGREGATE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
            write={
                "trino": "REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
                "duckdb": "REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
                "hive": "REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
                "presto": "REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
                "spark": "AGGREGATE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)",
            },
        )
        self.validate_all(
            "TRIM('SL', 'SSparkSQLS')", write={"spark": "TRIM('SL' FROM 'SSparkSQLS')"}
        )
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
            "ARRAY(0, 1, 2)",
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "[0, 1, 2]",
                "presto": "ARRAY[0, 1, 2]",
                "hive": "ARRAY(0, 1, 2)",
                "spark": "ARRAY(0, 1, 2)",
            },
        )

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "clickhouse": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname NULLS FIRST",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "snowflake": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname NULLS FIRST",
            },
        )
        self.validate_all(
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            write={
                "duckdb": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "presto": "SELECT APPROX_DISTINCT(a) FROM foo",
                "hive": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "spark": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            },
        )
        self.validate_all(
            "MONTH('2021-03-01')",
            write={
                "duckdb": "MONTH(CAST('2021-03-01' AS DATE))",
                "presto": "MONTH(CAST(CAST('2021-03-01' AS TIMESTAMP) AS DATE))",
                "hive": "MONTH(TO_DATE('2021-03-01'))",
                "spark": "MONTH(TO_DATE('2021-03-01'))",
            },
        )
        self.validate_all(
            "YEAR('2021-03-01')",
            write={
                "duckdb": "YEAR(CAST('2021-03-01' AS DATE))",
                "presto": "YEAR(CAST(CAST('2021-03-01' AS TIMESTAMP) AS DATE))",
                "hive": "YEAR(TO_DATE('2021-03-01'))",
                "spark": "YEAR(TO_DATE('2021-03-01'))",
            },
        )
        self.validate_all(
            "'\u6bdb'",
            write={
                "duckdb": "'毛'",
                "presto": "'毛'",
                "hive": "'毛'",
                "spark": "'毛'",
            },
        )
        self.validate_all(
            "SELECT LEFT(x, 2), RIGHT(x, 2)",
            write={
                "duckdb": "SELECT LEFT(x, 2), RIGHT(x, 2)",
                "presto": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - (2 - 1))",
                "hive": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - (2 - 1))",
                "spark": "SELECT LEFT(x, 2), RIGHT(x, 2)",
            },
        )
        self.validate_all(
            "MAP_FROM_ARRAYS(ARRAY(1), c)",
            write={
                "duckdb": "MAP([1], c)",
                "presto": "MAP(ARRAY[1], c)",
                "hive": "MAP(ARRAY(1), c)",
                "spark": "MAP_FROM_ARRAYS(ARRAY(1), c)",
                "snowflake": "OBJECT_CONSTRUCT([1], c)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SORT(x)",
            write={
                "duckdb": "SELECT ARRAY_SORT(x)",
                "presto": "SELECT ARRAY_SORT(x)",
                "hive": "SELECT SORT_ARRAY(x)",
                "spark": "SELECT ARRAY_SORT(x)",
            },
        )

    def test_bool_or(self):
        self.validate_all(
            "SELECT a, LOGICAL_OR(b) FROM table GROUP BY a",
            write={"spark": "SELECT a, BOOL_OR(b) FROM table GROUP BY a"},
        )

    def test_current_user(self):
        self.validate_all(
            "CURRENT_USER",
            write={"spark": "CURRENT_USER()"},
        )
        self.validate_all(
            "CURRENT_USER()",
            write={"spark": "CURRENT_USER()"},
        )

    def test_transform_query(self):
        self.validate_identity("SELECT TRANSFORM(x) USING 'x' AS (x INT) FROM t")
        self.validate_identity(
            "SELECT TRANSFORM(zip_code, name, age) USING 'cat' AS (a, b, c) FROM person WHERE zip_code > 94511"
        )
        self.validate_identity(
            "SELECT TRANSFORM(zip_code, name, age) USING 'cat' AS (a STRING, b STRING, c STRING) FROM person WHERE zip_code > 94511"
        )
        self.validate_identity(
            "SELECT TRANSFORM(name, age) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' NULL DEFINED AS 'NULL' USING 'cat' AS (name_age STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '@' LINES TERMINATED BY '\n' NULL DEFINED AS 'NULL' FROM person"
        )
        self.validate_identity(
            "SELECT TRANSFORM(zip_code, name, age) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('field.delim'='\t') USING 'cat' AS (a STRING, b STRING, c STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('field.delim'='\t') FROM person WHERE zip_code > 94511"
        )
        self.validate_identity(
            "SELECT TRANSFORM(zip_code, name, age) USING 'cat' FROM person WHERE zip_code > 94500"
        )

    def test_insert_cte(self):
        self.validate_all(
            "INSERT OVERWRITE TABLE table WITH cte AS (SELECT cola FROM other_table) SELECT cola FROM cte",
            write={
                "databricks": "WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte",
                "hive": "WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte",
                "spark": "WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte",
                "spark2": "WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte",
            },
        )

    def test_explode_to_unnest(self):
        self.validate_all(
            "SELECT EXPLODE(x) FROM tbl",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) AS col FROM tbl CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1)) AS pos CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(x) - 1) AND pos_2 = (ARRAY_LENGTH(x) - 1))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(x)))) AS _u(pos) CROSS JOIN UNNEST(x) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(x) AND _u_2.pos_2 = CARDINALITY(x))",
                "spark": "SELECT EXPLODE(x) FROM tbl",
            },
        )
        self.validate_all(
            "SELECT EXPLODE(col) FROM _u",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col_2, NULL) AS col_2 FROM _u CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(col)) - 1)) AS pos CROSS JOIN UNNEST(col) AS col_2 WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(col) - 1) AND pos_2 = (ARRAY_LENGTH(col) - 1))",
                "presto": "SELECT IF(_u_2.pos = _u_3.pos_2, _u_3.col_2) AS col_2 FROM _u CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(col)))) AS _u_2(pos) CROSS JOIN UNNEST(col) WITH ORDINALITY AS _u_3(col_2, pos_2) WHERE _u_2.pos = _u_3.pos_2 OR (_u_2.pos > CARDINALITY(col) AND _u_3.pos_2 = CARDINALITY(col))",
                "spark": "SELECT EXPLODE(col) FROM _u",
            },
        )
        self.validate_all(
            "SELECT EXPLODE(col) AS exploded FROM schema.tbl",
            write={
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.exploded) AS exploded FROM schema.tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(col)))) AS _u(pos) CROSS JOIN UNNEST(col) WITH ORDINALITY AS _u_2(exploded, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(col) AND _u_2.pos_2 = CARDINALITY(col))",
            },
        )
        self.validate_all(
            "SELECT EXPLODE(ARRAY(1, 2))",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) AS col FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([1, 2])) - 1)) AS pos CROSS JOIN UNNEST([1, 2]) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH([1, 2]) - 1) AND pos_2 = (ARRAY_LENGTH([1, 2]) - 1))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1, 2])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1, 2]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1, 2]))",
            },
        )
        self.validate_all(
            "SELECT POSEXPLODE(ARRAY(2, 3)) AS x",
            write={
                "bigquery": "SELECT IF(pos = pos_2, x, NULL) AS x, IF(pos = pos_2, pos_2, NULL) AS pos_2 FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([2, 3])) - 1)) AS pos CROSS JOIN UNNEST([2, 3]) AS x WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH([2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([2, 3]) - 1))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.x) AS x, IF(_u.pos = _u_2.pos_2, _u_2.pos_2) AS pos_2 FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_2(x, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[2, 3]))",
            },
        )
        self.validate_all(
            "SELECT POSEXPLODE(x) AS (a, b)",
            write={
                "presto": "SELECT IF(_u.pos = _u_2.a, _u_2.b) AS b, IF(_u.pos = _u_2.a, _u_2.a) AS a FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(x)))) AS _u(pos) CROSS JOIN UNNEST(x) WITH ORDINALITY AS _u_2(b, a) WHERE _u.pos = _u_2.a OR (_u.pos > CARDINALITY(x) AND _u_2.a = CARDINALITY(x))",
            },
        )
        self.validate_all(
            "SELECT POSEXPLODE(ARRAY(2, 3)), EXPLODE(ARRAY(4, 5, 6)) FROM tbl",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_2, pos_2, NULL) AS pos_2, IF(pos = pos_3, col_2, NULL) AS col_2 FROM tbl CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([2, 3]), ARRAY_LENGTH([4, 5, 6])) - 1)) AS pos CROSS JOIN UNNEST([2, 3]) AS col WITH OFFSET AS pos_2 CROSS JOIN UNNEST([4, 5, 6]) AS col_2 WITH OFFSET AS pos_3 WHERE (pos = pos_2 OR (pos > (ARRAY_LENGTH([2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([2, 3]) - 1))) AND (pos = pos_3 OR (pos > (ARRAY_LENGTH([4, 5, 6]) - 1) AND pos_3 = (ARRAY_LENGTH([4, 5, 6]) - 1)))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col, IF(_u.pos = _u_2.pos_2, _u_2.pos_2) AS pos_2, IF(_u.pos = _u_3.pos_3, _u_3.col_2) AS col_2 FROM tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3]), CARDINALITY(ARRAY[4, 5, 6])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_2(col, pos_2) CROSS JOIN UNNEST(ARRAY[4, 5, 6]) WITH ORDINALITY AS _u_3(col_2, pos_3) WHERE (_u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[2, 3]))) AND (_u.pos = _u_3.pos_3 OR (_u.pos > CARDINALITY(ARRAY[4, 5, 6]) AND _u_3.pos_3 = CARDINALITY(ARRAY[4, 5, 6])))",
            },
        )
        self.validate_all(
            "SELECT col, pos, POSEXPLODE(ARRAY(2, 3)) FROM _u",
            write={
                "presto": "SELECT col, pos, IF(_u_2.pos_2 = _u_3.pos_3, _u_3.col_2) AS col_2, IF(_u_2.pos_2 = _u_3.pos_3, _u_3.pos_3) AS pos_3 FROM _u CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3])))) AS _u_2(pos_2) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_3(col_2, pos_3) WHERE _u_2.pos_2 = _u_3.pos_3 OR (_u_2.pos_2 > CARDINALITY(ARRAY[2, 3]) AND _u_3.pos_3 = CARDINALITY(ARRAY[2, 3]))",
            },
        )

    def test_strip_modifiers(self):
        without_modifiers = "SELECT * FROM t"
        with_modifiers = f"{without_modifiers} CLUSTER BY y DISTRIBUTE BY x SORT BY z"
        query = self.parse_one(with_modifiers)

        for dialect in Dialects:
            with self.subTest(f"Transpiling query with CLUSTER/DISTRIBUTE/SORT BY to {dialect}"):
                name = dialect.value
                if name in ("", "databricks", "hive", "spark", "spark2"):
                    self.assertEqual(query.sql(name), with_modifiers)
                else:
                    self.assertEqual(query.sql(name), without_modifiers)
