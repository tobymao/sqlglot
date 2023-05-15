from tests.dialects.test_dialect import Validator


class TestSpark(Validator):
    dialect = "spark"

    def test_ddl(self):
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
                "presto": "CREATE TABLE x WITH (TABLE_FORMAT='ICEBERG', PARTITIONED_BY=ARRAY['MONTHS'])",
                "hive": "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
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
COMMENT='Test comment: blah'
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
  col_a INT
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date STRING
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
                "presto": "CAST(SUBSTR(CAST(x AS VARCHAR), 1, 10) AS DATE)",
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

    def test_hint(self):
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
        self.validate_identity("SELECT UNIX_TIMESTAMP()")
        self.validate_identity("TRIM('    SparkSQL   ')")
        self.validate_identity("TRIM(BOTH 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("TRIM(LEADING 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("TRIM(TRAILING 'SL' FROM 'SSparkSQLS')")
        self.validate_identity("SPLIT(str, pattern, lim)")

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
                "hive": "SELECT MONTHS_BETWEEN(TO_DATE('2020-03-05'), TO_DATE('2020-01-01'))",
                "presto": "SELECT DATE_DIFF('MONTH', CAST(SUBSTR(CAST('2020-01-01' AS VARCHAR), 1, 10) AS DATE), CAST(SUBSTR(CAST('2020-03-05' AS VARCHAR), 1, 10) AS DATE))",
                "spark": "SELECT DATEDIFF(MONTH, TO_DATE('2020-01-01'), TO_DATE('2020-03-05'))",
                "spark2": "SELECT MONTHS_BETWEEN(TO_DATE('2020-03-05'), TO_DATE('2020-01-01'))",
                "trino": "SELECT DATE_DIFF('MONTH', CAST(SUBSTR(CAST('2020-01-01' AS VARCHAR), 1, 10) AS DATE), CAST(SUBSTR(CAST('2020-03-05' AS VARCHAR), 1, 10) AS DATE))",
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
                "bigquery": "SELECT DATE_ADD(my_date_column, INTERVAL 1 DAY)",
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
                "duckdb": "LIST_VALUE(0, 1, 2)",
                "presto": "ARRAY[0, 1, 2]",
                "hive": "ARRAY(0, 1, 2)",
                "spark": "ARRAY(0, 1, 2)",
            },
        )

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "clickhouse": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname NULLS FIRST",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "snowflake": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname NULLS FIRST",
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
                "presto": "MONTH(CAST(SUBSTR(CAST('2021-03-01' AS VARCHAR), 1, 10) AS DATE))",
                "hive": "MONTH(TO_DATE('2021-03-01'))",
                "spark": "MONTH(TO_DATE('2021-03-01'))",
            },
        )
        self.validate_all(
            "YEAR('2021-03-01')",
            write={
                "duckdb": "YEAR(CAST('2021-03-01' AS DATE))",
                "presto": "YEAR(CAST(SUBSTR(CAST('2021-03-01' AS VARCHAR), 1, 10) AS DATE))",
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
                "duckdb": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
                "presto": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
                "hive": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
                "spark": "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
            },
        )
        self.validate_all(
            "MAP_FROM_ARRAYS(ARRAY(1), c)",
            write={
                "duckdb": "MAP(LIST_VALUE(1), c)",
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

    def test_iif(self):
        self.validate_all(
            "SELECT IIF(cond, 'True', 'False')",
            write={"spark": "SELECT IF(cond, 'True', 'False')"},
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
