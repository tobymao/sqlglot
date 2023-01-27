from sqlglot import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestPresto(Validator):
    dialect = "presto"

    def test_cast(self):
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
                "duckdb": "CAST(LIST_VALUE(1, 2) AS BIGINT[])",
                "presto": "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
                "spark": "CAST(ARRAY(1, 2) AS ARRAY<LONG>)",
                "snowflake": "CAST([1, 2] AS ARRAY)",
            },
        )
        self.validate_all(
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INT,INT))",
            write={
                "bigquery": "CAST(MAP([1], [1]) AS MAP<INT64, INT64>)",
                "duckdb": "CAST(MAP(LIST_VALUE(1), LIST_VALUE(1)) AS MAP(INT, INT))",
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
                "duckdb": "CAST(MAP(LIST_VALUE('a', 'b', 'c'), LIST_VALUE(LIST_VALUE(1), LIST_VALUE(2), LIST_VALUE(3))) AS MAP(TEXT, INT[]))",
                "presto": "CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INTEGER)))",
                "hive": "CAST(MAP('a', ARRAY(1), 'b', ARRAY(2), 'c', ARRAY(3)) AS MAP<STRING, ARRAY<INT>>)",
                "spark": "CAST(MAP_FROM_ARRAYS(ARRAY('a', 'b', 'c'), ARRAY(ARRAY(1), ARRAY(2), ARRAY(3))) AS MAP<STRING, ARRAY<INT>>)",
                "snowflake": "CAST(OBJECT_CONSTRUCT('a', [1], 'b', [2], 'c', [3]) AS OBJECT)",
            },
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
            write={
                "bigquery": "CAST(x AS TIMESTAMPTZ)",
                "duckdb": "CAST(x AS TIMESTAMPTZ(9))",
                "presto": "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
                "hive": "CAST(x AS TIMESTAMPTZ)",
                "spark": "CAST(x AS TIMESTAMPTZ)",
            },
        )

    def test_regex(self):
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

    def test_time(self):
        self.validate_all(
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
            write={
                "duckdb": "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
                "spark": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%S')",
            write={
                "duckdb": "STRPTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_PARSE(x, '%Y-%m-%d %H:%i:%S')",
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
                "duckdb": "TO_TIMESTAMP(CAST(x AS BIGINT))",
                "presto": "FROM_UNIXTIME(x)",
                "hive": "FROM_UNIXTIME(x)",
                "spark": "FROM_UNIXTIME(x)",
            },
        )
        self.validate_identity("FROM_UNIXTIME(a, b)")
        self.validate_identity("FROM_UNIXTIME(a, b, c)")
        self.validate_identity("TRIM(a, b)")
        self.validate_identity("VAR_POP(a)")
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
            "CREATE TABLE test STORED = 'PARQUET' AS SELECT 1",
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
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
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

    def test_presto(self):
        self.validate_identity("SELECT BOOL_OR(a > 10) FROM asd AS T(a)")
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
                "duckdb": "SELECT LIST_VALUE(1, 2)",
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
        self.validate_identity("SELECT * FROM (VALUES (1))")
        self.validate_identity("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE")
        self.validate_identity("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self.validate_identity("APPROX_PERCENTILE(a, b, c, d)")

    def test_encode_decode(self):
        self.validate_all(
            "TO_UTF8(x)",
            write={
                "spark": "ENCODE(x, 'utf-8')",
            },
        )
        self.validate_all(
            "FROM_UTF8(x)",
            write={
                "spark": "DECODE(x, 'utf-8')",
            },
        )
        self.validate_all(
            "FROM_UTF8(x, y)",
            write={
                "presto": "FROM_UTF8(x, y)",
            },
        )
        self.validate_all(
            "ENCODE(x, 'utf-8')",
            write={
                "presto": "TO_UTF8(x)",
            },
        )
        self.validate_all(
            "DECODE(x, 'utf-8')",
            write={
                "presto": "FROM_UTF8(x)",
            },
        )
        self.validate_all(
            "ENCODE(x, 'invalid')",
            write={
                "presto": UnsupportedError,
            },
        )
        self.validate_all(
            "DECODE(x, 'invalid')",
            write={
                "presto": UnsupportedError,
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
