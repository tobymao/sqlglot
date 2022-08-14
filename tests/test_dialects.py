import unittest

from sqlglot import (
    Dialect,
    Dialects,
    ErrorLevel,
    ParseError,
    UnsupportedError,
    transpile,
)


class TestDialects(unittest.TestCase):
    maxDiff = None

    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

    def test_enum(self):
        for dialect in Dialects:
            self.assertIsNotNone(Dialect[dialect])
            self.assertIsNotNone(Dialect.get(dialect))
            self.assertIsNotNone(Dialect.get_or_raise(dialect))
            self.assertIsNotNone(Dialect[dialect.value])

    def test_duckdb(self):
        self.validate(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            "STRPTIME('2020-01-01', '%Y-%m-%d')",
            read="duckdb",
            write="duckdb",
        )
        self.validate("EPOCH(x)", "EPOCH(x)", read="duckdb")
        self.validate("EPOCH(x)", "TO_UNIXTIME(x)", read="duckdb", write="presto")
        self.validate(
            "EPOCH_MS(x)", "FROM_UNIXTIME(x / 1000)", read="duckdb", write="presto"
        )
        self.validate(
            "STRFTIME(x, '%y-%-m-%S')",
            "DATE_FORMAT(x, '%y-%c-%S')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STRPTIME(x, '%y-%-m')",
            "DATE_PARSE(x, '%y-%c')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "LIST_VALUE(0, 1, 2)", "ARRAY[0, 1, 2]", read="duckdb", write="presto"
        )
        self.validate("Array(1, 2)", "LIST_VALUE(1, 2)", write="duckdb")

        self.validate("REGEXP_MATCHES(x, y)", "REGEXP_MATCHES(x, y)", read="duckdb")
        self.validate("REGEXP_MATCHES(x, y)", "x RLIKE y", read="duckdb", write="hive")
        self.validate(
            "REGEXP_MATCHES('abc', 'abc')",
            "REGEXP_LIKE('abc', 'abc')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "REGEXP_MATCHES('abc', '(b|c).*')",
            "REGEXP_LIKE('abc', '(b|c).*')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "REGEXP_MATCHES(x, 'abc')",
            "REGEXP_LIKE(x, 'abc')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STR_SPLIT(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STRING_SPLIT(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STRING_TO_ARRAY(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STR_SPLIT_REGEX(x, 'a')",
            "REGEXP_SPLIT(x, 'a')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STRING_SPLIT_REGEX(x, 'a')",
            "REGEXP_SPLIT(x, 'a')",
            read="duckdb",
            write="presto",
        )

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", "STRUCT_EXTRACT(x, 'abc')", read="duckdb"
        )

        self.validate(
            "QUANTILE(x, 0.5)",
            "APPROX_PERCENTILE(x, 0.5)",
            read="duckdb",
            write="presto",
            unsupported_level=ErrorLevel.IGNORE,
        )
        self.validate(
            "QUANTILE(x, 0.5)", "PERCENTILE(x, 0.5)", read="duckdb", write="spark"
        )
        self.validate(
            "PERCENTILE(x, 0.5)", "QUANTILE(x, 0.5)", read="hive", write="duckdb"
        )

        self.validate("MONTH(x)", "MONTH(x)", write="duckdb", identity=False)
        self.validate("YEAR(x)", "YEAR(x)", write="duckdb", identity=False)
        self.validate("DAY(x)", "DAY(x)", write="duckdb", identity=False)

        self.validate(
            "DATEDIFF(a, b)",
            "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
            read="hive",
            write="duckdb",
        )
        self.validate(
            "STR_TO_UNIX('2020-01-01', '%Y-%M-%d')",
            "EPOCH(STRPTIME('2020-01-01', '%Y-%M-%d'))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_DATE('2020-01-01')",
            "CAST('2020-01-01' AS DATE)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_TIME('2020-01-01')",
            "CAST('2020-01-01' AS TIMESTAMP)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_UNIX('2020-01-01')",
            "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            "STRFTIME(x, '%Y-%m-%d')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_UNIX(x)",
            "EPOCH(x)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TS_OR_DS_TO_DATE_STR(x)",
            "STRFTIME(CAST(x AS DATE), '%Y-%m-%d')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_STR(x, y)",
            "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), y)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME(x)",
            "TO_TIMESTAMP(CAST(x AS BIGINT))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME_STR(x)",
            "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), '%Y-%m-%d %H:%M:%S')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            read="duckdb",
            write="hive",
        )
        self.validate(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "TO_TIMESTAMP(x)",
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "TS_OR_DS_TO_DATE(x)",
            "CAST(x AS DATE)",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "CAST(x AS DATE)",
            "CAST(x AS DATE)",
            read="duckdb",
            identity=False,
        )

        self.validate(
            "UNNEST(x)",
            "EXPLODE(x)",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "EXPLODE(x)",
            "UNNEST(x)",
            read="spark",
            write="duckdb",
        )

        self.validate("1d", "1 AS d", read="duckdb")
        self.validate("1d", "CAST(1 AS DOUBLE)", read="spark", write="duckdb")
        self.validate(
            "POW(2S, 3)", "POW(CAST(2 AS SMALLINT), 3)", read="spark", write="duckdb"
        )

        self.validate(
            "DATE_TO_DATE_STR(x)",
            "STRFTIME(x, '%Y-%m-%d')",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DATE_TO_DATE_STR(x)",
            "DATE_FORMAT(x, 'yyyy-MM-dd')",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "CAST(STRPTIME(CAST(x AS STRING), '%Y%m%d') AS DATE)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "TO_DATE(CAST(x AS STRING), 'yyyyMMdd')",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "ARRAY_SUM(ARRAY(1, 2))",
            "LIST_SUM(LIST_VALUE(1, 2))",
            read="spark",
            write="duckdb",
        )

        self.validate(
            "SAFE_DIVIDE(x, y)",
            "IF(y <> 0, x / y, NULL)",
            read="bigquery",
            write="duckdb",
        )

        self.validate(
            "STRUCT_PACK(x := 1, y := '2')",
            "STRUCT_PACK(x := 1, y := '2')",
            read="duckdb",
        )
        self.validate(
            "STRUCT_PACK(x := 1, y := '2')",
            "STRUCT(x = 1, y = '2')",
            read="duckdb",
            write="spark",
        )

    def test_mysql(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            read="mysql",
            write="mysql",
        )

        self.validate(
            "x ILIKE '%y'",
            "LOWER(x) LIKE '%y'",
            read="postgres",
            write="mysql",
        )

    def test_starrocks(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            read="starrocks",
            write="starrocks",
        )

        self.validate(
            "SELECT CAST(`a` AS TEXT), CAST(`b` AS TIMESTAMP), CAST(`c` AS TIMESTAMPTZ) FROM foo",
            "SELECT CAST(`a` AS STRING), CAST(`b` AS DATETIME), CAST(`c` AS DATETIME) FROM foo",
            read="hive",
            write="starrocks",
        )

    def test_bigquery(self):
        self.validate(
            '"""x"""',
            "'x'",
            read="bigquery",
            write="presto",
        )
        self.validate(
            '"""x\'"""',
            "'x'''",
            read="bigquery",
            write="presto",
        )
        self.validate(
            r'r"""/\*.*\*/"""',
            r"'/\*.*\*/'",
            read="bigquery",
            write="presto",
        )
        self.validate(
            r'r"/\*.*\*/"',
            r"'/\\*.*\\*/'",
            read="bigquery",
            write="hive",
        )

        self.validate(
            "SELECT CAST(a AS INT) FROM foo",
            "SELECT CAST(a AS INT64) FROM foo",
            write="bigquery",
        )
        self.validate(
            "SELECT CAST(a AS INT64) FROM foo",
            "SELECT CAST(a AS BIGINT) FROM foo",
            read="bigquery",
            write="duckdb",
        )
        self.validate(
            "SELECT CAST(a AS DECIMAL) FROM foo",
            "SELECT CAST(a AS NUMERIC) FROM foo",
            write="bigquery",
        )
        self.validate(
            'SELECT CAST("a" AS DOUBLE) FROM foo',
            "SELECT CAST(`a` AS FLOAT64) FROM foo",
            write="bigquery",
        )

        self.validate(
            "[1, 2, 3]",
            "[1, 2, 3]",
            write="bigquery",
        )
        self.validate(
            "SELECT ARRAY(1, 2, 3) AS y FROM foo",
            "SELECT [1, 2, 3] AS y FROM foo",
            read="spark",
            write="bigquery",
        )
        self.validate(
            "SELECT [1, 2, 3] AS y FROM foo",
            "SELECT ARRAY(1, 2, 3) AS y FROM foo",
            read="bigquery",
            write="spark",
        )
        self.validate(
            "SELECT * FROM UNNEST(['7', '14']) AS x",
            "SELECT * FROM UNNEST(ARRAY['7', '14']) AS (x)",
            read="bigquery",
            write="presto",
        )
        self.validate(
            "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x",
            "SELECT * FROM UNNEST(['7', '14'])",
            read="presto",
            write="bigquery",
        )
        self.validate(
            "SELECT * FROM UNNEST(ARRAY['7', '14']) AS x(y)",
            "SELECT * FROM UNNEST(['7', '14']) AS y",
            read="presto",
            write="bigquery",
        )

        with self.assertRaises(ParseError):
            transpile("SELECT * FROM UNNEST(x) AS x(y)", read="bigquery")

        self.validate(
            "x IS unknown",
            "x IS NULL",
            read="bigquery",
            write="duckdb",
        )
        self.validate(
            "current_datetime",
            "CURRENT_DATETIME()",
            read="bigquery",
        )

        self.validate(
            "current_time",
            "CURRENT_TIME()",
            read="bigquery",
        )

        self.validate(
            "current_timestamp",
            "CURRENT_TIMESTAMP()",
            read="bigquery",
        )

        self.validate(
            "SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)",
            "SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)",
            read="bigquery",
        )

        self.validate(
            "SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)",
            "SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)",
            read="bigquery",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)",
            read="spark",
            write="bigquery",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            read="spark",
            write="bigquery",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a int64, struct_col_b string>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)",
            read="bigquery",
            write="bigquery",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            read="bigquery",
            write="bigquery",
        )

    def test_postgres(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS DOUBLE) FROM foo",
            "SELECT CAST(`a`.`b` AS DOUBLE PRECISION) FROM foo",
            read="postgres",
            write="postgres",
        )
        self.validate(
            "CREATE TABLE x (a BYTEA)",
            "CREATE TABLE x (a BINARY)",
            read="postgres",
            write="hive",
        )

        self.validate(
            "CREATE TABLE x (a UUID)",
            "CREATE TABLE x (a UUID)",
            read="postgres",
            write="hive",
        )

        self.validate(
            "CREATE TABLE x (a INT SERIAL)",
            "CREATE TABLE x (a INTEGER AUTOINCREMENT)",
            read="postgres",
            write="sqlite",
        )
        self.validate(
            "CREATE TABLE x (a INTEGER AUTOINCREMENT)",
            "CREATE TABLE x (a INT SERIAL)",
            read="sqlite",
            write="postgres",
        )

        self.validate(
            "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            "CURRENT_DATE - INTERVAL '1' DAY",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "DATE_ADD(CURRENT_DATE(), INTERVAL 1 + 3 DAY)",
            "CURRENT_DATE + INTERVAL '4' DAY",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "CURRENT_DATE('UTC')",
            "CURRENT_DATE AT TIME ZONE 'UTC'",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "CURRENT_TIMESTAMP()",
            "CURRENT_TIMESTAMP",
            read="bigquery",
            write="postgres",
        )

        for read, write in [(None, "postgres")]:
            for a, b in [
                ("JSON_EXTRACT(x, 'y')", "x->'y'"),
                ("JSON_EXTRACT_SCALAR(x, 'y')", "x->>'y'"),
                ("JSONB_EXTRACT(x, 'y')", "x#>'y'"),
                ("JSONB_EXTRACT_SCALAR(x, 'y')", "x#>>'y'"),
            ]:
                self.validate(a, b, read=read, write=write, identity=False)
                self.validate(b, a, read=write, write=read, identity=False)

        self.validate(
            "x->'1'",
            "x->'1'",
            read="postgres",
            write="sqlite",
        )
        self.validate(
            "x#>'1'",
            "x->'1'",
            read="postgres",
            write="sqlite",
        )

        self.validate(
            "STRFTIME(x, '%y-%-m-%S')",
            "TO_CHAR(x, 'YY-FMMM-SS')",
            read="duckdb",
            write="postgres",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "DATE_ADD(x, y, 'day')",
                write="postgres",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT * FROM x FETCH 1 ROW",
            "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
            read="postgres",
        )

    def test_presto(self):
        self.validate(
            'SELECT "a"."b" FROM foo',
            'SELECT "a"."b" FROM "foo"',
            read="presto",
            write="presto",
            identify=True,
        )
        self.validate(
            "SELECT a.b FROM foo", "SELECT a.b FROM foo", read="presto", write="spark"
        )
        self.validate(
            'SELECT "a"."b" FROM foo',
            "SELECT `a`.`b` FROM `foo`",
            read="presto",
            write="spark",
            identify=True,
        )
        self.validate(
            "SELECT a.b FROM foo",
            "SELECT `a`.`b` FROM `foo`",
            read="presto",
            write="spark",
            identify=True,
        )
        self.validate(
            "SELECT ARRAY[1, 2]", "SELECT ARRAY(1, 2)", read="presto", write="spark"
        )
        self.validate(
            "CAST(a AS ARRAY(INT))",
            "CAST(a AS ARRAY(INTEGER))",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
            "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INT,INT))",
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INTEGER, INTEGER))",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(MAP(ARRAY['a','b','c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INT)))",
            "CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INTEGER)))",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
            "CAST(x AS TIMESTAMP(9) WITH TIME ZONE)",
            read="presto",
            write="presto",
        )
        self.validate("CAST(a AS TEXT)", "CAST(a AS VARCHAR)", write="presto")
        self.validate("CAST(a AS STRING)", "CAST(a AS VARCHAR)", write="presto")
        self.validate(
            "CAST(a AS VARCHAR)", "CAST(a AS STRING)", read="presto", write="spark"
        )

        self.validate("x & 1", "BITWISE_AND(x, 1)", read="hive", write="presto")
        self.validate("~x", "BITWISE_NOT(x)", read="hive", write="presto")
        self.validate("x | 1", "BITWISE_OR(x, 1)", read="hive", write="presto")
        self.validate(
            "x << 1", "BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)", read="hive", write="presto"
        )
        self.validate(
            "x >> 1",
            "BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
            read="hive",
            write="presto",
        )
        self.validate("x & 1 > 0", "BITWISE_AND(x, 1) > 0", read="hive", write="presto")

        self.validate("REGEXP_LIKE(a, 'x')", "a RLIKE 'x'", read="presto", write="hive")
        self.validate("a RLIKE 'x'", "REGEXP_LIKE(a, 'x')", read="hive", write="presto")
        self.validate(
            "a REGEXP 'x'", "REGEXP_LIKE(a, 'x')", read="hive", write="presto"
        )

        self.validate(
            "SPLIT(x, 'a.')",
            "SPLIT(x, CONCAT('\\\\Q', 'a.'))",
            read="presto",
            write="hive",
        )
        self.validate(
            "REGEXP_SPLIT(x, 'a.')", "SPLIT(x, 'a.')", read="presto", write="hive"
        )

        self.validate(
            "CASE WHEN x > 1 THEN 1 WHEN x > 2 THEN 2 END",
            "CASE WHEN x > 1 THEN 1 WHEN x > 2 THEN 2 END",
            write="presto",
        )

        self.validate(
            "ARRAY_CONTAINS(x, 1)", "CONTAINS(x, 1)", read="hive", write="presto"
        )
        self.validate("SIZE(x)", "CARDINALITY(x)", read="hive", write="presto")
        self.validate("SIZE(x)", "ARRAY_LENGTH(x)", read="hive", write="duckdb")
        self.validate("CARDINALITY(x)", "SIZE(x)", read="presto", write="hive")
        self.validate("ARRAY_SIZE(x)", "CARDINALITY(x)", write="presto", identity=False)
        self.validate(
            "ARRAY_SIZE(x)", "ARRAY_LENGTH(x)", write="duckdb", identity=False
        )

        self.validate(
            "PERCENTILE(x, 0.5)",
            "APPROX_PERCENTILE(x, 0.5)",
            read="hive",
            write="presto",
            unsupported_level=ErrorLevel.IGNORE,
        )

        self.validate(
            "STR_POSITION(x, 'a')", "STRPOS(x, 'a')", write="presto", identity=False
        )
        self.validate(
            "STR_POSITION(x, 'a')", "LOCATE('a', x)", read="presto", write="hive"
        )
        self.validate("LOCATE('a', x)", "STRPOS(x, 'a')", read="hive", write="presto")
        self.validate(
            "LOCATE('a', x, 3)",
            "STRPOS(SUBSTR(x, 3), 'a') + 3 - 1",
            read="hive",
            write="presto",
        )

        self.validate(
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%s')",
            "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            read="presto",
            write="hive",
        )
        self.validate(
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            "FROM_UNIXTIME(UNIX_TIMESTAMP(x))",
            read="presto",
            write="hive",
        )
        self.validate(
            "DATE_PARSE(x, '%Y-%m-%d')",
            "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-dd'))",
            read="presto",
            write="hive",
        )
        self.validate(
            "TIME_STR_TO_UNIX(x)",
            "TO_UNIXTIME(DATE_PARSE(x, '%Y-%m-%d %H:%i:%S'))",
            write="presto",
        )
        self.validate(
            "TIME_STR_TO_TIME(x)",
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            write="presto",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
            write="presto",
        )
        self.validate(
            "UNIX_TO_TIME_STR(x)",
            "DATE_FORMAT(FROM_UNIXTIME(x), '%Y-%m-%d %H:%i:%S')",
            write="presto",
        )
        self.validate(
            "FROM_UNIXTIME(x)",
            "FROM_UNIXTIME(x)",
            read="presto",
            write="hive",
        )
        self.validate(
            "TO_UNIXTIME(x)",
            "UNIX_TIMESTAMP(x)",
            read="presto",
            write="hive",
        )
        self.validate(
            "DATE_ADD('day', 1, x)",
            "DATE_ADD(x, 1)",
            read="presto",
            write="hive",
        )
        self.validate(
            "DATE_DIFF('day', a, b)",
            "DATEDIFF(b, a)",
            read="presto",
            write="hive",
        )
        self.validate(
            "DATE_DIFF(a, b)",
            "DATE_DIFF('day', b, a)",
            write="presto",
            identity=False,
        )
        self.validate(
            "DATE_DIFF(a, b)",
            "DATE_DIFF('day', b, a)",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "DATE_DIFF('month', b, a)",
            "DATE_DIFF('month', b, a)",
            read="presto",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "TS_OR_DS_TO_DATE(x)",
            "CAST(DATE_PARSE(SUBSTR(CAST(x AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE)",
            write="presto",
            identity=False,
        )
        self.validate(
            "DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d')",
            "STR_TO_TIME(SUBSTR(x, 1, 10), '%Y-%m-%d')",
            read="presto",
            identity=False,
        )

        self.validate(
            "SELECT APPROX_DISTINCT(a) FROM foo",
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            read="presto",
            write="spark",
        )

        sql = transpile(
            "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
            read="presto",
            write="spark",
            unsupported_level=ErrorLevel.IGNORE,
        )[0]
        self.assertEqual(sql, "SELECT APPROX_COUNT_DISTINCT(a) FROM foo")

        ctas = "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1"
        self.assertEqual(transpile(ctas, read="presto", write="presto")[0], ctas)

        sql = transpile(ctas, read="presto", write="spark")[0]
        self.assertEqual(sql, "CREATE TABLE test STORED AS PARQUET AS SELECT 1")
        self.validate(
            "CREATE TABLE test WITH (FORMAT = 'PARQUET', X = '1', Z = '2') AS SELECT 1",
            "CREATE TABLE test STORED AS PARQUET TBLPROPERTIES ('X' = '1', 'Z' = '2') AS SELECT 1",
            read="presto",
            write="spark",
        )
        self.validate(
            "CREATE TABLE test STORED AS parquet TBLPROPERTIES ('x' = '1', 'Z' = '2') AS SELECT 1",
            "CREATE TABLE test WITH (FORMAT = 'parquet', x = '1', Z = '2') AS SELECT 1",
            read="spark",
            write="presto",
        )

        self.validate(
            "SELECT JSON_EXTRACT(x, '$.name')",
            "SELECT GET_JSON_OBJECT(x, '$.name')",
            read="presto",
            write="spark",
        )
        self.validate(
            "SELECT JSON_EXTRACT_SCALAR(x, '$.name')",
            "SELECT GET_JSON_OBJECT(x, '$.name')",
            read="presto",
            write="spark",
        )

        # pylint: disable=anomalous-backslash-in-string
        self.validate(
            "INITCAP('new york')",
            "REGEXP_REPLACE('new york', '(\w)(\w*)', x -> UPPER(x[1]) || LOWER(x[2]))",
            read="hive",
            write="presto",
        )

        self.validate("''''", "''''", read="presto", write="presto")
        self.validate("''''", "'\\''", read="presto", write="hive")
        self.validate("'x'", "'x'", read="presto", write="presto")
        self.validate("'x'", "'x'", read="presto", write="hive")
        self.validate("'''x'''", "'''x'''", read="presto", write="presto")
        self.validate("'''x'''", "'\\'x\\''", read="presto", write="hive")
        self.validate("'''x'", "'\\'x'", read="presto", write="hive")
        self.validate("'x'''", "'x\\''", read="presto", write="hive")

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", 'x."abc"', read="duckdb", write="presto"
        )
        self.validate(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            'x."y"."abc"',
            read="duckdb",
            write="presto",
        )

        self.validate("MONTH(x)", "MONTH(x)", read="presto", write="spark")
        self.validate("MONTH(x)", "MONTH(x)", read="presto", write="hive")
        self.validate(
            "MONTH(x)",
            "MONTH(CAST(DATE_PARSE(SUBSTR(CAST(x AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE))",
            read="hive",
            write="presto",
        )

        self.validate("DAY(x)", "DAY(x)", read="presto", write="hive")
        self.validate(
            "DAY(x)",
            "DAY(CAST(DATE_PARSE(SUBSTR(CAST(x AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE))",
            read="hive",
            write="presto",
        )

        self.validate("YEAR(x)", "YEAR(x)", read="presto", write="spark")
        self.validate("YEAR(x)", "YEAR(x)", read="presto", write="hive")
        self.validate(
            "YEAR(x)",
            "YEAR(CAST(DATE_PARSE(SUBSTR(CAST(x AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE))",
            read="hive",
            write="presto",
        )

        self.validate(
            "CONCAT_WS('-', 'a', 'b')",
            "ARRAY_JOIN(ARRAY['a', 'b'], '-')",
            write="presto",
        )
        self.validate("CONCAT_WS('-', x)", "ARRAY_JOIN(x, '-')", write="presto")
        self.validate("IF(x > 1, 1, 0)", "IF(x > 1, 1, 0)", write="presto")
        self.validate(
            "CASE WHEN 1 THEN x ELSE 0 END",
            "CASE WHEN 1 THEN x ELSE 0 END",
            write="presto",
        )
        self.validate("x[y]", "x[y]", read="presto", identity=False)
        self.validate("x[y]", "x[y]", write="presto", identity=False)

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT APPROX_DISTINCT(a, 0.1) FROM foo",
                read="presto",
                write="spark",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT * FROM x TABLESAMPLE(10) y",
            "SELECT * FROM x AS y TABLESAMPLE(10)",
            read="hive",
            write="presto",
        )

        self.validate("'\u6bdb'", "'\u6bdb'", read="presto")

        self.validate(
            "SELECT ARRAY_SORT(x)",
            "SELECT ARRAY_SORT(x)",
            read="presto",
        )

        self.validate(
            "SELECT ARRAY_SORT(x)",
            "SELECT SORT_ARRAY(x)",
            read="presto",
            write="hive",
        )

        self.validate(
            "SELECT SORT_ARRAY(x)",
            "SELECT ARRAY_SORT(x)",
            read="hive",
            write="presto",
        )

        self.validate(
            "SELECT SORT_ARRAY(x, False)",
            "SELECT ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
            read="hive",
            write="presto",
        )

        self.validate(
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            read="presto",
            write="spark",
        )
        self.validate(
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            "SELECT ARRAY_SORT(x, (left, right) -> -1)",
            read="spark",
            write="presto",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT ARRAY_SORT(x, (left, right) -> -1)",
                read="presto",
                write="hive",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT NULL as foo FROM baz",
            'SELECT NULL AS "foo" FROM "baz"',
            read="presto",
            write="presto",
            identify=True,
        )
        self.validate(
            "SELECT true as foo FROM baz",
            'SELECT TRUE AS "foo" FROM "baz"',
            read="presto",
            write="presto",
            identify=True,
        )
        self.validate(
            "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) as foo FROM baz",
            "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
            read="presto",
            write="presto",
            identify=False,
        )
        self.validate(
            "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) as foo FROM baz",
            'SELECT IF(COALESCE("bar", 0) = 1, TRUE, FALSE) AS "foo" FROM "baz"',
            read="hive",
            write="presto",
            identify=True,
        )
        self.validate(
            "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
            "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)",
            write="presto",
        )
        self.validate(
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
            write="presto",
        )
        self.validate(
            "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t(a)",
            write="presto",
        )

        self.validate(
            "SELECT a FROM x CROSS JOIN UNNEST(ARRAY(y))AS t (a)",
            "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            read="presto",
            write="hive",
        )
        self.validate(
            "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            "SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t(a)",
            read="hive",
            write="presto",
        )

        self.validate(
            "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY = ARRAY['y', 'z'])",
            "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            read="presto",
            write="hive",
        )
        self.validate(
            "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY = ARRAY['y', 'z'])",
            read="hive",
            write="presto",
        )
        self.validate(
            "CREATE TABLE x WITH (bucket_by = ARRAY['y'], bucket_count = 64) AS SELECT 1 AS y",
            "CREATE TABLE x WITH (bucket_by = ARRAY['y'], bucket_count = 64) AS SELECT 1 AS y",
            read="presto",
        )

        self.validate(
            "DATE_TO_DATE_STR(x)",
            "DATE_FORMAT(x, '%Y-%m-%d')",
            read="presto",
            write="presto",
        )
        self.validate(
            "DATE_TO_DATE_STR(x)",
            "STRFTIME(x, '%Y-%m-%d')",
            read="presto",
            write="duckdb",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(DATE_FORMAT(x, '%Y%m%d') AS INT)",
            read="presto",
            write="presto",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
            read="presto",
            write="duckdb",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "CAST(DATE_PARSE(CAST(x AS VARCHAR), '%Y%m%d') AS DATE)",
            read="presto",
            write="presto",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "CAST(STRPTIME(CAST(x AS STRING), '%Y%m%d') AS DATE)",
            read="presto",
            write="duckdb",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
            read="presto",
            write="presto",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            read="presto",
            write="spark",
        )

        self.validate(
            "LEVENSHTEIN(col1, col2)",
            "LEVENSHTEIN_DISTANCE(col1, col2)",
            write="presto",
        )

        self.validate(
            "LEVENSHTEIN(coalesce(col1, col2), coalesce(col2, col1))",
            "LEVENSHTEIN_DISTANCE(COALESCE(col1, col2), COALESCE(col2, col1))",
            write="presto",
        )

        self.validate(
            "ARRAY_FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            write="presto",
        )

        self.validate(
            "FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            read="presto",
            identity=False,
        )
        self.validate(
            "SELECT a AS b FROM x GROUP BY b",
            "SELECT a AS b FROM x GROUP BY 1",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
            read="presto",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
            "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
            read="presto",
            write="presto",
        )

    def test_hive(self):
        sql = transpile('SELECT "a"."b" FROM "foo"', write="hive")[0]
        self.assertEqual(sql, "SELECT `a`.`b` FROM `foo`")
        self.validate("""'["x"]'""", """'["x"]'""", write="hive", identity=True)
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            read="hive",
            write="hive",
        )
        self.validate(
            'SELECT "a"."b" FROM foo',
            "SELECT `a`.`b` FROM `foo`",
            write="hive",
            identify=True,
        )
        self.validate(
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            "SELECT APPROX_DISTINCT(a) FROM foo",
            read="hive",
            write="presto",
        )
        self.validate(
            "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            read="hive",
            write="presto",
        )
        self.validate(
            "SELECT GET_JSON_OBJECT(x, '$.name')",
            "SELECT JSON_EXTRACT_SCALAR(x, '$.name')",
            read="hive",
            write="presto",
        )

        self.validate(
            "MAP(a, b, c, d)",
            "MAP(a, b, c, d)",
            read="hive",
            write="hive",
        )
        self.validate(
            "MAP(a, b)",
            "MAP(a, b)",
            read="hive",
            write="hive",
        )
        self.validate(
            "MAP(a, b)",
            "MAP(ARRAY[a], ARRAY[b])",
            read="hive",
            write="presto",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "MAP(a, b)",
                read="presto",
                write="hive",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "MAP(a, b, c, d)",
            "MAP(ARRAY[a, c], ARRAY[b, d])",
            read="hive",
            write="presto",
        )
        self.validate(
            "MAP(ARRAY(a, b), ARRAY(c, d))",
            "MAP(a, c, b, d)",
            read="presto",
            write="hive",
        )
        self.validate(
            'MAP(ARRAY("a", "b"), ARRAY("c", "d"))',
            "MAP(`a`, `c`, `b`, `d`)",
            read="presto",
            write="hive",
        )
        self.validate(
            "MAP(ARRAY(a), ARRAY(b))",
            "MAP(a, b)",
            read="presto",
            write="hive",
        )
        self.validate(
            "MAP(ARRAY('a'), ARRAY('b'))",
            "MAP('a', 'b')",
            read="presto",
            write="hive",
        )
        self.validate("LOG(10)", "LN(10)", read="hive", write="presto")
        self.validate("LOG(2, 10)", "LOG(2, 10)", read="hive", write="presto")
        self.validate("'\\''", "''''", read="hive", write="presto")
        self.validate("'\"x\"'", "'\"x\"'", read="hive", write="presto")
        self.validate("\"'x'\"", "'''x'''", read="hive", write="presto")
        self.validate('ds = "2020-01-01"', "ds = '2020-01-01'", read="hive")
        self.validate("ds = \"1''2\"", "ds = '1\\'\\'2'", read="hive")
        self.validate("ds = \"1''2\"", "ds = '1''''2'", read="hive", write="presto")
        self.validate("x == 1", "x = 1", read="hive")
        self.validate("x == 1", "x = 1", read="hive", write="presto")
        self.validate("x div y", "CAST(x / y AS INTEGER)", read="hive", write="presto")

        self.validate(
            "DATE_STR_TO_DATE(x)",
            "TO_DATE(x)",
            write="hive",
        )
        self.validate(
            "STR_TO_TIME('2020-01-01', 'yyyy-MM-dd')",
            "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            write="hive",
            identity=False,
        )
        self.validate(
            "STR_TO_TIME('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            write="hive",
            identity=False,
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy')",
            "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy'))",
            write="hive",
            identity=False,
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "TS_OR_DS_ADD('2020-01-01', 1, 'DAY')",
            read="hive",
            write=None,
            identity=False,
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "DATE_ADD('2020-01-01', 1)",
            read="hive",
        )
        self.validate(
            "DATE_SUB('2020-01-01', 1)",
            "DATE_ADD('2020-01-01', 1 * -1)",
            read="hive",
        )
        self.validate(
            "DATE_SUB('2020-01-01', 1)",
            "DATE_FORMAT(DATE_ADD('DAY', 1 * -1, DATE_PARSE(SUBSTR('2020-01-01', 1, 10), '%Y-%m-%d')), '%Y-%m-%d')",
            read="hive",
            write="presto",
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "DATE_FORMAT(DATE_ADD('DAY', 1, DATE_PARSE(SUBSTR('2020-01-01', 1, 10), '%Y-%m-%d')), '%Y-%m-%d')",
            read="hive",
            write="presto",
        )
        self.validate(
            "TS_OR_DS_ADD('2021-02-01', 1, 'DAY')",
            "DATE_FORMAT(DATE_ADD('DAY', 1, DATE_PARSE(SUBSTR('2021-02-01', 1, 10), '%Y-%m-%d')), '%Y-%m-%d')",
            write="presto",
            identity=False,
        )
        self.validate(
            "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "TS_OR_DS_ADD('2021-02-01', 1, 'DAY')",
            "STRFTIME(CAST('2021-02-01' AS DATE) + INTERVAL 1 DAY, '%Y-%m-%d')",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "STRFTIME(CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY, '%Y-%m-%d')",
            read="hive",
            write="duckdb",
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-02')",
            "DATE_DIFF(TS_OR_DS_TO_DATE('2020-01-02'), TS_OR_DS_TO_DATE('2020-01-02'))",
            read="hive",
            write=None,
            identity=False,
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATEDIFF(TO_DATE('2020-01-02'), TO_DATE('2020-01-01'))",
            read="hive",
        )
        self.validate(
            "DATEDIFF(TO_DATE(y), x)",
            "DATE_DIFF('day', CAST(DATE_PARSE(SUBSTR(CAST(x AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE), "
            "CAST(DATE_PARSE(SUBSTR(CAST(DATE_FORMAT(DATE_PARSE(SUBSTR(CAST(y AS VARCHAR), 1, 10), '%Y-%m-%d'), '%Y-%m-%d') "
            "AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE))",
            read="hive",
            write="presto",
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATE_DIFF('day', CAST(DATE_PARSE(SUBSTR(CAST('2020-01-01' AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE), "
            "CAST(DATE_PARSE(SUBSTR(CAST('2020-01-02' AS VARCHAR), 1, 10), '%Y-%m-%d') AS DATE))",
            read="hive",
            write="presto",
        )

        self.validate("COLLECT_LIST(x)", "ARRAY_AGG(x)", read="hive", write="presto")
        self.validate("ARRAY_AGG(x)", "COLLECT_LIST(x)", read="presto", write="hive")
        self.validate("COLLECT_SET(x)", "SET_AGG(x)", read="hive", write="presto")
        self.validate("SET_AGG(x)", "COLLECT_SET(x)", read="presto", write="hive")
        self.validate("IF(x > 1, 1, 0)", "IF(x > 1, 1, 0)", write="hive")
        self.validate(
            "CASE WHEN 1 THEN x ELSE 0 END",
            "CASE WHEN 1 THEN x ELSE 0 END",
            write="hive",
        )

        self.validate(
            "UNIX_TIMESTAMP(x)",
            "STR_TO_UNIX(x, '%Y-%m-%d %H:%M:%S')",
            read="hive",
            identity=False,
        )
        self.validate(
            "TIME_STR_TO_UNIX(x)",
            "UNIX_TIMESTAMP(x)",
            write="hive",
        )
        self.validate(
            "TIME_STR_TO_TIME(x)",
            "CAST(x AS TIMESTAMP)",
            write="hive",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "CAST(x AS STRING)",
            write="hive",
        )
        self.validate(
            "UNIX_TO_TIME_STR(x)",
            "FROM_UNIXTIME(x)",
            write="hive",
        )
        self.validate(
            "FROM_UNIXTIME(x)",
            "DATE_FORMAT(FROM_UNIXTIME(x), '%Y-%m-%d %H:%i:%S')",
            read="hive",
            write="presto",
        )
        self.validate(
            "TS_OR_DS_TO_DATE(x)",
            "TO_DATE(x)",
            write="hive",
            identity=False,
        )
        self.validate(
            "TO_DATE(x)",
            "TS_OR_DS_TO_DATE_STR(x)",
            read="hive",
            identity=False,
        )

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", "x.`abc`", read="duckdb", write="hive"
        )
        self.validate(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            "x.`y`.`abc`",
            read="duckdb",
            write="hive",
        )

        self.validate(
            "MONTH('2021-03-01')",
            "MONTH(CAST('2021-03-01' AS DATE))",
            read="hive",
            write="duckdb",
        )
        self.validate("MONTH(x)", "MONTH(x)", read="duckdb", write="hive")

        self.validate(
            "DAY('2021-03-01')",
            "DAY(CAST('2021-03-01' AS DATE))",
            read="hive",
            write="duckdb",
        )
        self.validate("DAY(x)", "DAY(x)", read="duckdb", write="hive")

        self.validate("'\\\\a'", "'\\\\a'", read="hive")
        self.validate("'\\\\a'", "'\\a'", read="hive", write="presto")
        self.validate("'\\a'", "'\\\\a'", read="presto", write="hive")

        self.validate("1s", "CAST(1 AS SMALLINT)", read="hive")
        self.validate("1S", "CAST(1 AS SMALLINT)", read="hive")
        self.validate("1Y", "CAST(1 AS TINYINT)", read="hive")
        self.validate("1L", "CAST(1 AS BIGINT)", read="hive")
        self.validate("1.0bd", "CAST(1.0 AS DECIMAL)", read="hive")

        self.validate("TRY_CAST(1 AS INT)", "CAST(1 AS INT)", write="hive")
        self.validate(
            "CAST(1 AS INT)", "TRY_CAST(1 AS INTEGER)", read="hive", write="presto"
        )
        self.validate(
            "CAST(1 AS INT)", "CAST(1 AS INT)", read="hive", write="starrocks"
        )

        self.validate(
            "DATE_TO_DATE_STR(x)",
            "DATE_FORMAT(x, 'yyyy-MM-dd')",
            read="hive",
            write="hive",
        )
        self.validate(
            "DATE_TO_DATE_STR(x)",
            "DATE_FORMAT(x, '%Y-%m-%d')",
            read="hive",
            write="presto",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
            read="hive",
            write="hive",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(DATE_FORMAT(x, '%Y%m%d') AS INT)",
            read="hive",
            write="presto",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "TO_DATE(CAST(x AS STRING), 'yyyyMMdd')",
            read="hive",
            write="hive",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "CAST(DATE_PARSE(CAST(x AS VARCHAR), '%Y%m%d') AS DATE)",
            read="hive",
            write="presto",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            read="hive",
            write="hive",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
            read="hive",
            write="presto",
        )

        self.validate(
            "SELECT * FROM x TABLESAMPLE(1) foo",
            "SELECT * FROM x TABLESAMPLE(1) AS foo",
            read="hive",
            write="hive",
        )
        self.validate(
            "SELECT * FROM x TABLESAMPLE(1) foo",
            "SELECT * FROM x AS foo TABLESAMPLE(1)",
            read="hive",
            write="presto",
        )
        self.validate(
            "SELECT a AS b FROM x GROUP BY b",
            "SELECT a AS b FROM x GROUP BY 1",
            write="hive",
        )

    def test_spark(self):
        self.validate(
            'SELECT "a"."b" FROM "foo"',
            "SELECT `a`.`b` FROM `foo`",
            write="spark",
        )

        self.validate("CAST(a AS TEXT)", "CAST(a AS STRING)", write="spark")
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            "SELECT CAST(`a`.`b` AS SHORT) FROM foo",
            read="spark",
        )
        self.validate(
            'SELECT "a"."b" FROM foo',
            "SELECT `a`.`b` FROM `foo`",
            write="spark",
            identify=True,
        )
        self.validate(
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            "SELECT APPROX_DISTINCT(a) FROM foo",
            read="spark",
            write="presto",
        )
        self.validate(
            "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            read="spark",
            write="spark",
        )
        self.validate(
            "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE test USING ICEBERG STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (TABLE_FORMAT = 'ICEBERG', FORMAT = 'PARQUET') AS SELECT 1",
            read="spark",
            write="presto",
        )

        self.validate("ARRAY(0, 1, 2)", "ARRAY[0, 1, 2]", read="spark", write="presto")
        self.validate(
            "ARRAY(0, 1, 2)", "LIST_VALUE(0, 1, 2)", read="spark", write="duckdb"
        )
        self.validate(
            "SELECT /*+ COALESCE(3) */ * FROM x",
            "SELECT /*+ COALESCE(3) */ * FROM x",
            read="spark",
        )
        self.validate(
            "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
            "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
            read="spark",
        )
        self.validate(
            "x IN ('a', 'a''b')", "x IN ('a', 'a\\'b')", read="presto", write="spark"
        )

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", "x.`abc`", read="duckdb", write="spark"
        )
        self.validate(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            "x.`y`.`abc`",
            read="duckdb",
            write="spark",
        )

        self.validate(
            "MONTH('2021-03-01')",
            "MONTH(CAST('2021-03-01' AS DATE))",
            read="spark",
            write="duckdb",
        )
        self.validate(
            "YEAR('2021-03-01')",
            "YEAR(CAST('2021-03-01' AS DATE))",
            read="spark",
            write="duckdb",
        )
        self.validate("MONTH(x)", "MONTH(x)", read="duckdb", write="spark")

        self.validate("'\u6bdb'", "'毛'", read="spark")

        self.validate(
            "SELECT LEFT(x, 2), RIGHT(x, 2)",
            "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
            read="spark",
            write="presto",
        )
        self.validate(
            "ARRAY_SUM(ARRAY(1, 2))",
            "AGGREGATE(ARRAY(1, 2), 0, (acc, x) -> acc + x, acc -> acc)",
            write="spark",
        )
        self.validate(
            "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            "AGGREGATE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            write="spark",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100 ) SELECT sum(n) FROM t",
                read="presto",
                write="spark",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t (a)",
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            write="spark",
        )
        self.validate(
            "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t (a, b)",
            "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) t AS b",
            write="spark",
        )
        self.validate(
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t (a)",
            "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            write="spark",
        )

        self.validate(
            "MAP(a, b)",
            "MAP_FROM_ARRAYS(a, b)",
            read="presto",
            write="spark",
        )

        self.validate(
            "MAP(ARRAY[1], ARRAY[2])",
            "MAP_FROM_ARRAYS(ARRAY(1), ARRAY(2))",
            read="presto",
            write="spark",
        )
        self.validate(
            "MAP_FROM_ARRAYS(ARRAY(1), c)",
            "MAP(ARRAY[1], c)",
            read="spark",
            write="presto",
        )

        self.validate(
            "SELECT SORT_ARRAY(x, FALSE)",
            "SELECT SORT_ARRAY(x, FALSE)",
            read="hive",
            write="spark",
        )
        self.validate(
            "SELECT SORT_ARRAY(x, TRUE)",
            "SELECT SORT_ARRAY(x, TRUE)",
            read="hive",
            write="spark",
        )
        self.validate(
            "SELECT SORT_ARRAY(x, TRUE)",
            "SELECT SORT_ARRAY(x, TRUE)",
            read="spark",
            write="hive",
        )
        self.validate(
            "SELECT ARRAY_SORT(x)",
            "SELECT SORT_ARRAY(x)",
            read="spark",
            write="hive",
        )

        self.validate(
            "ARRAY_FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            write="spark",
        )

        self.validate(
            "FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a array<int>, col_b array<array<int>>)",
            "CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "SELECT 4 << 1",
            "SELECT SHIFTLEFT(4, 1)",
            read="hive",
            write="spark",
        )

        self.validate(
            "SELECT 4 >> 1",
            "SELECT SHIFTRIGHT(4, 1)",
            read="hive",
            write="spark",
        )

        self.validate(
            "SELECT SHIFTRIGHT(4, 1)",
            "SELECT 4 >> 1",
            read="spark",
            write="hive",
        )

        self.validate(
            "SELECT SHIFTLEFT(4, 1)",
            "SELECT 4 << 1",
            read="spark",
            write="hive",
        )
        self.validate(
            "SELECT * FROM VALUES ('x'), ('y') AS t(z)",
            "SELECT * FROM (VALUES ('x'), ('y')) AS t(z)",
            write="spark",
        )

        self.validate(
            'CREATE TABLE blah (col_a INT) COMMENT "Test comment: blah" PARTITIONED BY (date STRING) STORED AS ICEBERG',
            "CREATE TABLE blah (col_a INT) COMMENT 'Test comment: blah' PARTITIONED BY (date STRING) STORED AS ICEBERG",
            read="spark",
            write="spark",
        )

        self.validate(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
            "CREATE TABLE z (a INT) COMMENT 'x'",
            read="mysql",
            write="spark",
        )

    def test_snowflake(self):
        self.validate(
            'x:a:"b c"',
            "x['a']['b c']",
            read="snowflake",
        )
        self.validate(
            "CAST(x AS DOUBLE PRECISION)",
            "CAST(x AS DOUBLE)",
            read="snowflake",
        )

        self.validate(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            read="snowflake",
        )
        self.validate(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            read="bigquery",
            write="snowflake",
        )
        self.validate(
            "SELECT a FROM test QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test QUALIFY z ORDER BY a LIMIT 10",
            read="bigquery",
            write="snowflake",
        )
        self.validate(
            "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729)",
            "SELECT TO_TIMESTAMP(1659981729)",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            "SELECT TO_TIMESTAMP(1659981729)",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('1659981729')",
            "SELECT TO_TIMESTAMP('1659981729')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('2013-04-05 01:02:03')",
            "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-mm-dd hh24:mi:ss')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'MM/dd/yyyy HH:mm:ss')",
            read="snowflake",
            write="spark",
        )
        self.validate(
            "SELECT strptime('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S');",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read="duckdb",
            write="snowflake",
        )
        self.validate(
            "SELECT IFF(TRUE, 'true', 'false')",
            "SELECT IFF(TRUE, 'true', 'false')",
            read="snowflake",
        )

    def test_sqlite(self):
        self.validate(
            """
            CREATE TABLE "Track"
            (
                CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
                FOREIGN KEY ("AlbumId") REFERENCES "Album" ("AlbumId")
                    ON DELETE NO ACTION ON UPDATE NO ACTION,
                FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
                FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
            )
            """,
            """CREATE TABLE "Track" (
  CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
  FOREIGN KEY ("AlbumId") REFERENCES "Album"("AlbumId") ON DELETE NO ACTION ON UPDATE NO ACTION,
  FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
)""",
            read="sqlite",
            write="sqlite",
            pretty=True,
        )
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            "SELECT CAST(`a`.`b` AS INTEGER) FROM foo",
            read="sqlite",
            write="sqlite",
        )
        self.validate(
            "CAST(`a`.`b` AS INT)",
            'CAST("a"."b" AS INTEGER)',
            read="spark",
            write="sqlite",
        )

        self.validate(
            "LEVENSHTEIN(col1, col2)", "EDITDIST3(col1, col2)", write="sqlite"
        )

        self.validate(
            "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
            read="sqlite",
            write="mysql",
        )

        self.validate(
            "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
            "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
            read="mysql",
            write="sqlite",
        )
        self.validate(
            """CREATE TABLE "x" ("Name" NVARCHAR(200) NOT NULL)""",
            """CREATE TABLE "x" ("Name" TEXT(200) NOT NULL)""",
            read="sqlite",
            write="sqlite",
        )

    def test_oracle(self):
        self.validate(
            "CREATE TABLE z (n1 NUMBER, n2 NUMBER(10), n3 NUMBER(10, 8))",
            "CREATE TABLE z (n1 REAL, n2 REAL(10), n3 REAL(10, 8))",
            read="oracle",
            write="sqlite",
        )
        self.validate(
            "CREATE TABLE z (n1 INTEGER, n2 INTEGER(10), n3 REAL(8), c1 VARCHAR(30))",
            "CREATE TABLE z (n1 NUMBER, n2 NUMBER(10), n3 FLOAT(8), c1 VARCHAR2(30))",
            read="sqlite",
            write="oracle",
        )
        self.validate(
            "SELECT TOP 10 x FROM y",
            "SELECT x FROM y LIMIT 10",
            read="oracle",
        )
        self.validate(
            "SELECT a AS b FROM x GROUP BY b",
            "SELECT a AS b FROM x GROUP BY 1",
            write="oracle",
        )

    def test_tableau(self):
        self.validate(
            "IF(x = 'a', y, NULL)",
            "IF x = 'a' THEN y ELSE NULL END",
            write="tableau",
        )
        self.validate(
            "COALESCE(a, 0)",
            "IFNULL(a, 0)",
            write="tableau",
        )
        self.validate(
            "COUNT(DISTINCT(a))",
            "COUNTD((a))",
            write="tableau",
        )
        self.validate(
            "COUNT(a)",
            "COUNT(a)",
            write="tableau",
        )
        self.validate(
            "COUNT(DISTINCT x)",
            "COUNTD(x)",
            write="tableau",
        )

    def test_trino(self):
        self.validate(
            "ARRAY_SUM(ARRAY(1, 2))",
            "REDUCE(ARRAY[1, 2], 0, (acc, x) -> acc + x, acc -> acc)",
            write="trino",
        )

    def test_clickhouse(self):
        self.validate(
            "dictGet(x, 'y')",
            "dictGet(x, 'y')",
            write="clickhouse",
        )

        self.validate(
            "select * from x final",
            "SELECT * FROM x FINAL",
            read="clickhouse",
        )
        self.validate(
            "select * from x y final",
            "SELECT * FROM x AS y FINAL",
            read="clickhouse",
        )
