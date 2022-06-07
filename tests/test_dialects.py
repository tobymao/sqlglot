# pylint: disable=too-many-statements
import unittest

from sqlglot import ErrorLevel, UnsupportedError, transpile


class TestDialects(unittest.TestCase):
    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

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
            "CAST(a AS DATE) - CAST(b AS DATE)",
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
            "CAST(a AS ARRAY<INTEGER>)",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
            "CAST(ARRAY[1, 2] AS ARRAY<BIGINT>)",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(INT,INT))",
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP<INTEGER, INTEGER>)",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(MAP(ARRAY['a','b','c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP(VARCHAR, ARRAY(INT)))",
            "CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]) AS MAP<VARCHAR, ARRAY<INTEGER>>)",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(ARRAY[1, 2] AS ARRAY<BIGINT>)",
            "CAST(ARRAY[1, 2] AS ARRAY<BIGINT>)",
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
            "SELECT * FROM x TABLESAMPLE(10)",
            "SELECT * FROM x",
            read="hive",
            write="presto",
            unsupported_level=ErrorLevel.IGNORE,
        )

        self.validate("'\u6bdb'", "'\u6bdb'", read="presto")

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM x TABLESAMPLE(10)",
                read="hive",
                write="presto",
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
            "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t (a) CROSS JOIN UNNEST(z) AS u (b)",
            write="presto",
        )
        self.validate(
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t (a)",
            write="presto",
        )
        self.validate(
            "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t (a)",
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
            "SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t (a)",
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
        self.validate("'\"x\"'", "'\"x\"'", read="hive", write="presto")
        self.validate("\"'x'\"", "'''x'''", read="hive", write="presto")
        self.validate('ds = "2020-01-01"', "ds = '2020-01-01'", read="hive")
        self.validate("ds = \"1''2\"", "ds = '1\\'\\'2'", read="hive")
        self.validate("ds = \"1''2\"", "ds = '1''''2'", read="hive", write="presto")
        self.validate("x == 1", "x = 1", read="hive")
        self.validate("x == 1", "x = 1", read="hive", write="presto")
        self.validate("x div y", "CAST(x / y AS INTEGER)", read="hive", write="presto")

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
            "x",
            write="hive",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "x",
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
            "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
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

    def test_snowflake(self):
        self.validate(
            'x:a:"b c"',
            "x['a']['b c']",
            read="snowflake",
        )

    def test_sqlite(self):
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
