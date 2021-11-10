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
            "STRFTIME(x, 'y')", "DATE_FORMAT(x, 'y')", read="duckdb", write="presto"
        )
        self.validate(
            "STRPTIME(x, 'y')", "DATE_PARSE(x, 'y')", read="duckdb", write="presto"
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

    def test_mysql(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            "SELECT CAST(`a`.`b` AS INT) FROM foo",
            read="mysql",
            write="mysql",
        )

    def test_postgres(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS DOUBLE) FROM foo",
            "SELECT CAST(`a`.`b` AS DOUBLE PRECISION) FROM foo",
            read="postgres",
            write="postgres",
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
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(ARRAY(INT(9))))",
            "CAST(MAP(ARRAY[1], ARRAY[1]) AS MAP(ARRAY(INTEGER(9))))",
            read="presto",
            write="presto",
        )
        self.validate(
            "CAST(ARRAY[1, 2] AS ARRAY<BIGINT>)",
            "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))",
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
            "CASE WHEN x > 1 THEN 1 ELSE 0 END", "IF(x > 1, 1, 0)", write="presto"
        )
        self.validate("CASE WHEN x > 1 THEN 1 END", "IF(x > 1, 1)", write="presto")
        self.validate(
            "CASE WHEN x > 1 THEN 1 WHEN x > 2 THEN 2 END",
            "CASE WHEN x > 1 THEN 1 WHEN x > 2 THEN 2 END",
            write="presto",
        )

        self.validate(
            "ARRAY_CONTAINS(x, 1)", "CONTAINS(x, 1)", read="hive", write="presto"
        )
        self.validate("SIZE(x)", "CARDINALITY(x)", read="hive", write="presto")
        self.validate("CARDINALITY(x)", "SIZE(x)", read="presto", write="hive")
        self.validate("ARRAY_SIZE(x)", "CARDINALITY(x)", write="presto", identity=False)

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
            "DATE_FORMAT(x, 'y')", "DATE_FORMAT(x, 'y')", read="presto", write="hive"
        )
        self.validate(
            "DATE_PARSE(x, 'y')",
            "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'y'))",
            read="presto",
            write="hive",
        )
        self.validate(
            "TIME_STR_TO_UNIX(x)",
            "TO_UNIXTIME(DATE_PARSE(x, '%Y-%m-%d %H:%i:%s'))",
            write="presto",
        )
        self.validate(
            "TIME_STR_TO_TIME(x)",
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            write="presto",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%s')",
            write="presto",
        )
        self.validate(
            "UNIX_TO_TIME_STR(x)",
            "DATE_FORMAT(FROM_UNIXTIME(x), '%Y-%m-%d %H:%i:%s')",
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
            "DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d')",
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

        sql = transpile(
            "SELECT JSON_EXTRACT(x, '$.name')", read="presto", write="spark"
        )[0]
        self.assertEqual(sql, "SELECT GET_JSON_OBJECT(x, '$.name')")

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
            "MONTH(DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d'))",
            read="hive",
            write="presto",
        )

        self.validate("DAY(x)", "DAY(x)", read="presto", write="hive")
        self.validate(
            "DAY(x)",
            "DAY(DATE_PARSE(SUBSTR(x, 1, 10), '%Y-%m-%d'))",
            read="hive",
            write="presto",
        )
        self.validate(
            "CONCAT_WS('-', 'a', 'b')",
            "ARRAY_JOIN(ARRAY['a', 'b'], '-')",
            write="presto",
        )
        self.validate("CONCAT_WS('-', x)", "ARRAY_JOIN(x, '-')", write="presto")

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

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM x TABLESAMPLE(10)",
                read="hive",
                write="presto",
                unsupported_level=ErrorLevel.RAISE,
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
            "MAP(ARRAY[a, c], ARRAY[b, d])",
            read="hive",
            write="presto",
        )
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
            "DATE_DIFF(DATE_STR_TO_DATE('2020-01-02'), DATE_STR_TO_DATE('2020-01-02'))",
            read="hive",
            write=None,
            identity=False,
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATEDIFF('2020-01-02', '2020-01-01')",
            read="hive",
        )
        self.validate(
            "DATEDIFF(TO_DATE(y), x)",
            "DATE_DIFF('day', DATE_PARSE(x, '%Y-%m-%d'), DATE_PARSE(DATE_FORMAT(DATE_PARSE(SUBSTR(y, 1, 10), '%Y-%m-%d'), '%Y-%m-%d'), '%Y-%m-%d'))",
            read="hive",
            write="presto",
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATE_DIFF('day', DATE_PARSE('2020-01-01', '%Y-%m-%d'), DATE_PARSE('2020-01-02', '%Y-%m-%d'))",
            read="hive",
            write="presto",
        )

        self.validate("COLLECT_LIST(x)", "ARRAY_AGG(x)", read="hive", write="presto")
        self.validate("ARRAY_AGG(x)", "COLLECT_LIST(x)", read="presto", write="hive")
        self.validate("COLLECT_SET(x)", "SET_AGG(x)", read="hive", write="presto")
        self.validate("SET_AGG(x)", "COLLECT_SET(x)", read="presto", write="hive")

        self.validate(
            "CASE WHEN x > 1 THEN 1 ELSE 0 END", "IF(x > 1, 1, 0)", write="hive"
        )
        self.validate("CASE WHEN x > 1 THEN 1 END", "IF(x > 1, 1)", write="hive")

        self.validate(
            "UNIX_TIMESTAMP(x)",
            "STR_TO_UNIX(x, 'yyyy-MM-dd HH:mm:ss')",
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
        self.validate("MONTH(x)", "MONTH(x)", read="duckdb", write="spark")

        with self.assertRaises(UnsupportedError):
            transpile(
                "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100 ) SELECT sum(n) FROM t",
                read="presto",
                write="spark",
                unsupported_level=ErrorLevel.RAISE,
            )

    def test_sqlite(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            "SELECT CAST(`a`.`b` AS INTEGER) FROM foo",
            read="sqlite",
            write="sqlite",
        )
