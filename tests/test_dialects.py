import unittest

from sqlglot import transpile
from sqlglot.errors import ErrorLevel, UnsupportedError

class TestDialects(unittest.TestCase):
    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

    def test_duckdb(self):
        self.validate(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            "STRPTIME('2020-01-01', '%Y-%m-%d')",
            read='duckdb',
            write='duckdb',
        )
        self.validate('EPOCH(x)', 'EPOCH(x)', read='duckdb')
        self.validate('EPOCH(x)', 'TO_UNIXTIME(x)', read='duckdb', write='presto')
        self.validate('EPOCH_MS(x)', 'FROM_UNIXTIME(x / 1000)', read='duckdb', write='presto')
        self.validate("STRFTIME(x, 'y')", "DATE_FORMAT(x, 'y')", read='duckdb', write='presto')
        self.validate("STRPTIME(x, 'y')", "DATE_PARSE(x, 'y')", read='duckdb', write='presto')
        self.validate("LIST_VALUE(0, 1, 2)", "ARRAY[0, 1, 2]", read='duckdb', write='presto')
        self.validate('Array(1, 2)', 'LIST_VALUE(1, 2)', write='duckdb')

    def test_mysql(self):
        self.validate(
            'SELECT CAST(`a`.`b` AS INT) FROM foo', 'SELECT CAST(`a`.`b` AS INT) FROM foo',
            read='mysql',
            write='mysql',
        )

    def test_postgres(self):
        self.validate(
            'SELECT CAST(`a`.`b` AS DOUBLE) FROM foo',
            'SELECT CAST(`a`.`b` AS DOUBLE PRECISION) FROM foo',
            read='postgres',
            write='postgres',
        )

    def test_presto(self):
        self.validate('SELECT "a"."b" FROM foo', 'SELECT "a"."b" FROM "foo"', read='presto', write='presto', identify=True)
        self.validate('SELECT a.b FROM foo', 'SELECT a.b FROM foo', read='presto', write='spark')
        self.validate('SELECT "a"."b" FROM foo', 'SELECT `a`.`b` FROM `foo`', read='presto', write='spark', identify=True)
        self.validate('SELECT a.b FROM foo', 'SELECT `a`.`b` FROM `foo`', read='presto', write='spark', identify=True)
        self.validate('SELECT ARRAY[1, 2]', 'SELECT ARRAY(1, 2)', read='presto', write='spark')
        self.validate('CAST(a AS ARRAY(INT))', 'CAST(a AS ARRAY[INTEGER])', read='presto', write='presto')
        self.validate('CAST(a AS TEXT)', 'CAST(a AS VARCHAR)', write='presto')
        self.validate('CAST(a AS STRING)', 'CAST(a AS VARCHAR)', write='presto')
        self.validate('CAST(a AS VARCHAR)', 'CAST(a AS STRING)', read='presto', write='spark')

        self.validate('x & 1', 'BITWISE_AND(x, 1)', read='hive', write='presto')
        self.validate('~x', 'BITWISE_NOT(x)', read='hive', write='presto')
        self.validate('x | 1', 'BITWISE_OR(x, 1)', read='hive', write='presto')
        self.validate('x << 1', 'BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)', read='hive', write='presto')
        self.validate('x >> 1', 'BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)', read='hive', write='presto')
        self.validate('x & 1 > 0', 'BITWISE_AND(x, 1) > 0', read='hive', write='presto')

        self.validate("DATE_FORMAT(x, 'y')", "DATE_FORMAT(x, 'y')", read='presto', write='hive')
        self.validate("DATE_PARSE(x, 'y')", "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'y'))", read='presto', write='hive')
        self.validate(
            'TIME_STR_TO_UNIX(x)',
            "TO_UNIXTIME(DATE_PARSE(x, '%Y-%m-%d %H:%i:%s'))",
            write='presto',
        )
        self.validate(
            'TIME_STR_TO_TIME(x)',
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            write='presto',
        )
        self.validate(
            'TIME_TO_TIME_STR(x)',
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%s')",
            write='presto',
        )
        self.validate(
            'UNIX_TO_TIME_STR(x)',
            "DATE_FORMAT(FROM_UNIXTIME(x), '%Y-%m-%d %H:%i:%s')",
            write='presto',
        )
        self.validate(
            'FROM_UNIXTIME(x)',
            "FROM_UNIXTIME(x)",
            read='presto',
            write='hive',
        )
        self.validate(
            'TO_UNIXTIME(x)',
            "UNIX_TIMESTAMP(x)",
            read='presto',
            write='hive',
        )
        self.validate(
            "DATE_ADD('day', 1, x)",
            "DATE_ADD(x, 1)",
            read='presto',
            write='hive',
        )
        self.validate(
            "DATE_DIFF('day', a, b)",
            "DATEDIFF(b, a)",
            read='presto',
            write='hive',
        )

        self.validate(
            'SELECT APPROX_DISTINCT(a) FROM foo',
            'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
            read='presto',
            write='spark',
        )

        sql = transpile(
            'SELECT APPROX_DISTINCT(a, 0.1) FROM foo',
            read='presto',
            write='spark',
            unsupported_level=ErrorLevel.IGNORE
        )[0]
        self.assertEqual(sql, 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo')

        ctas = "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1"
        self.assertEqual(transpile(ctas, read='presto', write='presto')[0], ctas)

        sql = transpile(ctas, read='presto', write='spark')[0]
        self.assertEqual(sql, "CREATE TABLE test STORED AS PARQUET AS SELECT 1")

        sql = transpile("SELECT JSON_EXTRACT(x, '$.name')", read='presto', write='spark')[0]
        self.assertEqual(sql, "SELECT GET_JSON_OBJECT(x, '$.name')")

        self.validate("''''", "''''", read='presto', write='presto')
        self.validate("''''", "'\\''", read='presto', write='hive')
        self.validate("'x'", "'x'", read='presto', write='presto')
        self.validate("'x'", "'x'", read='presto', write='hive')
        self.validate("'''x'''", "'''x'''", read='presto', write='presto')
        self.validate("'''x'''", "'\\'x\\''", read='presto', write='hive')
        self.validate("'''x'", "'\\'x'", read='presto', write='hive')
        self.validate("'x'''", "'x\\''", read='presto', write='hive')

        with self.assertRaises(UnsupportedError):
            transpile(
                'SELECT APPROX_DISTINCT(a, 0.1) FROM foo',
                read='presto',
                write='spark',
                unsupported_level=ErrorLevel.RAISE,
            )

    def test_hive(self):
        sql = transpile('SELECT "a"."b" FROM "foo"', write='hive')[0]
        self.assertEqual(sql, "SELECT `a`.`b` FROM `foo`")
        self.validate(
            'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo',
            'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo',
            read='hive',
            write='hive',
        )
        self.validate('SELECT "a"."b" FROM foo', 'SELECT `a`.`b` FROM `foo`', write='hive', identify=True)
        self.validate(
            'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
            'SELECT APPROX_DISTINCT(a) FROM foo',
            read='hive',
            write='presto',
        )
        self.validate(
            'CREATE TABLE test STORED AS PARQUET AS SELECT 1',
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            read='hive',
            write='presto',
        )
        self.validate(
            "SELECT GET_JSON_OBJECT(x, '$.name', '$.name')",
            "SELECT JSON_EXTRACT(x, '$.name')",
            read='hive',
            write='presto',
        )

        self.validate(
            "STR_TO_TIME('2020-01-01', 'yyyy-MM-dd')",
            "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            write='hive',
            identity=False,
        )
        self.validate(
            "STR_TO_TIME('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            write='hive',
            identity=False,
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy')",
            "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy'))",
            write='hive',
            identity=False,
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "DATE_ADD(DATE_STR_TO_DATE('2020-01-01'), 1)",
            read='hive',
            write=None,
            identity=False,
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "DATE_ADD('2020-01-01', 1)",
            read='hive',
        )
        self.validate(
            "DATE_SUB('2020-01-01', 1)",
            "DATE_ADD('2020-01-01', 1 * -1)",
            read='hive',
        )
        self.validate(
            "DATE_SUB('2020-01-01', 1)",
            "DATE_ADD('day', 1 * -1, DATE_PARSE('2020-01-01', '%Y-%m-%d'))",
            read='hive',
            write='presto',
        )
        self.validate(
            "DATE_ADD('2020-01-01', 1)",
            "DATE_ADD('day', 1, DATE_PARSE('2020-01-01', '%Y-%m-%d'))",
            read='hive',
            write='presto',
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-02')",
            "DATE_DIFF(DATE_STR_TO_DATE('2020-01-02'), DATE_STR_TO_DATE('2020-01-02'))",
            read='hive',
            write=None,
            identity=False,
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATEDIFF('2020-01-02', '2020-01-01')",
            read='hive',
        )
        self.validate(
            "DATEDIFF(TO_DATE(y), x)",
            "DATE_DIFF('day', DATE_PARSE(x, '%Y-%m-%d'), DATE_PARSE(DATE_FORMAT(DATE_PARSE(SUBSTR(y, 1, 10), '%Y-%m-%d'), '%Y-%m-%d'), '%Y-%m-%d'))",
            read='hive',
            write='presto',
        )
        self.validate(
            "DATEDIFF('2020-01-02', '2020-01-01')",
            "DATE_DIFF('day', DATE_PARSE('2020-01-01', '%Y-%m-%d'), DATE_PARSE('2020-01-02', '%Y-%m-%d'))",
            read='hive',
            write='presto',
        )

        self.validate("COLLECT_LIST(x)", "ARRAY_AGG(x)", read='hive', write='presto')
        self.validate("ARRAY_AGG(x)", "COLLECT_LIST(x)", read='presto', write='hive')

        self.validate(
            "UNIX_TIMESTAMP(x)",
            "STR_TO_UNIX(x, 'yyyy-MM-dd HH:mm:ss')",
            read='hive',
            identity=False,
        )
        self.validate(
            'TIME_STR_TO_UNIX(x)',
            "UNIX_TIMESTAMP(x)",
            write='hive',
        )
        self.validate(
            'TIME_STR_TO_TIME(x)',
            'x',
            write='hive',
        )
        self.validate(
            'TIME_TO_TIME_STR(x)',
            'x',
            write='hive',
        )
        self.validate(
            'UNIX_TO_TIME_STR(x)',
            "FROM_UNIXTIME(x)",
            write='hive',
        )

    def test_spark(self):
        self.validate(
            'SELECT "a"."b" FROM "foo"',
            'SELECT `a`.`b` FROM `foo`',
            write='spark',
        )

        self.validate('CAST(a AS TEXT)', 'CAST(a AS STRING)', write='spark')
        self.validate('SELECT CAST(`a`.`b` AS SMALLINT) FROM foo', 'SELECT CAST(`a`.`b` AS SHORT) FROM foo', read='spark')
        self.validate('SELECT "a"."b" FROM foo', 'SELECT `a`.`b` FROM `foo`', write='spark', identify=True)
        self.validate(
            'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
            'SELECT APPROX_DISTINCT(a) FROM foo',
            read='spark',
            write='presto',
        )
        self.validate(
            'CREATE TABLE test STORED AS PARQUET AS SELECT 1',
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            read='spark',
            write='presto',
        )

        self.validate("ARRAY(0, 1, 2)", "ARRAY[0, 1, 2]", read='spark', write='presto')
        self.validate("ARRAY(0, 1, 2)", "LIST_VALUE(0, 1, 2)", read='spark', write='duckdb')
        self.validate('SELECT /*+ COALESCE(3) */ * FROM x','SELECT /*+ COALESCE(3) */ * FROM x', read='spark')

    def test_sqlite(self):
        self.validate(
            'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo',
            'SELECT CAST(`a`.`b` AS INTEGER) FROM foo',
            read='sqlite',
            write='sqlite',
        )
