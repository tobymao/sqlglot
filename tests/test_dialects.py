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

        self.validate("DATE_FORMAT(x, 'y')", "DATE_FORMAT(x, 'y')", read='presto', write='hive')
        self.validate("DATE_PARSE(x, 'y')", "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')", read='presto', write='hive')
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
            "TO_UTC_TIMESTAMP(FROM_UNIXTIME(x, 'yyyy-MM-dd HH:mm:ss'), 'UTC')",
            read='presto',
            write='hive',
        )
        self.validate(
            'TO_UNIXTIME(x)',
            "UNIX_TIMESTAMP(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')",
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

        self.validate("COLLECT_LIST(x)", "ARRAY_AGG(x)", read='hive', write='presto')
        self.validate("ARRAY_AGG(x)", "COLLECT_LIST(x)", read='presto', write='hive')
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
            "TO_UTC_TIMESTAMP(FROM_UNIXTIME(x, 'yyyy-MM-dd HH:mm:ss'), 'UTC')",
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
