import unittest

from sqlglot import transpile
from sqlglot.errors import ErrorLevel, UnsupportedError

class TestDialects(unittest.TestCase):
    def test_mysql(self):
        sql = transpile('SELECT CAST(`a`.`b` AS INT) FROM foo', read='mysql', write='mysql')[0]
        self.assertEqual(sql, 'SELECT CAST(`a`.`b` AS INT) FROM foo')

    def test_postgres(self):
        sql = transpile('SELECT CAST(`a`.`b` AS DOUBLE) FROM foo', read='postgres', write='postgres')[0]
        self.assertEqual(sql, 'SELECT CAST(`a`.`b` AS DOUBLE PRECISION) FROM foo')

    def test_presto(self):
        sql = transpile('SELECT "a"."b" FROM foo', read='presto', write='presto', identify=True)[0]
        self.assertEqual(sql, 'SELECT "a"."b" FROM "foo"')

        sql = transpile('SELECT a.b FROM foo', read='presto', write='spark')[0]
        self.assertEqual(sql, 'SELECT a.b FROM foo')

        sql = transpile('SELECT "a"."b" FROM foo', read='presto', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = transpile('SELECT a.b FROM foo', read='presto', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = transpile('SELECT APPROX_DISTINCT(a) FROM foo', read='presto', write='spark')[0]
        self.assertEqual(sql, 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo')

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

        sql = transpile('SELECT CAST(`a`.`b` AS SMALLINT) FROM foo', read='hive', write='hive')[0]
        self.assertEqual(sql, 'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo')

        sql = transpile('SELECT "a"."b" FROM foo', write='hive', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = transpile('SELECT APPROX_COUNT_DISTINCT(a) FROM foo', read='hive', write='presto')[0]
        self.assertEqual(sql, 'SELECT APPROX_DISTINCT(a) FROM foo')

        sql = transpile('CREATE TABLE test STORED AS PARQUET AS SELECT 1', read='hive', write='presto')[0]
        self.assertEqual(sql, "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1")

        sql = transpile("SELECT GET_JSON_OBJECT(x, '$.name')", read='hive', write='presto')[0]
        self.assertEqual(sql, "SELECT JSON_EXTRACT(x, '$.name')")

    def test_spark(self):
        sql = transpile('SELECT "a"."b" FROM "foo"', write='spark')[0]
        self.assertEqual(sql, "SELECT `a`.`b` FROM `foo`")

        sql = transpile('SELECT CAST(`a`.`b` AS SMALLINT) FROM foo', read='spark')[0]
        self.assertEqual(sql, 'SELECT CAST(`a`.`b` AS SHORT) FROM foo')

        sql = transpile('SELECT "a"."b" FROM foo', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = transpile('SELECT APPROX_COUNT_DISTINCT(a) FROM foo', read='spark', write='presto')[0]
        self.assertEqual(sql, 'SELECT APPROX_DISTINCT(a) FROM foo')

        sql = transpile('CREATE TABLE test STORED AS PARQUET AS SELECT 1', read='spark', write='presto')[0]
        self.assertEqual(sql, "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1")

        sql = transpile('SELECT /*+ COALESCE(3) */ * FROM x', read='spark')[0]
        self.assertEqual(sql, 'SELECT /*+ COALESCE(3) */ * FROM x')

    def test_sqlite(self):
        sql = transpile('SELECT CAST(`a`.`b` AS SMALLINT) FROM foo', read='sqlite', write='sqlite')[0]
        self.assertEqual(sql, 'SELECT CAST(`a`.`b` AS INTEGER) FROM foo')
