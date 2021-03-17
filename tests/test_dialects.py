import unittest

import sqlglot
from sqlglot.errors import UnsupportedError
from sqlglot.generator import UnsupportedLevel

class TestDialects(unittest.TestCase):
    def test_spark(self):
        sql = sqlglot.transpile('SELECT "a"."b" FROM "foo"', write='spark')[0]
        self.assertEqual(sql, "SELECT `a`.`b` FROM `foo`")

        sql = sqlglot.transpile('SELECT "a"."b" FROM foo', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

    def test_presto(self):
        sql = sqlglot.transpile('SELECT "a"."b" FROM foo', read='presto', write='presto', identify=True)[0]
        self.assertEqual(sql, 'SELECT "a"."b" FROM "foo"')

    def test_presto_to_spark(self):
        sql = sqlglot.transpile('SELECT a.b FROM foo', read='presto', write='spark')[0]
        self.assertEqual(sql, 'SELECT a.b FROM foo')

        sql = sqlglot.transpile('SELECT "a"."b" FROM foo', read='presto', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = sqlglot.transpile('SELECT a.b FROM foo', read='presto', write='spark', identify=True)[0]
        self.assertEqual(sql, 'SELECT `a`.`b` FROM `foo`')

        sql = sqlglot.transpile('SELECT APPROX_DISTINCT(a) FROM foo', read='presto', write='spark')[0]
        self.assertEqual(sql, 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo')

        sql = sqlglot.transpile(
            'SELECT APPROX_DISTINCT(a, 0.1) FROM foo',
            read='presto',
            write='spark',
            unsupported_level=UnsupportedLevel.IGNORE
        )[0]
        self.assertEqual(sql, 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo')

        with self.assertRaises(UnsupportedError):
            sqlglot.transpile(
                'SELECT APPROX_DISTINCT(a, 0.1) FROM foo',
                read='presto',
                write='spark',
                unsupported_level=UnsupportedLevel.RAISE,
            )

    def test_spark_to_presto(self):
        sql = sqlglot.transpile('SELECT APPROX_COUNT_DISTINCT(a) FROM foo', read='spark', write='presto')[0]
        self.assertEqual(sql, 'SELECT APPROX_DISTINCT(a) FROM foo')
