import os
import unittest

from sqlglot import transpile

class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, 'fixtures')

    def validate(self, sql, target, write=None):
        self.assertEqual(transpile(sql, write=write)[0], target)

    def test_space(self):
        self.validate('SELECT MIN(3)>MIN(2)', 'SELECT MIN(3) > MIN(2)')
        self.validate('SELECT MIN(3)>=MIN(2)', 'SELECT MIN(3) >= MIN(2)')
        self.validate('SELECT 1>0', 'SELECT 1 > 0')
        self.validate('SELECT 3>=3', 'SELECT 3 >= 3')

    def test_comments(self):
        self.validate('SELECT 1 FROM foo -- comment', 'SELECT 1 FROM foo')
        self.validate('SELECT 1 /* inline */ FROM foo -- comment', 'SELECT 1 FROM foo')

        self.validate(
            """
            SELECT 1 -- comment
            FROM foo -- comment
            """,
            'SELECT 1 FROM foo'
        )

        self.validate(
            """
            SELECT 1 /* big comment
             like this */
            FROM foo -- comment
            """,
            'SELECT 1 FROM foo'
        )

    def test_if(self):
        self.validate('SELECT IF(a > 1, 1, 0) FROM foo', 'SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END FROM foo')
        self.validate('SELECT IF(a > 1, 1) FROM foo', 'SELECT CASE WHEN a > 1 THEN 1 END FROM foo')

    def test_time(self):
        self.validate("STR_TO_TIME('x', 'y')", "STRPTIME('x', 'y')", write='duckdb')
        self.validate("STR_TO_UNIX('x', 'y')", "EPOCH(STRPTIME('x', 'y'))", write='duckdb')
        self.validate("TIME_TO_STR(x, 'y')", "STRFTIME(x, 'y')", write='duckdb')
        self.validate("TIME_TO_UNIX(x)", "EPOCH(x)", write='duckdb')
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "STRFTIME(EPOCH_MS(CAST((123 AS BIGINT) * 1000)), 'y')",
            write='duckdb'
        )
        self.validate(
            "UNIX_TO_TIME(123)",
            "EPOCH_MS(CAST((123 AS BIGINT) * 1000))",
            write='duckdb',
        )

        self.validate("STR_TO_TIME('x', 'y')", "DATE_FORMAT('x', 'yyyy-MM-dd HH:mm:ss')", write='hive')
        self.validate(
            "STR_TO_UNIX('x', 'y')",
            "UNIX_TIMESTAMP('x', 'y')",
            write='hive',
        )
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write='hive')
        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')",
            write='hive',
        )
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "FROM_UNIXTIME(123, 'y')",
            write='hive'
        )
        self.validate(
            "UNIX_TO_TIME(123)",
            "TO_UTC_TIMESTAMP(FROM_UNIXTIME(123, 'yyyy-MM-dd HH:mm:ss'), 'UTC')",
            write='hive',
        )

        self.validate("STR_TO_TIME('x', 'y')", "DATE_PARSE('x', 'y')", write='presto')
        self.validate("STR_TO_UNIX('x', 'y')", "TO_UNIXTIME(DATE_PARSE('x', 'y'))", write='presto')
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write='presto')
        self.validate("TIME_TO_UNIX(x)", "TO_UNIXTIME(x)", write='presto')
        self.validate("UNIX_TO_STR(123, 'y')", "DATE_FORMAT(FROM_UNIXTIME(123), 'y')", write='presto')
        self.validate("UNIX_TO_TIME(123)", "FROM_UNIXTIME(123)", write='presto')

        self.validate("STR_TO_TIME('x', 'y')", "TO_TIMESTAMP('x', 'y')", write='spark')
        self.validate("STR_TO_UNIX('x', 'y')", "UNIX_TIMESTAMP('x', 'y')", write='spark')
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write='spark')

        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')",
            write='spark',
        )
        self.validate("UNIX_TO_STR(123, 'y')", "FROM_UNIXTIME(123, 'y')", write='spark')
        self.validate(
            "UNIX_TO_TIME(123)",
            "TO_UTC_TIMESTAMP(FROM_UNIXTIME(123, 'yyyy-MM-dd HH:mm:ss'), 'UTC')",
            write='spark',
        )

    def test_identity(self):
        with open(os.path.join(self.fixtures_dir, 'identity.sql')) as f:
            for sql in f:
                self.assertEqual(transpile(sql)[0], sql.strip())

    def test_pretty(self):
        with open(os.path.join(self.fixtures_dir, 'pretty.sql')) as f:
            lines = f.read().split(';')
            size = len(lines)

            for i in range(0, size, 2):
                if i + 1 < size:
                    sql = lines[i]
                    pretty = lines[i + 1].strip()
                    generated = transpile(sql, pretty=True)[0]
                    self.assertEqual(generated, pretty)
