import os
import unittest

from sqlglot import TokenType, transpile


class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, 'fixtures')

    def validate(self, sql, target, write=None):
        self.assertEqual(transpile(sql, write=write)[0], target)

    def test_custom_transform(self):
        self.assertEqual(
            transpile('SELECT CAST(a AS INT) FROM x', transforms={TokenType.INT: 'SPECIAL INT'})[0],
            'SELECT CAST(a AS SPECIAL INT) FROM x',
        )

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

        self.validate("STR_TO_TIME(x, 'y')", "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'y'))", write='hive')
        self.validate("STR_TO_TIME(x, 'yyyy-MM-dd HH:mm:ss')", "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')", write='hive')
        self.validate("STR_TO_TIME(x, 'yyyy-MM-dd')", "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')", write='hive')

        self.validate(
            "STR_TO_UNIX('x', 'y')",
            "UNIX_TIMESTAMP('x', 'y')",
            write='hive',
        )
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write='hive')

        self.validate("TIME_STR_TO_TIME(x)", "TIME_STR_TO_TIME(x)", write=None)
        self.validate("TIME_STR_TO_UNIX(x)", "TIME_STR_TO_UNIX(x)", write=None)
        self.validate("TIME_TO_TIME_STR(x)", "TIME_TO_TIME_STR(x)", write=None)
        self.validate("TIME_TO_STR(x, 'y')", "TIME_TO_STR(x, 'y')", write=None)
        self.validate("TIME_TO_UNIX(x)", "TIME_TO_UNIX(x)", write=None)
        self.validate("UNIX_TO_STR(x, 'y')", "UNIX_TO_STR(x, 'y')", write=None)
        self.validate("UNIX_TO_TIME(x)", "UNIX_TO_TIME(x)", write=None)
        self.validate("UNIX_TO_TIME_STR(x)", "UNIX_TO_TIME_STR(x)", write=None)
        self.validate("TIME_STR_TO_DATE(x)", "TIME_STR_TO_DATE(x)", write=None)

        self.validate("TIME_STR_TO_DATE(x)", "TO_DATE(x)", write='hive')
        self.validate("UNIX_TO_STR(x, 'yyyy-MM-dd HH:mm:ss')", "FROM_UNIXTIME(x)", write='hive')
        self.validate("STR_TO_UNIX(x, 'yyyy-MM-dd HH:mm:ss')", "UNIX_TIMESTAMP(x)", write='hive')
        self.validate("IF(x > 1, x + 1)", "IF(x > 1, x + 1)", write='presto')
        self.validate("IF(x > 1, 1 + 1)", "IF(x > 1, 1 + 1)", write='hive')
        self.validate("IF(x > 1, 1, 0)", "IF(x > 1, 1, 0)", write='hive')

        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(x)",
            write='hive',
        )
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "FROM_UNIXTIME(123, 'y')",
            write='hive'
        )
        self.validate(
            "UNIX_TO_TIME(123)",
            "FROM_UNIXTIME(123)",
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
            "UNIX_TIMESTAMP(x)",
            write='spark',
        )
        self.validate("UNIX_TO_STR(123, 'y')", "FROM_UNIXTIME(123, 'y')", write='spark')
        self.validate(
            "UNIX_TO_TIME(123)",
            "FROM_UNIXTIME(123)",
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
