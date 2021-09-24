# pylint: disable=too-many-statements
import os
import unittest

from sqlglot import ErrorLevel, ParseError, TokenType, transpile


class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures")
    maxDiff = None

    def validate(self, sql, target, write=None):
        self.assertEqual(transpile(sql, write=write)[0], target)

    def test_asc(self):
        self.validate("SELECT x FROM y ORDER BY x ASC", "SELECT x FROM y ORDER BY x")

    def test_custom_transform(self):
        self.assertEqual(
            transpile(
                "SELECT CAST(a AS INT) FROM x",
                transforms={TokenType.INT: "SPECIAL INT"},
            )[0],
            "SELECT CAST(a AS SPECIAL INT) FROM x",
        )

    def test_space(self):
        self.validate("SELECT MIN(3)>MIN(2)", "SELECT MIN(3) > MIN(2)")
        self.validate("SELECT MIN(3)>=MIN(2)", "SELECT MIN(3) >= MIN(2)")
        self.validate("SELECT 1>0", "SELECT 1 > 0")
        self.validate("SELECT 3>=3", "SELECT 3 >= 3")

    def test_comments(self):
        self.validate("SELECT 1 FROM foo -- comment", "SELECT 1 FROM foo")
        self.validate("SELECT 1 /* inline */ FROM foo -- comment", "SELECT 1 FROM foo")

        self.validate(
            """
            SELECT 1 -- comment
            FROM foo -- comment
            """,
            "SELECT 1 FROM foo",
        )

        self.validate(
            """
            SELECT 1 /* big comment
             like this */
            FROM foo -- comment
            """,
            "SELECT 1 FROM foo",
        )

    def test_types(self):
        self.validate("INT x", "CAST(x AS INT)")
        self.validate("VARCHAR x y", "CAST(x AS VARCHAR) AS y")
        self.validate("STRING x y", "CAST(x AS TEXT) AS y")
        self.validate("x::INT", "CAST(x AS INT)")
        self.validate("x::INTEGER", "CAST(x AS INT)")
        self.validate("x::INT y", "CAST(x AS INT) AS y")
        self.validate("x::INT AS y", "CAST(x AS INT) AS y")

        with self.assertRaises(ParseError):
            transpile("x::z")

    def test_not_range(self):
        self.validate("a NOT LIKE b", "NOT a LIKE b")
        self.validate("a NOT BETWEEN b AND c", "NOT a BETWEEN b AND c")
        self.validate("a NOT IN (1, 2)", "NOT a IN (1, 2)")

    def test_extract(self):
        self.validate(
            "EXTRACT(day FROM '2020-01-01'::TIMESTAMP)",
            "EXTRACT(day FROM CAST('2020-01-01' AS TIMESTAMP))",
        )
        self.validate(
            "EXTRACT(timezone FROM '2020-01-01'::TIMESTAMP)",
            "EXTRACT(timezone FROM CAST('2020-01-01' AS TIMESTAMP))",
        )
        self.validate(
            "EXTRACT(year FROM '2020-01-01'::TIMESTAMP WITH TIME ZONE)",
            "EXTRACT(year FROM CAST('2020-01-01' AS TIMESTAMPTZ))",
        )
        self.validate(
            "extract(month from '2021-01-31'::timestamp without time zone)",
            "EXTRACT(month FROM CAST('2021-01-31' AS TIMESTAMP))",
        )

    def test_if(self):
        self.validate(
            "SELECT IF(a > 1, 1, 0) FROM foo",
            "SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END FROM foo",
        )
        self.validate(
            "SELECT IF(a > 1, 1) FROM foo", "SELECT CASE WHEN a > 1 THEN 1 END FROM foo"
        )

    def test_time(self):
        self.validate("TIMESTAMP '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMP)")
        self.validate(
            "TIMESTAMP WITH TIME ZONE '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMPTZ)"
        )
        self.validate(
            "TIMESTAMP WITHOUT TIME ZONE '2020-01-01'",
            "CAST('2020-01-01' AS TIMESTAMP)",
        )
        self.validate("'2020-01-01'::TIMESTAMP", "CAST('2020-01-01' AS TIMESTAMP)")
        self.validate(
            "'2020-01-01'::TIMESTAMP WITHOUT TIME ZONE",
            "CAST('2020-01-01' AS TIMESTAMP)",
        )
        self.validate(
            "'2020-01-01'::TIMESTAMP WITH TIME ZONE",
            "CAST('2020-01-01' AS TIMESTAMPTZ)",
        )
        self.validate("DATE '2020-01-01'", "CAST('2020-01-01' AS DATE)")
        self.validate("'2020-01-01'::DATE", "CAST('2020-01-01' AS DATE)")
        self.validate("STR_TO_TIME('x', 'y')", "STRPTIME('x', 'y')", write="duckdb")
        self.validate(
            "STR_TO_UNIX('x', 'y')", "EPOCH(STRPTIME('x', 'y'))", write="duckdb"
        )
        self.validate("TIME_TO_STR(x, 'y')", "STRFTIME(x, 'y')", write="duckdb")
        self.validate("TIME_TO_UNIX(x)", "EPOCH(x)", write="duckdb")
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "STRFTIME(TO_TIMESTAMP(CAST(123 AS BIGINT)), 'y')",
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME(123)",
            "TO_TIMESTAMP(CAST(123 AS BIGINT))",
            write="duckdb",
        )

        self.validate(
            "STR_TO_TIME(x, 'y')", "FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'y'))", write="hive"
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy-MM-dd HH:mm:ss')",
            "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            write="hive",
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy-MM-dd')",
            "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            write="hive",
        )

        self.validate(
            "STR_TO_UNIX('x', 'y')",
            "UNIX_TIMESTAMP('x', 'y')",
            write="hive",
        )
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write="hive")

        self.validate("TIME_STR_TO_TIME(x)", "TIME_STR_TO_TIME(x)", write=None)
        self.validate("TIME_STR_TO_UNIX(x)", "TIME_STR_TO_UNIX(x)", write=None)
        self.validate("TIME_TO_TIME_STR(x)", "TIME_TO_TIME_STR(x)", write=None)
        self.validate("TIME_TO_STR(x, 'y')", "TIME_TO_STR(x, 'y')", write=None)
        self.validate("TIME_TO_UNIX(x)", "TIME_TO_UNIX(x)", write=None)
        self.validate("UNIX_TO_STR(x, 'y')", "UNIX_TO_STR(x, 'y')", write=None)
        self.validate("UNIX_TO_TIME(x)", "UNIX_TO_TIME(x)", write=None)
        self.validate("UNIX_TO_TIME_STR(x)", "UNIX_TO_TIME_STR(x)", write=None)
        self.validate("TIME_STR_TO_DATE(x)", "TIME_STR_TO_DATE(x)", write=None)

        self.validate("TIME_STR_TO_DATE(x)", "TO_DATE(x)", write="hive")
        self.validate(
            "UNIX_TO_STR(x, 'yyyy-MM-dd HH:mm:ss')", "FROM_UNIXTIME(x)", write="hive"
        )
        self.validate(
            "STR_TO_UNIX(x, 'yyyy-MM-dd HH:mm:ss')", "UNIX_TIMESTAMP(x)", write="hive"
        )
        self.validate("IF(x > 1, x + 1)", "IF(x > 1, x + 1)", write="presto")
        self.validate("IF(x > 1, 1 + 1)", "IF(x > 1, 1 + 1)", write="hive")
        self.validate("IF(x > 1, 1, 0)", "IF(x > 1, 1, 0)", write="hive")

        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(x)",
            write="hive",
        )
        self.validate("UNIX_TO_STR(123, 'y')", "FROM_UNIXTIME(123, 'y')", write="hive")
        self.validate(
            "UNIX_TO_TIME(123)",
            "FROM_UNIXTIME(123)",
            write="hive",
        )

        self.validate("STR_TO_TIME('x', 'y')", "DATE_PARSE('x', 'y')", write="presto")
        self.validate(
            "STR_TO_UNIX('x', 'y')", "TO_UNIXTIME(DATE_PARSE('x', 'y'))", write="presto"
        )
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write="presto")
        self.validate("TIME_TO_UNIX(x)", "TO_UNIXTIME(x)", write="presto")
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "DATE_FORMAT(FROM_UNIXTIME(123), 'y')",
            write="presto",
        )
        self.validate("UNIX_TO_TIME(123)", "FROM_UNIXTIME(123)", write="presto")

        self.validate("STR_TO_TIME('x', 'y')", "TO_TIMESTAMP('x', 'y')", write="spark")
        self.validate(
            "STR_TO_UNIX('x', 'y')", "UNIX_TIMESTAMP('x', 'y')", write="spark"
        )
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write="spark")

        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(x)",
            write="spark",
        )
        self.validate("UNIX_TO_STR(123, 'y')", "FROM_UNIXTIME(123, 'y')", write="spark")
        self.validate(
            "UNIX_TO_TIME(123)",
            "FROM_UNIXTIME(123)",
            write="spark",
        )
        self.validate(
            "CREATE TEMPORARY TABLE test AS SELECT 1",
            "CREATE TEMPORARY VIEW test AS SELECT 1",
            write="spark",
        )

    def test_identity(self):
        with open(
            os.path.join(self.fixtures_dir, "identity.sql"), encoding="utf-8"
        ) as f:
            for sql in f:
                self.assertEqual(transpile(sql)[0], sql.strip())

    def test_partial(self):
        with open(
            os.path.join(self.fixtures_dir, "partial.sql"), encoding="utf-8"
        ) as f:
            for sql in f:
                self.assertEqual(
                    transpile(sql, error_level=ErrorLevel.IGNORE)[0], sql.strip()
                )

    def test_pretty(self):
        with open(os.path.join(self.fixtures_dir, "pretty.sql"), encoding="utf-8") as f:
            lines = f.read().split(";")
            size = len(lines)

            for i in range(0, size, 2):
                if i + 1 < size:
                    sql = lines[i]
                    pretty = lines[i + 1].strip()
                    generated = transpile(sql, pretty=True)[0]
                    self.assertEqual(generated, pretty)
