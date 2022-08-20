import os
import unittest
from unittest import mock

from sqlglot import parse_one, transpile
from sqlglot.errors import ErrorLevel, ParseError, UnsupportedError
from tests.helpers import (
    assert_logger_contains,
    load_sql_fixtures,
    load_sql_fixture_pairs,
)


class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures")
    maxDiff = None

    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

    def test_alias(self):
        for key in ("union", "filter", "over", "from", "join"):
            with self.subTest(f"alias {key}"):
                self.validate(f"SELECT x AS {key}", f"SELECT x AS {key}")
                self.validate(f'SELECT x "{key}"', f'SELECT x AS "{key}"')

                with self.assertRaises(ParseError):
                    self.validate(f"SELECT x {key}", "")

    def test_asc(self):
        self.validate("SELECT x FROM y ORDER BY x ASC", "SELECT x FROM y ORDER BY x")

    def test_paren(self):
        with self.assertRaises(ParseError):
            transpile("1 + (2 + 3")
            transpile("select f(")

    def test_some(self):
        self.validate(
            "SELECT * FROM x WHERE a = SOME (SELECT 1)",
            "SELECT * FROM x WHERE a = ANY (SELECT 1)",
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
        self.validate("a IS NOT NULL", "NOT a IS NULL")
        self.validate("a LIKE TEXT y", "a LIKE CAST(y AS TEXT)")

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
            "SELECT IF a > 1 THEN b END",
            "SELECT CASE WHEN a > 1 THEN b END",
        )
        self.validate(
            "SELECT IF a > 1 THEN b ELSE c END",
            "SELECT CASE WHEN a > 1 THEN b ELSE c END",
        )
        self.validate(
            "SELECT IF(a > 1, 1) FROM foo", "SELECT CASE WHEN a > 1 THEN 1 END FROM foo"
        )

    def test_ignore_nulls(self):
        self.validate("SELECT COUNT(x RESPECT NULLS)", "SELECT COUNT(x)")

    def test_time(self):
        self.validate("TIMESTAMP '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMP)")
        self.validate(
            "TIMESTAMP WITH TIME ZONE '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMPTZ)"
        )
        self.validate(
            "TIMESTAMP(9) WITH TIME ZONE '2020-01-01'",
            "CAST('2020-01-01' AS TIMESTAMPTZ(9))",
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
        self.validate(
            "timestamp with time zone '2025-11-20 00:00:00+00' AT TIME ZONE 'Africa/Cairo'",
            "CAST('2025-11-20 00:00:00+00' AS TIMESTAMPTZ) AT TIME ZONE 'Africa/Cairo'",
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

    @mock.patch("sqlglot.helper.logger")
    def test_index_offset(self, mock_logger):
        self.validate("x[0]", "x[1]", write="presto", identity=False)
        self.validate("x[1]", "x[0]", read="presto", identity=False)
        mock_logger.warning.assert_any_call("Applying array index offset (%s)", 1)
        mock_logger.warning.assert_any_call("Applying array index offset (%s)", -1)

    def test_identity(self):
        self.assertEqual(transpile("")[0], "")
        for sql in load_sql_fixtures("identity.sql"):
            with self.subTest(sql):
                self.assertEqual(transpile(sql)[0], sql.strip())

    def test_partial(self):
        for sql in load_sql_fixtures("partial.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    transpile(sql, error_level=ErrorLevel.IGNORE)[0], sql.strip()
                )

    def test_pretty(self):
        for _, sql, pretty in load_sql_fixture_pairs("pretty.sql"):
            with self.subTest(sql[:100]):
                generated = transpile(sql, pretty=True)[0]
                self.assertEqual(generated, pretty)
                self.assertEqual(parse_one(sql), parse_one(pretty))

    @mock.patch("sqlglot.parser.logger")
    def test_error_level(self, logger):
        invalid = "x + 1. ("
        errors = [
            "Required keyword: 'expressions' missing for <class 'sqlglot.expressions.Aliases'>. Line 1, Col: 8.\n  x + 1. \033[4m(\033[0m",
            "Expecting ). Line 1, Col: 8.\n  x + 1. \033[4m(\033[0m",
        ]

        transpile(invalid, error_level=ErrorLevel.WARN)
        for error in errors:
            assert_logger_contains(error, logger)

        with self.assertRaises(ParseError) as ctx:
            transpile(invalid, error_level=ErrorLevel.IMMEDIATE)
        self.assertEqual(str(ctx.exception), errors[0])

        with self.assertRaises(ParseError) as ctx:
            transpile(invalid, error_level=ErrorLevel.RAISE)
        self.assertEqual(str(ctx.exception), "\n\n".join(errors))

        more_than_max_errors = "(((("
        expected = (
            "Expecting ). Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "Required keyword: 'this' missing for <class 'sqlglot.expressions.Paren'>. Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "Expecting ). Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "... and 2 more"
        )
        with self.assertRaises(ParseError) as ctx:
            transpile(more_than_max_errors, error_level=ErrorLevel.RAISE)
        self.assertEqual(str(ctx.exception), expected)

    @mock.patch("sqlglot.generator.logger")
    def test_unsupported_level(self, logger):
        def unsupported(level):
            transpile(
                "SELECT MAP(a, b), MAP(a, b), MAP(a, b), MAP(a, b)",
                read="presto",
                write="hive",
                unsupported_level=level,
            )

        error = "Cannot convert array columns into map use SparkSQL instead."

        unsupported(ErrorLevel.WARN)
        assert_logger_contains("\n".join([error] * 4), logger, level="warning")

        with self.assertRaises(UnsupportedError) as ctx:
            unsupported(ErrorLevel.RAISE)
        self.assertEqual(str(ctx.exception).count(error), 3)

        with self.assertRaises(UnsupportedError) as ctx:
            unsupported(ErrorLevel.IMMEDIATE)
        self.assertEqual(str(ctx.exception).count(error), 1)
