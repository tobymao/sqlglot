import os
import unittest
from unittest import mock

from sqlglot import parse_one, transpile
from sqlglot.errors import ErrorLevel, ParseError, UnsupportedError
from tests.helpers import (
    assert_logger_contains,
    load_sql_fixture_pairs,
    load_sql_fixtures,
)


class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures")
    maxDiff = None

    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

    def test_alias(self):
        self.assertEqual(transpile("SELECT SUM(y) KEEP")[0], "SELECT SUM(y) AS KEEP")
        self.assertEqual(transpile("SELECT 1 overwrite")[0], "SELECT 1 AS overwrite")
        self.assertEqual(transpile("SELECT 1 is")[0], "SELECT 1 AS is")
        self.assertEqual(transpile("SELECT 1 current_time")[0], "SELECT 1 AS current_time")
        self.assertEqual(
            transpile("SELECT 1 current_timestamp")[0], "SELECT 1 AS current_timestamp"
        )
        self.assertEqual(transpile("SELECT 1 current_date")[0], "SELECT 1 AS current_date")
        self.assertEqual(transpile("SELECT 1 current_datetime")[0], "SELECT 1 AS current_datetime")
        self.assertEqual(transpile("SELECT 1 row")[0], "SELECT 1 AS row")

        self.assertEqual(
            transpile("SELECT 1 FROM a.b.table1 t UNPIVOT((c3) FOR c4 IN (a, b))")[0],
            "SELECT 1 FROM a.b.table1 AS t UNPIVOT((c3) FOR c4 IN (a, b))",
        )

        for key in ("union", "over", "from", "join"):
            with self.subTest(f"alias {key}"):
                self.validate(f"SELECT x AS {key}", f"SELECT x AS {key}")
                self.validate(f'SELECT x "{key}"', f'SELECT x AS "{key}"')

                with self.assertRaises(ParseError):
                    self.validate(f"SELECT x {key}", "")

    def test_asc(self):
        self.validate("SELECT x FROM y ORDER BY x ASC", "SELECT x FROM y ORDER BY x")

    def test_unary(self):
        self.validate("+++1", "1")
        self.validate("+-1", "-1")
        self.validate("+- - -1", "- - -1")

    def test_paren(self):
        with self.assertRaises(ParseError):
            transpile("1 + (2 + 3")
            transpile("select f(")

    def test_some(self):
        self.validate(
            "SELECT * FROM x WHERE a = SOME (SELECT 1)",
            "SELECT * FROM x WHERE a = ANY (SELECT 1)",
        )

    def test_leading_comma(self):
        self.validate(
            "SELECT FOO, BAR, BAZ",
            "SELECT\n    FOO\n  , BAR\n  , BAZ",
            leading_comma=True,
            pretty=True,
        )
        self.validate(
            "SELECT FOO, /*x*/\nBAR, /*y*/\nBAZ",
            "SELECT\n    FOO /* x */\n  , BAR /* y */\n  , BAZ",
            leading_comma=True,
            pretty=True,
        )
        # without pretty, this should be a no-op
        self.validate(
            "SELECT FOO, BAR, BAZ",
            "SELECT FOO, BAR, BAZ",
            leading_comma=True,
        )

    def test_space(self):
        self.validate("SELECT MIN(3)>MIN(2)", "SELECT MIN(3) > MIN(2)")
        self.validate("SELECT MIN(3)>=MIN(2)", "SELECT MIN(3) >= MIN(2)")
        self.validate("SELECT 1>0", "SELECT 1 > 0")
        self.validate("SELECT 3>=3", "SELECT 3 >= 3")

    def test_comments(self):
        self.validate("SELECT 1 /*/2 */", "SELECT 1 /* /2 */")
        self.validate("SELECT */*comment*/", "SELECT * /* comment */")
        self.validate(
            "SELECT * FROM table /*comment 1*/ /*comment 2*/",
            "SELECT * FROM table /* comment 1 */ /* comment 2 */",
        )
        self.validate("SELECT 1 FROM foo -- comment", "SELECT 1 FROM foo /* comment */")
        self.validate("SELECT --+5\nx FROM foo", "/* +5 */ SELECT x FROM foo")
        self.validate("SELECT --!5\nx FROM foo", "/* !5 */ SELECT x FROM foo")
        self.validate(
            "SELECT 1 /* inline */ FROM foo -- comment",
            "SELECT 1 /* inline */ FROM foo /* comment */",
        )
        self.validate(
            "SELECT FUN(x) /*x*/, [1,2,3] /*y*/", "SELECT FUN(x) /* x */, ARRAY(1, 2, 3) /* y */"
        )
        self.validate(
            """
            SELECT 1 -- comment
            FROM foo -- comment
            """,
            "SELECT 1 /* comment */ FROM foo /* comment */",
        )
        self.validate(
            """
            SELECT 1 /* big comment
             like this */
            FROM foo -- comment
            """,
            """SELECT 1 /* big comment
             like this */ FROM foo /* comment */""",
        )
        self.validate(
            "select x from foo --       x",
            "SELECT x FROM foo /*       x */",
        )
        self.validate(
            """select x, --
            from foo""",
            "SELECT x FROM foo",
        )
        self.validate(
            """
-- comment 1
-- comment 2
-- comment 3
SELECT * FROM foo
            """,
            "/* comment 1 */ /* comment 2 */ /* comment 3 */ SELECT * FROM foo",
        )
        self.validate(
            """
-- comment 1
-- comment 2
-- comment 3
SELECT * FROM foo""",
            """/* comment 1 */
/* comment 2 */
/* comment 3 */
SELECT
  *
FROM foo""",
            pretty=True,
        )
        self.validate(
            """
SELECT * FROM tbl /*line1
line2
line3*/ /*another comment*/ where 1=1 -- comment at the end""",
            """SELECT * FROM tbl /* line1
line2
line3 */ /* another comment */ WHERE 1 = 1 /* comment at the end */""",
        )
        self.validate(
            """
SELECT * FROM tbl /*line1
line2
line3*/ /*another comment*/ where 1=1 -- comment at the end""",
            """SELECT
  *
FROM tbl /* line1
line2
line3 */
/* another comment */
WHERE
  1 = 1 /* comment at the end */""",
            pretty=True,
        )
        self.validate(
            """
            /* multi
               line
               comment
            */
            SELECT
              tbl.cola /* comment 1 */ + tbl.colb /* comment 2 */,
              CAST(x AS CHAR), # comment 3
              y               -- comment 4
            FROM
              bar /* comment 5 */,
              tbl #          comment 6
            """,
            """/* multi
               line
               comment
            */
SELECT
  tbl.cola /* comment 1 */ + tbl.colb /* comment 2 */,
  CAST(x AS CHAR), /* comment 3 */
  y /* comment 4 */
FROM bar /* comment 5 */, tbl /*          comment 6 */""",
            read="mysql",
            pretty=True,
        )
        self.validate(
            """
            SELECT a FROM b
            WHERE foo
            -- comment 1
            AND bar
            -- comment 2
            AND bla
            -- comment 3
            LIMIT 10
            ;
            """,
            "SELECT a FROM b WHERE foo AND /* comment 1 */ bar AND /* comment 2 */ bla LIMIT 10 /* comment 3 */",
        )
        self.validate(
            """
            SELECT a FROM b WHERE foo
            -- comment 1
            """,
            "SELECT a FROM b WHERE foo /* comment 1 */",
        )
        self.validate(
            """
            select a
            -- from
            from b
            -- where
            where foo
            -- comment 1
            and bar
            -- comment 2
            and bla
            """,
            """SELECT
  a
/* from */
FROM b
/* where */
WHERE
  foo /* comment 1 */ AND bar AND bla /* comment 2 */""",
            pretty=True,
        )
        self.validate(
            """
            -- test
            WITH v AS (
              SELECT
                1 AS literal
            )
            SELECT
              *
            FROM v
            """,
            """/* test */
WITH v AS (
  SELECT
    1 AS literal
)
SELECT
  *
FROM v""",
            pretty=True,
        )
        self.validate(
            "(/* 1 */ 1 ) /* 2 */",
            "(1) /* 1 */ /* 2 */",
        )
        self.validate(
            "select * from t where not a in (23) /*test*/ and b in (14)",
            "SELECT * FROM t WHERE NOT a IN (23) /* test */ AND b IN (14)",
        )
        self.validate(
            "select * from t where a in (23) /*test*/ and b in (14)",
            "SELECT * FROM t WHERE a IN (23) /* test */ AND b IN (14)",
        )
        self.validate(
            "select * from t where ((condition = 1)/*test*/)",
            "SELECT * FROM t WHERE ((condition = 1) /* test */)",
        )
        self.validate(
            "SELECT 1 // hi this is a comment",
            "SELECT 1 /* hi this is a comment */",
            read="snowflake",
        )
        self.validate(
            "-- comment\nDROP TABLE IF EXISTS foo",
            "/* comment */ DROP TABLE IF EXISTS foo",
        )
        self.validate(
            """
            -- comment1
            -- comment2

            -- comment3
            DROP TABLE IF EXISTS db.tba
            """,
            """/* comment1 */
/* comment2 */
/* comment3 */
DROP TABLE IF EXISTS db.tba""",
            pretty=True,
        )
        self.validate(
            """
            CREATE TABLE db.tba AS
            SELECT a, b, c
            FROM tb_01
            WHERE
            -- comment5
              a = 1 AND b = 2 --comment6
              -- and c = 1
            -- comment7
            """,
            """CREATE TABLE db.tba AS
SELECT
  a,
  b,
  c
FROM tb_01
WHERE
  a /* comment5 */ = 1 AND b = 2 /* comment6 */
  /* and c = 1 */
  /* comment7 */""",
            pretty=True,
        )

    def test_types(self):
        self.validate("INT 1", "CAST(1 AS INT)")
        self.validate("VARCHAR 'x' y", "CAST('x' AS VARCHAR) AS y")
        self.validate("STRING 'x' y", "CAST('x' AS TEXT) AS y")
        self.validate("x::INT", "CAST(x AS INT)")
        self.validate("x::INTEGER", "CAST(x AS INT)")
        self.validate("x::INT y", "CAST(x AS INT) AS y")
        self.validate("x::INT AS y", "CAST(x AS INT) AS y")
        self.validate("x::INT::BOOLEAN", "CAST(CAST(x AS INT) AS BOOLEAN)")
        self.validate("CAST(x::INT AS BOOLEAN)", "CAST(CAST(x AS INT) AS BOOLEAN)")
        self.validate("CAST(x AS INT)::BOOLEAN", "CAST(CAST(x AS INT) AS BOOLEAN)")

        with self.assertRaises(ParseError):
            transpile("x::z")

    def test_not_range(self):
        self.validate("a NOT LIKE b", "NOT a LIKE b")
        self.validate("a NOT BETWEEN b AND c", "NOT a BETWEEN b AND c")
        self.validate("a NOT IN (1, 2)", "NOT a IN (1, 2)")
        self.validate("a IS NOT NULL", "NOT a IS NULL")
        self.validate("a LIKE TEXT 'y'", "a LIKE CAST('y' AS TEXT)")

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
        self.validate("extract(week from current_date + 2)", "EXTRACT(week FROM CURRENT_DATE + 2)")
        self.validate(
            "EXTRACT(minute FROM datetime1 - datetime2)",
            "EXTRACT(minute FROM datetime1 - datetime2)",
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
        self.validate("SELECT IF(a > 1, 1) FROM foo", "SELECT CASE WHEN a > 1 THEN 1 END FROM foo")

    def test_with(self):
        self.validate(
            "WITH a AS (SELECT 1) WITH b AS (SELECT 2) SELECT *",
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT *",
        )
        self.validate(
            "WITH a AS (SELECT 1), WITH b AS (SELECT 2) SELECT *",
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT *",
        )
        self.validate(
            "WITH A(filter) AS (VALUES 1, 2, 3) SELECT * FROM A WHERE filter >= 2",
            "WITH A(filter) AS (VALUES (1), (2), (3)) SELECT * FROM A WHERE filter >= 2",
        )
        self.validate(
            "SELECT BOOL_OR(a > 10) FROM (VALUES 1, 2, 15) AS T(a)",
            "SELECT BOOL_OR(a > 10) FROM (VALUES (1), (2), (15)) AS T(a)",
            write="presto",
        )

    def test_alter(self):
        self.validate(
            "ALTER TABLE integers ADD k INTEGER",
            "ALTER TABLE integers ADD COLUMN k INT",
        )
        self.validate(
            "ALTER TABLE integers ALTER i SET DATA TYPE VARCHAR",
            "ALTER TABLE integers ALTER COLUMN i TYPE VARCHAR",
        )
        self.validate(
            "ALTER TABLE integers ALTER i TYPE VARCHAR COLLATE foo USING bar",
            "ALTER TABLE integers ALTER COLUMN i TYPE VARCHAR COLLATE foo USING bar",
        )

    def test_time(self):
        self.validate("INTERVAL '1 day'", "INTERVAL '1' day")
        self.validate("INTERVAL '1 days' * 5", "INTERVAL '1' days * 5")
        self.validate("5 * INTERVAL '1 day'", "5 * INTERVAL '1' day")
        self.validate("INTERVAL 1 day", "INTERVAL '1' day")
        self.validate("INTERVAL 2 months", "INTERVAL '2' months")
        self.validate("TIMESTAMP '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMP)")
        self.validate("TIMESTAMP WITH TIME ZONE '2020-01-01'", "CAST('2020-01-01' AS TIMESTAMPTZ)")
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
        self.validate("STR_TO_UNIX('x', 'y')", "EPOCH(STRPTIME('x', 'y'))", write="duckdb")
        self.validate("TIME_TO_STR(x, 'y')", "STRFTIME(x, 'y')", write="duckdb")
        self.validate("TIME_TO_UNIX(x)", "EPOCH(x)", write="duckdb")
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "STRFTIME(TO_TIMESTAMP(123), 'y')",
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME(123)",
            "TO_TIMESTAMP(123)",
            write="duckdb",
        )

        self.validate(
            "STR_TO_TIME(x, 'y')",
            "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'y')) AS TIMESTAMP)",
            write="hive",
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy-MM-dd HH:mm:ss')",
            "CAST(x AS TIMESTAMP)",
            write="hive",
        )
        self.validate(
            "STR_TO_TIME(x, 'yyyy-MM-dd')",
            "CAST(x AS TIMESTAMP)",
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
        self.validate("TIME_TO_TIME_STR(x)", "CAST(x AS TEXT)", write=None)
        self.validate("TIME_TO_STR(x, 'y')", "TIME_TO_STR(x, 'y')", write=None)
        self.validate("TIME_TO_UNIX(x)", "TIME_TO_UNIX(x)", write=None)
        self.validate("UNIX_TO_STR(x, 'y')", "UNIX_TO_STR(x, 'y')", write=None)
        self.validate("UNIX_TO_TIME(x)", "UNIX_TO_TIME(x)", write=None)
        self.validate("UNIX_TO_TIME_STR(x)", "UNIX_TO_TIME_STR(x)", write=None)
        self.validate("TIME_STR_TO_DATE(x)", "TIME_STR_TO_DATE(x)", write=None)

        self.validate("TIME_STR_TO_DATE(x)", "TO_DATE(x)", write="hive")
        self.validate("UNIX_TO_STR(x, 'yyyy-MM-dd HH:mm:ss')", "FROM_UNIXTIME(x)", write="hive")
        self.validate("STR_TO_UNIX(x, 'yyyy-MM-dd HH:mm:ss')", "UNIX_TIMESTAMP(x)", write="hive")
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
        self.validate("STR_TO_UNIX('x', 'y')", "TO_UNIXTIME(DATE_PARSE('x', 'y'))", write="presto")
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write="presto")
        self.validate("TIME_TO_UNIX(x)", "TO_UNIXTIME(x)", write="presto")
        self.validate(
            "UNIX_TO_STR(123, 'y')",
            "DATE_FORMAT(FROM_UNIXTIME(123), 'y')",
            write="presto",
        )
        self.validate("UNIX_TO_TIME(123)", "FROM_UNIXTIME(123)", write="presto")

        self.validate("STR_TO_TIME('x', 'y')", "TO_TIMESTAMP('x', 'y')", write="spark")
        self.validate("STR_TO_UNIX('x', 'y')", "UNIX_TIMESTAMP('x', 'y')", write="spark")
        self.validate("TIME_TO_STR(x, 'y')", "DATE_FORMAT(x, 'y')", write="spark")

        self.validate(
            "TIME_TO_UNIX(x)",
            "UNIX_TIMESTAMP(x)",
            write="spark",
        )
        self.validate("UNIX_TO_STR(123, 'y')", "FROM_UNIXTIME(123, 'y')", write="spark")
        self.validate(
            "UNIX_TO_TIME(123)",
            "CAST(FROM_UNIXTIME(123) AS TIMESTAMP)",
            write="spark",
        )
        self.validate(
            "CREATE TEMPORARY TABLE test AS SELECT 1",
            "CREATE TEMPORARY VIEW test AS SELECT 1",
            write="spark",
        )

    @mock.patch("sqlglot.helper.logger")
    def test_index_offset(self, logger):
        self.validate("x[0]", "x[1]", write="presto", identity=False)
        self.validate("x[1]", "x[0]", read="presto", identity=False)
        logger.warning.assert_any_call("Applying array index offset (%s)", 1)
        logger.warning.assert_any_call("Applying array index offset (%s)", -1)

        self.validate("x[x - 1]", "x[x - 1]", write="presto", identity=False)
        self.validate(
            "x[array_size(y) - 1]", "x[CARDINALITY(y) - 1 + 1]", write="presto", identity=False
        )
        self.validate("x[3 - 1]", "x[3]", write="presto", identity=False)
        self.validate("MAP(a, b)[0]", "MAP(a, b)[0]", write="presto", identity=False)

    def test_identify_lambda(self):
        self.validate("x(y -> y)", 'X("y" -> "y")', identify=True)

    def test_identity(self):
        self.assertEqual(transpile("")[0], "")
        for sql in load_sql_fixtures("identity.sql"):
            with self.subTest(sql):
                self.assertEqual(transpile(sql)[0], sql.strip())

    def test_normalize_name(self):
        self.assertEqual(
            transpile("cardinality(x)", read="presto", write="presto", normalize_functions="lower")[
                0
            ],
            "cardinality(x)",
        )

    def test_partial(self):
        for sql in load_sql_fixtures("partial.sql"):
            with self.subTest(sql):
                self.assertEqual(transpile(sql, error_level=ErrorLevel.IGNORE)[0], sql.strip())

    def test_pretty(self):
        for _, sql, pretty in load_sql_fixture_pairs("pretty.sql"):
            with self.subTest(sql[:100]):
                generated = transpile(sql, pretty=True)[0]
                self.assertEqual(generated, pretty)
                self.assertEqual(parse_one(sql), parse_one(pretty))

    def test_pretty_line_breaks(self):
        self.assertEqual(transpile("SELECT '1\n2'", pretty=True)[0], "SELECT\n  '1\n2'")

    @mock.patch("sqlglot.parser.logger")
    def test_error_level(self, logger):
        invalid = "x + 1. ("
        expected_messages = [
            "Required keyword: 'expressions' missing for <class 'sqlglot.expressions.Aliases'>. Line 1, Col: 8.\n  x + 1. \033[4m(\033[0m",
            "Expecting ). Line 1, Col: 8.\n  x + 1. \033[4m(\033[0m",
        ]
        expected_errors = [
            {
                "description": "Required keyword: 'expressions' missing for <class 'sqlglot.expressions.Aliases'>",
                "line": 1,
                "col": 8,
                "start_context": "x + 1. ",
                "highlight": "(",
                "end_context": "",
                "into_expression": None,
            },
            {
                "description": "Expecting )",
                "line": 1,
                "col": 8,
                "start_context": "x + 1. ",
                "highlight": "(",
                "end_context": "",
                "into_expression": None,
            },
        ]

        transpile(invalid, error_level=ErrorLevel.WARN)
        for error in expected_messages:
            assert_logger_contains(error, logger)

        with self.assertRaises(ParseError) as ctx:
            transpile(invalid, error_level=ErrorLevel.IMMEDIATE)

        self.assertEqual(str(ctx.exception), expected_messages[0])
        self.assertEqual(ctx.exception.errors[0], expected_errors[0])

        with self.assertRaises(ParseError) as ctx:
            transpile(invalid, error_level=ErrorLevel.RAISE)

        self.assertEqual(str(ctx.exception), "\n\n".join(expected_messages))
        self.assertEqual(ctx.exception.errors, expected_errors)

        more_than_max_errors = "(((("
        expected_messages = (
            "Required keyword: 'this' missing for <class 'sqlglot.expressions.Paren'>. Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "Expecting ). Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "Expecting ). Line 1, Col: 4.\n  (((\033[4m(\033[0m\n\n"
            "... and 2 more"
        )
        expected_errors = [
            {
                "description": "Required keyword: 'this' missing for <class 'sqlglot.expressions.Paren'>",
                "line": 1,
                "col": 4,
                "start_context": "(((",
                "highlight": "(",
                "end_context": "",
                "into_expression": None,
            },
            {
                "description": "Expecting )",
                "line": 1,
                "col": 4,
                "start_context": "(((",
                "highlight": "(",
                "end_context": "",
                "into_expression": None,
            },
        ]
        # Also expect three trailing structured errors that match the first
        expected_errors += [expected_errors[1]] * 3

        with self.assertRaises(ParseError) as ctx:
            transpile(more_than_max_errors, error_level=ErrorLevel.RAISE)

        self.assertEqual(str(ctx.exception), expected_messages)
        self.assertEqual(ctx.exception.errors, expected_errors)

    @mock.patch("sqlglot.generator.logger")
    def test_unsupported_level(self, logger):
        def unsupported(level):
            transpile(
                "SELECT MAP(a, b), MAP(a, b), MAP(a, b), MAP(a, b)",
                read="presto",
                write="hive",
                unsupported_level=level,
            )

        error = "Cannot convert array columns into map."

        unsupported(ErrorLevel.WARN)
        assert_logger_contains("\n".join([error] * 4), logger, level="warning")

        with self.assertRaises(UnsupportedError) as ctx:
            unsupported(ErrorLevel.RAISE)
        self.assertEqual(str(ctx.exception).count(error), 3)

        with self.assertRaises(UnsupportedError) as ctx:
            unsupported(ErrorLevel.IMMEDIATE)
        self.assertEqual(str(ctx.exception).count(error), 1)
