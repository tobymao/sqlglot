import unittest
from unittest.mock import patch

from sqlglot import parse, parse_one, exp, Parser
from sqlglot.errors import ErrorLevel, ParseError
from tests.helpers import assert_logger_contains


class TestParser(unittest.TestCase):
    def test_parse_empty(self):
        self.assertIsNone(parse_one(""))

    def test_parse_into(self):
        self.assertIsInstance(parse_one("left join foo", into=exp.Join), exp.Join)
        self.assertIsInstance(parse_one("int", into=exp.DataType), exp.DataType)
        self.assertIsInstance(parse_one("array<int>", into=exp.DataType), exp.DataType)

    def test_column(self):
        columns = parse_one("select a, ARRAY[1] b, case when 1 then 1 end").find_all(
            exp.Column
        )
        assert len(list(columns)) == 1

        self.assertIsNotNone(parse_one("date").find(exp.Column))

    def test_table(self):
        tables = [
            t.sql() for t in parse_one("select * from a, b.c, .d").find_all(exp.Table)
        ]
        self.assertEqual(tables, ["a", "b.c", "d"])

    def test_select(self):
        self.assertIsNotNone(
            parse_one("select * from (select 1) x order by x.y").args["order"]
        )
        self.assertEqual(
            len(parse_one("select * from (select 1) x cross join y").args["joins"]), 1
        )

    def test_command(self):
        expressions = parse("SET x = 1; ADD JAR s3://a; SELECT 1")
        self.assertEqual(len(expressions), 3)
        self.assertEqual(expressions[0].sql(), "SET x = 1")
        self.assertEqual(expressions[1].sql(), "ADD JAR s3://a")
        self.assertEqual(expressions[2].sql(), "SELECT 1")

    def test_identify(self):
        expression = parse_one(
            """
            SELECT a, "b", c AS c, d AS "D", e AS "y|z'"
            FROM y."z"
        """
        )

        assert expression.expressions[0].text("this") == "a"
        assert expression.expressions[1].text("this") == "b"
        assert expression.expressions[2].text("alias") == "c"
        assert expression.expressions[3].text("alias") == "D"
        assert expression.expressions[4].text("alias") == "y|z'"
        table = expression.args["from"].expressions[0]
        assert table.args["this"].args["this"] == "z"
        assert table.args["db"].args["this"] == "y"

    def test_multi(self):
        expressions = parse(
            """
            SELECT * FROM a; SELECT * FROM b;
        """
        )

        assert len(expressions) == 2
        assert (
            expressions[0].args["from"].expressions[0].args["this"].args["this"] == "a"
        )
        assert (
            expressions[1].args["from"].expressions[0].args["this"].args["this"] == "b"
        )

    def test_expression(self):
        ignore = Parser(error_level=ErrorLevel.IGNORE)
        self.assertIsInstance(ignore.expression(exp.Hint, expressions=[""]), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint, y=""), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint), exp.Hint)

        default = Parser()
        self.assertIsInstance(default.expression(exp.Hint, expressions=[""]), exp.Hint)
        default.expression(exp.Hint, y="")
        default.expression(exp.Hint)
        self.assertEqual(len(default.errors), 3)

        warn = Parser(error_level=ErrorLevel.WARN)
        warn.expression(exp.Hint, y="")
        assert isinstance(warn.errors[0], ParseError)

    def test_parse_errors(self):
        with self.assertRaises(ParseError):
            parse_one("IF(a > 0, a, b, c)")

        with self.assertRaises(ParseError):
            parse_one("IF(a > 0)")

        with self.assertRaises(ParseError):
            parse_one("WITH cte AS (SELECT * FROM x)")

    def test_space(self):
        # self.assertEqual(
        #    parse_one("SELECT ROW() OVER(PARTITION  BY x) FROM x GROUP  BY y").sql(),
        #    "SELECT ROW() OVER (PARTITION BY x) FROM x GROUP BY y",
        # )

        self.assertEqual(
            parse_one(
                """SELECT   * FROM x GROUP
                BY y"""
            ).sql(),
            "SELECT * FROM x GROUP BY y",
        )

    def test_missing_by(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT FROM x ORDER BY")

    def test_annotations(self):
        expression = parse_one(
            """
            SELECT
                a #annotation1,
                b as B #annotation2:testing ,
                "test#annotation",c#annotation3, d #annotation4,
                e #
            FROM foo
        """
        )

        assert expression.expressions[0].name == "annotation1"
        assert expression.expressions[1].name == "annotation2:testing"
        assert expression.expressions[2].name == "test#annotation"
        assert expression.expressions[3].name == "annotation3"
        assert expression.expressions[4].name == "annotation4"
        assert expression.expressions[5].name == ""

    def test_pretty_config_override(self):
        self.assertEqual(parse_one("SELECT col FROM x").sql(), "SELECT col FROM x")
        with patch("sqlglot.pretty", True):
            self.assertEqual(
                parse_one("SELECT col FROM x").sql(), "SELECT\n  col\nFROM x"
            )

        self.assertEqual(
            parse_one("SELECT col FROM x").sql(pretty=True), "SELECT\n  col\nFROM x"
        )

    @patch("sqlglot.parser.logger")
    def test_comment_error_n(self, logger):
        parse_one(
            """CREATE TABLE x
(
-- test
)""",
            error_level=ErrorLevel.WARN,
        )

        assert_logger_contains(
            "Required keyword: 'expressions' missing for <class 'sqlglot.expressions.Schema'>. Line 4, Col: 1.",
            logger,
        )

    @patch("sqlglot.parser.logger")
    def test_comment_error_r(self, logger):
        parse_one(
            """CREATE TABLE x (-- test\r)""",
            error_level=ErrorLevel.WARN,
        )

        assert_logger_contains(
            "Required keyword: 'expressions' missing for <class 'sqlglot.expressions.Schema'>. Line 2, Col: 1.",
            logger,
        )
