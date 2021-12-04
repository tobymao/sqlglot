import unittest

import sqlglot.expressions as exp
from sqlglot import ErrorLevel, Parser, ParseError, parse, parse_one


class TestParser(unittest.TestCase):
    def test_column(self):
        columns = parse_one("select a, ARRAY[1] b, case when 1 then 1 end").find_all(
            exp.Column
        )
        assert len(list(columns)) == 1

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

        assert expression.args["expressions"][0].args["this"].args["this"] == "a"
        assert expression.args["expressions"][1].args["this"].args["this"] == "b"
        assert expression.args["expressions"][2].args["alias"].args["this"] == "c"
        assert expression.args["expressions"][3].args["alias"].args["this"] == "D"
        assert expression.args["expressions"][4].args["alias"].args["this"] == "y|z'"
        table = expression.args["from"].args["expressions"][0]
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
            expressions[0].args["from"].args["expressions"][0].args["this"].args["this"]
            == "a"
        )
        assert (
            expressions[1].args["from"].args["expressions"][0].args["this"].args["this"]
            == "b"
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

    def test_function_arguments_validation(self):
        with self.assertRaises(ParseError):
            parse_one("IF(a > 0, a, b, c)")

        with self.assertRaises(ParseError):
            parse_one("IF(a > 0)")
