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

    def test_cache_table(self):
        expression = parse_one(
            "CACHE LAZY TABLE a.b OPTIONS('storageLevel' = 'some_value') AS SELECT x FROM y"
        )
        self.assertEqual(expression.args["options"][0].args["this"], "storageLevel")
        self.assertEqual(expression.args["options"][1].args["this"], "some_value")

    def test_insert_with_dynamic_partitioning(self):
        expression = parse_one(
            "INSERT OVERWRITE TABLE a.b PARTITION(ds) SELECT x FROM y"
        )
        self.assertEqual(expression.args["partition"][0].args["this"], "ds")

        expression = parse_one(
            "INSERT OVERWRITE TABLE a.b PARTITION(ds='YYYY-MM-DD') SELECT x FROM y"
        )
        partition_key, partition_value = expression.args["partition"][0]
        self.assertEqual(partition_key.args["this"], "ds")
        self.assertEqual(partition_value.args["this"], "YYYY-MM-DD")

        expression = parse_one(
            "INSERT OVERWRITE TABLE a.b PARTITION(ds, hour) SELECT x FROM y"
        )
        self.assertEqual(expression.args["partition"][0].args["this"], "ds")
        self.assertEqual(expression.args["partition"][1].args["this"], "hour")

        expression = parse_one(
            "INSERT OVERWRITE TABLE a.b PARTITION(ds='YYYY-MM-DD', hour='hh') SELECT x FROM y"
        )

        partition_key_1, partition_value_1 = expression.args["partition"][0]
        self.assertEqual(partition_key_1.args["this"], "ds")
        self.assertEqual(partition_value_1.args["this"], "YYYY-MM-DD")

        partition_key_2, partition_value_2 = expression.args["partition"][1]
        self.assertEqual(partition_key_2.args["this"], "hour")
        self.assertEqual(partition_value_2.args["this"], "hh")
