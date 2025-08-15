import unittest

from sqlglot import exp, parse_one
from sqlglot.expressions import Func
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer


class TestGenerator(unittest.TestCase):
    def test_fallback_function_sql(self):
        class SpecialUDF(Func):
            arg_types = {"a": True, "b": False}

        class NewParser(Parser):
            FUNCTIONS = SpecialUDF.default_parser_mappings()

        tokens = Tokenizer().tokenize("SELECT SPECIAL_UDF(a) FROM x")
        expression = NewParser().parse(tokens)[0]
        self.assertEqual(expression.sql(), "SELECT SPECIAL_UDF(a) FROM x")

    def test_fallback_function_var_args_sql(self):
        class SpecialUDF(Func):
            arg_types = {"a": True, "expressions": False}
            is_var_len_args = True

        class NewParser(Parser):
            FUNCTIONS = SpecialUDF.default_parser_mappings()

        tokens = Tokenizer().tokenize("SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x")
        expression = NewParser().parse(tokens)[0]
        self.assertEqual(expression.sql(), "SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x")

        self.assertEqual(
            exp.DateTrunc(this=exp.to_column("event_date"), unit=exp.var("MONTH")).sql(),
            "DATE_TRUNC('MONTH', event_date)",
        )

    def test_identify(self):
        assert parse_one("x").sql(identify=True) == '"x"'
        assert parse_one("x").sql(identify="always") == '"x"'
        assert parse_one("X").sql(identify="always") == '"X"'
        assert parse_one("x").sql(identify="safe") == '"x"'
        assert parse_one("X").sql(identify="safe") == "X"
        assert parse_one("x as 1").sql(identify="safe") == '"x" AS "1"'
        assert parse_one("X as 1").sql(identify="safe") == 'X AS "1"'

    def test_generate_nested_binary(self):
        sql = "SELECT 'foo'" + (" || 'foo'" * 1000)
        self.assertEqual(parse_one(sql).sql(copy=False), sql)
