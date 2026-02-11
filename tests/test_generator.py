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

        sql = "SELECT SPECIAL_UDF(a) FROM x"
        tokens = Tokenizer().tokenize(sql)
        expression = NewParser().parse(tokens, sql)[0]
        self.assertEqual(expression.sql(), "SELECT SPECIAL_UDF(a) FROM x")

    def test_fallback_function_var_args_sql(self):
        class SpecialUDF(Func):
            arg_types = {"a": True, "expressions": False}
            is_var_len_args = True

        class NewParser(Parser):
            FUNCTIONS = SpecialUDF.default_parser_mappings()

        sql = "SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x"
        tokens = Tokenizer().tokenize(sql)
        expression = NewParser().parse(tokens, sql)[0]
        self.assertEqual(expression.sql(), sql)

        self.assertEqual(
            exp.DateTrunc(this=exp.to_column("event_date"), unit=exp.var("MONTH")).sql(),
            "DATE_TRUNC('MONTH', event_date)",
        )

    def test_identify(self):
        self.assertEqual(parse_one("x").sql(identify=True), '"x"')
        self.assertEqual(parse_one("x").sql(identify=False), "x")
        self.assertEqual(parse_one("X").sql(identify=True), '"X"')
        self.assertEqual(parse_one('"x"').sql(identify=False), '"x"')
        self.assertEqual(parse_one("x").sql(identify="safe"), '"x"')
        self.assertEqual(parse_one("X").sql(identify="safe"), "X")
        self.assertEqual(parse_one("x as 1").sql(identify="safe"), '"x" AS "1"')
        self.assertEqual(parse_one("X as 1").sql(identify="safe"), 'X AS "1"')

    def test_generate_nested_binary(self):
        sql = "SELECT 'foo'" + (" || 'foo'" * 1000)
        self.assertEqual(parse_one(sql).sql(copy=False), sql)

    def test_overlap_operator(self):
        for op in ("&<", "&>"):
            with self.subTest(op=op):
                input_sql = f"SELECT '[1,10]'::int4range {op} '[5,15]'::int4range"
                expected_sql = (
                    f"SELECT CAST('[1,10]' AS INT4RANGE) {op} CAST('[5,15]' AS INT4RANGE)"
                )
                ast = parse_one(input_sql, read="postgres")
                self.assertEqual(ast.sql(), expected_sql)
                self.assertEqual(ast.sql("postgres"), expected_sql)

    def test_pretty_nested_types(self):
        def assert_pretty_nested(
            datatype: exp.DataType,
            single_line: str,
            pretty: str,
            max_text_width: int = 10,
            **kwargs,
        ) -> None:
            self.assertEqual(datatype.sql(), single_line)
            self.assertEqual(
                datatype.sql(pretty=True, max_text_width=max_text_width, **kwargs), pretty
            )

        # STRUCT
        type_str = "STRUCT<a INT, b TEXT>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<\n  a INT,\n  b TEXT\n>",
        )

        # STRUCT - type def shorter than max text width so stays one line
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<a INT, b TEXT>",
            max_text_width=50,
        )

        # STRUCT, leading_comma = True
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<\n  a INT\n  , b TEXT\n>",
            leading_comma=True,
        )

        # ARRAY
        type_str = "ARRAY<DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "ARRAY<\n  DECIMAL(38, 9)\n>",
        )

        # ARRAY nested STRUCT
        type_str = "ARRAY<STRUCT<a INT, b TEXT>>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "ARRAY<\n  STRUCT<\n    a INT,\n    b TEXT\n  >\n>",
        )

        # RANGE
        type_str = "RANGE<DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "RANGE<\n  DECIMAL(38, 9)\n>",
        )

        # LIST
        type_str = "LIST<INT, INT, TEXT>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "LIST<\n  INT,\n  INT,\n  TEXT\n>",
        )

        # MAP
        type_str = "MAP<INT, DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "MAP<\n  INT,\n  DECIMAL(38, 9)\n>",
        )
