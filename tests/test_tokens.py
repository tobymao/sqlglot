import unittest

from sqlglot import parse_one
from sqlglot.tokens import Token


class TestTokens(unittest.TestCase):
    maxDiff = None

    def test_eq(self):
        self.assertEqual(Token.number(1), parse_one("1"))
        self.assertEqual(Token.string("1"), parse_one("'1'"))
        self.assertEqual(Token.number(1.0), parse_one("1.0"))
        self.assertEqual(Token.identifier("x"), parse_one('"x"').args["this"])
        self.assertEqual(
            parse_one("`x`", read="spark").args["this"], parse_one('a."x"').args["this"]
        )
        self.assertNotEqual(Token.var("a"), parse_one("A"))
        self.assertNotEqual(Token.string("1"), parse_one("1"))
        self.assertNotEqual(Token.string("a"), parse_one("'A'"))

    def test_hash(self):
        self.assertEqual(
            {
                Token.number(1): "a",
                Token.string("x"): "b",
                Token.identifier("x"): "c",
                Token.var("x"): "d",
            },
            {
                parse_one("1"): "a",
                parse_one("'x'"): "b",
                parse_one('"x"').args["this"]: "c",
                parse_one("x").args["this"]: "d",
            },
        )

    def test_sql(self):
        token = parse_one('"y"').args["this"]
        assert token.text == "y"
        assert token.sql() == '"y"'
        assert token.sql(dialect="spark") == "`y`"

        token = parse_one("'y'")
        assert token.sql(quote="-") == "-y-"
