import unittest

from sqlglot import parse_one
from sqlglot.tokens import Token


class TestTokens(unittest.TestCase):
    maxDiff = None

    def test_eq(self):
        self.assertEqual(Token.number(1), parse_one("1").token)
        self.assertEqual(Token.string("1"), parse_one("'1'").token)
        self.assertEqual(Token.number(1.0), parse_one("1.0").token)
        self.assertEqual(Token.identifier("x", col=4), parse_one('"x"').args["this"])
        self.assertNotEqual(
            parse_one("`x`", read="spark").args["this"], parse_one('a."x"').args["this"]
        )
        self.assertNotEqual(Token.var("a"), parse_one("A"))
        self.assertNotEqual(Token.string("1"), parse_one("1").token)
        self.assertNotEqual(Token.string("a", col=4), parse_one("'A'").token)

    def test_hash(self):
        self.assertEqual(
            {
                Token.number(1): "a",
                Token.string("x", col=4): "b",
                Token.identifier("x", col=4): "c",
                Token.var("x"): "d",
            },
            {
                parse_one("1").token: "a",
                parse_one("'x'").token: "b",
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
