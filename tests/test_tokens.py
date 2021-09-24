import unittest

from sqlglot import parse_one


class TestTokens(unittest.TestCase):
    def test_sql(self):
        token = parse_one('"y"').args["this"]
        assert token.text == "y"
        assert token.sql() == '"y"'
        assert token.sql(dialect="spark") == "`y`"

        token = parse_one("'y'")
        assert token.sql(quote="-") == "-y-"
