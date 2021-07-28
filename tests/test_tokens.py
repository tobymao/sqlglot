import unittest

from sqlglot import parse

class TestTokens(unittest.TestCase):
    def test_sql(self):
        token = parse('"y"')[0].args['this']
        assert token.text == 'y'
        assert token.sql() == '"y"'
        assert token.sql(dialect='spark') == '`y`'

        token = parse("'y'")[0]
        assert token.sql(quote='-') == '-y-'
