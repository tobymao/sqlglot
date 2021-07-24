import unittest

from sqlglot import parse


class TestParser(unittest.TestCase):
    def test_identify(self):
        expression = parse("""
            SELECT a, "b", c AS c, d AS "D", e AS "y|z'"
            FROM y."z"
        """)[0]

        assert expression.args['expressions'][0].args['this'].text == 'a'
        assert expression.args['expressions'][1].args['this'].text == 'b'
        assert expression.args['expressions'][2].args['alias'].text == 'c'
        assert expression.args['expressions'][3].args['alias'].text == 'D'
        assert expression.args['expressions'][4].args['alias'].text == "y|z'"
        table = expression.args['from'].args['expressions'][0]
        assert table.args['this'].text == 'z'
        assert table.args['db'].text == 'y'
