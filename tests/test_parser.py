import unittest

import sqlglot.expressions as exp
from sqlglot import parse


class TestParser(unittest.TestCase):
    def test_column(self):
        columns = parse("select a, ARRAY[1] b, case when 1 then 1 end")[0].find_all(exp.Column)
        assert len(list(columns)) == 1

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

    def test_multi(self):
        expressions = parse("""
            SELECT * FROM a; SELECT * FROM b;
        """)

        assert len(expressions) == 2
        assert expressions[0].args['from'].args['expressions'][0].args['this'].text == 'a'
        assert expressions[1].args['from'].args['expressions'][0].args['this'].text == 'b'
