import unittest

import sqlglot.expressions as exp
from sqlglot import parse

class TestExpressions(unittest.TestCase):
    def test_find(self):
        expression = parse("CREATE TABLE x STORED AS PARQUET AS SELECT * FROM y")[0]
        self.assertTrue(expression.find(exp.Create))
        self.assertFalse(expression.find(exp.Group))
        self.assertEqual([table.args['this'].text for table in expression.find_all(exp.Table)], ['y', 'x'])

    def test_find_all(self):
        expression = parse(
            """
            SELECT *
            FROM (
                SELECT b.*
                FROM a.b b
            ) x
            JOIN (
              SELECT c.foo
              FROM a.c c
              WHERE foo = 1
            ) y
              ON x.c = y.foo
            CROSS JOIN (
              SELECT *
              FROM (
                SELECT d.bar
                FROM d
              ) nested
            ) z
              ON x.c = y.foo
            """
        )[0]

        self.assertEqual(
            [table.args['this'].text for table in expression.find_all(exp.Table)],
            ['d', 'c', 'b'],
        )

    def test_sql(self):
        assert parse('x + y * 2')[0].sql() == 'x + y * 2'
        assert parse('select "x"')[0].sql(dialect='hive', pretty=True) == 'SELECT\n  `x`'

    def test_validate(self):
        exp.Hint(this='')

        with self.assertRaises(ValueError):
            exp.Hint(y='')

        with self.assertRaises(ValueError):
            exp.Hint()

    def test_transform_simple(self):
        expression = parse('IF(a > 0, a, b)')[0]

        def fun(node):
            if isinstance(node, exp.Column) and node.args['this'].text == 'a':
                return parse('c - 2')[0]
            return node

        actual_expression_1 = expression.transform(fun)
        self.assertEqual(
            actual_expression_1.sql(dialect='presto'),
            'IF(c - 2 > 0, c - 2, b)'
        )
        self.assertIsNot(actual_expression_1, expression)

        actual_expression_2 = expression.transform(fun, copy=False)
        self.assertEqual(
            actual_expression_2.sql(dialect='presto'),
            'IF(c - 2 > 0, c - 2, b)'
        )
        self.assertIs(actual_expression_2, expression)

    def test_transform_no_infinite_recursion(self):
        expression = parse('a')[0]

        def fun(node):
            if isinstance(node, exp.Column) and node.args['this'].text == 'a':
                return parse('FUN(a)')[0]
            return node

        self.assertEqual(
            expression.transform(fun).sql(dialect='sql'),
            'FUN(a)'
        )
