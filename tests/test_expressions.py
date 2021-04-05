import unittest

import sqlglot.expressions as exp
from sqlglot import parse

class TestExpressions(unittest.TestCase):
    def test_find(self):
        expression = parse("CREATE TABLE x STORED AS PARQUET AS SELECT * FROM y")[0]
        self.assertTrue(expression.find(exp.Create))
        self.assertFalse(expression.find(exp.Group))
        self.assertEqual([table.args['this'].text for table, _, _ in expression.findall(exp.Table)], ['x', 'y'])

    def test_findall(self):
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
            [table.args['this'].text for table, _, _ in expression.findall(exp.Table)],
            ['d', 'c', 'b'],
        )

    def test_validate(self):
        exp.Hint(this='')

        with self.assertRaises(ValueError):
            exp.Hint(y='')

        with self.assertRaises(ValueError):
            exp.Hint()
