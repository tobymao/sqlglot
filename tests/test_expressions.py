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
        assert parse('update tbl_name set foo = 123')[0].sql() == 'UPDATE tbl_name SET foo = 123'
        assert parse('update db.tbl_name set foo = 123 where tbl_name.bar = 234')[0].sql() == 'UPDATE db.tbl_name SET foo = 123 WHERE tbl_name.bar = 234'

    def test_validate(self):
        exp.Hint(this='')

        with self.assertRaises(ValueError):
            exp.Hint(y='')

        with self.assertRaises(ValueError):
            exp.Hint()
