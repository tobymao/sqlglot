import unittest

import sqlglot.expressions as exp
from sqlglot import parse

class TestExpressions(unittest.TestCase):
    def test_find(self):
        expression = parse("CREATE TABLE x STORED AS PARQUET AS SELECT * FROM y")[0]
        self.assertTrue(expression.find(exp.Create))
        self.assertFalse(expression.find(exp.Group))
        self.assertEqual([table.args['this'].text for table in expression.findall(exp.Table)], ['y', 'x'])
