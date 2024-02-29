import json
import unittest

from sqlglot import exp, parse_one
from sqlglot.optimizer.annotate_types import annotate_types
from tests.helpers import load_sql_fixtures


class CustomExpression(exp.Expression): ...


class TestSerDe(unittest.TestCase):
    def dump_load(self, expression):
        return exp.Expression.load(json.loads(json.dumps(expression.dump())))

    def test_serde(self):
        for sql in load_sql_fixtures("identity.sql"):
            with self.subTest(sql):
                before = parse_one(sql)
                after = self.dump_load(before)
                self.assertEqual(before, after)

    def test_custom_expression(self):
        before = CustomExpression()
        after = self.dump_load(before)
        self.assertEqual(before, after)

    def test_type_annotations(self):
        before = annotate_types(parse_one("CAST('1' AS STRUCT<x ARRAY<INT>>)"))
        after = self.dump_load(before)
        self.assertEqual(before.type, after.type)
        self.assertEqual(before.this.type, after.this.type)

    def test_meta(self):
        before = parse_one("SELECT * FROM X")
        before.meta["x"] = 1
        after = self.dump_load(before)
        self.assertEqual(before.meta, after.meta)
