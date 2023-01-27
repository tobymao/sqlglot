import unittest

from sqlglot.lineage import lineage


class TestLineage(unittest.TestCase):
    maxDiff = None

    def test_lineage(self) -> None:
        node = lineage("a", "SELECT a FROM x", {"x": {"a": "int"}})
        self.assertEqual(node.source.sql(), 'SELECT "x"."a" AS "a" FROM "x" AS "x"')
        self.assertGreater(len(node.to_html()._repr_html_()), 1000)
