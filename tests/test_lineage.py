import unittest

from sqlglot.lineage import lineage


class TestLineage(unittest.TestCase):
    maxDiff = None

    def test_lineage(self) -> None:
        node = lineage(
            "a",
            "SELECT a FROM y",
            schema={"x": {"a": "int"}},
            sources={"y": "SELECT * FROM x"},
        )
        self.assertEqual(
            node.source.sql(),
            "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */",
        )
        self.assertGreater(len(node.to_html()._repr_html_()), 1000)
