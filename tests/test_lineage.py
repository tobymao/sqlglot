from __future__ import annotations

import unittest

import sqlglot
from sqlglot.lineage import lineage
from sqlglot.schema import MappingSchema


class TestLineage(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        sqlglot.schema = MappingSchema()

    def test_lineage(self) -> None:
        node = lineage(
            "a",
            "SELECT a FROM z",
            schema={"x": {"a": "int"}},
            sources={"y": "SELECT * FROM x", "z": "SELECT a FROM y"},
        )
        self.assertEqual(
            node.source.sql(),
            "SELECT z.a AS a FROM (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) AS z /* source: z */",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */",
        )
        self.assertEqual(downstream.alias, "z")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.alias, "y")
        self.assertGreater(len(node.to_html()._repr_html_()), 1000)

    def test_lineage_sql_with_cte(self) -> None:
        node = lineage(
            "a",
            "WITH z AS (SELECT a FROM y) SELECT a FROM z",
            schema={"x": {"a": "int"}},
            sources={"y": "SELECT * FROM x"},
        )
        self.assertEqual(
            node.source.sql(),
            "WITH z AS (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) SELECT z.a AS a FROM z",
        )
        self.assertEqual(node.alias, "")

        # Node containing expanded CTE expression
        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */",
        )
        self.assertEqual(downstream.alias, "")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.alias, "y")

    def test_lineage_source_with_cte(self) -> None:
        node = lineage(
            "a",
            "SELECT a FROM z",
            schema={"x": {"a": "int"}},
            sources={"z": "WITH y AS (SELECT * FROM x) SELECT a FROM y"},
        )
        self.assertEqual(
            node.source.sql(),
            "SELECT z.a AS a FROM (WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y) AS z /* source: z */",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y",
        )
        self.assertEqual(downstream.alias, "z")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.alias, "")

    def test_lineage_source_with_star(self) -> None:
        node = lineage(
            "a",
            "WITH y AS (SELECT * FROM x) SELECT a FROM y",
        )
        self.assertEqual(
            node.source.sql(),
            "WITH y AS (SELECT * FROM x AS x) SELECT y.a AS a FROM y",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT * FROM x AS x",
        )
        self.assertEqual(downstream.alias, "")

    def test_lineage_external_col(self) -> None:
        node = lineage(
            "a",
            "WITH y AS (SELECT * FROM x) SELECT a FROM y JOIN z USING (uid)",
        )
        self.assertEqual(
            node.source.sql(),
            "WITH y AS (SELECT * FROM x AS x) SELECT a AS a FROM y JOIN z AS z ON y.uid = z.uid",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "?",
        )
        self.assertEqual(downstream.alias, "")

    def test_lineage_values(self) -> None:
        node = lineage(
            "a",
            "SELECT a FROM y",
            sources={"y": "SELECT a FROM (VALUES (1), (2)) AS t (a)"},
        )
        self.assertEqual(
            node.source.sql(),
            "SELECT y.a AS a FROM (SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)) AS y /* source: y */",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)",
        )
        self.assertEqual(downstream.expression.sql(), "t.a AS a")
        self.assertEqual(downstream.alias, "y")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "(VALUES (1), (2)) AS t(a)",
        )
        self.assertEqual(downstream.expression.sql(), "a")
        self.assertEqual(downstream.alias, "")
