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
        self.assertEqual(downstream.source.sql(), "SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)")
        self.assertEqual(downstream.expression.sql(), "t.a AS a")
        self.assertEqual(downstream.alias, "y")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "(VALUES (1), (2)) AS t(a)")
        self.assertEqual(downstream.expression.sql(), "a")
        self.assertEqual(downstream.alias, "")

    def test_lineage_cte_name_appears_in_schema(self) -> None:
        schema = {"a": {"b": {"t1": {"c1": "int"}, "t2": {"c2": "int"}}}}

        node = lineage(
            "c2",
            "WITH t1 AS (SELECT * FROM a.b.t2), inter AS (SELECT * FROM t1) SELECT * FROM inter",
            schema=schema,
        )

        self.assertEqual(
            node.source.sql(),
            "WITH t1 AS (SELECT t2.c2 AS c2 FROM a.b.t2 AS t2), inter AS (SELECT t1.c2 AS c2 FROM t1) SELECT inter.c2 AS c2 FROM inter",
        )
        self.assertEqual(node.alias, "")

        downstream = node.downstream[0]
        self.assertEqual(downstream.source.sql(), "SELECT t1.c2 AS c2 FROM t1")
        self.assertEqual(downstream.expression.sql(), "t1.c2 AS c2")
        self.assertEqual(downstream.alias, "")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "SELECT t2.c2 AS c2 FROM a.b.t2 AS t2")
        self.assertEqual(downstream.expression.sql(), "t2.c2 AS c2")
        self.assertEqual(downstream.alias, "")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "a.b.t2 AS t2")
        self.assertEqual(downstream.expression.sql(), "a.b.t2 AS t2")
        self.assertEqual(downstream.alias, "")

        self.assertEqual(downstream.downstream, [])

    def test_lineage_union(self) -> None:
        node = lineage(
            "x",
            "SELECT ax AS x FROM a UNION SELECT bx FROM b UNION SELECT cx FROM c",
        )
        assert len(node.downstream) == 3

        node = lineage(
            "x",
            "SELECT x FROM (SELECT ax AS x FROM a UNION SELECT bx FROM b UNION SELECT cx FROM c)",
        )
        assert len(node.downstream) == 3
