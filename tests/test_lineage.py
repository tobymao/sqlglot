from __future__ import annotations

import unittest

import sqlglot
from sqlglot.expressions import Placeholder, Select
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
        self.assertEqual(node.source_name, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */",
        )
        self.assertEqual(downstream.source_name, "z")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.source_name, "y")
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
            "WITH z AS (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) SELECT z.a AS a FROM z AS z",
        )
        self.assertEqual(node.source_name, "")
        self.assertEqual(node.reference_node_name, "")

        # Node containing expanded CTE expression
        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */",
        )
        self.assertEqual(downstream.source_name, "")
        self.assertEqual(downstream.reference_node_name, "z")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.source_name, "y")
        self.assertEqual(downstream.reference_node_name, "")

    def test_lineage_source_with_cte(self) -> None:
        node = lineage(
            "a",
            "SELECT a FROM z",
            schema={"x": {"a": "int"}},
            sources={"z": "WITH y AS (SELECT * FROM x) SELECT a FROM y"},
        )
        self.assertEqual(
            node.source.sql(),
            "SELECT z.a AS a FROM (WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y) AS z /* source: z */",
        )
        self.assertEqual(node.source_name, "")
        self.assertEqual(node.reference_node_name, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y",
        )
        self.assertEqual(downstream.source_name, "z")
        self.assertEqual(downstream.reference_node_name, "")

        downstream = downstream.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT x.a AS a FROM x AS x",
        )
        self.assertEqual(downstream.source_name, "z")
        self.assertEqual(downstream.reference_node_name, "y")

    def test_lineage_source_with_star(self) -> None:
        node = lineage(
            "a",
            "WITH y AS (SELECT * FROM x) SELECT a FROM y",
        )
        self.assertEqual(
            node.source.sql(),
            "WITH y AS (SELECT * FROM x AS x) SELECT y.a AS a FROM y AS y",
        )
        self.assertEqual(node.source_name, "")
        self.assertEqual(node.reference_node_name, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "SELECT * FROM x AS x",
        )
        self.assertEqual(downstream.source_name, "")
        self.assertEqual(downstream.reference_node_name, "y")

    def test_lineage_external_col(self) -> None:
        node = lineage(
            "a",
            "WITH y AS (SELECT * FROM x) SELECT a FROM y JOIN z USING (uid)",
        )
        self.assertEqual(
            node.source.sql(),
            "WITH y AS (SELECT * FROM x AS x) SELECT a AS a FROM y AS y JOIN z AS z ON y.uid = z.uid",
        )
        self.assertEqual(node.source_name, "")
        self.assertEqual(node.reference_node_name, "")

        downstream = node.downstream[0]
        self.assertEqual(
            downstream.source.sql(),
            "?",
        )
        self.assertEqual(downstream.source_name, "")
        self.assertEqual(downstream.reference_node_name, "")

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
        self.assertEqual(node.source_name, "")

        downstream = node.downstream[0]
        self.assertEqual(downstream.source.sql(), "SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)")
        self.assertEqual(downstream.expression.sql(), "t.a AS a")
        self.assertEqual(downstream.source_name, "y")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "(VALUES (1), (2)) AS t(a)")
        self.assertEqual(downstream.expression.sql(), "a")
        self.assertEqual(downstream.source_name, "y")

    def test_lineage_cte_name_appears_in_schema(self) -> None:
        schema = {"a": {"b": {"t1": {"c1": "int"}, "t2": {"c2": "int"}}}}

        node = lineage(
            "c2",
            "WITH t1 AS (SELECT * FROM a.b.t2), inter AS (SELECT * FROM t1) SELECT * FROM inter",
            schema=schema,
        )

        self.assertEqual(
            node.source.sql(),
            "WITH t1 AS (SELECT t2.c2 AS c2 FROM a.b.t2 AS t2), inter AS (SELECT t1.c2 AS c2 FROM t1 AS t1) SELECT inter.c2 AS c2 FROM inter AS inter",
        )
        self.assertEqual(node.source_name, "")

        downstream = node.downstream[0]
        self.assertEqual(downstream.source.sql(), "SELECT t1.c2 AS c2 FROM t1 AS t1")
        self.assertEqual(downstream.expression.sql(), "t1.c2 AS c2")
        self.assertEqual(downstream.source_name, "")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "SELECT t2.c2 AS c2 FROM a.b.t2 AS t2")
        self.assertEqual(downstream.expression.sql(), "t2.c2 AS c2")
        self.assertEqual(downstream.source_name, "")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.source.sql(), "a.b.t2 AS t2")
        self.assertEqual(downstream.expression.sql(), "a.b.t2 AS t2")
        self.assertEqual(downstream.source_name, "")

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

    def test_lineage_lateral_flatten(self) -> None:
        node = lineage(
            "VALUE",
            "SELECT FLATTENED.VALUE FROM TEST_TABLE, LATERAL FLATTEN(INPUT => RESULT, OUTER => TRUE) FLATTENED",
            dialect="snowflake",
        )
        self.assertEqual(node.name, "VALUE")

        downstream = node.downstream[0]
        self.assertEqual(downstream.name, "FLATTENED.VALUE")
        self.assertEqual(
            downstream.source.sql(dialect="snowflake"),
            "LATERAL FLATTEN(INPUT => TEST_TABLE.RESULT, OUTER => TRUE) AS FLATTENED(SEQ, KEY, PATH, INDEX, VALUE, THIS)",
        )
        self.assertEqual(downstream.expression.sql(dialect="snowflake"), "VALUE")
        self.assertEqual(len(downstream.downstream), 1)

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.name, "TEST_TABLE.RESULT")
        self.assertEqual(downstream.source.sql(dialect="snowflake"), "TEST_TABLE AS TEST_TABLE")

        node = lineage(
            "FIELD",
            "SELECT FLATTENED.VALUE:field::text AS FIELD FROM SNOWFLAKE.SCHEMA.MODEL AS MODEL_ALIAS, LATERAL FLATTEN(INPUT => MODEL_ALIAS.A) AS FLATTENED",
            schema={"SNOWFLAKE": {"SCHEMA": {"TABLE": {"A": "integer"}}}},
            sources={"SNOWFLAKE.SCHEMA.MODEL": "SELECT A FROM SNOWFLAKE.SCHEMA.TABLE"},
            dialect="snowflake",
        )
        self.assertEqual(node.name, "FIELD")

        downstream = node.downstream[0]
        self.assertEqual(downstream.name, "FLATTENED.VALUE")
        self.assertEqual(
            downstream.source.sql(dialect="snowflake"),
            "LATERAL FLATTEN(INPUT => MODEL_ALIAS.A) AS FLATTENED(SEQ, KEY, PATH, INDEX, VALUE, THIS)",
        )
        self.assertEqual(downstream.expression.sql(dialect="snowflake"), "VALUE")
        self.assertEqual(len(downstream.downstream), 1)

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.name, "MODEL_ALIAS.A")
        self.assertEqual(downstream.source_name, "SNOWFLAKE.SCHEMA.MODEL")
        self.assertEqual(
            downstream.source.sql(dialect="snowflake"),
            "SELECT TABLE.A AS A FROM SNOWFLAKE.SCHEMA.TABLE AS TABLE",
        )
        self.assertEqual(downstream.expression.sql(dialect="snowflake"), "TABLE.A AS A")
        self.assertEqual(len(downstream.downstream), 1)

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.name, "TABLE.A")
        self.assertEqual(
            downstream.source.sql(dialect="snowflake"), "SNOWFLAKE.SCHEMA.TABLE AS TABLE"
        )
        self.assertEqual(
            downstream.expression.sql(dialect="snowflake"), "SNOWFLAKE.SCHEMA.TABLE AS TABLE"
        )

    def test_subquery(self) -> None:
        node = lineage(
            "output",
            "SELECT (SELECT max(t3.my_column) my_column FROM foo t3) AS output FROM table3",
        )
        self.assertEqual(node.name, "output")
        node = node.downstream[0]
        self.assertEqual(node.name, "my_column")
        node = node.downstream[0]
        self.assertEqual(node.name, "t3.my_column")
        self.assertEqual(node.source.sql(), "foo AS t3")

        node = lineage(
            "y",
            "SELECT SUM((SELECT max(a) a from x) + (SELECT min(b) b from x) + c) AS y FROM x",
        )
        self.assertEqual(node.name, "y")
        self.assertEqual(len(node.downstream), 3)
        self.assertEqual(node.downstream[0].name, "a")
        self.assertEqual(node.downstream[1].name, "b")
        self.assertEqual(node.downstream[2].name, "x.c")

        node = lineage(
            "x",
            "WITH cte AS (SELECT a, b FROM z) SELECT sum(SELECT a FROM cte) AS x, (SELECT b FROM cte) as y FROM cte",
        )
        self.assertEqual(node.name, "x")
        self.assertEqual(len(node.downstream), 1)
        node = node.downstream[0]
        self.assertEqual(node.name, "a")
        node = node.downstream[0]
        self.assertEqual(node.name, "cte.a")
        self.assertEqual(node.reference_node_name, "cte")
        node = node.downstream[0]
        self.assertEqual(node.name, "z.a")

        node = lineage(
            "a",
            """
            WITH foo AS (
              SELECT
                1 AS a
            ), bar AS (
              (
                SELECT
                  a + 1 AS a
                FROM foo
              )
            )
            (
              SELECT
                a + b AS a
              FROM bar
              CROSS JOIN (
                SELECT
                  2 AS b
              ) AS baz
            )
            """,
        )
        self.assertEqual(node.name, "a")
        self.assertEqual(len(node.downstream), 2)
        a, b = sorted(node.downstream, key=lambda n: n.name)
        self.assertEqual(a.name, "bar.a")
        self.assertEqual(len(a.downstream), 1)
        self.assertEqual(b.name, "baz.b")
        self.assertEqual(b.downstream, [])

        node = a.downstream[0]
        self.assertEqual(node.name, "foo.a")

        # Select from derived table
        node = lineage(
            "a",
            "SELECT a FROM (SELECT a FROM x) subquery",
        )
        self.assertEqual(node.name, "a")
        self.assertEqual(len(node.downstream), 1)
        node = node.downstream[0]
        self.assertEqual(node.name, "subquery.a")
        self.assertEqual(node.reference_node_name, "subquery")

        node = lineage(
            "a",
            "SELECT a FROM (SELECT a FROM x)",
        )
        self.assertEqual(node.name, "a")
        self.assertEqual(len(node.downstream), 1)
        node = node.downstream[0]
        self.assertEqual(node.name, "_q_0.a")
        self.assertEqual(node.reference_node_name, "_q_0")

    def test_lineage_cte_union(self) -> None:
        query = """
        WITH dataset AS (
            SELECT *
            FROM catalog.db.table_a

            UNION

            SELECT *
            FROM catalog.db.table_b
        )

        SELECT x, created_at FROM dataset;
        """
        node = lineage("x", query)

        self.assertEqual(node.name, "x")

        downstream_a = node.downstream[0]
        self.assertEqual(downstream_a.name, "0")
        self.assertEqual(downstream_a.source.sql(), "SELECT * FROM catalog.db.table_a AS table_a")
        self.assertEqual(downstream_a.reference_node_name, "dataset")
        downstream_b = node.downstream[1]
        self.assertEqual(downstream_b.name, "0")
        self.assertEqual(downstream_b.source.sql(), "SELECT * FROM catalog.db.table_b AS table_b")
        self.assertEqual(downstream_b.reference_node_name, "dataset")

    def test_lineage_source_union(self) -> None:
        query = "SELECT x, created_at FROM dataset;"
        node = lineage(
            "x",
            query,
            sources={
                "dataset": """
                SELECT *
                FROM catalog.db.table_a

                UNION

                SELECT *
                FROM catalog.db.table_b
                """
            },
        )

        self.assertEqual(node.name, "x")

        downstream_a = node.downstream[0]
        self.assertEqual(downstream_a.name, "0")
        self.assertEqual(downstream_a.source_name, "dataset")
        self.assertEqual(downstream_a.source.sql(), "SELECT * FROM catalog.db.table_a AS table_a")
        self.assertEqual(downstream_a.reference_node_name, "")
        downstream_b = node.downstream[1]
        self.assertEqual(downstream_b.name, "0")
        self.assertEqual(downstream_b.source_name, "dataset")
        self.assertEqual(downstream_b.source.sql(), "SELECT * FROM catalog.db.table_b AS table_b")
        self.assertEqual(downstream_b.reference_node_name, "")

    def test_select_star(self) -> None:
        node = lineage("x", "SELECT x from (SELECT * from table_a)")

        self.assertEqual(node.name, "x")

        downstream = node.downstream[0]
        self.assertEqual(downstream.name, "_q_0.x")
        self.assertEqual(downstream.source.sql(), "SELECT * FROM table_a AS table_a")

        downstream = downstream.downstream[0]
        self.assertEqual(downstream.name, "*")
        self.assertEqual(downstream.source.sql(), "table_a AS table_a")

    def test_unnest(self) -> None:
        node = lineage(
            "b",
            "with _data as (select [struct(1 as a, 2 as b)] as col) select b from _data cross join unnest(col)",
        )
        self.assertEqual(node.name, "b")

    def test_lineage_normalize(self) -> None:
        node = lineage("a", "WITH x AS (SELECT 1 a) SELECT a FROM x", dialect="snowflake")
        self.assertEqual(node.name, "A")

        with self.assertRaises(sqlglot.errors.SqlglotError):
            lineage('"a"', "WITH x AS (SELECT 1 a) SELECT a FROM x", dialect="snowflake")

    def test_ddl_lineage(self) -> None:
        sql = """
        INSERT /*+ HINT1 */
        INTO target (x, y)
        SELECT subq.x, subq.y
        FROM (
          SELECT /*+ HINT2 */
            t.x AS x,
            TO_DATE('2023-12-19', 'YYYY-MM-DD') AS y
          FROM s.t t
          WHERE 1 = 1 AND y = TO_DATE('2023-12-19', 'YYYY-MM-DD')
        ) subq
        """

        node = lineage("y", sql, dialect="oracle")

        self.assertEqual(node.name, "Y")
        self.assertEqual(node.expression.sql(dialect="oracle"), "SUBQ.Y AS Y")

        downstream = node.downstream[0]
        self.assertEqual(downstream.name, "SUBQ.Y")
        self.assertEqual(
            downstream.expression.sql(dialect="oracle"), "TO_DATE('2023-12-19', 'YYYY-MM-DD') AS Y"
        )

    def test_trim(self) -> None:
        sql = """
            SELECT a, b, c
            FROM (select a, b, c from y) z
        """

        node = lineage("a", sql, trim_selects=False)

        self.assertEqual(node.name, "a")
        self.assertEqual(
            node.source.sql(),
            "SELECT z.a AS a, z.b AS b, z.c AS c FROM (SELECT y.a AS a, y.b AS b, y.c AS c FROM y AS y) AS z",
        )

        downstream = node.downstream[0]
        self.assertEqual(downstream.name, "z.a")
        self.assertEqual(downstream.source.sql(), "SELECT y.a AS a, y.b AS b, y.c AS c FROM y AS y")

    def test_node_name_doesnt_contain_comment(self) -> None:
        sql = "SELECT * FROM (SELECT x /* c */ FROM t1) AS t2"
        node = lineage("x", sql)

        self.assertEqual(len(node.downstream), 1)
        self.assertEqual(len(node.downstream[0].downstream), 1)
        self.assertEqual(node.downstream[0].downstream[0].name, "t1.x")

    def test_pivot_without_alias(self) -> None:
        sql = """
        SELECT 
            a as other_a
        FROM (select value,category from sample_data)
        PIVOT (
            sum(value)
            FOR category IN ('a', 'b')
        );
        """
        node = lineage("other_a", sql)

        # The lineage should reflect the actual data flow:
        # 1. other_a is an alias of PIVOT's output column 'a'
        # 2. Column 'a' is created by PIVOT operation on sample_data.value
        # 3. Subquery (_q_0) is an intermediate step but not relevant for column naming
        # self.assertEqual(node.downstream[0].name, "_q_0.value")
        self.assertEqual(node.downstream[0].name, "a")  # Column created by PIVOT
        self.assertTrue(any(d.name == "sample_data.value" for d in node.downstream[0].downstream))
        self.assertTrue(
            any(d.name == "sample_data.category" for d in node.downstream[0].downstream)
        )

    def test_pivot_with_alias(self) -> None:
        sql = """
            SELECT 
                cat_a_s as other_as
            FROM sample_data
            PIVOT (
                sum(value) as s, max(price)
                FOR category IN ('a' as cat_a, 'b')
            )
        """
        node = lineage("other_as", sql)

        self.assertEqual(len(node.downstream), 1)
        self.assertEqual(node.downstream[0].name, "sample_data.value")

    def test_pivot_with_cte(self) -> None:
        sql = """
        WITH t as (
            SELECT 
                a as other_a
            FROM sample_data
            PIVOT (
                sum(value)
                FOR category IN ('a', 'b')
            )
        )
        select other_a from t
        """
        node = lineage("other_a", sql)

        self.assertEqual(node.downstream[0].name, "t.other_a")
        self.assertEqual(node.downstream[0].reference_node_name, "t")
        self.assertEqual(node.downstream[0].downstream[0].name, "sample_data.value")

    def test_pivot_with_implicit_column_of_pivoted_source(self) -> None:
        sql = """
        SELECT empid
        FROM quarterly_sales
            PIVOT(SUM(amount) FOR quarter IN (
            '2023_Q1',
            '2023_Q2',
            '2023_Q3'))
        ORDER BY empid;
        """
        node = lineage("empid", sql)

        self.assertEqual(node.downstream[0].name, "quarterly_sales.empid")

    def test_pivot_with_implicit_column_of_pivoted_source_and_cte(self) -> None:
        sql = """
        WITH t as (
            SELECT empid
            FROM quarterly_sales
            PIVOT(SUM(amount) FOR quarter IN (
                '2023_Q1',
                '2023_Q2',
                '2023_Q3'))
        )
        select empid from t
        """
        node = lineage("empid", sql)

        self.assertEqual(node.downstream[0].name, "t.empid")
        self.assertEqual(node.downstream[0].reference_node_name, "t")
        self.assertEqual(node.downstream[0].downstream[0].name, "quarterly_sales.empid")

    def test_pivot_in_subquery(self) -> None:
        sql = """
       SELECT 
           sub.product_id,
           sub.q1,
           sub.q2
       FROM (
           SELECT *
           FROM sales_data
           PIVOT (
               SUM(amount)
               FOR quarter IN ('Q1' as q1, 'Q2' as q2)
           )
       ) sub;
       """
        # Track lineage through subquery with PIVOT
        product_id_node = lineage("product_id", sql)
        q1_node = lineage("q1", sql)
        q2_node = lineage("q2", sql)

        # Verify lineage for each column
        self.assertEqual(product_id_node.name, "product_id")
        self.assertEqual(product_id_node.downstream[0].name, "sub.product_id")
        self.assertEqual(product_id_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(q1_node.name, "q1")
        self.assertEqual(q1_node.downstream[0].name, "sub.q1")
        self.assertEqual(q1_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(q2_node.name, "q2")
        self.assertEqual(q2_node.downstream[0].name, "sub.q2")
        self.assertEqual(q2_node.downstream[0].downstream[0].source.name, "sales_data")

    def test_pivot_with_correlated_subquery(self) -> None:
        sql = """
       SELECT 
           p.product_id,
           p.q1_amount,
           (
               SELECT AVG(amount)
               FROM sales_data s2
               PIVOT (
                   AVG(amount)
                   FOR quarter IN ('Q1' as q1_avg)
               )
               WHERE s2.product_id = p.product_id
           ) as avg_amount
       FROM sales_data s1
       PIVOT (
           SUM(amount)
           FOR quarter IN ('Q1' as q1_amount)
       ) p;
       """
        # Track lineage with correlated subquery containing PIVOT
        product_id_node = lineage("product_id", sql)
        q1_amount_node = lineage("q1_amount", sql)
        avg_amount_node = lineage("avg_amount", sql)

        # Verify lineage across query contexts
        self.assertEqual(product_id_node.name, "product_id")
        self.assertEqual(product_id_node.downstream[0].name, "s1.product_id")
        self.assertEqual(product_id_node.downstream[0].source.name, "sales_data")

        self.assertEqual(q1_amount_node.name, "q1_amount")
        self.assertEqual(q1_amount_node.downstream[0].name, "s1.q1_amount")
        self.assertEqual(q1_amount_node.downstream[0].source.name, "sales_data")

        self.assertEqual(avg_amount_node.name, "avg_amount")
        self.assertIsInstance(avg_amount_node.downstream[0].source, Select)
        self.assertEqual(avg_amount_node.downstream[0].downstream[0].name, "s1.amount")
        self.assertEqual(avg_amount_node.downstream[0].downstream[0].source.name, "sales_data")

    def test_pivot_subquery_in_from(self) -> None:
        sql = """
       SELECT 
           t1.product_id,
           t1.q1_sales,
           t2.q1_inventory
       FROM (
           SELECT *
           FROM sales_data
           PIVOT (
               SUM(amount)
               FOR quarter IN ('Q1' as q1_sales)
           )
       ) t1
       LEFT JOIN (
           SELECT *
           FROM inventory_data
           PIVOT (
               SUM(quantity)
               FOR quarter IN ('Q1' as q1_inventory)
           )
       ) t2 ON t1.product_id = t2.product_id;
       """
        # Track lineage from multiple subqueries with PIVOT in FROM clause
        product_id_node = lineage("product_id", sql)
        q1_sales_node = lineage("q1_sales", sql)
        q1_inventory_node = lineage("q1_inventory", sql)

        # Verify lineage through complex query structure
        self.assertEqual(product_id_node.name, "product_id")
        self.assertEqual(product_id_node.downstream[0].reference_node_name, "t1")
        self.assertEqual(product_id_node.downstream[0].name, "t1.product_id")
        self.assertEqual(product_id_node.downstream[0].downstream[0].name, "*")
        self.assertEqual(product_id_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(q1_sales_node.name, "q1_sales")
        self.assertEqual(q1_sales_node.downstream[0].reference_node_name, "t1")
        self.assertEqual(q1_sales_node.downstream[0].name, "t1.q1_sales")
        self.assertEqual(q1_sales_node.downstream[0].downstream[0].name, "*")
        self.assertEqual(q1_sales_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(q1_inventory_node.name, "q1_inventory")
        self.assertEqual(q1_inventory_node.downstream[0].reference_node_name, "t2")
        self.assertEqual(q1_inventory_node.downstream[0].name, "t2.q1_inventory")
        self.assertEqual(q1_inventory_node.downstream[0].downstream[0].name, "*")
        self.assertEqual(
            q1_inventory_node.downstream[0].downstream[0].source.name, "inventory_data"
        )

    def test_pivot_with_cte_and_subquery(self) -> None:
        sql = """
       WITH base_pivot AS (
           SELECT *
           FROM sales_data
           PIVOT (
               SUM(amount)
               FOR quarter IN ('Q1' as q1_amount, 'Q2' as q2_amount)
           )
       )
       SELECT 
           b.product_id,
           b.q1_amount,
           (
               SELECT MAX(q1_amount)
               FROM base_pivot
               WHERE category = b.category
           ) as max_q1_amount
       FROM base_pivot b;
       """
        # Track lineage through CTE and subquery combination
        product_id_node = lineage("product_id", sql)
        q1_amount_node = lineage("q1_amount", sql)
        max_q1_amount_node = lineage("max_q1_amount", sql)

        # Verify lineage through CTE and correlated subquery
        self.assertEqual(product_id_node.name, "product_id")
        self.assertEqual(product_id_node.downstream[0].reference_node_name, "base_pivot")
        self.assertEqual(product_id_node.downstream[0].name, "b.product_id")
        self.assertEqual(product_id_node.downstream[0].downstream[0].name, "*")
        self.assertEqual(product_id_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(q1_amount_node.name, "q1_amount")
        self.assertEqual(q1_amount_node.downstream[0].reference_node_name, "base_pivot")
        self.assertEqual(q1_amount_node.downstream[0].name, "b.q1_amount")
        self.assertEqual(q1_amount_node.downstream[0].downstream[0].name, "*")
        self.assertEqual(q1_amount_node.downstream[0].downstream[0].source.name, "sales_data")

        self.assertEqual(max_q1_amount_node.name, "max_q1_amount")
        self.assertEqual(
            max_q1_amount_node.downstream[0].downstream[0].reference_node_name, "base_pivot"
        )
        self.assertEqual(
            max_q1_amount_node.downstream[0].downstream[0].name, "base_pivot.q1_amount"
        )
        self.assertEqual(max_q1_amount_node.downstream[0].downstream[0].downstream[0].name, "*")
        self.assertEqual(
            max_q1_amount_node.downstream[0].downstream[0].downstream[0].source.name, "sales_data"
        )

    def test_unpivot_without_alias(self) -> None:
        sql = """
        SELECT 
            value as other_value,
            category
        FROM sample_data
        UNPIVOT (
            value
            FOR category IN (a, b)
        );
        """
        # Track lineage of value and category columns from UNPIVOT
        value_node = lineage("other_value", sql)
        category_node = lineage("category", sql)

        # Verify both value and category columns coming from sample_data
        self.assertEqual(value_node.name, "other_value")
        self.assertEqual(value_node.downstream[0].name, "sample_data.value")
        self.assertEqual(value_node.downstream[0].source.name, "sample_data")

        self.assertEqual(category_node.name, "category")
        self.assertEqual(category_node.downstream[0].name, "sample_data.category")
        self.assertEqual(category_node.downstream[0].source.name, "sample_data")

    def test_unpivot_with_alias(self) -> None:
        sql = """
        SELECT 
            unpvt.val as other_value,
            unpvt.category
        FROM sample_data
        UNPIVOT (
            value
            FOR category IN (a , b)
        ) unpvt
        """
        # Track lineage of aliased value and category columns
        value_node = lineage("other_value", sql)
        category_node = lineage("category", sql)

        # Verify both columns' source table with alias
        self.assertEqual(value_node.name, "other_value")
        self.assertEqual(value_node.downstream[0].name, "sample_data.val")
        self.assertEqual(value_node.downstream[0].source.name, "sample_data")

        self.assertEqual(category_node.name, "category")
        self.assertEqual(category_node.downstream[0].name, "sample_data.category")
        self.assertEqual(category_node.downstream[0].source.name, "sample_data")

    def test_unpivot_with_cte(self) -> None:
        sql = """
        WITH t as (
            SELECT 
                value as other_value,
                category
            FROM sample_data
            UNPIVOT (
                value
                FOR category IN (a, b)
            )
        )
        select other_value, category from t
        """
        # Track lineage of both columns through CTE
        value_node = lineage("other_value", sql)
        category_node = lineage("category", sql)

        # Verify source table for both columns through CTE
        self.assertEqual(value_node.name, "other_value")
        self.assertEqual(value_node.downstream[0].reference_node_name, "t")
        self.assertEqual(value_node.downstream[0].downstream[0].source.name, "sample_data")

        self.assertEqual(category_node.name, "category")
        self.assertEqual(category_node.downstream[0].reference_node_name, "t")
        self.assertEqual(category_node.downstream[0].downstream[0].source.name, "sample_data")

    def test_unpivot_multiple_value_columns(self) -> None:
        sql = """
        SELECT 
            amount,
            quantity,
            year,
            region  -- pass-through column
        FROM sales_data
        UNPIVOT (
            (amount, quantity)
            FOR year IN (
                (amount_2021, qty_2021) AS '2021',
                (amount_2022, qty_2022) AS '2022'
            )
        );
        """
        # Track lineage of value, category and pass-through columns
        amount_node = lineage("amount", sql)
        quantity_node = lineage("quantity", sql)
        year_node = lineage("year", sql)
        region_node = lineage("region", sql)  # pass-through column

        # Verify source table for all columns
        self.assertEqual(amount_node.name, "amount")
        self.assertEqual(amount_node.downstream[0].name, "sales_data.amount")
        self.assertEqual(amount_node.downstream[0].source.name, "sales_data")

        self.assertEqual(quantity_node.name, "quantity")
        self.assertEqual(quantity_node.downstream[0].name, "sales_data.quantity")
        self.assertEqual(quantity_node.downstream[0].source.name, "sales_data")

        self.assertEqual(year_node.name, "year")
        self.assertEqual(year_node.downstream[0].name, "sales_data.year")
        self.assertEqual(year_node.downstream[0].source.name, "sales_data")

        self.assertEqual(region_node.name, "region")
        self.assertEqual(region_node.downstream[0].name, "sales_data.region")
        self.assertEqual(region_node.downstream[0].source.name, "sales_data")

    def test_unpivot_nested_cte(self) -> None:
        sql = """
        WITH base_unpivot AS (
            SELECT value, category, created_at  -- pass-through column
            FROM sample_data
            UNPIVOT (
                value
                FOR category IN (a, b)
            )
        ),
        second_level AS (
            SELECT 
                value as transformed_value,
                category as item_category,
                created_at
            FROM base_unpivot
        )
        SELECT transformed_value, item_category, created_at 
        FROM second_level;
        """
        # Track lineage through multiple CTEs including pass-through column
        value_node = lineage("transformed_value", sql)
        category_node = lineage("item_category", sql)
        created_at_node = lineage("created_at", sql)  # pass-through column

        # Verify source table through CTE chain
        self.assertEqual(value_node.name, "transformed_value")
        self.assertEqual(value_node.downstream[0].reference_node_name, "second_level")
        self.assertEqual(value_node.downstream[0].downstream[0].reference_node_name, "base_unpivot")
        self.assertEqual(
            value_node.downstream[0].downstream[0].downstream[0].name, "sample_data.value"
        )
        self.assertEqual(
            value_node.downstream[0].downstream[0].downstream[0].source.name, "sample_data"
        )

        self.assertEqual(category_node.name, "item_category")
        self.assertEqual(category_node.downstream[0].reference_node_name, "second_level")
        self.assertEqual(
            category_node.downstream[0].downstream[0].reference_node_name, "base_unpivot"
        )
        self.assertEqual(
            category_node.downstream[0].downstream[0].downstream[0].name, "sample_data.category"
        )
        self.assertEqual(
            category_node.downstream[0].downstream[0].downstream[0].source.name, "sample_data"
        )

        self.assertEqual(created_at_node.name, "created_at")
        self.assertEqual(created_at_node.downstream[0].reference_node_name, "second_level")
        self.assertEqual(
            created_at_node.downstream[0].downstream[0].reference_node_name, "base_unpivot"
        )
        self.assertEqual(
            created_at_node.downstream[0].downstream[0].downstream[0].name, "sample_data.created_at"
        )
        self.assertEqual(
            created_at_node.downstream[0].downstream[0].downstream[0].source.name, "sample_data"
        )

    def test_unpivot_with_join(self) -> None:
        sql = """
        WITH unpivoted_data AS (
            SELECT value, category, status  -- pass-through column
            FROM sample_data
            UNPIVOT (
                value
                FOR category IN (a, b)
            )
        )
        SELECT 
            u.value,
            u.category,
            u.status,
            m.category_name
        FROM unpivoted_data u
        JOIN metadata m ON u.category = m.category_code;
        """
        # Track lineage of all columns including pass-through
        value_node = lineage("value", sql)
        category_node = lineage("category", sql)
        status_node = lineage("status", sql)  # pass-through column
        category_name_node = lineage("category_name", sql)

        # Verify source tables for all columns
        self.assertEqual(value_node.name, "value")
        self.assertEqual(value_node.downstream[0].reference_node_name, "unpivoted_data")
        self.assertEqual(value_node.downstream[0].downstream[0].name, "sample_data.value")
        self.assertEqual(value_node.downstream[0].downstream[0].source.name, "sample_data")

        self.assertEqual(category_node.name, "category")
        self.assertEqual(category_node.downstream[0].reference_node_name, "unpivoted_data")
        self.assertEqual(category_node.downstream[0].downstream[0].name, "sample_data.category")
        self.assertEqual(category_node.downstream[0].downstream[0].source.name, "sample_data")

        self.assertEqual(status_node.name, "status")
        self.assertEqual(status_node.downstream[0].reference_node_name, "unpivoted_data")
        self.assertEqual(status_node.downstream[0].downstream[0].name, "sample_data.status")
        self.assertEqual(status_node.downstream[0].downstream[0].source.name, "sample_data")

        self.assertEqual(category_name_node.name, "category_name")
        self.assertEqual(category_name_node.downstream[0].name, "m.category_name")
        self.assertEqual(category_name_node.downstream[0].source.name, "metadata")

    def test_unpivot_in_subquery(self) -> None:
        sql = """
       SELECT 
           sub.other_value,
           sub.category
       FROM (
           SELECT 
               value as other_value,
               category
           FROM sample_data
           UNPIVOT (
               value
               FOR category IN (a, b)
           )
       ) sub;
       """
        # Track column lineage through subquery
        value_node = lineage("other_value", sql)
        category_node = lineage("category", sql)

        # Verify lineage
        self.assertEqual(value_node.name, "other_value")
        self.assertEqual(value_node.downstream[0].reference_node_name, "sub")
        self.assertEqual(value_node.downstream[0].downstream[0].name, "sample_data.value")
        self.assertEqual(value_node.downstream[0].downstream[0].source.name, "sample_data")

        self.assertEqual(category_node.name, "category")
        self.assertEqual(category_node.downstream[0].reference_node_name, "sub")
        self.assertEqual(category_node.downstream[0].downstream[0].name, "sample_data.category")
        self.assertEqual(category_node.downstream[0].downstream[0].source.name, "sample_data")

    def test_unpivot_with_correlated_subquery(self) -> None:
        sql = """
       SELECT 
           value,
           category,
           (
               SELECT MAX(value)
               FROM sample_data s2
               UNPIVOT (
                   value
                   FOR category IN (a, b)
               )
               WHERE s2.category = s1.category
           ) as max_value
       FROM sample_data s1
       UNPIVOT (
           value
           FOR category IN (a, b)
       );
       """
        # Track lineage with correlated subquery
        outer_value_node = lineage("value", sql)
        category_node = lineage("category", sql)
        max_value_node = lineage("max_value", sql)

        # Verify lineage tracking across query contexts
        self.assertEqual(outer_value_node.name, "value")
        self.assertIsInstance(outer_value_node.downstream[0].source, Placeholder)
        self.assertEqual(outer_value_node.downstream[0].name, "sample_data.value")

        self.assertEqual(category_node.name, "category")
        self.assertIsInstance(category_node.downstream[0].source, Placeholder)
        self.assertEqual(category_node.downstream[0].name, "sample_data.category")

        self.assertEqual(max_value_node.name, "max_value")
        self.assertIsInstance(max_value_node.downstream[0].source, Select)
        self.assertEqual(max_value_node.downstream[0].downstream[0].source.name, "sample_data")

    def test_unpivot_subquery_in_from(self) -> None:
        sql = """
       SELECT 
           t1.value as main_value,
           t1.category as main_category,
           t2.value as sub_value
       FROM (
           SELECT value, category
           FROM main_data
           UNPIVOT (
               value
               FOR category IN (a, b)
           )
       ) t1
       LEFT JOIN (
           SELECT value, category
           FROM sub_data
           UNPIVOT (
               value
               FOR category IN (x, y)
           )
       ) t2 ON t1.category = t2.category;
       """
        # Track lineage from multiple subqueries in FROM clause
        main_value_node = lineage("main_value", sql)
        main_category_node = lineage("main_category", sql)
        sub_value_node = lineage("sub_value", sql)

        # Verify source tables through subquery structure
        self.assertEqual(main_value_node.name, "main_value")
        self.assertEqual(main_value_node.downstream[0].reference_node_name, "t1")
        self.assertEqual(main_value_node.downstream[0].downstream[0].name, "main_data.value")
        self.assertEqual(main_value_node.downstream[0].downstream[0].source.name, "main_data")

        self.assertEqual(main_category_node.name, "main_category")
        self.assertEqual(main_category_node.downstream[0].reference_node_name, "t1")
        self.assertEqual(main_category_node.downstream[0].downstream[0].name, "main_data.category")
        self.assertEqual(main_category_node.downstream[0].downstream[0].source.name, "main_data")

        self.assertEqual(sub_value_node.name, "sub_value")
        self.assertEqual(sub_value_node.downstream[0].reference_node_name, "t2")
        self.assertEqual(sub_value_node.downstream[0].downstream[0].name, "sub_data.value")
        self.assertEqual(sub_value_node.downstream[0].downstream[0].source.name, "sub_data")
