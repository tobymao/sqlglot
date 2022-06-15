import unittest

import sqlglot
from sqlglot import optimizer
from sqlglot.optimizer.schema import ensure_schema, MappingSchema
from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.scope import traverse_scope
from tests.helpers import (
    load_sql_fixture_pairs,
    load_sql_fixtures,
    TPCH_SCHEMA,
)


class TestOptimizer(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.schema = {
            "x": {
                "a": "INT",
                "b": "INT",
            },
            "y": {
                "b": "INT",
                "c": "INT",
            },
        }

    def check_file(self, file, func, pretty=False, **kwargs):
        for sql, expected in load_sql_fixture_pairs(f"optimizer/{file}.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    func(sqlglot.parse_one(sql), **kwargs).sql(pretty=pretty),
                    expected,
                )

    def test_optimize(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"a": "INT", "b": "INT"},
            "z": {"a": "INT", "c": "INT"},
        }

        self.check_file("optimizer", optimizer.optimize, pretty=True, schema=schema)

    def test_isolate_table_selects(self):
        self.check_file(
            "isolate_table_selects",
            optimizer.isolate_table_selects.isolate_table_selects,
        )

    def test_qualify_tables(self):
        self.check_file(
            "qualify_tables",
            optimizer.qualify_tables.qualify_tables,
            db="db",
            catalog="c",
        )

    def test_normalize(self):
        self.assertEqual(
            optimizer.normalize.normalize(
                sqlglot.parse_one("x AND (y OR z)"),
                dnf=True,
            ).sql(),
            "(x AND y) OR (x AND z)",
        )

        self.check_file(
            "normalize",
            optimizer.normalize.normalize,
        )

    def test_qualify_columns(self):
        self.check_file(
            "qualify_columns",
            optimizer.qualify_columns.qualify_columns,
            schema=self.schema,
        )

    def test_qualify_columns__invalid(self):
        for sql in load_sql_fixtures("optimizer/qualify_columns__invalid.sql"):
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    optimizer.qualify_columns.qualify_columns(
                        sqlglot.parse_one(sql), schema=self.schema
                    )

    def test_quote_identities(self):
        self.check_file("quote_identities", optimizer.quote_identities.quote_identities)

    def test_pushdown_projection(self):
        def pushdown_projections(expression, **kwargs):
            expression = optimizer.qualify_columns.qualify_columns(expression, **kwargs)
            expression = optimizer.pushdown_projections.pushdown_projections(expression)
            return expression

        self.check_file(
            "pushdown_projections", pushdown_projections, schema=self.schema
        )

    def test_simplify(self):
        self.check_file("simplify", optimizer.simplify.simplify)

    def test_decorrelate_subqueries(self):
        self.check_file(
            "decorrelate_subqueries",
            optimizer.decorrelate_subqueries.decorrelate_subqueries,
        )

    def test_pushdown_predicates(self):
        self.check_file(
            "pushdown_predicates", optimizer.pushdown_predicates.pushdown_predicates
        )

    def test_expand_multi_table_selects(self):
        self.check_file(
            "expand_multi_table_selects",
            optimizer.expand_multi_table_selects.expand_multi_table_selects,
        )

    def test_optimize_joins(self):
        self.check_file(
            "optimize_joins",
            optimizer.optimize_joins.optimize_joins,
        )

    def test_eliminate_subqueries(self):
        self.check_file(
            "eliminate_subqueries",
            optimizer.eliminate_subqueries.eliminate_subqueries,
            pretty=True,
        )

    def test_tpch(self):
        self.check_file(
            "tpc-h/tpc-h", optimizer.optimize, schema=TPCH_SCHEMA, pretty=True
        )

    def test_schema(self):
        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                }
            }
        )
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2"))

        schema = ensure_schema(
            {
                "db": {
                    "x": {
                        "a": "uint64",
                    }
                }
            }
        )
        self.assertEqual(schema.column_names(exp.Table(this="x", db="db")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2", db="db"))

        schema = ensure_schema(
            {
                "c": {
                    "db": {
                        "x": {
                            "a": "uint64",
                        }
                    }
                }
            }
        )
        self.assertEqual(
            schema.column_names(exp.Table(this="x", db="db", catalog="c")), ["a"]
        )
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2", db="db"))

        schema = ensure_schema(
            MappingSchema(
                {
                    "x": {
                        "a": "uint64",
                    }
                }
            )
        )
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["a"])

        with self.assertRaises(OptimizeError):
            ensure_schema({})

    def test_scope(self):
        sql = """
        WITH q AS (
          SELECT x.b FROM x
        ), r AS (
          SELECT y.b FROM y
        )
        SELECT
          r.b,
          s.b
        FROM r
        JOIN (
          SELECT y.c AS b FROM y
        ) s
        ON s.b = r.b
        WHERE s.b > (SELECT MAX(x.a) FROM x WHERE x.b = s.b)
        """
        scopes = traverse_scope(sqlglot.parse_one(sql))
        self.assertEqual(len(scopes), 5)
        self.assertEqual(scopes[0].expression.sql(), "SELECT x.b FROM x")
        self.assertEqual(scopes[1].expression.sql(), "SELECT y.b FROM y")
        self.assertEqual(
            scopes[2].expression.sql(), "SELECT MAX(x.a) FROM x WHERE x.b = s.b"
        )
        self.assertEqual(scopes[3].expression.sql(), "SELECT y.c AS b FROM y")
        self.assertEqual(scopes[4].expression.sql(), sqlglot.parse_one(sql).sql())

        self.assertEqual(set(scopes[4].sources), {"q", "r", "s"})
        self.assertEqual(len(scopes[4].columns), 6)
        self.assertEqual(set(c.text("table") for c in scopes[4].columns), {"r", "s"})
        self.assertEqual(scopes[4].source_columns("q"), [])
        self.assertEqual(len(scopes[4].source_columns("r")), 2)
        self.assertEqual(
            set(c.text("table") for c in scopes[4].source_columns("r")), {"r"}
        )
