import unittest
from functools import partial

from sqlglot import exp, optimizer, parse_one, table
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.schema import MappingSchema, ensure_schema
from sqlglot.optimizer.scope import build_scope, traverse_scope
from tests.helpers import TPCH_SCHEMA, load_sql_fixture_pairs, load_sql_fixtures


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
            "z": {
                "b": "INT",
                "c": "INT",
            },
        }

    def check_file(self, file, func, pretty=False, **kwargs):
        for i, (meta, sql, expected) in enumerate(load_sql_fixture_pairs(f"optimizer/{file}.sql"), start=1):
            dialect = meta.get("dialect")
            leave_tables_isolated = meta.get("leave_tables_isolated")

            func_kwargs = {**kwargs}
            if leave_tables_isolated is not None:
                func_kwargs["leave_tables_isolated"] = leave_tables_isolated.lower() in ("true", "1")

            with self.subTest(f"{i}, {sql}"):
                self.assertEqual(
                    func(parse_one(sql, read=dialect), **func_kwargs).sql(pretty=pretty, dialect=dialect),
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
                parse_one("x AND (y OR z)"),
                dnf=True,
            ).sql(),
            "(x AND y) OR (x AND z)",
        )

        self.check_file(
            "normalize",
            optimizer.normalize.normalize,
        )

    def test_qualify_columns(self):
        def qualify_columns(expression, **kwargs):
            expression = optimizer.qualify_tables.qualify_tables(expression)
            expression = optimizer.qualify_columns.qualify_columns(expression, **kwargs)
            return expression

        self.check_file("qualify_columns", qualify_columns, schema=self.schema)

    def test_qualify_columns__invalid(self):
        for sql in load_sql_fixtures("optimizer/qualify_columns__invalid.sql"):
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    optimizer.qualify_columns.qualify_columns(parse_one(sql), schema=self.schema)

    def test_quote_identities(self):
        self.check_file("quote_identities", optimizer.quote_identities.quote_identities)

    def test_pushdown_projection(self):
        def pushdown_projections(expression, **kwargs):
            expression = optimizer.qualify_tables.qualify_tables(expression)
            expression = optimizer.qualify_columns.qualify_columns(expression, **kwargs)
            expression = optimizer.pushdown_projections.pushdown_projections(expression)
            return expression

        self.check_file("pushdown_projections", pushdown_projections, schema=self.schema)

    def test_simplify(self):
        self.check_file("simplify", optimizer.simplify.simplify)

    def test_unnest_subqueries(self):
        self.check_file(
            "unnest_subqueries",
            optimizer.unnest_subqueries.unnest_subqueries,
            pretty=True,
        )

    def test_pushdown_predicates(self):
        self.check_file("pushdown_predicates", optimizer.pushdown_predicates.pushdown_predicates)

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

    def test_merge_subqueries(self):
        optimize = partial(
            optimizer.optimize,
            rules=[
                optimizer.qualify_tables.qualify_tables,
                optimizer.qualify_columns.qualify_columns,
                optimizer.merge_subqueries.merge_subqueries,
            ],
        )

        self.check_file("merge_subqueries", optimize, schema=self.schema)

    def test_eliminate_subqueries(self):
        self.check_file("eliminate_subqueries", optimizer.eliminate_subqueries.eliminate_subqueries)

    def test_tpch(self):
        self.check_file("tpc-h/tpc-h", optimizer.optimize, schema=TPCH_SCHEMA, pretty=True)

    def test_schema(self):
        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                }
            }
        )
        self.assertEqual(
            schema.column_names(
                table(
                    "x",
                )
            ),
            ["a"],
        )
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2"))

        schema = ensure_schema(
            {
                "db": {
                    "x": {
                        "a": "uint64",
                    }
                }
            }
        )
        self.assertEqual(schema.column_names(table("x", db="db")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2", db="db"))

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
        self.assertEqual(schema.column_names(table("x", db="db", catalog="c")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2", db="db"))

        schema = ensure_schema(
            MappingSchema(
                {
                    "x": {
                        "a": "uint64",
                    }
                }
            )
        )
        self.assertEqual(schema.column_names(table("x")), ["a"])

        with self.assertRaises(OptimizeError):
            ensure_schema({})

    def test_file_schema(self):
        expression = parse_one(
            """
            SELECT *
            FROM READ_CSV('tests/fixtures/optimizer/tpc-h/nation.csv.gz', 'delimiter', '|')
            """
        )
        self.assertEqual(
            """
SELECT
  "_q_0"."n_nationkey" AS "n_nationkey",
  "_q_0"."n_name" AS "n_name",
  "_q_0"."n_regionkey" AS "n_regionkey",
  "_q_0"."n_comment" AS "n_comment"
FROM READ_CSV('tests/fixtures/optimizer/tpc-h/nation.csv.gz', 'delimiter', '|') AS "_q_0"
""".strip(),
            optimizer.optimize(expression).sql(pretty=True),
        )

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
        for scopes in traverse_scope(parse_one(sql)), list(build_scope(parse_one(sql)).traverse()):
            self.assertEqual(len(scopes), 5)
            self.assertEqual(scopes[0].expression.sql(), "SELECT x.b FROM x")
            self.assertEqual(scopes[1].expression.sql(), "SELECT y.b FROM y")
            self.assertEqual(scopes[2].expression.sql(), "SELECT MAX(x.a) FROM x WHERE x.b = s.b")
            self.assertEqual(scopes[3].expression.sql(), "SELECT y.c AS b FROM y")
            self.assertEqual(scopes[4].expression.sql(), parse_one(sql).sql())

            self.assertEqual(set(scopes[4].sources), {"q", "r", "s"})
            self.assertEqual(len(scopes[4].columns), 6)
            self.assertEqual(set(c.table for c in scopes[4].columns), {"r", "s"})
            self.assertEqual(scopes[4].source_columns("q"), [])
            self.assertEqual(len(scopes[4].source_columns("r")), 2)
            self.assertEqual(set(c.table for c in scopes[4].source_columns("r")), {"r"})

    def test_literal_type_annotation(self):
        tests = {
            "SELECT 5": exp.DataType.Type.INT,
            "SELECT 5.3": exp.DataType.Type.DOUBLE,
            "SELECT 'bla'": exp.DataType.Type.VARCHAR,
            "5": exp.DataType.Type.INT,
            "5.3": exp.DataType.Type.DOUBLE,
            "'bla'": exp.DataType.Type.VARCHAR,
        }

        for sql, target_type in tests.items():
            expression = parse_one(sql)
            annotated_expression = annotate_types(expression)

            self.assertEqual(annotated_expression.find(exp.Literal).type, target_type)

    def test_boolean_type_annotation(self):
        tests = {
            "SELECT TRUE": exp.DataType.Type.BOOLEAN,
            "FALSE": exp.DataType.Type.BOOLEAN,
        }

        for sql, target_type in tests.items():
            expression = parse_one(sql)
            annotated_expression = annotate_types(expression)

            self.assertEqual(annotated_expression.find(exp.Boolean).type, target_type)

    def test_cast_type_annotation(self):
        expression = parse_one("CAST('2020-01-01' AS TIMESTAMPTZ(9))")
        annotate_types(expression)

        self.assertEqual(expression.type, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.this.type, exp.DataType.Type.VARCHAR)
        self.assertEqual(expression.args["to"].type, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.args["to"].expressions[0].type, exp.DataType.Type.INT)

    def test_cache_annotation(self):
        expression = parse_one("CACHE LAZY TABLE x OPTIONS('storageLevel' = 'value') AS SELECT 1")
        annotated_expression = annotate_types(expression)

        self.assertEqual(annotated_expression.expression.expressions[0].type, exp.DataType.Type.INT)

    def test_binary_annotation(self):
        expression = parse_one("SELECT 0.0 + (2 + 3)")
        annotate_types(expression)

        expression = expression.expressions[0]

        self.assertEqual(expression.type, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.left.type, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.right.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.left.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.right.type, exp.DataType.Type.INT)
