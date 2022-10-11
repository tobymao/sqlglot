import unittest
from functools import partial

import duckdb
from pandas.testing import assert_frame_equal

import sqlglot
from sqlglot import exp, optimizer, parse_one, table
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.schema import MappingSchema, ensure_schema
from sqlglot.optimizer.scope import build_scope, traverse_scope, walk_in_scope
from tests.helpers import (
    TPCH_SCHEMA,
    load_sql_fixture_pairs,
    load_sql_fixtures,
    string_to_bool,
)


class TestOptimizer(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect()
        cls.conn.execute(
            """
        CREATE TABLE x (a INT, b INT);
        CREATE TABLE y (b INT, c INT);
        CREATE TABLE z (b INT, c INT);
        
        INSERT INTO x VALUES (1, 1);
        INSERT INTO x VALUES (2, 2);
        INSERT INTO x VALUES (2, 2);
        INSERT INTO x VALUES (3, 3);
        INSERT INTO x VALUES (null, null);
        
        INSERT INTO y VALUES (2, 2);
        INSERT INTO y VALUES (2, 2);
        INSERT INTO y VALUES (3, 3);
        INSERT INTO y VALUES (4, 4);
        INSERT INTO y VALUES (null, null);
        
        INSERT INTO y VALUES (3, 3);
        INSERT INTO y VALUES (3, 3);
        INSERT INTO y VALUES (4, 4);
        INSERT INTO y VALUES (5, 5);
        INSERT INTO y VALUES (null, null);
        """
        )

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

    def check_file(self, file, func, pretty=False, execute=False, **kwargs):
        for i, (meta, sql, expected) in enumerate(load_sql_fixture_pairs(f"optimizer/{file}.sql"), start=1):
            title = meta.get("title") or f"{i}, {sql}"
            dialect = meta.get("dialect")
            leave_tables_isolated = meta.get("leave_tables_isolated")

            func_kwargs = {**kwargs}
            if leave_tables_isolated is not None:
                func_kwargs["leave_tables_isolated"] = string_to_bool(leave_tables_isolated)

            optimized = func(parse_one(sql, read=dialect), **func_kwargs)

            with self.subTest(title):
                self.assertEqual(
                    optimized.sql(pretty=pretty, dialect=dialect),
                    expected,
                )

            should_execute = meta.get("execute")
            if should_execute is None:
                should_execute = execute

            if string_to_bool(should_execute):
                with self.subTest(f"(execute) {title}"):
                    df1 = self.conn.execute(sqlglot.transpile(sql, read=dialect, write="duckdb")[0]).df()
                    df2 = self.conn.execute(optimized.sql(pretty=pretty, dialect="duckdb")).df()
                    assert_frame_equal(df1, df2)

    def test_optimize(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
            "z": {"a": "INT", "c": "INT"},
        }

        self.check_file("optimizer", optimizer.optimize, pretty=True, execute=True, schema=schema)

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

        self.check_file("qualify_columns", qualify_columns, execute=True, schema=self.schema)

    def test_qualify_columns__with_invisible(self):
        def qualify_columns(expression, **kwargs):
            expression = optimizer.qualify_tables.qualify_tables(expression)
            expression = optimizer.qualify_columns.qualify_columns(expression, **kwargs)
            return expression

        schema = MappingSchema(self.schema, {"x": {"a"}, "y": {"b"}, "z": {"b"}})
        self.check_file("qualify_columns__with_invisible", qualify_columns, schema=schema)

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

    def test_eliminate_joins(self):
        self.check_file(
            "eliminate_joins",
            optimizer.eliminate_joins.eliminate_joins,
            pretty=True,
        )

    def test_eliminate_ctes(self):
        self.check_file(
            "eliminate_ctes",
            optimizer.eliminate_ctes.eliminate_ctes,
            pretty=True,
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

        self.check_file("merge_subqueries", optimize, execute=True, schema=self.schema)

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
        expression = parse_one(sql)
        for scopes in traverse_scope(expression), list(build_scope(expression).traverse()):
            self.assertEqual(len(scopes), 5)
            self.assertEqual(scopes[0].expression.sql(), "SELECT x.b FROM x")
            self.assertEqual(scopes[1].expression.sql(), "SELECT y.b FROM y")
            self.assertEqual(scopes[2].expression.sql(), "SELECT y.c AS b FROM y")
            self.assertEqual(scopes[3].expression.sql(), "SELECT MAX(x.a) FROM x WHERE x.b = s.b")
            self.assertEqual(scopes[4].expression.sql(), parse_one(sql).sql())

            self.assertEqual(set(scopes[4].sources), {"q", "r", "s"})
            self.assertEqual(len(scopes[4].columns), 6)
            self.assertEqual(set(c.table for c in scopes[4].columns), {"r", "s"})
            self.assertEqual(scopes[4].source_columns("q"), [])
            self.assertEqual(len(scopes[4].source_columns("r")), 2)
            self.assertEqual(set(c.table for c in scopes[4].source_columns("r")), {"r"})

            self.assertEqual({c.sql() for c in scopes[-1].find_all(exp.Column)}, {"r.b", "s.b"})
            self.assertEqual(scopes[-1].find(exp.Column).sql(), "r.b")
            self.assertEqual({c.sql() for c in scopes[0].find_all(exp.Column)}, {"x.b"})

        # Check that we can walk in scope from an arbitrary node
        self.assertEqual(
            {node.sql() for node, *_ in walk_in_scope(expression.find(exp.Where)) if isinstance(node, exp.Column)},
            {"s.b"},
        )

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
            expression = annotate_types(parse_one(sql))
            self.assertEqual(expression.find(exp.Literal).type, target_type)

    def test_boolean_type_annotation(self):
        tests = {
            "SELECT TRUE": exp.DataType.Type.BOOLEAN,
            "FALSE": exp.DataType.Type.BOOLEAN,
        }

        for sql, target_type in tests.items():
            expression = annotate_types(parse_one(sql))
            self.assertEqual(expression.find(exp.Boolean).type, target_type)

    def test_cast_type_annotation(self):
        expression = annotate_types(parse_one("CAST('2020-01-01' AS TIMESTAMPTZ(9))"))

        self.assertEqual(expression.type, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.this.type, exp.DataType.Type.VARCHAR)
        self.assertEqual(expression.args["to"].type, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.args["to"].expressions[0].type, exp.DataType.Type.INT)

    def test_cache_annotation(self):
        expression = annotate_types(parse_one("CACHE LAZY TABLE x OPTIONS('storageLevel' = 'value') AS SELECT 1"))
        self.assertEqual(expression.expression.expressions[0].type, exp.DataType.Type.INT)

    def test_binary_annotation(self):
        expression = annotate_types(parse_one("SELECT 0.0 + (2 + 3)")).expressions[0]

        self.assertEqual(expression.type, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.left.type, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.right.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.left.type, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.right.type, exp.DataType.Type.INT)

    def test_derived_tables_column_annotation(self):
        schema = {"x": {"cola": "INT"}, "y": {"cola": "FLOAT"}}
        sql = """
            SELECT a.cola AS cola
            FROM (
                SELECT x.cola + y.cola AS cola
                FROM (
                    SELECT x.cola AS cola
                    FROM x AS x
                ) AS x
                JOIN (
                    SELECT y.cola AS cola
                    FROM y AS y
                ) AS y
            ) AS a
        """

        expression = annotate_types(parse_one(sql), schema=schema)
        self.assertEqual(expression.expressions[0].type, exp.DataType.Type.FLOAT)  # a.cola AS cola

        addition_alias = expression.args["from"].expressions[0].this.expressions[0]
        self.assertEqual(addition_alias.type, exp.DataType.Type.FLOAT)  # x.cola + y.cola AS cola

        addition = addition_alias.this
        self.assertEqual(addition.type, exp.DataType.Type.FLOAT)
        self.assertEqual(addition.this.type, exp.DataType.Type.INT)
        self.assertEqual(addition.expression.type, exp.DataType.Type.FLOAT)

    def test_cte_column_annotation(self):
        schema = {"x": {"cola": "CHAR"}, "y": {"colb": "TEXT"}}
        sql = """
            WITH tbl AS (
                SELECT x.cola + 'bla' AS cola, y.colb AS colb
                FROM (
                    SELECT x.cola AS cola
                    FROM x AS x
                ) AS x
                JOIN (
                    SELECT y.colb AS colb
                    FROM y AS y
                ) AS y
            )
            SELECT tbl.cola + tbl.colb + 'foo' AS col
            FROM tbl AS tbl
        """

        expression = annotate_types(parse_one(sql), schema=schema)
        self.assertEqual(expression.expressions[0].type, exp.DataType.Type.TEXT)  # tbl.cola + tbl.colb + 'foo' AS col

        outer_addition = expression.expressions[0].this  # (tbl.cola + tbl.colb) + 'foo'
        self.assertEqual(outer_addition.type, exp.DataType.Type.TEXT)
        self.assertEqual(outer_addition.left.type, exp.DataType.Type.TEXT)
        self.assertEqual(outer_addition.right.type, exp.DataType.Type.VARCHAR)

        inner_addition = expression.expressions[0].this.left  # tbl.cola + tbl.colb
        self.assertEqual(inner_addition.left.type, exp.DataType.Type.VARCHAR)
        self.assertEqual(inner_addition.right.type, exp.DataType.Type.TEXT)

        cte_select = expression.args["with"].expressions[0].this
        self.assertEqual(cte_select.expressions[0].type, exp.DataType.Type.VARCHAR)  # x.cola + 'bla' AS cola
        self.assertEqual(cte_select.expressions[1].type, exp.DataType.Type.TEXT)  # y.colb AS colb

        cte_select_addition = cte_select.expressions[0].this  # x.cola + 'bla'
        self.assertEqual(cte_select_addition.type, exp.DataType.Type.VARCHAR)
        self.assertEqual(cte_select_addition.left.type, exp.DataType.Type.CHAR)
        self.assertEqual(cte_select_addition.right.type, exp.DataType.Type.VARCHAR)

        # Check that x.cola AS cola and y.colb AS colb have types CHAR and TEXT, respectively
        for d, t in zip(cte_select.find_all(exp.Subquery), [exp.DataType.Type.CHAR, exp.DataType.Type.TEXT]):
            self.assertEqual(d.this.expressions[0].this.type, t)

    def test_function_annotation(self):
        schema = {"x": {"cola": "VARCHAR", "colb": "CHAR"}}
        sql = "SELECT x.cola || TRIM(x.colb) AS col FROM x AS x"

        concat_expr_alias = annotate_types(parse_one(sql), schema=schema).expressions[0]
        self.assertEqual(concat_expr_alias.type, exp.DataType.Type.VARCHAR)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type, exp.DataType.Type.VARCHAR)
        self.assertEqual(concat_expr.left.type, exp.DataType.Type.VARCHAR)  # x.cola
        self.assertEqual(concat_expr.right.type, exp.DataType.Type.VARCHAR)  # TRIM(x.colb)
        self.assertEqual(concat_expr.right.this.type, exp.DataType.Type.CHAR)  # x.colb

    def test_unknown_annotation(self):
        schema = {"x": {"cola": "VARCHAR"}}
        sql = "SELECT x.cola || SOME_ANONYMOUS_FUNC(x.cola) AS col FROM x AS x"

        concat_expr_alias = annotate_types(parse_one(sql), schema=schema).expressions[0]
        self.assertEqual(concat_expr_alias.type, exp.DataType.Type.UNKNOWN)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type, exp.DataType.Type.UNKNOWN)
        self.assertEqual(concat_expr.left.type, exp.DataType.Type.VARCHAR)  # x.cola
        self.assertEqual(concat_expr.right.type, exp.DataType.Type.UNKNOWN)  # SOME_ANONYMOUS_FUNC(x.cola)
        self.assertEqual(concat_expr.right.expressions[0].type, exp.DataType.Type.VARCHAR)  # x.cola (arg)

    def test_null_annotation(self):
        expression = annotate_types(parse_one("SELECT NULL + 2 AS col")).expressions[0].this
        self.assertEqual(expression.left.type, exp.DataType.Type.NULL)
        self.assertEqual(expression.right.type, exp.DataType.Type.INT)

        # NULL <op> UNKNOWN should yield NULL
        sql = "SELECT NULL || SOME_ANONYMOUS_FUNC() AS result"

        concat_expr_alias = annotate_types(parse_one(sql)).expressions[0]
        self.assertEqual(concat_expr_alias.type, exp.DataType.Type.NULL)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type, exp.DataType.Type.NULL)
        self.assertEqual(concat_expr.left.type, exp.DataType.Type.NULL)
        self.assertEqual(concat_expr.right.type, exp.DataType.Type.UNKNOWN)

    def test_nullable_annotation(self):
        nullable = exp.DataType.build("NULLABLE", expressions=exp.DataType.build("BOOLEAN"))
        expression = annotate_types(parse_one("NULL AND FALSE"))

        self.assertEqual(expression.type, nullable)
        self.assertEqual(expression.left.type, exp.DataType.Type.NULL)
        self.assertEqual(expression.right.type, exp.DataType.Type.BOOLEAN)
