import unittest
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from unittest.mock import patch

import duckdb
from pandas.testing import assert_frame_equal

import sqlglot
from sqlglot import exp, optimizer, parse_one
from sqlglot.errors import OptimizeError, SchemaError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.scope import build_scope, traverse_scope, walk_in_scope
from sqlglot.schema import MappingSchema
from tests.helpers import (
    TPCDS_SCHEMA,
    TPCH_SCHEMA,
    assert_logger_contains,
    load_sql_fixture_pairs,
    load_sql_fixtures,
    string_to_bool,
)


def parse_and_optimize(func, sql, read_dialect, **kwargs):
    return func(parse_one(sql, read=read_dialect), **kwargs)


def qualify_columns(expression, **kwargs):
    expression = optimizer.qualify.qualify(
        expression, infer_schema=True, validate_qualify_columns=False, identify=False, **kwargs
    )
    return expression


def pushdown_projections(expression, **kwargs):
    expression = optimizer.qualify_tables.qualify_tables(expression)
    expression = optimizer.qualify_columns.qualify_columns(expression, infer_schema=True, **kwargs)
    expression = optimizer.pushdown_projections.pushdown_projections(expression, **kwargs)
    return expression


def normalize(expression, **kwargs):
    expression = optimizer.normalize.normalize(expression, dnf=False)
    return optimizer.simplify.simplify(expression)


def simplify(expression, **kwargs):
    return optimizer.simplify.simplify(expression, constant_propagation=True, **kwargs)


class TestOptimizer(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        sqlglot.schema = MappingSchema()
        cls.conn = duckdb.connect()
        cls.conn.execute(
            """
        CREATE TABLE x (a INT, b INT);
        CREATE TABLE y (b INT, c INT);
        CREATE TABLE z (b INT, c INT);
        CREATE TABLE w (d TEXT, e TEXT);

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

        INSERT INTO w VALUES ('a', 'b');
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
            "w": {
                "d": "TEXT",
                "e": "TEXT",
            },
            "temporal": {
                "d": "DATE",
                "t": "DATETIME",
            },
        }

    def check_file(
        self, file, func, pretty=False, execute=False, set_dialect=False, only=None, **kwargs
    ):
        with ProcessPoolExecutor() as pool:
            results = {}

            for i, (meta, sql, expected) in enumerate(
                load_sql_fixture_pairs(f"optimizer/{file}.sql"), start=1
            ):
                title = meta.get("title") or f"{i}, {sql}"
                if only and title != only:
                    continue
                dialect = meta.get("dialect")
                leave_tables_isolated = meta.get("leave_tables_isolated")

                func_kwargs = {**kwargs}
                if leave_tables_isolated is not None:
                    func_kwargs["leave_tables_isolated"] = string_to_bool(leave_tables_isolated)

                if set_dialect and dialect:
                    func_kwargs["dialect"] = dialect

                future = pool.submit(parse_and_optimize, func, sql, dialect, **func_kwargs)
                results[future] = (
                    sql,
                    title,
                    expected,
                    dialect,
                    execute if meta.get("execute") is None else False,
                )

        for future in as_completed(results):
            sql, title, expected, dialect, execute = results[future]

            with self.subTest(title):
                optimized = future.result()
                actual = optimized.sql(pretty=pretty, dialect=dialect)
                self.assertEqual(
                    expected,
                    actual,
                )

            if string_to_bool(execute):
                with self.subTest(f"(execute) {title}"):
                    df1 = self.conn.execute(
                        sqlglot.transpile(sql, read=dialect, write="duckdb")[0]
                    ).df()
                    df2 = self.conn.execute(optimized.sql(dialect="duckdb")).df()
                    assert_frame_equal(df1, df2)

    @patch("sqlglot.generator.logger")
    def test_optimize(self, logger):
        self.assertEqual(optimizer.optimize("x = 1 + 1", identify=None).sql(), "x = 2")

        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
            "z": {"a": "INT", "c": "INT"},
            "u": {"f": "INT", "g": "INT", "h": "TEXT"},
        }

        self.check_file(
            "optimizer",
            optimizer.optimize,
            infer_schema=True,
            pretty=True,
            execute=True,
            schema=schema,
            set_dialect=True,
        )

    def test_isolate_table_selects(self):
        self.check_file(
            "isolate_table_selects",
            optimizer.isolate_table_selects.isolate_table_selects,
            schema=self.schema,
        )

    def test_qualify_tables(self):
        self.assertEqual(
            optimizer.qualify_tables.qualify_tables(
                parse_one("select a from b"), catalog="catalog"
            ).sql(),
            "SELECT a FROM b AS b",
        )

        self.assertEqual(
            optimizer.qualify_tables.qualify_tables(parse_one("select a from b"), db='"DB"').sql(),
            'SELECT a FROM "DB".b AS b',
        )

        self.check_file(
            "qualify_tables",
            optimizer.qualify_tables.qualify_tables,
            db="db",
            catalog="c",
            set_dialect=True,
        )

    def test_normalize(self):
        self.assertEqual(
            optimizer.normalize.normalize(
                parse_one("x AND (y OR z)"),
                dnf=True,
            ).sql(),
            "(x AND y) OR (x AND z)",
        )

        self.assertEqual(
            optimizer.normalize.normalize(
                parse_one("x AND (y OR z)"),
            ).sql(),
            "x AND (y OR z)",
        )

        self.check_file("normalize", normalize)

    @patch("sqlglot.generator.logger")
    def test_qualify_columns(self, logger):
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one("WITH x AS (SELECT a FROM db.y) SELECT * FROM db.x"),
                schema={"db": {"x": {"z": "int"}, "y": {"a": "int"}}},
                expand_stars=False,
            ).sql(),
            "WITH x AS (SELECT y.a AS a FROM db.y) SELECT * FROM db.x",
        )

        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one("WITH x AS (SELECT a FROM db.y) SELECT z FROM db.x"),
                schema={"db": {"x": {"z": "int"}, "y": {"a": "int"}}},
                infer_schema=False,
            ).sql(),
            "WITH x AS (SELECT y.a AS a FROM db.y) SELECT x.z AS z FROM db.x",
        )

        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one("select y from x"),
                schema={},
                infer_schema=False,
            ).sql(),
            "SELECT y AS y FROM x",
        )

        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "WITH X AS (SELECT Y.A FROM DB.y CROSS JOIN a.b.INFORMATION_SCHEMA.COLUMNS) SELECT `A` FROM X",
                    read="bigquery",
                ),
                dialect="bigquery",
            ).sql(),
            'WITH "x" AS (SELECT "y"."a" AS "a" FROM "DB"."y" AS "y" CROSS JOIN "a"."b"."INFORMATION_SCHEMA"."COLUMNS" AS "COLUMNS") SELECT "x"."a" AS "a" FROM "x" AS "x"',
        )

        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "CREATE FUNCTION udfs.`myTest`(`x` FLOAT64) AS (1)",
                    read="bigquery",
                ),
                dialect="bigquery",
            ).sql(dialect="bigquery"),
            "CREATE FUNCTION `udfs`.`myTest`(`x` FLOAT64) AS (1)",
        )

        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one("SELECT `bar_bazfoo_$id` FROM test", read="spark"),
                schema={"test": {"bar_bazFoo_$id": "BIGINT"}},
                dialect="spark",
            ).sql(dialect="spark"),
            "SELECT `test`.`bar_bazfoo_$id` AS `bar_bazfoo_$id` FROM `test` AS `test`",
        )

        qualified = optimizer.qualify.qualify(
            parse_one("WITH t AS (SELECT 1 AS c) (SELECT c FROM t)")
        )
        self.assertIs(qualified.selects[0].parent, qualified.this)
        self.assertEqual(
            qualified.sql(),
            'WITH "t" AS (SELECT 1 AS "c") (SELECT "t"."c" AS "c" FROM "t" AS "t")',
        )

        self.check_file(
            "qualify_columns", qualify_columns, execute=True, schema=self.schema, set_dialect=True
        )
        self.check_file(
            "qualify_columns_ddl", qualify_columns, schema=self.schema, set_dialect=True
        )

    def test_qualify_columns__with_invisible(self):
        schema = MappingSchema(self.schema, {"x": {"a"}, "y": {"b"}, "z": {"b"}})
        self.check_file("qualify_columns__with_invisible", qualify_columns, schema=schema)

    def test_pushdown_cte_alias_columns(self):
        self.check_file(
            "pushdown_cte_alias_columns", optimizer.qualify_columns.pushdown_cte_alias_columns
        )

    def test_qualify_columns__invalid(self):
        for sql in load_sql_fixtures("optimizer/qualify_columns__invalid.sql"):
            with self.subTest(sql):
                with self.assertRaises((OptimizeError, SchemaError)):
                    expression = optimizer.qualify_columns.qualify_columns(
                        parse_one(sql), schema=self.schema
                    )
                    optimizer.qualify_columns.validate_qualify_columns(expression)

    def test_normalize_identifiers(self):
        self.check_file(
            "normalize_identifiers",
            optimizer.normalize_identifiers.normalize_identifiers,
            set_dialect=True,
        )

        self.assertEqual(optimizer.normalize_identifiers.normalize_identifiers("a%").sql(), '"a%"')

    def test_quote_identifiers(self):
        self.check_file(
            "quote_identifiers",
            optimizer.qualify_columns.quote_identifiers,
            set_dialect=True,
        )

    def test_pushdown_projection(self):
        self.check_file("pushdown_projections", pushdown_projections, schema=self.schema)

    def test_simplify(self):
        self.check_file("simplify", simplify, set_dialect=True)

        expression = parse_one("TRUE AND TRUE AND TRUE")
        self.assertEqual(exp.true(), optimizer.simplify.simplify(expression))
        self.assertEqual(exp.true(), optimizer.simplify.simplify(expression.this))

        # CONCAT in (e.g.) Presto is parsed as Concat instead of SafeConcat which is the default type
        # This test checks that simplify_concat preserves the corresponding expression types.
        concat = parse_one("CONCAT('a', x, 'b', 'c')", read="presto")
        simplified_concat = optimizer.simplify.simplify(concat)

        safe_concat = parse_one("CONCAT('a', x, 'b', 'c')")
        simplified_safe_concat = optimizer.simplify.simplify(safe_concat)

        self.assertEqual(simplified_concat.args["safe"], False)
        self.assertEqual(simplified_safe_concat.args["safe"], True)

        self.assertEqual("CONCAT('a', x, 'bc')", simplified_concat.sql(dialect="presto"))
        self.assertEqual("CONCAT('a', x, 'bc')", simplified_safe_concat.sql())

        anon_unquoted_str = parse_one("anonymous(x, y)")
        self.assertEqual(optimizer.simplify.gen(anon_unquoted_str), "ANONYMOUS x,y")

        query = parse_one("SELECT x FROM t")
        self.assertEqual(optimizer.simplify.gen(query), optimizer.simplify.gen(query.copy()))

        anon_unquoted_identifier = exp.Anonymous(
            this=exp.to_identifier("anonymous"), expressions=[exp.column("x"), exp.column("y")]
        )
        self.assertEqual(optimizer.simplify.gen(anon_unquoted_identifier), "ANONYMOUS x,y")

        anon_quoted = parse_one('"anonymous"(x, y)')
        self.assertEqual(optimizer.simplify.gen(anon_quoted), '"anonymous" x,y')

        with self.assertRaises(ValueError) as e:
            anon_invalid = exp.Anonymous(this=5)
            optimizer.simplify.gen(anon_invalid)

        self.assertIn("Anonymous.this expects a str or an Identifier, got 'int'.", str(e.exception))

    def test_unnest_subqueries(self):
        self.check_file(
            "unnest_subqueries",
            optimizer.unnest_subqueries.unnest_subqueries,
            pretty=True,
        )

    def test_pushdown_predicates(self):
        self.check_file("pushdown_predicates", optimizer.pushdown_predicates.pushdown_predicates)

    def test_expand_alias_refs(self):
        # check order of lateral expansion with no schema
        self.assertEqual(
            optimizer.optimize("SELECT a + 1 AS d, d + 1 AS e FROM x WHERE e > 1 GROUP BY e").sql(),
            'SELECT "x"."a" + 1 AS "d", "x"."a" + 1 + 1 AS "e" FROM "x" AS "x" WHERE ("x"."a" + 2) > 1 GROUP BY "x"."a" + 1 + 1',
        )

        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one("SELECT CAST(x AS INT) AS y FROM z AS z"),
                schema={"l": {"c": "int"}},
                infer_schema=False,
            ).sql(),
            "SELECT CAST(x AS INT) AS y FROM z AS z",
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

    @patch("sqlglot.generator.logger")
    def test_merge_subqueries(self, logger):
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

    def test_canonicalize(self):
        optimize = partial(
            optimizer.optimize,
            rules=[
                optimizer.qualify.qualify,
                optimizer.qualify_columns.quote_identifiers,
                annotate_types,
                optimizer.canonicalize.canonicalize,
            ],
        )
        self.check_file("canonicalize", optimize, schema=self.schema)

    def test_tpch(self):
        self.check_file("tpc-h/tpc-h", optimizer.optimize, schema=TPCH_SCHEMA, pretty=True)

    def test_tpcds(self):
        self.check_file("tpc-ds/tpc-ds", optimizer.optimize, schema=TPCDS_SCHEMA, pretty=True)

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
        many_unions = parse_one(" UNION ALL ".join(["SELECT x FROM t"] * 10000))
        scopes_using_traverse = list(build_scope(many_unions).traverse())
        scopes_using_traverse_scope = traverse_scope(many_unions)
        self.assertEqual(len(scopes_using_traverse), len(scopes_using_traverse_scope))
        assert all(
            x.expression is y.expression
            for x, y in zip(scopes_using_traverse, scopes_using_traverse_scope)
        )

        sql = """
        WITH q AS (
          SELECT x.b FROM x
        ), r AS (
          SELECT y.b FROM y
        ), z as (
          SELECT cola, colb FROM (VALUES(1, 'test')) AS tab(cola, colb)
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
            self.assertEqual(len(scopes), 7)
            self.assertEqual(scopes[0].expression.sql(), "SELECT x.b FROM x")
            self.assertEqual(scopes[1].expression.sql(), "SELECT y.b FROM y")
            self.assertEqual(scopes[2].expression.sql(), "(VALUES (1, 'test')) AS tab(cola, colb)")
            self.assertEqual(
                scopes[3].expression.sql(),
                "SELECT cola, colb FROM (VALUES (1, 'test')) AS tab(cola, colb)",
            )
            self.assertEqual(scopes[4].expression.sql(), "SELECT y.c AS b FROM y")
            self.assertEqual(scopes[5].expression.sql(), "SELECT MAX(x.a) FROM x WHERE x.b = s.b")
            self.assertEqual(scopes[6].expression.sql(), parse_one(sql).sql())

            self.assertEqual(set(scopes[6].sources), {"q", "z", "r", "s"})
            self.assertEqual(len(scopes[6].columns), 6)
            self.assertEqual({c.table for c in scopes[6].columns}, {"r", "s"})
            self.assertEqual(scopes[6].source_columns("q"), [])
            self.assertEqual(len(scopes[6].source_columns("r")), 2)
            self.assertEqual({c.table for c in scopes[6].source_columns("r")}, {"r"})

            self.assertEqual({c.sql() for c in scopes[-1].find_all(exp.Column)}, {"r.b", "s.b"})
            self.assertEqual(scopes[-1].find(exp.Column).sql(), "r.b")
            self.assertEqual({c.sql() for c in scopes[0].find_all(exp.Column)}, {"x.b"})

        # Check that we can walk in scope from an arbitrary node
        self.assertEqual(
            {
                node.sql()
                for node, *_ in walk_in_scope(expression.find(exp.Where))
                if isinstance(node, exp.Column)
            },
            {"s.b"},
        )

        # Check that parentheses don't introduce a new scope unless an alias is attached
        sql = "SELECT * FROM (((SELECT * FROM (t1 JOIN t2) AS t3) JOIN (SELECT * FROM t4)))"
        expression = parse_one(sql)
        for scopes in traverse_scope(expression), list(build_scope(expression).traverse()):
            self.assertEqual(len(scopes), 4)

            self.assertEqual(scopes[0].expression.sql(), "t1, t2")
            self.assertEqual(set(scopes[0].sources), {"t1", "t2"})

            self.assertEqual(scopes[1].expression.sql(), "SELECT * FROM (t1, t2) AS t3")
            self.assertEqual(set(scopes[1].sources), {"t3"})

            self.assertEqual(scopes[2].expression.sql(), "SELECT * FROM t4")
            self.assertEqual(set(scopes[2].sources), {"t4"})

            self.assertEqual(
                scopes[3].expression.sql(),
                "SELECT * FROM (((SELECT * FROM (t1, t2) AS t3), (SELECT * FROM t4)))",
            )
            self.assertEqual(set(scopes[3].sources), {""})

        inner_query = "SELECT bar FROM baz"
        for udtf in (f"UNNEST(({inner_query}))", f"LATERAL ({inner_query})"):
            sql = f"SELECT a FROM foo CROSS JOIN {udtf}"
            expression = parse_one(sql)

            for scopes in traverse_scope(expression), list(build_scope(expression).traverse()):
                self.assertEqual(len(scopes), 3)

                self.assertEqual(scopes[0].expression.sql(), inner_query)
                self.assertEqual(set(scopes[0].sources), {"baz"})

                self.assertEqual(scopes[1].expression.sql(), udtf)
                self.assertEqual(set(scopes[1].sources), {"", "foo"})  # foo is a lateral source

                self.assertEqual(scopes[2].expression.sql(), f"SELECT a FROM foo CROSS JOIN {udtf}")
                self.assertEqual(set(scopes[2].sources), {"", "foo"})

    @patch("sqlglot.optimizer.scope.logger")
    def test_scope_warning(self, logger):
        self.assertEqual(len(traverse_scope(parse_one("WITH q AS (@y) SELECT * FROM q"))), 1)
        assert_logger_contains(
            "Cannot traverse scope %s with type '%s'",
            logger,
            level="warning",
        )

    def test_struct_type_annotation(self):
        tests = {
            ("SELECT STRUCT(1 AS col)", "spark"): "STRUCT<col INT>",
            ("SELECT STRUCT(1 AS col, 2.5 AS row)", "spark"): "STRUCT<col INT, row DOUBLE>",
            ("SELECT STRUCT(1)", "bigquery"): "STRUCT<INT>",
            (
                "SELECT STRUCT(1 AS col, 2.5 AS row, struct(3.5 AS inner_col, 4 AS inner_row) AS nested_struct)",
                "spark",
            ): "STRUCT<col INT, row DOUBLE, nested_struct STRUCT<inner_col DOUBLE, inner_row INT>>",
            (
                "SELECT STRUCT(1 AS col, 2.5, ARRAY[1, 2, 3] AS nested_array, 'foo')",
                "bigquery",
            ): "STRUCT<col INT, DOUBLE, nested_array ARRAY<INT>, VARCHAR>",
            ("SELECT STRUCT(1, 2.5, 'bar')", "spark"): "STRUCT<INT, DOUBLE, VARCHAR>",
            ('SELECT STRUCT(1 AS "CaseSensitive")', "spark"): 'STRUCT<"CaseSensitive" INT>',
            ("SELECT STRUCT_PACK(a := 1, b := 2.5)", "duckdb"): "STRUCT<a INT, b DOUBLE>",
            ("SELECT ROW(1, 2.5, 'foo')", "presto"): "STRUCT<INT, DOUBLE, VARCHAR>",
        }

        for (sql, dialect), target_type in tests.items():
            with self.subTest(sql):
                expression = annotate_types(parse_one(sql, read=dialect))
                assert expression.expressions[0].is_type(target_type)

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
            self.assertEqual(expression.find(exp.Literal).type.this, target_type)

    def test_boolean_type_annotation(self):
        tests = {
            "SELECT TRUE": exp.DataType.Type.BOOLEAN,
            "FALSE": exp.DataType.Type.BOOLEAN,
        }

        for sql, target_type in tests.items():
            expression = annotate_types(parse_one(sql))
            self.assertEqual(expression.find(exp.Boolean).type.this, target_type)

    def test_cast_type_annotation(self):
        expression = annotate_types(parse_one("CAST('2020-01-01' AS TIMESTAMPTZ(9))"))
        self.assertEqual(expression.type.this, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.this.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(expression.args["to"].type.this, exp.DataType.Type.TIMESTAMPTZ)
        self.assertEqual(expression.args["to"].expressions[0].this.type.this, exp.DataType.Type.INT)

        expression = annotate_types(parse_one("ARRAY(1)::ARRAY<INT>"))
        self.assertEqual(expression.type, parse_one("ARRAY<INT>", into=exp.DataType))

        expression = annotate_types(parse_one("CAST(x AS INTERVAL)"))
        self.assertEqual(expression.type.this, exp.DataType.Type.INTERVAL)
        self.assertEqual(expression.this.type.this, exp.DataType.Type.UNKNOWN)
        self.assertEqual(expression.args["to"].type.this, exp.DataType.Type.INTERVAL)

    def test_cache_annotation(self):
        expression = annotate_types(
            parse_one("CACHE LAZY TABLE x OPTIONS('storageLevel' = 'value') AS SELECT 1")
        )
        self.assertEqual(expression.expression.expressions[0].type.this, exp.DataType.Type.INT)

    def test_binary_annotation(self):
        expression = annotate_types(parse_one("SELECT 0.0 + (2 + 3)")).expressions[0]

        self.assertEqual(expression.type.this, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.left.type.this, exp.DataType.Type.DOUBLE)
        self.assertEqual(expression.right.type.this, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.type.this, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.left.type.this, exp.DataType.Type.INT)
        self.assertEqual(expression.right.this.right.type.this, exp.DataType.Type.INT)

        for numeric_type in ("BIGINT", "DOUBLE", "INT"):
            query = f"SELECT '1' + CAST(x AS {numeric_type})"
            expression = annotate_types(parse_one(query)).expressions[0]
            self.assertEqual(expression.type, exp.DataType.build(numeric_type))

    def test_typeddiv_annotation(self):
        expressions = annotate_types(
            parse_one("SELECT 2 / 3, 2 / 3.0", dialect="presto")
        ).expressions

        self.assertEqual(expressions[0].type.this, exp.DataType.Type.BIGINT)
        self.assertEqual(expressions[1].type.this, exp.DataType.Type.DOUBLE)

        expressions = annotate_types(
            parse_one("SELECT SUM(2 / 3), CAST(2 AS DECIMAL) / 3", dialect="mysql")
        ).expressions

        self.assertEqual(expressions[0].type.this, exp.DataType.Type.DOUBLE)
        self.assertEqual(expressions[0].this.type.this, exp.DataType.Type.DOUBLE)
        self.assertEqual(expressions[1].type.this, exp.DataType.Type.DECIMAL)

    def test_bracket_annotation(self):
        expression = annotate_types(parse_one("SELECT A[:]")).expressions[0]

        self.assertEqual(expression.type.this, exp.DataType.Type.UNKNOWN)
        self.assertEqual(expression.expressions[0].type.this, exp.DataType.Type.UNKNOWN)

        expression = annotate_types(parse_one("SELECT ARRAY[1, 2, 3][1]")).expressions[0]
        self.assertEqual(expression.this.type.sql(), "ARRAY<INT>")
        self.assertEqual(expression.type.this, exp.DataType.Type.INT)

        expression = annotate_types(parse_one("SELECT ARRAY[1, 2, 3][1 : 2]")).expressions[0]
        self.assertEqual(expression.this.type.sql(), "ARRAY<INT>")
        self.assertEqual(expression.type.sql(), "ARRAY<INT>")

        expression = annotate_types(
            parse_one("SELECT ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]][1][2]")
        ).expressions[0]
        self.assertEqual(expression.this.this.type.sql(), "ARRAY<ARRAY<INT>>")
        self.assertEqual(expression.this.type.sql(), "ARRAY<INT>")
        self.assertEqual(expression.type.this, exp.DataType.Type.INT)

        expression = annotate_types(
            parse_one("SELECT ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]][1:2]")
        ).expressions[0]
        self.assertEqual(expression.type.sql(), "ARRAY<ARRAY<INT>>")

        expression = annotate_types(parse_one("MAP(1.0, 2, '2', 3.0)['2']", read="spark"))
        self.assertEqual(expression.type.this, exp.DataType.Type.DOUBLE)

        expression = annotate_types(parse_one("MAP(1.0, 2, x, 3.0)[2]", read="spark"))
        self.assertEqual(expression.type.this, exp.DataType.Type.UNKNOWN)

        expression = annotate_types(parse_one("MAP(ARRAY(1.0, x), ARRAY(2, 3.0))[x]"))
        self.assertEqual(expression.type.this, exp.DataType.Type.DOUBLE)

        expression = annotate_types(
            parse_one("SELECT MAP(1.0, 2, 2, t.y)[2] FROM t", read="spark"),
            schema={"t": {"y": "int"}},
        ).expressions[0]
        self.assertEqual(expression.type.this, exp.DataType.Type.INT)

    def test_interval_math_annotation(self):
        schema = {
            "x": {
                "a": "DATE",
                "b": "DATETIME",
            }
        }
        for sql, expected_type in [
            (
                "SELECT '2023-01-01' + INTERVAL '1' DAY",
                exp.DataType.Type.DATE,
            ),
            (
                "SELECT '2023-01-01' + INTERVAL '1' HOUR",
                exp.DataType.Type.DATETIME,
            ),
            (
                "SELECT '2023-01-01 00:00:01' + INTERVAL '1' HOUR",
                exp.DataType.Type.DATETIME,
            ),
            ("SELECT 'nonsense' + INTERVAL '1' DAY", exp.DataType.Type.UNKNOWN),
            ("SELECT x.a + INTERVAL '1' DAY FROM x AS x", exp.DataType.Type.DATE),
            (
                "SELECT x.a + INTERVAL '1' HOUR FROM x AS x",
                exp.DataType.Type.DATETIME,
            ),
            ("SELECT x.b + INTERVAL '1' DAY FROM x AS x", exp.DataType.Type.DATETIME),
            ("SELECT x.b + INTERVAL '1' HOUR FROM x AS x", exp.DataType.Type.DATETIME),
            (
                "SELECT DATE_ADD('2023-01-01', 1, 'DAY')",
                exp.DataType.Type.DATE,
            ),
            (
                "SELECT DATE_ADD('2023-01-01 00:00:00', 1, 'DAY')",
                exp.DataType.Type.DATETIME,
            ),
            ("SELECT DATE_ADD(x.a, 1, 'DAY') FROM x AS x", exp.DataType.Type.DATE),
            (
                "SELECT DATE_ADD(x.a, 1, 'HOUR') FROM x AS x",
                exp.DataType.Type.DATETIME,
            ),
            ("SELECT DATE_ADD(x.b, 1, 'DAY') FROM x AS x", exp.DataType.Type.DATETIME),
            ("SELECT DATE_TRUNC('DAY', x.a) FROM x AS x", exp.DataType.Type.DATE),
            ("SELECT DATE_TRUNC('DAY', x.b) FROM x AS x", exp.DataType.Type.DATETIME),
            (
                "SELECT DATE_TRUNC('SECOND', x.a) FROM x AS x",
                exp.DataType.Type.DATETIME,
            ),
            (
                "SELECT DATE_TRUNC('DAY', '2023-01-01') FROM x AS x",
                exp.DataType.Type.DATE,
            ),
            (
                "SELECT DATEDIFF('2023-01-01', '2023-01-02', DAY) FROM x AS x",
                exp.DataType.Type.INT,
            ),
        ]:
            with self.subTest(sql):
                expression = annotate_types(parse_one(sql), schema=schema)
                self.assertEqual(expected_type, expression.expressions[0].type.this)
                self.assertEqual(sql, expression.sql())

    def test_lateral_annotation(self):
        expression = optimizer.optimize(
            parse_one("SELECT c FROM (select 1 a) as x LATERAL VIEW EXPLODE (a) AS c")
        ).expressions[0]
        self.assertEqual(expression.type.this, exp.DataType.Type.INT)

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
        self.assertEqual(
            expression.expressions[0].type.this, exp.DataType.Type.FLOAT
        )  # a.cola AS cola

        addition_alias = expression.args["from"].this.this.expressions[0]
        self.assertEqual(
            addition_alias.type.this, exp.DataType.Type.FLOAT
        )  # x.cola + y.cola AS cola

        addition = addition_alias.this
        self.assertEqual(addition.type.this, exp.DataType.Type.FLOAT)
        self.assertEqual(addition.this.type.this, exp.DataType.Type.INT)
        self.assertEqual(addition.expression.type.this, exp.DataType.Type.FLOAT)

    def test_cte_column_annotation(self):
        schema = {"x": {"cola": "CHAR"}, "y": {"colb": "TEXT", "colc": "BOOLEAN"}}
        sql = """
            WITH tbl AS (
                SELECT x.cola + 'bla' AS cola, y.colb AS colb, y.colc AS colc
                FROM (
                    SELECT x.cola AS cola
                    FROM x AS x
                ) AS x
                JOIN (
                    SELECT y.colb AS colb, y.colc AS colc
                    FROM y AS y
                ) AS y
            )
            SELECT tbl.cola + tbl.colb + 'foo' AS col
            FROM tbl AS tbl
            WHERE tbl.colc = True
        """

        expression = annotate_types(parse_one(sql), schema=schema)
        self.assertEqual(
            expression.expressions[0].type.this, exp.DataType.Type.TEXT
        )  # tbl.cola + tbl.colb + 'foo' AS col

        outer_addition = expression.expressions[0].this  # (tbl.cola + tbl.colb) + 'foo'
        self.assertEqual(outer_addition.type.this, exp.DataType.Type.TEXT)
        self.assertEqual(outer_addition.left.type.this, exp.DataType.Type.TEXT)
        self.assertEqual(outer_addition.right.type.this, exp.DataType.Type.VARCHAR)

        inner_addition = expression.expressions[0].this.left  # tbl.cola + tbl.colb
        self.assertEqual(inner_addition.left.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(inner_addition.right.type.this, exp.DataType.Type.TEXT)

        # WHERE tbl.colc = True
        self.assertEqual(expression.args["where"].this.type.this, exp.DataType.Type.BOOLEAN)

        cte_select = expression.args["with"].expressions[0].this
        self.assertEqual(
            cte_select.expressions[0].type.this, exp.DataType.Type.VARCHAR
        )  # x.cola + 'bla' AS cola
        self.assertEqual(
            cte_select.expressions[1].type.this, exp.DataType.Type.TEXT
        )  # y.colb AS colb
        self.assertEqual(
            cte_select.expressions[2].type.this, exp.DataType.Type.BOOLEAN
        )  # y.colc AS colc

        cte_select_addition = cte_select.expressions[0].this  # x.cola + 'bla'
        self.assertEqual(cte_select_addition.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(cte_select_addition.left.type.this, exp.DataType.Type.CHAR)
        self.assertEqual(cte_select_addition.right.type.this, exp.DataType.Type.VARCHAR)

        # Check that x.cola AS cola and y.colb AS colb have types CHAR and TEXT, respectively
        for d, t in zip(
            cte_select.find_all(exp.Subquery), [exp.DataType.Type.CHAR, exp.DataType.Type.TEXT]
        ):
            self.assertEqual(d.this.expressions[0].this.type.this, t)

    def test_function_annotation(self):
        schema = {"x": {"cola": "VARCHAR", "colb": "CHAR"}}
        sql = (
            "SELECT x.cola || TRIM(x.colb) AS col, DATE(x.colb), DATEFROMPARTS(y, m, d) FROM x AS x"
        )

        expression = annotate_types(parse_one(sql), schema=schema)
        concat_expr_alias = expression.expressions[0]
        self.assertEqual(concat_expr_alias.type.this, exp.DataType.Type.VARCHAR)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(concat_expr.left.type.this, exp.DataType.Type.VARCHAR)  # x.cola
        self.assertEqual(concat_expr.right.type.this, exp.DataType.Type.VARCHAR)  # TRIM(x.colb)
        self.assertEqual(concat_expr.right.this.type.this, exp.DataType.Type.CHAR)  # x.colb

        date_expr = expression.expressions[1]
        self.assertEqual(date_expr.type.this, exp.DataType.Type.DATE)

        date_expr = expression.expressions[2]
        self.assertEqual(date_expr.type.this, exp.DataType.Type.DATE)

        sql = "SELECT CASE WHEN 1=1 THEN x.cola ELSE x.colb END AS col FROM x AS x"

        case_expr_alias = annotate_types(parse_one(sql), schema=schema).expressions[0]
        self.assertEqual(case_expr_alias.type.this, exp.DataType.Type.VARCHAR)

        case_expr = case_expr_alias.this
        self.assertEqual(case_expr.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(case_expr.args["default"].type.this, exp.DataType.Type.CHAR)

        case_ifs_expr = case_expr.args["ifs"][0]
        self.assertEqual(case_ifs_expr.type.this, exp.DataType.Type.VARCHAR)
        self.assertEqual(case_ifs_expr.args["true"].type.this, exp.DataType.Type.VARCHAR)

        timestamp = annotate_types(parse_one("TIMESTAMP(x)"))
        self.assertEqual(timestamp.type.this, exp.DataType.Type.TIMESTAMP)

        timestamptz = annotate_types(parse_one("TIMESTAMP(x)", read="bigquery"))
        self.assertEqual(timestamptz.type.this, exp.DataType.Type.TIMESTAMPTZ)

    def test_unknown_annotation(self):
        schema = {"x": {"cola": "VARCHAR"}}
        sql = "SELECT x.cola + SOME_ANONYMOUS_FUNC(x.cola) AS col FROM x AS x"

        concat_expr_alias = annotate_types(parse_one(sql), schema=schema).expressions[0]
        self.assertEqual(concat_expr_alias.type.this, exp.DataType.Type.UNKNOWN)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type.this, exp.DataType.Type.UNKNOWN)
        self.assertEqual(concat_expr.left.type.this, exp.DataType.Type.VARCHAR)  # x.cola
        self.assertEqual(
            concat_expr.right.type.this, exp.DataType.Type.UNKNOWN
        )  # SOME_ANONYMOUS_FUNC(x.cola)
        self.assertEqual(
            concat_expr.right.expressions[0].type.this, exp.DataType.Type.VARCHAR
        )  # x.cola (arg)

        annotate_types(parse_one("select x from y lateral view explode(y) as x")).expressions[0]

    def test_null_annotation(self):
        expression = annotate_types(parse_one("SELECT NULL + 2 AS col")).expressions[0].this
        self.assertEqual(expression.left.type.this, exp.DataType.Type.NULL)
        self.assertEqual(expression.right.type.this, exp.DataType.Type.INT)

        # NULL <op> UNKNOWN should yield NULL
        sql = "SELECT NULL + SOME_ANONYMOUS_FUNC() AS result"

        concat_expr_alias = annotate_types(parse_one(sql)).expressions[0]
        self.assertEqual(concat_expr_alias.type.this, exp.DataType.Type.NULL)

        concat_expr = concat_expr_alias.this
        self.assertEqual(concat_expr.type.this, exp.DataType.Type.NULL)
        self.assertEqual(concat_expr.left.type.this, exp.DataType.Type.NULL)
        self.assertEqual(concat_expr.right.type.this, exp.DataType.Type.UNKNOWN)

    def test_nullable_annotation(self):
        nullable = exp.DataType.build("NULLABLE", expressions=exp.DataType.build("BOOLEAN"))
        expression = annotate_types(parse_one("NULL AND FALSE"))

        self.assertEqual(expression.type, nullable)
        self.assertEqual(expression.left.type.this, exp.DataType.Type.NULL)
        self.assertEqual(expression.right.type.this, exp.DataType.Type.BOOLEAN)

    def test_predicate_annotation(self):
        expression = annotate_types(parse_one("x BETWEEN a AND b"))
        self.assertEqual(expression.type.this, exp.DataType.Type.BOOLEAN)

        expression = annotate_types(parse_one("x IN (a, b, c, d)"))
        self.assertEqual(expression.type.this, exp.DataType.Type.BOOLEAN)

    def test_aggfunc_annotation(self):
        schema = {"x": {"cola": "SMALLINT", "colb": "FLOAT", "colc": "TEXT", "cold": "DATE"}}

        tests = {
            ("AVG", "cola"): exp.DataType.Type.DOUBLE,
            ("SUM", "cola"): exp.DataType.Type.BIGINT,
            ("SUM", "colb"): exp.DataType.Type.DOUBLE,
            ("MIN", "cola"): exp.DataType.Type.SMALLINT,
            ("MIN", "colb"): exp.DataType.Type.FLOAT,
            ("MAX", "colc"): exp.DataType.Type.TEXT,
            ("MAX", "cold"): exp.DataType.Type.DATE,
            ("COUNT", "colb"): exp.DataType.Type.BIGINT,
            ("STDDEV", "cola"): exp.DataType.Type.DOUBLE,
            ("ABS", "cola"): exp.DataType.Type.SMALLINT,
            ("ABS", "colb"): exp.DataType.Type.FLOAT,
        }

        for (func, col), target_type in tests.items():
            expression = annotate_types(
                parse_one(f"SELECT {func}(x.{col}) AS _col_0 FROM x AS x"), schema=schema
            )
            self.assertEqual(expression.expressions[0].type.this, target_type)

    def test_concat_annotation(self):
        expression = annotate_types(parse_one("CONCAT('A', 'B')"))
        self.assertEqual(expression.type.this, exp.DataType.Type.VARCHAR)

    def test_root_subquery_annotation(self):
        expression = annotate_types(parse_one("(SELECT 1, 2 FROM x) LIMIT 0"))
        self.assertIsInstance(expression, exp.Subquery)
        self.assertEqual(exp.DataType.Type.INT, expression.selects[0].type.this)
        self.assertEqual(exp.DataType.Type.INT, expression.selects[1].type.this)

    def test_nested_type_annotation(self):
        schema = {"order": {"customer_id": "bigint", "item_id": "bigint", "item_price": "numeric"}}
        sql = """
            SELECT ARRAY_AGG(DISTINCT order.item_id) FILTER (WHERE order.item_price > 10) AS items,
            FROM order AS order
            GROUP BY order.customer_id
        """
        expression = annotate_types(parse_one(sql), schema=schema)

        self.assertEqual(exp.DataType.Type.ARRAY, expression.selects[0].type.this)
        self.assertEqual(expression.selects[0].type.sql(), "ARRAY<BIGINT>")

        expression = annotate_types(
            parse_one("SELECT ARRAY_CAT(ARRAY[1,2,3], ARRAY[4,5])", read="postgres")
        )
        self.assertEqual(exp.DataType.Type.ARRAY, expression.selects[0].type.this)
        self.assertEqual(expression.selects[0].type.sql(), "ARRAY<INT>")

        schema = MappingSchema({"t": {"c": "STRUCT<`f` STRING>"}}, dialect="bigquery")
        expression = annotate_types(parse_one("SELECT t.c, [t.c] FROM t"), schema=schema)

        self.assertEqual(expression.selects[0].type.sql(dialect="bigquery"), "STRUCT<`f` STRING>")
        self.assertEqual(
            expression.selects[1].type.sql(dialect="bigquery"), "ARRAY<STRUCT<`f` STRING>>"
        )

        expression = annotate_types(
            parse_one("SELECT unnest(t.x) FROM t AS t", dialect="postgres"),
            schema={"t": {"x": "array<int>"}},
        )
        self.assertTrue(expression.selects[0].is_type("int"))

    def test_type_annotation_cache(self):
        sql = "SELECT 1 + 1"
        expression = annotate_types(parse_one(sql))

        self.assertEqual(exp.DataType.Type.INT, expression.selects[0].type.this)

        expression.selects[0].this.replace(parse_one("1.2"))
        expression = annotate_types(expression)

        self.assertEqual(exp.DataType.Type.DOUBLE, expression.selects[0].type.this)

    def test_user_defined_type_annotation(self):
        schema = MappingSchema({"t": {"x": "int"}}, dialect="postgres")
        expression = annotate_types(parse_one("SELECT CAST(x AS IPADDRESS) FROM t"), schema=schema)

        self.assertEqual(exp.DataType.Type.USERDEFINED, expression.selects[0].type.this)
        self.assertEqual(expression.selects[0].type.sql(dialect="postgres"), "IPADDRESS")

    def test_unnest_annotation(self):
        expression = annotate_types(
            optimizer.qualify.qualify(
                parse_one(
                    """
                SELECT a, a.b, a.b.c FROM x, UNNEST(x.a) AS a
                """,
                    read="bigquery",
                )
            ),
            schema={"x": {"a": "ARRAY<STRUCT<b STRUCT<c int>>>"}},
        )
        self.assertEqual(expression.selects[0].type, exp.DataType.build("STRUCT<b STRUCT<c int>>"))
        self.assertEqual(expression.selects[1].type, exp.DataType.build("STRUCT<c int>"))
        self.assertEqual(expression.selects[2].type, exp.DataType.build("int"))

    def test_recursive_cte(self):
        query = parse_one(
            """
            with recursive t(n) AS
            (
              select 1
              union all
              select n + 1
              FROM t
              where n < 3
            ), y AS (
              select n
              FROM t
              union all
              select n + 1
              FROM y
              where n < 2
            )
            select * from y
            """
        )

        scope_t, scope_y = build_scope(query).cte_scopes
        self.assertEqual(set(scope_t.cte_sources), {"t"})
        self.assertEqual(set(scope_y.cte_sources), {"t", "y"})

    def test_schema_with_spaces(self):
        schema = {
            "a": {
                "b c": "text",
                '"d e"': "text",
            }
        }

        self.assertEqual(
            optimizer.optimize(parse_one("SELECT * FROM a"), schema=schema),
            parse_one('SELECT "a"."b c" AS "b c", "a"."d e" AS "d e" FROM "a" AS "a"'),
        )

    def test_quotes(self):
        schema = {
            "example": {
                '"source"': {
                    "id": "text",
                    '"name"': "text",
                    '"payload"': "text",
                }
            }
        }

        expected = parse_one(
            """
            SELECT
             "source"."ID" AS "ID",
             "source"."name" AS "name",
             "source"."payload" AS "payload"
            FROM "EXAMPLE"."source" AS "source"
            """,
            read="snowflake",
        ).sql(pretty=True, dialect="snowflake")

        for func in (optimizer.qualify.qualify, optimizer.optimize):
            source_query = parse_one('SELECT * FROM example."source" AS "source"', read="snowflake")
            transformed = func(source_query, dialect="snowflake", schema=schema)
            self.assertEqual(transformed.sql(pretty=True, dialect="snowflake"), expected)

    def test_no_pseudocolumn_expansion(self):
        schema = {
            "a": {
                "a": "text",
                "b": "text",
                "_PARTITIONDATE": "date",
                "_PARTITIONTIME": "timestamp",
            }
        }

        self.assertEqual(
            optimizer.optimize(
                parse_one("SELECT * FROM a"), schema=MappingSchema(schema, dialect="bigquery")
            ),
            parse_one('SELECT "a"."a" AS "a", "a"."b" AS "b" FROM "a" AS "a"'),
        )

    def test_semistructured(self):
        query = parse_one("select a.b:c from d", read="snowflake")
        qualified = optimizer.qualify.qualify(query)
        self.assertEqual(qualified.expressions[0].alias, "c")
