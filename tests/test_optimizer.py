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
from sqlglot.optimizer.normalize import normalization_distance
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


def qualify_columns(expression, validate_qualify_columns=True, **kwargs):
    expression = optimizer.qualify.qualify(
        expression,
        infer_schema=True,
        validate_qualify_columns=validate_qualify_columns,
        identify=False,
        **kwargs,
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


def annotate_functions(expression, **kwargs):
    from sqlglot.dialects import Dialect

    dialect = kwargs.get("dialect")
    schema = kwargs.get("schema")

    annotators = Dialect.get_or_raise(dialect).ANNOTATORS
    annotated = annotate_types(expression, annotators=annotators, schema=schema)

    return annotated.expressions[0]


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
        self,
        file,
        func,
        pretty=False,
        execute=False,
        only=None,
        **kwargs,
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
                validate_qualify_columns = meta.get("validate_qualify_columns")

                func_kwargs = {**kwargs}
                if leave_tables_isolated is not None:
                    func_kwargs["leave_tables_isolated"] = string_to_bool(leave_tables_isolated)

                if validate_qualify_columns is not None:
                    func_kwargs["validate_qualify_columns"] = string_to_bool(
                        validate_qualify_columns
                    )

                if dialect:
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
                parse_one(
                    "WITH cte AS (SELECT * FROM t) SELECT * FROM cte PIVOT(SUM(c) FOR v IN ('x', 'y'))"
                ),
                db="db",
                catalog="catalog",
            ).sql(),
            "WITH cte AS (SELECT * FROM catalog.db.t AS t) SELECT * FROM cte AS cte PIVOT(SUM(c) FOR v IN ('x', 'y')) AS _q_0",
        )

        self.assertEqual(
            optimizer.qualify_tables.qualify_tables(
                parse_one(
                    "WITH cte AS (SELECT * FROM t) SELECT * FROM cte PIVOT(SUM(c) FOR v IN ('x', 'y')) AS pivot_alias"
                ),
                db="db",
                catalog="catalog",
            ).sql(),
            "WITH cte AS (SELECT * FROM catalog.db.t AS t) SELECT * FROM cte AS cte PIVOT(SUM(c) FOR v IN ('x', 'y')) AS pivot_alias",
        )

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
            optimizer.qualify.qualify(
                parse_one(
                    """
                    SELECT Teams.Name, count(*)
                    FROM raw.TeamMemberships as TeamMemberships
                    join raw.Teams
                        on Teams.Id = TeamMemberships.TeamId
                    GROUP BY 1
                    """,
                    read="bigquery",
                ),
                schema={
                    "raw": {
                        "TeamMemberships": {
                            "Id": "INTEGER",
                            "UserId": "INTEGER",
                            "TeamId": "INTEGER",
                        },
                        "Teams": {
                            "Id": "INTEGER",
                            "Name": "STRING",
                        },
                    }
                },
                dialect="bigquery",
            ).sql(dialect="bigquery"),
            "SELECT `teams`.`name` AS `name`, count(*) AS `_col_1` FROM `raw`.`TeamMemberships` AS `teammemberships` JOIN `raw`.`Teams` AS `teams` ON `teams`.`id` = `teammemberships`.`teamid` GROUP BY `teams`.`name`",
        )
        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "SELECT `my_db.my_table`.`my_column` FROM `my_db.my_table`",
                    read="bigquery",
                ),
                dialect="bigquery",
            ).sql(dialect="bigquery"),
            "SELECT `my_table`.`my_column` AS `my_column` FROM `my_db.my_table` AS `my_table`",
        )

        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(
                    "WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t AS child WHERE x < 10) SELECT * FROM t"
                ),
                schema={},
                infer_schema=False,
            ).sql(),
            "WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT child.x + 1 AS _col_0 FROM t AS child WHERE child.x < 10) SELECT t.x AS x FROM t",
        )

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
            'WITH "x" AS (SELECT "y"."a" AS "a" FROM "DB"."y" AS "y" CROSS JOIN "a"."b"."INFORMATION_SCHEMA.COLUMNS" AS "columns") SELECT "x"."a" AS "a" FROM "x" AS "x"',
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

        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(
                    "WITH tbl1 AS (SELECT STRUCT(1 AS `f0`, 2 as f1) AS col) SELECT tbl1.col.* from tbl1",
                    dialect="bigquery",
                ),
                schema=MappingSchema(schema=None, dialect="bigquery"),
                infer_schema=False,
            ).sql(dialect="bigquery"),
            "WITH tbl1 AS (SELECT STRUCT(1 AS `f0`, 2 AS f1) AS col) SELECT tbl1.col.`f0` AS `f0`, tbl1.col.f1 AS f1 FROM tbl1",
        )

        # can't coalesce USING columns because they don't exist in every already-joined table
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(
                    "SELECT id, dt, v FROM (SELECT t1.id, t1.dt, sum(coalesce(t2.v, 0)) AS v FROM t1 AS t1 LEFT JOIN lkp AS lkp USING (id) LEFT JOIN t2 AS t2 USING (other_id, dt, common) WHERE t1.id > 10 GROUP BY 1, 2) AS _q_0",
                    dialect="bigquery",
                ),
                schema=MappingSchema(
                    schema={
                        "t1": {"id": "int64", "dt": "date", "common": "int64"},
                        "lkp": {"id": "int64", "other_id": "int64", "common": "int64"},
                        "t2": {"other_id": "int64", "dt": "date", "v": "int64", "common": "int64"},
                    },
                    dialect="bigquery",
                ),
            ).sql(dialect="bigquery"),
            "SELECT _q_0.id AS id, _q_0.dt AS dt, _q_0.v AS v FROM (SELECT t1.id AS id, t1.dt AS dt, sum(coalesce(t2.v, 0)) AS v FROM t1 AS t1 LEFT JOIN lkp AS lkp ON t1.id = lkp.id LEFT JOIN t2 AS t2 ON lkp.other_id = t2.other_id AND t1.dt = t2.dt AND COALESCE(t1.common, lkp.common) = t2.common WHERE t1.id > 10 GROUP BY t1.id, t1.dt) AS _q_0",
        )

        # Detection of correlation where columns are referenced in derived tables nested within subqueries
        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "SELECT a.g FROM a WHERE a.e < (SELECT MAX(u) FROM (SELECT SUM(c.b) AS u FROM c WHERE  c.d = f GROUP BY c.e) w)"
                ),
                schema={
                    "a": {"g": "INT", "e": "INT", "f": "INT"},
                    "c": {"d": "INT", "e": "INT", "b": "INT"},
                },
                quote_identifiers=False,
            ).sql(),
            "SELECT a.g AS g FROM a AS a WHERE a.e < (SELECT MAX(w.u) AS _col_0 FROM (SELECT SUM(c.b) AS u FROM c AS c WHERE c.d = a.f GROUP BY c.e) AS w)",
        )

        # Detection of correlation where columns are referenced in derived tables nested within lateral joins
        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "SELECT u.user_id, l.log_date FROM users AS u CROSS JOIN LATERAL (SELECT l1.log_date FROM (SELECT l.log_date FROM logs AS l WHERE l.user_id = u.user_id AND l.log_date <= 100 ORDER BY l.log_date LIMIT 1) AS l1) AS l",
                    dialect="postgres",
                ),
                schema={
                    "users": {"user_id": "text", "log_date": "date"},
                    "logs": {"user_id": "text", "log_date": "date"},
                },
                quote_identifiers=False,
            ).sql("postgres"),
            "SELECT u.user_id AS user_id, l.log_date AS log_date FROM users AS u CROSS JOIN LATERAL (SELECT l1.log_date AS log_date FROM (SELECT l.log_date AS log_date FROM logs AS l WHERE l.user_id = u.user_id AND l.log_date <= 100 ORDER BY l.log_date LIMIT 1) AS l1) AS l",
        )

        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "SELECT A.b_id FROM A JOIN B ON A.b_id=B.b_id JOIN C USING(c_id)",
                    dialect="postgres",
                ),
                schema={
                    "A": {"b_id": "int"},
                    "B": {"b_id": "int", "c_id": "int"},
                    "C": {"c_id": "int"},
                },
                quote_identifiers=False,
            ).sql("postgres"),
            "SELECT a.b_id AS b_id FROM a AS a JOIN b AS b ON a.b_id = b.b_id JOIN c AS c ON b.c_id = c.c_id",
        )
        self.assertEqual(
            optimizer.qualify.qualify(
                parse_one(
                    "SELECT A.b_id FROM A JOIN B ON A.b_id=B.b_id JOIN C ON B.b_id = C.b_id JOIN D USING(d_id)",
                    dialect="postgres",
                ),
                schema={
                    "A": {"b_id": "int"},
                    "B": {"b_id": "int", "d_id": "int"},
                    "C": {"b_id": "int"},
                    "D": {"d_id": "int"},
                },
                quote_identifiers=False,
            ).sql("postgres"),
            "SELECT a.b_id AS b_id FROM a AS a JOIN b AS b ON a.b_id = b.b_id JOIN c AS c ON b.b_id = c.b_id JOIN d AS d ON b.d_id = d.d_id",
        )

        self.check_file(
            "qualify_columns",
            qualify_columns,
            execute=True,
            schema=self.schema,
        )
        self.check_file("qualify_columns_ddl", qualify_columns, schema=self.schema)

    def test_qualify_columns__with_invisible(self):
        schema = MappingSchema(self.schema, {"x": {"a"}, "y": {"b"}, "z": {"b"}})
        self.check_file("qualify_columns__with_invisible", qualify_columns, schema=schema)

    def test_pushdown_cte_alias_columns(self):
        self.check_file(
            "pushdown_cte_alias_columns",
            optimizer.qualify_columns.pushdown_cte_alias_columns,
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
        )

        self.assertEqual(optimizer.normalize_identifiers.normalize_identifiers("a%").sql(), '"a%"')

    def test_quote_identifiers(self):
        self.check_file(
            "quote_identifiers",
            optimizer.qualify_columns.quote_identifiers,
        )

    def test_pushdown_projection(self):
        self.check_file("pushdown_projections", pushdown_projections, schema=self.schema)

    def test_simplify(self):
        self.check_file("simplify", simplify)

        expression = parse_one("SELECT a, c, b FROM table1 WHERE 1 = 1")
        self.assertEqual(simplify(simplify(expression.find(exp.Where))).sql(), "WHERE TRUE")

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
        self.assertEqual(optimizer.simplify.gen(anon_unquoted_str), "ANONYMOUS(x,y)")

        query = parse_one("SELECT x FROM t")
        self.assertEqual(optimizer.simplify.gen(query), optimizer.simplify.gen(query.copy()))

        anon_unquoted_identifier = exp.Anonymous(
            this=exp.to_identifier("anonymous"),
            expressions=[exp.column("x"), exp.column("y")],
        )
        self.assertEqual(optimizer.simplify.gen(anon_unquoted_identifier), "ANONYMOUS(x,y)")

        anon_quoted = parse_one('"anonymous"(x, y)')
        self.assertEqual(optimizer.simplify.gen(anon_quoted), '"anonymous"(x,y)')

        with self.assertRaises(ValueError) as e:
            anon_invalid = exp.Anonymous(this=5)
            optimizer.simplify.gen(anon_invalid)

        self.assertIn(
            "Anonymous.this expects a str or an Identifier, got 'int'.",
            str(e.exception),
        )

        sql = parse_one(
            """
        WITH cte AS (select 1 union select 2), cte2 AS (
            SELECT ROW() OVER (PARTITION BY y) FROM (
                (select 1) limit 10
            )
        )
        SELECT
          *,
          a + 1,
          a div 1,
          filter("B", (x, y) -> x + y)
          FROM (z AS z CROSS JOIN z) AS f(a) LEFT JOIN a.b.c.d.e.f.g USING(n) ORDER BY 1
        """
        )
        self.assertEqual(
            optimizer.simplify.gen(sql),
            """
SELECT :with,WITH :expressions,CTE :this,UNION :this,SELECT :expressions,1,:expression,SELECT :expressions,2,:distinct,True,:alias, AS cte,CTE :this,SELECT :expressions,WINDOW :this,ROW(),:partition_by,y,:over,OVER,:from,FROM ((SELECT :expressions,1):limit,LIMIT :expression,10),:alias, AS cte2,:expressions,STAR,a + 1,a DIV 1,FILTER("B",LAMBDA :this,x + y,:expressions,x,y),:from,FROM (z AS z:joins,JOIN :this,z,:kind,CROSS) AS f(a),:joins,JOIN :this,a.b.c.d.e.f.g,:side,LEFT,:using,n,:order,ORDER :expressions,ORDERED :this,1,:nulls_first,True
""".strip(),
        )
        self.assertEqual(
            optimizer.simplify.gen(parse_one("select item_id /* description */"), comments=True),
            "SELECT :expressions,item_id /* description */",
        )

    def test_unnest_subqueries(self):
        self.check_file("unnest_subqueries", optimizer.unnest_subqueries.unnest_subqueries)

    def test_pushdown_predicates(self):
        self.check_file("pushdown_predicates", optimizer.pushdown_predicates.pushdown_predicates)

    def test_expand_alias_refs(self):
        # check order of lateral expansion with no schema
        self.assertEqual(
            optimizer.optimize("SELECT a + 1 AS d, d + 1 AS e FROM x WHERE e > 1 GROUP BY e").sql(),
            'SELECT "x"."a" + 1 AS "d", "x"."a" + 1 + 1 AS "e" FROM "x" AS "x" WHERE ("x"."a" + 2) > 1 GROUP BY "x"."a" + 1 + 1',
        )

        unused_schema = {"l": {"c": "int"}}
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one("SELECT CAST(x AS INT) AS y FROM z AS z"),
                schema=unused_schema,
                infer_schema=False,
            ).sql(),
            "SELECT CAST(x AS INT) AS y FROM z AS z",
        )

        # BigQuery expands overlapping alias only for GROUP BY + HAVING
        sql = "WITH data AS (SELECT 1 AS id, 2 AS my_id, 'a' AS name, 'b' AS full_name) SELECT id AS my_id, CONCAT(id, name) AS full_name FROM data WHERE my_id = 1 GROUP BY my_id, full_name HAVING my_id = 1"
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(sql, dialect="bigquery"),
                schema=MappingSchema(schema=unused_schema, dialect="bigquery"),
            ).sql(),
            "WITH data AS (SELECT 1 AS id, 2 AS my_id, 'a' AS name, 'b' AS full_name) SELECT data.id AS my_id, CONCAT(data.id, data.name) AS full_name FROM data WHERE data.my_id = 1 GROUP BY data.id, CONCAT(data.id, data.name) HAVING data.id = 1",
        )

        # Clickhouse expands overlapping alias across the entire query
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(sql, dialect="clickhouse"),
                schema=MappingSchema(schema=unused_schema, dialect="clickhouse"),
            ).sql(),
            "WITH data AS (SELECT 1 AS id, 2 AS my_id, 'a' AS name, 'b' AS full_name) SELECT data.id AS my_id, CONCAT(data.id, data.name) AS full_name FROM data WHERE data.id = 1 GROUP BY data.id, CONCAT(data.id, data.name) HAVING data.id = 1",
        )

        # Edge case: BigQuery shouldn't expand aliases in complex expressions
        sql = "WITH data AS (SELECT 1 AS id) SELECT FUNC(id) AS id FROM data GROUP BY FUNC(id)"
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(sql, dialect="bigquery"),
                schema=MappingSchema(schema=unused_schema, dialect="bigquery"),
            ).sql(),
            "WITH data AS (SELECT 1 AS id) SELECT FUNC(data.id) AS id FROM data GROUP BY FUNC(data.id)",
        )

        sql = "SELECT x.a, max(x.b) as x FROM x AS x GROUP BY 1 HAVING x > 1"
        self.assertEqual(
            optimizer.qualify_columns.qualify_columns(
                parse_one(sql, dialect="bigquery"),
                schema=MappingSchema(schema=unused_schema, dialect="bigquery"),
            ).sql(),
            "SELECT x.a AS a, MAX(x.b) AS x FROM x AS x GROUP BY 1 HAVING x > 1",
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
            optimizer.optimize(expression, infer_csv_schemas=True).sql(pretty=True),
        )

    def test_scope(self):
        ast = parse_one("SELECT IF(a IN UNNEST(b), 1, 0) AS c FROM t", dialect="bigquery")
        self.assertEqual(build_scope(ast).columns, [exp.column("a"), exp.column("b")])

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
                for node in walk_in_scope(expression.find(exp.Where))
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

        # Check DML statement scopes
        sql = (
            "UPDATE customers SET total_spent = (SELECT 1 FROM t1) WHERE EXISTS (SELECT 1 FROM t2)"
        )
        self.assertEqual(len(traverse_scope(parse_one(sql))), 3)

        sql = "UPDATE tbl1 SET col = 1 WHERE EXISTS (SELECT 1 FROM tbl2 WHERE tbl1.id = tbl2.id)"
        self.assertEqual(len(traverse_scope(parse_one(sql))), 1)

        sql = "UPDATE tbl1 SET col = 0"
        self.assertEqual(len(traverse_scope(parse_one(sql))), 0)

    @patch("sqlglot.optimizer.scope.logger")
    def test_scope_warning(self, logger):
        self.assertEqual(len(traverse_scope(parse_one("WITH q AS (@y) SELECT * FROM q"))), 1)
        assert_logger_contains(
            "Cannot traverse scope %s with type '%s'",
            logger,
            level="warning",
        )

    def test_annotate_types(self):
        for i, (meta, sql, expected) in enumerate(
            load_sql_fixture_pairs("optimizer/annotate_types.sql"), start=1
        ):
            title = meta.get("title") or f"{i}, {sql}"
            dialect = meta.get("dialect")
            result = parse_and_optimize(annotate_types, sql, dialect)

            with self.subTest(title):
                self.assertEqual(result.type.sql(), exp.DataType.build(expected).sql())

    def test_annotate_funcs(self):
        test_schema = {
            "tbl": {"bin_col": "BINARY", "str_col": "STRING", "bignum_col": "BIGNUMERIC"}
        }

        for i, (meta, sql, expected) in enumerate(
            load_sql_fixture_pairs("optimizer/annotate_functions.sql"), start=1
        ):
            title = meta.get("title") or f"{i}, {sql}"
            dialect = meta.get("dialect") or ""
            sql = f"SELECT {sql} FROM tbl"

            for dialect in dialect.split(", "):
                result = parse_and_optimize(
                    annotate_functions, sql, dialect, schema=test_schema, dialect=dialect
                )

                with self.subTest(title):
                    self.assertEqual(
                        result.type.sql(dialect), exp.DataType.build(expected).sql(dialect)
                    )

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
            cte_select.find_all(exp.Subquery),
            [exp.DataType.Type.CHAR, exp.DataType.Type.TEXT],
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

        # Ensures we don't raise if there are unqualified columns
        annotate_types(parse_one("select x from y lateral view explode(y) as x")).expressions[0]

        # NULL <op> UNKNOWN should yield UNKNOWN
        self.assertEqual(
            annotate_types(parse_one("SELECT NULL + ANONYMOUS_FUNC()")).expressions[0].type.this,
            exp.DataType.Type.UNKNOWN,
        )

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
                parse_one(f"SELECT {func}(x.{col}) AS _col_0 FROM x AS x"),
                schema=schema,
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
        schema = {
            "order": {
                "customer_id": "bigint",
                "item_id": "bigint",
                "item_price": "numeric",
            }
        }
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
            expression.selects[1].type.sql(dialect="bigquery"),
            "ARRAY<STRUCT<`f` STRING>>",
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

        self.assertEqual(
            annotate_types(
                optimizer.qualify.qualify(
                    parse_one(
                        "SELECT x FROM UNNEST(GENERATE_DATE_ARRAY('2021-01-01', current_date(), interval 1 day)) AS x"
                    )
                )
            )
            .selects[0]
            .type,
            exp.DataType.build("date"),
        )

        self.assertEqual(
            annotate_types(
                optimizer.qualify.qualify(
                    parse_one(
                        "SELECT x FROM UNNEST(GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-06 02:00:00', interval 1 day)) AS x"
                    )
                )
            )
            .selects[0]
            .type,
            exp.DataType.build("timestamp"),
        )

    def test_map_annotation(self):
        # ToMap annotation
        expression = annotate_types(parse_one("SELECT MAP {'x': 1}", read="duckdb"))
        self.assertEqual(expression.selects[0].type, exp.DataType.build("MAP(VARCHAR, INT)"))

        # Map annotation
        expression = annotate_types(
            parse_one("SELECT MAP(['key1', 'key2', 'key3'], [10, 20, 30])", read="duckdb")
        )
        self.assertEqual(expression.selects[0].type, exp.DataType.build("MAP(VARCHAR, INT)"))

        # VarMap annotation
        expression = annotate_types(parse_one("SELECT MAP('a', 'b')", read="spark"))
        self.assertEqual(expression.selects[0].type, exp.DataType.build("MAP(VARCHAR, VARCHAR)"))

    def test_union_annotation(self):
        for left, right, expected_type in (
            ("SELECT 1::INT AS c", "SELECT 2::BIGINT AS c", "BIGINT"),
            ("SELECT 1 AS c", "SELECT NULL AS c", "INT"),
            ("SELECT FOO() AS c", "SELECT 1 AS c", "UNKNOWN"),
            ("SELECT FOO() AS c", "SELECT BAR() AS c", "UNKNOWN"),
        ):
            with self.subTest(f"left: {left}, right: {right}, expected: {expected_type}"):
                lr = annotate_types(parse_one(f"SELECT t.c FROM ({left} UNION ALL {right}) t(c)"))
                rl = annotate_types(parse_one(f"SELECT t.c FROM ({right} UNION ALL {left}) t(c)"))
                assert lr.selects[0].type == rl.selects[0].type == exp.DataType.build(expected_type)

        union_by_name = annotate_types(
            parse_one(
                "SELECT t.a, t.d FROM (SELECT 1 a, 3 d, UNION ALL BY NAME SELECT 7.0 d, 8::BIGINT a) AS t(a, d)"
            )
        )
        self.assertEqual(union_by_name.selects[0].type.this, exp.DataType.Type.BIGINT)
        self.assertEqual(union_by_name.selects[1].type.this, exp.DataType.Type.DOUBLE)

        # Test chained UNIONs
        sql = """
            WITH t AS
            (
                SELECT NULL AS col
                UNION
                SELECT NULL AS col
                UNION
                SELECT 'a' AS col
                UNION
                SELECT NULL AS col
                UNION
                SELECT NULL AS col
            )
            SELECT col FROM t;
        """
        self.assertEqual(optimizer.optimize(sql).selects[0].type.this, exp.DataType.Type.VARCHAR)

        # Test UNIONs with nested subqueries
        sql = """
            WITH t AS
            (
                SELECT NULL AS col
                UNION
                (SELECT NULL AS col UNION ALL SELECT 'a' AS col)
            )
            SELECT col FROM t;
        """
        self.assertEqual(optimizer.optimize(sql).selects[0].type.this, exp.DataType.Type.VARCHAR)

        sql = """
            WITH t AS
            (
                (SELECT NULL AS col UNION ALL SELECT 'a' AS col)
                UNION
                SELECT NULL AS col
            )
            SELECT col FROM t;
        """
        self.assertEqual(optimizer.optimize(sql).selects[0].type.this, exp.DataType.Type.VARCHAR)

    def test_udtf_annotation(self):
        table_udtf = parse_one(
            "SELECT * FROM TABLE(GENERATOR(ROWCOUNT => 100000))",
            read="snowflake",
        )
        self.assertEqual(
            annotate_types(table_udtf, dialect="snowflake").sql("snowflake"),
            "SELECT * FROM TABLE(GENERATOR(ROWCOUNT => 100000))",
        )

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
                parse_one("SELECT * FROM a"),
                schema=MappingSchema(schema, dialect="bigquery"),
            ),
            parse_one('SELECT "a"."a" AS "a", "a"."b" AS "b" FROM "a" AS "a"'),
        )

    def test_semistructured(self):
        query = parse_one("select a.b:c from d", read="snowflake")
        qualified = optimizer.qualify.qualify(query)
        self.assertEqual(qualified.expressions[0].alias, "c")

    def test_normalization_distance(self):
        def gen_expr(depth: int) -> exp.Expression:
            return parse_one(" OR ".join("a AND b" for _ in range(depth)))

        self.assertEqual(4, normalization_distance(gen_expr(2), max_=100))
        self.assertEqual(18, normalization_distance(gen_expr(3), max_=100))
        self.assertEqual(110, normalization_distance(gen_expr(10), max_=100))
