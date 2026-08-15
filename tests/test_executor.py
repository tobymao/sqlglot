import ast
import csv
import datetime
import unittest
from collections import Counter
from datetime import date, time
from concurrent.futures import ProcessPoolExecutor

import duckdb
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from sqlglot import exp, find_tables, parse_one, transpile
from sqlglot.errors import ExecuteError
from sqlglot.executor import execute
from sqlglot.executor.python import Python, PythonExecutor
from sqlglot.executor.table import Table, ensure_tables
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
from tests.helpers import (
    FIXTURES_DIR,
    SKIP_INTEGRATION,
    TPCH_SCHEMA,
    TPCDS_SCHEMA,
    load_sql_fixture_pairs,
)

DIR_TPCH = FIXTURES_DIR + "/optimizer/tpc-h/"
DIR_TPCDS = FIXTURES_DIR + "/optimizer/tpc-ds/"


def open_file(file_name):
    """Open a file that may be compressed as gzip and return it in universal newline mode."""
    with open(file_name, "rb") as f:
        gzipped = f.read(2) == b"\x1f\x8b"

    if gzipped:
        import gzip

        return gzip.open(file_name, "rt", newline="")

    return open(file_name, encoding="utf-8", newline="")


_schema = None
_tables = None


def initializer(schema, tables):
    global _schema, _tables
    _schema = schema
    _tables = tables


def mp_execute(expression, meta):
    if not meta.get("execute"):
        return None

    tables = {}

    for t in find_tables(expression):
        name = t.name
        tables[name] = _tables[name]

    return execute(expression, schema=_schema, tables=tables)


@unittest.skipIf(SKIP_INTEGRATION, "Skipping Integration Tests since `SKIP_INTEGRATION` is set")
class TestExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tpch_conn = duckdb.connect()
        cls.tpcds_conn = duckdb.connect()
        cls.tpch_tables = {}
        cls.tpcds_tables = {}

        def setup(conn, directory, table, columns, tables):
            file_name = f"{directory}{table}.csv.gz"

            conn.execute(
                f"""
                CREATE VIEW {table} AS
                SELECT *
                FROM READ_CSV('{file_name}', delim='|', header=True, columns={columns})
                """
            )

            reader = csv.reader(open_file(file_name), delimiter="|")
            rows = []
            ctypes = []
            tables[table] = rows

            next(reader)

            for row in reader:
                if not ctypes:
                    for i, v in enumerate(row):
                        try:
                            ctypes.append(type(ast.literal_eval(v)))
                        except (ValueError, SyntaxError):
                            ctypes.append(str)

                rows.append(
                    tuple(None if (t is not str and v == "") else t(v) for t, v in zip(ctypes, row))
                )

            tables[table] = Table(columns=columns, rows=rows)

        for table, columns in TPCH_SCHEMA.items():
            setup(cls.tpch_conn, DIR_TPCH, table, columns, cls.tpch_tables)

        for table, columns in TPCDS_SCHEMA.items():
            setup(cls.tpcds_conn, DIR_TPCDS, table, columns, cls.tpcds_tables)

        cls.cache = {}
        cls.tpch_sqls = list(load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql"))
        cls.tpcds_sqls = list(load_sql_fixture_pairs("optimizer/tpc-ds/tpc-ds.sql"))

    @classmethod
    def tearDownClass(cls):
        cls.tpch_conn.close()
        cls.tpcds_conn.close()

    def cached_execute(self, sql, tpch=True):
        conn = self.tpch_conn if tpch else self.tpcds_conn
        if sql not in self.cache:
            self.cache[sql] = conn.execute(transpile(sql, write="duckdb")[0]).fetchdf()
        return self.cache[sql]

    def rename_anonymous(self, source, target):
        for i, column in enumerate(source.columns):
            if "_col_" in column:
                source.rename(columns={column: target.columns[i]}, inplace=True)

    def test_py_dialect(self):
        generate = Python().generate
        self.assertEqual(generate(parse_one("'x '''")), r"'x \''")
        self.assertEqual(generate(parse_one("MAP([1], [2])")), "MAP([1], [2])")
        self.assertEqual(generate(parse_one("1 is null")), "1 == None")
        self.assertEqual(generate(parse_one("x is null")), "scope[None][x] is None")
        self.assertEqual(generate(parse_one("x like 'y'")), "LIKE(scope[None][x], 'y')")
        self.assertEqual(generate(parse_one("x not like 'y'")), "NOT(LIKE(scope[None][x], 'y'))")

    def test_optimized_tpch(self):
        for i, (_, sql, optimized) in enumerate(self.tpch_sqls, start=1):
            with self.subTest(f"{i}, {sql}"):
                a = self.cached_execute(sql, tpch=True)
                b = self.tpch_conn.execute(transpile(optimized, write="duckdb")[0]).fetchdf()
                self.rename_anonymous(b, a)
                assert_frame_equal(a, b)

    def subtestHelper(self, i, table, tpch=True):
        with self.subTest(f"{'tpc-h' if tpch else 'tpc-ds'} {i + 1}"):
            _, sql, _ = self.tpch_sqls[i] if tpch else self.tpcds_sqls[i]
            a = self.cached_execute(sql, tpch=tpch)
            b = pd.DataFrame(
                ((np.nan if c is None else c for c in r) for r in table.rows),
                columns=table.columns,
            )
            assert_frame_equal(a, b, check_dtype=False, check_index_type=False)

    def _mp_execute(self, schema, tables, sqls, tpch):
        with ProcessPoolExecutor(
            initializer=initializer,
            initargs=(schema, tables),
        ) as pool:
            futures = [pool.submit(mp_execute, parse_one(sql), args) for args, sql, _ in sqls]
            for i, future in enumerate(futures):
                table = future.result()
                if table is not None:
                    self.subtestHelper(i, table, tpch=tpch)

    def test_execute_tpch(self):
        self._mp_execute(TPCH_SCHEMA, self.tpch_tables, self.tpch_sqls, True)

    def test_execute_tpcds(self):
        self._mp_execute(TPCDS_SCHEMA, self.tpcds_tables, self.tpcds_sqls, False)

    def test_execute_callable(self):
        tables = {
            "x": [
                {"a": "a", "b": "d"},
                {"a": "b", "b": "e"},
                {"a": "c", "b": "f"},
            ],
            "y": [
                {"b": "d", "c": "g"},
                {"b": "e", "c": "h"},
                {"b": "f", "c": "i"},
            ],
            "z": [],
        }
        schema = {
            "x": {
                "a": "VARCHAR",
                "b": "VARCHAR",
            },
            "y": {
                "b": "VARCHAR",
                "c": "VARCHAR",
            },
            "z": {"d": "VARCHAR"},
        }

        for sql, cols, rows in [
            ("SELECT * FROM x", ["a", "b"], [("a", "d"), ("b", "e"), ("c", "f")]),
            (
                "SELECT * FROM x JOIN y ON x.b = y.b",
                ["a", "b", "b", "c"],
                [("a", "d", "d", "g"), ("b", "e", "e", "h"), ("c", "f", "f", "i")],
            ),
            (
                "SELECT j.c AS d FROM x AS i JOIN y AS j ON i.b = j.b",
                ["d"],
                [("g",), ("h",), ("i",)],
            ),
            (
                "SELECT CONCAT(x.a, y.c) FROM x JOIN y ON x.b = y.b WHERE y.b = 'e'",
                ["_col_0"],
                [("bh",)],
            ),
            (
                "SELECT * FROM x JOIN y ON x.b = y.b WHERE y.b = 'e'",
                ["a", "b", "b", "c"],
                [("b", "e", "e", "h")],
            ),
            (
                "SELECT * FROM z",
                ["d"],
                [],
            ),
            (
                "SELECT d FROM z ORDER BY d",
                ["d"],
                [],
            ),
            (
                "SELECT a FROM x WHERE x.a <> 'b'",
                ["a"],
                [("a",), ("c",)],
            ),
            (
                "SELECT a AS i FROM x ORDER BY a",
                ["i"],
                [("a",), ("b",), ("c",)],
            ),
            (
                "SELECT a AS i FROM x ORDER BY i",
                ["i"],
                [("a",), ("b",), ("c",)],
            ),
            (
                "SELECT 100 - ORD(a) AS a, a AS i FROM x ORDER BY a",
                ["a", "i"],
                [(1, "c"), (2, "b"), (3, "a")],
            ),
            (
                "SELECT a /* test */ FROM x LIMIT 1",
                ["a"],
                [("a",)],
            ),
            (
                "SELECT DISTINCT a FROM (SELECT 1 AS a UNION ALL SELECT 1 AS a)",
                ["a"],
                [(1,)],
            ),
            (
                "SELECT DISTINCT a, SUM(b) AS b "
                "FROM (SELECT 'a' AS a, 1 AS b UNION ALL SELECT 'a' AS a, 2 AS b UNION ALL SELECT 'b' AS a, 1 AS b) "
                "GROUP BY a "
                "LIMIT 1",
                ["a", "b"],
                [("a", 3)],
            ),
            (
                "SELECT COUNT(1) AS a FROM (SELECT 1)",
                ["a"],
                [(1,)],
            ),
            (
                "SELECT COUNT(1) AS a FROM (SELECT 1) LIMIT 0",
                ["a"],
                [],
            ),
            (
                "SELECT a FROM x GROUP BY a LIMIT 0",
                ["a"],
                [],
            ),
            (
                "SELECT a FROM x LIMIT 0",
                ["a"],
                [],
            ),
        ]:
            with self.subTest(sql):
                result = execute(sql, schema=schema, tables=tables)
                self.assertEqual(result.columns, tuple(cols))
                self.assertEqual(result.rows, rows)

    def test_set_operations(self):
        tables = {
            "x": [
                {"a": "a"},
                {"a": "b"},
                {"a": "c"},
            ],
            "y": [
                {"a": "b"},
                {"a": "c"},
                {"a": "d"},
            ],
        }
        schema = {
            "x": {
                "a": "VARCHAR",
            },
            "y": {
                "a": "VARCHAR",
            },
        }

        for sql, cols, rows in [
            (
                "SELECT a FROM x UNION ALL SELECT a FROM y",
                ["a"],
                [("a",), ("b",), ("c",), ("b",), ("c",), ("d",)],
            ),
            (
                "SELECT a FROM x UNION SELECT a FROM y",
                ["a"],
                [("a",), ("b",), ("c",), ("d",)],
            ),
            (
                "SELECT a FROM x EXCEPT SELECT a FROM y",
                ["a"],
                [("a",)],
            ),
            (
                "(SELECT a FROM x) EXCEPT (SELECT a FROM y)",
                ["a"],
                [("a",)],
            ),
            (
                "SELECT a FROM x INTERSECT SELECT a FROM y",
                ["a"],
                [("b",), ("c",)],
            ),
            (
                """SELECT i.a
                FROM (
                  SELECT a FROM x UNION SELECT a FROM y
                ) AS i
                JOIN (
                  SELECT a FROM x UNION SELECT a FROM y
                ) AS j
                  ON i.a = j.a""",
                ["a"],
                [("a",), ("b",), ("c",), ("d",)],
            ),
            (
                "SELECT 1 AS a UNION SELECT 2 AS a UNION SELECT 3 AS a",
                ["a"],
                [(1,), (2,), (3,)],
            ),
            (
                "SELECT 1 / 2 AS a",
                ["a"],
                [
                    (0.5,),
                ],
            ),
            ("SELECT 1 / 0 AS a", ["a"], ZeroDivisionError),
            (
                exp.select(
                    exp.alias_(exp.Literal.number(1).div(exp.Literal.number(2), typed=True), "a")
                ),
                ["a"],
                [
                    (0,),
                ],
            ),
            (
                exp.select(
                    exp.alias_(exp.Literal.number(1).div(exp.Literal.number(0), safe=True), "a")
                ),
                ["a"],
                [
                    (None,),
                ],
            ),
            (
                "SELECT a FROM x UNION ALL SELECT a FROM x LIMIT 1",
                ["a"],
                [("a",)],
            ),
        ]:
            with self.subTest(sql):
                if isinstance(rows, list):
                    result = execute(sql, schema=schema, tables=tables)
                    self.assertEqual(result.columns, tuple(cols))
                    self.assertEqual(set(result.rows), set(rows))
                else:
                    with self.assertRaises(ExecuteError) as ctx:
                        execute(sql, schema=schema, tables=tables)
                    self.assertIsInstance(ctx.exception.__cause__, rows)

        duplicate_tables = {
            "x": [{"a": 1}, {"a": 1}, {"a": 1}, {"a": 2}],
            "y": [{"a": 1}, {"a": 1}, {"a": 3}],
        }
        self.assertEqual(
            execute("SELECT a FROM x INTERSECT ALL SELECT a FROM y", tables=duplicate_tables).rows,
            [(1,), (1,)],
        )
        self.assertEqual(
            execute("SELECT a FROM x EXCEPT ALL SELECT a FROM y", tables=duplicate_tables).rows,
            [(1,), (2,)],
        )
        self.assertEqual(
            execute("SELECT a FROM x INTERSECT SELECT a FROM y", tables=duplicate_tables).rows,
            [(1,)],
        )
        self.assertEqual(
            execute("SELECT a FROM x EXCEPT SELECT a FROM y", tables=duplicate_tables).rows,
            [(2,)],
        )

    def test_set_operation_order_by(self):
        schema = {"x": {"a": "int"}, "y": {"b": "int"}}
        tables = {"x": [{"a": 3}, {"a": 1}], "y": [{"b": 1}, {"b": 2}]}

        for sql, expected in (
            ("SELECT a FROM x UNION ALL SELECT b FROM y ORDER BY a", [(1,), (1,), (2,), (3,)]),
            ("SELECT a FROM x UNION SELECT b FROM y ORDER BY a", [(1,), (2,), (3,)]),
            ("SELECT a FROM x UNION ALL SELECT b FROM y ORDER BY a DESC", [(3,), (2,), (1,), (1,)]),
            ("SELECT a FROM x UNION ALL SELECT b FROM y ORDER BY a LIMIT 2", [(1,), (1,)]),
            ("SELECT a FROM x UNION ALL (SELECT b FROM y LIMIT 1) ORDER BY a", [(1,), (1,), (3,)]),
            ("SELECT a FROM x EXCEPT SELECT b FROM y ORDER BY a", [(3,)]),
            ("SELECT a FROM x INTERSECT SELECT b FROM y ORDER BY a", [(1,)]),
        ):
            with self.subTest(sql):
                self.assertEqual(execute(sql, schema, tables=tables).rows, expected)

    def test_offset_order_by(self):
        schema = {"x": {"a": "int"}, "y": {"b": "int"}}
        tables = {"x": [{"a": a} for a in (3, 1, 5, 2, 4)], "y": [{"b": 7}, {"b": 6}]}

        for sql, expected in (
            ("SELECT a FROM x ORDER BY a OFFSET 2", [(3,), (4,), (5,)]),
            ("SELECT a FROM x ORDER BY a LIMIT 2 OFFSET 1", [(2,), (3,)]),
            ("SELECT a FROM x ORDER BY a LIMIT 2 OFFSET 10", []),
            ("SELECT a FROM x WHERE a > 1 ORDER BY a LIMIT 2 OFFSET 1", [(3,), (4,)]),
            (
                "SELECT a, COUNT(*) AS c FROM x GROUP BY a ORDER BY a LIMIT 2 OFFSET 2",
                [(3, 1), (4, 1)],
            ),
            (
                "SELECT a FROM x UNION ALL SELECT b FROM y ORDER BY a LIMIT 3 OFFSET 2",
                [(3,), (4,), (5,)],
            ),
        ):
            with self.subTest(sql):
                self.assertEqual(execute(sql, schema, tables=tables).rows, expected)

    def test_offset_no_order_by(self):
        x_values = (3, 1, 5, 2, 4)
        y_values = (7, 6)
        g_values = (1, 2, 2, 3, 4, 4, 5, 5)

        schema = {"x": {"a": "int"}, "y": {"b": "int"}, "g": {"v": "int"}}
        tables = {
            "x": [{"a": a} for a in x_values],
            "y": [{"b": b} for b in y_values],
            "g": [{"v": v} for v in g_values],
        }

        rows_x = {(a,) for a in x_values}
        rows_union = rows_x | {(b,) for b in y_values}
        groups = set(Counter(g_values).items())
        groups_having_count = {group for group in groups if group[1] > 1}
        groups_having_key = {group for group in groups if group[0] > 2}

        # Row order is unspecified without ORDER BY, so assert cardinality and membership.
        for sql, count, allowed in (
            ("SELECT a FROM x OFFSET 2", 3, rows_x),
            ("SELECT a FROM x LIMIT 2 OFFSET 1", 2, rows_x),
            ("SELECT v, COUNT(*) AS c FROM g GROUP BY v LIMIT 2 OFFSET 1", 2, groups),
            ("SELECT v, COUNT(*) AS c FROM g GROUP BY v OFFSET 3", 2, groups),
            (
                "SELECT v, COUNT(*) AS c FROM g GROUP BY v HAVING COUNT(*) > 1 LIMIT 2",
                2,
                groups_having_count,
            ),
            (
                "SELECT v, COUNT(*) AS c FROM g GROUP BY v HAVING COUNT(*) > 1 LIMIT 2 OFFSET 1",
                2,
                groups_having_count,
            ),
            (
                "SELECT v, COUNT(*) AS c FROM g GROUP BY v HAVING v > 2 LIMIT 2 OFFSET 1",
                2,
                groups_having_key,
            ),
            ("SELECT a FROM x UNION ALL SELECT b FROM y LIMIT 3 OFFSET 2", 3, rows_union),
            ("SELECT a FROM x UNION ALL SELECT b FROM y OFFSET 2", 5, rows_union),
            ("SELECT COUNT(*) AS c FROM x LIMIT 1 OFFSET 1", 0, {(5,)}),
        ):
            with self.subTest(sql):
                rows = execute(sql, schema, tables=tables).rows
                self.assertEqual(len(rows), count)
                self.assertLessEqual(set(rows), allowed)

    def test_outer_joins_preserve_unmatched_rows(self):
        tables = {
            "x": [{"id": 1}, {"id": 2}],
            "y": [{"id": 2}, {"id": 3}],
        }

        self.assertEqual(
            set(execute("SELECT x.id, y.id FROM x FULL JOIN y ON x.id = y.id", tables=tables).rows),
            {(1, None), (2, 2), (None, 3)},
        )
        self.assertEqual(
            set(
                execute(
                    "SELECT x.id, y.id FROM x LEFT JOIN y ON x.id = y.id AND y.id > 2",
                    tables=tables,
                ).rows
            ),
            {(1, None), (2, None)},
        )
        self.assertEqual(
            execute("SELECT x.id, y.id FROM x JOIN y ON x.id < y.id", tables=tables).rows,
            [(1, 2), (1, 3), (2, 3)],
        )

    def test_execute_catalog_db_table(self):
        tables = {
            "catalog": {
                "db": {
                    "x": [
                        {"a": "a"},
                        {"a": "b"},
                        {"a": "c"},
                    ],
                }
            }
        }
        schema = {
            "catalog": {
                "db": {
                    "x": {
                        "a": "VARCHAR",
                    }
                }
            }
        }
        result1 = execute("SELECT * FROM x", schema=schema, tables=tables)
        result2 = execute("SELECT * FROM catalog.db.x", schema=schema, tables=tables)
        assert result1.columns == result2.columns
        assert result1.rows == result2.rows

    def test_execute_tables(self):
        tables = {
            "sushi": [
                {"id": 1, "price": 1.0},
                {"id": 2, "price": 2.0},
                {"id": 3, "price": 3.0},
            ],
            "order_items": [
                {"sushi_id": 1, "order_id": 1},
                {"sushi_id": 1, "order_id": 1},
                {"sushi_id": 2, "order_id": 1},
                {"sushi_id": 3, "order_id": 2},
            ],
            "orders": [
                {"id": 1, "user_id": 1},
                {"id": 2, "user_id": 2},
            ],
        }

        self.assertEqual(
            execute(
                """
            SELECT
              o.user_id,
              SUM(s.price) AS price
            FROM orders o
            JOIN order_items i
              ON o.id = i.order_id
            JOIN sushi s
              ON i.sushi_id = s.id
            GROUP BY o.user_id
        """,
                tables=tables,
            ).rows,
            [
                (1, 4.0),
                (2, 3.0),
            ],
        )

        self.assertEqual(
            execute(
                """
            SELECT
              o.id, x.*
            FROM orders o
            LEFT JOIN (
                SELECT
                  1 AS id, 'b' AS x
                UNION ALL
                SELECT
                  3 AS id, 'c' AS x
            ) x
              ON o.id = x.id
        """,
                tables=tables,
            ).rows,
            [(1, 1, "b"), (2, None, None)],
        )
        self.assertEqual(
            execute(
                """
            SELECT
              o.id, x.*
            FROM orders o
            RIGHT JOIN (
                SELECT
                  1 AS id,
                  'b' AS x
                UNION ALL
                SELECT
                  3 AS id, 'c' AS x
            ) x
              ON o.id = x.id
        """,
                tables=tables,
            ).rows,
            [
                (1, 1, "b"),
                (None, 3, "c"),
            ],
        )

    def test_execute_subqueries(self):
        tables = {
            "table": [
                {"a": 1, "b": 1},
                {"a": 2, "b": 2},
            ],
        }

        self.assertEqual(
            execute(
                """
            SELECT *
            FROM table
            WHERE a = (SELECT MAX(a) FROM table)
        """,
                tables=tables,
            ).rows,
            [
                (2, 2),
            ],
        )

        table1_view = exp.Select().select("id", "sub_type").from_("table1").subquery()
        select_from_sub_query = exp.Select().select("id AS id_alias", "sub_type").from_(table1_view)
        expression = exp.Select().select("*").from_("cte1").with_("cte1", as_=select_from_sub_query)

        schema = {"table1": {"id": "str", "sub_type": "str"}}
        executed = execute(expression, tables={t: [] for t in schema}, schema=schema)

        self.assertEqual(executed.rows, [])
        self.assertEqual(executed.columns, ("id_alias", "sub_type"))

    def test_subqueries(self):
        # expected rows are duckdb's, which postgres agrees with on every case here
        schema = {
            "x": {"a": "int"},
            "y": {"b": "int"},
            "empty_table": {"b": "int"},
            "tbl_with_null": {"b": "int"},
        }
        tables = {
            "x": [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}],
            "y": [{"b": 2}, {"b": 3}],
            "empty_table": [],
            "tbl_with_null": [{"b": 2}, {"b": None}],
        }
        cases = (
            ("SELECT a FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE b = x.a OR b = 3)", []),
            (
                "SELECT a FROM x WHERE EXISTS (SELECT 1 FROM y WHERE NOT b = x.a)",
                [(1,), (2,), (3,), (5,)],
            ),
            ("SELECT a FROM x WHERE EXISTS (SELECT 1 FROM empty_table)", []),
            (
                "SELECT a, (SELECT MAX(b) FROM y WHERE b > x.a) AS m FROM x",
                [(1, 3), (2, 3), (3, None), (5, None)],
            ),
            ("SELECT a FROM x WHERE (SELECT COUNT(*) FROM y WHERE b > x.a) > 0", [(1,), (2,)]),
            ("SELECT a FROM x WHERE a IN (SELECT b FROM y WHERE b = x.a OR b = 3)", [(2,), (3,)]),
            ("SELECT a FROM x WHERE a NOT IN (SELECT b FROM y)", [(1,), (5,)]),
            ("SELECT a FROM x WHERE a IN (SELECT b FROM tbl_with_null)", [(2,)]),
            ("SELECT a FROM x WHERE a NOT IN (SELECT b FROM tbl_with_null)", []),
            ("SELECT a FROM x WHERE a IN (5, (SELECT MIN(b) FROM y WHERE b > x.a))", [(5,)]),
            ("SELECT a FROM x WHERE (SELECT MIN(b) FROM y WHERE b > x.a) IN (1, 2)", [(1,)]),
            ("SELECT a FROM x WHERE a > ANY (SELECT b FROM tbl_with_null)", [(3,), (5,)]),
            ("SELECT a FROM x WHERE a > ALL (SELECT b FROM tbl_with_null)", []),
            ("SELECT a FROM x WHERE a > ANY (SELECT b FROM empty_table)", []),
            (
                "SELECT a FROM x WHERE a > ALL (SELECT b FROM empty_table)",
                [(1,), (2,), (3,), (5,)],
            ),
            ("SELECT a FROM x WHERE a > SOME (SELECT b FROM y)", [(3,), (5,)]),
            (
                "SELECT a FROM x WHERE EXISTS (SELECT 1 FROM y WHERE b = x.a AND EXISTS "
                "(SELECT 1 FROM tbl_with_null WHERE tbl_with_null.b = y.b))",
                [(2,)],
            ),
            (
                "SELECT a FROM x WHERE EXISTS (SELECT 1 FROM y WHERE b = 99 OR EXISTS "
                "(SELECT 1 FROM tbl_with_null WHERE tbl_with_null.b = x.a OR tbl_with_null.b = 99))",
                [(2,)],
            ),
            (
                "SELECT a FROM x WHERE a IN (SELECT b FROM y WHERE b = x.a) "
                "OR a IN (SELECT b FROM tbl_with_null WHERE b = x.a)",
                [(2,), (3,)],
            ),
            (
                "SELECT a FROM x WHERE a IN (SELECT b FROM y UNION SELECT b FROM tbl_with_null)",
                [(2,), (3,)],
            ),
            (
                "SELECT a FROM x WHERE EXISTS ((SELECT 1 FROM y WHERE b = x.a OR b = 99))",
                [(2,), (3,)],
            ),
            ("SELECT a FROM x GROUP BY a HAVING MAX(a) > (SELECT MIN(b) FROM y)", [(3,), (5,)]),
            (
                "WITH w AS (SELECT b, COUNT(*) AS k FROM y GROUP BY b) SELECT a FROM x "
                "WHERE EXISTS (SELECT 1 FROM w WHERE w.b = x.a OR w.k = 2)",
                [(2,), (3,)],
            ),
            (
                "WITH c AS (SELECT b FROM y) SELECT a FROM x WHERE a IN (SELECT b FROM c) "
                "AND NOT EXISTS (SELECT 1 FROM c WHERE b = x.a OR b = 99)",
                [],
            ),
            (
                "SELECT a FROM x WHERE EXISTS (SELECT 1 FROM y WHERE b = x.a OR EXISTS "
                "(SELECT 1 FROM tbl_with_null WHERE tbl_with_null.b = x.a))",
                [(2,), (3,)],
            ),
        )

        for sql, expected in cases:
            with self.subTest(sql):
                self.assertCountEqual(execute(sql, schema, tables=tables).rows, expected)

    def test_subquery_memoization(self):
        schema = {"x": {"a": "int"}, "y": {"b": "int"}}
        tables = {"x": [{"a": i % 3} for i in range(12)], "y": [{"b": 1}, {"b": 2}]}

        for sql, expected_plans in (
            ("SELECT a FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE b = x.a OR b = 9)", 1),
            (
                "SELECT a FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE b = 9 OR EXISTS "
                "(SELECT 1 FROM y AS y2 WHERE y2.b = x.a OR y2.b = 9))",
                2,
            ),
        ):
            with self.subTest(sql):
                executor = PythonExecutor(tables=ensure_tables(tables))
                executor.execute(Plan(optimize(sql, schema, leave_tables_isolated=True)))

                # one plan per subquery, and one run per distinct correlated value of the 12 rows
                self.assertEqual(len(executor._subquery_plans), expected_plans)
                self.assertEqual(
                    [len(cache) for _, cache in executor._subquery_plans.values()],
                    [3] * expected_plans,
                )

    def test_subquery_execution_does_not_mutate_the_plan(self):
        schema = {"x": {"a": "int"}, "y": {"b": "int"}}
        tables = {"x": [{"a": 1}, {"a": 2}], "y": [{"b": 2}]}
        sql = "SELECT a FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE b = x.a OR b = 9)"

        plan = Plan(optimize(sql, schema, leave_tables_isolated=True))
        before = plan.expression.sql()
        PythonExecutor(tables=ensure_tables(tables)).execute(plan)

        self.assertEqual(plan.expression.sql(), before)

    def test_subquery_cardinality(self):
        # a scalar subquery must yield a single row and column, as in duckdb and postgres
        for sql, tables in (
            (
                "SELECT a, (SELECT b FROM y) AS m FROM x",
                {"x": [{"a": 1}], "y": [{"b": 2}, {"b": 3}]},
            ),
            ("SELECT a, (SELECT b, b FROM y) AS m FROM x", {"x": [{"a": 1}], "y": [{"b": 2}]}),
            # the column count is a property of the query, so it is rejected even when no
            # outer row would have evaluated it -- duckdb reports this as a binder error
            ("SELECT a, (SELECT b, b FROM y) AS m FROM x", {"x": [], "y": [{"b": 2}]}),
        ):
            with self.subTest(sql):
                with self.assertRaises(ExecuteError):
                    execute(sql, schema={"x": {"a": "int"}, "y": {"b": "int"}}, tables=tables)

    def test_correlated_count(self):
        tables = {
            "parts": [{"pnum": 0, "qoh": 1}],
            "supplies": [],
        }

        schema = {
            "parts": {"pnum": "int", "qoh": "int"},
            "supplies": {"pnum": "int", "shipdate": "int"},
        }

        self.assertEqual(
            execute(
                """
			select *
			from parts
			where parts.qoh >= (
			  select count(supplies.shipdate) + 1
			  from supplies
			  where supplies.pnum = parts.pnum and supplies.shipdate < 10
            )
        """,
                tables=tables,
                schema=schema,
            ).rows,
            [
                (0, 1),
            ],
        )

    def test_table_depth_mismatch(self):
        tables = {"table": []}
        schema = {"db": {"table": {"col": "VARCHAR"}}}
        with self.assertRaises(ExecuteError):
            execute("SELECT * FROM table", schema=schema, tables=tables)

    def test_tables(self):
        tables = ensure_tables(
            {
                "catalog1": {
                    "db1": {
                        "t1": [
                            {"a": 1},
                        ],
                        "t2": [
                            {"a": 1},
                        ],
                    },
                    "db2": {
                        "t3": [
                            {"a": 1},
                        ],
                        "t4": [
                            {"a": 1},
                        ],
                    },
                },
                "catalog2": {
                    "db3": {
                        "t5": Table(columns=("a",), rows=[(1,)]),
                        "t6": Table(columns=("a",), rows=[(1,)]),
                    },
                    "db4": {
                        "t7": Table(columns=("a",), rows=[(1,)]),
                        "t8": Table(columns=("a",), rows=[(1,)]),
                    },
                },
            }
        )

        t1 = tables.find(exp.table_(table="t1", db="db1", catalog="catalog1"))
        self.assertEqual(t1.columns, ("a",))
        self.assertEqual(t1.rows, [(1,)])

        t8 = tables.find(exp.table_(table="t8"))
        self.assertEqual(t1.columns, t8.columns)
        self.assertEqual(t1.rows, t8.rows)

    def test_static_queries(self):
        for sql, cols, rows in [
            ("SELECT 1", ["1"], [(1,)]),
            ("SELECT 1 + 2 AS x", ["x"], [(3,)]),
            ("SELECT CONCAT('a', 'b') AS x", ["x"], [("ab",)]),
            ("SELECT CONCAT('a', 1) AS x", ["x"], [("a1",)]),
            ("SELECT 1 AS x, 2 AS y", ["x", "y"], [(1, 2)]),
            ("SELECT 'foo' LIMIT 1", ["foo"], [("foo",)]),
            (
                "SELECT SUM(x), COUNT(x) FROM (SELECT 1 AS x WHERE FALSE)",
                ["_col_0", "_col_1"],
                [(None, 0)],
            ),
        ]:
            with self.subTest(sql):
                result = execute(sql)
                self.assertEqual(result.columns, tuple(cols))
                self.assertEqual(result.rows, rows)

    def test_operators_apply_to_a_whole_case_expression(self):
        tables = {"t": [{"a": 1}, {"a": 2}]}

        for sql, expected in (
            ("SELECT (CASE WHEN a = 1 THEN 'x' END) IS NOT NULL AS c FROM t", [(True,), (False,)]),
            ("SELECT (CASE WHEN a = 1 THEN 'x' END) IS NULL AS c FROM t", [(False,), (True,)]),
            ("SELECT (CASE WHEN a = 1 THEN 1 ELSE 2 END) + 10 AS c FROM t", [(11,), (12,)]),
        ):
            with self.subTest(sql):
                self.assertEqual(execute(sql, tables=tables).rows, expected)

    def test_negated_like(self):
        """NOT LIKE must exclude what LIKE matches, and match no NULL either."""
        tables = {"t": [{"s": "Bump version"}, {"s": "Add feature"}, {"s": None}]}

        result = execute("SELECT s FROM t WHERE s LIKE 'Bump%'", tables=tables)
        self.assertEqual(result.rows, [("Bump version",)])

        for sql in (
            "SELECT s FROM t WHERE s NOT LIKE 'Bump%'",
            "SELECT s FROM t WHERE NOT (s LIKE 'Bump%')",
        ):
            with self.subTest(sql):
                self.assertEqual(execute(sql, tables=tables).rows, [("Add feature",)])

    def test_like_semantics(self):
        tables = {"t": [{"s": "Bump version"}, {"s": "Add feature"}]}

        for pattern, matches in (
            ("Bump", []),
            ("Bump%", [("Bump version",)]),
            ("%version", [("Bump version",)]),
            ("Bump_version", [("Bump version",)]),
            ("B.mp version", []),
            ("Bump version", [("Bump version",)]),
        ):
            with self.subTest(pattern):
                rows = execute(f"SELECT s FROM t WHERE s LIKE '{pattern}'", tables=tables).rows
                self.assertEqual(rows, matches)

                negated = execute(
                    f"SELECT s FROM t WHERE s NOT LIKE '{pattern}'", tables=tables
                ).rows
                self.assertEqual(
                    negated, [r for r in [("Bump version",), ("Add feature",)] if r not in matches]
                )

    def test_length(self):
        tables = {"t": [{"s": "abc"}, {"s": ""}, {"s": None}]}

        for func in ("LENGTH", "CHAR_LENGTH"):
            with self.subTest(func):
                rows = execute(f"SELECT {func}(s) FROM t", tables=tables).rows
                self.assertEqual(rows, [(3,), (0,), (None,)])

    def test_dpipe(self):
        tables = {"t": [{"a": "x", "b": "y", "n": 1, "arr": [1, 2], "nul": None}]}

        for expression, expected in (
            ("a || b", "xy"),
            ("a || b || a", "xyx"),
            ("a || n", "x1"),
            ("n || a", "1x"),
            ("a || nul", None),
            ("nul || a", None),
            ("arr || arr", [1, 2, 1, 2]),
            ("arr || n", [1, 2, 1]),
            ("n || arr", [1, 1, 2]),
            ("arr || nul", None),
            ("ARRAY_CONCAT(arr, arr)", [1, 2, 1, 2]),
            ("ARRAY_CAT(arr, arr, arr)", [1, 2, 1, 2, 1, 2]),
        ):
            with self.subTest(expression):
                rows = execute(f"SELECT {expression} FROM t", tables=tables).rows
                self.assertEqual(rows, [(expected,)])

    def test_dpipe_source_dialect_coercion(self):
        tables = {"t": [{"a": "x", "n": 1}]}

        rows = execute("SELECT a || n FROM t", tables=tables, dialect="postgres").rows
        self.assertEqual(rows, [("x1",)])

        with self.assertRaises(ExecuteError):
            execute("SELECT a || n FROM t", tables=tables, dialect="trino")

    def test_ilike_semantics(self):
        tables = {"t": [{"s": "Bump Version"}, {"s": "B.mp Version"}, {"s": "Add feature"}]}

        for pattern, matches in (
            ("bump", []),
            ("bump%", [("Bump Version",)]),
            ("%version", [("Bump Version",), ("B.mp Version",)]),
            ("bump_version", [("Bump Version",)]),
            ("b.mp version", [("B.mp Version",)]),
            ("b_mp version", [("Bump Version",), ("B.mp Version",)]),
            ("bump version", [("Bump Version",)]),
        ):
            for cased in (pattern.lower(), pattern.upper()):
                with self.subTest(cased):
                    rows = execute(f"SELECT s FROM t WHERE s ILIKE '{cased}'", tables=tables).rows
                    self.assertEqual(rows, matches)

                    negated = execute(
                        f"SELECT s FROM t WHERE s NOT ILIKE '{cased}'", tables=tables
                    ).rows
                    self.assertEqual(
                        negated, [(r["s"],) for r in tables["t"] if (r["s"],) not in matches]
                    )

    def test_typed_division(self):
        """Postgres' 10 / 3 is 3, but its 10 / 3.0 and 10 / 3::numeric are not."""
        schema = {"t": {"n": "INT", "d": "DECIMAL"}}
        tables = {"t": [{"n": 10, "d": 3}]}

        for sql, expected in (
            ("SELECT n / 3.0 AS x FROM t", 10 / 3),
            ("SELECT 3.0 / n AS x FROM t", 3.0 / 10),
            ("SELECT CAST(n AS DOUBLE) / 3 AS x FROM t", 10 / 3),
            ("SELECT n / d AS x FROM t", 10 / 3),  # decimal: real, but not a float
            ("SELECT n / 3 AS x FROM t", 3),
            ("SELECT -n / 3 AS x FROM t", -3),
        ):
            for dialect in ("postgres", "sqlite"):
                with self.subTest(f"{dialect}: {sql}"):
                    result = execute(sql, schema=schema, tables=tables, dialect=dialect)
                    self.assertEqual(result.rows, [(expected,)])

        result = execute("SELECT n / 3 AS x FROM t", tables=tables, dialect="postgres")
        self.assertEqual(result.rows, [(3,)])

    def test_typed_division_of_null_is_null(self):
        schema = {"t": {"n": "INT"}}
        tables = {"t": [{"n": None}]}

        for sql in ("SELECT n / 3 AS x FROM t", "SELECT 3 / n AS x FROM t"):
            for dialect in ("postgres", "sqlite"):
                with self.subTest(f"{dialect}: {sql}"):
                    result = execute(sql, schema=schema, tables=tables, dialect=dialect)
                    self.assertEqual(result.rows, [(None,)])

    def test_null_ordering_honors_nulls_first_and_dialect_defaults(self):
        schema = {"t": {"a": "INT"}}
        tables = {"t": [{"a": 1}, {"a": None}, {"a": 3}, {"a": None}]}

        for sql, expected in (
            ("SELECT a FROM t ORDER BY a NULLS FIRST", [None, None, 1, 3]),
            ("SELECT a FROM t ORDER BY a NULLS LAST", [1, 3, None, None]),
            ("SELECT a FROM t ORDER BY a DESC NULLS FIRST", [None, None, 3, 1]),
            ("SELECT a FROM t ORDER BY a DESC NULLS LAST", [3, 1, None, None]),
        ):
            for dialect in ("postgres", "duckdb", "mysql"):
                with self.subTest(f"{dialect}: {sql}"):
                    result = execute(sql, schema=schema, tables=tables, dialect=dialect)
                    self.assertEqual([row[0] for row in result.rows], expected)

        for dialect, ascending, descending in (
            ("postgres", [1, 3, None, None], [None, None, 3, 1]),
            ("mysql", [None, None, 1, 3], [3, 1, None, None]),
            ("duckdb", [1, 3, None, None], [3, 1, None, None]),
        ):
            for sql, expected in (
                ("SELECT a FROM t ORDER BY a", ascending),
                ("SELECT a FROM t ORDER BY a DESC", descending),
            ):
                with self.subTest(f"{dialect}: {sql}"):
                    result = execute(sql, schema=schema, tables=tables, dialect=dialect)
                    self.assertEqual([row[0] for row in result.rows], expected)

    def test_aggregate_without_group_by(self):
        result = execute("SELECT SUM(x) FROM t", tables={"t": [{"x": 1}, {"x": 2}]})
        self.assertEqual(result.columns, ("_col_0",))
        self.assertEqual(result.rows, [(3,)])

    def test_in_subquery_without_a_from(self):
        tables = {"x": [{"a": 1}, {"a": 2}, {"a": None}]}

        for sql, rows in (
            ("SELECT x.a FROM x WHERE x.a IN (SELECT 1)", [(1,)]),
            ("SELECT x.a FROM x WHERE x.a IN (SELECT 1 + 1)", [(2,)]),
            ("SELECT x.a FROM x WHERE x.a IN (SELECT 1 UNION SELECT 2)", [(1,), (2,)]),
        ):
            with self.subTest(sql):
                self.assertEqual(execute(sql, tables=tables).rows, rows)

    def test_scalar_functions(self):
        now = datetime.datetime.now()

        for sql, expected in [
            ("CONCAT('a', 'b')", "ab"),
            ("CONCAT('a', NULL)", None),
            ("CONCAT_WS('_', 'a', 'b')", "a_b"),
            ("STR_POSITION('foobarbar', 'bar')", 4),
            ("STR_POSITION('foobarbar', 'bar', 5)", 7),
            ("STR_POSITION('foobarbar', NULL)", None),
            ("STR_POSITION(NULL, 'bar')", None),
            ("UPPER('foo')", "FOO"),
            ("UPPER(NULL)", None),
            ("LOWER('FOO')", "foo"),
            ("LOWER(NULL)", None),
            ("IFNULL('a', 'b')", "a"),
            ("IFNULL(NULL, 'b')", "b"),
            ("IFNULL(NULL, NULL)", None),
            ("SUBSTRING('12345')", "12345"),
            ("SUBSTRING('12345', 3)", "345"),
            ("SUBSTRING('12345', 3, 0)", ""),
            ("SUBSTRING('12345', 3, 1)", "3"),
            ("SUBSTRING('12345', 3, 2)", "34"),
            ("SUBSTRING('12345', 3, 3)", "345"),
            ("SUBSTRING('12345', 3, 4)", "345"),
            ("SUBSTRING('12345', -3)", "345"),
            ("SUBSTRING('12345', -3, 0)", ""),
            ("SUBSTRING('12345', -3, 1)", "3"),
            ("SUBSTRING('12345', -3, 2)", "34"),
            ("SUBSTRING('12345', 0)", ""),
            ("SUBSTRING('12345', 0, 1)", ""),
            ("SUBSTRING(NULL)", None),
            ("SUBSTRING(NULL, 1)", None),
            ("CAST(1 AS TEXT)", "1"),
            ("CAST('1' AS LONG)", 1),
            ("CAST('1.1' AS FLOAT)", 1.1),
            ("CAST('12:05:01' AS TIME)", time(12, 5, 1)),
            ("COALESCE(NULL)", None),
            ("COALESCE(NULL, NULL)", None),
            ("COALESCE(NULL, 'b')", "b"),
            ("COALESCE('a', 'b')", "a"),
            ("1 << 1", 2),
            ("1 >> 1", 0),
            ("1 & 1", 1),
            ("1 | 1", 1),
            ("1 < 1", False),
            ("1 <= 1", True),
            ("1 > 1", False),
            ("1 >= 1", True),
            ("1 + NULL", None),
            ("IF(true, 1, 0)", 1),
            ("IF(false, 1, 0)", 0),
            ("CASE WHEN 0 = 1 THEN 'foo' ELSE 'bar' END", "bar"),
            ("CAST('2022-01-01' AS DATE) + INTERVAL '1' DAY", date(2022, 1, 2)),
            ("INTERVAL '1' week", datetime.timedelta(weeks=1)),
            ("1 IN (1, 2, 3)", True),
            ("1 IN (2, 3)", False),
            ("1 IN (1)", True),
            ("NULL IS NULL", True),
            ("NULL IS NOT NULL", False),
            ("NULL = NULL", None),
            ("NULL <> NULL", None),
            ("YEAR(CURRENT_TIMESTAMP)", now.year),
            ("MONTH(CURRENT_TIME)", now.month),
            ("DAY(CURRENT_DATETIME())", now.day),
            ("YEAR(CURRENT_DATE())", now.year),
            ("MONTH(CURRENT_DATE())", now.month),
            ("DAY(CURRENT_DATE())", now.day),
            ("YEAR(CURRENT_TIMESTAMP) + 1", now.year + 1),
            (
                "YEAR(CURRENT_TIMESTAMP) IN (YEAR(CURRENT_TIMESTAMP) + 1, YEAR(CURRENT_TIMESTAMP) * 10)",
                False,
            ),
            ("YEAR(CURRENT_TIMESTAMP) = (YEAR(CURRENT_TIMESTAMP))", True),
            ("YEAR(CURRENT_TIMESTAMP) <> (YEAR(CURRENT_TIMESTAMP))", False),
            ("YEAR(CURRENT_DATE()) + 1", now.year + 1),
            (
                "YEAR(CURRENT_DATE()) IN (YEAR(CURRENT_DATE()) + 1, YEAR(CURRENT_DATE()) * 10)",
                False,
            ),
            ("YEAR(CURRENT_DATE()) = (YEAR(CURRENT_DATE()))", True),
            ("YEAR(CURRENT_DATE()) <> (YEAR(CURRENT_DATE()))", False),
            ("1::bool", True),
            ("0::bool", False),
            ("MAP(['a'], [1]).a", 1),
            ("MAP()", {}),
            ("STRFTIME('%j', '2023-03-23 15:00:00')", "082"),
            ("STRFTIME('%j', NULL)", None),
            ("DATESTRTODATE('2022-01-01')", date(2022, 1, 1)),
            ("TIMESTRTOTIME('2022-01-01')", datetime.datetime(2022, 1, 1)),
            ("LEFT('12345', 3)", "123"),
            ("RIGHT('12345', 3)", "345"),
            ("DATEDIFF('2022-01-03'::date, '2022-01-01'::TIMESTAMP::DATE)", 2),
            ("TRIM(' foo ')", "foo"),
            ("TRIM('afoob', 'ab')", "foo"),
            ("REVERSE('foo')", "oof"),
            ("REVERSE(NULL)", None),
            ("ARRAY_JOIN(['foo', 'bar'], ':')", "foo:bar"),
            ("ARRAY_JOIN(['hello', null ,'world'], ' ', ',')", "hello , world"),
            ("ARRAY_JOIN(['', null ,'world'], ' ', ',')", " , world"),
            ("STRUCT('foo', 'bar', null, null)", {"foo": "bar"}),
            ("ROUND(1.5)", 2),
            ("ROUND(1.2)", 1),
            ("ROUND(1.2345, 2)", 1.23),
            ("ROUND(NULL)", None),
            ("POWER(2, 3)", 8),
            ("POWER(NULL, 3)", None),
            ("POWER(2, NULL)", None),
            ("POWER(NULL, NULL)", None),
            (
                "UNIXTOTIME(1659981729)",
                datetime.datetime(2022, 8, 8, 18, 2, 9, tzinfo=datetime.timezone.utc),
            ),
            ("TIMESTRTOTIME('2013-04-05 01:02:03')", datetime.datetime(2013, 4, 5, 1, 2, 3)),
            (
                "UNIXTOTIME(40 * 365 * 86400)",
                datetime.datetime(2009, 12, 22, 00, 00, 00, tzinfo=datetime.timezone.utc),
            ),
            (
                "STRTOTIME('08/03/2024 12:34:56', '%d/%m/%Y %H:%M:%S')",
                datetime.datetime(2024, 3, 8, 12, 34, 56),
            ),
            ("STRTOTIME('27/01/2024', '%d/%m/%Y')", datetime.datetime(2024, 1, 27)),
        ]:
            with self.subTest(sql):
                result = execute(f"SELECT {sql}")
                self.assertEqual(result.rows, [(expected,)])

        result = execute(
            "WITH t AS (SELECT 'a' AS c1, 'b' AS c2) SELECT NVL(c1, c2) FROM t",
            dialect="oracle",
        )
        self.assertEqual(result.rows, [("a",)])

    def test_sql_three_valued_boolean_logic(self):
        for sql, expected in [
            ("NOT TRUE", False),
            ("NOT FALSE", True),
            ("NOT NULL", None),
            ("TRUE AND NULL", None),
            ("FALSE AND NULL", False),
            ("TRUE OR NULL", True),
            ("FALSE OR NULL", None),
            ("NULL IN (1, 2)", None),
            ("3 NOT IN (1, 2, NULL)", None),
            ("3 NOT IN (1, 2)", True),
        ]:
            with self.subTest(sql):
                self.assertEqual(execute(f"SELECT {sql}").rows, [(expected,)])

        tables = {
            "policy": [
                {"id": 1, "flag": True},
                {"id": 2, "flag": False},
                {"id": 3, "flag": None},
            ]
        }
        schema = {"policy": {"id": "INT", "flag": "BOOLEAN"}}
        self.assertEqual(
            execute("SELECT id FROM policy WHERE NOT flag", tables=tables, schema=schema).rows,
            [(2,)],
        )

    def test_sql_boolean_logic_with_numpy_scalars(self):
        tables = {
            "policy": [
                {"id": 1, "left_flag": np.bool_(True), "right_flag": np.bool_(True)},
                {"id": 2, "left_flag": np.bool_(False), "right_flag": np.bool_(True)},
                {"id": 3, "left_flag": None, "right_flag": np.bool_(True)},
            ]
        }
        schema = {
            "policy": {
                "id": "INT",
                "left_flag": "BOOLEAN",
                "right_flag": "BOOLEAN",
            }
        }

        self.assertEqual(
            execute("SELECT id FROM policy WHERE left_flag", tables=tables, schema=schema).rows,
            [(1,)],
        )
        self.assertEqual(
            execute(
                "SELECT id FROM policy WHERE left_flag AND right_flag",
                tables=tables,
                schema=schema,
            ).rows,
            [(1,)],
        )
        self.assertEqual(
            execute(
                "SELECT id FROM policy WHERE left_flag OR right_flag",
                tables=tables,
                schema=schema,
            ).rows,
            [(1,), (2,), (3,)],
        )

    def test_sql_boolean_logic_preserves_short_circuiting(self):
        evaluations = []

        def record_evaluation():
            evaluations.append(True)
            return True

        executor = PythonExecutor(env={"RECORD_EVALUATION": record_evaluation})
        context = executor.context({})

        for sql, expected in [
            ("FALSE AND RECORD_EVALUATION()", False),
            ("TRUE OR RECORD_EVALUATION()", True),
        ]:
            with self.subTest(sql):
                expression = parse_one(sql)
                self.assertEqual(context.eval(executor.generate(expression)), expected)

        self.assertEqual(evaluations, [])

    def test_case_sensitivity(self):
        result = execute("SELECT A AS A FROM X", tables={"x": [{"a": 1}]})
        self.assertEqual(result.columns, ("a",))
        self.assertEqual(result.rows, [(1,)])

        result = execute('SELECT A AS "A" FROM X', tables={"x": [{"a": 1}]})
        self.assertEqual(result.columns, ("A",))
        self.assertEqual(result.rows, [(1,)])

    def test_nested_table_reference(self):
        tables = {
            "some_catalog": {
                "some_schema": {
                    "some_table": [
                        {"id": 1, "price": 1.0},
                        {"id": 2, "price": 2.0},
                        {"id": 3, "price": 3.0},
                    ]
                }
            }
        }

        result = execute("SELECT * FROM some_catalog.some_schema.some_table s", tables=tables)

        self.assertEqual(result.columns, ("id", "price"))
        self.assertEqual(result.rows, [(1, 1.0), (2, 2.0), (3, 3.0)])

    def test_group_by(self):
        tables = {
            "x": [
                {"a": 1, "b": 10},
                {"a": 2, "b": 20},
                {"a": 3, "b": 28},
                {"a": 2, "b": 25},
                {"a": 1, "b": 40},
            ],
        }

        for sql, expected, columns in (
            (
                "SELECT a, AVG(b) FROM x GROUP BY a ORDER BY AVG(b)",
                [(2, 22.5), (1, 25.0), (3, 28.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, AVG(b) FROM x GROUP BY a having avg(b) > 23",
                [(1, 25.0), (3, 28.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, AVG(b) FROM x GROUP BY a having avg(b + 1) > 23",
                [(1, 25.0), (2, 22.5), (3, 28.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, AVG(b) FROM x GROUP BY a having sum(b) + 5 > 50",
                [(1, 25.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a + 1 AS a, AVG(b + 1) FROM x GROUP BY a + 1 having AVG(b + 1) > 26",
                [(4, 29.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, avg(b) FROM x GROUP BY a HAVING a = 1",
                [(1, 25.0)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a + 1, avg(b) FROM x GROUP BY a + 1 HAVING a + 1 = 2",
                [(2, 25.0)],
                ("_col_0", "_col_1"),
            ),
            (
                "SELECT a FROM x GROUP BY a ORDER BY AVG(b)",
                [(2,), (1,), (3,)],
                ("a",),
            ),
            (
                "SELECT a, SUM(b) FROM x GROUP BY a ORDER BY COUNT(*)",
                [(3, 28), (1, 50), (2, 45)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, SUM(b) FROM x GROUP BY a ORDER BY COUNT(*) DESC",
                [(1, 50), (2, 45), (3, 28)],
                ("a", "_col_1"),
            ),
            (
                "SELECT a, ARRAY_UNIQUE_AGG(b) FROM x GROUP BY a",
                [(1, [40, 10]), (2, [25, 20]), (3, [28])],
                ("a", "_col_1"),
            ),
        ):
            with self.subTest(sql):
                result = execute(sql, tables=tables)
                self.assertEqual(result.columns, columns)
                self.assertEqual(result.rows, expected)

    def test_nested_values(self):
        tables = {"foo": [{"raw": {"name": "Hello, World", "a": [{"b": 1}]}}]}

        result = execute("SELECT raw:name AS name FROM foo", dialect="snowflake", tables=tables)
        self.assertEqual(result.columns, ("NAME",))
        self.assertEqual(result.rows, [("Hello, World",)])

        result = execute("SELECT raw:a[0].b AS b FROM foo", dialect="snowflake", tables=tables)
        self.assertEqual(result.columns, ("B",))
        self.assertEqual(result.rows, [(1,)])

        result = execute("SELECT raw:a[1].b AS b FROM foo", dialect="snowflake", tables=tables)
        self.assertEqual(result.columns, ("B",))
        self.assertEqual(result.rows, [(None,)])

        result = execute("SELECT raw:a[0].c AS c FROM foo", dialect="snowflake", tables=tables)
        self.assertEqual(result.columns, ("C",))
        self.assertEqual(result.rows, [(None,)])

        tables = {
            '"ITEM"': [
                {"id": 1, "attributes": {"flavor": "cherry", "taste": "sweet"}},
                {"id": 2, "attributes": {"flavor": "lime", "taste": "sour"}},
                {"id": 3, "attributes": {"flavor": "apple", "taste": None}},
            ]
        }
        result = execute(
            "SELECT i.attributes.flavor FROM `ITEM` i", dialect="bigquery", tables=tables
        )

        self.assertEqual(result.columns, ("flavor",))
        self.assertEqual(result.rows, [("cherry",), ("lime",), ("apple",)])

        tables = {"t": [{"x": [1, 2, 3]}]}

        result = execute("SELECT x FROM t", dialect="duckdb", tables=tables)
        self.assertEqual(result.columns, ("x",))
        self.assertEqual(result.rows, [([1, 2, 3],)])

    def test_agg_order(self):
        plan = Plan(
            optimize("""
            SELECT
              AVG(bill_length_mm) AS avg_bill_length,
              AVG(bill_depth_mm) AS avg_bill_depth
            FROM penguins
            """)
        )

        assert [agg.alias for agg in plan.root.aggregations] == [
            "avg_bill_length",
            "avg_bill_depth",
        ]

    def test_table_to_pylist(self):
        columns = ["id", "product", "price"]
        rows = [[1, "Shirt", 20.0], [2, "Shoes", 60.0]]
        table = Table(columns=columns, rows=rows)
        expected = [
            {"id": 1, "product": "Shirt", "price": 20.0},
            {"id": 2, "product": "Shoes", "price": 60.0},
        ]
        self.assertEqual(table.to_pylist(), expected)
