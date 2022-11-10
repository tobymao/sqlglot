import unittest

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

from sqlglot import exp, parse_one
from sqlglot.executor import execute
from sqlglot.executor.python import Python
from tests.helpers import (
    FIXTURES_DIR,
    SKIP_INTEGRATION,
    TPCH_SCHEMA,
    load_sql_fixture_pairs,
)

DIR = FIXTURES_DIR + "/optimizer/tpc-h/"


@unittest.skipIf(SKIP_INTEGRATION, "Skipping Integration Tests since `SKIP_INTEGRATION` is set")
class TestExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect()

        for table in TPCH_SCHEMA:
            cls.conn.execute(
                f"""
                CREATE VIEW {table} AS
                SELECT *
                FROM READ_CSV_AUTO('{DIR}{table}.csv.gz')
                """
            )

        cls.cache = {}
        cls.sqls = [
            (sql, expected)
            for _, sql, expected in load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql")
        ]

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def cached_execute(self, sql):
        if sql not in self.cache:
            self.cache[sql] = self.conn.execute(sql).fetchdf()
        return self.cache[sql]

    def rename_anonymous(self, source, target):
        for i, column in enumerate(source.columns):
            if "_col_" in column:
                source.rename(columns={column: target.columns[i]}, inplace=True)

    def test_py_dialect(self):
        self.assertEqual(Python().generate(parse_one("'x '''")), r"'x \''")

    def test_optimized_tpch(self):
        for i, (sql, optimized) in enumerate(self.sqls[:20], start=1):
            with self.subTest(f"{i}, {sql}"):
                a = self.cached_execute(sql)
                b = self.conn.execute(optimized).fetchdf()
                self.rename_anonymous(b, a)
                assert_frame_equal(a, b)

    def test_execute_tpch(self):
        def to_csv(expression):
            if isinstance(expression, exp.Table):
                return parse_one(
                    f"READ_CSV('{DIR}{expression.name}.csv.gz', 'delimiter', '|') AS {expression.name}"
                )
            return expression

        for sql, _ in self.sqls[0:3]:
            a = self.cached_execute(sql)
            sql = parse_one(sql).transform(to_csv).sql(pretty=True)
            table = execute(sql, TPCH_SCHEMA)
            b = pd.DataFrame(table.rows, columns=table.columns)
            assert_frame_equal(a, b, check_dtype=False)

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
        ]:
            result = execute(sql, schema=schema, tables=tables)
            self.assertEqual(result.columns, tuple(cols))
            self.assertEqual(result.rows, rows)
