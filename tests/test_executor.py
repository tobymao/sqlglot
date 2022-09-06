import unittest

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

from sqlglot import exp, parse_one
from sqlglot.executor import execute
from sqlglot.executor.python import Python
from tests.helpers import FIXTURES_DIR, TPCH_SCHEMA, load_sql_fixture_pairs

DIR = FIXTURES_DIR + "/optimizer/tpc-h/"


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
        for sql, optimized in self.sqls[0:20]:
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
