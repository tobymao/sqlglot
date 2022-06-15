import unittest

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

from sqlglot.executor import execute
from tests.helpers import load_sql_fixture_pairs, FIXTURES_DIR, TPCH_SCHEMA


class TestExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect()

        for table in TPCH_SCHEMA:
            cls.conn.execute(
                f"""
                CREATE VIEW {table} AS
                SELECT *
                FROM READ_CSV_AUTO('{FIXTURES_DIR}/optimizer/tpc-h/{table}.csv.gz')
                """
            )

        cls.cache = {}
        cls.sqls = list(load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql"))

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

    def test_optimizer_tpch(self):
        for sql, optimized in self.sqls:
            import time
            now = time.time()
            a = self.cached_execute(sql)
            print(time.time() - now)
            now = time.time()
            b = self.conn.execute(optimized).fetchdf()
            print("op", time.time() - now)
            self.rename_anonymous(b, a)
            assert_frame_equal(a, b)

    def test_execute_tpch(self):
        for sql, _ in self.sqls[0:1]:
            a = self.cached_execute(sql)
            b = pd.DataFrame(execute(sql, TPCH_SCHEMA).table)
            assert_frame_equal(a, b, check_dtype=False)
