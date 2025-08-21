import unittest

from sqlglot import parse_one


class TestNetezza(unittest.TestCase):
    def test_basic_select_roundtrip(self):
        sql = "SELECT 1 AS x"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_table_distribute_on(self):
        sql = "CREATE TABLE t (a INT) DISTRIBUTE ON (a)"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_table_distribute_on_random(self):
        sql = "CREATE TABLE t (a INT) DISTRIBUTE ON RANDOM"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_table_organize_on(self):
        sql = "CREATE TABLE t (a INT, b INT) ORGANIZE ON (a, b)"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_type_mapping_byteint(self):
        expr = parse_one("CAST(a AS TINYINT)")
        self.assertEqual(expr.sql(dialect="netezza"), "CAST(a AS BYTEINT)")
