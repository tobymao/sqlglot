import unittest

from sqlglot.optimizer import optimize
from sqlglot.optimizer.decorrelate_subqueries import decorrelate_subqueries
from sqlglot.optimizer.expand_multi_table_selects import expand_multi_table_selects
from sqlglot.optimizer.projection_pushdown import projection_pushdown
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.quote_identities import quote_identities
from sqlglot.optimizer.simplify import simplify
from sqlglot import parse_one
from sqlglot.errors import OptimizeError
from tests.helpers import load_sql_fixture_pairs, load_sql_fixtures


class TestOptimizer(unittest.TestCase):
    maxDiff = None

    def test_optimize(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"a": "INT", "b": "INT"},
            "z": {"a": "INT", "c": "INT"},
        }
        self.assertEqual(
            optimize(
                parse_one("SELECT a FROM x"), schema=schema, db="db", catalog="c"
            ).sql(),
            'SELECT "x"."a" AS "a" FROM "c"."db"."x" AS "x"',
        )

        for sql, expected in load_sql_fixture_pairs("optimizer/optimizer.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    optimize(parse_one(sql), schema=schema).sql(pretty=True),
                    expected,
                )

    def test_qualify_tables(self):
        self.assertEqual(
            qualify_tables(parse_one("SELECT 1 FROM z"), db="db").sql(),
            "SELECT 1 FROM db.z AS z",
        )

        for sql, expected in load_sql_fixture_pairs("optimizer/qualify_tables.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    qualify_tables(parse_one(sql), db="db", catalog="c").sql(),
                    expected,
                )

    def test_qualify_columns(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
        }
        for sql, expected in load_sql_fixture_pairs("optimizer/qualify_columns.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    qualify_columns(parse_one(sql), schema=schema).sql(),
                    expected,
                )

    def test_qualify_columns__invalid(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
        }
        for sql in load_sql_fixtures("optimizer/qualify_columns__invalid.sql"):
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    qualify_columns(parse_one(sql), schema=schema)

    def test_quote_identities(self):
        for sql, expected in load_sql_fixture_pairs("optimizer/quote_identities.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    quote_identities(parse_one(sql)).sql(),
                    expected,
                )

    def test_projection_pushdown(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
        }
        for sql, expected in load_sql_fixture_pairs(
            "optimizer/projection_pushdown.sql"
        ):
            with self.subTest(sql):
                expression = parse_one(sql)
                expression = qualify_columns(expression, schema)
                expression = projection_pushdown(expression)
                self.assertEqual(
                    expression.sql(),
                    expected,
                )

    def test_simplify(self):
        for sql, expected in load_sql_fixture_pairs("optimizer/simplify.sql"):
            with self.subTest(sql):
                expression = parse_one(sql)
                expression = simplify(expression)
                self.assertEqual(
                    expression.sql(),
                    expected,
                )

    def test_decorrelate_subqueries(self):
        for sql, expected in load_sql_fixture_pairs(
            "optimizer/decorrelate_subqueries.sql"
        ):
            with self.subTest(sql):
                self.assertEqual(
                    decorrelate_subqueries(parse_one(sql)).sql(),
                    expected,
                )

    def test_expand_multi_table_selects(self):
        for sql, expected in load_sql_fixture_pairs(
            "optimizer/expand_multi_table_selects.sql"
        ):
            with self.subTest(sql):
                self.assertEqual(
                    expand_multi_table_selects(parse_one(sql)).sql(),
                    expected,
                )

    def test_tcph(self):
        schema = {
            "lineitem": {
                "l_orderkey": "uint64",
                "l_partkey": "uint64",
                "l_suppkey": "uint64",
                "l_linenumber": "uint64",
                "l_quantity": "float64",
                "l_extendedprice": "float64",
                "l_discount": "float64",
                "l_tax": "float64",
                "l_returnflag": "string",
                "l_linestatus": "string",
                "l_shipdate": "date32",
                "l_commitdate": "date32",
                "l_receiptdate": "date32",
                "l_shipinstruct": "string",
                "l_shipmode": "string",
                "l_comment": "string",
            },
            "orders": {
                "o_orderkey": "uint64",
                "o_custkey": "uint64",
                "o_orderstatus": "string",
                "o_totalprice": "float64",
                "o_orderdate": "date32",
                "o_orderpriority": "string",
                "o_clerk": "string",
                "o_shippriority": "int32",
                "o_comment": "string",
            },
            "customer": {
                "c_custkey": "uint64",
                "c_name": "string",
                "c_address": "string",
                "c_nationkey": "uint64",
                "c_phone": "string",
                "c_acctbal": "float64",
                "c_mktsegment": "string",
                "c_comment": "string",
            },
            "part": {
                "p_partkey": "uint64",
                "p_name": "string",
                "p_mfgr": "string",
                "p_brand": "string",
                "p_type": "string",
                "p_size": "int32",
                "p_container": "string",
                "p_retailprice": "float64",
                "p_comment": "string",
            },
            "supplier": {
                "s_suppkey": "uint64",
                "s_name": "string",
                "s_address": "string",
                "s_nationkey": "uint64",
                "s_phone": "string",
                "s_acctbal": "float64",
                "s_comment": "string",
            },
            "partsupp": {
                "ps_partkey": "uint64",
                "ps_suppkey": "uint64",
                "ps_availqty": "int32",
                "ps_supplycost": "float64",
                "ps_comment": "string",
            },
            "nation": {
                "n_nationkey": "uint64",
                "n_name": "string",
                "n_regionkey": "uint64",
                "n_comment": "string",
            },
            "region": {
                "r_regionkey": "uint64",
                "r_name": "string",
                "r_comment": "string",
            },
        }

        for sql, expected in load_sql_fixture_pairs("optimizer/tcp-h.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    optimize(parse_one(sql), schema=schema).sql(pretty=True),
                    expected,
                )
