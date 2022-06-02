import unittest

import sqlglot
import sqlglot.optimizer
from sqlglot.optimizer import optimize
from sqlglot.optimizer.projection_pushdown import projection_pushdown
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.schema import ensure_schema, MappingSchema
from sqlglot import parse_one
from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError
from tests.helpers import load_sql_fixture_pairs, load_sql_fixtures


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
        }

    def check_file(self, file, func=None, pretty=False, **kwargs):
        module = getattr(sqlglot.optimizer, file)
        func = getattr(module, func or file)

        for sql, expected in load_sql_fixture_pairs(f"optimizer/{file}.sql"):
            with self.subTest(sql):
                self.assertEqual(
                    func(parse_one(sql), **kwargs).sql(pretty=pretty),
                    expected,
                )

    def test_optimize(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"a": "INT", "b": "INT"},
            "z": {"a": "INT", "c": "INT"},
        }

        self.check_file("optimizer", func="optimize", pretty=True, schema=schema)

    def test_qualify_tables(self):
        self.check_file("qualify_tables", db="db", catalog="c")

    def test_conjunctive_normal_form(self):
        self.check_file("conjunctive_normal_form")

    def test_qualify_columns(self):
        self.check_file("qualify_columns", schema=self.schema)

    def test_qualify_columns__invalid(self):
        for sql in load_sql_fixtures("optimizer/qualify_columns__invalid.sql"):
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    qualify_columns(parse_one(sql), schema=self.schema)

    def test_quote_identities(self):
        self.check_file("quote_identities")

    def test_projection_pushdown(self):
        for sql, expected in load_sql_fixture_pairs(
            "optimizer/projection_pushdown.sql"
        ):
            with self.subTest(sql):
                expression = parse_one(sql)
                expression = qualify_columns(expression, self.schema)
                expression = projection_pushdown(expression)
                self.assertEqual(
                    expression.sql(),
                    expected,
                )

    def test_simplify(self):
        self.check_file("simplify")

    def test_decorrelate_subqueries(self):
        self.check_file("decorrelate_subqueries")

    def test_predicate_pushdown(self):
        self.check_file("predicate_pushdown")

    def test_expand_multi_table_selects(self):
        self.check_file("expand_multi_table_selects")

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

    def test_schema(self):
        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                }
            }
        )
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2"))

        schema = ensure_schema(
            {
                "db": {
                    "x": {
                        "a": "uint64",
                    }
                }
            }
        )
        self.assertEqual(schema.column_names(exp.Table(this="x", db="db")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2", db="db"))

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
        self.assertEqual(
            schema.column_names(exp.Table(this="x", db="db", catalog="c")), ["a"]
        )
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db", catalog="c2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(exp.Table(this="x2", db="db"))

        schema = ensure_schema(
            MappingSchema(
                {
                    "x": {
                        "a": "uint64",
                    }
                }
            )
        )
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["a"])

        with self.assertRaises(OptimizeError):
            ensure_schema({})
