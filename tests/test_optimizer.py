import doctest
import inspect
import os
import unittest

import sqlglot
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


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests in the expressions module
    """
    for _, module in inspect.getmembers(sqlglot.optimizer, inspect.ismodule):
        tests.addTests(doctest.DocTestSuite(module))
    return tests


class TestOptimizer(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures/optimizer")

    def test_optimize(self):
        schema = {
            "x": {"a": "INT"},
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
