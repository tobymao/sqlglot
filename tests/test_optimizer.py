import doctest
import os
import unittest

from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot import parse_one
from sqlglot.errors import OptimizeError
from tests.helpers import load_sql_fixture_pairs, load_sql_fixtures


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests in the expressions module
    """
    from sqlglot.optimizer import qualify_columns as mod

    tests.addTests(doctest.DocTestSuite(mod))
    return tests


class TestOptimizer(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures/optimizer")

    def test_qualify_tables(self):
        self.assertEqual(
            qualify_tables(parse_one("SELECT 1 FROM z"), db="db").sql(),
            "SELECT 1 FROM db.z",
        )
        self.assertEqual(
            qualify_tables(parse_one("SELECT 1 FROM z"), db="db", catalog="c").sql(),
            "SELECT 1 FROM c.db.z",
        )
        self.assertEqual(
            qualify_tables(parse_one("SELECT 1 FROM y.z"), db="db", catalog="c").sql(),
            "SELECT 1 FROM c.y.z",
        )
        self.assertEqual(
            qualify_tables(
                parse_one("SELECT 1 FROM x.y.z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM x.y.z",
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
