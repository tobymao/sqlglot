import doctest
import os
import unittest

from sqlglot import optimizer
from sqlglot import parse_one
from sqlglot.errors import OptimizeError
from tests.helpers import load_sql_fixture_pairs, load_sql_fixtures


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests in the expressions module
    """
    from sqlglot.optimizer.rules import qualify_columns

    tests.addTests(doctest.DocTestSuite(qualify_columns))
    return tests


class TestOptimizer(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures/optimizer")

    def test_qualify_tables(self):
        self.assertEqual(
            optimizer.qualify_tables(parse_one("SELECT 1 FROM z"), db="db").sql(),
            "SELECT 1 FROM db.z",
        )
        self.assertEqual(
            optimizer.qualify_tables(
                parse_one("SELECT 1 FROM z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM c.db.z",
        )
        self.assertEqual(
            optimizer.qualify_tables(
                parse_one("SELECT 1 FROM y.z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM c.y.z",
        )
        self.assertEqual(
            optimizer.qualify_tables(
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
                    optimizer.qualify_columns(parse_one(sql), schema=schema).sql(),
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
                    optimizer.qualify_columns(parse_one(sql), schema=schema)
