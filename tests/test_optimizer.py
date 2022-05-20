import doctest
import os
import unittest

from sqlglot.optimizer import optimize
from sqlglot import parse_one


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests in the expressions module
    """
    from sqlglot.optimizer import qualify_columns

    tests.addTests(doctest.DocTestSuite(qualify_columns))
    return tests


class TestOptimizer(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, "fixtures/optimizer")

    def test_qualify_tables(self):
        self.assertEqual(
            optimize.qualify_tables(parse_one("SELECT 1 FROM z"), db="db").sql(),
            "SELECT 1 FROM db.z",
        )
        self.assertEqual(
            optimize.qualify_tables(
                parse_one("SELECT 1 FROM z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM c.db.z",
        )
        self.assertEqual(
            optimize.qualify_tables(
                parse_one("SELECT 1 FROM y.z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM c.y.z",
        )
        self.assertEqual(
            optimize.qualify_tables(
                parse_one("SELECT 1 FROM x.y.z"), db="db", catalog="c"
            ).sql(),
            "SELECT 1 FROM x.y.z",
        )

    def test_qualify_columns(self):
        schema = {
            "x": {"a": "INT", "b": "INT"},
            "y": {"b": "INT", "c": "INT"},
        }
        with open(
            os.path.join(self.fixtures_dir, "qualify_columns.sql"), encoding="utf-8"
        ) as f:
            lines = [
                line for line in f.readlines() if line and not line.startswith("--")
            ]
            statements = "\n".join(lines).split(";")

            size = len(statements)

            for i in range(0, size, 2):
                if i + 1 < size:
                    sql = statements[i].strip()
                    expected = statements[i + 1].strip()
                    with self.subTest(sql):
                        self.assertEqual(
                            optimize.qualify_columns(
                                parse_one(sql), schema=schema
                            ).sql(),
                            expected,
                        )

    def test_qualify_columns__invalid(self):
        pass
