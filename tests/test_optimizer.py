import unittest

from sqlglot import optimizer
from sqlglot import parse_one


class TestParser(unittest.TestCase):
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
