# pylint: disable=no-member
import unittest

from sqlglot import parse_one
from sqlglot.rewriter import Rewriter


class TestRewriter(unittest.TestCase):
    def test_ctas(self):
        expression = parse_one("SELECT * FROM y")

        self.assertEqual(
            Rewriter(expression).ctas("x").expression.sql(),
            "CREATE TABLE x AS SELECT * FROM y",
        )

        self.assertEqual(
            Rewriter(expression)
            .ctas("x", db="foo", format="parquet", y="2")
            .expression.sql("hive"),
            "CREATE TABLE foo.x STORED AS PARQUET TBLPROPERTIES ('y' = '2') AS SELECT * FROM y",
        )

        self.assertEqual(expression.sql(), "SELECT * FROM y")

        rewriter = Rewriter(expression).ctas("x")
        self.assertEqual(rewriter.expression.sql(), "CREATE TABLE x AS SELECT * FROM y")

        with self.assertRaises(ValueError):
            rewriter.ctas("y").expression.sql()

    def test_add_selects(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x) y")

        self.assertEqual(
            Rewriter(expression)
            .add_selects(
                "a",
                "sum(b) as c",
            )
            .expression.sql("hive"),
            "SELECT *, a, SUM(b) AS c FROM (SELECT * FROM x) AS y",
        )
