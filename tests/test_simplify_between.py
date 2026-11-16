import sqlite3
import unittest
from contextlib import closing

from sqlglot import parse_one
from sqlglot.optimizer import optimize
from sqlglot.optimizer.simplify import simplify


class TestSimplifyBetween(unittest.TestCase):
    def test_symmetric_constant_bounds(self):
        for predicate, expected in (
            ("5 BETWEEN SYMMETRIC 10 AND 1", "TRUE"),
            ("5 NOT BETWEEN SYMMETRIC 10 AND 1", "FALSE"),
            ("5 BETWEEN SYMMETRIC 1 AND 10", "TRUE"),
            ("5 BETWEEN SYMMETRIC 1 AND 1", "FALSE"),
            ("1 BETWEEN SYMMETRIC 1 AND 1", "TRUE"),
            ("5 BETWEEN ASYMMETRIC 10 AND 1", "FALSE"),
            ("5 BETWEEN 10 AND 1", "FALSE"),
        ):
            with self.subTest(predicate=predicate):
                self.assertEqual(
                    simplify(parse_one(predicate, read="postgres"), dialect="postgres").sql(),
                    expected,
                )

    def test_symmetric_truth_values_and_grouping(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.executescript(
                "CREATE TABLE t(x INTEGER, lo INTEGER, hi INTEGER);"
                "INSERT INTO t VALUES (5,10,1),(5,1,10),(1,10,1),(10,10,1),"
                "(11,10,1),(0,10,1),(5,1,1),(NULL,10,1),(5,NULL,1),"
                "(5,10,NULL),(5,NULL,10),(5,NULL,NULL);"
            )
            predicate = "x BETWEEN SYMMETRIC lo AND hi"
            for projection in (
                predicate,
                f"NOT ({predicate})",
                f"({predicate}) AND x = 11",
                f"({predicate}) OR x = 11",
                f"({predicate}) IS NULL",
                f"({predicate}) = FALSE",
                f"CASE WHEN {predicate} THEN 1 ELSE 0 END",
                f"({predicate}) AND x BETWEEN lo AND hi",
                f"x BETWEEN lo AND hi AND ({predicate})",
                f"({predicate}) OR x BETWEEN lo AND hi",
                f"x BETWEEN lo AND hi OR ({predicate})",
            ):
                sql = f"SELECT {projection} FROM t"
                expression = parse_one(sql, read="postgres")
                expected = connection.execute(expression.sql("sqlite")).fetchall()
                for transformed in (
                    simplify(expression.copy(), dialect="postgres"),
                    optimize(
                        expression,
                        schema={"t": {"x": "INT", "lo": "INT", "hi": "INT"}},
                        dialect="postgres",
                    ),
                ):
                    with self.subTest(sql=sql):
                        self.assertEqual(
                            connection.execute(transformed.sql("sqlite")).fetchall(), expected
                        )
