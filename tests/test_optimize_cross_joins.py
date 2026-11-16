import sqlite3
import unittest

from sqlglot import exp
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.qualify import qualify


class TestOptimizeCrossJoins(unittest.TestCase):
    def test_conditional_cross_joins(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.executescript(
            "CREATE TABLE x(a INTEGER); INSERT INTO x VALUES (1),(2),(NULL);"
            "CREATE TABLE y(a INTEGER); INSERT INTO y VALUES (1),(3),(NULL);"
            "CREATE TABLE z(a INTEGER); INSERT INTO z VALUES (2),(3),(NULL);"
        )
        schema = {name: {"a": "INT"} for name in ("x", "y", "z")}
        for criteria in (
            "ON x.a = y.a",
            "ON x.a < y.a",
            "ON y.a = 3",
            "ON x.a = y.a OR y.a IS NULL",
            "ON FALSE",
            "ON NULL",
            "ON TRUE",
            "USING(a)",
        ):
            queries = [
                f"SELECT x.a, y.a FROM x CROSS JOIN y {criteria}",
                f"SELECT x.a, y.a, z.a FROM x CROSS JOIN y {criteria} LEFT JOIN z ON y.a = z.a",
            ]
            # RIGHT JOIN was added in SQLite 3.39; older CI runtimes still cover INNER/LEFT joins.
            if sqlite3.sqlite_version_info >= (3, 39, 0):
                queries.append(
                    f"SELECT x.a, y.a, z.a FROM x CROSS JOIN y {criteria} RIGHT JOIN z ON y.a = z.a"
                )
            for sql in queries:
                expected = connection.execute(sql).fetchall()
                for dialect in ("sqlite", "mysql"):
                    for rules in ((qualify, optimize_joins), RULES):
                        with self.subTest(sql=sql, dialect=dialect, rules=rules):
                            optimized = optimize(sql, schema=schema, dialect=dialect, rules=rules)
                            self.assertCountEqual(
                                connection.execute(optimized.sql("sqlite")).fetchall(), expected
                            )

    def test_unconditional_cross_join_is_preserved(self):
        expression = optimize(
            "SELECT x.a, y.a FROM x CROSS JOIN y", schema={"x": {"a": "INT"}, "y": {"a": "INT"}}
        )
        self.assertEqual(expression.find(exp.Join).kind, "CROSS")
