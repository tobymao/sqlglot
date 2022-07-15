import unittest

from sqlglot import parse_one, table
from sqlglot.diff import diff, Insert, Remove, Keep
from sqlglot.expressions import Join


class TestDiff(unittest.TestCase):
    def test_simple(self):
        self._validate_delta_only(
            diff(parse_one("SELECT a + b"), parse_one("SELECT a - b")),
            [
                Remove(parse_one("a + b")),
                Insert(parse_one("a - b")),
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a, b, c"), parse_one("SELECT a, c")),
            [
                Remove(parse_one("b")),
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a, b"), parse_one("SELECT a, b, c")),
            [
                Insert(parse_one("c")),
            ],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM table_one"),
                parse_one("SELECT a FROM table_two"),
            ),
            [
                Remove(table("table_one")),
                Insert(table("table_two")),
            ],
        )

    def test_cte(self):
        expr_src = """
            WITH
                cte1 AS (SELECT a, b, LOWER(c) AS c FROM table_one WHERE d = 'filter'),
                cte2 AS (SELECT d, e, f FROM table_two)
            SELECT a, b, d, e FROM cte1 JOIN cte2 ON f = c
        """
        expr_tgt = """
            WITH
                cte2 AS (SELECT d, e, f FROM table_two),
                cte1 AS (SELECT a, b, c FROM table_one WHERE d = 'different_filter')
            SELECT a, b, d, e FROM cte1 JOIN cte2 ON f = c
        """

        self._validate_delta_only(
            diff(parse_one(expr_src), parse_one(expr_tgt)),
            [
                Remove(parse_one("LOWER(c) AS c")),
                Insert(parse_one("c")),
                Remove(parse_one("LOWER(c)")),
                Remove(parse_one("c")),
                Remove(parse_one("'filter'")),
                Insert(parse_one("'different_filter'")),
            ],
        )

    def test_join(self):
        expr_src = "SELECT a, b FROM t1 LEFT JOIN t2 ON t1.key = t2.key"
        expr_tgt = "SELECT a, b FROM t1 RIGHT JOIN t2 ON t1.key = t2.key"

        changes = diff(parse_one(expr_src), parse_one(expr_tgt))
        changes = _delta_only(changes)

        self.assertEqual(len(changes), 2)
        self.assertTrue(isinstance(changes[0], Remove))
        self.assertTrue(isinstance(changes[1], Insert))
        self.assertTrue(all(isinstance(c.expression, Join) for c in changes))

    def test_no_delta(self):
        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM t WHERE b IN (1, 2, 3)"),
                parse_one("SELECT a FROM t WHERE b IN (3, 2, 1)"),
            ),
            [],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM t WHERE b AND c AND d"),
                parse_one("SELECT a FROM t WHERE d AND b AND c"),
            ),
            [],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM t WHERE b OR c OR d"),
                parse_one("SELECT a FROM t WHERE d OR b OR c"),
            ),
            [],
        )

    def _validate_delta_only(self, actual_diff, expected_delta):
        actual_delta = _delta_only(actual_diff)
        self.assertEqual(actual_delta, expected_delta)


def _delta_only(changes):
    return [d for d in changes if not isinstance(d, Keep)]
