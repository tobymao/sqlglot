import unittest

from sqlglot import parse_one
from sqlglot.diff import diff, Insert, Remove, Keep
from sqlglot.expressions import Identifier, Table


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
            diff(parse_one("SELECT a, b, c"), parse_one("SELECT c, a")),
            [
                Remove(parse_one("b")),
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a, b"), parse_one("SELECT c, b, a")),
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
                Remove(
                    Table(
                        this=Identifier(this="table_one", quoted=False),
                        db=None,
                        catalog=None,
                    )
                ),
                Insert(
                    Table(
                        this=Identifier(this="table_two", quoted=False),
                        db=None,
                        catalog=None,
                    )
                ),
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

    def test_no_delta(self):
        self._validate_delta_only(
            diff(parse_one("SELECT a, b, c"), parse_one("SELECT c, b, a")), []
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM t WHERE b IN (1, 2, 3)"),
                parse_one("SELECT a FROM t WHERE b IN (3, 2, 1)"),
            ),
            [],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM t WHERE b AND c"),
                parse_one("SELECT a FROM t WHERE c AND b"),
            ),
            [],
        )

    def _validate_delta_only(self, actual_diff, expected_delta):
        actual_delta = [d for d in actual_diff if not isinstance(d, Keep)]
        self.assertEqual(actual_delta, expected_delta)
