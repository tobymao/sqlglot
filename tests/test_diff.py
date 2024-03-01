import unittest

from sqlglot import exp, parse_one
from sqlglot.diff import Insert, Keep, Move, Remove, Update, diff
from sqlglot.expressions import Join, to_table


class TestDiff(unittest.TestCase):
    def test_simple(self):
        self._validate_delta_only(
            diff(parse_one("SELECT a + b"), parse_one("SELECT a - b")),
            [
                Remove(parse_one("a + b")),  # the Add node
                Insert(parse_one("a - b")),  # the Sub node
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a, b, c"), parse_one("SELECT a, c")),
            [
                Remove(parse_one("b")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a, b"), parse_one("SELECT a, b, c")),
            [
                Insert(parse_one("c")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT a FROM table_one"),
                parse_one("SELECT a FROM table_two"),
            ),
            [
                Update(
                    to_table("table_one", quoted=False),
                    to_table("table_two", quoted=False),
                ),  # the Table node
            ],
        )

    def test_lambda(self):
        self._validate_delta_only(
            diff(parse_one("SELECT a, b, c, x(a -> a)"), parse_one("SELECT a, b, c, x(b -> b)")),
            [
                Update(
                    exp.Lambda(this=exp.to_identifier("a"), expressions=[exp.to_identifier("a")]),
                    exp.Lambda(this=exp.to_identifier("b"), expressions=[exp.to_identifier("b")]),
                ),
            ],
        )

    def test_udf(self):
        self._validate_delta_only(
            diff(parse_one('SELECT a, b, "my.udf1"()'), parse_one('SELECT a, b, "my.udf2"()')),
            [
                Insert(parse_one('"my.udf2"()')),
                Remove(parse_one('"my.udf1"()')),
            ],
        )
        self._validate_delta_only(
            diff(
                parse_one('SELECT a, b, "my.udf"(x, y, z)'),
                parse_one('SELECT a, b, "my.udf"(x, y, w)'),
            ),
            [
                Insert(exp.column("w")),
                Remove(exp.column("z")),
            ],
        )

    def test_node_position_changed(self):
        self._validate_delta_only(
            diff(parse_one("SELECT a, b, c"), parse_one("SELECT c, a, b")),
            [
                Move(parse_one("c")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT a + b"), parse_one("SELECT b + a")),
            [
                Move(parse_one("a")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff(parse_one("SELECT aaaa AND bbbb"), parse_one("SELECT bbbb AND aaaa")),
            [
                Move(parse_one("aaaa")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff(
                parse_one("SELECT aaaa OR bbbb OR cccc"),
                parse_one("SELECT cccc OR bbbb OR aaaa"),
            ),
            [
                Move(parse_one("aaaa")),  # the Column node
                Move(parse_one("cccc")),  # the Column node
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
                cte1 AS (SELECT a, b, c FROM table_one WHERE d = 'different_filter'),
                cte2 AS (SELECT d, e, f FROM table_two)
            SELECT a, b, d, e FROM cte1 JOIN cte2 ON f = c
        """

        self._validate_delta_only(
            diff(parse_one(expr_src), parse_one(expr_tgt)),
            [
                Remove(parse_one("LOWER(c) AS c")),  # the Alias node
                Remove(parse_one("LOWER(c)")),  # the Lower node
                Remove(parse_one("'filter'")),  # the Literal node
                Insert(parse_one("'different_filter'")),  # the Literal node
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

    def test_window_functions(self):
        expr_src = parse_one("SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b)")
        expr_tgt = parse_one("SELECT RANK() OVER (PARTITION BY a ORDER BY b)")

        self._validate_delta_only(diff(expr_src, expr_src), [])

        self._validate_delta_only(
            diff(expr_src, expr_tgt),
            [
                Remove(parse_one("ROW_NUMBER()")),  # the Anonymous node
                Insert(parse_one("RANK()")),  # the Anonymous node
            ],
        )

    def test_pre_matchings(self):
        expr_src = parse_one("SELECT 1")
        expr_tgt = parse_one("SELECT 1, 2, 3, 4")

        self._validate_delta_only(
            diff(expr_src, expr_tgt),
            [
                Remove(expr_src),
                Insert(expr_tgt),
                Insert(exp.Literal.number(2)),
                Insert(exp.Literal.number(3)),
                Insert(exp.Literal.number(4)),
            ],
        )

        self._validate_delta_only(
            diff(expr_src, expr_tgt, matchings=[(expr_src, expr_tgt)]),
            [
                Insert(exp.Literal.number(2)),
                Insert(exp.Literal.number(3)),
                Insert(exp.Literal.number(4)),
            ],
        )

        with self.assertRaises(ValueError):
            diff(expr_src, expr_tgt, matchings=[(expr_src, expr_tgt), (expr_src, expr_tgt)])

    def test_identifier(self):
        expr_src = parse_one("SELECT a FROM tbl")
        expr_tgt = parse_one("SELECT a, tbl.b from tbl")

        self._validate_delta_only(
            diff(expr_src, expr_tgt),
            [
                Insert(expression=exp.to_column("tbl.b")),
            ],
        )

    def _validate_delta_only(self, actual_diff, expected_delta):
        actual_delta = _delta_only(actual_diff)
        self.assertEqual(set(actual_delta), set(expected_delta))


def _delta_only(changes):
    return [d for d in changes if not isinstance(d, Keep)]
