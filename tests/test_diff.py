import unittest

from sqlglot import exp, parse_one
from sqlglot.diff import Insert, Move, Remove, Update, diff


def diff_delta_only(source, target, matchings=None, **kwargs):
    return diff(source, target, matchings=matchings, delta_only=True, **kwargs)


class TestDiff(unittest.TestCase):
    def test_simple(self):
        self._validate_delta_only(
            diff_delta_only(parse_one("SELECT a + b"), parse_one("SELECT a - b")),
            [
                Remove(expression=parse_one("a + b")),  # the Add node
                Insert(expression=parse_one("a - b")),  # the Sub node
                Move(source=parse_one("a"), target=parse_one("a")),  # the `a` Column node
                Move(source=parse_one("b"), target=parse_one("b")),  # the `b` Column node
            ],
        )

        self._validate_delta_only(
            diff_delta_only(parse_one("SELECT a, b, c"), parse_one("SELECT a, c")),
            [
                Remove(expression=parse_one("b")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff_delta_only(parse_one("SELECT a, b"), parse_one("SELECT a, b, c")),
            [
                Insert(expression=parse_one("c")),  # the Column node
            ],
        )

        self._validate_delta_only(
            diff_delta_only(
                parse_one("SELECT a FROM table_one"),
                parse_one("SELECT a FROM table_two"),
            ),
            [
                Update(
                    source=exp.to_table("table_one", quoted=False),
                    target=exp.to_table("table_two", quoted=False),
                ),  # the Table node
            ],
        )

    def test_lambda(self):
        self._validate_delta_only(
            diff_delta_only(
                parse_one("SELECT a, b, c, x(a -> a)"), parse_one("SELECT a, b, c, x(b -> b)")
            ),
            [
                Update(
                    source=exp.Lambda(
                        this=exp.to_identifier("a"), expressions=[exp.to_identifier("a")]
                    ),
                    target=exp.Lambda(
                        this=exp.to_identifier("b"), expressions=[exp.to_identifier("b")]
                    ),
                ),
            ],
        )

    def test_udf(self):
        self._validate_delta_only(
            diff_delta_only(
                parse_one('SELECT a, b, "my.udf1"()'), parse_one('SELECT a, b, "my.udf2"()')
            ),
            [
                Insert(expression=parse_one('"my.udf2"()')),
                Remove(expression=parse_one('"my.udf1"()')),
            ],
        )
        self._validate_delta_only(
            diff_delta_only(
                parse_one('SELECT a, b, "my.udf"(x, y, z)'),
                parse_one('SELECT a, b, "my.udf"(x, y, w)'),
            ),
            [
                Insert(expression=exp.column("w")),
                Remove(expression=exp.column("z")),
            ],
        )

    def test_node_position_changed(self):
        expr_src = parse_one("SELECT a, b, c")
        expr_tgt = parse_one("SELECT c, a, b")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Move(source=expr_src.selects[2], target=expr_tgt.selects[0]),
            ],
        )

        expr_src = parse_one("SELECT a + b")
        expr_tgt = parse_one("SELECT b + a")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Move(source=expr_src.selects[0].left, target=expr_tgt.selects[0].right),
            ],
        )

        expr_src = parse_one("SELECT aaaa AND bbbb")
        expr_tgt = parse_one("SELECT bbbb AND aaaa")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Move(source=expr_src.selects[0].left, target=expr_tgt.selects[0].right),
            ],
        )

        expr_src = parse_one("SELECT aaaa OR bbbb OR cccc")
        expr_tgt = parse_one("SELECT cccc OR bbbb OR aaaa")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Move(source=expr_src.selects[0].left.left, target=expr_tgt.selects[0].right),
                Move(source=expr_src.selects[0].right, target=expr_tgt.selects[0].left.left),
            ],
        )

        expr_src = parse_one("SELECT a, b FROM t WHERE CONCAT('a', 'b') = 'ab'")
        expr_tgt = parse_one("SELECT a FROM t WHERE CONCAT('a', 'b', b) = 'ab'")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Move(source=expr_src.selects[1], target=expr_tgt.find(exp.Concat).expressions[-1]),
            ],
        )

        expr_src = parse_one("SELECT a as a, b as b FROM t WHERE CONCAT('a', 'b') = 'ab'")
        expr_tgt = parse_one("SELECT a as a FROM t WHERE CONCAT('a', 'b', b) = 'ab'")

        b_alias = expr_src.selects[1]

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Remove(expression=b_alias),
                Move(source=b_alias.this, target=expr_tgt.find(exp.Concat).expressions[-1]),
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
            diff_delta_only(parse_one(expr_src), parse_one(expr_tgt)),
            [
                Remove(expression=parse_one("LOWER(c) AS c")),  # the Alias node
                Remove(expression=parse_one("LOWER(c)")),  # the Lower node
                Remove(expression=parse_one("'filter'")),  # the Literal node
                Insert(expression=parse_one("'different_filter'")),  # the Literal node
                Move(source=parse_one("c"), target=parse_one("c")),  # the new Column c
            ],
        )

    def test_join(self):
        expr_src = parse_one("SELECT a, b FROM t1 LEFT JOIN t2 ON t1.key = t2.key")
        expr_tgt = parse_one("SELECT a, b FROM t1 RIGHT JOIN t2 ON t1.key = t2.key")

        src_join = expr_src.find(exp.Join)
        tgt_join = expr_tgt.find(exp.Join)

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Remove(expression=src_join),
                Insert(expression=tgt_join),
                Move(source=exp.to_table("t2"), target=exp.to_table("t2")),
                Move(source=src_join.args["on"], target=tgt_join.args["on"]),
            ],
        )

    def test_window_functions(self):
        expr_src = parse_one("SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b)")
        expr_tgt = parse_one("SELECT RANK() OVER (PARTITION BY a ORDER BY b)")

        self._validate_delta_only(diff_delta_only(expr_src, expr_src), [])

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Remove(expression=parse_one("ROW_NUMBER()")),
                Insert(expression=parse_one("RANK()")),
                Update(source=expr_src.selects[0], target=expr_tgt.selects[0]),
            ],
        )

        expr_src = parse_one("SELECT MAX(x) OVER (ORDER BY y) FROM z", "oracle")
        expr_tgt = parse_one("SELECT MAX(x) KEEP (DENSE_RANK LAST ORDER BY y) FROM z", "oracle")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [Update(source=expr_src.selects[0], target=expr_tgt.selects[0])],
        )

    def test_pre_matchings(self):
        expr_src = parse_one("SELECT 1")
        expr_tgt = parse_one("SELECT 1, 2, 3, 4")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Remove(expression=expr_src),
                Insert(expression=expr_tgt),
                Insert(expression=exp.Literal.number(2)),
                Insert(expression=exp.Literal.number(3)),
                Insert(expression=exp.Literal.number(4)),
                Move(source=exp.Literal.number(1), target=exp.Literal.number(1)),
            ],
        )

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt, matchings=[(expr_src, expr_tgt)]),
            [
                Insert(expression=exp.Literal.number(2)),
                Insert(expression=exp.Literal.number(3)),
                Insert(expression=exp.Literal.number(4)),
            ],
        )

        self._validate_delta_only(
            diff_delta_only(
                expr_src, expr_tgt, matchings=[(expr_src, expr_tgt), (expr_src, expr_tgt)]
            ),
            [
                Insert(expression=exp.Literal.number(2)),
                Insert(expression=exp.Literal.number(3)),
                Insert(expression=exp.Literal.number(4)),
            ],
        )

        expr_tgt.selects[0].replace(expr_src.selects[0])

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt, matchings=[(expr_src, expr_tgt)]),
            [
                Insert(expression=exp.Literal.number(2)),
                Insert(expression=exp.Literal.number(3)),
                Insert(expression=exp.Literal.number(4)),
            ],
        )

    def test_identifier(self):
        expr_src = parse_one("SELECT a FROM tbl")
        expr_tgt = parse_one("SELECT a, tbl.b from tbl")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Insert(expression=exp.to_column("tbl.b")),
            ],
        )

        expr_src = parse_one("SELECT 1 AS c1, 2 AS c2")
        expr_tgt = parse_one("SELECT 2 AS c1, 3 AS c2")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Remove(expression=exp.alias_(1, "c1")),
                Remove(expression=exp.Literal.number(1)),
                Insert(expression=exp.alias_(3, "c2")),
                Insert(expression=exp.Literal.number(3)),
                Update(source=exp.alias_(2, "c2"), target=exp.alias_(2, "c1")),
            ],
        )

    def test_dialect_aware_diff(self):
        from sqlglot.generator import logger

        with self.assertLogs(logger) as cm:
            # We want to assert there are no warnings, but the 'assertLogs' method does not support that.
            # Therefore, we are adding a dummy warning, and then we will assert it is the only warning.
            logger.warning("Dummy warning")

            expression = parse_one("SELECT foo FROM bar FOR UPDATE", dialect="oracle")
            self._validate_delta_only(
                diff_delta_only(expression, expression.copy(), dialect="oracle"), []
            )

        self.assertEqual(["WARNING:sqlglot:Dummy warning"], cm.output)

    def test_non_expression_leaf_delta(self):
        expr_src = parse_one("SELECT a UNION SELECT b")
        expr_tgt = parse_one("SELECT a UNION ALL SELECT b")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Update(source=expr_src, target=expr_tgt),
            ],
        )

        expr_src = parse_one("SELECT a FROM t ORDER BY b ASC")
        expr_tgt = parse_one("SELECT a FROM t ORDER BY b DESC")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Update(
                    source=expr_src.find(exp.Order).expressions[0],
                    target=expr_tgt.find(exp.Order).expressions[0],
                ),
            ],
        )

        expr_src = parse_one("SELECT a, b FROM t ORDER BY c ASC")
        expr_tgt = parse_one("SELECT b, a FROM t ORDER BY c DESC")

        self._validate_delta_only(
            diff_delta_only(expr_src, expr_tgt),
            [
                Update(
                    source=expr_src.find(exp.Order).expressions[0],
                    target=expr_tgt.find(exp.Order).expressions[0],
                ),
                Move(source=expr_src.selects[0], target=expr_tgt.selects[1]),
            ],
        )

    def _validate_delta_only(self, actual_delta, expected_delta):
        self.assertEqual(set(actual_delta), set(expected_delta))
