from sqlglot import ErrorLevel, ParseError, UnsupportedError, exp, parse_one
from tests.dialects.test_dialect import Validator


class TestVertica(Validator):
    dialect = "vertica"

    def test_select_baseline(self):
        self.validate_identity("SELECT a FROM tbl")
        self.validate_identity("WITH cte AS (SELECT 1 AS a) SELECT a FROM cte")
        self.validate_identity("SELECT * FROM tbl ORDER BY a LIMIT 10 OFFSET 5")
        self.validate_identity("SELECT * FROM tbl FOR UPDATE")

    def test_minus(self):
        self.validate_all(
            "SELECT 1 EXCEPT SELECT 2",
            read={"vertica": "SELECT 1 MINUS SELECT 2"},
            write={"vertica": "SELECT 1 EXCEPT SELECT 2"},
        )

    def test_at_epoch_query(self):
        self.validate_identity("AT EPOCH LATEST SELECT 1")
        self.validate_identity("AT EPOCH 42 SELECT 1")
        self.validate_identity("AT TIME '2024-01-01 00:00:00' SELECT 1")
        self.validate_identity("AT EPOCH LATEST WITH cte AS (SELECT 1) SELECT * FROM cte")
        self.validate_identity("AT TIME '2024-01-01 00:00:00' WITH cte AS (SELECT 1) SELECT * FROM cte")
        self.validate_identity("AT EPOCH LATEST SELECT 1 UNION SELECT 2")
        self.validate_identity("AT TIME '2024-01-01 00:00:00' SELECT 1 UNION SELECT 2")
        self.validate_identity("SELECT * FROM (AT TIME '2024-01-01 00:00:00' SELECT 1 AS a) AS t")

    def test_at_epoch_table(self):
        self.validate_identity("SELECT * FROM x AT EPOCH LATEST")
        self.validate_identity("SELECT * FROM x AT EPOCH 42")
        self.validate_identity("SELECT * FROM x AT TIME '2024-01-01 00:00:00'")

    def test_limit_over(self):
        self.validate_identity(
            "SELECT store_region, store_name FROM store.store_dimension LIMIT 2 OVER (PARTITION BY store_region ORDER BY number_of_employees)"
        )
        self.validate_identity(
            "SELECT a, b FROM t LIMIT 3 OVER (PARTITION BY a ORDER BY b DESC NULLS LAST)"
        )
        self.validate_identity("SELECT * FROM t LIMIT ALL")

    def test_limit_all_over_not_supported(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT * FROM t LIMIT ALL OVER (ORDER BY x)")

        expression = exp.select("*").from_("t")
        expression.set(
            "limit",
            exp.Limit(
                expression=exp.column("ALL"),
                order=exp.Order(expressions=[exp.Ordered(this=exp.column("x"))]),
            ),
        )

        with self.assertRaises(UnsupportedError):
            expression.sql(dialect="vertica", unsupported_level=ErrorLevel.RAISE)

    def test_match_clause(self):
        self.validate_identity(
            "SELECT * FROM clicks MATCH (PARTITION BY user_id ORDER BY ts DEFINE A AS action = 'A', B AS action = 'B' PATTERN P AS (A B) ROWS MATCH FIRST EVENT)"
        )
        self.validate_identity(
            "SELECT * FROM clicks MATCH (ORDER BY ts DEFINE A AS action = 'A' PATTERN P AS (A+) ROWS MATCH ALL EVENTS)"
        )

    def test_timeseries_clause(self):
        self.validate_identity(
            "SELECT slice_time, symbol, TS_FIRST_VALUE(bid) FROM Tickstore TIMESERIES slice_time AS '5 seconds' OVER (PARTITION BY symbol ORDER BY ts)"
        )
        self.validate_identity(
            "SELECT slice_time, TS_FIRST_VALUE(bid) FROM Tickstore TIMESERIES slice_time AS '1 second' OVER (ORDER BY ts) ORDER BY slice_time"
        )

    def test_set_operation_precedence(self):
        expression = parse_one(
            "SELECT 1 AS x UNION SELECT 2 AS x INTERSECT SELECT 3 AS x", read="vertica"
        )
        self.assertIsInstance(expression, exp.Union)
        self.assertIsInstance(expression.args["expression"], exp.Intersect)

        expression = parse_one(
            "SELECT 1 AS x EXCEPT SELECT 2 AS x INTERSECT SELECT 3 AS x", read="vertica"
        )
        self.assertIsInstance(expression, exp.Except)
        self.assertIsInstance(expression.args["expression"], exp.Intersect)

        expression = parse_one(
            "(SELECT 1 AS x UNION SELECT 2 AS x) INTERSECT SELECT 3 AS x", read="vertica"
        )
        self.assertIsInstance(expression, exp.Intersect)
        self.assertIsInstance(expression.args["this"], exp.Subquery)

        expression = parse_one(
            "SELECT 1 AS x EXCEPT SELECT 2 AS x EXCEPT SELECT 3 AS x", read="vertica"
        )
        self.assertIsInstance(expression, exp.Except)
        self.assertIsInstance(expression.args["this"], exp.Except)

        expression = parse_one(
            "SELECT 1 AS x UNION SELECT 2 AS x INTERSECT SELECT 3 AS x LIMIT 1",
            read="vertica",
        )
        self.assertIsInstance(expression, exp.Union)
        self.assertIsInstance(expression.args.get("limit"), exp.Limit)
        self.assertIsNone(expression.args["expression"].args.get("limit"))

        expression = parse_one("SELECT 1 MINUS SELECT 2 INTERSECT SELECT 3", read="vertica")
        self.assertIsInstance(expression, exp.Except)
        self.assertIsInstance(expression.args["expression"], exp.Intersect)

    def test_set_operation_all_not_supported(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT 1 INTERSECT ALL SELECT 1")

        with self.assertRaises(ParseError):
            self.parse_one("SELECT 1 EXCEPT ALL SELECT 1")

        with self.assertRaises(UnsupportedError):
            parse_one("SELECT 1 INTERSECT ALL SELECT 1").sql(
                dialect="vertica", unsupported_level=ErrorLevel.RAISE
            )

        with self.assertRaises(UnsupportedError):
            parse_one("SELECT 1 EXCEPT ALL SELECT 1").sql(
                dialect="vertica", unsupported_level=ErrorLevel.RAISE
            )

    def test_hints(self):
        self.validate_all(
            "WITH /*+ ENABLE_WITH_CLAUSE_MATERIALIZATION */ cte AS (SELECT 1) SELECT * FROM cte",
            read={"vertica": "WITH /*+ENABLE_WITH_CLAUSE_MATERIALIZATION*/ cte AS (SELECT 1) SELECT * FROM cte"},
            write={"vertica": "WITH /*+ ENABLE_WITH_CLAUSE_MATERIALIZATION */ cte AS (SELECT 1) SELECT * FROM cte"},
        )
        self.validate_all(
            "SELECT a, SUM(b) FROM t GROUP BY /*+ GBYTYPE(HASH) */ a",
            read={"vertica": "SELECT a, SUM(b) FROM t GROUP BY /*+GBYTYPE(HASH)*/ a"},
            write={"vertica": "SELECT a, SUM(b) FROM t GROUP BY /*+ GBYTYPE(HASH) */ a"},
        )
        self.validate_all(
            "WITH /*+ ENABLE_WITH_CLAUSE_MATERIALIZATION */ RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
            read={
                "vertica": "WITH /*+ENABLE_WITH_CLAUSE_MATERIALIZATION*/ RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t"
            },
            write={
                "vertica": "WITH /*+ ENABLE_WITH_CLAUSE_MATERIALIZATION */ RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t"
            },
        )
        self.validate_all(
            "SELECT a, SUM(b) FROM t GROUP BY /*+ GBYTYPE(PIPELINED) */ a",
            read={"vertica": "SELECT a, SUM(b) FROM t GROUP BY /*+GBYTYPE(PIPELINED)*/ a"},
            write={"vertica": "SELECT a, SUM(b) FROM t GROUP BY /*+ GBYTYPE(PIPELINED) */ a"},
        )
