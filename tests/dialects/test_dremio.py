from tests.dialects.test_dialect import Validator
from sqlglot import parse_one, exp, UnsupportedError, ErrorLevel, transpile, ParseError
from sqlglot.optimizer.annotate_types import annotate_types


class TestDremio(Validator):
    dialect = "dremio"
    maxDiff = None

    def test_type_mappings(self):
        self.validate_identity("CAST(x AS SMALLINT)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS TINYINT)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS BINARY)", "CAST(x AS VARBINARY)")
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS NCHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS CHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TIMESTAMPNTZ)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS ARRAY)", "CAST(x AS LIST)")
        self.validate_identity("CAST(x AS BIT)", "CAST(x AS BOOLEAN)")

        # unsupported types
        with self.assertRaises(UnsupportedError):
            transpile(
                "CAST(x AS TIMESTAMPTZ)",
                read="oracle",
                write="dremio",
                unsupported_level=ErrorLevel.IMMEDIATE,
            )
        with self.assertRaises(UnsupportedError):
            transpile(
                "CAST(x AS TIMESTAMPLTZ)",
                read="oracle",
                write="dremio",
                unsupported_level=ErrorLevel.IMMEDIATE,
            )

    def test_concat_coalesce(self):
        self.validate_all(
            "SELECT CONCAT('a', NULL)",
            write={
                "dremio": "SELECT CONCAT('a', NULL)",
                "": "SELECT CONCAT(COALESCE('a', ''), COALESCE(NULL, ''))",
            },
        )

    def test_typed_division(self):
        def _div_result_type(sql: str, dialect: str):
            tree = parse_one(sql, read=dialect)
            annotate_types(tree, dialect=dialect)
            return tree.find(exp.Div).type.this

        assert _div_result_type("SELECT 5 / 2", "dremio") == exp.DataType.Type.BIGINT
        assert _div_result_type("SELECT 5 / 2", "oracle") == exp.DataType.Type.DOUBLE

    def test_user_defined_types_unsupported(self):
        with self.assertRaises(ParseError):
            self.parse_one("CAST(x AS MY_CUSTOM_TYPE)")

    def test_null_ordering(self):
        # NULLS LAST is the default, so generator can drop the clause
        self.validate_identity(
            "SELECT * FROM t ORDER BY a NULLS LAST", "SELECT * FROM t ORDER BY a"
        )
        self.validate_identity(
            "SELECT * FROM t ORDER BY a DESC NULLS LAST", "SELECT * FROM t ORDER BY a DESC"
        )

        # If the clause is not the default, it must be kept
        self.validate_identity(
            "SELECT * FROM t ORDER BY a NULLS FIRST",
        )
        self.validate_identity(
            "SELECT * FROM t ORDER BY a DESC NULLS FIRST",
        )

    def test_convert_timezone(self):
        self.validate_all(
            "SELECT CONVERT_TIMEZONE('America/Chicago', DateColumn)",
            write={
                "dremio": "SELECT CONVERT_TIMEZONE('America/Chicago', DateColumn)",
                "": "SELECT DateColumn AT TIME ZONE 'America/Chicago'",
            },
        )

    def test_interval_plural(self):
        self.validate_identity("INTERVAL '7' DAYS", "INTERVAL '7' DAY")

    def test_limit_only_literals(self):
        self.validate_identity("SELECT * FROM t LIMIT 1 + 1", "SELECT * FROM t LIMIT 2")

    def test_multi_arg_distinct_unsupported(self):
        self.validate_identity(
            "SELECT COUNT(DISTINCT a, b) FROM t",
            "SELECT COUNT(DISTINCT CASE WHEN a IS NULL THEN NULL WHEN b IS NULL THEN NULL ELSE (a, b) END) FROM t",
        )

    def test_time_mapping(self):
        ts = "CAST('2025-06-24 12:34:56' AS TIMESTAMP)"

        self.validate_all(
            f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
            read={
                "dremio": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "duckdb": f"SELECT STRFTIME({ts}, '%Y-%m-%d %H:%M:%S')",
            },
            write={
                "dremio": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "duckdb": f"SELECT STRFTIME({ts}, '%Y-%m-%d %H:%M:%S')",
            },
        )

        self.validate_all(
            f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FFF TZD')",
            read={
                "dremio": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FFF TZD')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.US TZ')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')",
                "duckdb": f"SELECT STRFTIME({ts}, '%y-%j %H:%M:%S.%f %Z')",
            },
            write={
                "dremio": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FFF TZD')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.US TZ')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')",
                "duckdb": f"SELECT STRFTIME({ts}, '%y-%j %H:%M:%S.%f %Z')",
            },
        )

    def test_time_diff(self):
        self.validate_identity("SELECT DATE_ADD(col, 1)")
        self.validate_identity("SELECT DATE_ADD(col, CAST(1 AS INTERVAL HOUR))")
        self.validate_identity(
            "SELECT DATE_ADD(TIMESTAMP '2022-01-01 12:00:00', CAST(-1 AS INTERVAL HOUR))",
            "SELECT DATE_ADD(CAST('2022-01-01 12:00:00' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))",
        )
        self.validate_identity(
            "SELECT DATE_ADD(col, 2, 'HOUR')", "SELECT TIMESTAMPADD(HOUR, 2, col)"
        )

        self.validate_identity("SELECT DATE_SUB(col, 1)")
        self.validate_identity("SELECT DATE_SUB(col, CAST(1 AS INTERVAL HOUR))")
        self.validate_identity(
            "SELECT DATE_SUB(TIMESTAMP '2022-01-01 12:00:00', CAST(-1 AS INTERVAL HOUR))",
            "SELECT DATE_SUB(CAST('2022-01-01 12:00:00' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))",
        )
        self.validate_identity(
            "SELECT DATE_SUB(col, 2, 'HOUR')", "SELECT TIMESTAMPADD(HOUR, -2, col)"
        )

        self.validate_identity("SELECT DATE_ADD(col, 2, 'DAY')", "SELECT DATE_ADD(col, 2)")

        self.validate_identity(
            "SELECT DATE_SUB(col, a, 'HOUR')", "SELECT TIMESTAMPADD(HOUR, a * -1, col)"
        )
