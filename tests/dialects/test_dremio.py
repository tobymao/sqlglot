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
                "": "SELECT CONCAT('a', COALESCE(NULL, ''))",
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
            f"SELECT TO_CHAR({ts}, 'yyyy-mm-dd hh24:mi:ss')",
            read={
                "dremio": f"SELECT TO_CHAR({ts}, 'yyyy-mm-dd hh24:mi:ss')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "duckdb": f"SELECT STRFTIME({ts}, '%Y-%m-%d %H:%M:%S')",
            },
            write={
                "dremio": f"SELECT TO_CHAR({ts}, 'yyyy-mm-dd hh24:mi:ss')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YYYY-MM-DD HH24:MI:SS')",
                "duckdb": f"SELECT STRFTIME({ts}, '%Y-%m-%d %H:%M:%S')",
            },
        )

        self.validate_all(
            f"SELECT TO_CHAR({ts}, 'yy-ddd hh24:mi:ss.fff tzd')",
            read={
                "dremio": f"SELECT TO_CHAR({ts}, 'yy-ddd hh24:mi:ss.fff tzd')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.US TZ')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')",
                "duckdb": f"SELECT STRFTIME({ts}, '%y-%j %H:%M:%S.%f %Z')",
            },
            write={
                "dremio": f"SELECT TO_CHAR({ts}, 'yy-ddd hh24:mi:ss.fff tzd')",
                "postgres": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.US TZ')",
                "oracle": f"SELECT TO_CHAR({ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')",
                "duckdb": f"SELECT STRFTIME({ts}, '%y-%j %H:%M:%S.%f %Z')",
            },
        )

    def test_to_char_special(self):
        # Numeric formats should have is_numeric=True
        to_char = self.validate_identity("TO_CHAR(5555, '#')").assert_is(exp.ToChar)
        assert to_char.args["is_numeric"] is True

        to_char = self.validate_identity("TO_CHAR(3.14, '#.#')").assert_is(exp.ToChar)
        assert to_char.args["is_numeric"] is True

        to_char = self.validate_identity("TO_CHAR(columnname, '#.##')").assert_is(exp.ToChar)
        assert to_char.args["is_numeric"] is True

        # Non-numeric formats or columns should have is_numeric=None or False
        to_char = self.validate_identity("TO_CHAR(5555)").assert_is(exp.ToChar)
        assert not to_char.args.get("is_numeric")

        to_char = self.validate_identity("TO_CHAR(3.14, columnname)").assert_is(exp.ToChar)
        assert not to_char.args.get("is_numeric")

        to_char = self.validate_identity("TO_CHAR(123, 'abcd')").assert_is(exp.ToChar)
        assert not to_char.args.get("is_numeric")

        to_char = self.validate_identity("TO_CHAR(3.14, UPPER('abcd'))").assert_is(exp.ToChar)
        assert not to_char.args.get("is_numeric")

    def test_date_add(self):
        self.validate_identity("SELECT DATE_ADD(col, 1)")
        self.validate_identity("SELECT DATE_ADD(col, CAST(1 AS INTERVAL HOUR))")
        self.validate_identity(
            "SELECT DATE_ADD(TIMESTAMP '2022-01-01 12:00:00', CAST(-1 AS INTERVAL HOUR))",
            "SELECT DATE_ADD(CAST('2022-01-01 12:00:00' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))",
        )

    def test_date_sub(self):
        self.validate_identity("SELECT DATE_SUB(col, 1)")
        self.validate_identity("SELECT DATE_SUB(col, CAST(1 AS INTERVAL HOUR))")
        self.validate_identity(
            "SELECT DATE_SUB(TIMESTAMP '2022-01-01 12:00:00', CAST(-1 AS INTERVAL HOUR))",
            "SELECT DATE_SUB(CAST('2022-01-01 12:00:00' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))",
        )

    def test_datetime_parsing(self):
        self.validate_identity(
            "SELECT DATE_FORMAT(CAST('2025-08-18 15:30:00' AS TIMESTAMP), 'yyyy-mm-dd')",
            "SELECT TO_CHAR(CAST('2025-08-18 15:30:00' AS TIMESTAMP), 'yyyy-mm-dd')",
        )

    def test_array_generate_range(self):
        self.validate_all(
            "ARRAY_GENERATE_RANGE(1, 4)",
            read={"dremio": "ARRAY_GENERATE_RANGE(1, 4)"},
            write={"duckdb": "GENERATE_SERIES(1, 4)"},
        )

    def test_current_date_utc(self):
        self.validate_identity("SELECT CURRENT_DATE_UTC")
        self.validate_identity(
            "SELECT CURRENT_DATE_UTC()",
            "SELECT CURRENT_DATE_UTC",
        )

    def test_repeatstr(self):
        self.validate_identity("SELECT REPEAT(x, 5)")
        self.validate_identity("SELECT REPEATSTR(x, 5)", "SELECT REPEAT(x, 5)")

    def test_regexp_like(self):
        self.validate_all(
            "REGEXP_MATCHES(x, y)",
            write={
                "dremio": "REGEXP_LIKE(x, y)",
                "duckdb": "REGEXP_MATCHES(x, y)",
                "presto": "REGEXP_LIKE(x, y)",
                "hive": "x RLIKE y",
                "spark": "x RLIKE y",
            },
        )
        self.validate_identity("REGEXP_MATCHES(x, y)", "REGEXP_LIKE(x, y)")

    def test_date_part(self):
        self.validate_identity(
            "SELECT DATE_PART('YEAR', date '2021-04-01')",
            "SELECT EXTRACT('YEAR' FROM CAST('2021-04-01' AS DATE))",
        )

    def test_datetype(self):
        self.validate_identity("DATETYPE(2024,2,2)", "DATE('2024-02-02')")
        self.validate_identity("DATETYPE(x,y,z)", "CAST(CONCAT(x, '-', y, '-', z) AS DATE)")
