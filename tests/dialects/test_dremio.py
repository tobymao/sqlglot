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
