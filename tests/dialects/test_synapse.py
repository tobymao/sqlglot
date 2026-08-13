from sqlglot import exp
from sqlglot.errors import ParseError
from tests.dialects.test_dialect import Validator


class TestSynapse(Validator):
    dialect = "synapse"

    def test_openrowset(self):
        expression = self.validate_identity(
            "SELECT * FROM OPENROWSET(BULK '/lake-raw/file.parquet', FORMAT='PARQUET') AS source",
            "SELECT * FROM OPENROWSET(BULK '/lake-raw/file.parquet', FORMAT = 'PARQUET') AS source",
        )

        openrowset = expression.find(exp.OpenRowset)
        self.assertIsNotNone(openrowset)
        self.assertEqual("'/lake-raw/file.parquet'", openrowset.args["bulk"].sql())
        self.assertEqual("'PARQUET'", openrowset.args["format"].sql())

    def test_openrowset_pretty(self):
        self.validate_identity(
            "SELECT * FROM OPENROWSET(BULK '/lake-raw/file.parquet', FORMAT='PARQUET') AS source",
            "SELECT\n"
            "  *\n"
            "FROM OPENROWSET(BULK '/lake-raw/file.parquet', FORMAT = 'PARQUET') AS source",
            pretty=True,
        )

    def test_openrowset_requires_bulk_and_format(self):
        with self.assertRaises(ParseError):
            self.parse_one("SELECT * FROM OPENROWSET(BULK '/lake-raw/file.parquet') AS source")

        with self.assertRaises(ParseError):
            self.parse_one(
                "SELECT * FROM OPENROWSET('/lake-raw/file.parquet', FORMAT = 'PARQUET') AS source"
            )

    def test_openrowset_does_not_parse_unimplemented_options(self):
        with self.assertRaises(ParseError):
            self.parse_one(
                "SELECT * FROM OPENROWSET("
                "BULK '/lake-raw/file.parquet', FORMAT_OPTIONS = (FIELDQUOTE = '\"')"
                ") AS source"
            )
