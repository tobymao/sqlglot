from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    dialect = "exasol"
    maxDiff = None

    def test_type_mappings(self):
        self.validate_identity("CAST(x AS BLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS LONGBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS LONGTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS MEDIUMBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS MEDIUMTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TINYBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TINYTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS VARBINARY)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS VARCHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS CHAR)", "CAST(x AS CHAR)")
        self.validate_identity("CAST(x AS TINYINT)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS INT)")
        self.validate_identity("CAST(x AS MEDIUMINT)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS BIGINT)")
        self.validate_identity("CAST(x AS FLOAT)")
        self.validate_identity("CAST(x AS DOUBLE)")
        self.validate_identity("CAST(x AS DECIMAL32)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL64)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL128)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL256)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DATE)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS TIMESTAMP)")
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            read={
                "tsql": "CAST(x AS DATETIME2)",
            },
            write={
                "exasol": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            read={
                "tsql": "CAST(x AS SMALLDATETIME)",
            },
            write={
                "exasol": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_identity("CAST(x AS BOOLEAN)")
        self.validate_identity(
            "CAST(x AS TIMESTAMPLTZ)", "CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)"
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMP(3) WITH LOCAL TIME ZONE)",
            "CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)",
        )

    def test_mod(self):
        self.validate_all(
            "SELECT MOD(x, 10)",
            read={"exasol": "SELECT MOD(x, 10)"},
            write={
                "teradata": "SELECT x MOD 10",
                "mysql": "SELECT x % 10",
                "exasol": "SELECT MOD(x, 10)",
            },
        )
