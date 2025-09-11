from tests.dialects.test_dialect import Validator


class TestFabric(Validator):
    dialect = "fabric"
    maxDiff = None

    def test_type_mappings(self):
        """Test that types are correctly mapped to their alternatives"""
        self.validate_identity("CAST(x AS BOOLEAN)", "CAST(x AS BIT)")
        self.validate_identity("CAST(x AS DATE)", "CAST(x AS DATE)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS DECIMAL)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DOUBLE)", "CAST(x AS FLOAT)")
        self.validate_identity("CAST(x AS IMAGE)", "CAST(x AS VARBINARY)")
        self.validate_identity("CAST(x AS INT)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS JSON)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS MONEY)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS NCHAR)", "CAST(x AS CHAR)")
        self.validate_identity("CAST(x AS NVARCHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS ROWVERSION)", "CAST(x AS ROWVERSION)")
        self.validate_identity("CAST(x AS SMALLDATETIME)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS SMALLMONEY)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS VARCHAR(MAX))")
        self.validate_identity("CAST(x AS TIMESTAMP)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS TIMESTAMPNTZ)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS TINYINT)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS UTINYINT)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS UUID)", "CAST(x AS UNIQUEIDENTIFIER)")
        self.validate_identity("CAST(x AS VARIANT)", "CAST(x AS SQL_VARIANT)")
        self.validate_identity("CAST(x AS XML)", "CAST(x AS VARCHAR)")

    def test_precision_capping(self):
        """Test that TIME, DATETIME2 & DATETIMEOFFSET precision is capped at 6 digits"""
        # Default precision should be 6
        self.validate_identity("CAST(x AS TIME)", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2)", "CAST(x AS DATETIME2(6))")

        # Precision <= 6 should be preserved
        self.validate_identity("CAST(x AS TIME(3))", "CAST(x AS TIME(3))")
        self.validate_identity("CAST(x AS DATETIME2(3))", "CAST(x AS DATETIME2(3))")

        self.validate_identity("CAST(x AS TIME(6))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(6))", "CAST(x AS DATETIME2(6))")

        # Precision > 6 should be capped at 6
        self.validate_identity("CAST(x AS TIME(7))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(7))", "CAST(x AS DATETIME2(6))")

        self.validate_identity("CAST(x AS TIME(9))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(9))", "CAST(x AS DATETIME2(6))")

    def test_timestamptz_without_at_time_zone(self):
        # TIMESTAMPTZ should be cast to TIMESTAMP when not in an AT TIME ZONE
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ)",
            "CAST(x AS DATETIME2(6))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(3))",
            "CAST(x AS DATETIME2(3))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(6))",
            "CAST(x AS DATETIME2(6))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(9))",
            "CAST(x AS DATETIME2(6))",
        )

    def test_timestamptz_with_at_time_zone(self):
        # TIMESTAMPTZ should be DATETIMEOFFSET when in an AT TIME ZONE expression and then cast to TIMESTAMP
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ) AT TIME ZONE 'Pacific Standard Time'",
            "CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE 'Pacific Standard Time' AS DATETIME2(6))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(3)) AT TIME ZONE 'Pacific Standard Time'",
            "CAST(CAST(x AS DATETIMEOFFSET(3)) AT TIME ZONE 'Pacific Standard Time' AS DATETIME2(3))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(6)) AT TIME ZONE 'Pacific Standard Time'",
            "CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE 'Pacific Standard Time' AS DATETIME2(6))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ(9)) AT TIME ZONE 'Pacific Standard Time'",
            "CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE 'Pacific Standard Time' AS DATETIME2(6))",
        )

    def test_unix_to_time(self):
        """Test UnixToTime transformation to DATEADD with microseconds"""

        self.validate_identity(
            "UNIX_TO_TIME(column)",
            "DATEADD(MICROSECONDS, CAST(ROUND(column * 1e6, 0) AS BIGINT), CAST('1970-01-01' AS DATETIME2(6)))",
        )

    def test_varchar_precision_inference(self):
        # Test VARCHAR without precision conversion to VARCHAR(1)
        self.validate_identity(
            "CREATE TABLE t (col VARCHAR)",
            "CREATE TABLE t (col VARCHAR(1))",
        )

        # Test VARCHAR with existing precision should remain unchanged
        self.validate_identity("CREATE TABLE t (col VARCHAR(50))")

        # Test CHAR without precision conversion to CHAR(1)
        self.validate_identity(
            "CREATE TABLE t (col CHAR)",
            "CREATE TABLE t (col CHAR(1))",
        )

        # Test CHAR with existing precision should remain unchanged
        self.validate_identity("CREATE TABLE t (col CHAR(10))")

        # Test cross-dialect conversion: non-TSQL VARCHAR -> TSQL VARCHAR(MAX)
        self.validate_all(
            "CREATE TABLE t (col VARCHAR(MAX))",
            read={
                "postgres": "CREATE TABLE t (col VARCHAR)",
                "tsql": "CREATE TABLE t (col VARCHAR(MAX))",
            },
        )

        # Test cross-dialect conversion: non-TSQL CHAR -> TSQL CHAR(MAX)
        self.validate_all(
            "CREATE TABLE t (col CHAR(MAX))",
            read={
                "postgres": "CREATE TABLE t (col CHAR)",
                "tsql": "CREATE TABLE t (col CHAR(MAX))",
            },
        )
