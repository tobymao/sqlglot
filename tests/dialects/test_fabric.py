from tests.dialects.test_dialect import Validator


class TestFabric(Validator):
    dialect = "fabric"
    maxDiff = None

    def test_type_mappings(self):
        """Test unsupported types are correctly mapped to their alternatives"""
        self.validate_identity("CAST(x AS TINYINT)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS SMALLDATETIME)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS NCHAR)", "CAST(x AS CHAR)")
        self.validate_identity("CAST(x AS NVARCHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS VARCHAR(MAX))")
        self.validate_identity("CAST(x AS IMAGE)", "CAST(x AS VARBINARY)")
        self.validate_identity("CAST(x AS MONEY)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS SMALLMONEY)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS JSON)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS XML)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS UNIQUEIDENTIFIER)", "CAST(x AS VARBINARY(MAX))")
        self.validate_identity("CAST(x AS TIMESTAMPTZ)", "CAST(x AS DATETIMEOFFSET(6))")
        self.validate_identity("CAST(x AS DOUBLE)", "CAST(x AS FLOAT)")

        # Test T-SQL override mappings
        self.validate_identity("CAST(x AS DECIMAL)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS INT)", "CAST(x AS INT)")

    def test_precision_capping(self):
        """Test that TIME, DATETIME2 & DATETIMEOFFSET precision is capped at 6 digits"""
        # Default precision should be 6
        self.validate_identity("CAST(x AS TIME)", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2)", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS DATETIMEOFFSET)", "CAST(x AS DATETIMEOFFSET(6))")

        # Precision <= 6 should be preserved
        self.validate_identity("CAST(x AS TIME(3))", "CAST(x AS TIME(3))")
        self.validate_identity("CAST(x AS DATETIME2(3))", "CAST(x AS DATETIME2(3))")
        self.validate_identity("CAST(x AS DATETIMEOFFSET(3))", "CAST(x AS DATETIMEOFFSET(3))")

        self.validate_identity("CAST(x AS TIME(6))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(6))", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS DATETIMEOFFSET(6))", "CAST(x AS DATETIMEOFFSET(6))")

        # Precision > 6 should be capped at 6
        self.validate_identity("CAST(x AS TIME(7))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(7))", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS DATETIMEOFFSET(7))", "CAST(x AS DATETIMEOFFSET(6))")

        self.validate_identity("CAST(x AS TIME(9))", "CAST(x AS TIME(6))")
        self.validate_identity("CAST(x AS DATETIME2(9))", "CAST(x AS DATETIME2(6))")
        self.validate_identity("CAST(x AS DATETIMEOFFSET(9))", "CAST(x AS DATETIMEOFFSET(6))")
