from tests.dialects.test_dialect import Validator


class TestFabric(Validator):
    dialect = "fabric"

    def test_type_mappings(self):
        """Test unsupported types are correctly mapped to their alternatives"""
        self.validate_all("CAST(x AS TINYINT)", write={"fabric": "CAST(x AS SMALLINT)"})
        self.validate_all("CAST(x AS DATETIME)", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all("CAST(x AS SMALLDATETIME)", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all("CAST(x AS NCHAR)", write={"fabric": "CAST(x AS CHAR)"})
        self.validate_all("CAST(x AS NVARCHAR)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS TEXT)", write={"fabric": "CAST(x AS VARCHAR(MAX))"})
        self.validate_all("CAST(x AS IMAGE)", write={"fabric": "CAST(x AS VARBINARY)"})
        self.validate_all("CAST(x AS MONEY)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS SMALLMONEY)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS JSON)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS XML)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all(
            "CAST(x AS UNIQUEIDENTIFIER)", write={"fabric": "CAST(x AS VARBINARY(MAX))"}
        )
        self.validate_all(
            "CAST(x AS TIMESTAMPTZ)", write={"fabric": "CAST(x AS DATETIMEOFFSET(6))"}
        )
        self.validate_all("CAST(x AS DOUBLE)", write={"fabric": "CAST(x AS FLOAT)"})
        # Test T-SQL override mappings
        self.validate_all("CAST(x AS DECIMAL)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS INT)", write={"fabric": "CAST(x AS INT)"})

    def test_precision_capping(self):
        """Test that TIME, DATETIME2 & DATETIMEOFFSET precision is capped at 6 digits"""
        # Default precision should be 6
        self.validate_all("CAST(x AS TIME)", write={"fabric": "CAST(x AS TIME(6))"})
        self.validate_all("CAST(x AS DATETIME2)", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all(
            "CAST(x AS DATETIMEOFFSET)", write={"fabric": "CAST(x AS DATETIMEOFFSET(6))"}
        )

        # Precision <= 6 should be preserved
        self.validate_all("CAST(x AS TIME(3))", write={"fabric": "CAST(x AS TIME(3))"})
        self.validate_all("CAST(x AS DATETIME2(3))", write={"fabric": "CAST(x AS DATETIME2(3))"})
        self.validate_all(
            "CAST(x AS DATETIMEOFFSET(3))", write={"fabric": "CAST(x AS DATETIMEOFFSET(3))"}
        )

        self.validate_all("CAST(x AS TIME(6))", write={"fabric": "CAST(x AS TIME(6))"})
        self.validate_all("CAST(x AS DATETIME2(6))", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all(
            "CAST(x AS DATETIMEOFFSET(6))", write={"fabric": "CAST(x AS DATETIMEOFFSET(6))"}
        )

        # Precision > 6 should be capped at 6
        self.validate_all("CAST(x AS TIME(7))", write={"fabric": "CAST(x AS TIME(6))"})
        self.validate_all("CAST(x AS DATETIME2(7))", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all(
            "CAST(x AS DATETIMEOFFSET(7))", write={"fabric": "CAST(x AS DATETIMEOFFSET(6))"}
        )

        self.validate_all("CAST(x AS TIME(9))", write={"fabric": "CAST(x AS TIME(6))"})
        self.validate_all("CAST(x AS DATETIME2(9))", write={"fabric": "CAST(x AS DATETIME2(6))"})
        self.validate_all(
            "CAST(x AS DATETIMEOFFSET(9))", write={"fabric": "CAST(x AS DATETIMEOFFSET(6))"}
        )
