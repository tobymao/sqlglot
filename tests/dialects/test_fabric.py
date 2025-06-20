from tests.dialects.test_dialect import Validator


class TestFabric(Validator):
    dialect = "fabric"

    def test_type_mappings(self):
        """Test unsupported types are correctly mapped to their alternatives"""
        self.validate_all("CAST(x AS TINYINT)", write={"fabric": "CAST(x AS SMALLINT)"})
        self.validate_all("CAST(x AS DATETIME)", write={"fabric": "CAST(x AS DATETIME2)"})
        self.validate_all("CAST(x AS SMALLDATETIME)", write={"fabric": "CAST(x AS DATETIME2)"})
        self.validate_all("CAST(x AS NCHAR)", write={"fabric": "CAST(x AS CHAR)"})
        self.validate_all("CAST(x AS NVARCHAR)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS TEXT)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS IMAGE)", write={"fabric": "CAST(x AS VARBINARY)"})
        self.validate_all("CAST(x AS MONEY)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS SMALLMONEY)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS JSON)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS XML)", write={"fabric": "CAST(x AS VARCHAR)"})
        self.validate_all(
            "CAST(x AS UNIQUEIDENTIFIER)", write={"fabric": "CAST(x AS VARBINARY(MAX))"}
        )
        self.validate_all("CAST(x AS TIMESTAMPTZ)", write={"fabric": "CAST(x AS DATETIME2)"})
        self.validate_all("CAST(x AS DOUBLE)", write={"fabric": "CAST(x AS FLOAT)"})
        # Test T-SQL override mappings
        self.validate_all("CAST(x AS DECIMAL)", write={"fabric": "CAST(x AS DECIMAL)"})
        self.validate_all("CAST(x AS INT)", write={"fabric": "CAST(x AS INT)"})
