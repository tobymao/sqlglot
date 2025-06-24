from tests.dialects.test_dialect import Validator


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
        self.validate_identity("CAST(x AS TIMESTAMPLTZ)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS TIMESTAMPTZ)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS ARRAY)", "CAST(x AS LIST)")
        self.validate_identity("CAST(x AS BIT)", "CAST(x AS BOOLEAN)")
