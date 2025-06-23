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
