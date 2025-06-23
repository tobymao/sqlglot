from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    def test_type_mappings(self):
        self.validate_all("CAST(x AS BLOB)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS LONGBLOB)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS LONGTEXT)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS MEDIUMBLOB)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS MEDIUMTEXT)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS TINYBLOB)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS TINYTEXT)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS TEXT)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS VARBINARY)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS VARCHAR)", write={"exasol": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS CHAR)", write={"exasol": "CAST(x AS CHAR)"})
