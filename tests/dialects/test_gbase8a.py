from tests.dialects.test_dialect import Validator


class TestGbase8a(Validator):
    dialect = "gbase8a"

    def test_identity(self):
        self.validate_identity("CAST(x AS ENUM('a', 'b'))")
        self.validate_identity("CAST(x AS SET('a', 'b'))")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
