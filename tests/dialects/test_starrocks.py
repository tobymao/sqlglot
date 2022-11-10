from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "starrocks"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
