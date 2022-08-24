from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
