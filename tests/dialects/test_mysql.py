from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")

    def test_string_literals(self):
        self.validate(
            'SELECT "2021-01-01" + INTERVAL 1 MONTH',
            "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            read="mysql",
            write="mysql",
        )
