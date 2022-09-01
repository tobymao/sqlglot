from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
            write={
                "mysql": "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
                "spark": "CREATE TABLE z (a INT) COMMENT 'x'",
            },
        )

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")

    def test_string_literals(self):
        self.validate_all(
            'SELECT "2021-01-01" + INTERVAL 1 MONTH',
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            },
        )
