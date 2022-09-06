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

    def test_introducers(self):
        self.validate_all(
            "_utf8mb4 'hola'",
            read={
                "mysql": "_utf8mb4'hola'",
            },
            write={
                "mysql": "_utf8mb4 'hola'",
            },
        )

    def test_binary_literal(self):
        self.validate_all(
            "SELECT 0xCC",
            write={
                "mysql": "SELECT b'11001100'",
                "spark": "SELECT X'11001100'",
            },
        )
        self.validate_all(
            "SELECT 0xz",
            write={
                "mysql": "SELECT `0xz`",
            },
        )
        self.validate_all(
            "SELECT 0XCC",
            write={
                "mysql": "SELECT 0 AS XCC",
            },
        )

    def test_string_literals(self):
        self.validate_all(
            'SELECT "2021-01-01" + INTERVAL 1 MONTH',
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            },
        )

    def test_convert(self):
        self.validate_all(
            "CONVERT(x USING latin1)",
            write={
                "mysql": "CAST(x AS CHAR CHARACTER SET latin1)",
            },
        )
        self.validate_all(
            "CAST(x AS CHAR CHARACTER SET latin1)",
            write={
                "mysql": "CAST(x AS CHAR CHARACTER SET latin1)",
            },
        )

    def test_hash_comments(self):
        self.validate_all(
            "SELECT 1 # arbitrary content,,, until end-of-line",
            write={
                "mysql": "SELECT 1",
            },
        )
