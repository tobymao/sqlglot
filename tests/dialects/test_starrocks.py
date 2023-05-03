from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "starrocks"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "starrocks": "SELECT REGEXP(abc, '%foo%')",
            },
        )
