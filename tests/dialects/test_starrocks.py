from tests.dialects.test_dialect import Validator


class TestStarrocks(Validator):
    dialect = "starrocks"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
        self.validate_identity(
            "SELECT DATE_DIFF('SECOND', '2010-11-30 23:59:59', '2010-11-30 20:58:59')"
        )
        self.validate_identity(
            "SELECT DATE_DIFF('MINUTE', '2010-11-30 23:59:59', '2010-11-30 20:58:59')"
        )

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP(abc, '%foo%')",
            read={
                "mysql": "SELECT REGEXP_LIKE(abc, '%foo%')",
                "starrocks": "SELECT REGEXP(abc, '%foo%')",
            },
            write={
                "mysql": "SELECT REGEXP_LIKE(abc, '%foo%')",
            },
        )
