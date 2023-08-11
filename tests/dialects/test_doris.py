from tests.dialects.test_dialect import Validator


class TestDoris(Validator):
    dialect = "doris"

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

    def test_concat(self):
        self.validate_all(
            "SELECT a || b",
            write={
                "doris": "SELECT CONCAT(a, b)",
            },
        )

    def test_to_date(self):
        self.validate_all(
            "SELECT TO_DATE('2019-12-01')",
            read={
                "oracle": "SELECT TO_DATE('2019-12-01')",
                "hive": "SELECT TO_DATE('2019-12-01')",
            },
            write={
                "doris": "SELECT TO_DATE('2019-12-01')",
            },
        ),
        self.validate_all(
            "SELECT STR_TO_DATE('2019-12-01', 'yyyy-MM-dd')",
            read={
                "oracle": "SELECT TO_DATE('2019-12-01', 'yyyy-MM-dd')",
            },
            write={
                "doris": "SELECT STR_TO_DATE('2019-12-01', 'yyyy-MM-dd')",
            },
        ),
        self.validate_all(
            "SELECT STR_TO_DATE('2019-12-01', '%Y-%m-%d')",
            write={
                "doris": "SELECT STR_TO_DATE('2019-12-01', '%Y-%m-%d')",
            },
        ),
        self.validate_all(
            "NVL2(x, y, z)",
            write={
                "doris": "CASE WHEN x IS NOT NULL THEN z ELSE y END",
            },
        ),
        self.validate_all(
            "to_char('2009-10-04 22:23:00', 'yyyymm')",
            write={
                "doris": "DATE_FORMAT('2009-10-04 22:23:00', 'yyyymm')",
            },
        ),
        self.validate_all(
            "to_char(t, 'FM99999999990.00')",
            write={
                "doris": "Round(t,2)",
            },
        ),
