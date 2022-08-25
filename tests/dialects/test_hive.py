from tests.dialects.test_dialect import Validator


class TestHive(Validator):
    dialect = "hive"

    def test_hive(self):
        self.validate_all(
            "PERCENTILE(x, 0.5)",
            write={
                "duckdb": "QUANTILE(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "hive": "PERCENTILE(x, 0.5)",
                "spark": "PERCENTILE(x, 0.5)",
            },
        )

    def test_time(self):
        self.validate_all(
            "DATEDIFF(a, b)",
            write={
                "duckdb": "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
                "presto": "DATE_DIFF('day', CAST(SUBSTR(CAST(b AS VARCHAR), 1, 10) AS DATE), CAST(SUBSTR(CAST(a AS VARCHAR), 1, 10) AS DATE))",
                "hive": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
                "spark": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
            },
        )
