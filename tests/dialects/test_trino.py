from sqlglot import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestTrino(Validator):
    dialect = "trino"

    def test_DAY_OF_WEEK(self):
        self.validate_all(
            "DAY_OF_WEEK(timestamp '2012-08-08 01:00')",
            write={
                "spark": "DAYOFWEEK(CAST('2012-08-08 01:00' AS TIMESTAMP))",
            },
        )

    def test_DAY_OF_MONTH(self):
        self.validate_all(
            "DAY_OF_MONTH(timestamp '2012-08-08 01:00')",
            write={
                "spark": "DAYOFMONTH(CAST('2012-08-08 01:00' AS TIMESTAMP))",
            },
        )

    def test_DAY_OF_YEAR(self):
        self.validate_all(
            "DAY_OF_YEAR(timestamp '2012-08-08 01:00')",
            write={
                "spark": "DAYOFYEAR(CAST('2012-08-08 01:00' AS TIMESTAMP))",
            },
        )

    def test_WEEK_OF_YEAR(self):
        self.validate_all(
            "WEEK_OF_YEAR(timestamp '2012-08-08 01:00')",
            write={
                "spark": "WEEKOFYEAR(CAST('2012-08-08 01:00' AS TIMESTAMP))",
            },
        )

    def test_FROM_JSON(self):
        self.validate_all(
            "SELECT CAST(JSON '[1,23,456]' AS ARRAY(INTEGER))",
            write={
                "spark": "SELECT FROM_JSON('[1,23,456]', 'ARRAY<INT>')",
            },
        )
        self.validate_all(
            """SELECT CAST(JSON '{"k1":1,"k2":23,"k3":456}' AS MAP(VARCHAR, INTEGER))""",
            write={
                "spark": "SELECT FROM_JSON('{\"k1\":1,\"k2\":23,\"k3\":456}', 'MAP<STRING, INT>')",
            },
        )

    def test_TO_JSON(self):
        self.validate_all(
            "SELECT CAST(ARRAY [1, 23, 456] AS JSON)",
            write={
                "spark": "SELECT TO_JSON(ARRAY(1, 23, 456))",
            },
        )

    def test_FROM_UTC_TIMESTAMP(self):
        self.validate_all(
            "SELECT timestamp '2012-10-31 00:00 UTC' AT TIME ZONE 'America/Sao_Paulo'",
            write={
                "spark": "SELECT FROM_UTC_TIMESTAMP(CAST('2012-10-31 00:00 ' AS TIMESTAMP), 'America/Sao_Paulo')",
            },
        )
