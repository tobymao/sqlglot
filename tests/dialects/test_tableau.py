from tests.dialects.test_dialect import Validator


class TestTableau(Validator):
    dialect = "tableau"

    def test_tableau(self):
        self.validate_all(
            "IF x = 'a' THEN y ELSE NULL END",
            read={
                "presto": "IF(x = 'a', y, NULL)",
            },
            write={
                "presto": "IF(x = 'a', y, NULL)",
                "hive": "IF(x = 'a', y, NULL)",
                "tableau": "IF x = 'a' THEN y ELSE NULL END",
            },
        )
        self.validate_all(
            "IFNULL(a, 0)",
            read={
                "presto": "COALESCE(a, 0)",
            },
            write={
                "presto": "COALESCE(a, 0)",
                "hive": "COALESCE(a, 0)",
                "tableau": "IFNULL(a, 0)",
            },
        )
        self.validate_all(
            "COUNTD(a)",
            read={
                "presto": "COUNT(DISTINCT a)",
            },
            write={
                "presto": "COUNT(DISTINCT a)",
                "hive": "COUNT(DISTINCT a)",
                "tableau": "COUNTD(a)",
            },
        )
        self.validate_all(
            "COUNTD((a))",
            read={
                "presto": "COUNT(DISTINCT(a))",
            },
            write={
                "presto": "COUNT(DISTINCT (a))",
                "hive": "COUNT(DISTINCT (a))",
                "tableau": "COUNTD((a))",
            },
        )
        self.validate_all(
            "COUNT(a)",
            read={
                "presto": "COUNT(a)",
            },
            write={
                "presto": "COUNT(a)",
                "hive": "COUNT(a)",
                "tableau": "COUNT(a)",
            },
        )
