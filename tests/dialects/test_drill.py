from tests.dialects.test_dialect import Validator


class TestDrill(Validator):
    dialect = "drill"

    def test_drill(self):
        self.validate_identity(
            "SELECT * FROM table(dfs.`test_data.xlsx`(type => 'excel', sheetName => 'secondSheet'))"
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM t) PIVOT(avg(c1) AS ac1 FOR c2 IN ('V' AS v))",
        )

        self.validate_all(
            "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            write={
                "drill": "SELECT '2021-01-01' + INTERVAL '1' MONTH",
                "mysql": "SELECT '2021-01-01' + INTERVAL '1' MONTH",
            },
        )

    def test_ilike(self):
        self.validate_all(
            "SELECT ILIKE(x, '%y')",
            write={
                "drill": "SELECT ILIKE(x, '%y')",
                "duckdb": "SELECT x ILIKE '%y'",
                "postgres": "SELECT x ILIKE '%y'",
            },
        )
        self.validate_all(
            "SELECT NOT ILIKE(x, '%y')",
            write={
                "drill": "SELECT NOT ILIKE(x, '%y')",
                "postgres": "SELECT NOT x ILIKE '%y'",
            },
        )
        self.validate_all(
            "SELECT ILIKE(x, '%y', '#')",
            write={
                "drill": "SELECT ILIKE(x, '%y', '#')",
                "postgres": "SELECT x ILIKE '%y' ESCAPE '#'",
            },
        )
        self.validate_all(
            "NOT ILIKE(x, '%y', '#')",
            read={"postgres": "x NOT ILIKE '%y' ESCAPE '#'"},
        )
        self.validate_all(
            "ILIKE(x, 'a') OR ILIKE(x, 'b')",
            read={"snowflake": "x ILIKE ANY ('a', 'b')"},
        )
        self.validate_all(
            "ILIKE(x, 'a', '#') OR ILIKE(x, 'b', '#')",
            read={"snowflake": "x ILIKE ANY ('a', 'b') ESCAPE '#'"},
        )
        self.validate_all(
            "x LIKE 'a' OR x LIKE 'b'",
            read={"snowflake": "x LIKE ANY ('a', 'b')"},
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl COMPUTE STATISTICS")
        self.validate_identity("ANALYZE TABLE tbl COMPUTE STATISTICS SAMPLE 5 PERCENT")
