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

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl COMPUTE STATISTICS")
        self.validate_identity("ANALYZE TABLE tbl COMPUTE STATISTICS SAMPLE 5 PERCENT")
