from tests.dialects.test_dialect import Validator


class TestDrill(Validator):
    dialect = "drill"

    def test_string_literals(self):
        self.validate_all(
            "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            },
        )

    def test_quotes(self):
        self.validate_all(
            "'\\''",
            write={
                "duckdb": "''''",
                "presto": "''''",
                "hive": "'\\''",
                "spark": "'\\''",
            },
        )
        self.validate_all(
            "'\"x\"'",
            write={
                "duckdb": "'\"x\"'",
                "presto": "'\"x\"'",
                "hive": "'\"x\"'",
                "spark": "'\"x\"'",
            },
        )
        self.validate_all(
            "'\\\\a'",
            read={
                "presto": "'\\\\a'",
            },
            write={
                "duckdb": "'\\\\a'",
                "presto": "'\\\\a'",
                "hive": "'\\\\a'",
                "spark": "'\\\\a'",
            },
        )

    def test_table_function(self):
        self.validate_all(
            "SELECT * FROM table( dfs.`test_data.xlsx` (type => 'excel', sheetName => 'secondSheet'))",
            write={
                "drill": "SELECT * FROM table(dfs.`test_data.xlsx`(type => 'excel', sheetName => 'secondSheet'))",
            },
        )
