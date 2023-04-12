from tests.dialects.test_dialect import Validator


class TestDrill(Validator):
    dialect = "drill"

    def test_drill(self):
        self.validate_all(
            "DATE_FORMAT(a, 'yyyy')",
            write={"drill": "TO_CHAR(a, 'yyyy')"},
        )

    def test_string_literals(self):
        self.validate_all(
            "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL '1' MONTH",
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

    def test_validate_pivot(self):
        self.validate_all(
            "SELECT * FROM (SELECT education_level, salary, marital_status, "
            "EXTRACT(year FROM age(birth_date)) age FROM cp.`employee.json`) PIVOT (avg(salary) AS "
            "avg_salary, avg(age) AS avg_age FOR marital_status IN ('M' married, 'S' single))",
            write={
                "drill": "SELECT * FROM (SELECT education_level, salary, marital_status, "
                "EXTRACT(year FROM age(birth_date)) AS age FROM cp.`employee.json`) "
                "PIVOT(AVG(salary) AS avg_salary, AVG(age) AS avg_age FOR marital_status "
                "IN ('M' AS married, 'S' AS single))"
            },
        )
