from tests.dialects.test_dialect import Validator


class TestDatabricks(Validator):
    dialect = "databricks"

    def test_datediff(self):
        self.validate_all(
            "SELECT DATEDIFF(year, 'start', 'end')", write={"tsql": "SELECT DATEDIFF(year, 'start', 'end')"}
        )

    def test_add_date(self):
        self.validate_all(
            "SELECT DATEADD(year, 1, '2020-01-01')", write={"tsql": "SELECT DATEADD(year, 1, '2020-01-01')"}
        )
