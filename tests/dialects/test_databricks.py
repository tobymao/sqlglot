from tests.dialects.test_dialect import Validator


class TestDatabricks(Validator):
    dialect = "databricks"

    def test_datediff(self):
        self.validate_all("SELECT DATEDIFF(year, 'start', 'end')", write={"tsql": "SELECT(year, 'start', 'end')"})
