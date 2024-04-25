from tests.dialects.test_dialect import Validator


class TestVertica(Validator):
    dialect = "vertica"
    maxDiff = None

    def test_vertica(self):
        self.validate_identity("CAST(x AS MONEY)")
        self.validate_identity("CAST(x AS LONGVARBINARY)")
        self.validate_identity("CAST(x AS INTERVAL DAY TO SECOND)")
        self.validate_identity("CAST(x AS INTERVAL YEAR TO MONTH)")
