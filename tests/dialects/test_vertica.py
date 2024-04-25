from tests.dialects.test_dialect import Validator


class TestVertica(Validator):
    dialect = "vertica"
    maxDiff = None

    def test_vertica(self):
        self.validate_identity("CAST(public.x AS MONEY)")
        self.validate_identity("CAST(public.x AS LONGVARBINARY)")
        self.validate_identity("CAST(public.x AS INTERVAL DAY TO SECOND)")
