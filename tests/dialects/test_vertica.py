from tests.dialects.test_dialect import Validator


class VerticaDialectTest(Validator):
    maxDiff = None  # type:ignore
    dialect = "vertica"  # type:ignore

    def test_vertica(self):
        pass
