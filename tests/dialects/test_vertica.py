
from sqlglot.dialects import Vertica
from tests.dialects.test_dialect import Validator


class VerticaDialectTest(Validator):

    maxDiff = None
    dialect = "vertica"
    def test_vertica(self):
        pass
       



