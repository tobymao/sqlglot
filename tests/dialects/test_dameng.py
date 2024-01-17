from tests.dialects.test_dialect import Validator

class TestDameng(Validator):
    dialect = "Dameng"

    def test_dameng(self):
        self.validate_identity("REGEXP_REPLACE('source', 'search')")
        self.validate_identity("COALESCE(c1, c2, c3)")

    def test_time(self):
        self.validate_identity("CURRENT_TIMESTAMP(precision)")