from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_basic(self):
        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM users ORDER BY ALL")
