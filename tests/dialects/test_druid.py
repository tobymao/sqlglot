from tests.dialects.test_dialect import Validator


class TestDruid(Validator):
    dialect = "druid"

    def test_druid(self):
        self.validate_identity("SELECT CEIL(__time TO WEEK) FROM t")
        self.validate_identity("SELECT CEIL(col) FROM t")
        self.validate_identity("SELECT FLOOR(__time TO WEEK) FROM t")
        self.validate_identity("SELECT FLOOR(col) FROM t")
