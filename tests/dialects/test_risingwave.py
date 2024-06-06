from tests.dialects.test_dialect import Validator


class TestRisingWave(Validator):
    dialect = "risingwave"
    maxDiff = None

    def test_risingwave(self):
        self.validate_all(
            "SELECT a FROM tbl",
            read={
                "": "SELECT a FROM tbl FOR UPDATE",
            },
        )
