from tests.dialects.test_dialect import Validator


class TestTeradata(Validator):
    dialect = "teradata"

    def test_translate(self):
        self.validate_all(
            "TRANSLATE(x USING LATIN_TO_UNICODE)",
            write={
                "teradata": "CAST(x AS CHAR CHARACTER SET UNICODE)",
            },
        )
        self.validate_all(
            "CAST(x AS CHAR CHARACTER SET UNICODE)",
            write={
                "teradata": "CAST(x AS CHAR CHARACTER SET UNICODE)",
            },
        )
