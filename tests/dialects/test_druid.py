from sqlglot.dialects.dialect import Dialects
from tests.dialects.test_dialect import Validator


class TestDruid(Validator):
    dialect = "druid"

    def test_druid(self):
        self.validate_identity("SELECT MOD(1000, 60)")
        self.validate_identity("SELECT CEIL(__time TO WEEK) FROM t")
        self.validate_identity("SELECT CEIL(col) FROM t")
        self.validate_identity("SELECT CEIL(price, 2) AS rounded_price FROM t")
        self.validate_identity("SELECT FLOOR(__time TO WEEK) FROM t")
        self.validate_identity("SELECT FLOOR(col) FROM t")
        self.validate_identity("SELECT FLOOR(price, 2) AS rounded_price FROM t")
        self.validate_identity("SELECT CURRENT_TIMESTAMP")
        self.validate_identity("SELECT ARRAY[1, 2, 3]")

        # validate across all dialects
        write = {dialect.value: "FLOOR(__time TO WEEK)" for dialect in Dialects}
        self.validate_all(
            "FLOOR(__time TO WEEK)",
            write=write,
        )

    def test_json_value(self):
        json_doc = """'{"item": "shoes", "price": "49.95"}'"""
        self.validate_identity(f"SELECT JSON_VALUE({json_doc}, '$.item')")
        self.validate_identity(f"SELECT JSON_VALUE({json_doc}, '$.price' RETURNING DOUBLE)")
        self.validate_identity("SELECT JSON_VALUE(x, '$.a' RETURNING BIGINT) FROM t")
