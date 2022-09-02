from tests.dialects.test_dialect import Validator


class TestClickhouse(Validator):
    dialect = "clickhouse"

    def test_clickhouse(self):
        self.validate_identity("dictGet(x, 'y')")
        self.validate_identity("SELECT * FROM x FINAL")
        self.validate_identity("SELECT * FROM x AS y FINAL")

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "clickhouse": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
            },
        )

        self.validate_all(
            "CAST(1 AS NULLABLE(Int64))",
            write={
                "clickhouse": "CAST(1 AS Nullable(BIGINT))",
            },
        )
