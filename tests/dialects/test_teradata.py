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
        self.validate_identity("CAST(x AS CHAR CHARACTER SET UNICODE)")

    def test_update(self):
        self.validate_all(
            "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
            write={
                "teradata": "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
                "mysql": "UPDATE A SET col2 = '' FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B WHERE A.col1 = B.col1",
            },
        )

    def test_create(self):
        self.validate_identity("CREATE TABLE x (y INT) PRIMARY INDEX (y) PARTITION BY y INDEX (y)")

        self.validate_all(
            "REPLACE VIEW a AS (SELECT b FROM c)",
            write={"teradata": "CREATE OR REPLACE VIEW a AS (SELECT b FROM c)"},
        )

        self.validate_all(
            "SEL a FROM b",
            write={"teradata": "SELECT a FROM b"},
        )

    def test_insert(self):
        self.validate_all(
            "INS INTO x SELECT * FROM y", write={"teradata": "INSERT INTO x SELECT * FROM y"}
        )

    def test_mod(self):
        self.validate_all("a MOD b", write={"teradata": "a MOD b", "mysql": "a % b"})

    def test_abbrev(self):
        self.validate_all("a LT b", write={"teradata": "a < b"})
        self.validate_all("a LE b", write={"teradata": "a <= b"})
        self.validate_all("a GT b", write={"teradata": "a > b"})
        self.validate_all("a GE b", write={"teradata": "a >= b"})
        self.validate_all("a ^= b", write={"teradata": "a <> b"})
        self.validate_all("a NE b", write={"teradata": "a <> b"})
        self.validate_all("a NOT= b", write={"teradata": "a <> b"})
