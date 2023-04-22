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
        self.validate_identity(
            "CREATE TABLE a (b INT) PRIMARY INDEX (y) PARTITION BY RANGE_N(b BETWEEN 'a', 'b' AND 'c' EACH '1')"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN 0, 1 AND 2 EACH 1)"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN *, 1 AND * EACH b) INDEX (a)"
        )

        self.validate_all(
            "REPLACE VIEW a AS (SELECT b FROM c)",
            write={"teradata": "CREATE OR REPLACE VIEW a AS (SELECT b FROM c)"},
        )

        self.validate_all(
            "CREATE VOLATILE TABLE a",
            write={
                "teradata": "CREATE VOLATILE TABLE a",
                "bigquery": "CREATE TABLE a",
                "clickhouse": "CREATE TABLE a",
                "databricks": "CREATE TABLE a",
                "drill": "CREATE TABLE a",
                "duckdb": "CREATE TABLE a",
                "hive": "CREATE TABLE a",
                "mysql": "CREATE TABLE a",
                "oracle": "CREATE TABLE a",
                "postgres": "CREATE TABLE a",
                "presto": "CREATE TABLE a",
                "redshift": "CREATE TABLE a",
                "snowflake": "CREATE TABLE a",
                "spark": "CREATE TABLE a",
                "sqlite": "CREATE TABLE a",
                "starrocks": "CREATE TABLE a",
                "tableau": "CREATE TABLE a",
                "trino": "CREATE TABLE a",
                "tsql": "CREATE TABLE a",
            },
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

        self.validate_all(
            "SEL a FROM b",
            write={"teradata": "SELECT a FROM b"},
        )

    def test_datatype(self):
        self.validate_all(
            "CREATE TABLE z (a ST_GEOMETRY(1))",
            write={
                "teradata": "CREATE TABLE z (a ST_GEOMETRY(1))",
                "redshift": "CREATE TABLE z (a GEOMETRY(1))",
            },
        )

        self.validate_identity("CREATE TABLE z (a SYSUDTLIB.INT)")

    def test_cast(self):
        self.validate_all(
            "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
            write={
                "teradata": "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
                "databricks": "DATE_FORMAT('1992-01', 'YYYY-DD')",
                "mysql": "DATE_FORMAT('1992-01', 'YYYY-DD')",
                "spark": "DATE_FORMAT('1992-01', 'YYYY-DD')",
                "": "TIME_TO_STR('1992-01', 'YYYY-DD')",
            },
        )
