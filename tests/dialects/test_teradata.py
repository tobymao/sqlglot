from sqlglot import exp
from tests.dialects.test_dialect import Validator


class TestTeradata(Validator):
    dialect = "teradata"

    def test_teradata(self):
        self.validate_all(
            "RANDOM(l, u)",
            write={
                "": "(u - l) * RAND() + l",
                "teradata": "RANDOM(l, u)",
            },
        )
        self.validate_identity("TO_NUMBER(expr, fmt, nlsparam)")
        self.validate_identity("SELECT TOP 10 * FROM tbl")
        self.validate_identity("SELECT * FROM tbl SAMPLE 5")
        self.validate_identity(
            "SELECT * FROM tbl SAMPLE 0.33, .25, .1",
            "SELECT * FROM tbl SAMPLE 0.33, 0.25, 0.1",
        )

        self.validate_all(
            "DATABASE tduser",
            read={
                "databricks": "USE tduser",
            },
            write={
                "databricks": "USE tduser",
                "teradata": "DATABASE tduser",
            },
        )

        self.validate_identity("SELECT 0x1d", "SELECT X'1d'")
        self.validate_identity("SELECT X'1D'", "SELECT X'1D'")
        self.validate_identity("SELECT x'1d'", "SELECT X'1d'")

        self.validate_identity(
            "RENAME TABLE emp TO employee", check_command_warning=True
        ).assert_is(exp.Command)

    def test_translate(self):
        self.validate_identity("TRANSLATE(x USING LATIN_TO_UNICODE)")
        self.validate_identity("TRANSLATE(x USING LATIN_TO_UNICODE WITH ERROR)")

    def test_update(self):
        self.validate_all(
            "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
            write={
                "teradata": "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
                "mysql": "UPDATE A SET col2 = '' FROM `schema`.tableA AS A, (SELECT col1 FROM `schema`.tableA GROUP BY col1) AS B WHERE A.col1 = B.col1",
            },
        )

    def test_statistics(self):
        self.validate_identity("COLLECT STATISTICS ON tbl INDEX(col)", check_command_warning=True)
        self.validate_identity("COLLECT STATS ON tbl COLUMNS(col)", check_command_warning=True)
        self.validate_identity("COLLECT STATS COLUMNS(col) ON tbl", check_command_warning=True)
        self.validate_identity("HELP STATISTICS personel.employee", check_command_warning=True)
        self.validate_identity(
            "HELP STATISTICS personnel.employee FROM my_qcd", check_command_warning=True
        )

    def test_create(self):
        self.validate_identity(
            "REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
            "CREATE OR REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
        )
        self.validate_identity(
            "REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
            "CREATE OR REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
        )
        self.validate_identity("CREATE TABLE x (y INT) PRIMARY INDEX (y) PARTITION BY y INDEX (y)")
        self.validate_identity("CREATE TABLE x (y INT) PARTITION BY y INDEX (y)")
        self.validate_identity(
            "CREATE MULTISET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT PRESERVE ROWS"
        )
        self.validate_identity(
            "CREATE SET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT DELETE ROWS"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PRIMARY INDEX (y) PARTITION BY RANGE_N(b BETWEEN 'a', 'b' AND 'c' EACH '1')"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN 0, 1 AND 2 EACH 1)"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN *, 1 AND * EACH b) INDEX (a)"
        )
        self.validate_identity(
            "CREATE TABLE a, NO FALLBACK PROTECTION, NO LOG, NO JOURNAL, CHECKSUM=ON, NO MERGEBLOCKRATIO, BLOCKCOMPRESSION=ALWAYS (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, NO FALLBACK PROTECTION, NO LOG, NO JOURNAL, CHECKSUM=ON, NO MERGEBLOCKRATIO, BLOCKCOMPRESSION=ALWAYS (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, WITH JOURNAL TABLE=x.y.z, CHECKSUM=OFF, MERGEBLOCKRATIO=1, DATABLOCKSIZE=10 KBYTES (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, BEFORE JOURNAL, AFTER JOURNAL, FREESPACE=1, DEFAULT DATABLOCKSIZE, BLOCKCOMPRESSION=DEFAULT (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, DUAL JOURNAL, DUAL AFTER JOURNAL, MERGEBLOCKRATIO=1 PERCENT, DATABLOCKSIZE=10 KILOBYTES (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, DUAL BEFORE JOURNAL, LOCAL AFTER JOURNAL, MAXIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=AUTOTEMP(c1 INT) (a INT)"
        )
        self.validate_identity(
            "CREATE VOLATILE MULTISET TABLE a, NOT LOCAL AFTER JOURNAL, FREESPACE=1 PERCENT, DATABLOCKSIZE=10 BYTES, WITH NO CONCURRENT ISOLATED LOADING FOR ALL (a INT)"
        )
        self.validate_identity(
            "CREATE VOLATILE SET TABLE example1 AS (SELECT col1, col2, col3 FROM table1) WITH DATA PRIMARY INDEX (col1) ON COMMIT PRESERVE ROWS"
        )
        self.validate_identity(
            "CREATE SET GLOBAL TEMPORARY TABLE a, NO BEFORE JOURNAL, NO AFTER JOURNAL, MINIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=NEVER (a INT)"
        )
        self.validate_all(
            """
            CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL,
            CHECKSUM = DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT)
            PRIMARY INDEX (a),
            INDEX(x, y)
            """,
            write={
                "teradata": "CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM=DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT) PRIMARY INDEX (a) INDEX (x, y)",
            },
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
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD', measurement INT COMPRESS)"
        )
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD', measurement INT COMPRESS (1, 2, 3))"
        )
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD' COMPRESS (CAST('9999-09-09' AS DATE)), measurement INT)"
        )

    def test_insert(self):
        self.validate_all(
            "INS INTO x SELECT * FROM y", write={"teradata": "INSERT INTO x SELECT * FROM y"}
        )

    def test_mod(self):
        self.validate_all("a MOD b", write={"teradata": "a MOD b", "mysql": "a % b"})

    def test_power(self):
        self.validate_all("a ** b", write={"teradata": "a ** b", "mysql": "POWER(a, b)"})

    def test_abbrev(self):
        self.validate_identity("a LT b", "a < b")
        self.validate_identity("a LE b", "a <= b")
        self.validate_identity("a GT b", "a > b")
        self.validate_identity("a GE b", "a >= b")
        self.validate_identity("a ^= b", "a <> b")
        self.validate_identity("a NE b", "a <> b")
        self.validate_identity("a NOT= b", "a <> b")
        self.validate_identity("a EQ b", "a = b")
        self.validate_identity("SEL a FROM b", "SELECT a FROM b")
        self.validate_identity(
            "SELECT col1, col2 FROM dbc.table1 WHERE col1 EQ 'value1' MINUS SELECT col1, col2 FROM dbc.table2",
            "SELECT col1, col2 FROM dbc.table1 WHERE col1 = 'value1' EXCEPT SELECT col1, col2 FROM dbc.table2",
        )
        self.validate_identity("UPD a SET b = 1", "UPDATE a SET b = 1")
        self.validate_identity("DEL FROM a", "DELETE FROM a")

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
            read={
                "bigquery": "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
            },
            write={
                "teradata": "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
                "bigquery": "PARSE_DATE('%Y-%d', '1992-01')",
                "databricks": "TO_DATE('1992-01', 'yyyy-dd')",
                "mysql": "STR_TO_DATE('1992-01', '%Y-%d')",
                "spark": "TO_DATE('1992-01', 'yyyy-dd')",
                "": "STR_TO_DATE('1992-01', '%Y-%d')",
            },
        )
        self.validate_identity("CAST('1992-01' AS FORMAT 'YYYY-DD')")

        self.validate_all(
            "TRYCAST('-2.5' AS DECIMAL(5, 2))",
            read={
                "snowflake": "TRY_CAST('-2.5' AS DECIMAL(5, 2))",
            },
            write={
                "snowflake": "TRY_CAST('-2.5' AS DECIMAL(5, 2))",
                "teradata": "TRYCAST('-2.5' AS DECIMAL(5, 2))",
            },
        )

    def test_format_override(self):
        # Teradata column format overrides use the `(FORMAT <format_string>)` syntax.
        # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
        self.validate_identity("SELECT ('a' || 'b') (FORMAT '...')")
        self.validate_identity("SELECT Col1 (FORMAT '+9999') FROM Test1")
        self.validate_identity("SELECT date_col (FORMAT 'YYYY-MM-DD') FROM t")
        self.validate_identity(
            "SELECT CAST(Col1 AS INTEGER) FROM Test1",
            "SELECT CAST(Col1 AS INT) FROM Test1",
        )

    def test_time(self):
        self.validate_identity("CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP WITH TIME ZONE)")

        self.validate_all(
            "CURRENT_TIMESTAMP",
            read={
                "teradata": "CURRENT_TIMESTAMP",
                "snowflake": "CURRENT_TIMESTAMP()",
            },
        )

        self.validate_all(
            "SELECT '2023-01-01' + INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' + INTERVAL '5' YEAR",
                "snowflake": "SELECT DATEADD(YEAR, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' - INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' - INTERVAL '5' YEAR",
                "snowflake": "SELECT DATEADD(YEAR, -5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' - INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' - INTERVAL '5' YEAR",
                "sqlite": "SELECT DATE_SUB('2023-01-01', 5, YEAR)",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' + INTERVAL '5' YEAR",
                "sqlite": "SELECT DATE_SUB('2023-01-01', -5, YEAR)",
            },
        )
        self.validate_all(
            "SELECT (90 * INTERVAL '1' DAY)",
            read={
                "teradata": "SELECT (90 * INTERVAL '1' DAY)",
                "snowflake": "SELECT INTERVAL '1' QUARTER",
            },
        )
        self.validate_all(
            "SELECT (7 * INTERVAL '1' DAY)",
            read={
                "teradata": "SELECT (7 * INTERVAL '1' DAY)",
                "snowflake": "SELECT INTERVAL '1' WEEK",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + (90 * INTERVAL '5' DAY)",
            read={
                "teradata": "SELECT '2023-01-01' + (90 * INTERVAL '5' DAY)",
                "snowflake": "SELECT DATEADD(QUARTER, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + (7 * INTERVAL '5' DAY)",
            read={
                "teradata": "SELECT '2023-01-01' + (7 * INTERVAL '5' DAY)",
                "snowflake": "SELECT DATEADD(WEEK, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "CAST(TO_CHAR(x, 'Q') AS INT)",
            read={
                "teradata": "CAST(TO_CHAR(x, 'Q') AS INT)",
                "snowflake": "DATE_PART(QUARTER, x)",
                "bigquery": "EXTRACT(QUARTER FROM x)",
            },
        )
        self.validate_all(
            "EXTRACT(MONTH FROM x)",
            read={
                "teradata": "EXTRACT(MONTH FROM x)",
                "snowflake": "DATE_PART(MONTH, x)",
                "bigquery": "EXTRACT(MONTH FROM x)",
            },
        )
        self.validate_all(
            "CAST(TO_CHAR(x, 'Q') AS INT)",
            read={
                "snowflake": "quarter(x)",
                "teradata": "CAST(TO_CHAR(x, 'Q') AS INT)",
            },
        )
