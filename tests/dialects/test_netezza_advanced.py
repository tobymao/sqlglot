import unittest

from sqlglot import parse_one


class TestNetezzaAdvanced(unittest.TestCase):
    def test_merge_roundtrip(self):
        sql = (
            "MERGE INTO TGT USING SRC ON TGT.ID = SRC.ID "
            "WHEN MATCHED THEN UPDATE SET VAL = SRC.VAL "
            "WHEN NOT MATCHED THEN INSERT (ID, VAL) VALUES (SRC.ID, SRC.VAL)"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_update_from_roundtrip(self):
        sql = "UPDATE TGT SET VAL = SRC.VAL FROM SRC WHERE TGT.ID = SRC.ID"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_external_table_simple(self):
        sql = "CREATE EXTERNAL TABLE t (a INT)"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_external_table_using_options(self):
        sql = (
            "CREATE EXTERNAL TABLE t (a INT) USING (DELIMITER '|', ENCODING 'UTF8')"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_create_external_table_using_many_options(self):
        # Mix of space and equals between key/value
        sql = (
            "CREATE EXTERNAL TABLE t (a INT) USING (DELIMITER '|', ESCAPECHAR '\\', QUOTECHAR '" + '"' + "', NULLVALUE 'NA', MAXERRORS 10)"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_identity_column(self):
        sql = (
            "CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), x INT)"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_truncate(self):
        sql = "TRUNCATE TABLE t"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_ctas_temp_with_properties(self):
        sql = (
            "CREATE TEMPORARY TABLE T DISTRIBUTE ON (A) AS SELECT 1 AS A"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_functions_nvl_decode(self):
        sql = "SELECT NVL(a, 0), DECODE(x, 1, 'one', 'other') FROM t"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_date_functions(self):
        sql = (
            "SELECT ADD_MONTHS(CAST('2020-01-31' AS DATE), 1), DATE_TRUNC('MONTH', ts), EXTRACT(YEAR FROM ts) FROM t"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_pg_cast_rewrite_to_cast(self):
        # Verify that PostgreSQL '::' style casts are rendered as standard CAST in Netezza
        expr = parse_one("SELECT 1::INT", read="postgres")
        self.assertEqual(expr.sql(dialect="netezza"), "SELECT CAST(1 AS INT)")

    def test_type_mappings_unicode_and_tz(self):
        self.assertEqual(
            parse_one("CAST(a AS NVARCHAR(10))").sql(dialect="netezza"),
            "CAST(a AS VARCHAR(10))",
        )
        self.assertEqual(
            parse_one("CAST(a AS NCHAR(5))").sql(dialect="netezza"),
            "CAST(a AS CHAR(5))",
        )
        self.assertEqual(
            parse_one("CAST(a AS TIMESTAMPTZ)").sql(dialect="netezza"),
            "CAST(a AS TIMESTAMP)",
        )
        self.assertEqual(
            parse_one("CAST(a AS TIMETZ)").sql(dialect="netezza"),
            "CAST(a AS TIME)",
        )

    def test_indexes_unsupported(self):
        # Generator should not emit CREATE INDEX for Netezza
        sql = "CREATE INDEX IDX ON T(A)"
        expr = parse_one(sql)
        self.assertEqual(expr.sql(dialect="netezza"), "")

    def test_coalesce_roundtrip(self):
        sql = "SELECT COALESCE(A, B, 0) FROM T"
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_window_partition(self):
        sql = (
            "SELECT SUM(VAL) OVER (PARTITION BY CAT ORDER BY TS ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM T"
        )
        expr = parse_one(sql, read="netezza")
        self.assertEqual(expr.sql(dialect="netezza"), sql)

    def test_time_datetime_functions(self):
        cases = [
            "SELECT CURRENT_DATE",
            "SELECT CURRENT_TIMESTAMP",
            "SELECT EXTRACT(YEAR FROM TS) FROM T",
            "SELECT DATE_TRUNC('DAY', TS) FROM T",
            "SELECT ADD_MONTHS(DATE '2020-01-31', 1)",
            "SELECT MONTHS_BETWEEN(TS, TS2) FROM T",
            "SELECT CAST('2024-01-02' AS DATE)",
            "SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP)",
            "SELECT DATE '2020-01-01' + 7",
        ]
        for sql in cases:
            expr = parse_one(sql, read="netezza")
            # Normalize DATE literal casting behavior to generator output
            out = expr.sql(dialect="netezza")
            # Accept either DATE 'yyyy-mm-dd' or CAST('yyyy-mm-dd' AS DATE)
            self.assertTrue(
                out == sql
                or out.replace("CAST('2020-01-31' AS DATE)", "DATE '2020-01-31'") == sql
                or out.replace("CAST('2020-01-01' AS DATE)", "DATE '2020-01-01'") == sql
            )
