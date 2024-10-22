# from tests.dialects.test_dialect import Validator


from tests.dialects.test_dialect import Validator


class TestDb2(Validator):
    dialect = "db2"

    def test_db2(self):
        self.validate_identity(
            "SELECT * FROM ATHENA_ACCOUNTS WHERE COUNTRY_CODE = 'US' AND STATE_CODE = 'US-IN'"
        )
        self.validate_identity("SELECT CURRENT_DATE FROM SYS")
        self.validate_identity("SELECT CURRENT_TIMESTAMP FROM SYS")
        self.validate_identity("SELECT DISTINCT MASTER, IOT_DESC AS `Geo`, ORD AS `CPQ Geo`")

        self.validate_all(
            "SELECT TO_CHAR(TIMESTAMP '1999-12-01 10:00:00')",
            write={
                "postgres": "SELECT TO_CHAR(CAST('1999-12-01 10:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')",
            },
        )
        self.validate_identity("SELECT TO_DATE('August 13, 2024, 1:00 A.M.')")
        self.validate_all("SELECT DISTINCT col1, col2 FROM table")
        self.validate_all("SELECT DISTINCT col2 FROM table ORDER BY CURRENT_DATE")

        self.validate_all("SELECT * FROM MIGOS ORDER BY QUAVO")

        self.validate_identity(
            "CREATE GLOBAL TEMPORARY TABLE t AS SELECT * FROM orders WITH NO DATA"
        )
        self.validate_identity("REGEXP_REPLACE('source', 'search', 'replace')")
        self.validate_identity("TIMESTAMP(3)")
        self.validate_identity("CURRENT_TIMESTAMP")
        self.validate_identity("ALTER TABLE tbl_name DROP CONSTRAINT fk_symbol")
        self.validate_identity("SELECT x FROM t WHERE cond FETCH FIRST 10 ROWS ONLY")
