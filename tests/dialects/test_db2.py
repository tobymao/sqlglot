from tests.dialects.test_dialect import Validator


class TestDB2(Validator):
    dialect = "db2"

    def test_db2(self):
        # Test basic identity
        self.validate_identity("SELECT * FROM table1")
        self.validate_identity("SELECT a, b, c FROM table1")

        # Test DB2 specific data types
        self.validate_identity("CREATE TABLE t (a SMALLINT, b INT, c BIGINT)")
        self.validate_identity("CREATE TABLE t (a CHAR(10), b VARCHAR(100))")
        self.validate_identity("CREATE TABLE t (a DECIMAL(10, 2))")
        self.validate_identity("CREATE TABLE t (a TIMESTAMP)")

        # Test DAYOFWEEK and DAYOFYEAR extracts
        self.validate_all(
            "SELECT DAYOFWEEK(date_col)",
            read={
                "db2": "SELECT DAYOFWEEK(date_col)",
            },
        )

        self.validate_all(
            "SELECT DAYOFYEAR(date_col)",
            read={
                "db2": "SELECT DAYOFYEAR(date_col)",
            },
        )

        # Test POSSTR function (Db2's string position function)
        self.validate_all(
            "SELECT POSSTR(haystack, needle)",
            read={
                "db2": "SELECT POSSTR(haystack, needle)",
            },
        )

        # Test VARCHAR_FORMAT (Db2's time to string function)
        self.validate_all(
            "SELECT VARCHAR_FORMAT(timestamp_col, 'YYYY-MM-DD')",
            read={
                "db2": "SELECT VARCHAR_FORMAT(timestamp_col, 'YYYY-MM-DD')",
            },
        )
