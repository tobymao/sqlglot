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

        # Test NCHAR and NVARCHAR (should preserve, not convert to CHAR/VARCHAR)
        self.validate_identity("CREATE TABLE t (a NCHAR(10))")
        self.validate_identity("CREATE TABLE t (a NVARCHAR(100))")

        # Test Db2-specific types (should roundtrip)
        self.validate_identity("CREATE TABLE t (a DBCLOB)")
        self.validate_identity("CREATE TABLE t (a GRAPHIC(100))")
        self.validate_identity("CREATE TABLE t (a VARGRAPHIC(100))")

        # Test that CHAR and NCHAR are distinct
        self.validate_identity("CREATE TABLE t (a CHAR(10), b NCHAR(10))")
        self.validate_identity("CREATE TABLE t (a VARCHAR(100), b NVARCHAR(100))")

    def test_null_ordering(self):
        """Test NULL_ORDERING = 'nulls_are_large' - NULLs sort last in ASC, first in DESC"""
        # ASC: NULLs should be last (no explicit NULLS FIRST/LAST needed)
        self.validate_identity("SELECT * FROM t ORDER BY x ASC")
        self.validate_identity("SELECT * FROM t ORDER BY x")

        # DESC: NULLs should be first (no explicit NULLS FIRST/LAST needed)
        self.validate_identity("SELECT * FROM t ORDER BY x DESC")

        # Explicit NULLS FIRST/LAST should be preserved
        self.validate_identity("SELECT * FROM t ORDER BY x ASC NULLS FIRST")
        self.validate_identity("SELECT * FROM t ORDER BY x DESC NULLS LAST")

    def test_typed_division(self):
        """Test TYPED_DIVISION = True - integer division returns integer"""
        # Integer division should remain as-is (Db2 does typed division)
        self.validate_identity("SELECT 5 / 2")
        self.validate_identity("SELECT a / b FROM t")

        # Decimal division
        self.validate_identity("SELECT 5.0 / 2.0")
        self.validate_identity("SELECT CAST(5 AS DECIMAL) / CAST(2 AS DECIMAL)")

    def test_strip_modifiers(self):
        """Test AFTER_HAVING_MODIFIER_TRANSFORMS - strips CLUSTER/DISTRIBUTE/SORT BY"""
        # These Hive/Spark clauses should be stripped when generating Db2 SQL
        self.validate_all(
            "SELECT * FROM t CLUSTER BY x",
            write={
                "db2": "SELECT * FROM t",
                "spark": "SELECT * FROM t CLUSTER BY x NULLS LAST",
            },
        )

        self.validate_all(
            "SELECT * FROM t DISTRIBUTE BY x",
            write={
                "db2": "SELECT * FROM t",
                "spark": "SELECT * FROM t DISTRIBUTE BY x NULLS LAST",
            },
        )

        self.validate_all(
            "SELECT * FROM t SORT BY x",
            write={
                "db2": "SELECT * FROM t",
                "spark": "SELECT * FROM t SORT BY x NULLS LAST",
            },
        )

        # Multiple modifiers
        self.validate_all(
            "SELECT * FROM t CLUSTER BY y DISTRIBUTE BY x SORT BY z",
            write={
                "db2": "SELECT * FROM t",
                "spark": "SELECT * FROM t CLUSTER BY y NULLS LAST DISTRIBUTE BY x NULLS LAST SORT BY z NULLS LAST",
            },
        )

    def test_type_transpilation(self):
        """Test type transpilation between Db2 and other dialects"""
        # NCHAR/NVARCHAR: Db2 preserves, other dialects convert to CHAR/VARCHAR
        self.validate_all(
            "CREATE TABLE t (a NCHAR(10))",
            write={
                "db2": "CREATE TABLE t (a NCHAR(10))",
                "postgres": "CREATE TABLE t (a CHAR(10))",
                "mysql": "CREATE TABLE t (a CHAR(10))",
                "snowflake": "CREATE TABLE t (a CHAR(10))",
            },
        )

        self.validate_all(
            "CREATE TABLE t (a NVARCHAR(100))",
            write={
                "db2": "CREATE TABLE t (a NVARCHAR(100))",
                "postgres": "CREATE TABLE t (a VARCHAR(100))",
                "mysql": "CREATE TABLE t (a VARCHAR(100))",
                "snowflake": "CREATE TABLE t (a VARCHAR(100))",
            },
        )

        # GRAPHIC/VARGRAPHIC/DBCLOB: Db2-specific, preserved in other dialects
        self.validate_all(
            "CREATE TABLE t (a GRAPHIC(10))",
            write={
                "db2": "CREATE TABLE t (a GRAPHIC(10))",
                "postgres": "CREATE TABLE t (a GRAPHIC(10))",
                "mysql": "CREATE TABLE t (a GRAPHIC(10))",
            },
        )

        self.validate_all(
            "CREATE TABLE t (a VARGRAPHIC(100))",
            write={
                "db2": "CREATE TABLE t (a VARGRAPHIC(100))",
                "postgres": "CREATE TABLE t (a VARGRAPHIC(100))",
                "mysql": "CREATE TABLE t (a VARGRAPHIC(100))",
            },
        )

        self.validate_all(
            "CREATE TABLE t (a DBCLOB)",
            write={
                "db2": "CREATE TABLE t (a DBCLOB)",
                "postgres": "CREATE TABLE t (a DBCLOB)",
                "mysql": "CREATE TABLE t (a DBCLOB)",
            },
        )

    def test_variable_tokens(self):
        """Test VAR_SINGLE_TOKENS = {'@'} - Db2 uses @ for variables"""
        self.validate_identity("SELECT @var")
        self.validate_identity("SET @var = 1")
