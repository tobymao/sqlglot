from tests.dialects.test_dialect import Validator


class TestVertica(Validator):
    dialect = "vertica"  # type:ignore
    maxDiff = None  # type:ignore

    def test_vertica(self):
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_tinyint INT64,
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_tinyint INT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_smallint INT64
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_smallint INT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_int INT64
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_int INT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_bigint INT64
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_bigint INT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_decimal NUMERIC
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_decimal NUMERIC)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_float FLOAT64
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_float FLOAT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_double FLOAT64
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_double FLOAT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_boolean BOOL
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_boolean BOOL)""",
            },
        )
