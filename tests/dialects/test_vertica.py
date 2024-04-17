from tests.dialects.test_dialect import Validator


class TestVertica(Validator):
    dialect = "vertica" 
    maxDiff = None

    def test_vertica(self):
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_tinyint INT64,
                col_smallint INT64,
                col_int INT64  
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_tinyint INT64, col_smallint INT64, col_int INT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_bigint INT64,
                col_decimal NUMERIC,
                col_float FLOAT64  
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_bigint INT64, col_decimal NUMERIC, col_float FLOAT64)""",
            },
        )
        self.validate_all(
            """
            CREATE TABLE test_table (
                col_double FLOAT64,
                col_boolean BOOL   
            );
            """,
            write={
                "vertica": """CREATE TABLE test_table (col_double FLOAT64, col_boolean BOOL)""",
            },
        )
