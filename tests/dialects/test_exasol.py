from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    dialect = "exasol"
    maxDiff = None

    def test_type_mappings(self):
        self.validate_identity("CAST(x AS BLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS LONGBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS LONGTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS MEDIUMBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS MEDIUMTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TINYBLOB)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TINYTEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS VARBINARY)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS VARCHAR)", "CAST(x AS VARCHAR)")
        self.validate_identity("CAST(x AS CHAR)", "CAST(x AS CHAR)")
        self.validate_identity("CAST(x AS TINYINT)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS INT)")
        self.validate_identity("CAST(x AS MEDIUMINT)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS BIGINT)")
        self.validate_identity("CAST(x AS FLOAT)")
        self.validate_identity("CAST(x AS DOUBLE)")
        self.validate_identity("CAST(x AS DECIMAL32)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL64)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL128)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DECIMAL256)", "CAST(x AS DECIMAL)")
        self.validate_identity("CAST(x AS DATE)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS TIMESTAMP)")
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            read={
                "tsql": "CAST(x AS DATETIME2)",
            },
            write={
                "exasol": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "CAST(x AS TIMESTAMP)",
            read={
                "tsql": "CAST(x AS SMALLDATETIME)",
            },
            write={
                "exasol": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_identity("CAST(x AS BOOLEAN)")
        self.validate_identity(
            "CAST(x AS TIMESTAMPLTZ)", "CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)"
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMP(3) WITH LOCAL TIME ZONE)",
            "CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)",
        )

    def test_mod(self):
        self.validate_all(
            "SELECT MOD(x, 10)",
            read={"exasol": "SELECT MOD(x, 10)"},
            write={
                "teradata": "SELECT x MOD 10",
                "mysql": "SELECT x % 10",
                "exasol": "SELECT MOD(x, 10)",
            },
        )

    def test_bits(self):
        self.validate_all(
            "SELECT BIT_AND(x, 1)",
            read={
                "exasol": "SELECT BIT_AND(x, 1)",
                "duckdb": "SELECT x & 1",
                "presto": "SELECT BITWISE_AND(x, 1)",
                "spark": "SELECT x & 1",
            },
            write={
                "exasol": "SELECT BIT_AND(x, 1)",
                "duckdb": "SELECT x & 1",
                "hive": "SELECT x & 1",
                "presto": "SELECT BITWISE_AND(x, 1)",
                "spark": "SELECT x & 1",
            },
        )
        self.validate_all(
            "SELECT BIT_OR(x, 1)",
            read={
                "exasol": "SELECT BIT_OR(x, 1)",
                "duckdb": "SELECT x | 1",
                "presto": "SELECT BITWISE_OR(x, 1)",
                "spark": "SELECT x | 1",
            },
            write={
                "exasol": "SELECT BIT_OR(x, 1)",
                "duckdb": "SELECT x | 1",
                "hive": "SELECT x | 1",
                "presto": "SELECT BITWISE_OR(x, 1)",
                "spark": "SELECT x | 1",
            },
        )

        self.validate_all(
            "SELECT BIT_XOR(x, 1)",
            read={
                "": "SELECT x ^ 1",
                "exasol": "SELECT BIT_XOR(x, 1)",
                "bigquery": "SELECT x ^ 1",
                "presto": "SELECT BITWISE_XOR(x, 1)",
                "postgres": "SELECT x # 1",
            },
            write={
                "": "SELECT x ^ 1",
                "exasol": "SELECT BIT_XOR(x, 1)",
                "bigquery": "SELECT x ^ 1",
                "duckdb": "SELECT XOR(x, 1)",
                "presto": "SELECT BITWISE_XOR(x, 1)",
                "postgres": "SELECT x # 1",
            },
        )
        self.validate_all(
            "SELECT BIT_NOT(x)",
            read={
                "exasol": "SELECT BIT_NOT(x)",
                "duckdb": "SELECT ~x",
                "presto": "SELECT BITWISE_NOT(x)",
                "spark": "SELECT ~x",
            },
            write={
                "exasol": "SELECT BIT_NOT(x)",
                "duckdb": "SELECT ~x",
                "hive": "SELECT ~x",
                "presto": "SELECT BITWISE_NOT(x)",
                "spark": "SELECT ~x",
            },
        )
        self.validate_all(
            "SELECT BIT_LSHIFT(x, 1)",
            read={
                "exasol": "SELECT BIT_LSHIFT(x, 1)",
                "spark": "SELECT SHIFTLEFT(x, 1)",
                "duckdb": "SELECT x << 1",
                "hive": "SELECT x << 1",
            },
            write={
                "exasol": "SELECT BIT_LSHIFT(x, 1)",
                "duckdb": "SELECT x << 1",
                "presto": "SELECT BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)",
                "hive": "SELECT x << 1",
                "spark": "SELECT SHIFTLEFT(x, 1)",
            },
        )
        self.validate_all(
            "SELECT BIT_RSHIFT(x, 1)",
            read={
                "exasol": "SELECT BIT_RSHIFT(x, 1)",
                "spark": "SELECT SHIFTRIGHT(x, 1)",
                "duckdb": "SELECT x >> 1",
                "hive": "SELECT x >> 1",
            },
            write={
                "exasol": "SELECT BIT_RSHIFT(x, 1)",
                "duckdb": "SELECT x >> 1",
                "presto": "SELECT BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
                "hive": "SELECT x >> 1",
                "spark": "SELECT SHIFTRIGHT(x, 1)",
            },
        )

    def test_aggregateFunctions(self):
        self.validate_all(
            "SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department",
            read={
                "exasol": "SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department",
            },
            write={
                "exasol": "SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department",
                "duckdb": "SELECT department, ALL (age >= 30) AS EVERY FROM employee_table GROUP BY department",
            },
        )
        (
            self.validate_all(
                "SELECT VAR_POP(current_salary)",
                write={
                    "exasol": "SELECT VAR_POP(current_salary)",
                    "duckdb": "SELECT VAR_POP(current_salary)",
                    "presto": "SELECT VAR_POP(current_salary)",
                },
                read={
                    "exasol": "SELECT VAR_POP(current_salary)",
                    "duckdb": "SELECT VAR_POP(current_salary)",
                    "presto": "SELECT VAR_POP(current_salary)",
                },
            ),
        )
        self.validate_all(
            "SELECT APPROXIMATE_COUNT_DISTINCT(y)",
            read={
                "spark": "SELECT APPROX_COUNT_DISTINCT(y)",
                "exasol": "SELECT APPROXIMATE_COUNT_DISTINCT(y)",
            },
            write={
                "redshift": "SELECT APPROXIMATE COUNT(DISTINCT y)",
                "spark": "SELECT APPROX_COUNT_DISTINCT(y)",
                "exasol": "SELECT APPROXIMATE_COUNT_DISTINCT(y)",
            },
        )

    def test_stringFunctions(self):
        self.validate_identity(
            "TO_CHAR(CAST(TO_DATE(date, 'YYYYMMDD') AS TIMESTAMP), 'DY') AS day_of_week"
        )
        self.validate_identity("SELECT TO_CHAR(12345.67890, '9999999.999999999') AS TO_CHAR")
        self.validate_identity(
            "SELECT TO_CHAR(DATE '1999-12-31') AS TO_CHAR",
            "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
        )
        self.validate_identity(
            "SELECT TO_CHAR(TIMESTAMP '1999-12-31 23:59:00', 'HH24:MI:SS DD-MM-YYYY') AS TO_CHAR",
            "SELECT TO_CHAR(CAST('1999-12-31 23:59:00' AS TIMESTAMP), 'HH24:MI:SS DD-MM-YYYY') AS TO_CHAR",
        )
        self.validate_identity("SELECT TO_CHAR(12345.6789) AS TO_CHAR")
        self.validate_identity("SELECT TO_CHAR(-12345.67890, '000G000G000D000000MI') AS TO_CHAR")
        self.validate_all(
            "EDIT_DISTANCE(col1, col2)",
            read={
                "exasol": "EDIT_DISTANCE(col1, col2)",
                "bigquery": "EDIT_DISTANCE(col1, col2)",
                "clickhouse": "editDistance(col1, col2)",
                "drill": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
            },
            write={
                "exasol": "EDIT_DISTANCE(col1, col2)",
                "bigquery": "EDIT_DISTANCE(col1, col2)",
                "clickhouse": "editDistance(col1, col2)",
                "drill": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
            },
        )
        (
            self.validate_all(
                "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                write={
                    "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                    "exasol": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                    "duckdb": "REGEXP_REPLACE(subject, pattern, replacement)",
                    "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                    "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                    "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
                },
                read={
                    "exasol": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                    "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                    "spark": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)",
                },
            ),
        )
        (
            self.validate_all(
                "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                write={
                    "exasol": "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                    "redshift": "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                    "presto": "SELECT DATE_FORMAT(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                    "oracle": "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                    "postgres": "SELECT TO_CHAR(CAST('1999-12-31' AS DATE)) AS TO_CHAR",
                },
                read={
                    "exasol": "SELECT TO_CHAR(DATE '1999-12-31') AS TO_CHAR",
                },
            ),
        )
