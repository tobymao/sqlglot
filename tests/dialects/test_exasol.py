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
        self.validate_identity("CAST(x AS TEXT)", "CAST(x AS LONG VARCHAR)")
        self.validate_identity(
            "SELECT CAST((CAST(202305 AS INT) - 100) AS LONG VARCHAR) AS CAL_YEAR_WEEK_ADJUSTED"
        )
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

        self.validate_identity(
            "SELECT id, department, hire_date, GROUP_CONCAT(id ORDER BY hire_date SEPARATOR ',') OVER (PARTITION BY department rows between 1 preceding and 1 following) GROUP_CONCAT_RESULT from employee_table ORDER BY department, hire_date",
            "SELECT id, department, hire_date, LISTAGG(id, ',') WITHIN GROUP (ORDER BY hire_date) OVER (PARTITION BY department rows BETWEEN 1 preceding AND 1 following) AS GROUP_CONCAT_RESULT FROM employee_table ORDER BY department, hire_date",
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT x ORDER BY y DESC)",
            write={
                "exasol": "LISTAGG(DISTINCT x, ',') WITHIN GROUP (ORDER BY y DESC)",
                "mysql": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR ',')",
                "tsql": "STRING_AGG(x, ',') WITHIN GROUP (ORDER BY y DESC)",
                "databricks": "LISTAGG(DISTINCT x, ',') WITHIN GROUP (ORDER BY y DESC)",
            },
        )
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
        self.validate_all(
            "STRPOS(haystack, needle)",
            write={
                "exasol": "INSTR(haystack, needle)",
                "bigquery": "INSTR(haystack, needle)",
                "databricks": "LOCATE(needle, haystack)",
                "oracle": "INSTR(haystack, needle)",
                "presto": "STRPOS(haystack, needle)",
            },
        )
        self.validate_all(
            r"SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL",
            write={
                "exasol": r"SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL",
                "bigquery": r"SELECT REGEXP_EXTRACT('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}') AS EMAIL",
                "snowflake": r"SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}') AS EMAIL",
                "presto": r"SELECT REGEXP_EXTRACT('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL",
            },
        )

    def test_datetime_functions(self):
        formats = {
            "HH12": "hour_12",
            "HH24": "hour_24",
            "ID": "iso_weekday",
            "IW": "iso_week_number",
            "uW": "week_number_uW",
            "VW": "week_number_VW",
            "IYYY": "iso_year",
            "MI": "minutes",
            "SS": "seconds",
            "DAY": "day_full",
            "DY": "day_abbr",
        }
        self.validate_identity(
            "SELECT TO_DATE('31-12-1999', 'dd-mm-yyyy') AS TO_DATE",
            "SELECT TO_DATE('31-12-1999', 'DD-MM-YYYY') AS TO_DATE",
        )
        self.validate_identity(
            "SELECT TO_DATE('31-12-1999', 'dd-mm-YY') AS TO_DATE",
            "SELECT TO_DATE('31-12-1999', 'DD-MM-YY') AS TO_DATE",
        )
        self.validate_identity("SELECT TO_DATE('31-DECEMBER-1999', 'DD-MONTH-YYYY') AS TO_DATE")
        self.validate_identity("SELECT TO_DATE('31-DEC-1999', 'DD-MON-YYYY') AS TO_DATE")
        self.validate_identity("SELECT WEEKOFYEAR('2024-05-22')", "SELECT WEEK('2024-05-22')")

        for fmt, alias in formats.items():
            with self.subTest(f"Testing TO_CHAR with format '{fmt}'"):
                self.validate_identity(
                    f"SELECT TO_CHAR(CAST('2024-07-08 13:45:00' AS TIMESTAMP), '{fmt}') AS {alias}"
                )

        self.validate_all(
            "TO_DATE(x, 'YYYY-MM-DD')",
            write={
                "exasol": "TO_DATE(x, 'YYYY-MM-DD')",
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(CAST(x AS TIMESTAMP) AS DATE)",
                "spark": "TO_DATE(x)",
                "snowflake": "TO_DATE(x, 'yyyy-mm-DD')",
                "databricks": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "TO_DATE(x, 'YYYY')",
            write={
                "exasol": "TO_DATE(x, 'YYYY')",
                "duckdb": "CAST(STRPTIME(x, '%Y') AS DATE)",
                "hive": "TO_DATE(x, 'yyyy')",
                "presto": "CAST(DATE_PARSE(x, '%Y') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy')",
                "snowflake": "TO_DATE(x, 'yyyy')",
                "databricks": "TO_DATE(x, 'yyyy')",
            },
        )
        self.validate_identity(
            "SELECT CONVERT_TZ(CAST('2012-03-25 02:30:00' AS TIMESTAMP), 'Europe/Berlin', 'UTC', 'INVALID REJECT AMBIGUOUS REJECT') AS CONVERT_TZ"
        )
        self.validate_all(
            "SELECT CONVERT_TZ('2012-05-10 12:00:00', 'Europe/Berlin', 'America/New_York')",
            read={
                "exasol": "SELECT CONVERT_TZ('2012-05-10 12:00:00', 'Europe/Berlin', 'America/New_York')",
                "mysql": "SELECT CONVERT_TZ('2012-05-10 12:00:00', 'Europe/Berlin', 'America/New_York')",
                "databricks": "SELECT CONVERT_TIMEZONE('Europe/Berlin', 'America/New_York', '2012-05-10 12:00:00')",
            },
            write={
                "exasol": "SELECT CONVERT_TZ('2012-05-10 12:00:00', 'Europe/Berlin', 'America/New_York')",
                "mysql": "SELECT CONVERT_TZ('2012-05-10 12:00:00', 'Europe/Berlin', 'America/New_York')",
                "databricks": "SELECT CONVERT_TIMEZONE('Europe/Berlin', 'America/New_York', '2012-05-10 12:00:00')",
                "snowflake": "SELECT CONVERT_TIMEZONE('Europe/Berlin', 'America/New_York', '2012-05-10 12:00:00')",
                "spark": "SELECT CONVERT_TIMEZONE('Europe/Berlin', 'America/New_York', '2012-05-10 12:00:00')",
                "redshift": "SELECT CONVERT_TIMEZONE('Europe/Berlin', 'America/New_York', '2012-05-10 12:00:00')",
                "duckdb": "SELECT CAST('2012-05-10 12:00:00' AS TIMESTAMP) AT TIME ZONE 'Europe/Berlin' AT TIME ZONE 'America/New_York'",
            },
        )
        self.validate_identity(
            "TIME_TO_STR(b, '%Y-%m-%d %H:%M:%S')",
            "TO_CHAR(b, 'YYYY-MM-DD HH:MI:SS')",
        )
        self.validate_identity(
            "SELECT TIME_TO_STR(CAST(STR_TO_TIME(date, '%Y%m%d') AS DATE), '%a') AS day_of_week",
            "SELECT TO_CHAR(CAST(TO_DATE(date, 'YYYYMMDD') AS DATE), 'DY') AS day_of_week",
        )
        self.validate_identity(
            "SELECT CAST(CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AT TIME ZONE 'CET' AS DATE) - 1",
            "SELECT CAST(CONVERT_TZ(CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), 'UTC', 'CET') AS DATE) - 1",
        )

        self.validate_all(
            "SELECT TRUNC(CAST('2006-12-31' AS DATE), 'MM') AS TRUNC",
            write={
                "exasol": "SELECT TRUNC(CAST('2006-12-31' AS DATE), 'MM') AS TRUNC",
                "presto": "SELECT DATE_TRUNC('MM', CAST('2006-12-31' AS DATE)) AS TRUNC",
                "databricks": "SELECT TRUNC(CAST('2006-12-31' AS DATE), 'MM') AS TRUNC",
            },
        )
        self.validate_all(
            "SELECT DATE_TRUNC('minute', TIMESTAMP '2006-12-31 23:59:59') DATE_TRUNC",
            write={
                "exasol": "SELECT DATE_TRUNC('MINUTE', CAST('2006-12-31 23:59:59' AS TIMESTAMP)) AS DATE_TRUNC",
                "presto": "SELECT DATE_TRUNC('MINUTE', CAST('2006-12-31 23:59:59' AS TIMESTAMP)) AS DATE_TRUNC",
                "databricks": "SELECT DATE_TRUNC('MINUTE', CAST('2006-12-31 23:59:59' AS TIMESTAMP)) AS DATE_TRUNC",
            },
        )

        from sqlglot.dialects.exasol import DATE_UNITS

        for unit in DATE_UNITS:
            with self.subTest(f"Testing ADD_{unit}S"):
                self.validate_all(
                    f"SELECT ADD_{unit}S(DATE '2000-02-28', 1)",
                    write={
                        "exasol": f"SELECT ADD_{unit}S(CAST('2000-02-28' AS DATE), 1)",
                        "bigquery": f"SELECT DATE_ADD(CAST('2000-02-28' AS DATE), INTERVAL 1 {unit})",
                        "duckdb": f"SELECT CAST('2000-02-28' AS DATE) + INTERVAL 1 {unit}",
                        "presto": f"SELECT DATE_ADD('{unit}', 1, CAST('2000-02-28' AS DATE))",
                        "redshift": f"SELECT DATEADD({unit}, 1, CAST('2000-02-28' AS DATE))",
                        "snowflake": f"SELECT DATEADD({unit}, 1, CAST('2000-02-28' AS DATE))",
                        "tsql": f"SELECT DATEADD({unit}, 1, CAST('2000-02-28' AS DATE))",
                    },
                )

            with self.subTest(f"Testing {unit}S_BETWEEN"):
                self.validate_all(
                    f"SELECT {unit}S_BETWEEN(TIMESTAMP '2000-02-28 00:00:00', CURRENT_TIMESTAMP)",
                    write={
                        "exasol": f"SELECT {unit}S_BETWEEN(CAST('2000-02-28 00:00:00' AS TIMESTAMP), CURRENT_TIMESTAMP())",
                        "bigquery": f"SELECT DATE_DIFF(CAST('2000-02-28 00:00:00' AS DATETIME), CURRENT_TIMESTAMP(), {unit})",
                        "duckdb": f"SELECT DATE_DIFF('{unit}', CURRENT_TIMESTAMP, CAST('2000-02-28 00:00:00' AS TIMESTAMP))",
                        "presto": f"SELECT DATE_DIFF('{unit}', CURRENT_TIMESTAMP, CAST('2000-02-28 00:00:00' AS TIMESTAMP))",
                        "redshift": f"SELECT DATEDIFF({unit}, GETDATE(), CAST('2000-02-28 00:00:00' AS TIMESTAMP))",
                        "snowflake": f"SELECT DATEDIFF({unit}, CURRENT_TIMESTAMP(), CAST('2000-02-28 00:00:00' AS TIMESTAMP))",
                        "tsql": f"SELECT DATEDIFF({unit}, GETDATE(), CAST('2000-02-28 00:00:00' AS DATETIME2))",
                    },
                )

    def test_number_functions(self):
        self.validate_identity("SELECT TRUNC(123.456, 2) AS TRUNC")
        self.validate_identity("SELECT DIV(1234, 2) AS DIV")

    def test_scalar(self):
        self.validate_all(
            "SELECT CURRENT_USER",
            read={
                "exasol": "SELECT USER",
                "spark": "SELECT CURRENT_USER()",
                "trino": "SELECT CURRENT_USER",
                "snowflake": "SELECT CURRENT_USER()",
            },
            write={
                "exasol": "SELECT CURRENT_USER",
                "spark": "SELECT CURRENT_USER()",
                "trino": "SELECT CURRENT_USER",
                "snowflake": "SELECT CURRENT_USER()",
            },
        )
        self.validate_all(
            'CREATE OR REPLACE VIEW "schema"."v" ("col" COMMENT IS \'desc\') AS SELECT "src_col" AS "col"',
            write={
                "databricks": "CREATE OR REPLACE VIEW `schema`.`v` (`col` COMMENT 'desc') AS SELECT `src_col` AS `col`",
                "exasol": 'CREATE OR REPLACE VIEW "schema"."v" ("col" COMMENT IS \'desc\') AS SELECT "src_col" AS "col"',
            },
        )
        self.validate_all(
            "HASH_SHA(x)",
            read={
                "clickhouse": "SHA1(x)",
                "exasol": "HASH_SHA1(x)",
                "presto": "SHA1(x)",
                "trino": "SHA1(x)",
            },
            write={
                "exasol": "HASH_SHA(x)",
                "clickhouse": "SHA1(x)",
                "bigquery": "SHA1(x)",
                "": "SHA(x)",
                "presto": "SHA1(x)",
                "trino": "SHA1(x)",
            },
        )
        self.validate_all(
            "HASH_MD5(x)",
            write={
                "exasol": "HASH_MD5(x)",
                "": "MD5(x)",
                "bigquery": "TO_HEX(MD5(x))",
                "clickhouse": "LOWER(HEX(MD5(x)))",
                "hive": "MD5(x)",
                "presto": "LOWER(TO_HEX(MD5(x)))",
                "spark": "MD5(x)",
                "trino": "LOWER(TO_HEX(MD5(x)))",
            },
        )
        self.validate_all(
            "HASHTYPE_MD5(x)",
            write={
                "exasol": "HASHTYPE_MD5(x)",
                "": "MD5_DIGEST(x)",
                "bigquery": "MD5(x)",
                "clickhouse": "MD5(x)",
                "hive": "UNHEX(MD5(x))",
                "presto": "MD5(x)",
                "spark": "UNHEX(MD5(x))",
                "trino": "MD5(x)",
            },
        )

        self.validate_all(
            "HASH_SHA256(x)",
            read={
                "clickhouse": "SHA256(x)",
                "presto": "SHA256(x)",
                "trino": "SHA256(x)",
                "postgres": "SHA256(x)",
                "duckdb": "SHA256(x)",
            },
            write={
                "exasol": "HASH_SHA256(x)",
                "bigquery": "SHA256(x)",
                "spark2": "SHA2(x, 256)",
                "clickhouse": "SHA256(x)",
                "postgres": "SHA256(x)",
                "presto": "SHA256(x)",
                "redshift": "SHA2(x, 256)",
                "trino": "SHA256(x)",
                "duckdb": "SHA256(x)",
                "snowflake": "SHA2(x, 256)",
            },
        )
        self.validate_all(
            "HASH_SHA512(x)",
            read={
                "clickhouse": "SHA512(x)",
                "presto": "SHA512(x)",
                "trino": "SHA512(x)",
            },
            write={
                "exasol": "HASH_SHA512(x)",
                "clickhouse": "SHA512(x)",
                "bigquery": "SHA512(x)",
                "spark2": "SHA2(x, 512)",
                "presto": "SHA512(x)",
                "trino": "SHA512(x)",
            },
        )
        self.validate_all(
            "SELECT NULLIFZERO(1) NIZ1",
            write={
                "exasol": "SELECT IF 1 = 0 THEN NULL ELSE 1 ENDIF AS NIZ1",
                "snowflake": "SELECT IFF(1 = 0, NULL, 1) AS NIZ1",
                "sqlite": "SELECT IIF(1 = 0, NULL, 1) AS NIZ1",
                "presto": "SELECT IF(1 = 0, NULL, 1) AS NIZ1",
                "spark": "SELECT IF(1 = 0, NULL, 1) AS NIZ1",
                "hive": "SELECT IF(1 = 0, NULL, 1) AS NIZ1",
                "duckdb": "SELECT CASE WHEN 1 = 0 THEN NULL ELSE 1 END AS NIZ1",
            },
        )
        self.validate_all(
            "SELECT ZEROIFNULL(NULL) NIZ1",
            write={
                "exasol": "SELECT IF NULL IS NULL THEN 0 ELSE NULL ENDIF AS NIZ1",
                "snowflake": "SELECT IFF(NULL IS NULL, 0, NULL) AS NIZ1",
                "sqlite": "SELECT IIF(NULL IS NULL, 0, NULL) AS NIZ1",
                "presto": "SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1",
                "spark": "SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1",
                "hive": "SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1",
                "duckdb": "SELECT CASE WHEN NULL IS NULL THEN 0 ELSE NULL END AS NIZ1",
            },
        )
        self.validate_identity(
            "SELECT name, age, IF age < 18 THEN 'underaged' ELSE 'adult' ENDIF AS LEGALITY FROM persons"
        )
