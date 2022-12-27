from sqlglot import ErrorLevel, UnsupportedError, transpile
from tests.dialects.test_dialect import Validator


class TestDuckDB(Validator):
    dialect = "duckdb"

    def test_time(self):
        self.validate_all(
            "EPOCH(x)",
            read={
                "presto": "TO_UNIXTIME(x)",
            },
            write={
                "bigquery": "TIME_TO_UNIX(x)",
                "duckdb": "EPOCH(x)",
                "presto": "TO_UNIXTIME(x)",
                "spark": "UNIX_TIMESTAMP(x)",
            },
        )
        self.validate_all(
            "EPOCH_MS(x)",
            write={
                "bigquery": "UNIX_TO_TIME(x / 1000)",
                "duckdb": "TO_TIMESTAMP(CAST(x / 1000 AS BIGINT))",
                "presto": "FROM_UNIXTIME(x / 1000)",
                "spark": "FROM_UNIXTIME(x / 1000)",
            },
        )
        self.validate_all(
            "STRFTIME(x, '%y-%-m-%S')",
            write={
                "bigquery": "TIME_TO_STR(x, '%y-%-m-%S')",
                "duckdb": "STRFTIME(x, '%y-%-m-%S')",
                "postgres": "TO_CHAR(x, 'YY-FMMM-SS')",
                "presto": "DATE_FORMAT(x, '%y-%c-%S')",
                "spark": "DATE_FORMAT(x, 'yy-M-ss')",
            },
        )
        self.validate_all(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            write={
                "duckdb": "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%y-%-m')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%y-%m', x)",
                "duckdb": "STRPTIME(x, '%y-%-m')",
                "presto": "DATE_PARSE(x, '%y-%c')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yy-M')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'yy-M')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP(x)",
            write={
                "duckdb": "CAST(x AS TIMESTAMP)",
                "presto": "CAST(x AS TIMESTAMP)",
                "hive": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%m/%d/%y %I:%M %p', x)",
                "duckdb": "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
                "presto": "DATE_PARSE(x, '%c/%e/%y %l:%i %p')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'M/d/yy h:mm a')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'M/d/yy h:mm a')",
            },
        )

    def test_duckdb(self):
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS table (cola INT, colb STRING) USING ICEBERG PARTITIONED BY (colb)",
            write={
                "duckdb": "CREATE TABLE IF NOT EXISTS table (cola INT, colb TEXT)",
            },
        )

        self.validate_all(
            "COL::BIGINT[]",
            write={
                "duckdb": "CAST(COL AS BIGINT[])",
                "presto": "CAST(COL AS ARRAY(BIGINT))",
                "hive": "CAST(COL AS ARRAY<BIGINT>)",
                "spark": "CAST(COL AS ARRAY<LONG>)",
                "postgres": "CAST(COL AS BIGINT[])",
                "snowflake": "CAST(COL AS ARRAY)",
            },
        )

        self.validate_all(
            "LIST_VALUE(0, 1, 2)",
            read={
                "spark": "ARRAY(0, 1, 2)",
            },
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "LIST_VALUE(0, 1, 2)",
                "presto": "ARRAY[0, 1, 2]",
                "spark": "ARRAY(0, 1, 2)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_LENGTH([0], 1) AS x",
            write={"duckdb": "SELECT ARRAY_LENGTH(LIST_VALUE(0), 1) AS x"},
        )
        self.validate_all(
            "REGEXP_MATCHES(x, y)",
            write={
                "duckdb": "REGEXP_MATCHES(x, y)",
                "presto": "REGEXP_LIKE(x, y)",
                "hive": "x RLIKE y",
                "spark": "x RLIKE y",
            },
        )
        self.validate_all(
            "STR_SPLIT(x, 'a')",
            write={
                "duckdb": "STR_SPLIT(x, 'a')",
                "presto": "SPLIT(x, 'a')",
                "hive": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
                "spark": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
            },
        )
        self.validate_all(
            "STRING_TO_ARRAY(x, 'a')",
            write={
                "duckdb": "STR_SPLIT(x, 'a')",
                "presto": "SPLIT(x, 'a')",
                "hive": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
                "spark": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
            },
        )
        self.validate_all(
            "STR_SPLIT_REGEX(x, 'a')",
            write={
                "duckdb": "STR_SPLIT_REGEX(x, 'a')",
                "presto": "REGEXP_SPLIT(x, 'a')",
                "hive": "SPLIT(x, 'a')",
                "spark": "SPLIT(x, 'a')",
            },
        )
        self.validate_all(
            "STRUCT_EXTRACT(x, 'abc')",
            write={
                "duckdb": "STRUCT_EXTRACT(x, 'abc')",
                "presto": 'x."abc"',
                "hive": "x.`abc`",
                "spark": "x.`abc`",
            },
        )
        self.validate_all(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            write={
                "duckdb": "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
                "presto": 'x."y"."abc"',
                "hive": "x.`y`.`abc`",
                "spark": "x.`y`.`abc`",
            },
        )

        self.validate_all(
            "QUANTILE(x, 0.5)",
            write={
                "duckdb": "QUANTILE(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "hive": "PERCENTILE(x, 0.5)",
                "spark": "PERCENTILE(x, 0.5)",
            },
        )

        self.validate_all(
            "CAST(x AS DATE)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "": "CAST(x AS DATE)",
            },
        )
        self.validate_all(
            "UNNEST(x)",
            read={
                "spark": "EXPLODE(x)",
            },
            write={
                "duckdb": "UNNEST(x)",
                "spark": "EXPLODE(x)",
            },
        )

        self.validate_all(
            "1d",
            write={
                "duckdb": "1 AS d",
                "spark": "1 AS d",
            },
        )
        self.validate_all(
            "CAST(1 AS DOUBLE)",
            read={
                "hive": "1d",
                "spark": "1d",
            },
        )
        self.validate_all(
            "POWER(CAST(2 AS SMALLINT), 3)",
            read={
                "hive": "POW(2S, 3)",
                "spark": "POW(2S, 3)",
            },
        )
        self.validate_all(
            "LIST_SUM(LIST_VALUE(1, 2))",
            read={
                "spark": "ARRAY_SUM(ARRAY(1, 2))",
            },
        )
        self.validate_all(
            "IF(y <> 0, x / y, NULL)",
            read={
                "bigquery": "SAFE_DIVIDE(x, y)",
            },
        )
        self.validate_all(
            "STRUCT_PACK(x := 1, y := '2')",
            write={
                "duckdb": "STRUCT_PACK(x := 1, y := '2')",
                "spark": "STRUCT(x = 1, y = '2')",
            },
        )
        self.validate_all(
            "ARRAY_SORT(x)",
            write={
                "duckdb": "ARRAY_SORT(x)",
                "presto": "ARRAY_SORT(x)",
                "hive": "SORT_ARRAY(x)",
                "spark": "SORT_ARRAY(x)",
            },
        )
        self.validate_all(
            "ARRAY_REVERSE_SORT(x)",
            write={
                "duckdb": "ARRAY_REVERSE_SORT(x)",
                "presto": "ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SORT_ARRAY(x, FALSE)",
                "spark": "SORT_ARRAY(x, FALSE)",
            },
        )
        self.validate_all(
            "LIST_REVERSE_SORT(x)",
            write={
                "duckdb": "ARRAY_REVERSE_SORT(x)",
                "presto": "ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SORT_ARRAY(x, FALSE)",
                "spark": "SORT_ARRAY(x, FALSE)",
            },
        )
        self.validate_all(
            "LIST_SORT(x)",
            write={
                "duckdb": "ARRAY_SORT(x)",
                "presto": "ARRAY_SORT(x)",
                "hive": "SORT_ARRAY(x)",
                "spark": "SORT_ARRAY(x)",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            },
        )
        self.validate_all(
            "MONTH('2021-03-01')",
            write={
                "duckdb": "MONTH('2021-03-01')",
                "presto": "MONTH('2021-03-01')",
                "hive": "MONTH('2021-03-01')",
                "spark": "MONTH('2021-03-01')",
            },
        )
        self.validate_all(
            "ARRAY_CONCAT(LIST_VALUE(1, 2), LIST_VALUE(3, 4))",
            write={
                "duckdb": "ARRAY_CONCAT(LIST_VALUE(1, 2), LIST_VALUE(3, 4))",
                "presto": "CONCAT(ARRAY[1, 2], ARRAY[3, 4])",
                "hive": "CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
                "spark": "CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
                "snowflake": "ARRAY_CAT([1, 2], [3, 4])",
                "bigquery": "ARRAY_CONCAT([1, 2], [3, 4])",
            },
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT a FROM b PIVOT(SUM(x) FOR y IN ('z', 'q'))",
                read="duckdb",
                unsupported_level=ErrorLevel.IMMEDIATE,
            )

    def test_array(self):
        self.validate_identity("ARRAY(SELECT id FROM t)")

    def test_cast(self):
        self.validate_all(
            "123::CHARACTER VARYING",
            write={
                "duckdb": "CAST(123 AS TEXT)",
            },
        )
