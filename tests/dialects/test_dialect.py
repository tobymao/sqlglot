import unittest

from sqlglot import (
    Dialect,
    Dialects,
    ErrorLevel,
    UnsupportedError,
    parse_one,
    transpile,
)


class Validator(unittest.TestCase):
    dialect = None

    def validate_identity(self, sql):
        self.assertEqual(transpile(sql, read=self.dialect, write=self.dialect)[0], sql)

    def validate_all(self, sql, read=None, write=None, pretty=False):
        """
        Validate that:
        1. Everything in `read` transpiles to `sql`
        2. `sql` transpiles to everything in `write`

        Args:
            sql (str): Main SQL expression
            dialect (str): dialect of `sql`
            read (dict): Mapping of dialect -> SQL
            write (dict): Mapping of dialect -> SQL
        """
        expression = parse_one(sql, read=self.dialect)

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                self.assertEqual(
                    parse_one(read_sql, read_dialect).sql(self.dialect, unsupported_level=ErrorLevel.IGNORE),
                    sql,
                )

        for write_dialect, write_sql in (write or {}).items():
            with self.subTest(f"{sql} -> {write_dialect}"):
                if write_sql is UnsupportedError:
                    with self.assertRaises(UnsupportedError):
                        expression.sql(write_dialect, unsupported_level=ErrorLevel.RAISE)
                else:
                    self.assertEqual(
                        expression.sql(
                            write_dialect,
                            unsupported_level=ErrorLevel.IGNORE,
                            pretty=pretty,
                        ),
                        write_sql,
                    )


class TestDialect(Validator):
    maxDiff = None

    def test_enum(self):
        for dialect in Dialects:
            self.assertIsNotNone(Dialect[dialect])
            self.assertIsNotNone(Dialect.get(dialect))
            self.assertIsNotNone(Dialect.get_or_raise(dialect))
            self.assertIsNotNone(Dialect[dialect.value])

    def test_cast(self):
        self.validate_all(
            "CAST(a AS TEXT)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "clickhouse": "CAST(a AS TEXT)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS TEXT)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS TEXT)",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(MAP('a', '1') AS MAP(TEXT, TEXT))",
            write={
                "clickhouse": "CAST(map('a', '1') AS Map(TEXT, TEXT))",
            },
        )
        self.validate_all(
            "CAST(ARRAY(1, 2) AS ARRAY<TINYINT>)",
            write={
                "clickhouse": "CAST([1, 2] AS Array(Int8))",
            },
        )
        self.validate_all(
            "CAST((1, 2) AS STRUCT<a: TINYINT, b: SMALLINT, c: INT, d: BIGINT>)",
            write={
                "clickhouse": "CAST((1, 2) AS Tuple(a Int8, b Int16, c Int32, d Int64))",
            },
        )
        self.validate_all(
            "CAST(a AS DATETIME)",
            write={
                "postgres": "CAST(a AS TIMESTAMP)",
                "sqlite": "CAST(a AS DATETIME)",
            },
        )
        self.validate_all(
            "CAST(a AS STRING)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS TEXT)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS TEXT)",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS VARCHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS VARCHAR2)",
                "postgres": "CAST(a AS VARCHAR)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR)",
                "snowflake": "CAST(a AS VARCHAR)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS VARCHAR)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR(3))",
            write={
                "bigquery": "CAST(a AS STRING(3))",
                "duckdb": "CAST(a AS TEXT(3))",
                "mysql": "CAST(a AS VARCHAR(3))",
                "hive": "CAST(a AS VARCHAR(3))",
                "oracle": "CAST(a AS VARCHAR2(3))",
                "postgres": "CAST(a AS VARCHAR(3))",
                "presto": "CAST(a AS VARCHAR(3))",
                "redshift": "CAST(a AS VARCHAR(3))",
                "snowflake": "CAST(a AS VARCHAR(3))",
                "spark": "CAST(a AS VARCHAR(3))",
                "starrocks": "CAST(a AS VARCHAR(3))",
            },
        )
        self.validate_all(
            "CAST(a AS SMALLINT)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "duckdb": "CAST(a AS SMALLINT)",
                "mysql": "CAST(a AS SMALLINT)",
                "hive": "CAST(a AS SMALLINT)",
                "oracle": "CAST(a AS NUMBER)",
                "postgres": "CAST(a AS SMALLINT)",
                "presto": "CAST(a AS SMALLINT)",
                "redshift": "CAST(a AS SMALLINT)",
                "snowflake": "CAST(a AS SMALLINT)",
                "spark": "CAST(a AS SHORT)",
                "sqlite": "CAST(a AS INTEGER)",
                "starrocks": "CAST(a AS SMALLINT)",
            },
        )
        self.validate_all(
            "TRY_CAST(a AS DOUBLE)",
            read={
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
            },
            write={
                "duckdb": "TRY_CAST(a AS DOUBLE)",
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
            },
        )

        self.validate_all(
            "CAST(a AS DOUBLE)",
            write={
                "bigquery": "CAST(a AS FLOAT64)",
                "clickhouse": "CAST(a AS Float64)",
                "duckdb": "CAST(a AS DOUBLE)",
                "mysql": "CAST(a AS DOUBLE)",
                "hive": "CAST(a AS DOUBLE)",
                "oracle": "CAST(a AS DOUBLE PRECISION)",
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "presto": "CAST(a AS DOUBLE)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
                "snowflake": "CAST(a AS DOUBLE)",
                "spark": "CAST(a AS DOUBLE)",
                "starrocks": "CAST(a AS DOUBLE)",
            },
        )
        self.validate_all(
            "CAST('1 DAY' AS INTERVAL)",
            write={
                "postgres": "CAST('1 DAY' AS INTERVAL)",
                "redshift": "CAST('1 DAY' AS INTERVAL)",
            },
        )
        self.validate_all(
            "CAST(a AS TIMESTAMP)",
            write={
                "starrocks": "CAST(a AS DATETIME)",
                "redshift": "CAST(a AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "CAST(a AS TIMESTAMPTZ)",
            write={
                "starrocks": "CAST(a AS DATETIME)",
                "redshift": "CAST(a AS TIMESTAMPTZ)",
            },
        )
        self.validate_all("CAST(a AS TINYINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS SMALLINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS BIGINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS INT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all(
            "CAST(a AS DECIMAL)",
            read={"oracle": "CAST(a AS NUMBER)"},
            write={"oracle": "CAST(a AS NUMBER)"},
        )

    def test_time(self):
        self.validate_all(
            "STR_TO_TIME(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
            },
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS TIMESTAMP)",
                "presto": "DATE_PARSE(x, '%Y-%m-%dT%H:%i:%S')",
                "redshift": "TO_TIMESTAMP(x, 'YYYY-MM-DDTHH:MI:SS')",
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            write={
                "duckdb": "STRPTIME('2020-01-01', '%Y-%m-%d')",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "oracle": "TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD')",
                "postgres": "TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD')",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d')",
                "redshift": "TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD')",
                "spark": "TO_TIMESTAMP('2020-01-01', 'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "STR_TO_TIME(x, '%y')",
            write={
                "duckdb": "STRPTIME(x, '%y')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yy')) AS TIMESTAMP)",
                "presto": "DATE_PARSE(x, '%y')",
                "oracle": "TO_TIMESTAMP(x, 'YY')",
                "postgres": "TO_TIMESTAMP(x, 'YY')",
                "redshift": "TO_TIMESTAMP(x, 'YY')",
                "spark": "TO_TIMESTAMP(x, 'yy')",
            },
        )
        self.validate_all(
            "STR_TO_UNIX('2020-01-01', '%Y-%M-%d')",
            write={
                "duckdb": "EPOCH(STRPTIME('2020-01-01', '%Y-%M-%d'))",
                "hive": "UNIX_TIMESTAMP('2020-01-01', 'yyyy-mm-dd')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%i-%d'))",
                "starrocks": "UNIX_TIMESTAMP('2020-01-01', '%Y-%i-%d')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_DATE('2020-01-01')",
            write={
                "duckdb": "CAST('2020-01-01' AS DATE)",
                "hive": "TO_DATE('2020-01-01')",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%s')",
                "starrocks": "TO_DATE('2020-01-01')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01')",
            write={
                "duckdb": "CAST('2020-01-01' AS TIMESTAMP)",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%s')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_UNIX('2020-01-01')",
            write={
                "duckdb": "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
                "hive": "UNIX_TIMESTAMP('2020-01-01')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%S'))",
            },
        )
        self.validate_all(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            write={
                "duckdb": "STRFTIME(x, '%Y-%m-%d')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd')",
                "oracle": "TO_CHAR(x, 'YYYY-MM-DD')",
                "postgres": "TO_CHAR(x, 'YYYY-MM-DD')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d')",
                "redshift": "TO_CHAR(x, 'YYYY-MM-DD')",
            },
        )
        self.validate_all(
            "TIME_TO_TIME_STR(x)",
            write={
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
                "redshift": "CAST(x AS TEXT)",
            },
        )
        self.validate_all(
            "TIME_TO_UNIX(x)",
            write={
                "duckdb": "EPOCH(x)",
                "hive": "UNIX_TIMESTAMP(x)",
                "presto": "TO_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE_STR(x)",
            write={
                "duckdb": "SUBSTRING(CAST(x AS TEXT), 1, 10)",
                "hive": "SUBSTRING(CAST(x AS STRING), 1, 10)",
                "presto": "SUBSTRING(CAST(x AS VARCHAR), 1, 10)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(SUBSTR(CAST(x AS VARCHAR), 1, 10) AS DATE)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE(x, '%-d')",
            write={
                "duckdb": "CAST(STRPTIME(x, '%-d') AS DATE)",
                "hive": "TO_DATE(x, 'd')",
                "presto": "CAST(DATE_PARSE(x, '%e') AS DATE)",
                "spark": "TO_DATE(x, 'd')",
            },
        )
        self.validate_all(
            "UNIX_TO_STR(x, y)",
            write={
                "duckdb": "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), y)",
                "hive": "FROM_UNIXTIME(x, y)",
                "presto": "DATE_FORMAT(FROM_UNIXTIME(x), y)",
                "starrocks": "FROM_UNIXTIME(x, y)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME(x)",
            write={
                "duckdb": "TO_TIMESTAMP(CAST(x AS BIGINT))",
                "hive": "FROM_UNIXTIME(x)",
                "oracle": "TO_DATE('1970-01-01','YYYY-MM-DD') + (x / 86400)",
                "postgres": "TO_TIMESTAMP(x)",
                "presto": "FROM_UNIXTIME(x)",
                "starrocks": "FROM_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME_STR(x)",
            write={
                "duckdb": "CAST(TO_TIMESTAMP(CAST(x AS BIGINT)) AS TEXT)",
                "hive": "FROM_UNIXTIME(x)",
                "presto": "CAST(FROM_UNIXTIME(x) AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DATE_STR(x)",
            write={
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DI(x)",
            write={
                "duckdb": "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
                "hive": "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
                "presto": "CAST(DATE_FORMAT(x, '%Y%m%d') AS INT)",
            },
        )
        self.validate_all(
            "DI_TO_DATE(x)",
            write={
                "duckdb": "CAST(STRPTIME(CAST(x AS TEXT), '%Y%m%d') AS DATE)",
                "hive": "TO_DATE(CAST(x AS STRING), 'yyyyMMdd')",
                "presto": "CAST(DATE_PARSE(CAST(x AS VARCHAR), '%Y%m%d') AS DATE)",
            },
        )
        self.validate_all(
            "TS_OR_DI_TO_DI(x)",
            write={
                "duckdb": "CAST(SUBSTR(REPLACE(CAST(x AS TEXT), '-', ''), 1, 8) AS INT)",
                "hive": "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
                "presto": "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
                "spark": "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            },
        )
        self.validate_all(
            "DATE_ADD(x, 1, 'day')",
            read={
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 'day')",
                "duckdb": "x + INTERVAL 1 day",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "postgres": "x + INTERVAL '1' 'day'",
                "presto": "DATE_ADD('day', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_ADD(x, y, 'day')",
            write={
                "postgres": UnsupportedError,
            },
        )
        self.validate_all(
            "DATE_ADD(x, 1)",
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 'day')",
                "duckdb": "x + INTERVAL 1 DAY",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "presto": "DATE_ADD('day', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'day')",
            write={
                "mysql": "DATE(x)",
                "starrocks": "DATE(x)",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'week')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'month')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'quarter')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'year')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'millenium')",
            write={
                "mysql": UnsupportedError,
                "starrocks": UnsupportedError,
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
            },
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%dT%H:%i:%S') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%d')",
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%d')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%d')",
                "hive": "CAST(x AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)",
                "spark": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "DATE_STR_TO_DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)",
                "spark": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "TS_OR_DS_ADD('2021-02-01', 1, 'DAY')",
            write={
                "duckdb": "CAST('2021-02-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD('2021-02-01', 1)",
                "presto": "DATE_ADD('DAY', 1, DATE_PARSE(SUBSTR('2021-02-01', 1, 10), '%Y-%m-%d'))",
                "spark": "DATE_ADD('2021-02-01', 1)",
            },
        )
        self.validate_all(
            "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            write={
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
                "presto": "DATE_ADD('day', 1, CAST('2020-01-01' AS DATE))",
                "spark": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            },
        )

        for unit in ("DAY", "MONTH", "YEAR"):
            self.validate_all(
                f"{unit}(x)",
                read={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "bigquery",
                        "duckdb",
                        "mysql",
                        "presto",
                        "starrocks",
                    )
                },
                write={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "bigquery",
                        "duckdb",
                        "mysql",
                        "presto",
                        "hive",
                        "spark",
                        "starrocks",
                    )
                },
            )

    def test_array(self):
        self.validate_all(
            "ARRAY(0, 1, 2)",
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "LIST_VALUE(0, 1, 2)",
                "presto": "ARRAY[0, 1, 2]",
                "spark": "ARRAY(0, 1, 2)",
            },
        )
        self.validate_all(
            "ARRAY_SIZE(x)",
            write={
                "bigquery": "ARRAY_LENGTH(x)",
                "duckdb": "ARRAY_LENGTH(x)",
                "presto": "CARDINALITY(x)",
                "spark": "SIZE(x)",
            },
        )
        self.validate_all(
            "ARRAY_SUM(ARRAY(1, 2))",
            write={
                "trino": "REDUCE(ARRAY[1, 2], 0, (acc, x) -> acc + x, acc -> acc)",
                "duckdb": "LIST_SUM(LIST_VALUE(1, 2))",
                "hive": "ARRAY_SUM(ARRAY(1, 2))",
                "presto": "ARRAY_SUM(ARRAY[1, 2])",
                "spark": "AGGREGATE(ARRAY(1, 2), 0, (acc, x) -> acc + x, acc -> acc)",
            },
        )
        self.validate_all(
            "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            write={
                "trino": "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
                "duckdb": "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
                "hive": "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
                "presto": "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
                "spark": "AGGREGATE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            },
        )

    def test_order_by(self):
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "bigquery": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "oracle": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            },
        )

    def test_json(self):
        self.validate_all(
            "JSON_EXTRACT(x, 'y')",
            read={
                "postgres": "x->'y'",
                "presto": "JSON_EXTRACT(x, 'y')",
                "starrocks": "x->'y'",
            },
            write={
                "oracle": "JSON_EXTRACT(x, 'y')",
                "postgres": "x->'y'",
                "presto": "JSON_EXTRACT(x, 'y')",
                "starrocks": "x->'y'",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_SCALAR(x, 'y')",
            read={
                "postgres": "x->>'y'",
                "presto": "JSON_EXTRACT_SCALAR(x, 'y')",
            },
            write={
                "postgres": "x->>'y'",
                "presto": "JSON_EXTRACT_SCALAR(x, 'y')",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT(x, 'y')",
            read={
                "postgres": "x#>'y'",
            },
            write={
                "postgres": "x#>'y'",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT_SCALAR(x, 'y')",
            read={
                "postgres": "x#>>'y'",
            },
            write={
                "postgres": "x#>>'y'",
            },
        )

    def test_cross_join(self):
        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t (a)",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            },
        )
        self.validate_all(
            "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t (a, b)",
            write={
                "presto": "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t(a, b)",
                "spark": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) t AS b",
            },
        )
        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t (a)",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t(a)",
                "spark": "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            },
        )

    def test_lateral_subquery(self):
        self.validate_identity(
            "SELECT art FROM tbl1 INNER JOIN LATERAL (SELECT art FROM tbl2) AS tbl2 ON tbl1.art = tbl2.art"
        )
        self.validate_identity(
            "SELECT * FROM tbl AS t LEFT JOIN LATERAL (SELECT * FROM b WHERE b.t_id = t.t_id) AS t ON TRUE"
        )

    def test_set_operators(self):
        self.validate_all(
            "SELECT * FROM a UNION SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
                "clickhouse": "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION SELECT * FROM b",
                "presto": "SELECT * FROM a UNION SELECT * FROM b",
                "spark": "SELECT * FROM a UNION SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION SELECT * FROM b",
                "presto": "SELECT * FROM a UNION SELECT * FROM b",
                "spark": "SELECT * FROM a UNION SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "clickhouse": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "presto": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "spark": "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "presto": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "spark": "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION SELECT * FROM b",
                "presto": "SELECT * FROM a UNION SELECT * FROM b",
                "spark": "SELECT * FROM a UNION SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            },
        )

    def test_operators(self):
        self.validate_identity("some.column LIKE 'foo' || another.column || 'bar' || LOWER(x)")
        self.validate_identity("some.column LIKE 'foo' + another.column + 'bar'")

        self.validate_all(
            "x ILIKE '%y'",
            read={
                "clickhouse": "x ILIKE '%y'",
                "duckdb": "x ILIKE '%y'",
                "postgres": "x ILIKE '%y'",
                "snowflake": "x ILIKE '%y'",
            },
            write={
                "bigquery": "LOWER(x) LIKE '%y'",
                "clickhouse": "x ILIKE '%y'",
                "duckdb": "x ILIKE '%y'",
                "hive": "LOWER(x) LIKE '%y'",
                "mysql": "LOWER(x) LIKE '%y'",
                "oracle": "LOWER(x) LIKE '%y'",
                "postgres": "x ILIKE '%y'",
                "presto": "LOWER(x) LIKE '%y'",
                "snowflake": "x ILIKE '%y'",
                "spark": "x ILIKE '%y'",
                "sqlite": "LOWER(x) LIKE '%y'",
                "starrocks": "LOWER(x) LIKE '%y'",
                "trino": "LOWER(x) LIKE '%y'",
            },
        )
        self.validate_all(
            "SELECT * FROM a ORDER BY col_a NULLS LAST",
            write={
                "mysql": UnsupportedError,
                "starrocks": UnsupportedError,
            },
        )
        self.validate_all(
            "POSITION(' ' in x)",
            write={
                "duckdb": "STRPOS(x, ' ')",
                "postgres": "STRPOS(x, ' ')",
                "presto": "STRPOS(x, ' ')",
                "spark": "LOCATE(' ', x)",
                "clickhouse": "position(x, ' ')",
                "snowflake": "POSITION(' ', x)",
            },
        )
        self.validate_all(
            "STR_POSITION('a', x)",
            write={
                "duckdb": "STRPOS(x, 'a')",
                "postgres": "STRPOS(x, 'a')",
                "presto": "STRPOS(x, 'a')",
                "spark": "LOCATE('a', x)",
                "clickhouse": "position(x, 'a')",
                "snowflake": "POSITION('a', x)",
            },
        )
        self.validate_all(
            "POSITION('a', x, 3)",
            write={
                "presto": "STRPOS(SUBSTR(x, 3), 'a') + 3 - 1",
                "spark": "LOCATE('a', x, 3)",
                "clickhouse": "position(x, 'a', 3)",
                "snowflake": "POSITION('a', x, 3)",
            },
        )
        self.validate_all(
            "CONCAT_WS('-', 'a', 'b')",
            write={
                "duckdb": "CONCAT_WS('-', 'a', 'b')",
                "presto": "ARRAY_JOIN(ARRAY['a', 'b'], '-')",
                "hive": "CONCAT_WS('-', 'a', 'b')",
                "spark": "CONCAT_WS('-', 'a', 'b')",
            },
        )

        self.validate_all(
            "CONCAT_WS('-', x)",
            write={
                "duckdb": "CONCAT_WS('-', x)",
                "presto": "ARRAY_JOIN(x, '-')",
                "hive": "CONCAT_WS('-', x)",
                "spark": "CONCAT_WS('-', x)",
            },
        )
        self.validate_all(
            "IF(x > 1, 1, 0)",
            write={
                "duckdb": "CASE WHEN x > 1 THEN 1 ELSE 0 END",
                "presto": "IF(x > 1, 1, 0)",
                "hive": "IF(x > 1, 1, 0)",
                "spark": "IF(x > 1, 1, 0)",
                "tableau": "IF x > 1 THEN 1 ELSE 0 END",
            },
        )
        self.validate_all(
            "CASE WHEN 1 THEN x ELSE 0 END",
            write={
                "duckdb": "CASE WHEN 1 THEN x ELSE 0 END",
                "presto": "CASE WHEN 1 THEN x ELSE 0 END",
                "hive": "CASE WHEN 1 THEN x ELSE 0 END",
                "spark": "CASE WHEN 1 THEN x ELSE 0 END",
                "tableau": "CASE WHEN 1 THEN x ELSE 0 END",
            },
        )
        self.validate_all(
            "x[y]",
            write={
                "duckdb": "x[y]",
                "presto": "x[y]",
                "hive": "x[y]",
                "spark": "x[y]",
            },
        )
        self.validate_all(
            """'["x"]'""",
            write={
                "duckdb": """'["x"]'""",
                "presto": """'["x"]'""",
                "hive": """'["x"]'""",
                "spark": """'["x"]'""",
            },
        )

        self.validate_all(
            'true or null as "foo"',
            write={
                "bigquery": "TRUE OR NULL AS `foo`",
                "duckdb": 'TRUE OR NULL AS "foo"',
                "presto": 'TRUE OR NULL AS "foo"',
                "hive": "TRUE OR NULL AS `foo`",
                "spark": "TRUE OR NULL AS `foo`",
            },
        )
        self.validate_all(
            "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) as foo FROM baz",
            write={
                "bigquery": "SELECT CASE WHEN COALESCE(bar, 0) = 1 THEN TRUE ELSE FALSE END AS foo FROM baz",
                "duckdb": "SELECT CASE WHEN COALESCE(bar, 0) = 1 THEN TRUE ELSE FALSE END AS foo FROM baz",
                "presto": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "hive": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "spark": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(col1, col2)",
            write={
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "presto": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(coalesce(col1, col2), coalesce(col2, col1))",
            write={
                "duckdb": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
                "presto": "LEVENSHTEIN_DISTANCE(COALESCE(col1, col2), COALESCE(col2, col1))",
                "hive": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
                "spark": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
            },
        )
        self.validate_all(
            "ARRAY_FILTER(the_array, x -> x > 0)",
            write={
                "presto": "FILTER(the_array, x -> x > 0)",
                "hive": "FILTER(the_array, x -> x > 0)",
                "spark": "FILTER(the_array, x -> x > 0)",
            },
        )

    def test_limit(self):
        self.validate_all(
            "SELECT x FROM y LIMIT 10",
            write={
                "sqlite": "SELECT x FROM y LIMIT 10",
                "oracle": "SELECT x FROM y FETCH FIRST 10 ROWS ONLY",
            },
        )
        self.validate_all(
            "SELECT x FROM y LIMIT 10 OFFSET 5",
            write={
                "sqlite": "SELECT x FROM y LIMIT 10 OFFSET 5",
                "oracle": "SELECT x FROM y OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
            },
        )
        self.validate_all(
            "SELECT x FROM y OFFSET 10 FETCH FIRST 3 ROWS ONLY",
            write={
                "oracle": "SELECT x FROM y OFFSET 10 ROWS FETCH FIRST 3 ROWS ONLY",
            },
        )
        self.validate_all(
            "SELECT x FROM y OFFSET 10 ROWS FETCH FIRST 3 ROWS ONLY",
            write={
                "oracle": "SELECT x FROM y OFFSET 10 ROWS FETCH FIRST 3 ROWS ONLY",
            },
        )
        self.validate_all(
            '"x" + "y"',
            read={
                "clickhouse": '`x` + "y"',
                "sqlite": '`x` + "y"',
                "redshift": '"x" + "y"',
            },
        )
        self.validate_all(
            "[1, 2]",
            write={
                "bigquery": "[1, 2]",
                "clickhouse": "[1, 2]",
            },
        )
        self.validate_all(
            "SELECT * FROM VALUES ('x'), ('y') AS t(z)",
            write={
                "spark": "SELECT * FROM VALUES ('x'), ('y') AS t(z)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (c CHAR, nc NCHAR, v1 VARCHAR, v2 VARCHAR2, nv NVARCHAR, nv2 NVARCHAR2)",
            write={
                "duckdb": "CREATE TABLE t (c CHAR, nc CHAR, v1 TEXT, v2 TEXT, nv TEXT, nv2 TEXT)",
                "hive": "CREATE TABLE t (c CHAR, nc CHAR, v1 STRING, v2 STRING, nv STRING, nv2 STRING)",
                "oracle": "CREATE TABLE t (c CHAR, nc CHAR, v1 VARCHAR2, v2 VARCHAR2, nv NVARCHAR2, nv2 NVARCHAR2)",
                "postgres": "CREATE TABLE t (c CHAR, nc CHAR, v1 VARCHAR, v2 VARCHAR, nv VARCHAR, nv2 VARCHAR)",
                "sqlite": "CREATE TABLE t (c TEXT, nc TEXT, v1 TEXT, v2 TEXT, nv TEXT, nv2 TEXT)",
            },
        )
        self.validate_all(
            "POWER(1.2, 3.4)",
            read={
                "hive": "pow(1.2, 3.4)",
                "postgres": "power(1.2, 3.4)",
            },
        )
        self.validate_all(
            "CREATE INDEX my_idx ON tbl (a, b)",
            read={
                "hive": "CREATE INDEX my_idx ON TABLE tbl (a, b)",
                "sqlite": "CREATE INDEX my_idx ON tbl (a, b)",
            },
            write={
                "hive": "CREATE INDEX my_idx ON TABLE tbl (a, b)",
                "postgres": "CREATE INDEX my_idx ON tbl (a, b)",
                "sqlite": "CREATE INDEX my_idx ON tbl (a, b)",
            },
        )
        self.validate_all(
            "CREATE UNIQUE INDEX my_idx ON tbl (a, b)",
            read={
                "hive": "CREATE UNIQUE INDEX my_idx ON TABLE tbl (a, b)",
                "sqlite": "CREATE UNIQUE INDEX my_idx ON tbl (a, b)",
            },
            write={
                "hive": "CREATE UNIQUE INDEX my_idx ON TABLE tbl (a, b)",
                "postgres": "CREATE UNIQUE INDEX my_idx ON tbl (a, b)",
                "sqlite": "CREATE UNIQUE INDEX my_idx ON tbl (a, b)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 TEXT, c2 TEXT(1024))",
            write={
                "duckdb": "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 TEXT, c2 TEXT(1024))",
                "hive": "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 STRING, c2 STRING(1024))",
                "oracle": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 CLOB, c2 CLOB(1024))",
                "postgres": "CREATE TABLE t (b1 BYTEA, b2 BYTEA(1024), c1 TEXT, c2 TEXT(1024))",
                "sqlite": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 TEXT, c2 TEXT(1024))",
                "redshift": "CREATE TABLE t (b1 VARBYTE, b2 VARBYTE(1024), c1 TEXT, c2 TEXT(1024))",
            },
        )

    def test_alias(self):
        self.validate_all(
            "SELECT a AS b FROM x GROUP BY b",
            write={
                "duckdb": "SELECT a AS b FROM x GROUP BY b",
                "presto": "SELECT a AS b FROM x GROUP BY 1",
                "hive": "SELECT a AS b FROM x GROUP BY 1",
                "oracle": "SELECT a AS b FROM x GROUP BY 1",
                "spark": "SELECT a AS b FROM x GROUP BY 1",
            },
        )
        self.validate_all(
            "SELECT y x FROM my_table t",
            write={
                "hive": "SELECT y AS x FROM my_table AS t",
                "oracle": "SELECT y AS x FROM my_table t",
                "postgres": "SELECT y AS x FROM my_table AS t",
                "sqlite": "SELECT y AS x FROM my_table AS t",
            },
        )
        self.validate_all(
            "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t JOIN cte2 WHERE cte1.a = cte2.c",
            write={
                "hive": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t JOIN cte2 WHERE cte1.a = cte2.c",
                "oracle": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 t JOIN cte2 WHERE cte1.a = cte2.c",
                "postgres": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t JOIN cte2 WHERE cte1.a = cte2.c",
                "sqlite": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t JOIN cte2 WHERE cte1.a = cte2.c",
            },
        )
