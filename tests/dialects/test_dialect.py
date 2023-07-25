import unittest

from sqlglot import Dialect, Dialects, ErrorLevel, UnsupportedError, parse_one
from sqlglot.dialects import Hive


class Validator(unittest.TestCase):
    dialect = None

    def parse_one(self, sql):
        return parse_one(sql, read=self.dialect)

    def validate_identity(self, sql, write_sql=None, pretty=False):
        expression = self.parse_one(sql)
        self.assertEqual(write_sql or sql, expression.sql(dialect=self.dialect, pretty=pretty))
        return expression

    def validate_all(self, sql, read=None, write=None, pretty=False, identify=False):
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
        expression = self.parse_one(sql)

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                self.assertEqual(
                    parse_one(read_sql, read_dialect).sql(
                        self.dialect,
                        unsupported_level=ErrorLevel.IGNORE,
                        pretty=pretty,
                        identify=identify,
                    ),
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
                            identify=identify,
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

    def test_get_or_raise(self):
        self.assertEqual(Dialect.get_or_raise(Hive), Hive)
        self.assertEqual(Dialect.get_or_raise(Hive()), Hive)
        self.assertEqual(Dialect.get_or_raise("hive"), Hive)

    def test_cast(self):
        self.validate_all(
            "CAST(a AS TEXT)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "clickhouse": "CAST(a AS TEXT)",
                "drill": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS CHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR(MAX))",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS BINARY(4))",
            write={
                "bigquery": "CAST(a AS BYTES)",
                "clickhouse": "CAST(a AS BINARY(4))",
                "drill": "CAST(a AS VARBINARY(4))",
                "duckdb": "CAST(a AS BLOB(4))",
                "mysql": "CAST(a AS BINARY(4))",
                "hive": "CAST(a AS BINARY(4))",
                "oracle": "CAST(a AS BLOB(4))",
                "postgres": "CAST(a AS BYTEA(4))",
                "presto": "CAST(a AS VARBINARY(4))",
                "redshift": "CAST(a AS VARBYTE(4))",
                "snowflake": "CAST(a AS BINARY(4))",
                "sqlite": "CAST(a AS BLOB(4))",
                "spark": "CAST(a AS BINARY(4))",
                "starrocks": "CAST(a AS BINARY(4))",
            },
        )
        self.validate_all(
            "CAST(a AS VARBINARY(4))",
            write={
                "bigquery": "CAST(a AS BYTES)",
                "clickhouse": "CAST(a AS VARBINARY(4))",
                "duckdb": "CAST(a AS BLOB(4))",
                "mysql": "CAST(a AS VARBINARY(4))",
                "hive": "CAST(a AS BINARY(4))",
                "oracle": "CAST(a AS BLOB(4))",
                "postgres": "CAST(a AS BYTEA(4))",
                "presto": "CAST(a AS VARBINARY(4))",
                "redshift": "CAST(a AS VARBYTE(4))",
                "snowflake": "CAST(a AS VARBINARY(4))",
                "sqlite": "CAST(a AS BLOB(4))",
                "spark": "CAST(a AS BINARY(4))",
                "starrocks": "CAST(a AS VARBINARY(4))",
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
                "drill": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS CHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR(MAX))",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "drill": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS CHAR)",
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
                "bigquery": "CAST(a AS STRING)",
                "drill": "CAST(a AS VARCHAR(3))",
                "duckdb": "CAST(a AS TEXT(3))",
                "mysql": "CAST(a AS CHAR(3))",
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
                "drill": "CAST(a AS INTEGER)",
                "duckdb": "CAST(a AS SMALLINT)",
                "mysql": "CAST(a AS SMALLINT)",
                "hive": "CAST(a AS SMALLINT)",
                "oracle": "CAST(a AS NUMBER)",
                "postgres": "CAST(a AS SMALLINT)",
                "presto": "CAST(a AS SMALLINT)",
                "redshift": "CAST(a AS SMALLINT)",
                "snowflake": "CAST(a AS SMALLINT)",
                "spark": "CAST(a AS SMALLINT)",
                "sqlite": "CAST(a AS INTEGER)",
                "starrocks": "CAST(a AS SMALLINT)",
            },
        )
        self.validate_all(
            "CAST(a AS DOUBLE)",
            read={
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
            },
            write={
                "duckdb": "CAST(a AS DOUBLE)",
                "drill": "CAST(a AS DOUBLE)",
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
            },
        )

        self.validate_all(
            "CAST(a AS DOUBLE)",
            write={
                "bigquery": "CAST(a AS FLOAT64)",
                "clickhouse": "CAST(a AS Float64)",
                "drill": "CAST(a AS DOUBLE)",
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
        self.validate_all(
            "CAST('127.0.0.1/32' AS INET)",
            read={"postgres": "INET '127.0.0.1/32'"},
        )

    def test_decode(self):
        self.validate_identity("DECODE(bin, charset)")

        self.validate_all(
            "SELECT DECODE(a, 1, 'one')",
            write={
                "": "SELECT CASE WHEN a = 1 THEN 'one' END",
                "oracle": "SELECT CASE WHEN a = 1 THEN 'one' END",
                "redshift": "SELECT CASE WHEN a = 1 THEN 'one' END",
                "snowflake": "SELECT CASE WHEN a = 1 THEN 'one' END",
                "spark": "SELECT CASE WHEN a = 1 THEN 'one' END",
            },
        )
        self.validate_all(
            "SELECT DECODE(a, 1, 'one', 'default')",
            write={
                "": "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'default' END",
                "oracle": "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'default' END",
                "redshift": "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'default' END",
                "snowflake": "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'default' END",
                "spark": "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'default' END",
            },
        )
        self.validate_all(
            "SELECT DECODE(a, NULL, 'null')",
            write={
                "": "SELECT CASE WHEN a IS NULL THEN 'null' END",
                "oracle": "SELECT CASE WHEN a IS NULL THEN 'null' END",
                "redshift": "SELECT CASE WHEN a IS NULL THEN 'null' END",
                "snowflake": "SELECT CASE WHEN a IS NULL THEN 'null' END",
                "spark": "SELECT CASE WHEN a IS NULL THEN 'null' END",
            },
        )
        self.validate_all(
            "SELECT DECODE(a, b, c)",
            write={
                "": "SELECT CASE WHEN a = b OR (a IS NULL AND b IS NULL) THEN c END",
                "oracle": "SELECT CASE WHEN a = b OR (a IS NULL AND b IS NULL) THEN c END",
                "redshift": "SELECT CASE WHEN a = b OR (a IS NULL AND b IS NULL) THEN c END",
                "snowflake": "SELECT CASE WHEN a = b OR (a IS NULL AND b IS NULL) THEN c END",
                "spark": "SELECT CASE WHEN a = b OR (a IS NULL AND b IS NULL) THEN c END",
            },
        )
        self.validate_all(
            "SELECT DECODE(tbl.col, 'some_string', 'foo')",
            write={
                "": "SELECT CASE WHEN tbl.col = 'some_string' THEN 'foo' END",
                "oracle": "SELECT CASE WHEN tbl.col = 'some_string' THEN 'foo' END",
                "redshift": "SELECT CASE WHEN tbl.col = 'some_string' THEN 'foo' END",
                "snowflake": "SELECT CASE WHEN tbl.col = 'some_string' THEN 'foo' END",
                "spark": "SELECT CASE WHEN tbl.col = 'some_string' THEN 'foo' END",
            },
        )

    def test_if_null(self):
        self.validate_all(
            "SELECT IFNULL(1, NULL) FROM foo",
            write={
                "": "SELECT COALESCE(1, NULL) FROM foo",
                "redshift": "SELECT COALESCE(1, NULL) FROM foo",
                "postgres": "SELECT COALESCE(1, NULL) FROM foo",
                "mysql": "SELECT COALESCE(1, NULL) FROM foo",
                "duckdb": "SELECT COALESCE(1, NULL) FROM foo",
                "spark": "SELECT COALESCE(1, NULL) FROM foo",
                "bigquery": "SELECT COALESCE(1, NULL) FROM foo",
                "presto": "SELECT COALESCE(1, NULL) FROM foo",
            },
        )

    def test_time(self):
        self.validate_all(
            "STR_TO_TIME(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
            },
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS TIMESTAMP)",
                "presto": "DATE_PARSE(x, '%Y-%m-%dT%T')",
                "drill": "TO_TIMESTAMP(x, 'yyyy-MM-dd''T''HH:mm:ss')",
                "redshift": "TO_TIMESTAMP(x, 'YYYY-MM-DDTHH:MI:SS')",
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            write={
                "drill": "TO_TIMESTAMP('2020-01-01', 'yyyy-MM-dd')",
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
                "drill": "TO_TIMESTAMP(x, 'yy')",
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
                "drill": "CAST('2020-01-01' AS DATE)",
                "duckdb": "CAST('2020-01-01' AS DATE)",
                "hive": "TO_DATE('2020-01-01')",
                "presto": "CAST('2020-01-01' AS TIMESTAMP)",
                "starrocks": "TO_DATE('2020-01-01')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01')",
            write={
                "drill": "CAST('2020-01-01' AS TIMESTAMP)",
                "duckdb": "CAST('2020-01-01' AS TIMESTAMP)",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "presto": "CAST('2020-01-01' AS TIMESTAMP)",
                "sqlite": "'2020-01-01'",
            },
        )
        self.validate_all(
            "TIME_STR_TO_UNIX('2020-01-01')",
            write={
                "duckdb": "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
                "hive": "UNIX_TIMESTAMP('2020-01-01')",
                "mysql": "UNIX_TIMESTAMP('2020-01-01')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%m-%d %T'))",
            },
        )
        self.validate_all(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            write={
                "drill": "TO_CHAR(x, 'yyyy-MM-dd')",
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
                "drill": "CAST(x AS VARCHAR)",
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
                "redshift": "CAST(x AS VARCHAR(MAX))",
            },
        )
        self.validate_all(
            "TIME_TO_UNIX(x)",
            write={
                "drill": "UNIX_TIMESTAMP(x)",
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
                "bigquery": "CAST(x AS DATE)",
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "postgres": "CAST(x AS DATE)",
                "presto": "CAST(CAST(x AS TIMESTAMP) AS DATE)",
                "snowflake": "CAST(x AS DATE)",
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
                "duckdb": "STRFTIME(TO_TIMESTAMP(x), y)",
                "hive": "FROM_UNIXTIME(x, y)",
                "presto": "DATE_FORMAT(FROM_UNIXTIME(x), y)",
                "starrocks": "FROM_UNIXTIME(x, y)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME(x)",
            write={
                "duckdb": "TO_TIMESTAMP(x)",
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
                "duckdb": "CAST(TO_TIMESTAMP(x) AS TEXT)",
                "hive": "FROM_UNIXTIME(x)",
                "presto": "CAST(FROM_UNIXTIME(x) AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DATE_STR(x)",
            write={
                "drill": "CAST(x AS VARCHAR)",
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DI(x)",
            write={
                "drill": "CAST(TO_DATE(x, 'yyyyMMdd') AS INT)",
                "duckdb": "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
                "hive": "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
                "presto": "CAST(DATE_FORMAT(x, '%Y%m%d') AS INT)",
            },
        )
        self.validate_all(
            "DI_TO_DATE(x)",
            write={
                "drill": "TO_DATE(CAST(x AS VARCHAR), 'yyyyMMdd')",
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
            "DATE_ADD(x, 1, 'DAY')",
            read={
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "snowflake": "DATEADD('DAY', 1, x)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 DAY)",
                "drill": "DATE_ADD(x, INTERVAL 1 DAY)",
                "duckdb": "x + INTERVAL 1 DAY",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "postgres": "x + INTERVAL '1 DAY'",
                "presto": "DATE_ADD('DAY', 1, x)",
                "snowflake": "DATEADD(DAY, 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "sqlite": "DATE(x, '1 DAY')",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
                "tsql": "DATEADD(DAY, 1, x)",
            },
        )
        self.validate_all(
            "DATE_ADD(x, 1)",
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 DAY)",
                "drill": "DATE_ADD(x, INTERVAL 1 DAY)",
                "duckdb": "x + INTERVAL 1 DAY",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "presto": "DATE_ADD('day', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('day', x)",
            read={
                "bigquery": "DATE_TRUNC(x, day)",
                "spark": "TRUNC(x, 'day')",
            },
            write={
                "bigquery": "DATE_TRUNC(x, day)",
                "duckdb": "DATE_TRUNC('day', x)",
                "mysql": "DATE(x)",
                "presto": "DATE_TRUNC('day', x)",
                "postgres": "DATE_TRUNC('day', x)",
                "snowflake": "DATE_TRUNC('day', x)",
                "starrocks": "DATE_TRUNC('day', x)",
                "spark": "TRUNC(x, 'day')",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(x, day)",
            read={
                "bigquery": "TIMESTAMP_TRUNC(x, day)",
                "duckdb": "DATE_TRUNC('day', x)",
                "presto": "DATE_TRUNC('day', x)",
                "postgres": "DATE_TRUNC('day', x)",
                "snowflake": "DATE_TRUNC('day', x)",
                "starrocks": "DATE_TRUNC('day', x)",
                "spark": "DATE_TRUNC('day', x)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('day', CAST(x AS DATE))",
            read={
                "presto": "DATE_TRUNC('day', x::DATE)",
                "snowflake": "DATE_TRUNC('day', x::DATE)",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(CAST(x AS DATE), day)",
            read={"postgres": "DATE_TRUNC('day', x::DATE)"},
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(CAST(x AS DATE), day)",
            read={"starrocks": "DATE_TRUNC('day', x::DATE)"},
        )
        self.validate_all(
            "DATE_TRUNC('week', x)",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')",
            },
        )
        self.validate_all(
            "DATE_TRUNC('month', x)",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC('quarter', x)",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC('year', x)",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC('millenium', x)",
            write={
                "mysql": UnsupportedError,
            },
        )
        self.validate_all(
            "DATE_TRUNC('year', x)",
            read={
                "bigquery": "DATE_TRUNC(x, year)",
                "spark": "TRUNC(x, 'year')",
            },
            write={
                "bigquery": "DATE_TRUNC(x, year)",
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
                "postgres": "DATE_TRUNC('year', x)",
                "snowflake": "DATE_TRUNC('year', x)",
                "starrocks": "DATE_TRUNC('year', x)",
                "spark": "TRUNC(x, 'year')",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(x, year)",
            read={
                "bigquery": "TIMESTAMP_TRUNC(x, year)",
                "postgres": "DATE_TRUNC(year, x)",
                "spark": "DATE_TRUNC('year', x)",
                "snowflake": "DATE_TRUNC(year, x)",
                "starrocks": "DATE_TRUNC('year', x)",
            },
            write={
                "bigquery": "TIMESTAMP_TRUNC(x, year)",
                "spark": "DATE_TRUNC('year', x)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('millenium', x)",
            write={
                "mysql": UnsupportedError,
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
            },
            write={
                "drill": "TO_DATE(x, 'yyyy-MM-dd''T''HH:mm:ss')",
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%dT%T') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%d')",
            write={
                "drill": "CAST(x AS DATE)",
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
                "drill": "CAST(x AS DATE)",
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)",
                "spark": "TO_DATE(x)",
                "sqlite": "x",
            },
        )
        self.validate_all(
            "TS_OR_DS_ADD('2021-02-01', 1, 'DAY')",
            write={
                "drill": "DATE_ADD(CAST('2021-02-01' AS DATE), INTERVAL 1 DAY)",
                "duckdb": "CAST('2021-02-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD('2021-02-01', 1)",
                "presto": "DATE_ADD('DAY', 1, CAST(CAST('2021-02-01' AS TIMESTAMP) AS DATE))",
                "spark": "DATE_ADD('2021-02-01', 1)",
            },
        )
        self.validate_all(
            "TS_OR_DS_ADD(x, 1, 'DAY')",
            write={
                "presto": "DATE_ADD('DAY', 1, CAST(CAST(x AS TIMESTAMP) AS DATE))",
                "hive": "DATE_ADD(x, 1)",
            },
        )
        self.validate_all(
            "TS_OR_DS_ADD(CURRENT_DATE, 1, 'DAY')",
            write={
                "presto": "DATE_ADD('DAY', 1, CURRENT_DATE)",
                "hive": "DATE_ADD(CURRENT_DATE, 1)",
            },
        )
        self.validate_all(
            "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            write={
                "drill": "DATE_ADD(CAST('2020-01-01' AS DATE), INTERVAL 1 DAY)",
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
                "presto": "DATE_ADD('day', 1, CAST('2020-01-01' AS DATE))",
                "spark": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            },
        )
        self.validate_all(
            "TIMESTAMP '2022-01-01'",
            write={
                "drill": "CAST('2022-01-01' AS TIMESTAMP)",
                "mysql": "CAST('2022-01-01' AS TIMESTAMP)",
                "starrocks": "CAST('2022-01-01' AS DATETIME)",
                "hive": "CAST('2022-01-01' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "TIMESTAMP('2022-01-01')",
            write={
                "mysql": "TIMESTAMP('2022-01-01')",
                "starrocks": "TIMESTAMP('2022-01-01')",
                "hive": "TIMESTAMP('2022-01-01')",
            },
        )

        for unit in ("DAY", "MONTH", "YEAR"):
            self.validate_all(
                f"{unit}(x)",
                read={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "bigquery",
                        "drill",
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
                        "drill",
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
                "drill": "REPEATED_COUNT(x)",
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
                "spark": "AGGREGATE(x, 0, (acc, x) -> acc + x, acc -> acc)",
                "presto": "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            },
        )

    def test_order_by(self):
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "bigquery": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
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
                "mysql": "JSON_EXTRACT(x, 'y')",
                "postgres": "x->'y'",
                "presto": "JSON_EXTRACT(x, 'y')",
                "starrocks": "x -> 'y'",
            },
            write={
                "mysql": "JSON_EXTRACT(x, 'y')",
                "oracle": "JSON_EXTRACT(x, 'y')",
                "postgres": "x -> 'y'",
                "presto": "JSON_EXTRACT(x, 'y')",
                "starrocks": "x -> 'y'",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_SCALAR(x, 'y')",
            read={
                "postgres": "x ->> 'y'",
                "presto": "JSON_EXTRACT_SCALAR(x, 'y')",
            },
            write={
                "postgres": "x ->> 'y'",
                "presto": "JSON_EXTRACT_SCALAR(x, 'y')",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_SCALAR(stream_data, '$.data.results')",
            read={
                "hive": "GET_JSON_OBJECT(stream_data, '$.data.results')",
                "mysql": "stream_data ->> '$.data.results'",
            },
            write={
                "hive": "GET_JSON_OBJECT(stream_data, '$.data.results')",
                "mysql": "stream_data ->> '$.data.results'",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT(x, 'y')",
            read={
                "postgres": "x#>'y'",
            },
            write={
                "postgres": "x #> 'y'",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT_SCALAR(x, 'y')",
            read={
                "postgres": "x#>>'y'",
            },
            write={
                "postgres": "x #>> 'y'",
            },
        )

    def test_cross_join(self):
        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t (a)",
            write={
                "drill": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            },
        )
        self.validate_all(
            "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t (a, b)",
            write={
                "drill": "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t(a, b)",
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
                "drill": "SELECT * FROM a UNION SELECT * FROM b",
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

        self.validate_all("LIKE(x, 'z')", write={"": "'z' LIKE x"})
        self.validate_all(
            "CONCAT(a, b, c)",
            write={
                "": "CONCAT(a, b, c)",
                "redshift": "a || b || c",
                "sqlite": "a || b || c",
            },
        )
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
                "drill": "x `ILIKE` '%y'",
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
            "POSITION(needle in haystack)",
            write={
                "drill": "STRPOS(haystack, needle)",
                "duckdb": "STRPOS(haystack, needle)",
                "postgres": "STRPOS(haystack, needle)",
                "presto": "STRPOS(haystack, needle)",
                "spark": "LOCATE(needle, haystack)",
                "clickhouse": "position(haystack, needle)",
                "snowflake": "POSITION(needle, haystack)",
                "mysql": "LOCATE(needle, haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            write={
                "drill": "STRPOS(haystack, needle)",
                "duckdb": "STRPOS(haystack, needle)",
                "postgres": "STRPOS(haystack, needle)",
                "presto": "STRPOS(haystack, needle)",
                "spark": "LOCATE(needle, haystack)",
                "clickhouse": "position(haystack, needle)",
                "snowflake": "POSITION(needle, haystack)",
                "mysql": "LOCATE(needle, haystack)",
            },
        )
        self.validate_all(
            "POSITION(needle, haystack, pos)",
            write={
                "drill": "STRPOS(SUBSTR(haystack, pos), needle) + pos - 1",
                "presto": "STRPOS(haystack, needle, pos)",
                "spark": "LOCATE(needle, haystack, pos)",
                "clickhouse": "position(haystack, needle, pos)",
                "snowflake": "POSITION(needle, haystack, pos)",
                "mysql": "LOCATE(needle, haystack, pos)",
            },
        )
        self.validate_all(
            "CONCAT_WS('-', 'a', 'b')",
            write={
                "duckdb": "CONCAT_WS('-', 'a', 'b')",
                "presto": "CONCAT_WS('-', 'a', 'b')",
                "hive": "CONCAT_WS('-', 'a', 'b')",
                "spark": "CONCAT_WS('-', 'a', 'b')",
                "trino": "CONCAT_WS('-', 'a', 'b')",
            },
        )

        self.validate_all(
            "CONCAT_WS('-', x)",
            write={
                "duckdb": "CONCAT_WS('-', x)",
                "hive": "CONCAT_WS('-', x)",
                "presto": "CONCAT_WS('-', x)",
                "spark": "CONCAT_WS('-', x)",
                "trino": "CONCAT_WS('-', x)",
            },
        )
        self.validate_all(
            "CONCAT(a)",
            write={
                "clickhouse": "a",
                "presto": "a",
                "trino": "a",
                "tsql": "a",
            },
        )
        self.validate_all(
            "COALESCE(CAST(a AS TEXT), '')",
            read={
                "drill": "CONCAT(a)",
                "duckdb": "CONCAT(a)",
                "postgres": "CONCAT(a)",
                "tsql": "CONCAT(a)",
            },
        )
        self.validate_all(
            "IF(x > 1, 1, 0)",
            write={
                "drill": "`IF`(x > 1, 1, 0)",
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
                "drill": "CASE WHEN 1 THEN x ELSE 0 END",
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
                "drill": "x[y]",
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
                "drill": "TRUE OR NULL AS `foo`",
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
                "drill": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "presto": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(coalesce(col1, col2), coalesce(col2, col1))",
            write={
                "duckdb": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
                "drill": "LEVENSHTEIN_DISTANCE(COALESCE(col1, col2), COALESCE(col2, col1))",
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
            "SELECT * FROM data LIMIT 10, 20",
            write={"sqlite": "SELECT * FROM data LIMIT 20 OFFSET 10"},
        )
        self.validate_all(
            "SELECT x FROM y LIMIT 10",
            read={
                "teradata": "SELECT TOP 10 x FROM y",
                "tsql": "SELECT TOP 10 x FROM y",
                "snowflake": "SELECT TOP 10 x FROM y",
            },
            write={
                "sqlite": "SELECT x FROM y LIMIT 10",
                "oracle": "SELECT x FROM y FETCH FIRST 10 ROWS ONLY",
                "tsql": "SELECT TOP 10 x FROM y",
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
                "sqlite": "SELECT x FROM y LIMIT 3 OFFSET 10",
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
                "duckdb": "CREATE TABLE t (c TEXT, nc TEXT, v1 TEXT, v2 TEXT, nv TEXT, nv2 TEXT)",
                "hive": "CREATE TABLE t (c CHAR, nc CHAR, v1 STRING, v2 STRING, nv STRING, nv2 STRING)",
                "oracle": "CREATE TABLE t (c CHAR, nc NCHAR, v1 VARCHAR2, v2 VARCHAR2, nv NVARCHAR2, nv2 NVARCHAR2)",
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
                "postgres": "CREATE INDEX my_idx ON tbl (a NULLS FIRST, b NULLS FIRST)",
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
                "postgres": "CREATE UNIQUE INDEX my_idx ON tbl (a NULLS FIRST, b NULLS FIRST)",
                "sqlite": "CREATE UNIQUE INDEX my_idx ON tbl (a, b)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 TEXT, c2 TEXT(1024))",
            write={
                "duckdb": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 TEXT, c2 TEXT(1024))",
                "hive": "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 STRING, c2 STRING(1024))",
                "oracle": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 CLOB, c2 CLOB(1024))",
                "postgres": "CREATE TABLE t (b1 BYTEA, b2 BYTEA(1024), c1 TEXT, c2 TEXT(1024))",
                "sqlite": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 TEXT, c2 TEXT(1024))",
                "redshift": "CREATE TABLE t (b1 VARBYTE, b2 VARBYTE(1024), c1 VARCHAR(MAX), c2 VARCHAR(1024))",
            },
        )

    def test_alias(self):
        self.validate_all(
            "SELECT a AS b FROM x GROUP BY b",
            write={
                "drill": "SELECT a AS b FROM x GROUP BY b",
                "duckdb": "SELECT a AS b FROM x GROUP BY b",
                "presto": "SELECT a AS b FROM x GROUP BY 1",
                "hive": "SELECT a AS b FROM x GROUP BY 1",
                "oracle": "SELECT a AS b FROM x GROUP BY 1",
                "spark": "SELECT a AS b FROM x GROUP BY b",
                "spark2": "SELECT a AS b FROM x GROUP BY 1",
            },
        )
        self.validate_all(
            "SELECT y x FROM my_table t",
            write={
                "drill": "SELECT y AS x FROM my_table AS t",
                "hive": "SELECT y AS x FROM my_table AS t",
                "oracle": "SELECT y AS x FROM my_table t",
                "postgres": "SELECT y AS x FROM my_table AS t",
                "sqlite": "SELECT y AS x FROM my_table AS t",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT * FROM my_table AS t) AS tbl",
            write={
                "drill": "SELECT * FROM (SELECT * FROM my_table AS t) AS tbl",
                "hive": "SELECT * FROM (SELECT * FROM my_table AS t) AS tbl",
                "oracle": "SELECT * FROM (SELECT * FROM my_table t) tbl",
                "postgres": "SELECT * FROM (SELECT * FROM my_table AS t) AS tbl",
                "sqlite": "SELECT * FROM (SELECT * FROM my_table AS t) AS tbl",
            },
        )
        self.validate_all(
            "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t CROSS JOIN cte2 WHERE cte1.a = cte2.c",
            write={
                "hive": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t CROSS JOIN cte2 WHERE cte1.a = cte2.c",
                "oracle": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 t CROSS JOIN cte2 WHERE cte1.a = cte2.c",
                "postgres": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t CROSS JOIN cte2 WHERE cte1.a = cte2.c",
                "sqlite": "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, e AS d FROM table2) SELECT b, d AS dd FROM cte1 AS t CROSS JOIN cte2 WHERE cte1.a = cte2.c",
            },
        )

    def test_nullsafe_eq(self):
        self.validate_all(
            "SELECT a IS NOT DISTINCT FROM b",
            read={
                "mysql": "SELECT a <=> b",
                "postgres": "SELECT a IS NOT DISTINCT FROM b",
            },
            write={
                "mysql": "SELECT a <=> b",
                "postgres": "SELECT a IS NOT DISTINCT FROM b",
            },
        )

    def test_nullsafe_neq(self):
        self.validate_all(
            "SELECT a IS DISTINCT FROM b",
            read={
                "postgres": "SELECT a IS DISTINCT FROM b",
            },
            write={
                "mysql": "SELECT NOT a <=> b",
                "postgres": "SELECT a IS DISTINCT FROM b",
            },
        )

    def test_hash_comments(self):
        self.validate_all(
            "SELECT 1 /* arbitrary content,,, until end-of-line */",
            read={
                "mysql": "SELECT 1 # arbitrary content,,, until end-of-line",
                "bigquery": "SELECT 1 # arbitrary content,,, until end-of-line",
                "clickhouse": "SELECT 1 #! arbitrary content,,, until end-of-line",
            },
        )
        self.validate_all(
            """/* comment1 */
SELECT
  x, /* comment2 */
  y /* comment3 */""",
            read={
                "mysql": """SELECT # comment1
  x, # comment2
  y # comment3""",
                "bigquery": """SELECT # comment1
  x, # comment2
  y # comment3""",
                "clickhouse": """SELECT # comment1
  x, # comment2
  y # comment3""",
            },
            pretty=True,
        )

    def test_transactions(self):
        self.validate_all(
            "BEGIN TRANSACTION",
            write={
                "bigquery": "BEGIN TRANSACTION",
                "mysql": "BEGIN",
                "postgres": "BEGIN",
                "presto": "START TRANSACTION",
                "trino": "START TRANSACTION",
                "redshift": "BEGIN",
                "snowflake": "BEGIN",
                "sqlite": "BEGIN TRANSACTION",
                "tsql": "BEGIN TRANSACTION",
            },
        )
        self.validate_all(
            "BEGIN",
            read={
                "presto": "START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE",
                "trino": "START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE",
            },
        )
        self.validate_all(
            "BEGIN",
            read={
                "presto": "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                "trino": "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            },
        )
        self.validate_all(
            "BEGIN IMMEDIATE TRANSACTION",
            write={"sqlite": "BEGIN IMMEDIATE TRANSACTION"},
        )

    def test_merge(self):
        self.validate_all(
            """
            MERGE INTO target USING source ON target.id = source.id
                WHEN NOT MATCHED THEN INSERT (id) values (source.id)
            """,
            write={
                "bigquery": "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)",
                "snowflake": "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)",
                "spark": "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)",
            },
        )
        self.validate_all(
            """
            MERGE INTO target USING source ON target.id = source.id
                WHEN MATCHED AND source.is_deleted = 1 THEN DELETE
                WHEN MATCHED THEN UPDATE SET val = source.val
                WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)
            """,
            write={
                "bigquery": "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND source.is_deleted = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET val = source.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
                "snowflake": "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND source.is_deleted = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET val = source.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
                "spark": "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND source.is_deleted = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET val = source.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
            },
        )
        self.validate_all(
            """
            MERGE INTO target USING source ON target.id = source.id
                WHEN MATCHED THEN UPDATE *
                WHEN NOT MATCHED THEN INSERT *
            """,
            write={
                "spark": "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE * WHEN NOT MATCHED THEN INSERT *",
            },
        )
        self.validate_all(
            """
            MERGE a b USING c d ON b.id = d.id
            WHEN MATCHED AND EXISTS (
                SELECT b.name
                EXCEPT
                SELECT d.name
            )
            THEN UPDATE SET b.name = d.name
            """,
            write={
                "bigquery": "MERGE INTO a AS b USING c AS d ON b.id = d.id WHEN MATCHED AND EXISTS(SELECT b.name EXCEPT DISTINCT SELECT d.name) THEN UPDATE SET b.name = d.name",
                "snowflake": "MERGE INTO a AS b USING c AS d ON b.id = d.id WHEN MATCHED AND EXISTS(SELECT b.name EXCEPT SELECT d.name) THEN UPDATE SET b.name = d.name",
                "spark": "MERGE INTO a AS b USING c AS d ON b.id = d.id WHEN MATCHED AND EXISTS(SELECT b.name EXCEPT SELECT d.name) THEN UPDATE SET b.name = d.name",
            },
        )

    def test_substring(self):
        self.validate_all(
            "SUBSTR('123456', 2, 3)",
            write={
                "bigquery": "SUBSTR('123456', 2, 3)",
                "oracle": "SUBSTR('123456', 2, 3)",
                "postgres": "SUBSTR('123456', 2, 3)",
            },
        )
        self.validate_all(
            "SUBSTRING('123456', 2, 3)",
            write={
                "bigquery": "SUBSTRING('123456', 2, 3)",
                "oracle": "SUBSTR('123456', 2, 3)",
                "postgres": "SUBSTRING('123456' FROM 2 FOR 3)",
            },
        )

    def test_logarithm(self):
        self.validate_all(
            "LOG(x)",
            read={
                "duckdb": "LOG(x)",
                "postgres": "LOG(x)",
                "redshift": "LOG(x)",
                "sqlite": "LOG(x)",
                "teradata": "LOG(x)",
            },
        )
        self.validate_all(
            "LN(x)",
            read={
                "bigquery": "LOG(x)",
                "clickhouse": "LOG(x)",
                "databricks": "LOG(x)",
                "drill": "LOG(x)",
                "hive": "LOG(x)",
                "mysql": "LOG(x)",
                "tsql": "LOG(x)",
            },
        )
        self.validate_all(
            "LOG(b, n)",
            read={
                "bigquery": "LOG(n, b)",
                "databricks": "LOG(b, n)",
                "drill": "LOG(b, n)",
                "hive": "LOG(b, n)",
                "mysql": "LOG(b, n)",
                "oracle": "LOG(b, n)",
                "postgres": "LOG(b, n)",
                "snowflake": "LOG(b, n)",
                "spark": "LOG(b, n)",
                "sqlite": "LOG(b, n)",
                "tsql": "LOG(n, b)",
            },
        )

    def test_count_if(self):
        self.validate_identity("COUNT_IF(DISTINCT cond)")

        self.validate_all(
            "SELECT COUNT_IF(cond) FILTER", write={"": "SELECT COUNT_IF(cond) AS FILTER"}
        )
        self.validate_all(
            "SELECT COUNT_IF(col % 2 = 0) FROM foo",
            write={
                "": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
                "databricks": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
                "presto": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
                "snowflake": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
                "sqlite": "SELECT SUM(CASE WHEN col % 2 = 0 THEN 1 ELSE 0 END) FROM foo",
                "tsql": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
            },
        )
        self.validate_all(
            "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
            read={
                "": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
                "databricks": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
                "tsql": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
            },
            write={
                "": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
                "databricks": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
                "presto": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
                "sqlite": "SELECT SUM(CASE WHEN col % 2 = 0 THEN 1 ELSE 0 END) FILTER(WHERE col < 1000) FROM foo",
                "tsql": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
            },
        )
