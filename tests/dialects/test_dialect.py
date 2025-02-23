import unittest

from sqlglot import (
    Dialect,
    Dialects,
    ErrorLevel,
    ParseError,
    TokenError,
    UnsupportedError,
    exp,
    parse_one,
)
from sqlglot.dialects import BigQuery, Hive, Snowflake
from sqlglot.parser import logger as parser_logger


class Validator(unittest.TestCase):
    dialect = None

    def parse_one(self, sql, **kwargs):
        return parse_one(sql, read=self.dialect, **kwargs)

    def validate_identity(
        self, sql, write_sql=None, pretty=False, check_command_warning=False, identify=False
    ):
        if check_command_warning:
            with self.assertLogs(parser_logger) as cm:
                expression = self.parse_one(sql)
                assert f"'{sql[:100]}' contains unsupported syntax" in cm.output[0]
        else:
            expression = self.parse_one(sql)

        self.assertEqual(
            write_sql or sql, expression.sql(dialect=self.dialect, pretty=pretty, identify=identify)
        )
        return expression

    def validate_all(self, sql, read=None, write=None, pretty=False, identify=False):
        """
        Validate that:
        1. Everything in `read` transpiles to `sql`
        2. `sql` transpiles to everything in `write`

        Args:
            sql (str): Main SQL expression
            read (dict): Mapping of dialect -> SQL
            write (dict): Mapping of dialect -> SQL
            pretty (bool): prettify both read and write
            identify (bool): quote identifiers in both read and write
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
        dialect_by_key = Dialect.classes
        for dialect in Dialects:
            self.assertIsNotNone(Dialect[dialect])
            self.assertIsNotNone(Dialect.get(dialect))
            self.assertIsNotNone(Dialect.get_or_raise(dialect))
            self.assertIsNotNone(Dialect[dialect.value])
            self.assertIn(dialect, dialect_by_key)

    def test_lazy_load(self):
        import subprocess

        code = "import sqlglot; assert len(sqlglot.Dialect._classes) == 1; print('Success')"
        result = subprocess.run(["python", "-c", code], capture_output=True, text=True)
        assert "Success" in result.stdout

    def test_get_or_raise(self):
        self.assertIsInstance(Dialect.get_or_raise(Hive), Hive)
        self.assertIsInstance(Dialect.get_or_raise(Hive()), Hive)
        self.assertIsInstance(Dialect.get_or_raise("hive"), Hive)

        with self.assertRaises(ValueError):
            Dialect.get_or_raise(1)

        default_mysql = Dialect.get_or_raise("mysql")
        self.assertEqual(default_mysql.normalization_strategy, "CASE_SENSITIVE")

        lowercase_mysql = Dialect.get_or_raise("mysql,normalization_strategy=lowercase")
        self.assertEqual(lowercase_mysql.normalization_strategy, "LOWERCASE")

        lowercase_mysql = Dialect.get_or_raise("mysql, normalization_strategy = lowercase")
        self.assertEqual(lowercase_mysql.normalization_strategy.value, "LOWERCASE")

        with self.assertRaises(AttributeError) as cm:
            Dialect.get_or_raise("mysql, normalization_strategy")

        self.assertEqual(str(cm.exception), "'bool' object has no attribute 'upper'")

        with self.assertRaises(ValueError) as cm:
            Dialect.get_or_raise("myqsl")

        self.assertEqual(str(cm.exception), "Unknown dialect 'myqsl'. Did you mean mysql?")

        with self.assertRaises(ValueError) as cm:
            Dialect.get_or_raise("asdfjasodiufjsd")

        self.assertEqual(str(cm.exception), "Unknown dialect 'asdfjasodiufjsd'.")

        oracle_with_settings = Dialect.get_or_raise(
            "oracle, normalization_strategy = lowercase, version = 19.5"
        )
        self.assertEqual(oracle_with_settings.normalization_strategy.value, "LOWERCASE")
        self.assertEqual(oracle_with_settings.settings, {"version": "19.5"})

        bool_settings = Dialect.get_or_raise("oracle, s1=TruE, s2=1, s3=FaLse, s4=0, s5=nonbool")
        self.assertEqual(
            bool_settings.settings,
            {"s1": True, "s2": True, "s3": False, "s4": False, "s5": "nonbool"},
        )

    def test_compare_dialects(self):
        bigquery_class = Dialect["bigquery"]
        bigquery_object = BigQuery()
        bigquery_string = "bigquery"

        snowflake_class = Dialect["snowflake"]
        snowflake_object = Snowflake()
        snowflake_string = "snowflake"

        self.assertEqual(snowflake_class, snowflake_class)
        self.assertEqual(snowflake_class, snowflake_object)
        self.assertEqual(snowflake_class, snowflake_string)
        self.assertEqual(snowflake_object, snowflake_object)
        self.assertEqual(snowflake_object, snowflake_string)

        self.assertNotEqual(snowflake_class, bigquery_class)
        self.assertNotEqual(snowflake_class, bigquery_object)
        self.assertNotEqual(snowflake_class, bigquery_string)
        self.assertNotEqual(snowflake_object, bigquery_object)
        self.assertNotEqual(snowflake_object, bigquery_string)

        self.assertTrue(snowflake_class in {"snowflake", "bigquery"})
        self.assertTrue(snowflake_object in {"snowflake", "bigquery"})
        self.assertFalse(snowflake_class in {"bigquery", "redshift"})
        self.assertFalse(snowflake_object in {"bigquery", "redshift"})

    def test_compare_dialect_versions(self):
        ddb_v1 = Dialect.get_or_raise("duckdb, version=1.0")
        ddb_v1_2 = Dialect.get_or_raise("duckdb, foo=bar, version=1.0")
        ddb_v2 = Dialect.get_or_raise("duckdb, version=2.2.4")
        ddb_latest = Dialect.get_or_raise("duckdb")

        self.assertTrue(ddb_latest.version > ddb_v2.version)
        self.assertTrue(ddb_v1.version < ddb_v2.version)

        self.assertTrue(ddb_v1.version == ddb_v1_2.version)
        self.assertTrue(ddb_latest.version == Dialect.get_or_raise("duckdb").version)

    def test_cast(self):
        self.validate_all(
            "CAST(a AS TEXT)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "clickhouse": "CAST(a AS Nullable(String))",
                "drill": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
                "materialize": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS CHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR(MAX))",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
                "tsql": "CAST(a AS VARCHAR(MAX))",
                "doris": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS BINARY(4))",
            write={
                "bigquery": "CAST(a AS BYTES)",
                "clickhouse": "CAST(a AS Nullable(BINARY(4)))",
                "drill": "CAST(a AS VARBINARY(4))",
                "duckdb": "CAST(a AS BLOB(4))",
                "materialize": "CAST(a AS BYTEA(4))",
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
                "clickhouse": "CAST(a AS Nullable(String))",
                "duckdb": "CAST(a AS BLOB(4))",
                "materialize": "CAST(a AS BYTEA(4))",
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
                "clickhouse": "CAST(map('a', '1') AS Map(String, Nullable(String)))",
            },
        )
        self.validate_all(
            "CAST(ARRAY(1, 2) AS ARRAY<TINYINT>)",
            write={
                "clickhouse": "CAST([1, 2] AS Array(Nullable(Int8)))",
            },
        )
        self.validate_all(
            "CAST((1, 2, 3, 4) AS STRUCT<a: TINYINT, b: SMALLINT, c: INT, d: BIGINT>)",
            write={
                "clickhouse": "CAST((1, 2, 3, 4) AS Tuple(a Nullable(Int8), b Nullable(Int16), c Nullable(Int32), d Nullable(Int64)))",
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
                "materialize": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS CHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS CLOB)",
                "postgres": "CAST(a AS TEXT)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR(MAX))",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
                "tsql": "CAST(a AS VARCHAR(MAX))",
                "doris": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "drill": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
                "materialize": "CAST(a AS VARCHAR)",
                "mysql": "CAST(a AS CHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS VARCHAR2)",
                "postgres": "CAST(a AS VARCHAR)",
                "presto": "CAST(a AS VARCHAR)",
                "redshift": "CAST(a AS VARCHAR)",
                "snowflake": "CAST(a AS VARCHAR)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS VARCHAR)",
                "tsql": "CAST(a AS VARCHAR)",
                "doris": "CAST(a AS VARCHAR)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR(3))",
            write={
                "bigquery": "CAST(a AS STRING)",
                "drill": "CAST(a AS VARCHAR(3))",
                "duckdb": "CAST(a AS TEXT(3))",
                "materialize": "CAST(a AS VARCHAR(3))",
                "mysql": "CAST(a AS CHAR(3))",
                "hive": "CAST(a AS VARCHAR(3))",
                "oracle": "CAST(a AS VARCHAR2(3))",
                "postgres": "CAST(a AS VARCHAR(3))",
                "presto": "CAST(a AS VARCHAR(3))",
                "redshift": "CAST(a AS VARCHAR(3))",
                "snowflake": "CAST(a AS VARCHAR(3))",
                "spark": "CAST(a AS VARCHAR(3))",
                "starrocks": "CAST(a AS VARCHAR(3))",
                "tsql": "CAST(a AS VARCHAR(3))",
                "doris": "CAST(a AS VARCHAR(3))",
            },
        )
        self.validate_all(
            "CAST(a AS SMALLINT)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "drill": "CAST(a AS INTEGER)",
                "duckdb": "CAST(a AS SMALLINT)",
                "materialize": "CAST(a AS SMALLINT)",
                "mysql": "CAST(a AS SIGNED)",
                "hive": "CAST(a AS SMALLINT)",
                "oracle": "CAST(a AS SMALLINT)",
                "postgres": "CAST(a AS SMALLINT)",
                "presto": "CAST(a AS SMALLINT)",
                "redshift": "CAST(a AS SMALLINT)",
                "snowflake": "CAST(a AS SMALLINT)",
                "spark": "CAST(a AS SMALLINT)",
                "sqlite": "CAST(a AS INTEGER)",
                "starrocks": "CAST(a AS SMALLINT)",
                "doris": "CAST(a AS SMALLINT)",
            },
        )
        self.validate_all(
            "CAST(a AS DOUBLE)",
            read={
                "postgres": "CAST(a AS DOUBLE PRECISION)",
                "redshift": "CAST(a AS DOUBLE PRECISION)",
            },
            write={
                "bigquery": "CAST(a AS FLOAT64)",
                "clickhouse": "CAST(a AS Nullable(Float64))",
                "doris": "CAST(a AS DOUBLE)",
                "drill": "CAST(a AS DOUBLE)",
                "duckdb": "CAST(a AS DOUBLE)",
                "materialize": "CAST(a AS DOUBLE PRECISION)",
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
                "doris": "CAST(a AS DATETIME)",
                "mysql": "CAST(a AS DATETIME)",
            },
        )
        self.validate_all(
            "CAST(a AS TIMESTAMPTZ)",
            write={
                "starrocks": "TIMESTAMP(a)",
                "redshift": "CAST(a AS TIMESTAMP WITH TIME ZONE)",
                "doris": "CAST(a AS DATETIME)",
                "mysql": "TIMESTAMP(a)",
            },
        )
        self.validate_all("CAST(a AS TINYINT)", write={"oracle": "CAST(a AS SMALLINT)"})
        self.validate_all("CAST(a AS SMALLINT)", write={"oracle": "CAST(a AS SMALLINT)"})
        self.validate_all("CAST(a AS BIGINT)", write={"oracle": "CAST(a AS INT)"})
        self.validate_all("CAST(a AS INT)", write={"oracle": "CAST(a AS INT)"})
        self.validate_all(
            "CAST(a AS DECIMAL)",
            read={"oracle": "CAST(a AS NUMBER)"},
            write={"oracle": "CAST(a AS NUMBER)"},
        )
        self.validate_all(
            "CAST('127.0.0.1/32' AS INET)",
            read={"postgres": "INET '127.0.0.1/32'"},
        )

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE a LIKE b",
            write={
                "": "CREATE TABLE a LIKE b",
                "bigquery": "CREATE TABLE a LIKE b",
                "clickhouse": "CREATE TABLE a AS b",
                "databricks": "CREATE TABLE a LIKE b",
                "doris": "CREATE TABLE a LIKE b",
                "drill": "CREATE TABLE a AS SELECT * FROM b LIMIT 0",
                "duckdb": "CREATE TABLE a AS SELECT * FROM b LIMIT 0",
                "hive": "CREATE TABLE a LIKE b",
                "mysql": "CREATE TABLE a LIKE b",
                "oracle": "CREATE TABLE a LIKE b",
                "postgres": "CREATE TABLE a (LIKE b)",
                "presto": "CREATE TABLE a (LIKE b)",
                "redshift": "CREATE TABLE a (LIKE b)",
                "snowflake": "CREATE TABLE a LIKE b",
                "spark": "CREATE TABLE a LIKE b",
                "sqlite": "CREATE TABLE a AS SELECT * FROM b LIMIT 0",
                "trino": "CREATE TABLE a (LIKE b)",
                "tsql": "SELECT TOP 0 * INTO a FROM b AS temp",
            },
        )

    def test_heredoc_strings(self):
        for dialect in ("clickhouse", "postgres", "redshift"):
            # Invalid matching tag
            with self.assertRaises(TokenError):
                parse_one("SELECT $tag1$invalid heredoc string$tag2$", dialect=dialect)

            # Unmatched tag
            with self.assertRaises(TokenError):
                parse_one("SELECT $tag1$invalid heredoc string", dialect=dialect)

            # Without tag
            self.validate_all(
                "SELECT 'this is a heredoc string'",
                read={
                    dialect: "SELECT $$this is a heredoc string$$",
                },
            )
            self.validate_all(
                "SELECT ''",
                read={
                    dialect: "SELECT $$$$",
                },
            )

            # With tag
            self.validate_all(
                "SELECT 'this is also a heredoc string'",
                read={
                    dialect: "SELECT $foo$this is also a heredoc string$foo$",
                },
            )
            self.validate_all(
                "SELECT ''",
                read={
                    dialect: "SELECT $foo$$foo$",
                },
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

    def test_is_ascii(self):
        self.validate_all(
            "SELECT IS_ASCII(x)",
            write={
                "": "SELECT IS_ASCII(x)",
                "sqlite": "SELECT (NOT x GLOB CAST(x'2a5b5e012d7f5d2a' AS TEXT))",
                "mysql": "SELECT REGEXP_LIKE(x, '^[[:ascii:]]*$')",
                "postgres": "SELECT (x ~ '^[[:ascii:]]*$')",
                "tsql": "SELECT (PATINDEX(CONVERT(VARCHAR(MAX), 0x255b5e002d7f5d25) COLLATE Latin1_General_BIN, x) = 0)",
                "oracle": "SELECT NVL(REGEXP_LIKE(x, '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)",
            },
        )

    def test_nvl2(self):
        self.validate_all(
            "SELECT NVL2(a, b, c)",
            write={
                "": "SELECT NVL2(a, b, c)",
                "bigquery": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "clickhouse": "SELECT CASE WHEN NOT (a IS NULL) THEN b ELSE c END",
                "databricks": "SELECT NVL2(a, b, c)",
                "doris": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "drill": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "duckdb": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "hive": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "mysql": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "oracle": "SELECT NVL2(a, b, c)",
                "postgres": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "presto": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "redshift": "SELECT NVL2(a, b, c)",
                "snowflake": "SELECT NVL2(a, b, c)",
                "spark": "SELECT NVL2(a, b, c)",
                "spark2": "SELECT NVL2(a, b, c)",
                "sqlite": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "starrocks": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "teradata": "SELECT NVL2(a, b, c)",
                "trino": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
                "tsql": "SELECT CASE WHEN NOT a IS NULL THEN b ELSE c END",
            },
        )
        self.validate_all(
            "SELECT NVL2(a, b)",
            write={
                "": "SELECT NVL2(a, b)",
                "bigquery": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "clickhouse": "SELECT CASE WHEN NOT (a IS NULL) THEN b END",
                "databricks": "SELECT NVL2(a, b)",
                "doris": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "drill": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "duckdb": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "hive": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "mysql": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "oracle": "SELECT NVL2(a, b)",
                "postgres": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "presto": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "redshift": "SELECT NVL2(a, b)",
                "snowflake": "SELECT NVL2(a, b)",
                "spark": "SELECT NVL2(a, b)",
                "spark2": "SELECT NVL2(a, b)",
                "sqlite": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "starrocks": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "teradata": "SELECT NVL2(a, b)",
                "trino": "SELECT CASE WHEN NOT a IS NULL THEN b END",
                "tsql": "SELECT CASE WHEN NOT a IS NULL THEN b END",
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
                "redshift": "TO_TIMESTAMP(x, 'YYYY-MM-DDTHH24:MI:SS')",
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
                "materialize": "TO_TIMESTAMP(x, 'YY')",
                "presto": "DATE_PARSE(x, '%y')",
                "oracle": "TO_TIMESTAMP(x, 'YY')",
                "postgres": "TO_TIMESTAMP(x, 'YY')",
                "redshift": "TO_TIMESTAMP(x, 'YY')",
                "spark": "TO_TIMESTAMP(x, 'yy')",
            },
        )
        self.validate_all(
            "STR_TO_UNIX('2020-01-01', '%Y-%m-%d')",
            write={
                "duckdb": "EPOCH(STRPTIME('2020-01-01', '%Y-%m-%d'))",
                "hive": "UNIX_TIMESTAMP('2020-01-01', 'yyyy-MM-dd')",
                "presto": "TO_UNIXTIME(COALESCE(TRY(DATE_PARSE(CAST('2020-01-01' AS VARCHAR), '%Y-%m-%d')), PARSE_DATETIME(DATE_FORMAT(CAST('2020-01-01' AS TIMESTAMP), '%Y-%m-%d'), 'yyyy-MM-dd')))",
                "starrocks": "UNIX_TIMESTAMP('2020-01-01', '%Y-%m-%d')",
                "doris": "UNIX_TIMESTAMP('2020-01-01', '%Y-%m-%d')",
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
                "doris": "TO_DATE('2020-01-01')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01')",
            write={
                "bigquery": "CAST('2020-01-01' AS DATETIME)",
                "databricks": "CAST('2020-01-01' AS TIMESTAMP)",
                "duckdb": "CAST('2020-01-01' AS TIMESTAMP)",
                "tsql": "CAST('2020-01-01' AS DATETIME2)",
                "mysql": "CAST('2020-01-01' AS DATETIME)",
                "postgres": "CAST('2020-01-01' AS TIMESTAMP)",
                "redshift": "CAST('2020-01-01' AS TIMESTAMP)",
                "snowflake": "CAST('2020-01-01' AS TIMESTAMP)",
                "spark": "CAST('2020-01-01' AS TIMESTAMP)",
                "trino": "CAST('2020-01-01' AS TIMESTAMP)",
                "clickhouse": "CAST('2020-01-01' AS DateTime64(6))",
                "drill": "CAST('2020-01-01' AS TIMESTAMP)",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "presto": "CAST('2020-01-01' AS TIMESTAMP)",
                "sqlite": "'2020-01-01'",
                "doris": "CAST('2020-01-01' AS DATETIME)",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01 12:13:14.123456+00:00')",
            write={
                "mysql": "CAST('2020-01-01 12:13:14.123456+00:00' AS DATETIME(6))",
                "trino": "CAST('2020-01-01 12:13:14.123456+00:00' AS TIMESTAMP(6))",
                "presto": "CAST('2020-01-01 12:13:14.123456+00:00' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01 12:13:14.123-08:00', 'America/Los_Angeles')",
            write={
                "mysql": "TIMESTAMP('2020-01-01 12:13:14.123-08:00')",
                "trino": "CAST('2020-01-01 12:13:14.123-08:00' AS TIMESTAMP(3) WITH TIME ZONE)",
                "presto": "CAST('2020-01-01 12:13:14.123-08:00' AS TIMESTAMP WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01 12:13:14-08:00', 'America/Los_Angeles')",
            write={
                "bigquery": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP)",
                "databricks": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP)",
                "duckdb": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMPTZ)",
                "tsql": "CAST('2020-01-01 12:13:14-08:00' AS DATETIMEOFFSET) AT TIME ZONE 'UTC'",
                "mysql": "TIMESTAMP('2020-01-01 12:13:14-08:00')",
                "postgres": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMPTZ)",
                "redshift": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP WITH TIME ZONE)",
                "snowflake": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMPTZ)",
                "spark": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP)",
                "trino": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP WITH TIME ZONE)",
                "clickhouse": "CAST('2020-01-01 12:13:14' AS DateTime64(6, 'America/Los_Angeles'))",
                "drill": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP)",
                "hive": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP)",
                "presto": "CAST('2020-01-01 12:13:14-08:00' AS TIMESTAMP WITH TIME ZONE)",
                "sqlite": "'2020-01-01 12:13:14-08:00'",
                "doris": "CAST('2020-01-01 12:13:14-08:00' AS DATETIME)",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME(col, 'America/Los_Angeles')",
            write={
                "bigquery": "CAST(col AS TIMESTAMP)",
                "databricks": "CAST(col AS TIMESTAMP)",
                "duckdb": "CAST(col AS TIMESTAMPTZ)",
                "tsql": "CAST(col AS DATETIMEOFFSET) AT TIME ZONE 'UTC'",
                "mysql": "TIMESTAMP(col)",
                "postgres": "CAST(col AS TIMESTAMPTZ)",
                "redshift": "CAST(col AS TIMESTAMP WITH TIME ZONE)",
                "snowflake": "CAST(col AS TIMESTAMPTZ)",
                "spark": "CAST(col AS TIMESTAMP)",
                "trino": "CAST(col AS TIMESTAMP WITH TIME ZONE)",
                "clickhouse": "CAST(col AS DateTime64(6, 'America/Los_Angeles'))",
                "drill": "CAST(col AS TIMESTAMP)",
                "hive": "CAST(col AS TIMESTAMP)",
                "presto": "CAST(col AS TIMESTAMP WITH TIME ZONE)",
                "sqlite": "col",
                "doris": "CAST(col AS DATETIME)",
            },
        )
        self.validate_all(
            "TIME_STR_TO_UNIX('2020-01-01')",
            write={
                "duckdb": "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
                "hive": "UNIX_TIMESTAMP('2020-01-01')",
                "mysql": "UNIX_TIMESTAMP('2020-01-01')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%m-%d %T'))",
                "doris": "UNIX_TIMESTAMP('2020-01-01')",
            },
        )
        self.validate_all(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            write={
                "bigquery": "FORMAT_DATE('%Y-%m-%d', x)",
                "drill": "TO_CHAR(x, 'yyyy-MM-dd')",
                "duckdb": "STRFTIME(x, '%Y-%m-%d')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd')",
                "materialize": "TO_CHAR(x, 'YYYY-MM-DD')",
                "oracle": "TO_CHAR(x, 'YYYY-MM-DD')",
                "postgres": "TO_CHAR(x, 'YYYY-MM-DD')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d')",
                "redshift": "TO_CHAR(x, 'YYYY-MM-DD')",
                "doris": "DATE_FORMAT(x, '%Y-%m-%d')",
            },
        )
        self.validate_all(
            "TIME_TO_STR(a, '%Y-%m-%d %H:%M:%S.%f')",
            write={
                "redshift": "TO_CHAR(a, 'YYYY-MM-DD HH24:MI:SS.US')",
                "tsql": "FORMAT(a, 'yyyy-MM-dd HH:mm:ss.ffffff')",
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
                "doris": "CAST(x AS STRING)",
            },
        )
        self.validate_all(
            "TIME_TO_UNIX(x)",
            write={
                "drill": "UNIX_TIMESTAMP(x)",
                "duckdb": "EPOCH(x)",
                "hive": "UNIX_TIMESTAMP(x)",
                "presto": "TO_UNIXTIME(x)",
                "doris": "UNIX_TIMESTAMP(x)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE_STR(x)",
            write={
                "duckdb": "SUBSTRING(CAST(x AS TEXT), 1, 10)",
                "hive": "SUBSTRING(CAST(x AS STRING), 1, 10)",
                "presto": "SUBSTRING(CAST(x AS VARCHAR), 1, 10)",
                "doris": "SUBSTRING(CAST(x AS STRING), 1, 10)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE(x)",
            write={
                "bigquery": "CAST(x AS DATE)",
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "materialize": "CAST(x AS DATE)",
                "postgres": "CAST(x AS DATE)",
                "presto": "CAST(CAST(x AS TIMESTAMP) AS DATE)",
                "snowflake": "TO_DATE(x)",
                "doris": "TO_DATE(x)",
                "mysql": "DATE(x)",
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
                "doris": "FROM_UNIXTIME(x, y)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME(x)",
            write={
                "duckdb": "TO_TIMESTAMP(x)",
                "hive": "FROM_UNIXTIME(x)",
                "oracle": "TO_DATE('1970-01-01', 'YYYY-MM-DD') + (x / 86400)",
                "materialize": "TO_TIMESTAMP(x)",
                "postgres": "TO_TIMESTAMP(x)",
                "presto": "FROM_UNIXTIME(x)",
                "starrocks": "FROM_UNIXTIME(x)",
                "doris": "FROM_UNIXTIME(x)",
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
                "snowflake": "DATEADD('DAY', 1, x)",
            },
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 DAY)",
                "drill": "DATE_ADD(x, INTERVAL 1 DAY)",
                "duckdb": "x + INTERVAL 1 DAY",
                "hive": "DATE_ADD(x, 1)",
                "materialize": "x + INTERVAL '1 DAY'",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "postgres": "x + INTERVAL '1 DAY'",
                "presto": "DATE_ADD('DAY', 1, x)",
                "snowflake": "DATEADD(DAY, 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "sqlite": "DATE(x, '1 DAY')",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
                "tsql": "DATEADD(DAY, 1, x)",
                "doris": "DATE_ADD(x, INTERVAL 1 DAY)",
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
                "presto": "DATE_ADD('DAY', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
                "doris": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('DAY', x)",
            read={
                "bigquery": "DATE_TRUNC(x, day)",
                "spark": "TRUNC(x, 'day')",
            },
            write={
                "bigquery": "DATE_TRUNC(x, DAY)",
                "duckdb": "DATE_TRUNC('DAY', x)",
                "mysql": "DATE(x)",
                "presto": "DATE_TRUNC('DAY', x)",
                "materialize": "DATE_TRUNC('DAY', x)",
                "postgres": "DATE_TRUNC('DAY', x)",
                "snowflake": "DATE_TRUNC('DAY', x)",
                "starrocks": "DATE_TRUNC('DAY', x)",
                "spark": "TRUNC(x, 'DAY')",
                "doris": "DATE_TRUNC(x, 'DAY')",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(x, DAY)",
            read={
                "bigquery": "TIMESTAMP_TRUNC(x, day)",
                "duckdb": "DATE_TRUNC('day', x)",
                "materialize": "DATE_TRUNC('day', x)",
                "presto": "DATE_TRUNC('day', x)",
                "postgres": "DATE_TRUNC('day', x)",
                "snowflake": "DATE_TRUNC('day', x)",
                "starrocks": "DATE_TRUNC('day', x)",
                "spark": "DATE_TRUNC('day', x)",
                "doris": "DATE_TRUNC('day', x)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('DAY', CAST(x AS DATE))",
            read={
                "presto": "DATE_TRUNC('DAY', x::DATE)",
                "snowflake": "DATE_TRUNC('DAY', x::DATE)",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(CAST(x AS DATE), DAY)",
            read={"postgres": "DATE_TRUNC('day', x::DATE)"},
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(CAST(x AS DATE), DAY)",
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
            "DATE_TRUNC('YEAR', x)",
            read={
                "bigquery": "DATE_TRUNC(x, year)",
                "spark": "TRUNC(x, 'year')",
            },
            write={
                "bigquery": "DATE_TRUNC(x, YEAR)",
                "materialize": "DATE_TRUNC('YEAR', x)",
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
                "postgres": "DATE_TRUNC('YEAR', x)",
                "snowflake": "DATE_TRUNC('YEAR', x)",
                "starrocks": "DATE_TRUNC('YEAR', x)",
                "spark": "TRUNC(x, 'YEAR')",
                "doris": "DATE_TRUNC(x, 'YEAR')",
            },
        )
        self.validate_all(
            "TIMESTAMP_TRUNC(x, YEAR)",
            read={
                "bigquery": "TIMESTAMP_TRUNC(x, year)",
                "materialize": "DATE_TRUNC('YEAR', x)",
                "postgres": "DATE_TRUNC(year, x)",
                "spark": "DATE_TRUNC('year', x)",
                "snowflake": "DATE_TRUNC(year, x)",
                "starrocks": "DATE_TRUNC('year', x)",
            },
            write={
                "bigquery": "TIMESTAMP_TRUNC(x, YEAR)",
                "spark": "DATE_TRUNC('YEAR', x)",
                "doris": "DATE_TRUNC(x, 'YEAR')",
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
            write={
                "drill": "TO_DATE(x, 'yyyy-MM-dd''T''HH:mm:ss')",
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%dT%T') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy-MM-ddTHH:mm:ss')",
                "doris": "STR_TO_DATE(x, '%Y-%m-%dT%T')",
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
                "doris": "STR_TO_DATE(x, '%Y-%m-%d')",
            },
        )
        self.validate_all(
            "DATE_STR_TO_DATE(x)",
            write={
                "drill": "CAST(x AS DATE)",
                "duckdb": "CAST(x AS DATE)",
                "hive": "CAST(x AS DATE)",
                "presto": "CAST(x AS DATE)",
                "spark": "CAST(x AS DATE)",
                "sqlite": "x",
                "tsql": "CAST(x AS DATE)",
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
                "mysql": "DATE_ADD('2021-02-01', INTERVAL 1 DAY)",
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
                "presto": "DATE_ADD('DAY', 1, CAST(CAST(CURRENT_DATE AS TIMESTAMP) AS DATE))",
                "hive": "DATE_ADD(CURRENT_DATE, 1)",
            },
        )
        self.validate_all(
            "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            write={
                "drill": "DATE_ADD(CAST('2020-01-01' AS DATE), INTERVAL 1 DAY)",
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
                "presto": "DATE_ADD('DAY', 1, CAST('2020-01-01' AS DATE))",
                "spark": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            },
        )
        self.validate_all(
            "TIMESTAMP '2022-01-01'",
            write={
                "drill": "CAST('2022-01-01' AS TIMESTAMP)",
                "mysql": "CAST('2022-01-01' AS DATETIME)",
                "starrocks": "CAST('2022-01-01' AS DATETIME)",
                "hive": "CAST('2022-01-01' AS TIMESTAMP)",
                "doris": "CAST('2022-01-01' AS DATETIME)",
            },
        )
        self.validate_all(
            "TIMESTAMP('2022-01-01')",
            write={
                "mysql": "TIMESTAMP('2022-01-01')",
                "starrocks": "TIMESTAMP('2022-01-01')",
                "hive": "TIMESTAMP('2022-01-01')",
                "doris": "TIMESTAMP('2022-01-01')",
            },
        )

        self.validate_all(
            "TIMESTAMP_TRUNC(x, DAY, 'UTC')",
            write={
                "": "TIMESTAMP_TRUNC(x, DAY, 'UTC')",
                "duckdb": "DATE_TRUNC('DAY', x)",
                "materialize": "DATE_TRUNC('DAY', x, 'UTC')",
                "presto": "DATE_TRUNC('DAY', x)",
                "postgres": "DATE_TRUNC('DAY', x, 'UTC')",
                "snowflake": "DATE_TRUNC('DAY', x)",
                "databricks": "DATE_TRUNC('DAY', x)",
                "clickhouse": "DATE_TRUNC('DAY', x, 'UTC')",
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
                        "presto",
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
                    )
                },
            )
            self.validate_all(
                f"{unit}(TS_OR_DS_TO_DATE(x))",
                write={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "mysql",
                        "doris",
                        "starrocks",
                    )
                },
            )
            self.validate_all(
                f"{unit}(CAST(x AS DATE))",
                read={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "mysql",
                        "doris",
                        "starrocks",
                    )
                },
            )

    def test_array(self):
        self.validate_all(
            "ARRAY(0, 1, 2)",
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "[0, 1, 2]",
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
                "duckdb": "LIST_SUM([1, 2])",
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
        self.validate_identity(
            "SELECT c FROM t ORDER BY a, b,",
            "SELECT c FROM t ORDER BY a, b",
        )

        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "bigquery": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            },
        )

        order_by_all_sql = "SELECT * FROM t ORDER BY ALL"
        self.validate_identity(order_by_all_sql).find(exp.Ordered).this.assert_is(exp.Column)

        for dialect in ("duckdb", "spark", "databricks"):
            with self.subTest(f"Testing ORDER BY ALL in {dialect}"):
                parse_one(order_by_all_sql, read=dialect).find(exp.Ordered).this.assert_is(exp.Var)

    def test_json(self):
        self.validate_all(
            """JSON_EXTRACT(x, '$["a b"]')""",
            write={
                "": """JSON_EXTRACT(x, '$["a b"]')""",
                "bigquery": """JSON_EXTRACT(x, '$[\\'a b\\']')""",
                "clickhouse": "JSONExtractString(x, 'a b')",
                "duckdb": """x -> '$."a b"'""",
                "mysql": """JSON_EXTRACT(x, '$."a b"')""",
                "postgres": "JSON_EXTRACT_PATH(x, 'a b')",
                "presto": """JSON_EXTRACT(x, '$["a b"]')""",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'a b')",
                "snowflake": """GET_PATH(x, '["a b"]')""",
                "spark": """GET_JSON_OBJECT(x, '$[\\'a b\\']')""",
                "sqlite": """x -> '$."a b"'""",
                "trino": """JSON_EXTRACT(x, '$["a b"]')""",
                "tsql": """ISNULL(JSON_QUERY(x, '$."a b"'), JSON_VALUE(x, '$."a b"'))""",
            },
        )
        self.validate_all(
            "JSON_EXTRACT(x, '$.y')",
            read={
                "bigquery": "JSON_EXTRACT(x, '$.y')",
                "duckdb": "x -> 'y'",
                "doris": "JSON_EXTRACT(x, '$.y')",
                "mysql": "JSON_EXTRACT(x, '$.y')",
                "postgres": "x->'y'",
                "presto": "JSON_EXTRACT(x, '$.y')",
                "snowflake": "GET_PATH(x, 'y')",
                "sqlite": "x -> '$.y'",
                "starrocks": "x -> '$.y'",
            },
            write={
                "bigquery": "JSON_EXTRACT(x, '$.y')",
                "clickhouse": "JSONExtractString(x, 'y')",
                "doris": "JSON_EXTRACT(x, '$.y')",
                "duckdb": "x -> '$.y'",
                "mysql": "JSON_EXTRACT(x, '$.y')",
                "oracle": "JSON_EXTRACT(x, '$.y')",
                "postgres": "JSON_EXTRACT_PATH(x, 'y')",
                "presto": "JSON_EXTRACT(x, '$.y')",
                "snowflake": "GET_PATH(x, 'y')",
                "spark": "GET_JSON_OBJECT(x, '$.y')",
                "sqlite": "x -> '$.y'",
                "starrocks": "x -> '$.y'",
                "tsql": "ISNULL(JSON_QUERY(x, '$.y'), JSON_VALUE(x, '$.y'))",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_SCALAR(x, '$.y')",
            read={
                "bigquery": "JSON_EXTRACT_SCALAR(x, '$.y')",
                "clickhouse": "JSONExtractString(x, 'y')",
                "duckdb": "x ->> 'y'",
                "postgres": "x ->> 'y'",
                "presto": "JSON_EXTRACT_SCALAR(x, '$.y')",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "spark": "GET_JSON_OBJECT(x, '$.y')",
                "snowflake": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "sqlite": "x ->> '$.y'",
            },
            write={
                "bigquery": "JSON_EXTRACT_SCALAR(x, '$.y')",
                "clickhouse": "JSONExtractString(x, 'y')",
                "duckdb": "x ->> '$.y'",
                "postgres": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "presto": "JSON_EXTRACT_SCALAR(x, '$.y')",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "snowflake": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "spark": "GET_JSON_OBJECT(x, '$.y')",
                "sqlite": "x ->> '$.y'",
                "tsql": "ISNULL(JSON_QUERY(x, '$.y'), JSON_VALUE(x, '$.y'))",
            },
        )
        self.validate_all(
            "JSON_EXTRACT(x, '$.y[0].z')",
            read={
                "bigquery": "JSON_EXTRACT(x, '$.y[0].z')",
                "duckdb": "x -> '$.y[0].z'",
                "doris": "JSON_EXTRACT(x, '$.y[0].z')",
                "mysql": "JSON_EXTRACT(x, '$.y[0].z')",
                "presto": "JSON_EXTRACT(x, '$.y[0].z')",
                "snowflake": "GET_PATH(x, 'y[0].z')",
                "sqlite": "x -> '$.y[0].z'",
                "starrocks": "x -> '$.y[0].z'",
            },
            write={
                "bigquery": "JSON_EXTRACT(x, '$.y[0].z')",
                "clickhouse": "JSONExtractString(x, 'y', 1, 'z')",
                "doris": "JSON_EXTRACT(x, '$.y[0].z')",
                "duckdb": "x -> '$.y[0].z'",
                "mysql": "JSON_EXTRACT(x, '$.y[0].z')",
                "oracle": "JSON_EXTRACT(x, '$.y[0].z')",
                "postgres": "JSON_EXTRACT_PATH(x, 'y', '0', 'z')",
                "presto": "JSON_EXTRACT(x, '$.y[0].z')",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'y', '0', 'z')",
                "snowflake": "GET_PATH(x, 'y[0].z')",
                "spark": "GET_JSON_OBJECT(x, '$.y[0].z')",
                "sqlite": "x -> '$.y[0].z'",
                "starrocks": "x -> '$.y[0].z'",
                "tsql": "ISNULL(JSON_QUERY(x, '$.y[0].z'), JSON_VALUE(x, '$.y[0].z'))",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_SCALAR(x, '$.y[0].z')",
            read={
                "bigquery": "JSON_EXTRACT_SCALAR(x, '$.y[0].z')",
                "clickhouse": "JSONExtractString(x, 'y', 1, 'z')",
                "duckdb": "x ->> '$.y[0].z'",
                "presto": "JSON_EXTRACT_SCALAR(x, '$.y[0].z')",
                "snowflake": "JSON_EXTRACT_PATH_TEXT(x, 'y[0].z')",
                "spark": 'GET_JSON_OBJECT(x, "$.y[0].z")',
                "sqlite": "x ->> '$.y[0].z'",
            },
            write={
                "bigquery": "JSON_EXTRACT_SCALAR(x, '$.y[0].z')",
                "clickhouse": "JSONExtractString(x, 'y', 1, 'z')",
                "duckdb": "x ->> '$.y[0].z'",
                "postgres": "JSON_EXTRACT_PATH_TEXT(x, 'y', '0', 'z')",
                "presto": "JSON_EXTRACT_SCALAR(x, '$.y[0].z')",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'y', '0', 'z')",
                "snowflake": "JSON_EXTRACT_PATH_TEXT(x, 'y[0].z')",
                "spark": "GET_JSON_OBJECT(x, '$.y[0].z')",
                "sqlite": "x ->> '$.y[0].z'",
                "tsql": "ISNULL(JSON_QUERY(x, '$.y[0].z'), JSON_VALUE(x, '$.y[0].z'))",
            },
        )
        self.validate_all(
            "JSON_EXTRACT(x, '$.y[*]')",
            write={
                "bigquery": UnsupportedError,
                "clickhouse": UnsupportedError,
                "duckdb": "x -> '$.y[*]'",
                "mysql": "JSON_EXTRACT(x, '$.y[*]')",
                "postgres": UnsupportedError,
                "presto": "JSON_EXTRACT(x, '$.y[*]')",
                "redshift": UnsupportedError,
                "snowflake": UnsupportedError,
                "spark": "GET_JSON_OBJECT(x, '$.y[*]')",
                "sqlite": UnsupportedError,
                "tsql": UnsupportedError,
            },
        )
        self.validate_all(
            "JSON_EXTRACT(x, '$.y[*]')",
            write={
                "bigquery": "JSON_EXTRACT(x, '$.y')",
                "clickhouse": "JSONExtractString(x, 'y')",
                "postgres": "JSON_EXTRACT_PATH(x, 'y')",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'y')",
                "snowflake": "GET_PATH(x, 'y')",
                "sqlite": "x -> '$.y'",
                "tsql": "ISNULL(JSON_QUERY(x, '$.y'), JSON_VALUE(x, '$.y'))",
            },
        )
        self.validate_all(
            "JSON_EXTRACT(x, '$.y.*')",
            write={
                "bigquery": UnsupportedError,
                "clickhouse": UnsupportedError,
                "duckdb": "x -> '$.y.*'",
                "mysql": "JSON_EXTRACT(x, '$.y.*')",
                "postgres": UnsupportedError,
                "presto": "JSON_EXTRACT(x, '$.y.*')",
                "redshift": UnsupportedError,
                "snowflake": UnsupportedError,
                "spark": UnsupportedError,
                "sqlite": UnsupportedError,
                "tsql": UnsupportedError,
            },
        )

        for dialect in ("duckdb", "starrocks"):
            with self.subTest(f"Generating json extraction with digit-prefixed key ({dialect})"):
                self.assertEqual(
                    parse_one("""select '{"0": "v"}' -> '0'""", read=dialect).sql(dialect=dialect),
                    """SELECT '{"0": "v"}' -> '0'""",
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
                "spark": "SELECT a, b FROM x LATERAL VIEW INLINE(ARRAYS_ZIP(y, z)) t AS a, b",
            },
        )
        self.validate_all(
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t (a)",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t(a)",
                "spark": "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            },
        )

        # UNNEST without column alias
        self.validate_all(
            "SELECT * FROM x CROSS JOIN UNNEST(y) AS t",
            write={
                "presto": "SELECT * FROM x CROSS JOIN UNNEST(y) AS t",
                "spark": UnsupportedError,
                "databricks": UnsupportedError,
            },
        )

        # UNNEST MAP Object into multiple columns, using single alias
        self.validate_all(
            "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t (a, b)",
            write={
                "presto": "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a, b)",
                "spark": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a, b",
                "hive": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a, b",
            },
        )

        # Unnest multiple Expression into respective mapped alias
        self.validate_all(
            "SELECT numbers, animals, n, a FROM (SELECT ARRAY(2, 5) AS numbers, ARRAY('dog', 'cat', 'bird') AS animals UNION ALL SELECT ARRAY(7, 8, 9), ARRAY('cow', 'pig')) AS x CROSS JOIN UNNEST(numbers, animals) AS t(n, a)",
            write={
                "presto": "SELECT numbers, animals, n, a FROM (SELECT ARRAY[2, 5] AS numbers, ARRAY['dog', 'cat', 'bird'] AS animals UNION ALL SELECT ARRAY[7, 8, 9], ARRAY['cow', 'pig']) AS x CROSS JOIN UNNEST(numbers, animals) AS t(n, a)",
                "spark": "SELECT numbers, animals, n, a FROM (SELECT ARRAY(2, 5) AS numbers, ARRAY('dog', 'cat', 'bird') AS animals UNION ALL SELECT ARRAY(7, 8, 9), ARRAY('cow', 'pig')) AS x LATERAL VIEW INLINE(ARRAYS_ZIP(numbers, animals)) t AS n, a",
                "hive": UnsupportedError,
            },
        )

        # Unnest column to more then 2 alias (STRUCT)
        self.validate_all(
            "SELECT a, b, c, d, e FROM x CROSS JOIN UNNEST(y) AS t(a, b, c, d)",
            write={
                "presto": "SELECT a, b, c, d, e FROM x CROSS JOIN UNNEST(y) AS t(a, b, c, d)",
                "spark": UnsupportedError,
                "hive": UnsupportedError,
            },
        )

    def test_multiple_chained_unnest(self):
        self.validate_all(
            "SELECT * FROM x CROSS JOIN UNNEST(a) AS j(lista) CROSS JOIN UNNEST(b) AS k(listb) CROSS JOIN UNNEST(c) AS l(listc)",
            write={
                "presto": "SELECT * FROM x CROSS JOIN UNNEST(a) AS j(lista) CROSS JOIN UNNEST(b) AS k(listb) CROSS JOIN UNNEST(c) AS l(listc)",
                "spark": "SELECT * FROM x LATERAL VIEW EXPLODE(a) j AS lista LATERAL VIEW EXPLODE(b) k AS listb LATERAL VIEW EXPLODE(c) l AS listc",
                "hive": "SELECT * FROM x LATERAL VIEW EXPLODE(a) j AS lista LATERAL VIEW EXPLODE(b) k AS listb LATERAL VIEW EXPLODE(c) l AS listc",
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
            "SELECT * FROM a UNION SELECT * FROM b ORDER BY x LIMIT 1",
            write={
                "": "SELECT * FROM a UNION SELECT * FROM b ORDER BY x LIMIT 1",
                "clickhouse": "SELECT * FROM (SELECT * FROM a UNION DISTINCT SELECT * FROM b) AS _l_0 ORDER BY x NULLS FIRST LIMIT 1",
                "tsql": "SELECT TOP 1 * FROM (SELECT * FROM a UNION SELECT * FROM b) AS _l_0 ORDER BY x",
            },
        )

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
                "clickhouse": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "clickhouse": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "clickhouse": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
            write={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "clickhouse": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
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
                "clickhouse": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "clickhouse": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
            write={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "clickhouse": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                "clickhouse": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
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
                "bigquery": "LOWER(x) LIKE LOWER('%y')",
                "clickhouse": "x ILIKE '%y'",
                "drill": "x `ILIKE` '%y'",
                "duckdb": "x ILIKE '%y'",
                "hive": "LOWER(x) LIKE LOWER('%y')",
                "mysql": "LOWER(x) LIKE LOWER('%y')",
                "oracle": "LOWER(x) LIKE LOWER('%y')",
                "postgres": "x ILIKE '%y'",
                "presto": "LOWER(x) LIKE LOWER('%y')",
                "snowflake": "x ILIKE '%y'",
                "spark": "x ILIKE '%y'",
                "sqlite": "LOWER(x) LIKE LOWER('%y')",
                "starrocks": "LOWER(x) LIKE LOWER('%y')",
                "trino": "LOWER(x) LIKE LOWER('%y')",
                "doris": "LOWER(x) LIKE LOWER('%y')",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "athena": "POSITION(needle in haystack)",
                "clickhouse": "POSITION(needle in haystack)",
                "databricks": "POSITION(needle in haystack)",
                "drill": "POSITION(needle in haystack)",
                "duckdb": "POSITION(needle in haystack)",
                "materialize": "POSITION(needle in haystack)",
                "mysql": "POSITION(needle in haystack)",
                "postgres": "POSITION(needle in haystack)",
                "presto": "POSITION(needle in haystack)",
                "redshift": "POSITION(needle in haystack)",
                "risingwave": "POSITION(needle in haystack)",
                "snowflake": "POSITION(needle in haystack)",
                "spark": "POSITION(needle in haystack)",
                "spark2": "POSITION(needle in haystack)",
                "teradata": "POSITION(needle in haystack)",
                "trino": "POSITION(needle in haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "clickhouse": "POSITION(haystack, needle)",
                "databricks": "POSITION(needle, haystack)",
                "snowflake": "POSITION(needle, haystack)",
                "spark2": "POSITION(needle, haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "athena": "STRPOS(haystack, needle)",
                "bigquery": "STRPOS(haystack, needle)",
                "drill": "STRPOS(haystack, needle)",
                "duckdb": "STRPOS(haystack, needle)",
                "postgres": "STRPOS(haystack, needle)",
                "presto": "STRPOS(haystack, needle)",
                "redshift": "STRPOS(haystack, needle)",
                "trino": "STRPOS(haystack, needle)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "bigquery": "INSTR(haystack, needle)",
                "databricks": "INSTR(haystack, needle)",
                "doris": "INSTR(haystack, needle)",
                "duckdb": "INSTR(haystack, needle)",
                "hive": "INSTR(haystack, needle)",
                "mysql": "INSTR(haystack, needle)",
                "oracle": "INSTR(haystack, needle)",
                "spark": "INSTR(haystack, needle)",
                "spark2": "INSTR(haystack, needle)",
                "sqlite": "INSTR(haystack, needle)",
                "starrocks": "INSTR(haystack, needle)",
                "teradata": "INSTR(haystack, needle)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "clickhouse": "LOCATE(needle, haystack)",
                "databricks": "LOCATE(needle, haystack)",
                "doris": "LOCATE(needle, haystack)",
                "hive": "LOCATE(needle, haystack)",
                "mysql": "LOCATE(needle, haystack)",
                "spark": "LOCATE(needle, haystack)",
                "spark2": "LOCATE(needle, haystack)",
                "starrocks": "LOCATE(needle, haystack)",
                "teradata": "LOCATE(needle, haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "athena": "CHARINDEX(needle, haystack)",
                "databricks": "CHARINDEX(needle, haystack)",
                "snowflake": "CHARINDEX(needle, haystack)",
                "tsql": "CHARINDEX(needle, haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle)",
            read={
                "tableau": "FIND(haystack, needle)",
            },
            write={
                "athena": "STRPOS(haystack, needle)",
                "bigquery": "INSTR(haystack, needle)",
                "clickhouse": "POSITION(haystack, needle)",
                "databricks": "LOCATE(needle, haystack)",
                "doris": "LOCATE(needle, haystack)",
                "drill": "STRPOS(haystack, needle)",
                "duckdb": "STRPOS(haystack, needle)",
                "hive": "LOCATE(needle, haystack)",
                "materialize": "POSITION(needle IN haystack)",
                "mysql": "LOCATE(needle, haystack)",
                "oracle": "INSTR(haystack, needle)",
                "postgres": "POSITION(needle IN haystack)",
                "presto": "STRPOS(haystack, needle)",
                "redshift": "POSITION(needle IN haystack)",
                "risingwave": "POSITION(needle IN haystack)",
                "snowflake": "CHARINDEX(needle, haystack)",
                "spark": "LOCATE(needle, haystack)",
                "spark2": "LOCATE(needle, haystack)",
                "sqlite": "INSTR(haystack, needle)",
                "tableau": "FIND(haystack, needle)",
                "teradata": "INSTR(haystack, needle)",
                "trino": "STRPOS(haystack, needle)",
                "tsql": "CHARINDEX(needle, haystack)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position)",
            read={
                "clickhouse": "POSITION(haystack, needle, position)",
                "databricks": "POSITION(needle, haystack, position)",
                "snowflake": "POSITION(needle, haystack, position)",
                "spark": "POSITION(needle, haystack, position)",
                "spark2": "POSITION(needle, haystack, position)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position)",
            read={
                "doris": "LOCATE(needle, haystack, position)",
                "hive": "LOCATE(needle, haystack, position)",
                "mysql": "LOCATE(needle, haystack, position)",
                "spark": "LOCATE(needle, haystack, position)",
                "spark2": "LOCATE(needle, haystack, position)",
                "starrocks": "LOCATE(needle, haystack, position)",
                "teradata": "LOCATE(needle, haystack, position)",
                "clickhouse": "LOCATE(needle, haystack, position)",
                "databricks": "LOCATE(needle, haystack, position)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position)",
            read={
                "bigquery": "INSTR(haystack, needle, position)",
                "doris": "INSTR(haystack, needle, position)",
                "oracle": "INSTR(haystack, needle, position)",
                "teradata": "INSTR(haystack, needle, position)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position)",
            read={
                "databricks": "CHARINDEX(needle, haystack, position)",
                "snowflake": "CHARINDEX(needle, haystack, position)",
                "tsql": "CHARINDEX(needle, haystack, position)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position)",
            write={
                "athena": "IF(STRPOS(SUBSTRING(haystack, position), needle) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle) + position - 1)",
                "bigquery": "INSTR(haystack, needle, position)",
                "clickhouse": "POSITION(haystack, needle, position)",
                "databricks": "LOCATE(needle, haystack, position)",
                "doris": "LOCATE(needle, haystack, position)",
                "drill": "`IF`(STRPOS(SUBSTRING(haystack, position), needle) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle) + position - 1)",
                "duckdb": "CASE WHEN STRPOS(SUBSTRING(haystack, position), needle) = 0 THEN 0 ELSE STRPOS(SUBSTRING(haystack, position), needle) + position - 1 END",
                "hive": "LOCATE(needle, haystack, position)",
                "materialize": "CASE WHEN POSITION(needle IN SUBSTRING(haystack FROM position)) = 0 THEN 0 ELSE POSITION(needle IN SUBSTRING(haystack FROM position)) + position - 1 END",
                "mysql": "LOCATE(needle, haystack, position)",
                "oracle": "INSTR(haystack, needle, position)",
                "postgres": "CASE WHEN POSITION(needle IN SUBSTRING(haystack FROM position)) = 0 THEN 0 ELSE POSITION(needle IN SUBSTRING(haystack FROM position)) + position - 1 END",
                "presto": "IF(STRPOS(SUBSTRING(haystack, position), needle) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle) + position - 1)",
                "redshift": "CASE WHEN POSITION(needle IN SUBSTRING(haystack FROM position)) = 0 THEN 0 ELSE POSITION(needle IN SUBSTRING(haystack FROM position)) + position - 1 END",
                "risingwave": "CASE WHEN POSITION(needle IN SUBSTRING(haystack FROM position)) = 0 THEN 0 ELSE POSITION(needle IN SUBSTRING(haystack FROM position)) + position - 1 END",
                "snowflake": "CHARINDEX(needle, haystack, position)",
                "spark": "LOCATE(needle, haystack, position)",
                "spark2": "LOCATE(needle, haystack, position)",
                "sqlite": "IIF(INSTR(SUBSTRING(haystack, position), needle) = 0, 0, INSTR(SUBSTRING(haystack, position), needle) + position - 1)",
                "tableau": "IF FIND(SUBSTRING(haystack, position), needle) = 0 THEN 0 ELSE FIND(SUBSTRING(haystack, position), needle) + position - 1 END",
                "teradata": "INSTR(haystack, needle, position)",
                "trino": "IF(STRPOS(SUBSTRING(haystack, position), needle) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle) + position - 1)",
                "tsql": "CHARINDEX(needle, haystack, position)",
            },
        )
        self.validate_all(
            "STR_POSITION(haystack, needle, position, occurrence)",
            read={
                "bigquery": "INSTR(haystack, needle, position, occurrence)",
                "oracle": "INSTR(haystack, needle, position, occurrence)",
                "teradata": "INSTR(haystack, needle, position, occurrence)",
            },
            write={
                "bigquery": "INSTR(haystack, needle, position, occurrence)",
                "oracle": "INSTR(haystack, needle, position, occurrence)",
                "presto": "IF(STRPOS(SUBSTRING(haystack, position), needle, occurrence) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle, occurrence) + position - 1)",
                "tableau": "IF FINDNTH(SUBSTRING(haystack, position), needle, occurrence) = 0 THEN 0 ELSE FINDNTH(SUBSTRING(haystack, position), needle, occurrence) + position - 1 END",
                "teradata": "INSTR(haystack, needle, position, occurrence)",
                "trino": "IF(STRPOS(SUBSTRING(haystack, position), needle, occurrence) = 0, 0, STRPOS(SUBSTRING(haystack, position), needle, occurrence) + position - 1)",
            },
        )
        self.validate_all(
            "CONCAT_WS('-', 'a', 'b')",
            write={
                "clickhouse": "CONCAT_WS('-', 'a', 'b')",
                "duckdb": "CONCAT_WS('-', 'a', 'b')",
                "presto": "CONCAT_WS('-', CAST('a' AS VARCHAR), CAST('b' AS VARCHAR))",
                "hive": "CONCAT_WS('-', 'a', 'b')",
                "spark": "CONCAT_WS('-', 'a', 'b')",
                "trino": "CONCAT_WS('-', CAST('a' AS VARCHAR), CAST('b' AS VARCHAR))",
            },
        )

        self.validate_all(
            "CONCAT_WS('-', x)",
            write={
                "clickhouse": "CONCAT_WS('-', x)",
                "duckdb": "CONCAT_WS('-', x)",
                "hive": "CONCAT_WS('-', x)",
                "presto": "CONCAT_WS('-', CAST(x AS VARCHAR))",
                "spark": "CONCAT_WS('-', x)",
                "trino": "CONCAT_WS('-', CAST(x AS VARCHAR))",
            },
        )
        self.validate_all(
            "CONCAT(a)",
            write={
                "clickhouse": "CONCAT(a)",
                "presto": "CAST(a AS VARCHAR)",
                "trino": "CAST(a AS VARCHAR)",
                "tsql": "a",
            },
        )
        self.validate_all(
            "CONCAT(COALESCE(a, ''))",
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
                "bigquery": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "duckdb": "SELECT CASE WHEN COALESCE(bar, 0) = 1 THEN TRUE ELSE FALSE END AS foo FROM baz",
                "presto": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "hive": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "spark": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(col1, col2)",
            read={
                "bigquery": "EDIT_DISTANCE(col1, col2)",
                "clickhouse": "editDistance(col1, col2)",
                "drill": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
                "postgres": "LEVENSHTEIN(col1, col2)",
                "presto": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "snowflake": "EDITDISTANCE(col1, col2)",
                "sqlite": "EDITDIST3(col1, col2)",
                "trino": "LEVENSHTEIN_DISTANCE(col1, col2)",
            },
            write={
                "bigquery": "EDIT_DISTANCE(col1, col2)",
                "clickhouse": "editDistance(col1, col2)",
                "drill": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
                "postgres": "LEVENSHTEIN(col1, col2)",
                "presto": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "snowflake": "EDITDISTANCE(col1, col2)",
                "sqlite": "EDITDIST3(col1, col2)",
                "trino": "LEVENSHTEIN_DISTANCE(col1, col2)",
            },
        )

        self.validate_all(
            "LEVENSHTEIN(col1, col2, 1, 2, 3)",
            write={
                "bigquery": UnsupportedError,
                "clickhouse": UnsupportedError,
                "drill": UnsupportedError,
                "duckdb": UnsupportedError,
                "hive": UnsupportedError,
                "spark": UnsupportedError,
                "postgres": "LEVENSHTEIN(col1, col2, 1, 2, 3)",
                "presto": UnsupportedError,
                "snowflake": UnsupportedError,
                "sqlite": UnsupportedError,
                "trino": UnsupportedError,
            },
        )
        self.validate_all(
            "LEVENSHTEIN(col1, col2, 1, 2, 3, 4)",
            write={
                "bigquery": UnsupportedError,
                "clickhouse": UnsupportedError,
                "drill": UnsupportedError,
                "duckdb": UnsupportedError,
                "hive": UnsupportedError,
                "spark": UnsupportedError,
                "postgres": "LEVENSHTEIN_LESS_EQUAL(col1, col2, 1, 2, 3, 4)",
                "presto": UnsupportedError,
                "snowflake": UnsupportedError,
                "sqlite": UnsupportedError,
                "trino": UnsupportedError,
            },
        )

        self.validate_all(
            "LEVENSHTEIN(coalesce(col1, col2), coalesce(col2, col1))",
            write={
                "bigquery": "EDIT_DISTANCE(COALESCE(col1, col2), COALESCE(col2, col1))",
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
        self.validate_all(
            "FILTER(the_array, x -> x > 0)",
            write={
                "presto": "FILTER(the_array, x -> x > 0)",
                "starrocks": "ARRAY_FILTER(the_array, x -> x > 0)",
            },
        )
        self.validate_all(
            "a / b",
            write={
                "bigquery": "a / b",
                "clickhouse": "a / b",
                "databricks": "a / b",
                "duckdb": "a / b",
                "hive": "a / b",
                "mysql": "a / b",
                "oracle": "a / b",
                "snowflake": "a / b",
                "spark": "a / b",
                "starrocks": "a / b",
                "drill": "CAST(a AS DOUBLE) / b",
                "postgres": "CAST(a AS DOUBLE PRECISION) / b",
                "presto": "CAST(a AS DOUBLE) / b",
                "redshift": "CAST(a AS DOUBLE PRECISION) / b",
                "sqlite": "CAST(a AS REAL) / b",
                "teradata": "CAST(a AS DOUBLE PRECISION) / b",
                "trino": "CAST(a AS DOUBLE) / b",
                "tsql": "CAST(a AS FLOAT) / b",
            },
        )
        self.validate_all(
            "MOD(8 - 1 + 7, 7)",
            write={
                "": "(8 - 1 + 7) % 7",
                "hive": "(8 - 1 + 7) % 7",
                "presto": "(8 - 1 + 7) % 7",
                "snowflake": "(8 - 1 + 7) % 7",
                "bigquery": "MOD(8 - 1 + 7, 7)",
            },
        )
        self.validate_all(
            "MOD(a, b + 1)",
            write={
                "": "a % (b + 1)",
                "hive": "a % (b + 1)",
                "presto": "a % (b + 1)",
                "snowflake": "a % (b + 1)",
                "bigquery": "MOD(a, b + 1)",
            },
        )

    def test_typeddiv(self):
        typed_div = exp.Div(this=exp.column("a"), expression=exp.column("b"), typed=True)
        div = exp.Div(this=exp.column("a"), expression=exp.column("b"))
        typed_div_dialect = "presto"
        div_dialect = "hive"
        INT = exp.DataType.Type.INT
        FLOAT = exp.DataType.Type.FLOAT

        for expression, types, dialect, expected in [
            (typed_div, (None, None), typed_div_dialect, "a / b"),
            (typed_div, (None, None), div_dialect, "a / b"),
            (div, (None, None), typed_div_dialect, "CAST(a AS DOUBLE) / b"),
            (div, (None, None), div_dialect, "a / b"),
            (typed_div, (INT, INT), typed_div_dialect, "a / b"),
            (typed_div, (INT, INT), div_dialect, "CAST(a / b AS BIGINT)"),
            (div, (INT, INT), typed_div_dialect, "CAST(a AS DOUBLE) / b"),
            (div, (INT, INT), div_dialect, "a / b"),
            (typed_div, (FLOAT, FLOAT), typed_div_dialect, "a / b"),
            (typed_div, (FLOAT, FLOAT), div_dialect, "a / b"),
            (div, (FLOAT, FLOAT), typed_div_dialect, "a / b"),
            (div, (FLOAT, FLOAT), div_dialect, "a / b"),
            (typed_div, (INT, FLOAT), typed_div_dialect, "a / b"),
            (typed_div, (INT, FLOAT), div_dialect, "a / b"),
            (div, (INT, FLOAT), typed_div_dialect, "a / b"),
            (div, (INT, FLOAT), div_dialect, "a / b"),
        ]:
            with self.subTest(f"{expression.__class__.__name__} {types} {dialect} -> {expected}"):
                expression = expression.copy()
                expression.left.type = types[0]
                expression.right.type = types[1]
                self.assertEqual(expected, expression.sql(dialect=dialect))

    def test_safediv(self):
        safe_div = exp.Div(this=exp.column("a"), expression=exp.column("b"), safe=True)
        div = exp.Div(this=exp.column("a"), expression=exp.column("b"))
        safe_div_dialect = "mysql"
        div_dialect = "snowflake"

        for expression, dialect, expected in [
            (safe_div, safe_div_dialect, "a / b"),
            (safe_div, div_dialect, "a / NULLIF(b, 0)"),
            (div, safe_div_dialect, "a / b"),
            (div, div_dialect, "a / b"),
        ]:
            with self.subTest(f"{expression.__class__.__name__} {dialect} -> {expected}"):
                self.assertEqual(expected, expression.sql(dialect=dialect))

        self.assertEqual(
            parse_one("CAST(x AS DECIMAL) / y", read="mysql").sql(dialect="postgres"),
            "CAST(x AS DECIMAL) / NULLIF(y, 0)",
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
                "hive": "CREATE TABLE t (c STRING, nc STRING, v1 STRING, v2 STRING, nv STRING, nv2 STRING)",
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
            "CREATE INDEX my_idx ON tbl(a, b)",
            read={
                "hive": "CREATE INDEX my_idx ON TABLE tbl(a, b)",
                "sqlite": "CREATE INDEX my_idx ON tbl(a, b)",
            },
            write={
                "hive": "CREATE INDEX my_idx ON TABLE tbl(a, b)",
                "postgres": "CREATE INDEX my_idx ON tbl(a NULLS FIRST, b NULLS FIRST)",
                "sqlite": "CREATE INDEX my_idx ON tbl(a, b)",
            },
        )
        self.validate_all(
            "CREATE UNIQUE INDEX my_idx ON tbl(a, b)",
            read={
                "hive": "CREATE UNIQUE INDEX my_idx ON TABLE tbl(a, b)",
                "sqlite": "CREATE UNIQUE INDEX my_idx ON tbl(a, b)",
            },
            write={
                "hive": "CREATE UNIQUE INDEX my_idx ON TABLE tbl(a, b)",
                "postgres": "CREATE UNIQUE INDEX my_idx ON tbl(a NULLS FIRST, b NULLS FIRST)",
                "sqlite": "CREATE UNIQUE INDEX my_idx ON tbl(a, b)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 TEXT, c2 TEXT(1024))",
            write={
                "duckdb": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 TEXT, c2 TEXT(1024))",
                "hive": "CREATE TABLE t (b1 BINARY, b2 BINARY(1024), c1 STRING, c2 VARCHAR(1024))",
                "oracle": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 CLOB, c2 CLOB(1024))",
                "postgres": "CREATE TABLE t (b1 BYTEA, b2 BYTEA(1024), c1 TEXT, c2 TEXT(1024))",
                "sqlite": "CREATE TABLE t (b1 BLOB, b2 BLOB(1024), c1 TEXT, c2 TEXT(1024))",
                "redshift": "CREATE TABLE t (b1 VARBYTE, b2 VARBYTE(1024), c1 VARCHAR(MAX), c2 VARCHAR(1024))",
            },
        )

    def test_alias(self):
        self.validate_all(
            'SELECT 1 AS "foo"',
            read={
                "mysql": "SELECT 1 'foo'",
                "sqlite": "SELECT 1 'foo'",
                "tsql": "SELECT 1 'foo'",
            },
        )

        for dialect in (
            "presto",
            "hive",
            "postgres",
            "clickhouse",
            "bigquery",
            "snowflake",
            "duckdb",
        ):
            with self.subTest(f"string alias: {dialect}"):
                with self.assertRaises(ParseError):
                    parse_one("SELECT 1 'foo'", dialect=dialect)

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

        # needs to preserve the target alias in then WHEN condition but not in the THEN clause
        self.validate_all(
            """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a
            WHEN MATCHED AND target.a <> src.a THEN UPDATE SET target.b = 'FOO'
            WHEN NOT MATCHED THEN INSERT (target.a, target.b) VALUES (src.a, src.b)""",
            write={
                "trino": """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a WHEN MATCHED AND target.a <> src.a THEN UPDATE SET b = 'FOO' WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)""",
                "postgres": """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a WHEN MATCHED AND target.a <> src.a THEN UPDATE SET b = 'FOO' WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)""",
            },
        )

        # needs to preserve the target alias in then WHEN condition and function but not in the THEN clause
        self.validate_all(
            """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a
            WHEN MATCHED THEN UPDATE SET target.b = COALESCE(src.b, target.b)
            WHEN NOT MATCHED THEN INSERT (target.a, target.b) VALUES (src.a, src.b)""",
            write={
                "trino": """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a WHEN MATCHED THEN UPDATE SET b = COALESCE(src.b, target.b) WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)""",
                "postgres": """MERGE INTO foo AS target USING (SELECT a, b FROM tbl) AS src ON src.a = target.a WHEN MATCHED THEN UPDATE SET b = COALESCE(src.b, target.b) WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)""",
            },
        )

    def test_substring(self):
        self.validate_all(
            "SUBSTR('123456', 2, 3)",
            write={
                "bigquery": "SUBSTRING('123456', 2, 3)",
                "oracle": "SUBSTR('123456', 2, 3)",
                "postgres": "SUBSTRING('123456' FROM 2 FOR 3)",
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
        for base in (2, 10):
            with self.subTest(f"Transpiling LOG base {base}"):
                self.validate_all(
                    f"LOG({base}, a)",
                    read={
                        "": f"LOG{base}(a)",
                        "bigquery": f"LOG{base}(a)",
                        "clickhouse": f"LOG{base}(a)",
                        "databricks": f"LOG{base}(a)",
                        "duckdb": f"LOG{base}(a)",
                        "mysql": f"LOG{base}(a)",
                        "postgres": f"LOG{base}(a)",
                        "presto": f"LOG{base}(a)",
                        "spark": f"LOG{base}(a)",
                        "sqlite": f"LOG{base}(a)",
                        "trino": f"LOG{base}(a)",
                        "tsql": f"LOG{base}(a)",
                    },
                    write={
                        "bigquery": f"LOG(a, {base})",
                        "clickhouse": f"LOG{base}(a)",
                        "duckdb": f"LOG({base}, a)",
                        "mysql": f"LOG({base}, a)",
                        "oracle": f"LOG({base}, a)",
                        "postgres": f"LOG({base}, a)",
                        "presto": f"LOG{base}(a)",
                        "redshift": f"LOG({base}, a)",
                        "snowflake": f"LOG({base}, a)",
                        "spark2": f"LOG({base}, a)",
                        "spark": f"LOG({base}, a)",
                        "sqlite": f"LOG({base}, a)",
                        "starrocks": f"LOG({base}, a)",
                        "tableau": f"LOG(a, {base})",
                        "trino": f"LOG({base}, a)",
                        "tsql": f"LOG(a, {base})",
                    },
                )

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
                "duckdb": "LOG(b, n)",
                "hive": "LOG(b, n)",
                "mysql": "LOG(b, n)",
                "oracle": "LOG(b, n)",
                "postgres": "LOG(b, n)",
                "snowflake": "LOG(b, n)",
                "spark": "LOG(b, n)",
                "sqlite": "LOG(b, n)",
                "trino": "LOG(b, n)",
                "tsql": "LOG(n, b)",
            },
            write={
                "clickhouse": UnsupportedError,
                "presto": UnsupportedError,
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
                "sqlite": "SELECT SUM(IIF(col % 2 = 0, 1, 0)) FROM foo",
                "tsql": "SELECT COUNT_IF(col % 2 = 0) FROM foo",
                "postgres": "SELECT SUM(CASE WHEN col % 2 = 0 THEN 1 ELSE 0 END) FROM foo",
                "redshift": "SELECT SUM(CASE WHEN col % 2 = 0 THEN 1 ELSE 0 END) FROM foo",
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
                "sqlite": "SELECT SUM(IIF(col % 2 = 0, 1, 0)) FILTER(WHERE col < 1000) FROM foo",
                "tsql": "SELECT COUNT_IF(col % 2 = 0) FILTER(WHERE col < 1000) FROM foo",
            },
        )

    def test_cast_to_user_defined_type(self):
        self.validate_identity("CAST(x AS some_udt(1234))")

        self.validate_all(
            "CAST(x AS some_udt)",
            write={
                "": "CAST(x AS some_udt)",
                "oracle": "CAST(x AS some_udt)",
                "postgres": "CAST(x AS some_udt)",
                "presto": "CAST(x AS some_udt)",
                "teradata": "CAST(x AS some_udt)",
                "tsql": "CAST(x AS some_udt)",
            },
        )

        with self.assertRaises(ParseError):
            parse_one("CAST(x AS some_udt)", read="bigquery")

    def test_qualify(self):
        self.validate_all(
            "SELECT * FROM t QUALIFY COUNT(*) OVER () > 1",
            write={
                "duckdb": "SELECT * FROM t QUALIFY COUNT(*) OVER () > 1",
                "snowflake": "SELECT * FROM t QUALIFY COUNT(*) OVER () > 1",
                "clickhouse": "SELECT * FROM t QUALIFY COUNT(*) OVER () > 1",
                "mysql": "SELECT * FROM (SELECT *, COUNT(*) OVER () AS _w FROM t) AS _t WHERE _w > 1",
                "oracle": "SELECT * FROM (SELECT *, COUNT(*) OVER () AS _w FROM t) _t WHERE _w > 1",
                "postgres": "SELECT * FROM (SELECT *, COUNT(*) OVER () AS _w FROM t) AS _t WHERE _w > 1",
                "tsql": "SELECT * FROM (SELECT *, COUNT_BIG(*) OVER () AS _w FROM t) AS _t WHERE _w > 1",
            },
        )
        self.validate_all(
            'SELECT "user id", some_id, 1 as other_id, 2 as "2 nd id" FROM t QUALIFY COUNT(*) OVER () > 1',
            write={
                "duckdb": 'SELECT "user id", some_id, 1 AS other_id, 2 AS "2 nd id" FROM t QUALIFY COUNT(*) OVER () > 1',
                "snowflake": 'SELECT "user id", some_id, 1 AS other_id, 2 AS "2 nd id" FROM t QUALIFY COUNT(*) OVER () > 1',
                "clickhouse": 'SELECT "user id", some_id, 1 AS other_id, 2 AS "2 nd id" FROM t QUALIFY COUNT(*) OVER () > 1',
                "mysql": "SELECT `user id`, some_id, other_id, `2 nd id` FROM (SELECT `user id`, some_id, 1 AS other_id, 2 AS `2 nd id`, COUNT(*) OVER () AS _w FROM t) AS _t WHERE _w > 1",
                "oracle": 'SELECT "user id", some_id, other_id, "2 nd id" FROM (SELECT "user id", some_id, 1 AS other_id, 2 AS "2 nd id", COUNT(*) OVER () AS _w FROM t) _t WHERE _w > 1',
                "postgres": 'SELECT "user id", some_id, other_id, "2 nd id" FROM (SELECT "user id", some_id, 1 AS other_id, 2 AS "2 nd id", COUNT(*) OVER () AS _w FROM t) AS _t WHERE _w > 1',
                "tsql": "SELECT [user id], some_id, other_id, [2 nd id] FROM (SELECT [user id] AS [user id], some_id AS some_id, 1 AS other_id, 2 AS [2 nd id], COUNT_BIG(*) OVER () AS _w FROM t) AS _t WHERE _w > 1",
            },
        )

    def test_nested_ctes(self):
        self.validate_all(
            "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
            write={
                "bigquery": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "clickhouse": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "databricks": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT c FROM t) AS subq",
                "duckdb": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "hive": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT c FROM t) AS subq",
                "mysql": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "postgres": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "presto": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "redshift": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "snowflake": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "spark": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT c FROM t) AS subq",
                "spark2": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT c FROM t) AS subq",
                "sqlite": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "trino": "SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq",
                "tsql": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT c AS c FROM t) AS subq",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq1) AS subq2",
            write={
                "bigquery": "SELECT * FROM (SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq1) AS subq2",
                "duckdb": "SELECT * FROM (SELECT * FROM (WITH t AS (SELECT 1 AS c) SELECT c FROM t) AS subq1) AS subq2",
                "hive": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT * FROM (SELECT c FROM t) AS subq1) AS subq2",
                "tsql": "WITH t AS (SELECT 1 AS c) SELECT * FROM (SELECT * FROM (SELECT c AS c FROM t) AS subq1) AS subq2",
            },
        )
        self.validate_all(
            "WITH t1(x) AS (SELECT 1) SELECT * FROM (WITH t2(y) AS (SELECT 2) SELECT y FROM t2) AS subq",
            write={
                "duckdb": "WITH t1(x) AS (SELECT 1) SELECT * FROM (WITH t2(y) AS (SELECT 2) SELECT y FROM t2) AS subq",
                "tsql": "WITH t1(x) AS (SELECT 1), t2(y) AS (SELECT 2) SELECT * FROM (SELECT y AS y FROM t2) AS subq",
            },
        )
        self.validate_all(
            """
WITH c AS (
  WITH b AS (
    WITH a1 AS (
      SELECT 1
    ), a2 AS (
      SELECT 2
    )
    SELECT * FROM a1, a2
  )
  SELECT *
  FROM b
)
SELECT *
FROM c""",
            write={
                "duckdb": "WITH c AS (WITH b AS (WITH a1 AS (SELECT 1), a2 AS (SELECT 2) SELECT * FROM a1, a2) SELECT * FROM b) SELECT * FROM c",
                "hive": "WITH a1 AS (SELECT 1), a2 AS (SELECT 2), b AS (SELECT * FROM a1, a2), c AS (SELECT * FROM b) SELECT * FROM c",
            },
        )
        self.validate_all(
            """
WITH subquery1 AS (
  WITH tmp AS (
    SELECT
      *
    FROM table0
  )
  SELECT
    *
  FROM tmp
), subquery2 AS (
  WITH tmp2 AS (
    SELECT
      *
    FROM table1
    WHERE
      a IN subquery1
  )
  SELECT
    *
  FROM tmp2
)
SELECT
  *
FROM subquery2
""",
            write={
                "hive": """WITH tmp AS (
  SELECT
    *
  FROM table0
), subquery1 AS (
  SELECT
    *
  FROM tmp
), tmp2 AS (
  SELECT
    *
  FROM table1
  WHERE
    a IN subquery1
), subquery2 AS (
  SELECT
    *
  FROM tmp2
)
SELECT
  *
FROM subquery2""",
            },
            pretty=True,
        )

    def test_unsupported_null_ordering(self):
        # We'll transpile a portable query from the following dialects to MySQL / T-SQL, which
        # both treat NULLs as small values, so the expected output queries should be equivalent
        with_last_nulls = "duckdb"
        with_small_nulls = "spark"
        with_large_nulls = "postgres"

        sql = "SELECT * FROM t ORDER BY c"
        sql_nulls_last = "SELECT * FROM t ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END, c"
        sql_nulls_first = "SELECT * FROM t ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c"

        for read_dialect, desc, nulls_first, expected_sql in (
            (with_last_nulls, False, None, sql_nulls_last),
            (with_last_nulls, True, None, sql),
            (with_last_nulls, False, True, sql),
            (with_last_nulls, True, True, sql_nulls_first),
            (with_last_nulls, False, False, sql_nulls_last),
            (with_last_nulls, True, False, sql),
            (with_small_nulls, False, None, sql),
            (with_small_nulls, True, None, sql),
            (with_small_nulls, False, True, sql),
            (with_small_nulls, True, True, sql_nulls_first),
            (with_small_nulls, False, False, sql_nulls_last),
            (with_small_nulls, True, False, sql),
            (with_large_nulls, False, None, sql_nulls_last),
            (with_large_nulls, True, None, sql_nulls_first),
            (with_large_nulls, False, True, sql),
            (with_large_nulls, True, True, sql_nulls_first),
            (with_large_nulls, False, False, sql_nulls_last),
            (with_large_nulls, True, False, sql),
        ):
            with self.subTest(
                f"read: {read_dialect}, descending: {desc}, nulls first: {nulls_first}"
            ):
                sort_order = " DESC" if desc else ""
                null_order = (
                    " NULLS FIRST"
                    if nulls_first
                    else (" NULLS LAST" if nulls_first is not None else "")
                )

                expected_sql = f"{expected_sql}{sort_order}"
                expression = parse_one(f"{sql}{sort_order}{null_order}", read=read_dialect)

                self.assertEqual(expression.sql(dialect="mysql"), expected_sql)
                self.assertEqual(expression.sql(dialect="tsql"), expected_sql)

    def test_random(self):
        self.validate_all(
            "RAND()",
            write={
                "bigquery": "RAND()",
                "clickhouse": "randCanonical()",
                "databricks": "RAND()",
                "doris": "RAND()",
                "drill": "RAND()",
                "duckdb": "RANDOM()",
                "hive": "RAND()",
                "mysql": "RAND()",
                "oracle": "RAND()",
                "postgres": "RANDOM()",
                "presto": "RAND()",
                "spark": "RAND()",
                "sqlite": "RANDOM()",
                "tsql": "RAND()",
            },
            read={
                "bigquery": "RAND()",
                "clickhouse": "randCanonical()",
                "databricks": "RAND()",
                "doris": "RAND()",
                "drill": "RAND()",
                "duckdb": "RANDOM()",
                "hive": "RAND()",
                "mysql": "RAND()",
                "oracle": "RAND()",
                "postgres": "RANDOM()",
                "presto": "RAND()",
                "spark": "RAND()",
                "sqlite": "RANDOM()",
                "tsql": "RAND()",
            },
        )

    def test_array_any(self):
        self.validate_all(
            "ARRAY_ANY(arr, x -> pred)",
            write={
                "": "ARRAY_ANY(arr, x -> pred)",
                "bigquery": "(ARRAY_LENGTH(arr) = 0 OR ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(arr) AS x WHERE pred)) <> 0)",
                "clickhouse": "(LENGTH(arr) = 0 OR LENGTH(arrayFilter(x -> pred, arr)) <> 0)",
                "databricks": "(SIZE(arr) = 0 OR SIZE(FILTER(arr, x -> pred)) <> 0)",
                "doris": UnsupportedError,
                "drill": UnsupportedError,
                "duckdb": "(ARRAY_LENGTH(arr) = 0 OR ARRAY_LENGTH(LIST_FILTER(arr, x -> pred)) <> 0)",
                "hive": UnsupportedError,
                "mysql": UnsupportedError,
                "oracle": UnsupportedError,
                "postgres": "(ARRAY_LENGTH(arr, 1) = 0 OR ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(arr) AS _t0(x) WHERE pred), 1) <> 0)",
                "presto": "ANY_MATCH(arr, x -> pred)",
                "redshift": UnsupportedError,
                "snowflake": UnsupportedError,
                "spark": "(SIZE(arr) = 0 OR SIZE(FILTER(arr, x -> pred)) <> 0)",
                "spark2": "(SIZE(arr) = 0 OR SIZE(FILTER(arr, x -> pred)) <> 0)",
                "sqlite": UnsupportedError,
                "starrocks": UnsupportedError,
                "tableau": UnsupportedError,
                "teradata": "(CARDINALITY(arr) = 0 OR CARDINALITY(FILTER(arr, x -> pred)) <> 0)",
                "trino": "ANY_MATCH(arr, x -> pred)",
                "tsql": UnsupportedError,
            },
        )

    def test_truncate(self):
        self.validate_identity("TRUNCATE TABLE table")
        self.validate_identity("TRUNCATE TABLE db.schema.test")
        self.validate_identity("TRUNCATE TABLE IF EXISTS db.schema.test")
        self.validate_identity("TRUNCATE TABLE t1, t2, t3")

    def test_create_sequence(self):
        self.validate_identity("CREATE SEQUENCE seq")
        self.validate_identity(
            "CREATE TEMPORARY SEQUENCE seq AS SMALLINT START WITH 3 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CACHE 1 NO CYCLE OWNED BY table.col"
        )
        self.validate_identity(
            "CREATE SEQUENCE seq START WITH 1 NO MINVALUE NO MAXVALUE CYCLE NO CACHE"
        )
        self.validate_identity("CREATE OR REPLACE TEMPORARY SEQUENCE seq INCREMENT BY 1 NO CYCLE")
        self.validate_identity(
            "CREATE OR REPLACE SEQUENCE IF NOT EXISTS seq COMMENT='test comment' ORDER"
        )
        self.validate_identity(
            "CREATE SEQUENCE schema.seq SHARING=METADATA NOORDER NOKEEP SCALE EXTEND SHARD EXTEND SESSION"
        )
        self.validate_identity(
            "CREATE SEQUENCE schema.seq SHARING=DATA ORDER KEEP NOSCALE NOSHARD GLOBAL"
        )
        self.validate_identity(
            "CREATE SEQUENCE schema.seq SHARING=DATA NOCACHE NOCYCLE SCALE NOEXTEND"
        )
        self.validate_identity(
            """CREATE TEMPORARY SEQUENCE seq AS BIGINT INCREMENT BY 2 MINVALUE 1 CACHE 1 NOMAXVALUE NO CYCLE OWNED BY NONE""",
            """CREATE TEMPORARY SEQUENCE seq AS BIGINT INCREMENT BY 2 MINVALUE 1 CACHE 1 NOMAXVALUE NO CYCLE""",
        )
        self.validate_identity(
            """CREATE TEMPORARY SEQUENCE seq START 1""",
            """CREATE TEMPORARY SEQUENCE seq START WITH 1""",
        )
        self.validate_identity(
            """CREATE TEMPORARY SEQUENCE seq START WITH = 1 INCREMENT BY = 2""",
            """CREATE TEMPORARY SEQUENCE seq START WITH 1 INCREMENT BY 2""",
        )

    def test_reserved_keywords(self):
        order = exp.select("*").from_("order")

        for dialect in ("duckdb", "presto", "redshift"):
            dialect = Dialect.get_or_raise(dialect)
            self.assertEqual(
                order.sql(dialect=dialect),
                f"SELECT * FROM {dialect.IDENTIFIER_START}order{dialect.IDENTIFIER_END}",
            )

        self.validate_identity(
            """SELECT partition.d FROM t PARTITION (d)""",
            """SELECT partition.d FROM t AS PARTITION(d)""",
        )

    def test_string_functions(self):
        for pad_func in ("LPAD", "RPAD"):
            ch_alias = "LEFTPAD" if pad_func == "LPAD" else "RIGHTPAD"
            for fill_pattern in ("", ", ' '"):
                with self.subTest(f"Testing {pad_func}() with pattern {fill_pattern}"):
                    self.validate_all(
                        f"SELECT {pad_func}('bar', 5{fill_pattern})",
                        read={
                            "snowflake": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "databricks": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "spark": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "postgres": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "clickhouse": f"SELECT {ch_alias}('bar', 5{fill_pattern})",
                        },
                        write={
                            "": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "spark": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "postgres": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "clickhouse": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "snowflake": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "databricks": f"SELECT {pad_func}('bar', 5{fill_pattern})",
                            "duckdb": f"SELECT {pad_func}('bar', 5, ' ')",
                            "mysql": f"SELECT {pad_func}('bar', 5, ' ')",
                            "hive": f"SELECT {pad_func}('bar', 5, ' ')",
                            "spark2": f"SELECT {pad_func}('bar', 5, ' ')",
                            "presto": f"SELECT {pad_func}('bar', 5, ' ')",
                            "trino": f"SELECT {pad_func}('bar', 5, ' ')",
                        },
                    )

    def test_generate_date_array(self):
        self.validate_all(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            write={
                "bigquery": "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), INTERVAL '1' WEEK))",
                "databricks": "SELECT * FROM EXPLODE(SEQUENCE(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), INTERVAL '1' WEEK))",
                "duckdb": "SELECT * FROM UNNEST(CAST(GENERATE_SERIES(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), (7 * INTERVAL '1' DAY)) AS DATE[]))",
                "mysql": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) FROM _generated_dates WHERE CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates",
                "postgres": "SELECT * FROM (SELECT CAST(value AS DATE) FROM GENERATE_SERIES(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), INTERVAL '1 WEEK') AS _t(value)) AS _unnested_generate_series",
                "presto": "SELECT * FROM UNNEST(SEQUENCE(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), (1 * INTERVAL '7' DAY)))",
                "redshift": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates",
                "snowflake": "SELECT * FROM (SELECT DATEADD(WEEK, CAST(value AS INT), CAST('2020-01-01' AS DATE)) AS value FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(WEEK, CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE)) + 1 - 1) + 1))) AS _u(seq, key, path, index, value, this))",
                "spark": "SELECT * FROM EXPLODE(SEQUENCE(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), INTERVAL '1' WEEK))",
                "trino": "SELECT * FROM UNNEST(SEQUENCE(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), (1 * INTERVAL '7' DAY)))",
                "tsql": "WITH _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_value AS date_value FROM _generated_dates) AS _generated_dates",
            },
        )
        self.validate_all(
            "WITH dates AS (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))) SELECT * FROM dates",
            write={
                "mysql": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) FROM _generated_dates WHERE CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) <= CAST('2020-02-01' AS DATE)), dates AS (SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates) SELECT * FROM dates",
                "redshift": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)), dates AS (SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates) SELECT * FROM dates",
                "tsql": "WITH _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)), dates AS (SELECT * FROM (SELECT date_value AS date_value FROM _generated_dates) AS _generated_dates) SELECT * FROM dates",
            },
        )
        self.validate_all(
            "WITH dates1 AS (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))), dates2 AS (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-03-01', INTERVAL 1 MONTH))) SELECT * FROM dates1 CROSS JOIN dates2",
            write={
                "mysql": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) FROM _generated_dates WHERE CAST(DATE_ADD(date_value, INTERVAL 1 WEEK) AS DATE) <= CAST('2020-02-01' AS DATE)), _generated_dates_1(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATE_ADD(date_value, INTERVAL 1 MONTH) AS DATE) FROM _generated_dates_1 WHERE CAST(DATE_ADD(date_value, INTERVAL 1 MONTH) AS DATE) <= CAST('2020-03-01' AS DATE)), dates1 AS (SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates), dates2 AS (SELECT * FROM (SELECT date_value FROM _generated_dates_1) AS _generated_dates_1) SELECT * FROM dates1 CROSS JOIN dates2",
                "redshift": "WITH RECURSIVE _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)), _generated_dates_1(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(MONTH, 1, date_value) AS DATE) FROM _generated_dates_1 WHERE CAST(DATEADD(MONTH, 1, date_value) AS DATE) <= CAST('2020-03-01' AS DATE)), dates1 AS (SELECT * FROM (SELECT date_value FROM _generated_dates) AS _generated_dates), dates2 AS (SELECT * FROM (SELECT date_value FROM _generated_dates_1) AS _generated_dates_1) SELECT * FROM dates1 CROSS JOIN dates2",
                "tsql": "WITH _generated_dates(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_value) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_value) AS DATE) <= CAST('2020-02-01' AS DATE)), _generated_dates_1(date_value) AS (SELECT CAST('2020-01-01' AS DATE) AS date_value UNION ALL SELECT CAST(DATEADD(MONTH, 1, date_value) AS DATE) FROM _generated_dates_1 WHERE CAST(DATEADD(MONTH, 1, date_value) AS DATE) <= CAST('2020-03-01' AS DATE)), dates1 AS (SELECT * FROM (SELECT date_value AS date_value FROM _generated_dates) AS _generated_dates), dates2 AS (SELECT * FROM (SELECT date_value AS date_value FROM _generated_dates_1) AS _generated_dates_1) SELECT * FROM dates1 CROSS JOIN dates2",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK)) AS _q(date_week)",
            write={
                "mysql": "WITH RECURSIVE _generated_dates(date_week) AS (SELECT CAST('2020-01-01' AS DATE) AS date_week UNION ALL SELECT CAST(DATE_ADD(date_week, INTERVAL 1 WEEK) AS DATE) FROM _generated_dates WHERE CAST(DATE_ADD(date_week, INTERVAL 1 WEEK) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_week FROM _generated_dates) AS _generated_dates",
                "redshift": "WITH RECURSIVE _generated_dates(date_week) AS (SELECT CAST('2020-01-01' AS DATE) AS date_week UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_week) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_week) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_week FROM _generated_dates) AS _generated_dates",
                "snowflake": "SELECT * FROM (SELECT DATEADD(WEEK, CAST(date_week AS INT), CAST('2020-01-01' AS DATE)) AS date_week FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(WEEK, CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE)) + 1 - 1) + 1))) AS _q(seq, key, path, index, date_week, this)) AS _q(date_week)",
                "tsql": "WITH _generated_dates(date_week) AS (SELECT CAST('2020-01-01' AS DATE) AS date_week UNION ALL SELECT CAST(DATEADD(WEEK, 1, date_week) AS DATE) FROM _generated_dates WHERE CAST(DATEADD(WEEK, 1, date_week) AS DATE) <= CAST('2020-02-01' AS DATE)) SELECT * FROM (SELECT date_week AS date_week FROM _generated_dates) AS _generated_dates",
            },
        )

        self.validate_all(
            "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            write={
                "snowflake": "SELECT ARRAY_SIZE((SELECT ARRAY_AGG(*) FROM (SELECT DATEADD(WEEK, CAST(value AS INT), CAST('2020-01-01' AS DATE)) AS value FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(WEEK, CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE)) + 1 - 1) + 1))) AS _u(seq, key, path, index, value, this))))",
            },
        )

    def test_set_operation_specifiers(self):
        self.validate_all(
            "SELECT 1 EXCEPT ALL SELECT 1",
            write={
                "": "SELECT 1 EXCEPT ALL SELECT 1",
                "bigquery": UnsupportedError,
                "clickhouse": "SELECT 1 EXCEPT SELECT 1",
                "databricks": "SELECT 1 EXCEPT ALL SELECT 1",
                "duckdb": "SELECT 1 EXCEPT ALL SELECT 1",
                "mysql": "SELECT 1 EXCEPT ALL SELECT 1",
                "oracle": "SELECT 1 EXCEPT ALL SELECT 1",
                "postgres": "SELECT 1 EXCEPT ALL SELECT 1",
                "presto": UnsupportedError,
                "redshift": UnsupportedError,
                "snowflake": UnsupportedError,
                "spark": "SELECT 1 EXCEPT ALL SELECT 1",
                "sqlite": UnsupportedError,
                "starrocks": UnsupportedError,
                "trino": UnsupportedError,
                "tsql": UnsupportedError,
            },
        )

    def test_normalize(self):
        for form in ("", ", nfkc"):
            with self.subTest(f"Testing NORMALIZE('str'{form}) roundtrip"):
                self.validate_all(
                    f"SELECT NORMALIZE('str'{form})",
                    read={
                        "presto": f"SELECT NORMALIZE('str'{form})",
                        "trino": f"SELECT NORMALIZE('str'{form})",
                        "bigquery": f"SELECT NORMALIZE('str'{form})",
                    },
                    write={
                        "presto": f"SELECT NORMALIZE('str'{form})",
                        "trino": f"SELECT NORMALIZE('str'{form})",
                        "bigquery": f"SELECT NORMALIZE('str'{form})",
                    },
                )

        self.assertIsInstance(parse_one("NORMALIZE('str', NFD)").args.get("form"), exp.Var)

    def test_coalesce(self):
        """
        Validate that "expressions" is a list for all the exp.Coalesce instances; This is important
        as some optimizer rules are coalesce specific and will iterate on "expressions"
        """

        # Check the 2-arg aliases
        for func in ("COALESCE", "IFNULL", "NVL"):
            self.assertIsInstance(self.parse_one(f"{func}(1, 2)").expressions, list)

        # Check the varlen case
        coalesce = self.parse_one("COALESCE(x, y, z)")
        self.assertIsInstance(coalesce.expressions, list)
        self.assertIsNone(coalesce.args.get("is_nvl"))

        # Check Oracle's NVL which is decoupled from COALESCE
        oracle_nvl = parse_one("NVL(x, y)", read="oracle")
        self.assertIsInstance(oracle_nvl.expressions, list)
        self.assertTrue(oracle_nvl.args.get("is_nvl"))

        # Check T-SQL's ISNULL which is parsed into exp.Coalesce
        self.assertIsInstance(parse_one("ISNULL(x, y)", read="tsql").expressions, list)

    def test_trim(self):
        self.validate_all(
            "TRIM('abc', 'a')",
            read={
                "bigquery": "TRIM('abc', 'a')",
                "snowflake": "TRIM('abc', 'a')",
            },
            write={
                "bigquery": "TRIM('abc', 'a')",
                "snowflake": "TRIM('abc', 'a')",
            },
        )

        self.validate_all(
            "LTRIM('Hello World', 'H')",
            read={
                "oracle": "LTRIM('Hello World', 'H')",
                "clickhouse": "TRIM(LEADING 'H' FROM 'Hello World')",
                "snowflake": "LTRIM('Hello World', 'H')",
                "bigquery": "LTRIM('Hello World', 'H')",
                "": "LTRIM('Hello World', 'H')",
            },
            write={
                "clickhouse": "TRIM(LEADING 'H' FROM 'Hello World')",
                "oracle": "LTRIM('Hello World', 'H')",
                "snowflake": "LTRIM('Hello World', 'H')",
                "bigquery": "LTRIM('Hello World', 'H')",
            },
        )

        self.validate_all(
            "RTRIM('Hello World', 'd')",
            read={
                "clickhouse": "TRIM(TRAILING 'd' FROM 'Hello World')",
                "oracle": "RTRIM('Hello World', 'd')",
                "snowflake": "RTRIM('Hello World', 'd')",
                "bigquery": "RTRIM('Hello World', 'd')",
                "": "RTRIM('Hello World', 'd')",
            },
            write={
                "clickhouse": "TRIM(TRAILING 'd' FROM 'Hello World')",
                "oracle": "RTRIM('Hello World', 'd')",
                "snowflake": "RTRIM('Hello World', 'd')",
                "bigquery": "RTRIM('Hello World', 'd')",
            },
        )

    def test_uuid(self):
        self.validate_all(
            "UUID()",
            read={
                "hive": "UUID()",
                "spark2": "UUID()",
                "spark": "UUID()",
                "databricks": "UUID()",
                "duckdb": "UUID()",
                "presto": "UUID()",
                "trino": "UUID()",
                "mysql": "UUID()",
                "postgres": "GEN_RANDOM_UUID()",
                "bigquery": "GENERATE_UUID()",
                "snowflake": "UUID_STRING()",
                "tsql": "NEWID()",
            },
            write={
                "hive": "UUID()",
                "spark2": "UUID()",
                "spark": "UUID()",
                "databricks": "UUID()",
                "duckdb": "UUID()",
                "presto": "UUID()",
                "trino": "UUID()",
                "mysql": "UUID()",
                "postgres": "GEN_RANDOM_UUID()",
                "bigquery": "GENERATE_UUID()",
                "snowflake": "UUID_STRING()",
                "tsql": "NEWID()",
            },
        )

    def test_escaped_identifier_delimiter(self):
        for dialect in ("databricks", "hive", "mysql", "spark2", "spark"):
            with self.subTest(f"Testing escaped backtick in identifier name for {dialect}"):
                self.validate_all(
                    'SELECT 1 AS "x`"',
                    read={
                        dialect: "SELECT 1 AS `x```",
                    },
                    write={
                        dialect: "SELECT 1 AS `x```",
                    },
                )

        for dialect in (
            "",
            "clickhouse",
            "duckdb",
            "postgres",
            "presto",
            "trino",
            "redshift",
            "snowflake",
            "sqlite",
        ):
            with self.subTest(f"Testing escaped double-quote in identifier name for {dialect}"):
                self.validate_all(
                    'SELECT 1 AS "x"""',
                    read={
                        dialect: 'SELECT 1 AS "x"""',
                    },
                    write={
                        dialect: 'SELECT 1 AS "x"""',
                    },
                )

        for dialect in ("clickhouse", "sqlite"):
            with self.subTest(f"Testing escaped backtick in identifier name for {dialect}"):
                self.validate_all(
                    'SELECT 1 AS "x`"',
                    read={
                        dialect: "SELECT 1 AS `x```",
                    },
                    write={
                        dialect: 'SELECT 1 AS "x`"',
                    },
                )

        self.validate_all(
            'SELECT 1 AS "x`"',
            read={
                "clickhouse": "SELECT 1 AS `x\\``",
            },
            write={
                "clickhouse": 'SELECT 1 AS "x`"',
            },
        )
        for name in ('"x\\""', '`x"`'):
            with self.subTest(f"Testing ClickHouse delimiter escaping: {name}"):
                self.validate_all(
                    'SELECT 1 AS "x"""',
                    read={
                        "clickhouse": f"SELECT 1 AS {name}",
                    },
                    write={
                        "clickhouse": 'SELECT 1 AS "x"""',
                    },
                )

        for name in ("[[x]]]", '"[x]"'):
            with self.subTest(f"Testing T-SQL delimiter escaping: {name}"):
                self.validate_all(
                    'SELECT 1 AS "[x]"',
                    read={
                        "tsql": f"SELECT 1 AS {name}",
                    },
                    write={
                        "tsql": "SELECT 1 AS [[x]]]",
                    },
                )
        for name in ('[x"]', '"x"""'):
            with self.subTest(f"Testing T-SQL delimiter escaping: {name}"):
                self.validate_all(
                    'SELECT 1 AS "x"""',
                    read={
                        "tsql": f"SELECT 1 AS {name}",
                    },
                    write={
                        "tsql": 'SELECT 1 AS [x"]',
                    },
                )

    def test_median(self):
        for suffix in (
            "",
            " OVER ()",
        ):
            self.validate_all(
                f"MEDIAN(x){suffix}",
                read={
                    "snowflake": f"MEDIAN(x){suffix}",
                    "duckdb": f"MEDIAN(x){suffix}",
                    "spark": f"MEDIAN(x){suffix}",
                    "databricks": f"MEDIAN(x){suffix}",
                    "redshift": f"MEDIAN(x){suffix}",
                    "oracle": f"MEDIAN(x){suffix}",
                },
                write={
                    "snowflake": f"MEDIAN(x){suffix}",
                    "duckdb": f"MEDIAN(x){suffix}",
                    "spark": f"MEDIAN(x){suffix}",
                    "databricks": f"MEDIAN(x){suffix}",
                    "redshift": f"MEDIAN(x){suffix}",
                    "oracle": f"MEDIAN(x){suffix}",
                    "clickhouse": f"median(x){suffix}",
                    "postgres": f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x){suffix}",
                },
            )

    def test_current_schema(self):
        self.validate_all(
            "CURRENT_SCHEMA()",
            read={
                "mysql": "SCHEMA()",
                "postgres": "CURRENT_SCHEMA()",
                "tsql": "SCHEMA_NAME()",
            },
            write={
                "sqlite": "'main'",
                "mysql": "SCHEMA()",
                "postgres": "CURRENT_SCHEMA",
                "tsql": "SCHEMA_NAME()",
            },
        )

    def test_integer_hex_strings(self):
        # Hex strings such as 0xCC represent INTEGER values in the read dialects
        integer_dialects = ("bigquery", "clickhouse")
        for read_dialect in integer_dialects:
            for write_dialect in (
                "",
                "duckdb",
                "databricks",
                "snowflake",
                "spark",
                "redshift",
            ):
                with self.subTest(f"Testing hex string -> INTEGER evaluation for {read_dialect}"):
                    self.assertEqual(
                        parse_one("SELECT 0xCC", read=read_dialect).sql(write_dialect), "SELECT 204"
                    )

            for other_integer_dialects in integer_dialects:
                self.assertEqual(
                    parse_one("SELECT 0xCC", read=read_dialect).sql(other_integer_dialects),
                    "SELECT 0xCC",
                )
