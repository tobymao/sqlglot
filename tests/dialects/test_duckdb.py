from sqlglot import ErrorLevel, UnsupportedError, exp, parse_one, transpile
from tests.dialects.test_dialect import Validator


class TestDuckDB(Validator):
    dialect = "duckdb"

    def test_duckdb(self):
        self.validate_identity("SELECT SUM(x) FILTER (x = 1)", "SELECT SUM(x) FILTER(WHERE x = 1)")

        # https://github.com/duckdb/duckdb/releases/tag/v0.8.0
        self.assertEqual(
            parse_one("a / b", read="duckdb").assert_is(exp.Div).sql(dialect="duckdb"), "a / b"
        )
        self.assertEqual(
            parse_one("a // b", read="duckdb").assert_is(exp.IntDiv).sql(dialect="duckdb"), "a // b"
        )

        self.validate_identity("SELECT * FROM foo ASOF LEFT JOIN bar ON a = b")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Year USING FIRST(Population)")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population) GROUP BY Country")
        self.validate_identity("PIVOT Cities ON Country, Name USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Country || '_' || Name USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population) GROUP BY Country, Name")
        self.validate_identity("SELECT {'a': 1} AS x")
        self.validate_identity("SELECT {'a': {'b': {'c': 1}}, 'd': {'e': 2}} AS x")
        self.validate_identity("SELECT {'x': 1, 'y': 2, 'z': 3}")
        self.validate_identity("SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345}")
        self.validate_identity("SELECT ROW(x, x + 1, y) FROM (SELECT 1 AS x, 'a' AS y)")
        self.validate_identity("SELECT (x, x + 1, y) FROM (SELECT 1 AS x, 'a' AS y)")
        self.validate_identity("SELECT a.x FROM (SELECT {'x': 1, 'y': 2, 'z': 3} AS a)")
        self.validate_identity("ATTACH DATABASE ':memory:' AS new_database")
        self.validate_identity(
            "SELECT {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'}"
        )
        self.validate_identity(
            "SELECT a['x space'] FROM (SELECT {'x space': 1, 'y': 2, 'z': 3} AS a)"
        )
        self.validate_identity(
            "PIVOT Cities ON Year IN (2000, 2010) USING SUM(Population) GROUP BY Country"
        )
        self.validate_identity(
            "PIVOT Cities ON Year USING SUM(Population) AS total, MAX(Population) AS max GROUP BY Country"
        )
        self.validate_identity(
            "WITH pivot_alias AS (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) SELECT * FROM pivot_alias"
        )
        self.validate_identity(
            "SELECT * FROM (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) AS pivot_alias"
        )

        self.validate_all("FROM (FROM tbl)", write={"duckdb": "SELECT * FROM (SELECT * FROM tbl)"})
        self.validate_all("FROM tbl", write={"duckdb": "SELECT * FROM tbl"})
        self.validate_all("0b1010", write={"": "0 AS b1010"})
        self.validate_all("0x1010", write={"": "0 AS x1010"})
        self.validate_all("x ~ y", write={"duckdb": "REGEXP_MATCHES(x, y)"})
        self.validate_all("SELECT * FROM 'x.y'", write={"duckdb": 'SELECT * FROM "x.y"'})
        self.validate_all(
            "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
            read={
                "duckdb": "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
                "hive": "DATEDIFF(a, b)",
                "spark": "DATEDIFF(a, b)",
                "spark2": "DATEDIFF(a, b)",
            },
        )
        self.validate_all(
            "XOR(a, b)",
            read={
                "": "a ^ b",
                "bigquery": "a ^ b",
                "presto": "BITWISE_XOR(a, b)",
                "postgres": "a # b",
            },
            write={
                "": "a ^ b",
                "bigquery": "a ^ b",
                "duckdb": "XOR(a, b)",
                "presto": "BITWISE_XOR(a, b)",
                "postgres": "a # b",
            },
        )
        self.validate_all(
            "PIVOT_WIDER Cities ON Year USING SUM(Population)",
            write={"duckdb": "PIVOT Cities ON Year USING SUM(Population)"},
        )
        self.validate_all(
            "WITH t AS (SELECT 1) FROM t", write={"duckdb": "WITH t AS (SELECT 1) SELECT * FROM t"}
        )
        self.validate_all(
            "WITH t AS (SELECT 1) SELECT * FROM (FROM t)",
            write={"duckdb": "WITH t AS (SELECT 1) SELECT * FROM (SELECT * FROM t)"},
        )
        self.validate_all(
            """SELECT DATEDIFF('day', t1."A", t1."B") FROM "table" AS t1""",
            write={
                "duckdb": """SELECT DATE_DIFF('day', t1."A", t1."B") FROM "table" AS t1""",
                "trino": """SELECT DATE_DIFF('day', t1."A", t1."B") FROM "table" AS t1""",
            },
        )
        self.validate_all(
            "SELECT DATE_DIFF('day', DATE '2020-01-01', DATE '2020-01-05')",
            write={
                "duckdb": "SELECT DATE_DIFF('day', CAST('2020-01-01' AS DATE), CAST('2020-01-05' AS DATE))",
                "trino": "SELECT DATE_DIFF('day', CAST('2020-01-01' AS DATE), CAST('2020-01-05' AS DATE))",
            },
        )
        self.validate_all(
            "WITH 'x' AS (SELECT 1) SELECT * FROM x",
            write={"duckdb": 'WITH "x" AS (SELECT 1) SELECT * FROM x'},
        )
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS table (cola INT, colb STRING) USING ICEBERG PARTITIONED BY (colb)",
            write={
                "duckdb": "CREATE TABLE IF NOT EXISTS table (cola INT, colb TEXT)",
            },
        )
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS table (cola INT COMMENT 'cola', colb STRING) USING ICEBERG PARTITIONED BY (colb)",
            write={
                "duckdb": "CREATE TABLE IF NOT EXISTS table (cola INT, colb TEXT)",
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
            "POWER(TRY_CAST(2 AS SMALLINT), 3)",
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
                "duckdb": "{'x': 1, 'y': '2'}",
                "spark": "STRUCT(x = 1, y = '2')",
            },
        )
        self.validate_all(
            "STRUCT_PACK(key1 := 'value1', key2 := 42)",
            write={
                "duckdb": "{'key1': 'value1', 'key2': 42}",
                "spark": "STRUCT(key1 = 'value1', key2 = 42)",
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
                "": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
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
        self.validate_all(
            "SELECT CAST(CAST(x AS DATE) AS DATE) + INTERVAL 1 DAY",
            read={
                "hive": "SELECT DATE_ADD(TO_DATE(x), 1)",
            },
        )
        self.validate_all(
            "SELECT CAST('2020-05-06' AS DATE) - INTERVAL 5 DAY",
            read={"bigquery": "SELECT DATE_SUB(CAST('2020-05-06' AS DATE), INTERVAL 5 DAY)"},
        )
        self.validate_all(
            "SELECT CAST('2020-05-06' AS DATE) + INTERVAL 5 DAY",
            read={"bigquery": "SELECT DATE_ADD(CAST('2020-05-06' AS DATE), INTERVAL 5 DAY)"},
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT REGEXP_EXTRACT(a, 'pattern', 1) from table",
                read="bigquery",
                write="duckdb",
                unsupported_level=ErrorLevel.IMMEDIATE,
            )

    def test_time(self):
        self.validate_identity("SELECT CURRENT_DATE")
        self.validate_identity("SELECT CURRENT_TIMESTAMP")

        self.validate_all(
            "SELECT MAKE_DATE(2016, 12, 25)", read={"bigquery": "SELECT DATE(2016, 12, 25)"}
        )
        self.validate_all(
            "SELECT CAST(CAST('2016-12-25 23:59:59' AS DATETIME) AS DATE)",
            read={"bigquery": "SELECT DATE(DATETIME '2016-12-25 23:59:59')"},
        )
        self.validate_all(
            "SELECT STRPTIME(STRFTIME(CAST(CAST('2016-12-25' AS TIMESTAMPTZ) AS DATE), '%d/%m/%Y') || ' ' || 'America/Los_Angeles', '%d/%m/%Y %Z')",
            read={
                "bigquery": "SELECT DATE(TIMESTAMP '2016-12-25', 'America/Los_Angeles')",
            },
        )
        self.validate_all(
            "SELECT CAST(CAST(STRPTIME('05/06/2020', '%m/%d/%Y') AS DATE) AS DATE)",
            read={"bigquery": "SELECT DATE(PARSE_DATE('%m/%d/%Y', '05/06/2020'))"},
        )
        self.validate_all(
            "SELECT CAST('2020-01-01' AS DATE) + INTERVAL (-1) DAY",
            read={"mysql": "SELECT DATE '2020-01-01' + INTERVAL -1 DAY"},
        )
        self.validate_all(
            "SELECT INTERVAL '1 quarter'",
            write={"duckdb": "SELECT (90 * INTERVAL '1' day)"},
        )
        self.validate_all(
            "SELECT ((DATE_TRUNC('DAY', CAST(CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP) + INTERVAL (0 - MOD((DAYOFWEEK(CAST(CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)) day) + (7 * INTERVAL (-5) day))) AS t1",
            read={
                "presto": "SELECT ((DATE_ADD('week', -5, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AS t1",
            },
        )
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
                "duckdb": "TO_TIMESTAMP(x / 1000)",
                "presto": "FROM_UNIXTIME(x / 1000)",
                "spark": "CAST(FROM_UNIXTIME(x / 1000) AS TIMESTAMP)",
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
                "presto": "DATE_FORMAT(x, '%Y-%m-%d %T')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%y-%-m')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%y-%-m', x)",
                "duckdb": "STRPTIME(x, '%y-%-m')",
                "presto": "DATE_PARSE(x, '%y-%c')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yy-M')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'yy-M')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP(x)",
            write={
                "bigquery": "UNIX_TO_TIME(x)",
                "duckdb": "TO_TIMESTAMP(x)",
                "presto": "FROM_UNIXTIME(x)",
                "hive": "FROM_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%-m/%-d/%y %-I:%M %p', x)",
                "duckdb": "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
                "presto": "DATE_PARSE(x, '%c/%e/%y %l:%i %p')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'M/d/yy h:mm a')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'M/d/yy h:mm a')",
            },
        )
        self.validate_all(
            "CAST(start AS TIMESTAMPTZ) AT TIME ZONE 'America/New_York'",
            read={
                "snowflake": "CONVERT_TIMEZONE('America/New_York', CAST(start AS TIMESTAMPTZ))",
            },
            write={
                "bigquery": "TIMESTAMP(DATETIME(CAST(start AS TIMESTAMP), 'America/New_York'))",
                "duckdb": "CAST(start AS TIMESTAMPTZ) AT TIME ZONE 'America/New_York'",
                "snowflake": "CONVERT_TIMEZONE('America/New_York', CAST(start AS TIMESTAMPTZ))",
            },
        )

    def test_sample(self):
        self.validate_all(
            "SELECT * FROM tbl USING SAMPLE 5",
            write={"duckdb": "SELECT * FROM tbl USING SAMPLE (5)"},
        )
        self.validate_all(
            "SELECT * FROM tbl USING SAMPLE 10%",
            write={"duckdb": "SELECT * FROM tbl USING SAMPLE (10 PERCENT)"},
        )
        self.validate_all(
            "SELECT * FROM tbl USING SAMPLE 10 PERCENT (bernoulli)",
            write={"duckdb": "SELECT * FROM tbl USING SAMPLE BERNOULLI (10 PERCENT)"},
        )
        self.validate_all(
            "SELECT * FROM tbl USING SAMPLE reservoir(50 ROWS) REPEATABLE (100)",
            write={"duckdb": "SELECT * FROM tbl USING SAMPLE RESERVOIR (50 ROWS) REPEATABLE (100)"},
        )
        self.validate_all(
            "SELECT * FROM tbl USING SAMPLE 10% (system, 377)",
            write={"duckdb": "SELECT * FROM tbl USING SAMPLE SYSTEM (10 PERCENT) REPEATABLE (377)"},
        )
        self.validate_all(
            "SELECT * FROM tbl TABLESAMPLE RESERVOIR(20%), tbl2 WHERE tbl.i=tbl2.i",
            write={
                "duckdb": "SELECT * FROM tbl TABLESAMPLE RESERVOIR (20 PERCENT), tbl2 WHERE tbl.i = tbl2.i"
            },
        )
        self.validate_all(
            "SELECT * FROM tbl, tbl2 WHERE tbl.i=tbl2.i USING SAMPLE RESERVOIR(20%)",
            write={
                "duckdb": "SELECT * FROM tbl, tbl2 WHERE tbl.i = tbl2.i USING SAMPLE RESERVOIR (20 PERCENT)"
            },
        )

    def test_array(self):
        self.validate_identity("ARRAY(SELECT id FROM t)")
        self.validate_identity("ARRAY((SELECT id FROM t))")

    def test_cast(self):
        self.validate_identity("CAST(x AS REAL)")
        self.validate_identity("CAST(x AS UINTEGER)")
        self.validate_identity("CAST(x AS UBIGINT)")
        self.validate_identity("CAST(x AS USMALLINT)")
        self.validate_identity("CAST(x AS UTINYINT)")
        self.validate_identity("CAST(x AS TEXT)")

        self.validate_all("CAST(x AS NUMERIC)", write={"duckdb": "CAST(x AS DOUBLE)"})
        self.validate_all("CAST(x AS CHAR)", write={"duckdb": "CAST(x AS TEXT)"})
        self.validate_all("CAST(x AS BPCHAR)", write={"duckdb": "CAST(x AS TEXT)"})
        self.validate_all("CAST(x AS STRING)", write={"duckdb": "CAST(x AS TEXT)"})
        self.validate_all("CAST(x AS INT1)", write={"duckdb": "CAST(x AS TINYINT)"})
        self.validate_all("CAST(x AS FLOAT4)", write={"duckdb": "CAST(x AS REAL)"})
        self.validate_all("CAST(x AS FLOAT)", write={"duckdb": "CAST(x AS REAL)"})
        self.validate_all("CAST(x AS INT4)", write={"duckdb": "CAST(x AS INT)"})
        self.validate_all("CAST(x AS INTEGER)", write={"duckdb": "CAST(x AS INT)"})
        self.validate_all("CAST(x AS SIGNED)", write={"duckdb": "CAST(x AS INT)"})
        self.validate_all("CAST(x AS BLOB)", write={"duckdb": "CAST(x AS BLOB)"})
        self.validate_all("CAST(x AS BYTEA)", write={"duckdb": "CAST(x AS BLOB)"})
        self.validate_all("CAST(x AS BINARY)", write={"duckdb": "CAST(x AS BLOB)"})
        self.validate_all("CAST(x AS VARBINARY)", write={"duckdb": "CAST(x AS BLOB)"})
        self.validate_all("CAST(x AS LOGICAL)", write={"duckdb": "CAST(x AS BOOLEAN)"})
        self.validate_all(
            "CAST(x AS BIT)",
            read={
                "duckdb": "CAST(x AS BITSTRING)",
            },
            write={
                "duckdb": "CAST(x AS BIT)",
                "tsql": "CAST(x AS BIT)",
            },
        )
        self.validate_all(
            "123::CHARACTER VARYING",
            write={
                "duckdb": "CAST(123 AS TEXT)",
            },
        )
        self.validate_all(
            "cast([[1]] as int[][])",
            write={
                "duckdb": "CAST(LIST_VALUE(LIST_VALUE(1)) AS INT[][])",
                "spark": "CAST(ARRAY(ARRAY(1)) AS ARRAY<ARRAY<INT>>)",
            },
        )
        self.validate_all(
            "CAST(x AS DATE) + INTERVAL (7 * -1) DAY", read={"spark": "DATE_SUB(x, 7)"}
        )
        self.validate_all(
            "TRY_CAST(1 AS DOUBLE)",
            read={
                "hive": "1d",
                "spark": "1d",
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
            "COL::BIGINT[]",
            write={
                "duckdb": "CAST(COL AS BIGINT[])",
                "presto": "CAST(COL AS ARRAY(BIGINT))",
                "hive": "CAST(COL AS ARRAY<BIGINT>)",
                "spark": "CAST(COL AS ARRAY<BIGINT>)",
                "postgres": "CAST(COL AS BIGINT[])",
                "snowflake": "CAST(COL AS ARRAY)",
            },
        )
        self.validate_all(
            "CAST([STRUCT_PACK(a := 1)] AS STRUCT(a BIGINT)[])",
            write={
                "duckdb": "CAST(LIST_VALUE({'a': 1}) AS STRUCT(a BIGINT)[])",
            },
        )
        self.validate_all(
            "CAST([[STRUCT_PACK(a := 1)]] AS STRUCT(a BIGINT)[][])",
            write={
                "duckdb": "CAST(LIST_VALUE(LIST_VALUE({'a': 1})) AS STRUCT(a BIGINT)[][])",
            },
        )

    def test_bool_or(self):
        self.validate_all(
            "SELECT a, LOGICAL_OR(b) FROM table GROUP BY a",
            write={"duckdb": "SELECT a, BOOL_OR(b) FROM table GROUP BY a"},
        )

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "snowflake": "ALTER TABLE db.t1 RENAME TO db.t2",
                "duckdb": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )
