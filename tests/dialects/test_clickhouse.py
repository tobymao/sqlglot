from datetime import date, datetime, timezone
from sqlglot import exp, parse_one
from sqlglot.dialects import ClickHouse
from sqlglot.expressions import convert
from sqlglot.optimizer import traverse_scope
from sqlglot.optimizer.qualify_columns import quote_identifiers
from tests.dialects.test_dialect import Validator
from sqlglot.errors import ErrorLevel


class TestClickhouse(Validator):
    dialect = "clickhouse"

    def test_clickhouse(self):
        expr = quote_identifiers(self.parse_one("{start_date:String}"), dialect="clickhouse")
        self.assertEqual(expr.sql("clickhouse"), "{start_date: String}")

        for string_type_enum in ClickHouse.Generator.STRING_TYPE_MAPPING:
            self.validate_identity(f"CAST(x AS {string_type_enum.value})", "CAST(x AS String)")

        # Arrays, maps and tuples can't be Nullable in ClickHouse
        for non_nullable_type in ("ARRAY<INT>", "MAP<INT, INT>", "STRUCT(a: INT)"):
            try_cast = parse_one(f"TRY_CAST(x AS {non_nullable_type})")
            target_type = try_cast.to.sql("clickhouse")
            self.assertEqual(try_cast.sql("clickhouse"), f"CAST(x AS {target_type})")

        for nullable_type in ("INT", "UINT", "BIGINT", "FLOAT", "DOUBLE", "TEXT", "DATE", "UUID"):
            try_cast = parse_one(f"TRY_CAST(x AS {nullable_type})")
            target_type = exp.DataType.build(nullable_type, dialect="clickhouse").sql("clickhouse")
            self.assertEqual(try_cast.sql("clickhouse"), f"CAST(x AS Nullable({target_type}))")

        expr = parse_one("count(x)")
        self.assertEqual(expr.sql(dialect="clickhouse"), "COUNT(x)")
        self.assertIsNone(expr._meta)

        self.validate_identity("SELECT 1 OR (1 = 2)")
        self.validate_identity("SELECT 1 AND (1 = 2)")
        self.validate_identity("SELECT json.a.:Int64")
        self.validate_identity("SELECT json.a.:JSON.b.:Int64")
        self.validate_identity("WITH arrayJoin([(1, [2, 3])]) AS arr SELECT arr")
        self.validate_identity("CAST(1 AS Bool)")
        self.validate_identity("SELECT toString(CHAR(104.1, 101, 108.9, 108.9, 111, 32))")
        self.validate_identity("@macro").assert_is(exp.Parameter).this.assert_is(exp.Var)
        self.validate_identity("SELECT toFloat(like)")
        self.validate_identity("SELECT like")
        self.validate_identity("SELECT STR_TO_DATE(str, fmt, tz)")
        self.validate_identity("SELECT STR_TO_DATE('05 12 2000', '%d %m %Y')")
        self.validate_identity("SELECT EXTRACT(YEAR FROM toDateTime('2023-02-01'))")
        self.validate_identity("extract(haystack, pattern)")
        self.validate_identity("SELECT * FROM x LIMIT 1 UNION ALL SELECT * FROM y")
        self.validate_identity("SELECT CAST(x AS Tuple(String, Array(Nullable(Float64))))")
        self.validate_identity("countIf(x, y)")
        self.validate_identity("x = y")
        self.validate_identity("x <> y")
        self.validate_identity("SELECT * FROM (SELECT a FROM b SAMPLE 0.01)")
        self.validate_identity("SELECT * FROM (SELECT a FROM b SAMPLE 1 / 10 OFFSET 1 / 2)")
        self.validate_identity("SELECT sum(foo * bar) FROM bla SAMPLE 10000000")
        self.validate_identity("CAST(x AS Nested(ID UInt32, Serial UInt32, EventTime DateTime))")
        self.validate_identity("CAST(x AS Enum('hello' = 1, 'world' = 2))")
        self.validate_identity("CAST(x AS Enum('hello', 'world'))")
        self.validate_identity("CAST(x AS Enum('hello' = 1, 'world'))")
        self.validate_identity("CAST(x AS Enum8('hello' = -123, 'world'))")
        self.validate_identity("CAST(x AS FixedString(1))")
        self.validate_identity("CAST(x AS LowCardinality(FixedString))")
        self.validate_identity("SELECT isNaN(1.0)")
        self.validate_identity("SELECT startsWith('Spider-Man', 'Spi')")
        self.validate_identity("SELECT xor(TRUE, FALSE)")
        self.validate_identity("CAST(['hello'], 'Array(Enum8(''hello'' = 1))')")
        self.validate_identity("SELECT x, COUNT() FROM y GROUP BY x WITH TOTALS")
        self.validate_identity("SELECT INTERVAL t.days DAY")
        self.validate_identity("SELECT match('abc', '([a-z]+)')")
        self.validate_identity("dictGet(x, 'y')")
        self.validate_identity("WITH final AS (SELECT 1) SELECT * FROM final")
        self.validate_identity("SELECT * FROM x FINAL")
        self.validate_identity("SELECT * FROM x AS y FINAL")
        self.validate_identity("'a' IN mapKeys(map('a', 1, 'b', 2))")
        self.validate_identity("CAST((1, 2) AS Tuple(a Int8, b Int16))")
        self.validate_identity("SELECT * FROM foo LEFT ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo LEFT ASOF JOIN bla")
        self.validate_identity("SELECT * FROM foo ASOF JOIN bla")
        self.validate_identity("SELECT * FROM foo ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo GLOBAL ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo GLOBAL LEFT ANY JOIN bla")
        self.validate_identity("SELECT quantile(0.5)(a)")
        self.validate_identity("SELECT quantiles(0.5)(a) AS x FROM t")
        self.validate_identity("SELECT quantilesIf(0.5)(a, a > 1) AS x FROM t")
        self.validate_identity("SELECT quantileState(0.5)(a) AS x FROM t")
        self.validate_identity("SELECT deltaSumMerge(a) AS x FROM t")
        self.validate_identity("SELECT quantiles(0.1, 0.2, 0.3)(a)")
        self.validate_identity("SELECT quantileTiming(0.5)(RANGE(100))")
        self.validate_identity("SELECT histogram(5)(a)")
        self.validate_identity("SELECT groupUniqArray(2)(a)")
        self.validate_identity("SELECT exponentialTimeDecayedAvg(60)(a, b)")
        self.validate_identity("levenshteinDistance(col1, col2)", "editDistance(col1, col2)")
        self.validate_identity("SELECT * FROM foo WHERE x GLOBAL IN (SELECT * FROM bar)")
        self.validate_identity("SELECT * FROM foo WHERE x GLOBAL NOT IN (SELECT * FROM bar)")
        self.validate_identity("POSITION(haystack, needle)")
        self.validate_identity("POSITION(haystack, needle, position)")
        self.validate_identity("CAST(x AS DATETIME)", "CAST(x AS DateTime)")
        self.validate_identity("CAST(x AS TIMESTAMPTZ)", "CAST(x AS DateTime)")
        self.validate_identity("CAST(x as MEDIUMINT)", "CAST(x AS Int32)")
        self.validate_identity("CAST(x AS DECIMAL(38, 2))", "CAST(x AS Decimal(38, 2))")
        self.validate_identity("SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src")
        self.validate_identity("""SELECT JSONExtractString('{"x": {"y": 1}}', 'x', 'y')""")
        self.validate_identity("SELECT * FROM table LIMIT 1 BY a, b")
        self.validate_identity("SELECT * FROM table LIMIT 2 OFFSET 1 BY a, b")
        self.validate_identity("TRUNCATE TABLE t1 ON CLUSTER test_cluster")
        self.validate_identity("TRUNCATE DATABASE db")
        self.validate_identity("TRUNCATE DATABASE db ON CLUSTER test_cluster")
        self.validate_identity(
            "SELECT DATE_BIN(toDateTime('2023-01-01 14:45:00'), INTERVAL '1' MINUTE, toDateTime('2023-01-01 14:35:30'), 'UTC')",
        )
        self.validate_identity(
            "SELECT CAST(1730098800 AS DateTime64) AS DATETIME, 'test' AS interp ORDER BY DATETIME WITH FILL FROM toDateTime64(1730098800, 3) - INTERVAL '7' HOUR TO toDateTime64(1730185140, 3) - INTERVAL '7' HOUR STEP toIntervalSecond(900) INTERPOLATE (interp)"
        )
        self.validate_identity(
            "SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) WINDOW window_name AS (PARTITION BY number) QUALIFY partition_count = 4 ORDER BY number"
        )
        self.validate_identity(
            "SELECT id, quantileGK(100, 0.95)(reading) OVER (PARTITION BY id ORDER BY id RANGE BETWEEN 30000 PRECEDING AND CURRENT ROW) AS window FROM table"
        )
        self.validate_identity(
            "SELECT * FROM table LIMIT 1 BY CONCAT(datalayerVariantNo, datalayerProductId, warehouse)"
        )
        self.validate_identity(
            """SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a')"""
        )
        self.validate_identity(
            "ATTACH DATABASE DEFAULT ENGINE = ORDINARY", check_command_warning=True
        )
        self.validate_identity(
            "SELECT n, source FROM (SELECT toFloat32(number % 10) AS n, 'original' AS source FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL"
        )
        self.validate_identity(
            "SELECT n, source FROM (SELECT toFloat32(number % 10) AS n, 'original' AS source FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5"
        )
        self.validate_identity(
            "SELECT toDate((number * 10) * 86400) AS d1, toDate(number * 86400) AS d2, 'original' AS source FROM numbers(10) WHERE (number % 3) = 1 ORDER BY d2 WITH FILL, d1 WITH FILL STEP 5"
        )
        self.validate_identity(
            "SELECT n, source, inter FROM (SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1)"
        )
        self.validate_identity(
            "SELECT SUM(1) AS impressions, arrayJoin(cities) AS city, arrayJoin(browsers) AS browser FROM (SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities, ['Firefox', 'Chrome', 'Chrome'] AS browsers) GROUP BY 2, 3"
        )
        self.validate_identity(
            "SELECT sum(1) AS impressions, (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city, t.2 AS browser FROM (SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities, ['Firefox', 'Chrome', 'Chrome'] AS browsers) GROUP BY 2, 3"
        )
        self.validate_identity(
            'SELECT CAST(tuple(1 AS "a", 2 AS "b", 3.0 AS "c").2 AS Nullable(String))'
        )
        self.validate_identity(
            "CREATE TABLE test (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()"
        )
        self.validate_identity(
            "CREATE TABLE test ON CLUSTER default (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()"
        )
        self.validate_identity(
            "CREATE MATERIALIZED VIEW test_view ON CLUSTER cl1 (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple() AS SELECT * FROM test_data"
        )
        self.validate_identity(
            "CREATE MATERIALIZED VIEW test_view ON CLUSTER cl1 TO table1 AS SELECT * FROM test_data"
        )
        self.validate_identity(
            "CREATE MATERIALIZED VIEW test_view TO db.table1 (id UInt8) AS SELECT * FROM test_data"
        )
        self.validate_identity(
            "CREATE TABLE t (foo String CODEC(LZ4HC(9), ZSTD, DELTA), size String ALIAS formatReadableSize(size_bytes), INDEX idx1 a TYPE bloom_filter(0.001) GRANULARITY 1, INDEX idx2 a TYPE set(100) GRANULARITY 2, INDEX idx3 a TYPE minmax GRANULARITY 3)"
        )
        self.validate_identity(
            "SELECT generate_series FROM generate_series(0, 10) AS g(x)",
        )
        self.validate_identity(
            "SELECT and(1, 2)",
            "SELECT 1 AND 2",
        )
        self.validate_identity(
            "SELECT or(1, 2)",
            "SELECT 1 OR 2",
        )
        self.validate_identity(
            "SELECT generate_series FROM generate_series(0, 10) AS g",
            "SELECT generate_series FROM generate_series(0, 10) AS g(generate_series)",
        )
        self.validate_identity(
            "INSERT INTO tab VALUES ({'key1': 1, 'key2': 10}), ({'key1': 2, 'key2': 20}), ({'key1': 3, 'key2': 30})",
            "INSERT INTO tab VALUES (map('key1', 1, 'key2', 10)), (map('key1', 2, 'key2', 20)), (map('key1', 3, 'key2', 30))",
        )
        self.validate_identity(
            "SELECT (toUInt8('1') + toUInt8('2')) IS NOT NULL",
            "SELECT NOT ((toUInt8('1') + toUInt8('2')) IS NULL)",
        )
        self.validate_identity(
            "SELECT $1$foo$1$",
            "SELECT 'foo'",
        )
        self.validate_identity(
            "SELECT * FROM table LIMIT 1, 2 BY a, b",
            "SELECT * FROM table LIMIT 2 OFFSET 1 BY a, b",
        )
        self.validate_identity(
            "SELECT SUM(1) AS impressions FROM (SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities) WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin']",
            "SELECT SUM(1) AS impressions FROM (SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities) WHERE arrayJoin(cities) IN ('Istanbul', 'Berlin')",
        )

        self.validate_all(
            "SELECT CAST(STR_TO_DATE(SUBSTRING(a.eta, 1, 10), '%Y-%m-%d') AS Nullable(DATE))",
            read={
                "clickhouse": "SELECT CAST(STR_TO_DATE(SUBSTRING(a.eta, 1, 10), '%Y-%m-%d') AS Nullable(DATE))",
                "oracle": "SELECT to_date(substr(a.eta, 1,10), 'YYYY-MM-DD')",
            },
        )
        self.validate_all(
            "CHAR(67) || CHAR(65) || CHAR(84)",
            read={
                "clickhouse": "CHAR(67) || CHAR(65) || CHAR(84)",
                "oracle": "CHR(67) || CHR(65) || CHR(84)",
            },
        )
        self.validate_all(
            "SELECT lagInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
            read={
                "clickhouse": "SELECT lagInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
                "oracle": "SELECT LAG(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
            },
        )
        self.validate_all(
            "SELECT leadInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
            read={
                "clickhouse": "SELECT leadInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
                "oracle": "SELECT LEAD(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees",
            },
        )
        self.validate_all(
            "SELECT CAST(STR_TO_DATE('05 12 2000', '%d %m %Y') AS Nullable(DATE))",
            read={
                "clickhouse": "SELECT CAST(STR_TO_DATE('05 12 2000', '%d %m %Y') AS Nullable(DATE))",
                "postgres": "SELECT TO_DATE('05 12 2000', 'DD MM YYYY')",
            },
            write={
                "clickhouse": "SELECT CAST(STR_TO_DATE('05 12 2000', '%d %m %Y') AS Nullable(DATE))",
                "postgres": "SELECT CAST(CAST(TO_DATE('05 12 2000', 'DD MM YYYY') AS TIMESTAMP) AS DATE)",
            },
        )
        self.validate_all(
            "SELECT * FROM x PREWHERE y = 1 WHERE z = 2",
            write={
                "": "SELECT * FROM x WHERE z = 2",
                "clickhouse": "SELECT * FROM x PREWHERE y = 1 WHERE z = 2",
            },
        )
        self.validate_all(
            "SELECT * FROM x AS prewhere",
            read={
                "clickhouse": "SELECT * FROM x AS prewhere",
                "duckdb": "SELECT * FROM x prewhere",
            },
        )
        self.validate_all(
            "SELECT a, b FROM (SELECT * FROM x) AS t(a, b)",
            read={
                "clickhouse": "SELECT a, b FROM (SELECT * FROM x) AS t(a, b)",
                "duckdb": "SELECT a, b FROM (SELECT * FROM x) AS t(a, b)",
            },
        )
        self.validate_all(
            "SELECT arrayJoin([1,2,3])",
            write={
                "clickhouse": "SELECT arrayJoin([1, 2, 3])",
                "postgres": "SELECT UNNEST(ARRAY[1, 2, 3])",
            },
        )
        self.validate_all(
            "has([1], x)",
            read={
                "postgres": "x = any(array[1])",
            },
        )
        self.validate_all(
            "NOT has([1], x)",
            read={
                "postgres": "any(array[1]) <> x",
            },
        )
        self.validate_all(
            "SELECT CAST('2020-01-01' AS Nullable(DateTime)) + INTERVAL '500' MICROSECOND",
            read={
                "duckdb": "SELECT TIMESTAMP '2020-01-01' + INTERVAL '500 us'",
                "postgres": "SELECT TIMESTAMP '2020-01-01' + INTERVAL '500 us'",
            },
            write={
                "clickhouse": "SELECT CAST('2020-01-01' AS Nullable(DateTime)) + INTERVAL '500' MICROSECOND",
                "duckdb": "SELECT CAST('2020-01-01' AS TIMESTAMP) + INTERVAL '500' MICROSECOND",
                "postgres": "SELECT CAST('2020-01-01' AS TIMESTAMP) + INTERVAL '500 MICROSECOND'",
            },
        )
        self.validate_all(
            "SELECT CURRENT_DATE()",
            read={
                "clickhouse": "SELECT CURRENT_DATE()",
                "postgres": "SELECT CURRENT_DATE",
            },
        )
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP()",
            read={
                "clickhouse": "SELECT CURRENT_TIMESTAMP()",
                "postgres": "SELECT CURRENT_TIMESTAMP",
            },
        )
        self.validate_all(
            "SELECT match('ThOmAs', CONCAT('(?i)', 'thomas'))",
            read={
                "postgres": "SELECT 'ThOmAs' ~* 'thomas'",
            },
        )
        self.validate_all(
            "SELECT match('ThOmAs', CONCAT('(?i)', x)) FROM t",
            read={
                "postgres": "SELECT 'ThOmAs' ~* x FROM t",
            },
        )
        self.validate_all(
            "SELECT '\\0'",
            read={
                "mysql": "SELECT '\0'",
            },
            write={
                "clickhouse": "SELECT '\\0'",
                "mysql": "SELECT '\0'",
            },
        )
        self.validate_all(
            "DATE_ADD(DAY, 1, x)",
            read={
                "clickhouse": "dateAdd(DAY, 1, x)",
                "presto": "DATE_ADD('DAY', 1, x)",
            },
            write={
                "clickhouse": "DATE_ADD(DAY, 1, x)",
                "presto": "DATE_ADD('DAY', 1, x)",
                "": "DATE_ADD(x, 1, 'DAY')",
            },
        )
        self.validate_all(
            "DATE_DIFF(DAY, a, b)",
            read={
                "clickhouse": "dateDiff(DAY, a, b)",
                "presto": "DATE_DIFF('DAY', a, b)",
            },
            write={
                "clickhouse": "DATE_DIFF(DAY, a, b)",
                "presto": "DATE_DIFF('DAY', a, b)",
                "": "DATEDIFF(b, a, DAY)",
            },
        )
        self.validate_all(
            "SELECT xor(1, 0)",
            read={
                "clickhouse": "SELECT xor(1, 0)",
                "mysql": "SELECT 1 XOR 0",
            },
            write={
                "mysql": "SELECT 1 XOR 0",
            },
        )
        self.validate_all(
            "SELECT xor(0, 1, xor(1, 0, 0))",
            write={
                "clickhouse": "SELECT xor(0, 1, xor(1, 0, 0))",
                "mysql": "SELECT 0 XOR 1 XOR 1 XOR 0 XOR 0",
            },
        )
        self.validate_all(
            "SELECT xor(xor(1, 0), 1)",
            read={
                "clickhouse": "SELECT xor(xor(1, 0), 1)",
                "mysql": "SELECT 1 XOR 0 XOR 1",
            },
            write={
                "clickhouse": "SELECT xor(xor(1, 0), 1)",
                "mysql": "SELECT 1 XOR 0 XOR 1",
            },
        )
        self.validate_all(
            "CONCAT(a, b)",
            read={
                "clickhouse": "CONCAT(a, b)",
                "mysql": "CONCAT(a, b)",
            },
            write={
                "mysql": "CONCAT(a, b)",
                "postgres": "CONCAT(a, b)",
            },
        )
        self.validate_all(
            r"'Enum8(\'Sunday\' = 0)'", write={"clickhouse": "'Enum8(''Sunday'' = 0)'"}
        )
        self.validate_all(
            "SELECT uniq(x) FROM (SELECT any(y) AS x FROM (SELECT 1 AS y))",
            read={
                "bigquery": "SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) x FROM (SELECT 1 y))",
            },
            write={
                "bigquery": "SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) AS x FROM (SELECT 1 AS y))",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "clickhouse": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
            },
        )
        self.validate_all(
            "CAST(1 AS NULLABLE(Int64))",
            write={
                "clickhouse": "CAST(1 AS Nullable(Int64))",
            },
        )
        self.validate_all(
            "CAST(1 AS Nullable(DateTime64(6, 'UTC')))",
            write={"clickhouse": "CAST(1 AS Nullable(DateTime64(6, 'UTC')))"},
        )
        self.validate_all(
            "SELECT x #! comment",
            write={"": "SELECT x /* comment */"},
        )
        self.validate_all(
            "SELECT quantileIf(0.5)(a, true)",
            write={
                "clickhouse": "SELECT quantileIf(0.5)(a, TRUE)",
            },
        )
        self.validate_identity(
            "SELECT POSITION(needle IN haystack)", "SELECT POSITION(haystack, needle)"
        )
        self.validate_identity(
            "SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result = 'break'"
        )
        self.validate_identity("SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_")
        self.validate_identity("SELECT * FROM x FORMAT PrettyCompact")
        self.validate_identity(
            "SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_ FORMAT PrettyCompact"
        )
        self.validate_all(
            "SELECT * FROM foo JOIN bar USING id, name",
            write={"clickhouse": "SELECT * FROM foo JOIN bar USING (id, name)"},
        )
        self.validate_all(
            "SELECT * FROM foo ANY LEFT JOIN bla ON foo.c1 = bla.c2",
            write={"clickhouse": "SELECT * FROM foo LEFT ANY JOIN bla ON foo.c1 = bla.c2"},
        )
        self.validate_all(
            "SELECT * FROM foo GLOBAL ANY LEFT JOIN bla ON foo.c1 = bla.c2",
            write={"clickhouse": "SELECT * FROM foo GLOBAL LEFT ANY JOIN bla ON foo.c1 = bla.c2"},
        )
        self.validate_all(
            """
            SELECT
                loyalty,
                count()
            FROM hits SEMI LEFT JOIN users USING (UserID)
            GROUP BY loyalty
            ORDER BY loyalty ASC
            """,
            write={
                "clickhouse": "SELECT loyalty, count() FROM hits LEFT SEMI JOIN users USING (UserID)"
                " GROUP BY loyalty ORDER BY loyalty ASC"
            },
        )
        self.validate_all(
            "SELECT quantile(0.5)(a)",
            read={
                "duckdb": "SELECT quantile(a, 0.5)",
                "clickhouse": "SELECT median(a)",
            },
            write={
                "clickhouse": "SELECT quantile(0.5)(a)",
            },
        )
        self.validate_all(
            "SELECT quantiles(0.5, 0.4)(a)",
            read={"duckdb": "SELECT quantile(a, [0.5, 0.4])"},
            write={"clickhouse": "SELECT quantiles(0.5, 0.4)(a)"},
        )
        self.validate_all(
            "SELECT quantiles(0.5)(a)",
            read={"duckdb": "SELECT quantile(a, [0.5])"},
            write={"clickhouse": "SELECT quantiles(0.5)(a)"},
        )

        self.validate_identity("SELECT isNaN(x)")
        self.validate_all(
            "SELECT IS_NAN(x), ISNAN(x)",
            write={"clickhouse": "SELECT isNaN(x), isNaN(x)"},
        )

        self.validate_identity("SELECT startsWith('a', 'b')")
        self.validate_all(
            "SELECT STARTS_WITH('a', 'b'), STARTSWITH('a', 'b')",
            write={"clickhouse": "SELECT startsWith('a', 'b'), startsWith('a', 'b')"},
        )
        self.validate_identity("SYSTEM STOP MERGES foo.bar", check_command_warning=True)

        self.validate_identity(
            "INSERT INTO FUNCTION s3('url', 'CSV', 'name String, value UInt32', 'gzip') SELECT name, value FROM existing_table"
        )
        self.validate_identity(
            "INSERT INTO FUNCTION remote('localhost', default.simple_table) VALUES (100, 'inserted via remote()')"
        )
        self.validate_identity(
            """INSERT INTO TABLE FUNCTION hdfs('hdfs://hdfs1:9000/test', 'TSV', 'name String, column2 UInt32, column3 UInt32') VALUES ('test', 1, 2)""",
            """INSERT INTO FUNCTION hdfs('hdfs://hdfs1:9000/test', 'TSV', 'name String, column2 UInt32, column3 UInt32') VALUES ('test', 1, 2)""",
        )

        self.validate_identity("SELECT 1 FORMAT TabSeparated")
        self.validate_identity("SELECT * FROM t FORMAT TabSeparated")
        self.validate_identity("SELECT FORMAT")
        self.validate_identity("1 AS FORMAT").assert_is(exp.Alias)

        self.validate_identity("SELECT formatDateTime(NOW(), '%Y-%m-%d', '%T')")
        self.validate_all(
            "SELECT formatDateTime(NOW(), '%Y-%m-%d')",
            read={
                "clickhouse": "SELECT formatDateTime(NOW(), '%Y-%m-%d')",
                "mysql": "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d')",
            },
            write={
                "clickhouse": "SELECT formatDateTime(NOW(), '%Y-%m-%d')",
                "mysql": "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d')",
            },
        )

        self.validate_identity("ALTER TABLE visits DROP PARTITION 201901")
        self.validate_identity("ALTER TABLE visits DROP PARTITION ALL")
        self.validate_identity(
            "ALTER TABLE visits DROP PARTITION tuple(toYYYYMM(toDate('2019-01-25')))"
        )
        self.validate_identity("ALTER TABLE visits DROP PARTITION ID '201901'")

        self.validate_identity("ALTER TABLE visits REPLACE PARTITION 201901 FROM visits_tmp")
        self.validate_identity("ALTER TABLE visits REPLACE PARTITION ALL FROM visits_tmp")
        self.validate_identity(
            "ALTER TABLE visits REPLACE PARTITION tuple(toYYYYMM(toDate('2019-01-25'))) FROM visits_tmp"
        )
        self.validate_identity("ALTER TABLE visits REPLACE PARTITION ID '201901' FROM visits_tmp")
        self.validate_identity("ALTER TABLE visits ON CLUSTER test_cluster DROP COLUMN col1")
        self.validate_identity("DELETE FROM tbl ON CLUSTER test_cluster WHERE date = '2019-01-01'")

        self.assertIsInstance(
            parse_one("Tuple(select Int64)", into=exp.DataType, read="clickhouse"), exp.DataType
        )

        self.validate_identity("INSERT INTO t (col1, col2) VALUES ('abcd', 1234)")
        self.validate_all(
            "INSERT INTO t (col1, col2) VALUES ('abcd', 1234)",
            read={
                # looks like values table function, but should be parsed as VALUES block
                "clickhouse": "INSERT INTO t (col1, col2) values('abcd', 1234)"
            },
            write={
                "clickhouse": "INSERT INTO t (col1, col2) VALUES ('abcd', 1234)",
                "postgres": "INSERT INTO t (col1, col2) VALUES ('abcd', 1234)",
            },
        )
        self.validate_identity("SELECT TRIM(TRAILING ')' FROM '(   Hello, world!   )')")
        self.validate_identity("SELECT TRIM(LEADING '(' FROM '(   Hello, world!   )')")
        self.validate_identity("current_timestamp").assert_is(exp.Column)

        self.validate_identity("SELECT * APPLY(sum) FROM columns_transformers")
        self.validate_identity("SELECT COLUMNS('[jk]') APPLY(toString) FROM columns_transformers")
        self.validate_identity(
            "SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers"
        )
        self.validate_identity("SELECT * APPLY(sum), COLUMNS('col') APPLY(sum) APPLY(avg) FROM t")
        self.validate_identity(
            "SELECT * FROM ABC WHERE hasAny(COLUMNS('.*field') APPLY(toUInt64) APPLY(to), (SELECT groupUniqArray(toUInt64(field))))"
        )
        self.validate_identity("SELECT col apply", "SELECT col AS apply")
        self.validate_identity(
            "SELECT name FROM data WHERE (SELECT DISTINCT name FROM data) IS NOT NULL",
            "SELECT name FROM data WHERE NOT ((SELECT DISTINCT name FROM data) IS NULL)",
        )

        self.validate_identity("SELECT 1_2_3_4_5", "SELECT 12345")
        self.validate_identity("SELECT 1_b", "SELECT 1_b")
        self.validate_identity(
            "SELECT COUNT(1) FROM table SETTINGS additional_table_filters = {'a': 'b', 'c': 'd'}"
        )
        self.validate_identity("SELECT arrayConcat([1, 2], [3, 4])")

    def test_clickhouse_values(self):
        values = exp.select("*").from_(
            exp.values([exp.tuple_(1, 2, 3)], alias="subq", columns=["a", "b", "c"])
        )
        self.assertEqual(
            values.sql("clickhouse"),
            "SELECT * FROM (SELECT 1 AS a, 2 AS b, 3 AS c) AS subq",
        )

        self.validate_identity("INSERT INTO t (col1, col2) VALUES ('abcd', 1234)")
        self.validate_identity(
            "INSERT INTO t (col1, col2) FORMAT Values('abcd', 1234)",
            "INSERT INTO t (col1, col2) VALUES ('abcd', 1234)",
        )

        self.validate_all(
            "SELECT col FROM (SELECT 1 AS col) AS _t",
            read={
                "duckdb": "SELECT col FROM (VALUES (1)) AS _t(col)",
            },
        )
        self.validate_all(
            "SELECT col1, col2 FROM (SELECT 1 AS col1, 2 AS col2 UNION ALL SELECT 3, 4) AS _t",
            read={
                "duckdb": "SELECT col1, col2 FROM (VALUES (1, 2), (3, 4)) AS _t(col1, col2)",
            },
        )

    def test_cte(self):
        self.validate_identity("WITH 'x' AS foo SELECT foo")
        self.validate_identity("WITH ['c'] AS field_names SELECT field_names")
        self.validate_identity("WITH SUM(bytes) AS foo SELECT foo FROM system.parts")
        self.validate_identity("WITH (SELECT foo) AS bar SELECT bar + 5")
        self.validate_identity("WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1")

        query = parse_one("""WITH (SELECT 1) AS y SELECT * FROM y""", read="clickhouse")
        self.assertIsInstance(query.args["with"].expressions[0].this, exp.Subquery)
        self.assertEqual(query.args["with"].expressions[0].alias, "y")

        query = "WITH 1 AS var SELECT var"
        for error_level in [ErrorLevel.IGNORE, ErrorLevel.RAISE, ErrorLevel.IMMEDIATE]:
            self.assertEqual(
                self.parse_one(query, error_level=error_level).sql(dialect=self.dialect),
                query,
            )

    def test_ternary(self):
        self.validate_all("x ? 1 : 2", write={"clickhouse": "CASE WHEN x THEN 1 ELSE 2 END"})
        self.validate_all(
            "IF(BAR(col), sign > 0 ? FOO() : 0, 1)",
            write={
                "clickhouse": "CASE WHEN BAR(col) THEN CASE WHEN sign > 0 THEN FOO() ELSE 0 END ELSE 1 END"
            },
        )
        self.validate_all(
            "x AND FOO() > 3 + 2 ? 1 : 2",
            write={"clickhouse": "CASE WHEN x AND FOO() > 3 + 2 THEN 1 ELSE 2 END"},
        )
        self.validate_all(
            "x ? (y ? 1 : 2) : 3",
            write={"clickhouse": "CASE WHEN x THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END"},
        )
        self.validate_all(
            "x AND (foo() ? FALSE : TRUE) ? (y ? 1 : 2) : 3",
            write={
                "clickhouse": "CASE WHEN x AND (CASE WHEN foo() THEN FALSE ELSE TRUE END) THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END"
            },
        )

        ternary = parse_one("x ? (y ? 1 : 2) : 3", read="clickhouse")

        self.assertIsInstance(ternary, exp.If)
        self.assertIsInstance(ternary.this, exp.Column)
        self.assertIsInstance(ternary.args["true"], exp.Paren)
        self.assertIsInstance(ternary.args["false"], exp.Literal)

        nested_ternary = ternary.args["true"].this

        self.assertIsInstance(nested_ternary.this, exp.Column)
        self.assertIsInstance(nested_ternary.args["true"], exp.Literal)
        self.assertIsInstance(nested_ternary.args["false"], exp.Literal)

        parse_one("a and b ? 1 : 2", read="clickhouse").assert_is(exp.If).this.assert_is(exp.And)

    def test_parameterization(self):
        self.validate_all(
            "SELECT {abc: UInt32}, {b: String}, {c: DateTime},{d: Map(String, Array(UInt8))}, {e: Tuple(UInt8, String)}",
            write={
                "clickhouse": "SELECT {abc: UInt32}, {b: String}, {c: DateTime}, {d: Map(String, Array(UInt8))}, {e: Tuple(UInt8, String)}",
                "": "SELECT :abc, :b, :c, :d, :e",
            },
        )
        self.validate_all(
            "SELECT * FROM {table: Identifier}",
            write={"clickhouse": "SELECT * FROM {table: Identifier}"},
        )

    def test_signed_and_unsigned_types(self):
        data_types = [
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
        ]
        for data_type in data_types:
            self.validate_all(
                f"pow(2, 32)::{data_type}",
                write={"clickhouse": f"CAST(pow(2, 32) AS {data_type})"},
            )

    def test_geom_types(self):
        data_types = ["Point", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        for data_type in data_types:
            with self.subTest(f"Casting to ClickHouse {data_type}"):
                self.validate_identity(f"SELECT CAST(val AS {data_type})")

    def test_aggregate_function_column_with_any_keyword(self):
        # Regression test for https://github.com/tobymao/sqlglot/issues/4723
        self.validate_all(
            """
            CREATE TABLE my_db.my_table
            (
                someId UUID,
                aggregatedColumn AggregateFunction(any, String),
                aggregatedColumnWithParams AggregateFunction(any(somecolumn), String),
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY (someId)
                    """,
            write={
                "clickhouse": """CREATE TABLE my_db.my_table (
  someId UUID,
  aggregatedColumn AggregateFunction(any, String),
  aggregatedColumnWithParams AggregateFunction(any(somecolumn), String)
)
ENGINE=AggregatingMergeTree()
ORDER BY (
  someId
)""",
            },
            pretty=True,
        )

    def test_ddl(self):
        db_table_expr = exp.Table(this=None, db=exp.to_identifier("foo"), catalog=None)
        create_with_cluster = exp.Create(
            this=db_table_expr,
            kind="DATABASE",
            properties=exp.Properties(expressions=[exp.OnCluster(this=exp.to_identifier("c"))]),
        )
        self.assertEqual(create_with_cluster.sql("clickhouse"), "CREATE DATABASE foo ON CLUSTER c")

        # Transpiled CREATE SCHEMA may have OnCluster property set
        create_with_cluster = exp.Create(
            this=db_table_expr,
            kind="SCHEMA",
            properties=exp.Properties(expressions=[exp.OnCluster(this=exp.to_identifier("c"))]),
        )
        self.assertEqual(create_with_cluster.sql("clickhouse"), "CREATE DATABASE foo ON CLUSTER c")

        ctas_with_comment = exp.Create(
            this=exp.table_("foo"),
            kind="TABLE",
            expression=exp.select("*").from_("db.other_table"),
            properties=exp.Properties(
                expressions=[
                    exp.EngineProperty(this=exp.var("Memory")),
                    exp.SchemaCommentProperty(this=exp.Literal.string("foo")),
                ],
            ),
        )
        self.assertEqual(
            ctas_with_comment.sql("clickhouse"),
            "CREATE TABLE foo ENGINE=Memory AS (SELECT * FROM db.other_table) COMMENT 'foo'",
        )

        self.validate_identity("CREATE FUNCTION linear_equation AS (x, k, b) -> k * x + b")
        self.validate_identity("CREATE MATERIALIZED VIEW a.b TO a.c (c Int32) AS SELECT * FROM a.d")
        self.validate_identity("""CREATE TABLE ip_data (ip4 IPv4, ip6 IPv6) ENGINE=TinyLog()""")
        self.validate_identity("""CREATE TABLE dates (dt1 Date32) ENGINE=TinyLog()""")
        self.validate_identity("CREATE TABLE named_tuples (a Tuple(select String, i Int64))")
        self.validate_identity("""CREATE TABLE t (a String) EMPTY AS SELECT * FROM dummy""")
        self.validate_identity(
            "CREATE TABLE t1 (a String EPHEMERAL, b String EPHEMERAL func(), c String MATERIALIZED func(), d String ALIAS func()) ENGINE=TinyLog()"
        )
        self.validate_identity(
            "CREATE TABLE t (a String, b String, c UInt64, PROJECTION p1 (SELECT a, sum(c) GROUP BY a, b), PROJECTION p2 (SELECT b, sum(c) GROUP BY b)) ENGINE=MergeTree()"
        )
        self.validate_identity(
            """CREATE TABLE xyz (ts DateTime, data String) ENGINE=MergeTree() ORDER BY ts SETTINGS index_granularity = 8192 COMMENT '{"key": "value"}'"""
        )
        self.validate_identity(
            "INSERT INTO FUNCTION s3('a', 'b', 'c', 'd', 'e') PARTITION BY CONCAT(s1, s2, s3, s4) SETTINGS set1 = 1, set2 = '2' SELECT * FROM some_table SETTINGS foo = 3"
        )
        self.validate_identity(
            'CREATE TABLE data5 ("x" UInt32, "y" UInt32) ENGINE=MergeTree ORDER BY (round(y / 1000000000), cityHash64(x)) SAMPLE BY cityHash64(x)'
        )
        self.validate_identity(
            "CREATE TABLE foo (x UInt32) TTL time_column + INTERVAL '1' MONTH DELETE WHERE column = 'value'"
        )
        self.validate_identity(
            "CREATE FUNCTION parity_str AS (n) -> IF(n % 2, 'odd', 'even')",
            "CREATE FUNCTION parity_str AS n -> CASE WHEN n % 2 THEN 'odd' ELSE 'even' END",
        )
        self.validate_identity(
            "CREATE TABLE a ENGINE=Memory AS SELECT 1 AS c COMMENT 'foo'",
            "CREATE TABLE a ENGINE=Memory AS (SELECT 1 AS c) COMMENT 'foo'",
        )
        self.validate_identity(
            'CREATE TABLE t1 ("x" UInt32, "y" Dynamic, "z" Dynamic(max_types = 10)) ENGINE=MergeTree ORDER BY x'
        )

        self.validate_all(
            "CREATE DATABASE x",
            read={
                "duckdb": "CREATE SCHEMA x",
            },
            write={
                "clickhouse": "CREATE DATABASE x",
                "duckdb": "CREATE SCHEMA x",
            },
        )
        self.validate_all(
            "DROP DATABASE x",
            read={
                "duckdb": "DROP SCHEMA x",
            },
            write={
                "clickhouse": "DROP DATABASE x",
                "duckdb": "DROP SCHEMA x",
            },
        )
        self.validate_all(
            """
            CREATE TABLE example1 (
               timestamp DateTime,
               x UInt32 TTL now() + INTERVAL 1 MONTH,
               y String TTL timestamp + INTERVAL 1 DAY,
               z String
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            """,
            write={
                "clickhouse": """CREATE TABLE example1 (
  timestamp DateTime,
  x UInt32 TTL now() + INTERVAL '1' MONTH,
  y String TTL timestamp + INTERVAL '1' DAY,
  z String
)
ENGINE=MergeTree
ORDER BY tuple()""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE test (id UInt64, timestamp DateTime64, data String, max_hits UInt64, sum_hits UInt64) ENGINE = MergeTree
            PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
            TTL timestamp + INTERVAL 1 DAY
            GROUP BY id, toStartOfDay(timestamp)
            SET
               max_hits = max(max_hits),
               sum_hits = sum(sum_hits)
            """,
            write={
                "clickhouse": """CREATE TABLE test (
  id UInt64,
  timestamp DateTime64,
  data String,
  max_hits UInt64,
  sum_hits UInt64
)
ENGINE=MergeTree
PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
TTL
  timestamp + INTERVAL '1' DAY
GROUP BY
  id,
  toStartOfDay(timestamp)
SET
  max_hits = max(max_hits),
  sum_hits = sum(sum_hits)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE test (id String, data String) ENGINE = AggregatingMergeTree()
                ORDER BY tuple()
            SETTINGS
                max_suspicious_broken_parts=500,
                parts_to_throw_insert=100
            """,
            write={
                "clickhouse": """CREATE TABLE test (
  id String,
  data String
)
ENGINE=AggregatingMergeTree()
ORDER BY tuple()
SETTINGS
  max_suspicious_broken_parts = 500,
  parts_to_throw_insert = 100""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE example_table
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE,
                d + INTERVAL 1 WEEK TO VOLUME 'aaa',
                d + INTERVAL 2 WEEK TO DISK 'bbb';
            """,
            write={
                "clickhouse": """CREATE TABLE example_table (
  d DateTime,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE,
  d + INTERVAL '1' WEEK TO VOLUME 'aaa',
  d + INTERVAL '2' WEEK TO DISK 'bbb'""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_with_where
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
            """,
            write={
                "clickhouse": """CREATE TABLE table_with_where (
  d DateTime,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE
WHERE
  toDayOfWeek(d) = 1""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_for_recompression
            (
                d DateTime,
                key UInt64,
                value String
            ) ENGINE MergeTree()
            ORDER BY tuple()
            PARTITION BY key
            TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
            SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
            """,
            write={
                "clickhouse": """CREATE TABLE table_for_recompression (
  d DateTime,
  key UInt64,
  value String
)
ENGINE=MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL
  d + INTERVAL '1' MONTH RECOMPRESS CODEC(ZSTD(17)),
  d + INTERVAL '1' YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS
  min_rows_for_wide_part = 0,
  min_bytes_for_wide_part = 0""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_for_aggregation
            (
                d DateTime,
                k1 Int,
                k2 Int,
                x Int,
                y Int
            )
            ENGINE = MergeTree
            ORDER BY (k1, k2)
            TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
            """,
            write={
                "clickhouse": """CREATE TABLE table_for_aggregation (
  d DateTime,
  k1 Int32,
  k2 Int32,
  x Int32,
  y Int32
)
ENGINE=MergeTree
ORDER BY (k1, k2)
TTL
  d + INTERVAL '1' MONTH
GROUP BY
  k1,
  k2
SET
  x = max(x),
  y = min(y)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE DICTIONARY discounts_dict (
                advertiser_id UInt64,
                discount_start_date Date,
                discount_end_date Date,
                amount Float64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'discounts'))
            LIFETIME(MIN 1 MAX 1000)
            LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
            RANGE(MIN discount_start_date MAX discount_end_date)
            """,
            write={
                "clickhouse": """CREATE DICTIONARY discounts_dict (
  advertiser_id UInt64,
  discount_start_date DATE,
  discount_end_date DATE,
  amount Float64
)
PRIMARY KEY (id)
SOURCE(CLICKHOUSE(
  TABLE 'discounts'
))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED(
  range_lookup_strategy 'max'
))
RANGE(MIN discount_start_date MAX discount_end_date)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE DICTIONARY my_ip_trie_dictionary (
                prefix String,
                asn UInt32,
                cca2 String DEFAULT '??'
            )
            PRIMARY KEY prefix
            SOURCE(CLICKHOUSE(TABLE 'my_ip_addresses'))
            LAYOUT(IP_TRIE)
            LIFETIME(3600);
            """,
            write={
                "clickhouse": """CREATE DICTIONARY my_ip_trie_dictionary (
  prefix String,
  asn UInt32,
  cca2 String DEFAULT '??'
)
PRIMARY KEY (prefix)
SOURCE(CLICKHOUSE(
  TABLE 'my_ip_addresses'
))
LAYOUT(IP_TRIE())
LIFETIME(MIN 0 MAX 3600)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE DICTIONARY polygons_test_dictionary
            (
                key Array(Array(Array(Tuple(Float64, Float64)))),
                name String
            )
            PRIMARY KEY key
            SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
            LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
            LIFETIME(0);
            """,
            write={
                "clickhouse": """CREATE DICTIONARY polygons_test_dictionary (
  key Array(Array(Array(Tuple(Float64, Float64)))),
  name String
)
PRIMARY KEY (key)
SOURCE(CLICKHOUSE(
  TABLE 'polygons_test_table'
))
LAYOUT(POLYGON(
  STORE_POLYGON_KEY_COLUMN 1
))
LIFETIME(MIN 0 MAX 0)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE t (
                a AggregateFunction(quantiles(0.5, 0.9), UInt64),
                b AggregateFunction(quantiles, UInt64),
                c SimpleAggregateFunction(sum, Float64),
                d AggregateFunction(count)
            )""",
            write={
                "clickhouse": """CREATE TABLE t (
  a AggregateFunction(quantiles(0.5, 0.9), UInt64),
  b AggregateFunction(quantiles, UInt64),
  c SimpleAggregateFunction(sum, Float64),
  d AggregateFunction(count)
)"""
            },
            pretty=True,
        )

        self.assertIsNotNone(
            self.validate_identity("CREATE TABLE t1 (a String MATERIALIZED func())").find(
                exp.ColumnConstraint
            )
        )

    def test_agg_functions(self):
        def extract_agg_func(query):
            return parse_one(query, read="clickhouse").selects[0].this

        self.assertIsInstance(
            extract_agg_func("select quantileGK(100, 0.95) OVER (PARTITION BY id) FROM table"),
            exp.AnonymousAggFunc,
        )
        self.assertIsInstance(
            extract_agg_func(
                "select quantileGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table"
            ),
            exp.ParameterizedAgg,
        )
        self.assertIsInstance(
            extract_agg_func("select quantileGKIf(100, 0.95) OVER (PARTITION BY id) FROM table"),
            exp.CombinedAggFunc,
        )
        self.assertIsInstance(
            extract_agg_func(
                "select quantileGKIf(100, 0.95)(reading) OVER (PARTITION BY id) FROM table"
            ),
            exp.CombinedParameterizedAgg,
        )

        parse_one("foobar(x)").assert_is(exp.Anonymous)

    def test_drop_on_cluster(self):
        for creatable in ("DATABASE", "TABLE", "VIEW", "DICTIONARY", "FUNCTION"):
            with self.subTest(f"Test DROP {creatable} ON CLUSTER"):
                self.validate_identity(f"DROP {creatable} test ON CLUSTER test_cluster")

    def test_datetime_funcs(self):
        # Each datetime func has an alias that is roundtripped to the original name e.g. (DATE_SUB, DATESUB) -> DATE_SUB
        datetime_funcs = (("DATE_SUB", "DATESUB"), ("DATE_ADD", "DATEADD"))

        # 2-arg functions of type <func>(date, unit)
        for func in (*datetime_funcs, ("TIMESTAMP_ADD", "TIMESTAMPADD")):
            func_name = func[0]
            for func_alias in func:
                self.validate_identity(
                    f"""SELECT {func_alias}(date, INTERVAL '3' YEAR)""",
                    f"""SELECT {func_name}(date, INTERVAL '3' YEAR)""",
                )

        # 3-arg functions of type <func>(unit, value, date)
        for func in (*datetime_funcs, ("DATE_DIFF", "DATEDIFF"), ("TIMESTAMP_SUB", "TIMESTAMPSUB")):
            func_name = func[0]
            for func_alias in func:
                with self.subTest(f"Test 3-arg date-time function {func_alias}"):
                    self.validate_identity(
                        f"SELECT {func_alias}(SECOND, 1, bar)",
                        f"SELECT {func_name}(SECOND, 1, bar)",
                    )

    def test_convert(self):
        self.assertEqual(
            convert(date(2020, 1, 1)).sql(dialect=self.dialect), "toDate('2020-01-01')"
        )

        # no fractional seconds
        self.assertEqual(
            convert(datetime(2020, 1, 1, 0, 0, 1)).sql(dialect=self.dialect),
            "CAST('2020-01-01 00:00:01' AS DateTime64(6))",
        )
        self.assertEqual(
            convert(datetime(2020, 1, 1, 0, 0, 1, tzinfo=timezone.utc)).sql(dialect=self.dialect),
            "CAST('2020-01-01 00:00:01' AS DateTime64(6, 'UTC'))",
        )

        # with fractional seconds
        self.assertEqual(
            convert(datetime(2020, 1, 1, 0, 0, 1, 1)).sql(dialect=self.dialect),
            "CAST('2020-01-01 00:00:01.000001' AS DateTime64(6))",
        )
        self.assertEqual(
            convert(datetime(2020, 1, 1, 0, 0, 1, 1, tzinfo=timezone.utc)).sql(
                dialect=self.dialect
            ),
            "CAST('2020-01-01 00:00:01.000001' AS DateTime64(6, 'UTC'))",
        )

    def test_timestr_to_time(self):
        # no fractional seconds
        time_strings = [
            "2020-01-01 00:00:01",
            "2020-01-01 00:00:01+01:00",
            " 2020-01-01 00:00:01-01:00 ",
            "2020-01-01T00:00:01+01:00",
        ]
        for time_string in time_strings:
            with self.subTest(f"'{time_string}'"):
                self.assertEqual(
                    exp.TimeStrToTime(this=exp.Literal.string(time_string)).sql(
                        dialect=self.dialect
                    ),
                    f"CAST('{time_string}' AS DateTime64(6))",
                )

        time_strings_no_utc = ["2020-01-01 00:00:01" for i in range(4)]
        for utc, no_utc in zip(time_strings, time_strings_no_utc):
            with self.subTest(f"'{time_string}' with UTC timezone"):
                self.assertEqual(
                    exp.TimeStrToTime(
                        this=exp.Literal.string(utc), zone=exp.Literal.string("UTC")
                    ).sql(dialect=self.dialect),
                    f"CAST('{no_utc}' AS DateTime64(6, 'UTC'))",
                )

        # with fractional seconds
        time_strings = [
            "2020-01-01 00:00:01.001",
            "2020-01-01 00:00:01.000001",
            "2020-01-01 00:00:01.001+00:00",
            "2020-01-01 00:00:01.000001-00:00",
            "2020-01-01 00:00:01.0001",
            "2020-01-01 00:00:01.1+00:00",
        ]

        for time_string in time_strings:
            with self.subTest(f"'{time_string}'"):
                self.assertEqual(
                    exp.TimeStrToTime(this=exp.Literal.string(time_string[0])).sql(
                        dialect=self.dialect
                    ),
                    f"CAST('{time_string[0]}' AS DateTime64(6))",
                )

        time_strings_no_utc = [
            "2020-01-01 00:00:01.001000",
            "2020-01-01 00:00:01.000001",
            "2020-01-01 00:00:01.001000",
            "2020-01-01 00:00:01.000001",
            "2020-01-01 00:00:01.000100",
            "2020-01-01 00:00:01.100000",
        ]

        for utc, no_utc in zip(time_strings, time_strings_no_utc):
            with self.subTest(f"'{time_string}' with UTC timezone"):
                self.assertEqual(
                    exp.TimeStrToTime(
                        this=exp.Literal.string(utc), zone=exp.Literal.string("UTC")
                    ).sql(dialect=self.dialect),
                    f"CAST('{no_utc}' AS DateTime64(6, 'UTC'))",
                )

    def test_grant(self):
        self.validate_identity("GRANT SELECT(x, y) ON db.table TO john WITH GRANT OPTION")
        self.validate_identity("GRANT INSERT(x, y) ON db.table TO john")

    def test_array_join(self):
        expr = self.validate_identity(
            "SELECT * FROM arrays_test ARRAY JOIN arr1, arrays_test.arr2 AS foo, ['a', 'b', 'c'] AS elem"
        )
        joins = expr.args["joins"]
        self.assertEqual(len(joins), 1)

        join = joins[0]
        self.assertEqual(join.kind, "ARRAY")
        self.assertIsInstance(join.this, exp.Column)

        self.assertEqual(len(join.expressions), 2)
        self.assertIsInstance(join.expressions[0], exp.Alias)
        self.assertIsInstance(join.expressions[0].this, exp.Column)

        self.assertIsInstance(join.expressions[1], exp.Alias)
        self.assertIsInstance(join.expressions[1].this, exp.Array)

        self.validate_identity("SELECT s, arr FROM arrays_test ARRAY JOIN arr")
        self.validate_identity("SELECT s, arr, a FROM arrays_test LEFT ARRAY JOIN arr AS a")
        self.validate_identity(
            "SELECT s, arr_external FROM arrays_test ARRAY JOIN [1, 2, 3] AS arr_external"
        )
        self.validate_identity(
            "SELECT * FROM arrays_test ARRAY JOIN [1, 2, 3] AS arr_external1, ['a', 'b', 'c'] AS arr_external2, splitByString(',', 'asd,qwerty,zxc') AS arr_external3"
        )

    def test_traverse_scope(self):
        sql = "SELECT * FROM t FINAL"
        scopes = traverse_scope(parse_one(sql, dialect=self.dialect))
        self.assertEqual(len(scopes), 1)
        self.assertEqual(set(scopes[0].sources), {"t"})

    def test_window_functions(self):
        self.validate_identity(
            "SELECT row_number(column1) OVER (PARTITION BY column2 ORDER BY column3) FROM table"
        )
        self.validate_identity(
            "SELECT row_number() OVER (PARTITION BY column2 ORDER BY column3) FROM table"
        )

    def test_functions(self):
        self.validate_identity("SELECT TRANSFORM(foo, [1, 2], ['first', 'second']) FROM table")
        self.validate_identity(
            "SELECT TRANSFORM(foo, [1, 2], ['first', 'second'], 'default') FROM table"
        )
