from unittest import mock

from sqlglot import ParseError, UnsupportedError, exp, parse_one
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    maxDiff = None
    dialect = "snowflake"

    def test_snowflake(self):
        self.validate_identity(
            "SELECT * FROM x ASOF JOIN y OFFSET MATCH_CONDITION (x.a > y.a)",
            "SELECT * FROM x ASOF JOIN y AS OFFSET MATCH_CONDITION (x.a > y.a)",
        )
        self.validate_identity(
            "SELECT * FROM x ASOF JOIN y LIMIT MATCH_CONDITION (x.a > y.a)",
            "SELECT * FROM x ASOF JOIN y AS LIMIT MATCH_CONDITION (x.a > y.a)",
        )

        self.validate_identity("SELECT session")
        self.validate_identity("x::nvarchar()", "CAST(x AS VARCHAR)")

        ast = self.parse_one("DATEADD(DAY, n, d)")
        ast.set("unit", exp.Literal.string("MONTH"))
        self.assertEqual(ast.sql("snowflake"), "DATEADD(MONTH, n, d)")

        self.validate_identity("SELECT DATE_PART(EPOCH_MILLISECOND, CURRENT_TIMESTAMP()) AS a")
        self.validate_identity("SELECT GET(a, b)")
        self.validate_identity("SELECT HASH_AGG(a, b, c, d)")
        self.validate_identity("SELECT GREATEST(1, 2, 3, NULL)")
        self.validate_identity("SELECT GREATEST_IGNORE_NULLS(1, 2, 3, NULL)")
        self.validate_identity("SELECT LEAST(5, NULL, 7, 3)")
        self.validate_identity("SELECT LEAST_IGNORE_NULLS(5, NULL, 7, 3)")
        self.validate_identity("SELECT MAX(x)")
        self.validate_identity("SELECT COUNT(x)")
        self.validate_identity("SELECT MIN(amount)")
        self.validate_identity("SELECT MODE(x)")
        self.validate_identity("SELECT MODE(status) OVER (PARTITION BY region) FROM orders")
        self.validate_identity("SELECT TAN(x)")
        self.validate_identity("SELECT COS(x)")
        self.validate_identity("SELECT SINH(1.5)")
        self.validate_identity("SELECT MOD(x, y)", "SELECT x % y")
        self.validate_identity("SELECT ROUND(x)")
        self.validate_identity("SELECT ROUND(123.456, -1)")
        self.validate_identity("SELECT ROUND(123.456, 2, 'HALF_AWAY_FROM_ZERO')")

        self.validate_identity("SELECT FLOOR(x)")
        self.validate_identity("SELECT FLOOR(135.135, 1)")
        self.validate_identity("SELECT FLOOR(x, -1)")
        self.validate_identity(
            "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) FROM employees"
        )
        self.assertEqual(
            # Ensures we don't fail when generating ParseJSON with the `safe` arg set to `True`
            self.validate_identity("""SELECT TRY_PARSE_JSON('{"x: 1}')""").sql(),
            """SELECT PARSE_JSON('{"x: 1}')""",
        )

        self.validate_identity(
            "SELECT APPROX_TOP_K(col) FROM t",
            "SELECT APPROX_TOP_K(col, 1) FROM t",
        )
        self.validate_identity("SELECT APPROX_TOP_K(category, 3) FROM t")
        self.validate_identity("APPROX_TOP_K(C4, 3, 5)").assert_is(exp.AggFunc)

        self.validate_identity("SELECT MINHASH(5, col)")
        self.validate_identity("SELECT MINHASH(5, col1, col2)")
        self.validate_identity("SELECT MINHASH(5, *)")
        self.validate_identity("SELECT MINHASH_COMBINE(minhash_col)")
        self.validate_identity("SELECT APPROXIMATE_SIMILARITY(minhash_col)")
        self.validate_identity(
            "SELECT APPROXIMATE_JACCARD_INDEX(minhash_col)",
            "SELECT APPROXIMATE_SIMILARITY(minhash_col)",
        )
        self.validate_identity("SELECT APPROX_PERCENTILE_ACCUMULATE(col)")
        self.validate_identity("SELECT APPROX_PERCENTILE_ESTIMATE(state, 0.5)")
        self.validate_identity("SELECT APPROX_TOP_K_ACCUMULATE(col, 10)")
        self.validate_identity("SELECT APPROX_TOP_K_COMBINE(state, 2)")
        self.validate_identity("SELECT APPROX_TOP_K_COMBINE(state)")
        self.validate_identity("SELECT APPROX_TOP_K_ESTIMATE(state_column, 4)")
        self.validate_identity("SELECT APPROX_TOP_K_ESTIMATE(state_column)")
        self.validate_identity("SELECT APPROX_PERCENTILE_COMBINE(state_column)")
        self.validate_identity("SELECT EQUAL_NULL(1, 2)")
        self.validate_identity("SELECT EXP(1)")
        self.validate_identity("SELECT FACTORIAL(5)")
        self.validate_identity("SELECT BIT_LENGTH('abc')")
        self.validate_identity("SELECT BIT_LENGTH(x'A1B2')")
        self.validate_all(
            "SELECT BITMAP_BIT_POSITION(10)",
            write={
                "duckdb": "SELECT (CASE WHEN 10 > 0 THEN 10 - 1 ELSE ABS(10) END) % 32768",
                "snowflake": "SELECT BITMAP_BIT_POSITION(10)",
            },
        )
        self.validate_identity("SELECT BITMAP_BUCKET_NUMBER(32769)")
        self.validate_identity("SELECT BITMAP_CONSTRUCT_AGG(value)")
        self.validate_all(
            "SELECT BITMAP_CONSTRUCT_AGG(v) FROM t",
            write={
                "snowflake": "SELECT BITMAP_CONSTRUCT_AGG(v) FROM t",
                "duckdb": "SELECT (SELECT CASE WHEN l IS NULL OR LENGTH(l) = 0 THEN NULL WHEN LENGTH(l) <> LENGTH(LIST_FILTER(l, __v -> __v BETWEEN 0 AND 32767)) THEN NULL WHEN LENGTH(l) < 5 THEN UNHEX(PRINTF('%04X', LENGTH(l)) || h || REPEAT('00', GREATEST(0, 4 - LENGTH(l)) * 2)) ELSE UNHEX('08000000000000000000' || h) END FROM (SELECT l, COALESCE(LIST_REDUCE(LIST_TRANSFORM(l, __x -> PRINTF('%02X%02X', CAST(__x AS INT) & 255, (CAST(__x AS INT) >> 8) & 255)), (__a, __b) -> __a || __b, ''), '') AS h FROM (SELECT LIST_SORT(LIST_DISTINCT(LIST(v) FILTER(WHERE NOT v IS NULL))) AS l))) FROM t",
            },
        )
        self.validate_identity(
            "SELECT BITMAP_COUNT(BITMAP_CONSTRUCT_AGG(value)) FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3, 5)))",
            "SELECT BITMAP_COUNT(BITMAP_CONSTRUCT_AGG(value)) FROM TABLE(FLATTEN(INPUT => [1, 2, 3, 5]))",
        )
        self.validate_identity("SELECT BOOLAND(1, -2)")
        self.validate_identity("SELECT BOOLXOR(2, 0)")
        self.validate_identity("SELECT BOOLOR(1, 0)")
        self.validate_identity("SELECT TO_BOOLEAN('true')")
        self.validate_identity("SELECT TO_BOOLEAN(1)")
        self.validate_identity("SELECT IS_NULL_VALUE(GET_PATH(payload, 'field'))")
        self.validate_identity("SELECT RTRIMMED_LENGTH(' ABCD ')")
        self.validate_identity("SELECT HEX_DECODE_STRING('48656C6C6F')")
        self.validate_identity("SELECT HEX_ENCODE('Hello World')")
        self.validate_identity("SELECT HEX_ENCODE('Hello World', 1)")
        self.validate_identity("SELECT HEX_ENCODE('Hello World', 0)")
        self.validate_identity("SELECT IFNULL(col1, col2)", "SELECT COALESCE(col1, col2)")
        self.validate_identity("SELECT NEXT_DAY('2025-10-15', 'FRIDAY')")
        self.validate_identity("SELECT NVL2(col1, col2, col3)")
        self.validate_identity("SELECT NVL(col1, col2)", "SELECT COALESCE(col1, col2)")
        self.validate_identity("SELECT CHR(8364)")
        self.validate_identity('SELECT CHECK_JSON(\'{"key": "value"}\')')
        self.validate_identity(
            "SELECT CHECK_XML('<root><key attribute=\"attr\">value</key></root>')"
        )
        self.validate_identity(
            "SELECT CHECK_XML('<root><key attribute=\"attr\">value</key></root>', TRUE)"
        )
        self.validate_identity("SELECT COMPRESS('Hello World', 'ZLIB')")
        self.validate_identity("SELECT DECOMPRESS_BINARY('compressed_data', 'SNAPPY')")
        self.validate_identity("SELECT DECOMPRESS_STRING('compressed_data', 'ZSTD')")
        self.validate_identity("SELECT LPAD('Hello', 10, '*')")
        self.validate_identity("SELECT LPAD(tbl.bin_col, 10)")
        self.validate_identity("SELECT RPAD('Hello', 10, '*')")
        self.validate_identity("SELECT RPAD(tbl.bin_col, 10)")
        self.validate_identity("SELECT SOUNDEX(column_name)")
        self.validate_identity("SELECT SOUNDEX_P123(column_name)")
        self.validate_identity("SELECT ABS(x)")
        self.validate_identity("SELECT ASIN(0.5)")
        self.validate_identity("SELECT ASINH(0.5)")
        self.validate_identity("SELECT ATAN(0.5)")
        self.validate_identity("SELECT ATAN2(0.5, 0.3)")
        self.validate_identity("SELECT ATANH(0.5)")
        self.validate_identity("SELECT CBRT(27.0)")
        self.validate_identity("SELECT POW(2, 3)", "SELECT POWER(2, 3)")
        self.validate_identity("SELECT POW(2.5, 3.0)", "SELECT POWER(2.5, 3.0)")
        self.validate_identity("SELECT SQUARE(2.5)", "SELECT POWER(2.5, 2)")
        self.validate_identity("SELECT SIGN(x)")
        self.validate_identity("SELECT COSH(1.5)")
        self.validate_identity("SELECT TANH(0.5)")
        self.validate_identity("SELECT JAROWINKLER_SIMILARITY('hello', 'world')")
        self.validate_identity("SELECT TRANSLATE(column_name, 'abc', '123')")
        self.validate_identity("SELECT UNICODE(column_name)")
        self.validate_identity("SELECT WIDTH_BUCKET(col, 0, 100, 10)")
        self.validate_identity("SELECT SPLIT_PART('11.22.33', '.', 1)")
        self.validate_identity("SELECT PI()")
        self.validate_identity("SELECT DEGREES(PI() / 3)")
        self.validate_identity("SELECT DEGREES(1)")
        self.validate_identity("SELECT RADIANS(180)")
        self.validate_all(
            "SELECT REGR_VALX(y, x)",
            write={
                "snowflake": "SELECT REGR_VALX(y, x)",
                "duckdb": "SELECT CASE WHEN y IS NULL THEN CAST(NULL AS DOUBLE) ELSE x END",
            },
        )
        self.validate_all(
            "SELECT REGR_VALY(y, x)",
            write={
                "snowflake": "SELECT REGR_VALY(y, x)",
                "duckdb": "SELECT CASE WHEN x IS NULL THEN CAST(NULL AS DOUBLE) ELSE y END",
            },
        )
        self.validate_identity("SELECT REGR_AVGX(y, x)")
        self.validate_identity("SELECT REGR_AVGY(y, x)")
        self.validate_identity("SELECT REGR_COUNT(y, x)")
        self.validate_identity("SELECT REGR_INTERCEPT(y, x)")
        self.validate_identity("SELECT REGR_R2(y, x)")
        self.validate_identity("SELECT REGR_SXX(y, x)")
        self.validate_identity("SELECT REGR_SXY(y, x)")
        self.validate_identity("SELECT REGR_SYY(y, x)")
        self.validate_identity("SELECT REGR_SLOPE(y, x)")
        self.validate_all(
            "SELECT IFF(x > 5, 10, 20)",
            write={
                "snowflake": "SELECT IFF(x > 5, 10, 20)",
                "duckdb": "SELECT CASE WHEN x > 5 THEN 10 ELSE 20 END",
            },
        )
        self.validate_all(
            "SELECT IFF(col IS NULL, 0, col)",
            write={
                "snowflake": "SELECT IFF(col IS NULL, 0, col)",
                "duckdb": "SELECT CASE WHEN col IS NULL THEN 0 ELSE col END",
            },
        )
        self.validate_all(
            "SELECT VAR_SAMP(x)",
            write={
                "snowflake": "SELECT VARIANCE(x)",
                "duckdb": "SELECT VARIANCE(x)",
                "postgres": "SELECT VAR_SAMP(x)",
            },
        )
        self.validate_all(
            "SELECT GREATEST(1, 2)",
            write={
                "snowflake": "SELECT GREATEST(1, 2)",
                "duckdb": "SELECT CASE WHEN 1 IS NULL OR 2 IS NULL THEN NULL ELSE GREATEST(1, 2) END",
            },
        )
        self.validate_all(
            "SELECT GREATEST_IGNORE_NULLS(1, 2)",
            write={
                "snowflake": "SELECT GREATEST_IGNORE_NULLS(1, 2)",
                "duckdb": "SELECT GREATEST(1, 2)",
            },
        )
        self.validate_all(
            "SELECT LEAST(1, 2)",
            write={
                "snowflake": "SELECT LEAST(1, 2)",
                "duckdb": "SELECT CASE WHEN 1 IS NULL OR 2 IS NULL THEN NULL ELSE LEAST(1, 2) END",
            },
        )
        self.validate_all(
            "SELECT LEAST_IGNORE_NULLS(1, 2)",
            write={
                "snowflake": "SELECT LEAST_IGNORE_NULLS(1, 2)",
                "duckdb": "SELECT LEAST(1, 2)",
            },
        )
        self.validate_all(
            "SELECT VAR_POP(x)",
            write={
                "snowflake": "SELECT VARIANCE_POP(x)",
                "duckdb": "SELECT VAR_POP(x)",
                "postgres": "SELECT VAR_POP(x)",
            },
        )
        self.validate_all(
            "SELECT SKEW(a)",
            write={
                "snowflake": "SELECT SKEW(a)",
                "duckdb": "SELECT SKEWNESS(a)",
                "spark": "SELECT SKEWNESS(a)",
                "trino": "SELECT SKEWNESS(a)",
            },
            read={
                "duckdb": "SELECT SKEWNESS(a)",
                "spark": "SELECT SKEWNESS(a)",
                "trino": "SELECT SKEWNESS(a)",
            },
        )
        self.validate_identity("SELECT RANDOM()")
        self.validate_identity("SELECT RANDOM(123)")
        self.validate_identity("SELECT RANDSTR(123, 456)")
        self.validate_identity("SELECT RANDSTR(123, RANDOM())")
        self.validate_identity("SELECT NORMAL(0, 1, RANDOM())")

        self.validate_all(
            "IS_NULL_VALUE(x)",
            write={
                "duckdb": "JSON_TYPE(x) = 'NULL'",
                "snowflake": "IS_NULL_VALUE(x)",
            },
        )
        # Test RANDSTR transpilation to DuckDB
        self.validate_all(
            "SELECT RANDSTR(10, 123)",
            write={
                "snowflake": "SELECT RANDSTR(10, 123)",
                "duckdb": "SELECT (SELECT LISTAGG(SUBSTRING('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), '') FROM (SELECT (ABS(HASH(i + 123)) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))",
            },
        )
        self.validate_all(
            "SELECT RANDSTR(10, RANDOM(123))",
            write={
                "snowflake": "SELECT RANDSTR(10, RANDOM(123))",
                "duckdb": "SELECT (SELECT LISTAGG(SUBSTRING('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), '') FROM (SELECT (ABS(HASH(i + 123)) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))",
            },
        )
        self.validate_all(
            "SELECT RANDSTR(10, RANDOM())",
            write={
                "snowflake": "SELECT RANDSTR(10, RANDOM())",
                "duckdb": "SELECT (SELECT LISTAGG(SUBSTRING('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), '') FROM (SELECT (ABS(HASH(i + RANDOM())) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))",
            },
        )

        self.validate_all(
            "SELECT BOOLNOT(0)",
            write={
                "snowflake": "SELECT BOOLNOT(0)",
                "duckdb": "SELECT NOT (ROUND(0, 0))",
            },
        )

        self.validate_all(
            "SELECT ZIPF(1, 10, 1234)",
            write={
                "duckdb": "SELECT (WITH rand AS (SELECT (ABS(HASH(1234)) % 1000000) / 1000000.0 AS r), weights AS (SELECT i, 1.0 / POWER(i, 1) AS w FROM RANGE(1, 10 + 1) AS t(i)), cdf AS (SELECT i, SUM(w) OVER (ORDER BY i NULLS FIRST) / SUM(w) OVER () AS p FROM weights) SELECT MIN(i) FROM cdf WHERE p >= (SELECT r FROM rand))",
                "snowflake": "SELECT ZIPF(1, 10, 1234)",
            },
        )

        self.validate_all(
            "SELECT ZIPF(2, 100, RANDOM())",
            write={
                "duckdb": "SELECT (WITH rand AS (SELECT RANDOM() AS r), weights AS (SELECT i, 1.0 / POWER(i, 2) AS w FROM RANGE(1, 100 + 1) AS t(i)), cdf AS (SELECT i, SUM(w) OVER (ORDER BY i NULLS FIRST) / SUM(w) OVER () AS p FROM weights) SELECT MIN(i) FROM cdf WHERE p >= (SELECT r FROM rand))",
                "snowflake": "SELECT ZIPF(2, 100, RANDOM())",
            },
        )

        self.validate_identity("SELECT GROUPING_ID(a, b) AS g_id FROM x GROUP BY ROLLUP (a, b)")
        self.validate_identity("PARSE_URL('https://example.com/path')")
        self.validate_identity("PARSE_URL('https://example.com/path', 1)")
        self.validate_identity("SELECT XMLGET(object_col, 'level2')")
        self.validate_identity("SELECT XMLGET(object_col, 'level3', 1)")
        self.validate_identity("SELECT {*} FROM my_table")
        self.validate_identity("SELECT {my_table.*} FROM my_table")
        self.validate_identity("SELECT {* ILIKE 'col1%'} FROM my_table")
        self.validate_identity("SELECT {* EXCLUDE (col1)} FROM my_table")
        self.validate_identity("SELECT {* EXCLUDE (col1, col2)} FROM my_table")
        self.validate_identity("SELECT a, b, COUNT(*) FROM x GROUP BY ALL LIMIT 100")
        self.validate_identity("STRTOK_TO_ARRAY('a b c')")
        self.validate_identity("STRTOK_TO_ARRAY('a.b.c', '.')")
        self.validate_identity("GET(a, b)")
        self.validate_identity("INSERT INTO test VALUES (x'48FAF43B0AFCEF9B63EE3A93EE2AC2')")
        self.validate_identity("SELECT STAR(tbl, exclude := [foo])")
        self.validate_identity("SELECT CAST([1, 2, 3] AS VECTOR(FLOAT, 3))")
        self.validate_identity("SELECT VECTOR_COSINE_SIMILARITY(a, b)")
        self.validate_identity("SELECT VECTOR_INNER_PRODUCT(a, b)")
        self.validate_identity("SELECT VECTOR_L1_DISTANCE(a, b)")
        self.validate_identity("SELECT VECTOR_L2_DISTANCE(a, b)")
        self.validate_identity("SELECT CONNECT_BY_ROOT test AS test_column_alias")
        self.validate_identity("SELECT number").selects[0].assert_is(exp.Column)
        self.validate_identity("INTERVAL '4 years, 5 months, 3 hours'")
        self.validate_identity("ALTER TABLE table1 CLUSTER BY (name DESC)")
        self.validate_identity("SELECT rename, replace")
        self.validate_identity("SELECT TIMEADD(HOUR, 2, CAST('09:05:03' AS TIME))")
        self.validate_identity("SELECT CAST(OBJECT_CONSTRUCT('a', 1) AS MAP(VARCHAR, INT))")
        self.validate_identity(
            "SELECT MAP_CAT(CAST(col AS MAP(VARCHAR, VARCHAR)), CAST(col AS MAP(VARCHAR, VARCHAR)))"
        )
        self.validate_identity("SELECT MAP_CONTAINS_KEY('k1', CAST(col AS MAP(VARCHAR, VARCHAR)))")
        self.validate_identity("SELECT MAP_DELETE(CAST(col AS MAP(VARCHAR, VARCHAR)), 'k1')")
        self.validate_identity("SELECT MAP_INSERT(CAST(col AS MAP(VARCHAR, VARCHAR)), 'b', '2')")
        self.validate_identity("SELECT MAP_KEYS(CAST(col AS MAP(VARCHAR, VARCHAR)))")
        self.validate_identity("SELECT MAP_PICK(CAST(col AS MAP(VARCHAR, VARCHAR)), 'a', 'c')")
        self.validate_identity("SELECT MAP_SIZE(CAST(col AS MAP(VARCHAR, VARCHAR)))")
        self.validate_identity("SELECT CAST(OBJECT_CONSTRUCT('a', 1) AS OBJECT(a CHAR NOT NULL))")
        self.validate_identity("SELECT CAST([1, 2, 3] AS ARRAY(INT))")
        self.validate_identity("SELECT CAST(obj AS OBJECT(x CHAR) RENAME FIELDS)")
        self.validate_identity("SELECT CAST(obj AS OBJECT(x CHAR, y VARCHAR) ADD FIELDS)")
        self.validate_identity("SELECT TO_TIMESTAMP(123.4)").selects[0].assert_is(exp.Anonymous)
        self.validate_identity("SELECT TO_TIMESTAMP(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_NTZ(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_LTZ(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_TZ(x) FROM t")
        self.validate_identity("TO_DECIMAL(expr)", "TO_NUMBER(expr)")
        self.validate_identity("TO_DECIMAL(expr, fmt)", "TO_NUMBER(expr, fmt)")
        self.validate_identity(
            "TO_DECIMAL(expr, fmt, precision, scale)", "TO_NUMBER(expr, fmt, precision, scale)"
        )
        self.validate_identity("TO_NUMBER(expr)")
        self.validate_identity("TO_NUMBER(expr, fmt)")
        self.validate_identity("TO_NUMBER(expr, fmt, precision, scale)")
        self.validate_identity("TO_DECFLOAT('123.456')")
        self.validate_identity("TO_DECFLOAT('1,234.56', '999,999.99')")
        self.validate_identity("TRY_TO_DECFLOAT('123.456')")
        self.validate_identity("TRY_TO_DECFLOAT('1,234.56', '999,999.99')")
        self.validate_all(
            "TRY_TO_BOOLEAN('true')",
            write={
                "snowflake": "TRY_TO_BOOLEAN('true')",
                "duckdb": "CASE WHEN UPPER(CAST('true' AS TEXT)) = 'ON' THEN TRUE WHEN UPPER(CAST('true' AS TEXT)) = 'OFF' THEN FALSE ELSE TRY_CAST('true' AS BOOLEAN) END",
            },
        )

        self.validate_identity("TRY_TO_DECIMAL('123.45')", "TRY_TO_NUMBER('123.45')")
        self.validate_identity(
            "TRY_TO_DECIMAL('123.45', '999.99')", "TRY_TO_NUMBER('123.45', '999.99')"
        )
        self.validate_identity(
            "TRY_TO_DECIMAL('123.45', '999.99', 10, 2)", "TRY_TO_NUMBER('123.45', '999.99', 10, 2)"
        )
        self.validate_all(
            "TRY_TO_DOUBLE('123.456')",
            write={
                "snowflake": "TRY_TO_DOUBLE('123.456')",
                "duckdb": "TRY_CAST('123.456' AS DOUBLE)",
            },
        )
        self.validate_identity("TRY_TO_DOUBLE('123.456', '999.99')")
        self.validate_all(
            "TRY_TO_DOUBLE('-4.56E-03', 'S9.99EEEE')",
            write={
                "snowflake": "TRY_TO_DOUBLE('-4.56E-03', 'S9.99EEEE')",
                "duckdb": UnsupportedError,
            },
        )
        self.validate_identity("TO_FILE(object_col)")
        self.validate_identity("TO_FILE('file.csv')")
        self.validate_identity("TO_FILE('file.csv', 'relativepath/')")
        self.validate_identity("TRY_TO_FILE(object_col)")
        self.validate_identity("TRY_TO_FILE('file.csv')")
        self.validate_identity("TRY_TO_FILE('file.csv', 'relativepath/')")
        self.validate_identity("TRY_TO_NUMBER('123.45')")
        self.validate_identity("TRY_TO_NUMBER('123.45', '999.99')")
        self.validate_identity("TRY_TO_NUMBER('123.45', '999.99', 10, 2)")
        self.validate_identity("TO_NUMERIC('123.45')", "TO_NUMBER('123.45')")
        self.validate_identity("TO_NUMERIC('123.45', '999.99')", "TO_NUMBER('123.45', '999.99')")
        self.validate_identity(
            "TO_NUMERIC('123.45', '999.99', 10, 2)", "TO_NUMBER('123.45', '999.99', 10, 2)"
        )
        self.validate_identity("TRY_TO_NUMERIC('123.45')", "TRY_TO_NUMBER('123.45')")
        self.validate_identity(
            "TRY_TO_NUMERIC('123.45', '999.99')", "TRY_TO_NUMBER('123.45', '999.99')"
        )
        self.validate_identity(
            "TRY_TO_NUMERIC('123.45', '999.99', 10, 2)", "TRY_TO_NUMBER('123.45', '999.99', 10, 2)"
        )
        self.validate_all(
            "TRY_TO_TIME('12:30:00')",
            write={
                "snowflake": "TRY_CAST('12:30:00' AS TIME)",
                "duckdb": "TRY_CAST('12:30:00' AS TIME)",
            },
        )
        self.validate_identity("TRY_TO_TIME('12:30:00', 'AUTO')")
        self.validate_all(
            "TRY_TO_TIMESTAMP('2024-01-15 12:30:00')",
            write={
                "snowflake": "TRY_CAST('2024-01-15 12:30:00' AS TIMESTAMP)",
                "duckdb": "TRY_CAST('2024-01-15 12:30:00' AS TIMESTAMP)",
            },
        )
        self.validate_identity("TRY_TO_TIMESTAMP('2024-01-15 12:30:00', 'AUTO')")
        self.validate_identity("ALTER TABLE authors ADD CONSTRAINT c1 UNIQUE (id, email)")
        self.validate_identity("RM @parquet_stage", check_command_warning=True)
        self.validate_identity("REMOVE @parquet_stage", check_command_warning=True)
        self.validate_identity("SELECT TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45)")
        self.validate_identity("SELECT TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123)")
        self.validate_identity("SELECT TIMESTAMP_LTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)")
        self.validate_identity("SELECT TIMESTAMP_TZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)")
        self.validate_identity(
            "SELECT TIMESTAMP_TZ_FROM_PARTS(2013, 4, 5, 12, 00, 00, 0, 'America/Los_Angeles')"
        )
        self.validate_identity(
            "SELECT TIMESTAMP_FROM_PARTS(CAST('2024-05-09' AS DATE), CAST('14:30:45' AS TIME))"
        )
        self.validate_identity(
            "SELECT TIMESTAMP_NTZ_FROM_PARTS(TO_DATE('2013-04-05'), TO_TIME('12:00:00'))",
            "SELECT TIMESTAMP_FROM_PARTS(CAST('2013-04-05' AS DATE), CAST('12:00:00' AS TIME))",
        )
        self.validate_identity(
            "SELECT TIMESTAMP_NTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)",
            "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)",
        )

        self.validate_identity("SELECT DATE_FROM_PARTS(1977, 8, 7)")
        self.validate_identity("SELECT GET_PATH(v, 'attr[0].name') FROM vartab")
        self.validate_identity("SELECT TO_ARRAY(CAST(x AS ARRAY))")
        self.validate_identity("SELECT TO_ARRAY(CAST(['test'] AS VARIANT))")
        self.validate_identity("SELECT ARRAY_UNIQUE_AGG(x)")
        self.validate_identity("SELECT ARRAY_APPEND([1, 2, 3], 4)")
        self.validate_identity("SELECT ARRAY_CAT([1, 2], [3, 4])")
        self.validate_identity("SELECT ARRAY_PREPEND([2, 3, 4], 1)")
        self.validate_identity("SELECT ARRAY_REMOVE([1, 2, 3], 2)")
        self.validate_identity("SELECT AI_AGG(review, 'Summarize the reviews')")
        self.validate_identity("SELECT AI_SUMMARIZE_AGG(review)")
        self.validate_identity("SELECT AI_CLASSIFY('text', ['travel', 'cooking'])")
        self.validate_identity("SELECT OBJECT_CONSTRUCT()")
        self.validate_identity("SELECT CURRENT_ACCOUNT()")
        self.validate_identity("SELECT CURRENT_ACCOUNT_NAME()")
        self.validate_identity("SELECT CURRENT_AVAILABLE_ROLES()")
        self.validate_identity("SELECT CURRENT_CLIENT()")
        self.validate_identity("SELECT CURRENT_IP_ADDRESS()")
        self.validate_identity("SELECT CURRENT_DATABASE()")
        self.validate_identity("SELECT CURRENT_SCHEMAS()")
        self.validate_identity("SELECT CURRENT_SECONDARY_ROLES()")
        self.validate_identity("SELECT CURRENT_SESSION()")
        self.validate_identity("SELECT CURRENT_STATEMENT()")
        self.validate_identity("SELECT CURRENT_VERSION()")
        self.validate_identity("SELECT CURRENT_TRANSACTION()")
        self.validate_identity("SELECT CURRENT_WAREHOUSE()")
        self.validate_identity("SELECT CURRENT_ORGANIZATION_USER()")
        self.validate_identity("SELECT CURRENT_REGION()")
        self.validate_identity("SELECT CURRENT_ROLE()")
        self.validate_identity("SELECT CURRENT_ROLE_TYPE()")
        self.validate_identity("SELECT DAY(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT DAYOFMONTH(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT DAYOFYEAR(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT MONTH(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT QUARTER(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT WEEK(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT WEEKISO(CURRENT_TIMESTAMP())")
        self.validate_identity("WEEKOFYEAR(tstamp)", "WEEK(tstamp)")
        self.validate_identity("SELECT YEAR(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT YEAROFWEEK(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT YEAROFWEEKISO(CURRENT_TIMESTAMP())")
        self.validate_all(
            "SELECT DAYOFWEEKISO('2024-01-15'::DATE)",
            write={
                "snowflake": "SELECT DAYOFWEEKISO(CAST('2024-01-15' AS DATE))",
                "duckdb": "SELECT ISODOW(CAST('2024-01-15' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT YEAROFWEEK('2024-12-31'::DATE)",
            write={
                "snowflake": "SELECT YEAROFWEEK(CAST('2024-12-31' AS DATE))",
                "duckdb": "SELECT EXTRACT(ISOYEAR FROM CAST('2024-12-31' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT YEAROFWEEKISO('2024-12-31'::DATE)",
            write={
                "snowflake": "SELECT YEAROFWEEKISO(CAST('2024-12-31' AS DATE))",
                "duckdb": "SELECT EXTRACT(ISOYEAR FROM CAST('2024-12-31' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT WEEKISO('2024-01-15'::DATE)",
            write={
                "snowflake": "SELECT WEEKISO(CAST('2024-01-15' AS DATE))",
                "duckdb": "SELECT WEEKOFYEAR(CAST('2024-01-15' AS DATE))",
            },
        )
        self.validate_identity("SELECT SUM(amount) FROM mytable GROUP BY ALL")
        self.validate_identity("SELECT STDDEV(x)")
        self.validate_identity("SELECT STDDEV(x) OVER (PARTITION BY 1)")
        self.validate_identity("SELECT STDDEV_POP(x)")
        self.validate_identity("SELECT STDDEV_POP(x) OVER (PARTITION BY 1)")
        self.validate_identity("SELECT STDDEV_SAMP(x)", "SELECT STDDEV(x)")
        self.validate_identity(
            "SELECT STDDEV_SAMP(x) OVER (PARTITION BY 1)", "SELECT STDDEV(x) OVER (PARTITION BY 1)"
        )
        self.validate_identity("SELECT KURTOSIS(x)")
        self.validate_identity("SELECT KURTOSIS(x) OVER (PARTITION BY 1)")
        self.validate_identity("WITH x AS (SELECT 1 AS foo) SELECT foo FROM IDENTIFIER('x')")
        self.validate_identity("WITH x AS (SELECT 1 AS foo) SELECT IDENTIFIER('foo') FROM x")
        self.validate_identity("INITCAP('iqamqinterestedqinqthisqtopic', 'q')")
        self.validate_identity("OBJECT_CONSTRUCT(*)")
        self.validate_identity("SELECT CAST('2021-01-01' AS DATE) + INTERVAL '1 DAY'")
        self.validate_identity("SELECT HLL(*)")
        self.validate_identity("SELECT HLL(a)")
        self.validate_identity("SELECT HLL(DISTINCT t.a)")
        self.validate_identity("SELECT HLL(a, b, c)")
        self.validate_identity("SELECT HLL(DISTINCT a, b, c)")
        self.validate_identity("$x")  # parameter
        self.validate_identity("a$b")  # valid snowflake identifier
        self.validate_identity("SELECT REGEXP_LIKE(a, b, c)")
        self.validate_identity("CREATE TABLE foo (bar DOUBLE AUTOINCREMENT START 0 INCREMENT 1)")
        self.validate_identity("COMMENT IF EXISTS ON TABLE foo IS 'bar'")
        self.validate_identity("SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', col)")
        self.validate_identity("SELECT CURRENT_ORGANIZATION_NAME()")
        self.validate_identity("ALTER TABLE a SWAP WITH b")
        self.validate_identity("SELECT MATCH_CONDITION")
        self.validate_identity("SELECT OBJECT_AGG(key, value) FROM tbl")
        self.validate_identity("1 /* /* */")
        self.validate_identity("TO_TIMESTAMP(col, fmt)")
        self.validate_identity("SELECT TO_CHAR(CAST('12:05:05' AS TIME))")
        self.validate_identity("SELECT TRIM(COALESCE(TO_CHAR(CAST(c AS TIME)), '')) FROM t")
        self.validate_identity("SELECT GET_PATH(PARSE_JSON(foo), 'bar')")
        self.validate_identity("SELECT PARSE_IP('192.168.1.1', 'INET')")
        self.validate_identity("SELECT PARSE_IP('192.168.1.1', 'INET', 0)")
        self.validate_identity("SELECT GET_PATH(foo, 'bar')")
        self.validate_identity("SELECT a, exclude, b FROM xxx")
        self.validate_identity("SELECT ARRAY_SORT(x, TRUE, FALSE)")
        self.validate_identity("SELECT BOOLXOR_AGG(col) FROM tbl")
        self.validate_identity(
            "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY col) OVER (PARTITION BY category)"
        )
        self.validate_identity(
            "SELECT DATEADD(DAY, -7, DATEADD(t.m, 1, CAST('2023-01-03' AS DATE))) FROM (SELECT 'month' AS m) AS t"
        ).selects[0].this.unit.assert_is(exp.Column)
        self.validate_identity(
            "SELECT STRTOK('hello world')", "SELECT SPLIT_PART('hello world', ' ', 1)"
        )
        self.validate_identity(
            "SELECT STRTOK('hello world', ' ')", "SELECT SPLIT_PART('hello world', ' ', 1)"
        )
        self.validate_identity(
            "SELECT STRTOK('hello world', ' ', 2)", "SELECT SPLIT_PART('hello world', ' ', 2)"
        )
        self.validate_identity("SELECT FILE_URL FROM DIRECTORY(@mystage) WHERE SIZE > 100000").args[
            "from_"
        ].this.this.assert_is(exp.DirectoryStage).this.assert_is(exp.Var)
        self.validate_identity(
            "SELECT AI_CLASSIFY('text', ['travel', 'cooking'], OBJECT_CONSTRUCT('output_mode', 'multi'))"
        )
        self.validate_identity(
            "SELECT * FROM table AT (TIMESTAMP => '2024-07-24') UNPIVOT(a FOR b IN (c)) AS pivot_table"
        )
        self.validate_identity(
            "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q3', '2023_Q4', '2024_Q1') DEFAULT ON NULL (0)) ORDER BY empid"
        )
        self.validate_identity(
            "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (SELECT DISTINCT quarter FROM ad_campaign_types_by_quarter WHERE television = TRUE ORDER BY quarter)) ORDER BY empid"
        )
        self.validate_identity(
            "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter)) ORDER BY empid"
        )
        self.validate_identity(
            "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY)) ORDER BY empid"
        )
        self.validate_identity(
            "MERGE INTO my_db AS ids USING (SELECT new_id FROM my_model WHERE NOT col IS NULL) AS new_ids ON ids.type = new_ids.type AND ids.source = new_ids.source WHEN NOT MATCHED THEN INSERT VALUES (new_ids.new_id)"
        )
        self.validate_identity(
            "INSERT OVERWRITE TABLE t SELECT 1", "INSERT OVERWRITE INTO t SELECT 1"
        )
        self.validate_identity(
            'DESCRIBE TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."WEB_SITE" type=stage'
        )
        self.validate_identity(
            "SELECT * FROM DATA AS DATA_L ASOF JOIN DATA AS DATA_R MATCH_CONDITION (DATA_L.VAL > DATA_R.VAL) ON DATA_L.ID = DATA_R.ID"
        )
        self.validate_identity(
            """SELECT TO_TIMESTAMP('2025-01-16T14:45:30.123+0500', 'yyyy-mm-DDThh24:mi:ss.ff9tzhtzm')"""
        )
        self.validate_identity(
            "SELECT * REPLACE (CAST(col AS TEXT) AS scol) FROM t",
            "SELECT * REPLACE (CAST(col AS VARCHAR) AS scol) FROM t",
        )
        self.validate_identity(
            "GET(value, 'foo')::VARCHAR",
            "CAST(GET(value, 'foo') AS VARCHAR)",
        )
        self.validate_identity(
            "SELECT 1 put",
            "SELECT 1 AS put",
        )
        self.validate_identity(
            "SELECT 1 get",
            "SELECT 1 AS get",
        )
        self.validate_identity(
            "WITH t (SELECT 1 AS c) SELECT c FROM t",
            "WITH t AS (SELECT 1 AS c) SELECT c FROM t",
        )
        self.validate_identity(
            "GET_PATH(json_data, '$id')",
            """GET_PATH(json_data, '["$id"]')""",
        )
        self.validate_identity(
            "CAST(x AS GEOGRAPHY)",
            "TO_GEOGRAPHY(x)",
        )
        self.validate_identity(
            "CAST(x AS GEOMETRY)",
            "TO_GEOMETRY(x)",
        )
        self.validate_identity(
            "transform(x, a int -> a + a + 1)",
            "TRANSFORM(x, a -> CAST(a AS INT) + CAST(a AS INT) + 1)",
        )
        self.validate_identity(
            "SELECT * FROM s WHERE c NOT IN (1, 2, 3)",
            "SELECT * FROM s WHERE NOT c IN (1, 2, 3)",
        )
        self.validate_identity(
            "SELECT * FROM s WHERE c NOT IN (SELECT * FROM t)",
            "SELECT * FROM s WHERE c <> ALL (SELECT * FROM t)",
        )
        self.validate_identity(
            "SELECT * FROM t1 INNER JOIN t2 USING (t1.col)",
            "SELECT * FROM t1 INNER JOIN t2 USING (col)",
        )
        self.validate_identity(
            "CURRENT_TIMESTAMP - INTERVAL '1 w' AND (1 = 1)",
            "CURRENT_TIMESTAMP() - INTERVAL '1 WEEK' AND (1 = 1)",
        )
        self.validate_identity(
            "REGEXP_REPLACE('target', 'pattern', '\n')",
            "REGEXP_REPLACE('target', 'pattern', '\\n')",
        )
        self.validate_identity(
            "SELECT a:from::STRING, a:from || ' test' ",
            "SELECT CAST(GET_PATH(a, 'from') AS VARCHAR), GET_PATH(a, 'from') || ' test'",
        )
        self.validate_identity(
            "SELECT a:select",
            "SELECT GET_PATH(a, 'select')",
        )
        self.validate_identity("x:from", "GET_PATH(x, 'from')")
        self.validate_identity(
            "value:values::string::int",
            "CAST(CAST(GET_PATH(value, 'values') AS VARCHAR) AS INT)",
        )
        self.validate_identity(
            """SELECT GET_PATH(PARSE_JSON('{"y": [{"z": 1}]}'), 'y[0]:z')""",
            """SELECT GET_PATH(PARSE_JSON('{"y": [{"z": 1}]}'), 'y[0].z')""",
        )
        self.validate_identity(
            "SELECT p FROM t WHERE p:val NOT IN ('2')",
            "SELECT p FROM t WHERE NOT GET_PATH(p, 'val') IN ('2')",
        )
        self.validate_identity(
            """SELECT PARSE_JSON('{"x": "hello"}'):x LIKE 'hello'""",
            """SELECT GET_PATH(PARSE_JSON('{"x": "hello"}'), 'x') LIKE 'hello'""",
        )
        self.validate_identity(
            """SELECT data:x LIKE 'hello' FROM some_table""",
            """SELECT GET_PATH(data, 'x') LIKE 'hello' FROM some_table""",
        )
        self.validate_identity(
            "SELECT SUM({ fn CONVERT(123, SQL_DOUBLE) })",
            "SELECT SUM(CAST(123 AS DOUBLE))",
        )
        self.validate_identity(
            "SELECT SUM({ fn CONVERT(123, SQL_VARCHAR) })",
            "SELECT SUM(CAST(123 AS VARCHAR))",
        )
        self.validate_identity(
            "SELECT TIMESTAMPFROMPARTS(d, t)",
            "SELECT TIMESTAMP_FROM_PARTS(d, t)",
        )
        self.validate_identity(
            "SELECT v:attr[0].name FROM vartab",
            "SELECT GET_PATH(v, 'attr[0].name') FROM vartab",
        )
        self.validate_identity(
            'SELECT v:"fruit" FROM vartab',
            """SELECT GET_PATH(v, 'fruit') FROM vartab""",
        )
        self.validate_identity(
            "v:attr[0]:name",
            "GET_PATH(v, 'attr[0].name')",
        )
        self.validate_identity(
            "a.x:from.b:c.d::int",
            "CAST(GET_PATH(a.x, 'from.b.c.d') AS INT)",
        )
        self.validate_identity(
            """SELECT PARSE_JSON('{"food":{"fruit":"banana"}}'):food.fruit::VARCHAR""",
            """SELECT CAST(GET_PATH(PARSE_JSON('{"food":{"fruit":"banana"}}'), 'food.fruit') AS VARCHAR)""",
        )
        self.validate_identity(
            "SELECT * FROM t, UNNEST(x) WITH ORDINALITY",
            "SELECT * FROM t, TABLE(FLATTEN(INPUT => x)) AS _t0(seq, key, path, index, value, this)",
        )
        self.validate_identity(
            "CREATE TABLE foo (ID INT COMMENT $$some comment$$)",
            "CREATE TABLE foo (ID INT COMMENT 'some comment')",
        )
        self.validate_identity(
            "SELECT state, city, SUM(retail_price * quantity) AS gross_revenue FROM sales GROUP BY ALL"
        )
        self.validate_identity(
            "SELECT * FROM foo window",
            "SELECT * FROM foo AS window",
        )
        self.validate_identity(
            r"SELECT RLIKE(a, $$regular expression with \ characters: \d{2}-\d{3}-\d{4}$$, 'i') FROM log_source",
            r"SELECT REGEXP_LIKE(a, 'regular expression with \\ characters: \\d{2}-\\d{3}-\\d{4}', 'i') FROM log_source",
        )
        self.validate_identity(
            r"SELECT $$a ' \ \t \x21 z $ $$",
            r"SELECT 'a \' \\ \\t \\x21 z $ '",
        )
        self.validate_identity(
            "SELECT {'test': 'best'}::VARIANT",
            "SELECT CAST(OBJECT_CONSTRUCT('test', 'best') AS VARIANT)",
        )
        self.validate_identity(
            "SELECT {fn DAYNAME('2022-5-13')}",
            "SELECT DAYNAME('2022-5-13')",
        )
        self.validate_identity(
            "SELECT {fn LOG(5)}",
            "SELECT LN(5)",
        )
        self.validate_identity(
            "SELECT {fn CEILING(5.3)}",
            "SELECT CEIL(5.3)",
        )
        self.validate_identity(
            "SELECT CEIL(3.14)",
        )
        self.validate_identity(
            "SELECT CEIL(3.14, 1)",
        )
        self.validate_identity(
            "CAST(x AS BYTEINT)",
            "CAST(x AS INT)",
        )
        self.validate_identity(
            "CAST(x AS CHAR VARYING)",
            "CAST(x AS VARCHAR)",
        )
        self.validate_identity(
            "CAST(x AS CHARACTER VARYING)",
            "CAST(x AS VARCHAR)",
        )
        self.validate_identity(
            "CAST(x AS NCHAR VARYING)",
            "CAST(x AS VARCHAR)",
        )
        self.validate_identity(
            "CREATE OR REPLACE TEMPORARY TABLE x (y NUMBER IDENTITY(0, 1))",
            "CREATE OR REPLACE TEMPORARY TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)",
        )
        self.validate_identity(
            "CREATE TEMPORARY TABLE x (y NUMBER AUTOINCREMENT(0, 1))",
            "CREATE TEMPORARY TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)",
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE x (y NUMBER(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER)",
            "CREATE OR REPLACE TABLE x (y DECIMAL(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER)",
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE x (y NUMBER(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER)",
            "CREATE OR REPLACE TABLE x (y DECIMAL(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER)",
        )
        self.validate_identity(
            "CREATE TABLE x (y NUMBER IDENTITY START 0 INCREMENT 1)",
            "CREATE TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)",
        )
        self.validate_identity(
            "ALTER TABLE foo ADD COLUMN id INT identity(1, 1)",
            "ALTER TABLE foo ADD id INT AUTOINCREMENT START 1 INCREMENT 1",
        )
        self.validate_identity(
            "SELECT DAYOFWEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)",
            "SELECT DAYOFWEEK(CAST('2016-01-02T23:39:20.123-07:00' AS TIMESTAMP))",
        )
        self.validate_identity(
            "SELECT * FROM xxx WHERE col ilike '%Don''t%'",
            "SELECT * FROM xxx WHERE col ILIKE '%Don\\'t%'",
        )
        self.validate_identity(
            "SELECT * EXCLUDE a, b FROM xxx",
            "SELECT * EXCLUDE (a), b FROM xxx",
        )
        self.validate_identity(
            "SELECT * RENAME a AS b, c AS d FROM xxx",
            "SELECT * RENAME (a AS b), c AS d FROM xxx",
        )

        # Support for optional trailing commas after tables in from clause
        self.validate_identity(
            "SELECT * FROM xxx, yyy, zzz,",
            "SELECT * FROM xxx, yyy, zzz",
        )
        self.validate_identity(
            "SELECT * FROM xxx, yyy, zzz, WHERE foo = bar",
            "SELECT * FROM xxx, yyy, zzz WHERE foo = bar",
        )
        self.validate_identity(
            "SELECT * FROM xxx, yyy, zzz",
            "SELECT * FROM xxx, yyy, zzz",
        )

        self.validate_all(
            "SELECT LTRIM(RTRIM(col)) FROM t1",
            write={
                "duckdb": "SELECT LTRIM(RTRIM(col)) FROM t1",
                "snowflake": "SELECT LTRIM(RTRIM(col)) FROM t1",
            },
        )
        self.validate_all(
            "SELECT value['x'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'x')])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT x FROM UNNEST([STRUCT('x' AS x)])",
                "snowflake": "SELECT value['x'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'x')])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT value['x'] AS x, value['y'] AS y, value['z'] AS z FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1, 'y', 2, 'z', 3)])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT x, y, z FROM UNNEST([STRUCT(1 AS x, 2 AS y, 3 AS z)])",
                "snowflake": "SELECT value['x'] AS x, value['y'] AS y, value['z'] AS z FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1, 'y', 2, 'z', 3)])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT u1['x'] AS x, u2['y'] AS y FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1)])) AS _t0(seq, key, path, index, u1, this) CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('y', 2)])) AS _t1(seq, key, path, index, u2, this)",
            read={
                "bigquery": "SELECT u1.x, u2.y FROM UNNEST([STRUCT(1 AS x)]) AS u1, UNNEST([STRUCT(2 AS y)]) AS u2",
                "snowflake": "SELECT u1['x'] AS x, u2['y'] AS y FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1)])) AS _t0(seq, key, path, index, u1, this) CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('y', 2)])) AS _t1(seq, key, path, index, u2, this)",
            },
        )
        self.validate_all(
            "SELECT t.id, value['name'] AS name, value['age'] AS age FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('name', 'John', 'age', 30)])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT t.id, name, age FROM t, UNNEST([STRUCT('John' AS name, 30 AS age)])",
                "snowflake": "SELECT t.id, value['name'] AS name, value['age'] AS age FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('name', 'John', 'age', 30)])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT value FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1)])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT value FROM UNNEST([STRUCT(1 AS x)]) AS value",
                "snowflake": "SELECT value FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 1)])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT t.col1, value['field1'] AS field1, other_col, value['field2'] AS field2 FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('field1', 'a', 'field2', 'b')])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT t.col1, field1, other_col, field2 FROM t, UNNEST([STRUCT('a' AS field1, 'b' AS field2)])",
                "snowflake": "SELECT t.col1, value['field1'] AS field1, other_col, value['field2'] AS field2 FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('field1', 'a', 'field2', 'b')])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT value['x'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'value')])) AS _t0(seq, key, path, index, value, this))",
            read={
                "bigquery": "SELECT * FROM (SELECT x FROM UNNEST([STRUCT('value' AS x)]))",
                "snowflake": "SELECT * FROM (SELECT value['x'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'value')])) AS _t0(seq, key, path, index, value, this))",
            },
        )
        self.validate_all(
            "SELECT value FROM TABLE(FLATTEN(INPUT => [1, 2, 3])) AS _t0(seq, key, path, index, value, this)",
            read={
                "bigquery": "SELECT value FROM UNNEST([1, 2, 3]) AS value",
                "snowflake": "SELECT value FROM TABLE(FLATTEN(INPUT => [1, 2, 3])) AS _t0(seq, key, path, index, value, this)",
            },
        )
        self.validate_all(
            "SELECT * FROM t1 AS t1 CROSS JOIN t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i",
            read={
                "bigquery": "SELECT * FROM t1 AS t1, t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i",
                "snowflake": "SELECT * FROM t1 AS t1 CROSS JOIN t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i",
            },
        )
        self.validate_all(
            "SELECT value['x'] AS x, yval, zval FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'x', 'y', ['y1', 'y2', 'y3'], 'z', ['z1', 'z2', 'z3'])])) AS _t0(seq, key, path, index, value, this) CROSS JOIN TABLE(FLATTEN(INPUT => value['y'])) AS _t1(seq, key, path, index, yval, this) CROSS JOIN TABLE(FLATTEN(INPUT => value['z'])) AS _t2(seq, key, path, index, zval, this)",
            read={
                "bigquery": "SELECT x, yval, zval FROM UNNEST([STRUCT('x' AS x, ['y1', 'y2', 'y3'] AS y, ['z1', 'z2', 'z3'] AS z)]), UNNEST(y) AS yval, UNNEST(z) AS zval",
                "snowflake": "SELECT value['x'] AS x, yval, zval FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('x', 'x', 'y', ['y1', 'y2', 'y3'], 'z', ['z1', 'z2', 'z3'])])) AS _t0(seq, key, path, index, value, this) CROSS JOIN TABLE(FLATTEN(INPUT => value['y'])) AS _t1(seq, key, path, index, yval, this) CROSS JOIN TABLE(FLATTEN(INPUT => value['z'])) AS _t2(seq, key, path, index, zval, this)",
            },
        )
        self.validate_all(
            "SELECT _u['foo'] AS foo, bar, baz FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('foo', 'x', 'bars', ['y', 'z'], 'bazs', ['w'])])) AS _t0(seq, key, path, index, _u, this) CROSS JOIN TABLE(FLATTEN(INPUT => _u['bars'])) AS _t1(seq, key, path, index, bar, this) CROSS JOIN TABLE(FLATTEN(INPUT => _u['bazs'])) AS _t2(seq, key, path, index, baz, this)",
            read={
                "bigquery": "SELECT _u.foo, bar, baz FROM UNNEST([struct('x' AS foo, ['y', 'z'] AS bars, ['w'] AS bazs)]) AS _u, UNNEST(_u.bars) AS bar, UNNEST(_u.bazs) AS baz",
            },
        )
        self.validate_all(
            "SELECT _u, _u['foo'] AS foo, _u['bar'] AS bar FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('foo', 'x', 'bar', 'y')])) AS _t0(seq, key, path, index, _u, this)",
            read={
                "bigquery": "select _u, _u.foo, _u.bar from unnest([struct('x' as foo, 'y' AS bar)]) as _u",
            },
        )
        self.validate_all(
            "SELECT _u['foo'][0].bar FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT('foo', [OBJECT_CONSTRUCT('bar', 1)])])) AS _t0(seq, key, path, index, _u, this)",
            read={
                "bigquery": "select _u.foo[0].bar from unnest([struct([struct(1 as bar)] as foo)]) as _u",
            },
        )
        self.validate_all(
            "SELECT ARRAY_INTERSECTION([1, 2], [2, 3])",
            write={
                "snowflake": "SELECT ARRAY_INTERSECTION([1, 2], [2, 3])",
                "starrocks": "SELECT ARRAY_INTERSECT([1, 2], [2, 3])",
            },
        )
        self.validate_all(
            "CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT)",
            write={
                "duckdb": "CREATE TABLE test_table (id DECIMAL(38, 0) NOT NULL)",
                "snowflake": "CREATE TABLE test_table (id DECIMAL(38, 0) NOT NULL AUTOINCREMENT)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2025-01-16 14:45:30.123', 'yyyy-mm-DD hh24:mi:ss.ff6')",
            write={
                "": "SELECT STR_TO_TIME('2025-01-16 14:45:30.123', '%Y-%m-%d %H:%M:%S.%f')",
                "snowflake": "SELECT TO_TIMESTAMP('2025-01-16 14:45:30.123', 'yyyy-mm-DD hh24:mi:ss.ff6')",
            },
        )
        self.validate_all(
            "ARRAY_CONSTRUCT_COMPACT(1, null, 2)",
            write={
                "spark": "ARRAY_COMPACT(ARRAY(1, NULL, 2))",
                "snowflake": "ARRAY_CONSTRUCT_COMPACT(1, NULL, 2)",
            },
        )
        self.validate_all(
            "ARRAY_COMPACT(arr)",
            read={
                "spark": "ARRAY_COMPACT(arr)",
                "databricks": "ARRAY_COMPACT(arr)",
                "snowflake": "ARRAY_COMPACT(arr)",
            },
            write={
                "spark": "ARRAY_COMPACT(arr)",
                "databricks": "ARRAY_COMPACT(arr)",
            },
        )
        self.validate_all(
            "OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL)",
            read={
                "bigquery": "JSON_OBJECT(['key_1', 'key_2'], ['one', NULL])",
                "duckdb": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
            },
            write={
                "bigquery": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "duckdb": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "snowflake": "OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL)",
            },
        )
        # Test simple case - uses MAKE_TIME (values within normal ranges)
        self.validate_all(
            "SELECT TIME_FROM_PARTS(12, 34, 56)",
            write={
                "duckdb": "SELECT MAKE_TIME(12, 34, 56)",
                "snowflake": "SELECT TIME_FROM_PARTS(12, 34, 56)",
            },
        )
        # Test with nanoseconds - uses INTERVAL arithmetic
        self.validate_all(
            "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
            write={
                "duckdb": "SELECT CAST('00:00:00' AS TIME) + INTERVAL ((12 * 3600) + (34 * 60) + 56 + (987654321 / 1000000000.0)) SECOND",
                "snowflake": "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
            },
        )
        # Test overflow normalization - documented Snowflake feature with INTERVAL arithmetic
        self.validate_all(
            "SELECT TIME_FROM_PARTS(0, 100, 0)",
            write={
                "duckdb": "SELECT CAST('00:00:00' AS TIME) + INTERVAL ((0 * 3600) + (100 * 60) + 0) SECOND",
                "snowflake": "SELECT TIME_FROM_PARTS(0, 100, 0)",
            },
        )
        self.validate_identity(
            "SELECT TIMESTAMPNTZFROMPARTS(2013, 4, 5, 12, 00, 00)",
            "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)",
        )
        self.validate_all(
            "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)",
            read={
                "duckdb": "SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00)",
                "snowflake": "SELECT TIMESTAMP_NTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)",
            },
            write={
                "duckdb": "SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00)",
                "snowflake": "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_FROM_PARTS(TO_DATE('2023-06-15'), TO_TIME('14:30:45'))",
            write={
                "duckdb": "SELECT CAST('2023-06-15' AS DATE) + CAST('14:30:45' AS TIME)",
                "snowflake": "SELECT TIMESTAMP_FROM_PARTS(CAST('2023-06-15' AS DATE), CAST('14:30:45' AS TIME))",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_NTZ_FROM_PARTS(TO_DATE('2023-06-15'), TO_TIME('14:30:45'))",
            write={
                "duckdb": "SELECT CAST('2023-06-15' AS DATE) + CAST('14:30:45' AS TIME)",
                "snowflake": "SELECT TIMESTAMP_FROM_PARTS(CAST('2023-06-15' AS DATE), CAST('14:30:45' AS TIME))",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_LTZ_FROM_PARTS(2023, 6, 15, 14, 30, 45)",
            write={
                "duckdb": "SELECT CAST(MAKE_TIMESTAMP(2023, 6, 15, 14, 30, 45) AS TIMESTAMPTZ)",
                "snowflake": "SELECT TIMESTAMP_LTZ_FROM_PARTS(2023, 6, 15, 14, 30, 45)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_TZ_FROM_PARTS(2023, 6, 15, 14, 30, 45, 0, 'America/Los_Angeles')",
            write={
                "duckdb": "SELECT MAKE_TIMESTAMP(2023, 6, 15, 14, 30, 45) AT TIME ZONE 'America/Los_Angeles'",
                "snowflake": "SELECT TIMESTAMP_TZ_FROM_PARTS(2023, 6, 15, 14, 30, 45, 0, 'America/Los_Angeles')",
            },
        )
        self.validate_all(
            """WITH vartab(v) AS (select parse_json('[{"attr": [{"name": "banana"}]}]')) SELECT GET_PATH(v, '[0].attr[0].name') FROM vartab""",
            write={
                "bigquery": """WITH vartab AS (SELECT PARSE_JSON('[{"attr": [{"name": "banana"}]}]') AS v) SELECT JSON_EXTRACT(v, '$[0].attr[0].name') FROM vartab""",
                "duckdb": """WITH vartab(v) AS (SELECT JSON('[{"attr": [{"name": "banana"}]}]')) SELECT v -> '$[0].attr[0].name' FROM vartab""",
                "mysql": """WITH vartab(v) AS (SELECT '[{"attr": [{"name": "banana"}]}]') SELECT JSON_EXTRACT(v, '$[0].attr[0].name') FROM vartab""",
                "presto": """WITH vartab(v) AS (SELECT JSON_PARSE('[{"attr": [{"name": "banana"}]}]')) SELECT JSON_EXTRACT(v, '$[0].attr[0].name') FROM vartab""",
                "snowflake": """WITH vartab(v) AS (SELECT PARSE_JSON('[{"attr": [{"name": "banana"}]}]')) SELECT GET_PATH(v, '[0].attr[0].name') FROM vartab""",
                "tsql": """WITH vartab(v) AS (SELECT '[{"attr": [{"name": "banana"}]}]') SELECT ISNULL(JSON_QUERY(v, '$[0].attr[0].name'), JSON_VALUE(v, '$[0].attr[0].name')) FROM vartab""",
            },
        )
        self.validate_all(
            """WITH vartab(v) AS (select parse_json('{"attr": [{"name": "banana"}]}')) SELECT GET_PATH(v, 'attr[0].name') FROM vartab""",
            write={
                "bigquery": """WITH vartab AS (SELECT PARSE_JSON('{"attr": [{"name": "banana"}]}') AS v) SELECT JSON_EXTRACT(v, '$.attr[0].name') FROM vartab""",
                "duckdb": """WITH vartab(v) AS (SELECT JSON('{"attr": [{"name": "banana"}]}')) SELECT v -> '$.attr[0].name' FROM vartab""",
                "mysql": """WITH vartab(v) AS (SELECT '{"attr": [{"name": "banana"}]}') SELECT JSON_EXTRACT(v, '$.attr[0].name') FROM vartab""",
                "presto": """WITH vartab(v) AS (SELECT JSON_PARSE('{"attr": [{"name": "banana"}]}')) SELECT JSON_EXTRACT(v, '$.attr[0].name') FROM vartab""",
                "snowflake": """WITH vartab(v) AS (SELECT PARSE_JSON('{"attr": [{"name": "banana"}]}')) SELECT GET_PATH(v, 'attr[0].name') FROM vartab""",
                "tsql": """WITH vartab(v) AS (SELECT '{"attr": [{"name": "banana"}]}') SELECT ISNULL(JSON_QUERY(v, '$.attr[0].name'), JSON_VALUE(v, '$.attr[0].name')) FROM vartab""",
            },
        )
        self.validate_all(
            """SELECT PARSE_JSON('{"fruit":"banana"}'):fruit""",
            write={
                "bigquery": """SELECT JSON_EXTRACT(PARSE_JSON('{"fruit":"banana"}'), '$.fruit')""",
                "databricks": """SELECT PARSE_JSON('{"fruit":"banana"}'):fruit""",
                "duckdb": """SELECT JSON('{"fruit":"banana"}') -> '$.fruit'""",
                "mysql": """SELECT JSON_EXTRACT('{"fruit":"banana"}', '$.fruit')""",
                "presto": """SELECT JSON_EXTRACT(JSON_PARSE('{"fruit":"banana"}'), '$.fruit')""",
                "snowflake": """SELECT GET_PATH(PARSE_JSON('{"fruit":"banana"}'), 'fruit')""",
                "spark": """SELECT GET_JSON_OBJECT('{"fruit":"banana"}', '$.fruit')""",
                "tsql": """SELECT ISNULL(JSON_QUERY('{"fruit":"banana"}', '$.fruit'), JSON_VALUE('{"fruit":"banana"}', '$.fruit'))""",
            },
        )
        self.validate_all(
            "SELECT TO_ARRAY(['test'])",
            write={
                "snowflake": "SELECT TO_ARRAY(['test'])",
                "spark": "SELECT ARRAY('test')",
            },
        )
        self.validate_all(
            "SELECT TO_ARRAY(['test'])",
            write={
                "snowflake": "SELECT TO_ARRAY(['test'])",
                "spark": "SELECT ARRAY('test')",
            },
        )
        self.validate_all(
            # We need to qualify the columns in this query because "value" would be ambiguous
            'WITH t(x, "value") AS (SELECT [1, 2, 3], 1) SELECT IFF(_u.pos = _u_2.pos_2, _u_2."value", NULL) AS "value" FROM t CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (GREATEST(ARRAY_SIZE(t.x)) - 1) + 1))) AS _u(seq, key, path, index, pos, this) CROSS JOIN TABLE(FLATTEN(INPUT => t.x)) AS _u_2(seq, key, path, pos_2, "value", this) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > (ARRAY_SIZE(t.x) - 1) AND _u_2.pos_2 = (ARRAY_SIZE(t.x) - 1))',
            read={
                "duckdb": 'WITH t(x, "value") AS (SELECT [1,2,3], 1) SELECT UNNEST(t.x) AS "value" FROM t',
            },
        )
        self.validate_all(
            "SELECT { 'Manitoba': 'Winnipeg', 'foo': 'bar' } AS province_capital",
            write={
                "duckdb": "SELECT {'Manitoba': 'Winnipeg', 'foo': 'bar'} AS province_capital",
                "snowflake": "SELECT OBJECT_CONSTRUCT('Manitoba', 'Winnipeg', 'foo', 'bar') AS province_capital",
                "spark": "SELECT STRUCT('Winnipeg' AS Manitoba, 'bar' AS foo) AS province_capital",
            },
        )
        self.validate_all(
            "SELECT COLLATE('B', 'und:ci')",
            write={
                "bigquery": "SELECT COLLATE('B', 'und:ci')",
                "snowflake": "SELECT COLLATE('B', 'und:ci')",
            },
        )

        self.validate_all(
            "SELECT To_BOOLEAN('T')",
            write={
                "duckdb": "SELECT CASE WHEN UPPER(CAST('T' AS TEXT)) = 'ON' THEN TRUE WHEN UPPER(CAST('T' AS TEXT)) = 'OFF' THEN FALSE WHEN ISNAN(TRY_CAST('T' AS REAL)) OR ISINF(TRY_CAST('T' AS REAL)) THEN ERROR('TO_BOOLEAN: Non-numeric values NaN and INF are not supported') ELSE CAST('T' AS BOOLEAN) END",
            },
        )
        self.validate_all(
            "SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d",
            read={
                "oracle": "SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d",
            },
            write={
                "oracle": "SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d",
                "snowflake": "SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d",
            },
        )
        self.validate_all(
            "SELECT INSERT(a, 0, 0, 'b')",
            read={
                "mysql": "SELECT INSERT(a, 0, 0, 'b')",
                "snowflake": "SELECT INSERT(a, 0, 0, 'b')",
                "tsql": "SELECT STUFF(a, 0, 0, 'b')",
            },
            write={
                "mysql": "SELECT INSERT(a, 0, 0, 'b')",
                "snowflake": "SELECT INSERT(a, 0, 0, 'b')",
                "tsql": "SELECT STUFF(a, 0, 0, 'b')",
            },
        )
        self.validate_all(
            "ARRAY_GENERATE_RANGE(0, 3)",
            write={
                "bigquery": "GENERATE_ARRAY(0, 3 - 1)",
                "postgres": "GENERATE_SERIES(0, 3 - 1)",
                "presto": "SEQUENCE(0, 3 - 1)",
                "snowflake": "ARRAY_GENERATE_RANGE(0, (3 - 1) + 1)",
            },
        )
        self.validate_all(
            "ARRAY_GENERATE_RANGE(0, 3 + 1)",
            read={
                "bigquery": "GENERATE_ARRAY(0, 3)",
                "postgres": "GENERATE_SERIES(0, 3)",
                "presto": "SEQUENCE(0, 3)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('year', TIMESTAMP '2020-01-01')",
            write={
                "hive": "SELECT EXTRACT(year FROM CAST('2020-01-01' AS TIMESTAMP))",
                "snowflake": "SELECT DATE_PART('year', CAST('2020-01-01' AS TIMESTAMP))",
                "spark": "SELECT EXTRACT(year FROM CAST('2020-01-01' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT * FROM (VALUES (0) foo(bar))",
            write={"snowflake": "SELECT * FROM (VALUES (0)) AS foo(bar)"},
        )
        self.validate_all(
            "OBJECT_CONSTRUCT('a', b, 'c', d)",
            read={
                "": "STRUCT(b as a, d as c)",
            },
            write={
                "duckdb": "{'a': b, 'c': d}",
                "snowflake": "OBJECT_CONSTRUCT('a', b, 'c', d)",
                "": "STRUCT(b AS a, d AS c)",
            },
        )
        self.validate_identity("OBJECT_CONSTRUCT(a, b, c, d)")

        self.validate_all(
            "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1",
            write={
                "": "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) = 1",
                "databricks": "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) = 1",
                "hive": "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1",
                "presto": "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1",
                "snowflake": "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1",
                "spark": "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1",
                "sqlite": "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1",
                "trino": "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1",
            },
        )
        self.validate_all(
            "SELECT BOOLOR_AGG(c1), BOOLOR_AGG(c2) FROM test",
            write={
                "": "SELECT LOGICAL_OR(c1), LOGICAL_OR(c2) FROM test",
                "duckdb": "SELECT BOOL_OR(CAST(c1 AS BOOLEAN)), BOOL_OR(CAST(c2 AS BOOLEAN)) FROM test",
                "oracle": "SELECT MAX(c1), MAX(c2) FROM test",
                "postgres": "SELECT BOOL_OR(c1), BOOL_OR(c2) FROM test",
                "snowflake": "SELECT BOOLOR_AGG(c1), BOOLOR_AGG(c2) FROM test",
                "spark": "SELECT BOOL_OR(c1), BOOL_OR(c2) FROM test",
                "sqlite": "SELECT MAX(c1), MAX(c2) FROM test",
            },
        )
        self.validate_all(
            "SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test",
            write={
                "": "SELECT LOGICAL_AND(c1), LOGICAL_AND(c2) FROM test",
                "duckdb": "SELECT BOOL_AND(CAST(c1 AS BOOLEAN)), BOOL_AND(CAST(c2 AS BOOLEAN)) FROM test",
                "oracle": "SELECT MIN(c1), MIN(c2) FROM test",
                "postgres": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "snowflake": "SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test",
                "spark": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "sqlite": "SELECT MIN(c1), MIN(c2) FROM test",
                "mysql": "SELECT MIN(c1), MIN(c2) FROM test",
            },
        )
        self.validate_all(
            "SELECT BOOLXOR_AGG(c1) FROM test",
            write={
                "duckdb": "SELECT COUNT_IF(CAST(c1 AS BOOLEAN)) = 1 FROM test",
                "snowflake": "SELECT BOOLXOR_AGG(c1) FROM test",
            },
        )
        for suffix in (
            "",
            " OVER ()",
        ):
            self.validate_all(
                f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x){suffix}",
                read={
                    "postgres": f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x){suffix}",
                },
                write={
                    "": f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x NULLS LAST){suffix}",
                    "duckdb": f"SELECT QUANTILE_CONT(x, 0.5 ORDER BY x){suffix}",
                    "postgres": f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x){suffix}",
                    "snowflake": f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x){suffix}",
                },
            )
            for func in (
                "COVAR_POP",
                "COVAR_SAMP",
            ):
                self.validate_all(
                    f"SELECT {func}(y, x){suffix}",
                    write={
                        "": f"SELECT {func}(y, x){suffix}",
                        "duckdb": f"SELECT {func}(y, x){suffix}",
                        "postgres": f"SELECT {func}(y, x){suffix}",
                        "snowflake": f"SELECT {func}(y, x){suffix}",
                    },
                )
        self.validate_all(
            "TO_CHAR(x, y)",
            read={
                "": "TO_CHAR(x, y)",
                "snowflake": "TO_VARCHAR(x, y)",
            },
            write={
                "": "CAST(x AS TEXT)",
                "databricks": "TO_CHAR(x, y)",
                "drill": "TO_CHAR(x, y)",
                "oracle": "TO_CHAR(x, y)",
                "postgres": "TO_CHAR(x, y)",
                "snowflake": "TO_CHAR(x, y)",
                "teradata": "TO_CHAR(x, y)",
            },
        )
        for to_func in ("TO_CHAR", "TO_VARCHAR"):
            with self.subTest(f"Testing transpilation of {to_func}"):
                self.validate_identity(
                    f"{to_func}(foo::DATE, 'yyyy')",
                    "TO_CHAR(CAST(foo AS DATE), 'yyyy')",
                )
                self.validate_all(
                    f"{to_func}(foo::TIMESTAMP, 'YYYY-MM')",
                    write={
                        "snowflake": "TO_CHAR(CAST(foo AS TIMESTAMP), 'yyyy-mm')",
                        "duckdb": "STRFTIME(CAST(foo AS TIMESTAMP), '%Y-%m')",
                    },
                )
        self.validate_all(
            "SQUARE(x)",
            write={
                "bigquery": "POWER(x, 2)",
                "clickhouse": "POWER(x, 2)",
                "databricks": "POWER(x, 2)",
                "drill": "POW(x, 2)",
                "duckdb": "POWER(x, 2)",
                "hive": "POWER(x, 2)",
                "mysql": "POWER(x, 2)",
                "oracle": "POWER(x, 2)",
                "postgres": "POWER(x, 2)",
                "presto": "POWER(x, 2)",
                "redshift": "POWER(x, 2)",
                "snowflake": "POWER(x, 2)",
                "spark": "POWER(x, 2)",
                "sqlite": "POWER(x, 2)",
                "starrocks": "POWER(x, 2)",
                "teradata": "x ** 2",
                "trino": "POWER(x, 2)",
                "tsql": "POWER(x, 2)",
            },
        )
        self.validate_all(
            "POWER(x, 2)",
            read={
                "oracle": "SQUARE(x)",
                "snowflake": "SQUARE(x)",
                "tsql": "SQUARE(x)",
            },
        )
        self.validate_all(
            "DIV0(foo, bar)",
            write={
                "snowflake": "IFF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)",
                "sqlite": "IIF(bar = 0 AND NOT foo IS NULL, 0, CAST(foo AS REAL) / bar)",
                "presto": "IF(bar = 0 AND NOT foo IS NULL, 0, CAST(foo AS DOUBLE) / bar)",
                "spark": "IF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)",
                "hive": "IF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)",
                "duckdb": "CASE WHEN bar = 0 AND NOT foo IS NULL THEN 0 ELSE foo / bar END",
            },
        )
        self.validate_all(
            "DIV0(a - b, c - d)",
            write={
                "snowflake": "IFF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))",
                "sqlite": "IIF((c - d) = 0 AND NOT (a - b) IS NULL, 0, CAST((a - b) AS REAL) / (c - d))",
                "presto": "IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, CAST((a - b) AS DOUBLE) / (c - d))",
                "spark": "IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))",
                "hive": "IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))",
                "duckdb": "CASE WHEN (c - d) = 0 AND NOT (a - b) IS NULL THEN 0 ELSE (a - b) / (c - d) END",
            },
        )
        self.validate_all(
            "DIV0NULL(foo, bar)",
            write={
                "snowflake": "IFF(bar = 0 OR bar IS NULL, 0, foo / bar)",
                "sqlite": "IIF(bar = 0 OR bar IS NULL, 0, CAST(foo AS REAL) / bar)",
                "presto": "IF(bar = 0 OR bar IS NULL, 0, CAST(foo AS DOUBLE) / bar)",
                "spark": "IF(bar = 0 OR bar IS NULL, 0, foo / bar)",
                "hive": "IF(bar = 0 OR bar IS NULL, 0, foo / bar)",
                "duckdb": "CASE WHEN bar = 0 OR bar IS NULL THEN 0 ELSE foo / bar END",
            },
        )
        self.validate_all(
            "DIV0NULL(a - b, c - d)",
            write={
                "snowflake": "IFF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))",
                "sqlite": "IIF((c - d) = 0 OR (c - d) IS NULL, 0, CAST((a - b) AS REAL) / (c - d))",
                "presto": "IF((c - d) = 0 OR (c - d) IS NULL, 0, CAST((a - b) AS DOUBLE) / (c - d))",
                "spark": "IF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))",
                "hive": "IF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))",
                "duckdb": "CASE WHEN (c - d) = 0 OR (c - d) IS NULL THEN 0 ELSE (a - b) / (c - d) END",
            },
        )
        self.validate_all(
            "ZEROIFNULL(foo)",
            write={
                "snowflake": "IFF(foo IS NULL, 0, foo)",
                "sqlite": "IIF(foo IS NULL, 0, foo)",
                "presto": "IF(foo IS NULL, 0, foo)",
                "spark": "IF(foo IS NULL, 0, foo)",
                "hive": "IF(foo IS NULL, 0, foo)",
                "duckdb": "CASE WHEN foo IS NULL THEN 0 ELSE foo END",
            },
        )
        self.validate_all(
            "NULLIFZERO(foo)",
            write={
                "snowflake": "IFF(foo = 0, NULL, foo)",
                "sqlite": "IIF(foo = 0, NULL, foo)",
                "presto": "IF(foo = 0, NULL, foo)",
                "spark": "IF(foo = 0, NULL, foo)",
                "hive": "IF(foo = 0, NULL, foo)",
                "duckdb": "CASE WHEN foo = 0 THEN NULL ELSE foo END",
            },
        )
        self.validate_all(
            "SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx",
            read={
                "duckdb": "SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx",
            },
            write={
                "snowflake": "SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx",
                "duckdb": "SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx",
            },
        )
        self.validate_all(
            '''SELECT PARSE_JSON('{"a": {"b c": "foo"}}'):a:"b c"''',
            write={
                "duckdb": """SELECT JSON('{"a": {"b c": "foo"}}') -> '$.a."b c"'""",
                "mysql": """SELECT JSON_EXTRACT('{"a": {"b c": "foo"}}', '$.a."b c"')""",
                "snowflake": """SELECT GET_PATH(PARSE_JSON('{"a": {"b c": "foo"}}'), 'a["b c"]')""",
            },
        )
        self.validate_all(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            write={
                "bigquery": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS LAST LIMIT 10",
                "snowflake": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            },
        )
        self.validate_all(
            "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            write={
                "bigquery": "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z NULLS LAST) = 1",
                "snowflake": "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(col, 'DD-MM-YYYY HH12:MI:SS') FROM t",
            write={
                "bigquery": "SELECT PARSE_TIMESTAMP('%d-%m-%Y %I:%M:%S', col) FROM t",
                "duckdb": "SELECT STRPTIME(col, '%d-%m-%Y %I:%M:%S') FROM t",
                "snowflake": "SELECT TO_TIMESTAMP(col, 'DD-mm-yyyy hh12:mi:ss') FROM t",
                "spark": "SELECT TO_TIMESTAMP(col, 'dd-MM-yyyy hh:mm:ss') FROM t",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729)",
            write={
                "bigquery": "SELECT TIMESTAMP_SECONDS(1659981729)",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729)",
                "spark": "SELECT CAST(FROM_UNIXTIME(1659981729) AS TIMESTAMP)",
                "redshift": "SELECT (TIMESTAMP 'epoch' + 1659981729 * INTERVAL '1 SECOND')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            write={
                "bigquery": "SELECT TIMESTAMP_MILLIS(1659981729000)",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000, 3)",
                "spark": "SELECT TIMESTAMP_MILLIS(1659981729000)",
                "redshift": "SELECT (TIMESTAMP 'epoch' + (1659981729000 / POWER(10, 3)) * INTERVAL '1 SECOND')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(16599817290000, 4)",
            write={
                "bigquery": "SELECT TIMESTAMP_SECONDS(CAST(16599817290000 / POWER(10, 4) AS INT64))",
                "snowflake": "SELECT TO_TIMESTAMP(16599817290000, 4)",
                "spark": "SELECT TIMESTAMP_SECONDS(16599817290000 / POWER(10, 4))",
                "redshift": "SELECT (TIMESTAMP 'epoch' + (16599817290000 / POWER(10, 4)) * INTERVAL '1 SECOND')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('1659981729')",
            write={
                "snowflake": "SELECT TO_TIMESTAMP('1659981729')",
                "spark": "SELECT CAST(FROM_UNIXTIME('1659981729') AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
            write={
                "bigquery": "SELECT TIMESTAMP_SECONDS(CAST(1659981729000000000 / POWER(10, 9) AS INT64))",
                "duckdb": "SELECT TO_TIMESTAMP(1659981729000000000 / POWER(10, 9)) AT TIME ZONE 'UTC'",
                "presto": "SELECT FROM_UNIXTIME(CAST(1659981729000000000 AS DOUBLE) / POW(10, 9))",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
                "spark": "SELECT TIMESTAMP_SECONDS(1659981729000000000 / POWER(10, 9))",
                "redshift": "SELECT (TIMESTAMP 'epoch' + (1659981729000000000 / POWER(10, 9)) * INTERVAL '1 SECOND')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2013-04-05 01:02:03')",
            write={
                "bigquery": "SELECT CAST('2013-04-05 01:02:03' AS DATETIME)",
                "snowflake": "SELECT CAST('2013-04-05 01:02:03' AS TIMESTAMP)",
                "spark": "SELECT CAST('2013-04-05 01:02:03' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/DD/yyyy hh24:mi:ss')",
            read={
                "bigquery": "SELECT PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', '04/05/2013 01:02:03')",
                "duckdb": "SELECT STRPTIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S')",
            },
            write={
                "bigquery": "SELECT PARSE_TIMESTAMP('%m/%d/%Y %T', '04/05/2013 01:02:03')",
                "snowflake": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/DD/yyyy hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'MM/dd/yyyy HH:mm:ss')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP('2024-01-15 3:00 AM', 'YYYY-MM-DD HH12:MI PM')",
            write={
                "duckdb": "STRPTIME('2024-01-15 3:00 AM', '%Y-%m-%d %I:%M %p')",
                "snowflake": "TO_TIMESTAMP('2024-01-15 3:00 AM', 'yyyy-mm-DD hh12:mi pm')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP('2024-01-15 3:00 PM', 'YYYY-MM-DD HH12:MI AM')",
            write={
                "duckdb": "STRPTIME('2024-01-15 3:00 PM', '%Y-%m-%d %I:%M %p')",
                "snowflake": "TO_TIMESTAMP('2024-01-15 3:00 PM', 'yyyy-mm-DD hh12:mi pm')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP('2024-01-15 3:00 PM', 'YYYY-MM-DD HH12:MI PM')",
            write={
                "duckdb": "STRPTIME('2024-01-15 3:00 PM', '%Y-%m-%d %I:%M %p')",
                "snowflake": "TO_TIMESTAMP('2024-01-15 3:00 PM', 'yyyy-mm-DD hh12:mi pm')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP('2024-01-15 3:00 AM', 'YYYY-MM-DD HH12:MI AM')",
            write={
                "duckdb": "STRPTIME('2024-01-15 3:00 AM', '%Y-%m-%d %I:%M %p')",
                "snowflake": "TO_TIMESTAMP('2024-01-15 3:00 AM', 'yyyy-mm-DD hh12:mi pm')",
            },
        )
        self.validate_all(
            "SELECT IFF(TRUE, 'true', 'false')",
            write={
                "snowflake": "SELECT IFF(TRUE, 'true', 'false')",
                "spark": "SELECT IF(TRUE, 'true', 'false')",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
                "snowflake": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname",
            },
        )
        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT a)",
            write={
                "spark": "SELECT COLLECT_LIST(DISTINCT a)",
                "snowflake": "SELECT ARRAY_AGG(DISTINCT a)",
                "duckdb": "SELECT ARRAY_AGG(DISTINCT a) FILTER(WHERE a IS NOT NULL)",
                "presto": "SELECT ARRAY_AGG(DISTINCT a) FILTER(WHERE a IS NOT NULL)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_AGG(col) WITHIN GROUP (ORDER BY sort_col)",
            write={
                "snowflake": "SELECT ARRAY_AGG(col) WITHIN GROUP (ORDER BY sort_col)",
                "duckdb": "SELECT ARRAY_AGG(col ORDER BY sort_col) FILTER(WHERE col IS NOT NULL)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT col) WITHIN GROUP (ORDER BY col DESC)",
            write={
                "snowflake": "SELECT ARRAY_AGG(DISTINCT col) WITHIN GROUP (ORDER BY col DESC)",
                "duckdb": "SELECT ARRAY_AGG(DISTINCT col ORDER BY col DESC NULLS FIRST) FILTER(WHERE col IS NOT NULL)",
            },
        )
        self.validate_all(
            "ARRAY_TO_STRING(x, '')",
            read={
                "duckdb": "ARRAY_TO_STRING(x, '')",
            },
            write={
                "spark": "ARRAY_JOIN(x, '')",
                "snowflake": "ARRAY_TO_STRING(x, '')",
                "duckdb": "ARRAY_TO_STRING(x, '')",
            },
        )
        self.validate_all(
            "TO_ARRAY(x)",
            write={
                "spark": "IF(x IS NULL, NULL, ARRAY(x))",
                "snowflake": "TO_ARRAY(x)",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            write={
                "snowflake": UnsupportedError,
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            write={
                "snowflake": UnsupportedError,
            },
        )
        self.validate_all(
            "SELECT ARRAY_UNION_AGG(a)",
            write={
                "snowflake": "SELECT ARRAY_UNION_AGG(a)",
            },
        )
        self.validate_all(
            "SELECT $$a$$",
            write={
                "snowflake": "SELECT 'a'",
            },
        )
        self.validate_all(
            "SELECT RLIKE(a, b)",
            write={
                "hive": "SELECT a RLIKE b",
                "snowflake": "SELECT REGEXP_LIKE(a, b)",
                "spark": "SELECT a RLIKE b",
            },
        )
        self.validate_all(
            "'foo' REGEXP 'bar'",
            write={
                "snowflake": "REGEXP_LIKE('foo', 'bar')",
                "postgres": "'foo' ~ 'bar'",
                "mysql": "REGEXP_LIKE('foo', 'bar')",
                "bigquery": "REGEXP_CONTAINS('foo', 'bar')",
            },
        )
        self.validate_all(
            "'foo' NOT REGEXP 'bar'",
            write={
                "snowflake": "NOT REGEXP_LIKE('foo', 'bar')",
                "postgres": "NOT 'foo' ~ 'bar'",
                "mysql": "NOT REGEXP_LIKE('foo', 'bar')",
                "bigquery": "NOT REGEXP_CONTAINS('foo', 'bar')",
            },
        )
        self.validate_all(
            "SELECT a FROM test pivot",
            write={
                "snowflake": "SELECT a FROM test AS pivot",
            },
        )
        self.validate_all(
            "SELECT a FROM test unpivot",
            write={
                "snowflake": "SELECT a FROM test AS unpivot",
            },
        )
        self.validate_all(
            "trim(date_column, 'UTC')",
            write={
                "bigquery": "TRIM(date_column, 'UTC')",
                "snowflake": "TRIM(date_column, 'UTC')",
                "postgres": "TRIM('UTC' FROM date_column)",
            },
        )
        self.validate_all(
            "trim(date_column)",
            write={
                "snowflake": "TRIM(date_column)",
                "bigquery": "TRIM(date_column)",
            },
        )
        self.validate_all(
            "DECODE(x, a, b, c, d, e)",
            write={
                "duckdb": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d ELSE e END",
                "snowflake": "DECODE(x, a, b, c, d, e)",
            },
        )
        self.validate_all(
            "DECODE(TRUE, a.b = 'value', 'value')",
            write={
                "duckdb": "CASE WHEN TRUE = (a.b = 'value') OR (TRUE IS NULL AND (a.b = 'value') IS NULL) THEN 'value' END",
                "snowflake": "DECODE(TRUE, a.b = 'value', 'value')",
            },
        )
        self.validate_all(
            "SELECT BOOLAND(1, -2)",
            read={
                "snowflake": "SELECT BOOLAND(1, -2)",
            },
            write={
                "snowflake": "SELECT BOOLAND(1, -2)",
                "duckdb": "SELECT ((ROUND(1, 0)) AND (ROUND(-2, 0)))",
            },
        )
        self.validate_all(
            "SELECT BOOLOR(1, 0)",
            write={
                "snowflake": "SELECT BOOLOR(1, 0)",
                "duckdb": "SELECT ((ROUND(1, 0)) OR (ROUND(0, 0)))",
            },
        )
        self.validate_all(
            "SELECT BOOLXOR(2, 0.3)",
            read={
                "snowflake": "SELECT BOOLXOR(2, 0.3)",
            },
            write={
                "snowflake": "SELECT BOOLXOR(2, 0.3)",
                "duckdb": "SELECT (ROUND(2, 0) AND (NOT ROUND(0.3, 0))) OR ((NOT ROUND(2, 0)) AND ROUND(0.3, 0))",
            },
        )
        self.validate_all(
            "SELECT APPROX_PERCENTILE(a, 0.5) FROM t",
            read={
                "trino": "SELECT APPROX_PERCENTILE(a, 1, 0.5, 0.001) FROM t",
                "presto": "SELECT APPROX_PERCENTILE(a, 1, 0.5, 0.001) FROM t",
            },
            write={
                "trino": "SELECT APPROX_PERCENTILE(a, 0.5) FROM t",
                "presto": "SELECT APPROX_PERCENTILE(a, 0.5) FROM t",
                "snowflake": "SELECT APPROX_PERCENTILE(a, 0.5) FROM t",
            },
        )

        self.validate_all(
            "SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT('key5', 'value5'), 'key1', 5), 'key2', 2.2), 'key3', 'value3')",
            write={
                "snowflake": "SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT('key5', 'value5'), 'key1', 5), 'key2', 2.2), 'key3', 'value3')",
                "duckdb": "SELECT STRUCT_INSERT(STRUCT_INSERT(STRUCT_INSERT({'key5': 'value5'}, key1 := 5), key2 := 2.2), key3 := 'value3')",
            },
        )

        self.validate_all(
            "SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), 'key1', 5), 'key2', 2.2), 'key3', 'value3')",
            write={
                "snowflake": "SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), 'key1', 5), 'key2', 2.2), 'key3', 'value3')",
                "duckdb": "SELECT STRUCT_INSERT(STRUCT_INSERT(STRUCT_PACK(key1 := 5), key2 := 2.2), key3 := 'value3')",
            },
        )

        self.validate_identity(
            """SELECT ARRAY_CONSTRUCT('foo')::VARIANT[0]""",
            """SELECT CAST(['foo'] AS VARIANT)[0]""",
        )

        self.validate_all(
            "SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')",
            write={
                "snowflake": "SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')",
                "spark": "SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')",
                "databricks": "SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')",
                "redshift": "SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')",
            },
        )

        self.validate_all(
            "SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000')",
            write={
                "snowflake": "SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000')",
                "spark": "SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000')",
                "databricks": "SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000')",
                "redshift": "SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000')",
                "mysql": "SELECT CONVERT_TZ('2024-08-06 09:10:00.000', 'America/Los_Angeles', 'America/New_York')",
                "duckdb": "SELECT CAST('2024-08-06 09:10:00.000' AS TIMESTAMP) AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'America/New_York'",
            },
        )

        self.validate_identity(
            "SELECT UUID_STRING(), UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', 'foo')"
        )

        self.validate_all(
            "UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', 'foo')",
            read={
                "snowflake": "UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', 'foo')",
            },
            write={
                "hive": "UUID()",
                "spark2": "UUID()",
                "spark": "UUID()",
                "databricks": "UUID()",
                "duckdb": "UUID()",
                "presto": "UUID()",
                "trino": "UUID()",
                "postgres": "GEN_RANDOM_UUID()",
                "bigquery": "GENERATE_UUID()",
            },
        )
        self.validate_identity("TRY_TO_TIMESTAMP(foo)").assert_is(exp.Anonymous)
        self.validate_identity("TRY_TO_TIMESTAMP('12345')").assert_is(exp.Anonymous)
        self.validate_all(
            "SELECT TRY_TO_TIMESTAMP('2024-01-15 12:30:00.000')",
            write={
                "snowflake": "SELECT TRY_CAST('2024-01-15 12:30:00.000' AS TIMESTAMP)",
                "duckdb": "SELECT TRY_CAST('2024-01-15 12:30:00.000' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TRY_TO_TIMESTAMP('invalid')",
            write={
                "snowflake": "SELECT TRY_CAST('invalid' AS TIMESTAMP)",
                "duckdb": "SELECT TRY_CAST('invalid' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TRY_TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/DD/yyyy hh24:mi:ss')",
            write={
                "snowflake": "SELECT TRY_TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/DD/yyyy hh24:mi:ss')",
                "duckdb": "SELECT CAST(TRY_STRPTIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S') AS TIMESTAMP)",
            },
        )

        self.validate_all(
            "EDITDISTANCE(col1, col2)",
            write={
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "snowflake": "EDITDISTANCE(col1, col2)",
            },
        )
        self.validate_all(
            "EDITDISTANCE(col1, col2, 3)",
            write={
                "bigquery": "EDIT_DISTANCE(col1, col2, max_distance => 3)",
                "duckdb": "CASE WHEN LEVENSHTEIN(col1, col2) IS NULL OR 3 IS NULL THEN NULL ELSE LEAST(LEVENSHTEIN(col1, col2), 3) END",
                "postgres": "LEVENSHTEIN_LESS_EQUAL(col1, col2, 3)",
                "snowflake": "EDITDISTANCE(col1, col2, 3)",
            },
        )
        self.validate_identity("SELECT BITNOT(a)")
        self.validate_identity("SELECT BIT_NOT(a)", "SELECT BITNOT(a)")
        self.validate_all(
            "SELECT BITNOT(-1)",
            write={
                "duckdb": "SELECT ~(-1)",
                "snowflake": "SELECT BITNOT(-1)",
            },
        )
        self.validate_identity("SELECT BITAND(a, b)")
        self.validate_identity("SELECT BITAND(a, b, 'LEFT')")
        self.validate_identity("SELECT BIT_AND(a, b)", "SELECT BITAND(a, b)")
        self.validate_identity("SELECT BIT_AND(a, b, 'LEFT')", "SELECT BITAND(a, b, 'LEFT')")
        self.validate_identity("SELECT BITOR(a, b)")
        self.validate_identity("SELECT BITOR(a, b, 'LEFT')")
        self.validate_identity("SELECT BIT_OR(a, b)", "SELECT BITOR(a, b)")
        self.validate_identity("SELECT BIT_OR(a, b, 'RIGHT')", "SELECT BITOR(a, b, 'RIGHT')")
        self.validate_identity("SELECT BITXOR(a, b)")
        self.validate_identity("SELECT BITXOR(a, b, 'LEFT')")
        self.validate_identity("SELECT BIT_XOR(a, b)", "SELECT BITXOR(a, b)")
        self.validate_identity("SELECT BIT_XOR(a, b, 'LEFT')", "SELECT BITXOR(a, b, 'LEFT')")

        # duckdb has an order of operations precedence issue with bitshift and bitwise operators
        self.validate_all(
            "SELECT BITOR(BITSHIFTLEFT(5, 16), BITSHIFTLEFT(3, 8))",
            write={"duckdb": "SELECT (CAST(5 AS INT128) << 16) | (CAST(3 AS INT128) << 8)"},
        )
        self.validate_all(
            "SELECT BITAND(BITSHIFTLEFT(255, 4), BITSHIFTLEFT(15, 2))",
            write={
                "snowflake": "SELECT BITAND(BITSHIFTLEFT(255, 4), BITSHIFTLEFT(15, 2))",
                "duckdb": "SELECT (CAST(255 AS INT128) << 4) & (CAST(15 AS INT128) << 2)",
            },
        )
        self.validate_all(
            "SELECT BITSHIFTLEFT(255, 4)",
            write={
                "snowflake": "SELECT BITSHIFTLEFT(255, 4)",
                "duckdb": "SELECT CAST(255 AS INT128) << 4",
            },
        )
        self.validate_all(
            "SELECT BITSHIFTRIGHT(255, 4)",
            write={
                "snowflake": "SELECT BITSHIFTRIGHT(255, 4)",
                "duckdb": "SELECT CAST(255 AS INT128) >> 4",
            },
        )
        self.validate_all(
            "SELECT BITSHIFTLEFT(X'002A'::BINARY, 1)",
            write={
                "snowflake": "SELECT BITSHIFTLEFT(CAST(x'002A' AS BINARY), 1)",
                "duckdb": "SELECT CAST(CAST(CAST(UNHEX('002A') AS BLOB) AS BIT) << 1 AS BLOB)",
            },
        )
        self.validate_all(
            "SELECT BITSHIFTRIGHT(X'002A'::BINARY, 1)",
            write={
                "snowflake": "SELECT BITSHIFTRIGHT(CAST(x'002A' AS BINARY), 1)",
                "duckdb": "SELECT CAST(CAST(CAST(UNHEX('002A') AS BLOB) AS BIT) >> 1 AS BLOB)",
            },
        )

        self.validate_all(
            "OCTET_LENGTH('A')",
            read={
                "bigquery": "BYTE_LENGTH('A')",
                "snowflake": "OCTET_LENGTH('A')",
            },
        )

        self.validate_identity("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT)")

        self.validate_all(
            "SELECT HEX_DECODE_BINARY('65')",
            write={
                "bigquery": "SELECT FROM_HEX('65')",
                "duckdb": "SELECT UNHEX('65')",
                "snowflake": "SELECT HEX_DECODE_BINARY('65')",
            },
        )

        self.validate_all(
            "DAYOFWEEKISO(foo)",
            read={
                "snowflake": "DAYOFWEEKISO(foo)",
                "presto": "DAY_OF_WEEK(foo)",
                "trino": "DAY_OF_WEEK(foo)",
            },
            write={
                "duckdb": "ISODOW(foo)",
            },
        )

        self.validate_all(
            "DAYOFWEEKISO(foo)",
            read={
                "presto": "DOW(foo)",
                "trino": "DOW(foo)",
            },
        )

        self.validate_all(
            "DAYOFYEAR(foo)",
            read={
                "presto": "DOY(foo)",
                "trino": "DOY(foo)",
            },
            write={
                "snowflake": "DAYOFYEAR(foo)",
            },
        )

        self.validate_identity("TO_JSON(OBJECT_CONSTRUCT('name', 'Alice'))")

        with self.assertRaises(ParseError):
            parse_one(
                "SELECT id, PRIOR name AS parent_name, name FROM tree CONNECT BY NOCYCLE PRIOR id = parent_id",
                dialect="snowflake",
            )

        self.validate_all(
            "SELECT CAST(1 AS DOUBLE), CAST(1 AS DOUBLE)",
            read={
                "bigquery": "SELECT CAST(1 AS BIGDECIMAL), CAST(1 AS BIGNUMERIC)",
            },
            write={
                "snowflake": "SELECT CAST(1 AS DOUBLE), CAST(1 AS DOUBLE)",
            },
        )

        self.validate_all(
            "SELECT DATE_PART(WEEKISO, CAST('2013-12-25' AS DATE))",
            read={
                "bigquery": "SELECT EXTRACT(ISOWEEK FROM CAST('2013-12-25' AS DATE))",
                "snowflake": "SELECT DATE_PART(WEEKISO, CAST('2013-12-25' AS DATE))",
            },
            write={
                "duckdb": "SELECT CAST(STRFTIME(CAST('2013-12-25' AS DATE), '%V') AS INT)",
            },
        )
        # DATE_PART/EXTRACT with specifiers not supported in DuckDB
        self.validate_all(
            "SELECT DATE_PART(YEAROFWEEK, CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEK, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06' AS DATE), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(YEAROFWEEKISO, CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEKISO, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06' AS DATE), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(NANOSECOND, CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMPNTZ))",
            write={
                "snowflake": "SELECT DATE_PART(NANOSECOND, CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(STRFTIME(CAST(CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMP) AS TIMESTAMP_NS), '%n') AS BIGINT)",
            },
        )
        # TIMESTAMP_NTZ tests - using NTZ for consistent behavior across timezones
        self.validate_all(
            "SELECT EXTRACT(YEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(YEAR, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(YEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(QUARTER FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(QUARTER, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(QUARTER FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(MONTH FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(MONTH, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(MONTH FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(WEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(WEEK, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(WEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(WEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(WEEKISO, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06 11:45:00' AS TIMESTAMP), '%V') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAY FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(DAY, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(DAY FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFMONTH FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(DAY, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(DAY FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFWEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEK, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(DAYOFWEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFWEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEKISO, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(ISODOW FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFYEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFYEAR, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(DAYOFYEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(YEAROFWEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEK, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06 11:45:00' AS TIMESTAMP), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(YEAROFWEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEKISO, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06 11:45:00' AS TIMESTAMP), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(HOUR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(HOUR, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(HOUR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(MINUTE FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(MINUTE, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(MINUTE FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(SECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(SECOND, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EXTRACT(SECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(NANOSECOND FROM CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(NANOSECOND, CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(STRFTIME(CAST(CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMP) AS TIMESTAMP_NS), '%n') AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(EPOCH_SECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_SECOND, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT CAST(EPOCH(CAST('2026-01-06 11:45:00' AS TIMESTAMP)) AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(EPOCH_MILLISECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_MILLISECOND, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EPOCH_MS(CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(EPOCH_MICROSECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_MICROSECOND, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EPOCH_US(CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(EPOCH_NANOSECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ))",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_NANOSECOND, CAST('2026-01-06 11:45:00' AS TIMESTAMPNTZ))",
                "duckdb": "SELECT EPOCH_NS(CAST('2026-01-06 11:45:00' AS TIMESTAMP))",
            },
        )
        # EXTRACT from DATE - exhaustive tests
        self.validate_all(
            "SELECT EXTRACT(YEAR FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(YEAR, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(YEAR FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(QUARTER FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(QUARTER, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(QUARTER FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(MONTH FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(MONTH, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(MONTH FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(WEEK FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(WEEK, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(WEEK FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(WEEKISO FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(WEEKISO, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06' AS DATE), '%V') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAY FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(DAY, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(DAY FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFMONTH FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(DAY, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(DAY FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFWEEK FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEK, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(DAYOFWEEK FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFWEEKISO FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEKISO, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(ISODOW FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(DAYOFYEAR FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFYEAR, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT EXTRACT(DAYOFYEAR FROM CAST('2026-01-06' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(YEAROFWEEK FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEK, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06' AS DATE), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(YEAROFWEEKISO FROM CAST('2026-01-06' AS DATE))",
            write={
                "snowflake": "SELECT DATE_PART(YEAROFWEEKISO, CAST('2026-01-06' AS DATE))",
                "duckdb": "SELECT CAST(STRFTIME(CAST('2026-01-06' AS DATE), '%G') AS INT)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(HOUR FROM CAST('11:45:00.123456789' AS TIME))",
            write={
                "snowflake": "SELECT DATE_PART(HOUR, CAST('11:45:00.123456789' AS TIME))",
                "duckdb": "SELECT EXTRACT(HOUR FROM CAST('11:45:00.123456789' AS TIME))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(MINUTE FROM CAST('11:45:00.123456789' AS TIME))",
            write={
                "snowflake": "SELECT DATE_PART(MINUTE, CAST('11:45:00.123456789' AS TIME))",
                "duckdb": "SELECT EXTRACT(MINUTE FROM CAST('11:45:00.123456789' AS TIME))",
            },
        )
        self.validate_all(
            "SELECT EXTRACT(SECOND FROM CAST('11:45:00.123456789' AS TIME))",
            write={
                "snowflake": "SELECT DATE_PART(SECOND, CAST('11:45:00.123456789' AS TIME))",
                "duckdb": "SELECT EXTRACT(SECOND FROM CAST('11:45:00.123456789' AS TIME))",
            },
        )

        self.validate_all(
            "SELECT ST_MAKEPOINT(10, 20)",
            write={
                "snowflake": "SELECT ST_MAKEPOINT(10, 20)",
                "starrocks": "SELECT ST_POINT(10, 20)",
            },
        )

        self.validate_all(
            "LAST_DAY(CAST('2023-04-15' AS DATE))",
            write={
                "snowflake": "LAST_DAY(CAST('2023-04-15' AS DATE))",
                "duckdb": "LAST_DAY(CAST('2023-04-15' AS DATE))",
            },
        )

        self.validate_all(
            "LAST_DAY(CAST('2023-04-15' AS DATE), MONTH)",
            write={
                "snowflake": "LAST_DAY(CAST('2023-04-15' AS DATE), MONTH)",
                "duckdb": "LAST_DAY(CAST('2023-04-15' AS DATE))",
            },
        )

        self.validate_all(
            "LAST_DAY(CAST('2024-06-15' AS DATE), YEAR)",
            write={
                "snowflake": "LAST_DAY(CAST('2024-06-15' AS DATE), YEAR)",
                "duckdb": "MAKE_DATE(EXTRACT(YEAR FROM CAST('2024-06-15' AS DATE)), 12, 31)",
            },
        )

        self.validate_all(
            "LAST_DAY(CAST('2024-01-15' AS DATE), QUARTER)",
            write={
                "snowflake": "LAST_DAY(CAST('2024-01-15' AS DATE), QUARTER)",
                "duckdb": "LAST_DAY(MAKE_DATE(EXTRACT(YEAR FROM CAST('2024-01-15' AS DATE)), EXTRACT(QUARTER FROM CAST('2024-01-15' AS DATE)) * 3, 1))",
            },
        )

        self.validate_all(
            "LAST_DAY(CAST('2025-12-15' AS DATE), WEEK)",
            write={
                "snowflake": "LAST_DAY(CAST('2025-12-15' AS DATE), WEEK)",
                "duckdb": "CAST(CAST('2025-12-15' AS DATE) + INTERVAL ((7 - EXTRACT(DAYOFWEEK FROM CAST('2025-12-15' AS DATE))) % 7) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT ST_DISTANCE(a, b)",
            write={
                "snowflake": "SELECT ST_DISTANCE(a, b)",
                "starrocks": "SELECT ST_DISTANCE_SPHERE(ST_X(a), ST_Y(a), ST_X(b), ST_Y(b))",
            },
        )

        self.validate_all(
            "SELECT DATE_PART(DAYOFWEEKISO, foo)",
            read={
                "snowflake": "SELECT DATE_PART(WEEKDAY_ISO, foo)",
            },
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEKISO, foo)",
                "duckdb": "SELECT EXTRACT(ISODOW FROM foo)",
            },
        )

        self.validate_all(
            "SELECT DATE_PART(DAYOFWEEK_ISO, foo)",
            write={
                "snowflake": "SELECT DATE_PART(DAYOFWEEKISO, foo)",
                "duckdb": "SELECT EXTRACT(ISODOW FROM foo)",
            },
        )
        self.validate_identity("ALTER TABLE foo ADD col1 VARCHAR(512), col2 VARCHAR(512)")
        self.validate_identity(
            "ALTER TABLE foo ADD col1 VARCHAR NOT NULL TAG (key1='value_1'), col2 VARCHAR NOT NULL TAG (key2='value_2')"
        )
        self.validate_identity("ALTER TABLE foo ADD IF NOT EXISTS col1 INT, col2 INT")
        self.validate_identity("ALTER TABLE foo ADD IF NOT EXISTS col1 INT, IF NOT EXISTS col2 INT")
        self.validate_identity("ALTER TABLE foo ADD col1 INT, IF NOT EXISTS col2 INT")
        self.validate_identity("ALTER TABLE IF EXISTS foo ADD IF NOT EXISTS col1 INT")
        # ADD_MONTHS - Basic integer months with type preservation
        self.validate_all(
            "SELECT ADD_MONTHS('2023-01-31', 1)",
            write={
                "duckdb": "SELECT CASE WHEN LAST_DAY(CAST('2023-01-31' AS TIMESTAMP)) = CAST('2023-01-31' AS TIMESTAMP) THEN LAST_DAY(CAST('2023-01-31' AS TIMESTAMP) + INTERVAL 1 MONTH) ELSE CAST('2023-01-31' AS TIMESTAMP) + INTERVAL 1 MONTH END",
                "snowflake": "SELECT ADD_MONTHS('2023-01-31', 1)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2023-01-31'::date, 1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2023-01-31' AS DATE)) = CAST('2023-01-31' AS DATE) THEN LAST_DAY(CAST('2023-01-31' AS DATE) + INTERVAL 1 MONTH) ELSE CAST('2023-01-31' AS DATE) + INTERVAL 1 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2023-01-31' AS DATE), 1)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2023-01-31'::timestamptz, 1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2023-01-31' AS TIMESTAMPTZ)) = CAST('2023-01-31' AS TIMESTAMPTZ) THEN LAST_DAY(CAST('2023-01-31' AS TIMESTAMPTZ) + INTERVAL 1 MONTH) ELSE CAST('2023-01-31' AS TIMESTAMPTZ) + INTERVAL 1 MONTH END AS TIMESTAMPTZ)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2023-01-31' AS TIMESTAMPTZ), 1)",
            },
        )

        # ADD_MONTHS - Float month values (rounded to integer)
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, 2.7)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(2.7) AS INT))) ELSE CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(2.7) AS INT)) END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), 2.7)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, -2.3)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(-2.3) AS INT))) ELSE CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(-2.3) AS INT)) END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), -2.3)",
            },
        )

        # ADD_MONTHS - Decimal month values (rounded to integer)
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, 3.2::DECIMAL(10,2))",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(CAST(3.2 AS DECIMAL(10, 2))) AS INT))) ELSE CAST('2016-05-15' AS DATE) + TO_MONTHS(CAST(ROUND(CAST(3.2 AS DECIMAL(10, 2))) AS INT)) END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), CAST(3.2 AS DECIMAL(10, 2)))",
            },
        )

        # ADD_MONTHS - End-of-month preservation (Snowflake semantic)
        self.validate_all(
            "SELECT ADD_MONTHS('2016-02-29'::DATE, 1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-02-29' AS DATE)) = CAST('2016-02-29' AS DATE) THEN LAST_DAY(CAST('2016-02-29' AS DATE) + INTERVAL 1 MONTH) ELSE CAST('2016-02-29' AS DATE) + INTERVAL 1 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-02-29' AS DATE), 1)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-31'::DATE, 1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-31' AS DATE)) = CAST('2016-05-31' AS DATE) THEN LAST_DAY(CAST('2016-05-31' AS DATE) + INTERVAL 1 MONTH) ELSE CAST('2016-05-31' AS DATE) + INTERVAL 1 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-31' AS DATE), 1)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-31'::DATE, -1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-31' AS DATE)) = CAST('2016-05-31' AS DATE) THEN LAST_DAY(CAST('2016-05-31' AS DATE) + INTERVAL (-1) MONTH) ELSE CAST('2016-05-31' AS DATE) + INTERVAL (-1) MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-31' AS DATE), -1)",
            },
        )

        # ADD_MONTHS - Mid-month dates (end-of-month logic should not trigger)
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, 1)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + INTERVAL 1 MONTH) ELSE CAST('2016-05-15' AS DATE) + INTERVAL 1 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), 1)",
            },
        )

        # ADD_MONTHS - NULL handling
        self.validate_all(
            "SELECT ADD_MONTHS(NULL::DATE, 2)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST(NULL AS DATE)) = CAST(NULL AS DATE) THEN LAST_DAY(CAST(NULL AS DATE) + INTERVAL 2 MONTH) ELSE CAST(NULL AS DATE) + INTERVAL 2 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST(NULL AS DATE), 2)",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, NULL)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + INTERVAL (NULL) MONTH) ELSE CAST('2016-05-15' AS DATE) + INTERVAL (NULL) MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), NULL)",
            },
        )

        # ADD_MONTHS - Zero months
        self.validate_all(
            "SELECT ADD_MONTHS('2016-05-15'::DATE, 0)",
            write={
                "duckdb": "SELECT CAST(CASE WHEN LAST_DAY(CAST('2016-05-15' AS DATE)) = CAST('2016-05-15' AS DATE) THEN LAST_DAY(CAST('2016-05-15' AS DATE) + INTERVAL 0 MONTH) ELSE CAST('2016-05-15' AS DATE) + INTERVAL 0 MONTH END AS DATE)",
                "snowflake": "SELECT ADD_MONTHS(CAST('2016-05-15' AS DATE), 0)",
            },
        )

        self.validate_identity("SELECT HOUR(CAST('08:50:57' AS TIME))")
        self.validate_identity("SELECT MINUTE(CAST('08:50:57' AS TIME))")
        self.validate_identity("SELECT SECOND(CAST('08:50:57' AS TIME))")
        self.validate_identity("SELECT HOUR(CAST('2024-05-09 08:50:57' AS TIMESTAMP))")
        self.validate_identity("SELECT MONTHNAME(CAST('2024-05-09' AS DATE))")
        self.validate_all(
            "SELECT DAYNAME(TO_DATE('2025-01-15'))",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2025-01-15' AS DATE), '%a')",
                "snowflake": "SELECT DAYNAME(CAST('2025-01-15' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT DAYNAME(TO_TIMESTAMP('2025-02-28 10:30:45'))",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2025-02-28 10:30:45' AS TIMESTAMP), '%a')",
                "snowflake": "SELECT DAYNAME(CAST('2025-02-28 10:30:45' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT MONTHNAME(TO_DATE('2025-01-15'))",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2025-01-15' AS DATE), '%b')",
                "snowflake": "SELECT MONTHNAME(CAST('2025-01-15' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT MONTHNAME(TO_TIMESTAMP('2025-02-28 10:30:45'))",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2025-02-28 10:30:45' AS TIMESTAMP), '%b')",
                "snowflake": "SELECT MONTHNAME(CAST('2025-02-28 10:30:45' AS TIMESTAMP))",
            },
        )
        self.validate_identity("SELECT PREVIOUS_DAY(CAST('2024-05-09' AS DATE), 'MONDAY')")
        self.validate_identity("SELECT TIME_FROM_PARTS(14, 30, 45)")
        self.validate_identity("SELECT TIME_FROM_PARTS(14, 30, 45, 123)")

        self.validate_identity(
            "SELECT MONTHS_BETWEEN(CAST('2019-03-15' AS DATE), CAST('2019-02-15' AS DATE))"
        )
        self.validate_identity(
            "SELECT MONTHS_BETWEEN(CAST('2019-03-01 02:00:00' AS TIMESTAMP), CAST('2019-02-15 01:00:00' AS TIMESTAMP))"
        )

        self.validate_identity(
            "SELECT TIME_SLICE(CAST('2024-05-09 08:50:57.891' AS TIMESTAMP), 15, 'MINUTE')"
        )
        self.validate_identity("SELECT TIME_SLICE(CAST('2024-05-09' AS DATE), 1, 'DAY')")
        self.validate_identity(
            "SELECT TIME_SLICE(CAST('2024-05-09 08:50:57.891' AS TIMESTAMP), 1, 'HOUR', 'start')"
        )

        # TIME_SLICE transpilation to DuckDB
        self.validate_all(
            "SELECT TIME_SLICE(TIMESTAMP '2024-03-15 14:37:42', 1, 'HOUR')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15 14:37:42' AS TIMESTAMP), 1, 'HOUR')",
                "duckdb": "SELECT TIME_BUCKET(INTERVAL 1 HOUR, CAST('2024-03-15 14:37:42' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(TIMESTAMP '2024-03-15 14:37:42', 1, 'HOUR', 'END')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15 14:37:42' AS TIMESTAMP), 1, 'HOUR', 'END')",
                "duckdb": "SELECT TIME_BUCKET(INTERVAL 1 HOUR, CAST('2024-03-15 14:37:42' AS TIMESTAMP)) + INTERVAL 1 HOUR",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(DATE '2024-03-15', 1, 'DAY')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15' AS DATE), 1, 'DAY')",
                "duckdb": "SELECT TIME_BUCKET(INTERVAL 1 DAY, CAST('2024-03-15' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(DATE '2024-03-15', 1, 'DAY', 'END')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15' AS DATE), 1, 'DAY', 'END')",
                "duckdb": "SELECT CAST(TIME_BUCKET(INTERVAL 1 DAY, CAST('2024-03-15' AS DATE)) + INTERVAL 1 DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(TIMESTAMP '2024-03-15 14:37:42', 15, 'MINUTE')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15 14:37:42' AS TIMESTAMP), 15, 'MINUTE')",
                "duckdb": "SELECT TIME_BUCKET(INTERVAL 15 MINUTE, CAST('2024-03-15 14:37:42' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(TIMESTAMP '2024-03-15 14:37:42', 1, 'QUARTER')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15 14:37:42' AS TIMESTAMP), 1, 'QUARTER')",
                "duckdb": "SELECT TIME_BUCKET(INTERVAL 1 QUARTER, CAST('2024-03-15 14:37:42' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT TIME_SLICE(DATE '2024-03-15', 1, 'WEEK', 'END')",
            write={
                "snowflake": "SELECT TIME_SLICE(CAST('2024-03-15' AS DATE), 1, 'WEEK', 'END')",
                "duckdb": "SELECT CAST(TIME_BUCKET(INTERVAL 1 WEEK, CAST('2024-03-15' AS DATE)) + INTERVAL 1 WEEK AS DATE)",
            },
        )

        for join in ("FULL OUTER", "LEFT", "RIGHT", "LEFT OUTER", "RIGHT OUTER", "INNER"):
            with self.subTest(f"Testing transpilation of {join} from Snowflake to DuckDB"):
                self.validate_all(
                    f"SELECT * FROM t1 {join} JOIN t2",
                    read={
                        "snowflake": f"SELECT * FROM t1 {join} JOIN t2",
                    },
                    write={
                        "duckdb": "SELECT * FROM t1, t2",
                    },
                )

        self.validate_identity(
            "SELECT * EXCLUDE foo RENAME bar AS baz FROM tbl",
            "SELECT * EXCLUDE (foo) RENAME (bar AS baz) FROM tbl",
        )

        self.validate_all(
            "WITH foo AS (SELECT [1] AS arr_1) SELECT (SELECT unnested_arr FROM TABLE(FLATTEN(INPUT => arr_1)) AS _t0(seq, key, path, index, unnested_arr, this)) AS f FROM foo",
            read={
                "bigquery": "WITH foo AS (SELECT [1] AS arr_1) SELECT (SELECT unnested_arr FROM UNNEST(arr_1) AS unnested_arr) AS f FROM foo",
            },
        )

        self.validate_identity("SELECT LIKE(col, 'pattern')", "SELECT col LIKE 'pattern'")
        self.validate_identity("SELECT ILIKE(col, 'pattern')", "SELECT col ILIKE 'pattern'")
        self.validate_identity(
            "SELECT LIKE(col, 'pattern', '\\\\')", "SELECT col LIKE 'pattern' ESCAPE '\\\\'"
        )
        self.validate_identity(
            "SELECT ILIKE(col, 'pattern', '\\\\')", "SELECT col ILIKE 'pattern' ESCAPE '\\\\'"
        )
        self.validate_identity(
            "SELECT LIKE(col, 'pattern', '!')", "SELECT col LIKE 'pattern' ESCAPE '!'"
        )
        self.validate_identity(
            "SELECT ILIKE(col, 'pattern', '!')", "SELECT col ILIKE 'pattern' ESCAPE '!'"
        )

        expr = self.validate_identity("SELECT BASE64_ENCODE('Hello World')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "SELECT TO_BASE64(ENCODE('Hello World'))")
        self.validate_all(
            "SELECT BASE64_ENCODE(x)",
            write={
                "duckdb": "SELECT TO_BASE64(x)",
                "snowflake": "SELECT BASE64_ENCODE(x)",
            },
        )
        self.validate_all(
            "SELECT BASE64_ENCODE(x, 76)",
            write={
                "duckdb": "SELECT RTRIM(REGEXP_REPLACE(TO_BASE64(x), '(.{76})', '\\1' || CHR(10), 'g'), CHR(10))",
                "snowflake": "SELECT BASE64_ENCODE(x, 76)",
            },
        )
        self.validate_all(
            "SELECT BASE64_ENCODE(x, 76, '+/=')",
            write={
                "duckdb": "SELECT RTRIM(REGEXP_REPLACE(TO_BASE64(x), '(.{76})', '\\1' || CHR(10), 'g'), CHR(10))",
                "snowflake": "SELECT BASE64_ENCODE(x, 76, '+/=')",
            },
        )

        self.validate_all(
            "SELECT BASE64_DECODE_STRING('U25vd2ZsYWtl')",
            write={
                "snowflake": "SELECT BASE64_DECODE_STRING('U25vd2ZsYWtl')",
                "duckdb": "SELECT DECODE(FROM_BASE64('U25vd2ZsYWtl'))",
            },
        )
        self.validate_all(
            "SELECT BASE64_DECODE_STRING('U25vd2ZsYWtl', '-_+')",
            write={
                "snowflake": "SELECT BASE64_DECODE_STRING('U25vd2ZsYWtl', '-_+')",
                "duckdb": "SELECT DECODE(FROM_BASE64(REPLACE(REPLACE(REPLACE('U25vd2ZsYWtl', '-', '+'), '_', '/'), '+', '=')))",
            },
        )
        self.validate_all(
            "SELECT BASE64_DECODE_BINARY(x)",
            write={
                "snowflake": "SELECT BASE64_DECODE_BINARY(x)",
                "duckdb": "SELECT FROM_BASE64(x)",
            },
        )
        self.validate_all(
            "SELECT BASE64_DECODE_BINARY(x, '-_+')",
            write={
                "snowflake": "SELECT BASE64_DECODE_BINARY(x, '-_+')",
                "duckdb": "SELECT FROM_BASE64(REPLACE(REPLACE(REPLACE(x, '-', '+'), '_', '/'), '+', '='))",
            },
        )

        self.validate_identity("SELECT TRY_HEX_DECODE_BINARY('48656C6C6F')")

        self.validate_identity("SELECT TRY_HEX_DECODE_STRING('48656C6C6F')")

        self.validate_all(
            "SELECT ARRAY_CONTAINS(CAST('1' AS VARIANT), ['1'])",
            read={
                "presto": "SELECT CONTAINS(ARRAY['1'], '1')",
                "snowflake": "SELECT ARRAY_CONTAINS(CAST('1' AS VARIANT), ['1'])",
            },
        )
        self.validate_all(
            "SELECT ARRAY_CONTAINS(CAST(CAST('2020-10-10' AS DATE) AS VARIANT), [CAST('2020-10-10' AS DATE)])",
            read={
                "presto": "SELECT CONTAINS(ARRAY[DATE '2020-10-10'], DATE '2020-10-10')",
                "snowflake": "SELECT ARRAY_CONTAINS(CAST(CAST('2020-10-10' AS DATE) AS VARIANT), [CAST('2020-10-10' AS DATE)])",
            },
        )
        self.validate_identity("SELECT ARRAY_CONTAINS(1, [1])")

        self.validate_all(
            "SELECT x'ABCD'",
            write={
                "snowflake": "SELECT x'ABCD'",
                "duckdb": "SELECT UNHEX('ABCD')",
            },
        )

        self.validate_all(
            "SET a = 1",
            write={
                "snowflake": "SET a = 1",
                "bigquery": "SET a = 1",
                "duckdb": "SET VARIABLE a = 1",
            },
        )
        self.validate_all(
            "CAST(6.43 AS FLOAT)",
            write={
                "snowflake": "CAST(6.43 AS DOUBLE)",
                "duckdb": "CAST(6.43 AS DOUBLE)",
            },
        )
        self.validate_all(
            "UNIFORM(1, 10, RANDOM(5))",
            write={
                "snowflake": "UNIFORM(1, 10, RANDOM(5))",
                "databricks": "UNIFORM(1, 10, 5)",
                "duckdb": "CAST(FLOOR(1 + RANDOM() * (10 - 1 + 1)) AS BIGINT)",
            },
        )
        self.validate_all(
            "UNIFORM(1, 10, RANDOM())",
            write={
                "snowflake": "UNIFORM(1, 10, RANDOM())",
                "databricks": "UNIFORM(1, 10)",
                "duckdb": "CAST(FLOOR(1 + RANDOM() * (10 - 1 + 1)) AS BIGINT)",
            },
        )
        self.validate_all(
            "UNIFORM(1, 10, 5)",
            write={
                "snowflake": "UNIFORM(1, 10, 5)",
                "databricks": "UNIFORM(1, 10, 5)",
                "duckdb": "CAST(FLOOR(1 + (ABS(HASH(5)) % 1000000) / 1000000.0 * (10 - 1 + 1)) AS BIGINT)",
            },
        )
        self.validate_all(
            "NORMAL(0, 1, 42)",
            write={
                "snowflake": "NORMAL(0, 1, 42)",
                "duckdb": "0 + (1 * SQRT(-2 * LN(GREATEST((ABS(HASH(42)) % 1000000) / 1000000.0, 1e-10))) * COS(2 * PI() * (ABS(HASH(42 + 1)) % 1000000) / 1000000.0))",
            },
        )
        self.validate_all(
            "NORMAL(10.5, 2.5, RANDOM())",
            write={
                "snowflake": "NORMAL(10.5, 2.5, RANDOM())",
                "duckdb": "10.5 + (2.5 * SQRT(-2 * LN(GREATEST(RANDOM(), 1e-10))) * COS(2 * PI() * RANDOM()))",
            },
        )
        self.validate_all(
            "NORMAL(10.5, 2.5, RANDOM(5))",
            write={
                "snowflake": "NORMAL(10.5, 2.5, RANDOM(5))",
                "duckdb": "10.5 + (2.5 * SQRT(-2 * LN(GREATEST((ABS(HASH(5)) % 1000000) / 1000000.0, 1e-10))) * COS(2 * PI() * (ABS(HASH(5 + 1)) % 1000000) / 1000000.0))",
            },
        )
        self.validate_all(
            "SYSDATE()",
            write={
                "snowflake": "SYSDATE()",
                "duckdb": "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
            },
        )
        self.validate_identity("SYSTIMESTAMP()", "CURRENT_TIMESTAMP()")
        self.validate_identity("GETDATE()", "CURRENT_TIMESTAMP()")
        self.validate_identity("LOCALTIMESTAMP", "CURRENT_TIMESTAMP")
        self.validate_identity("LOCALTIMESTAMP()", "CURRENT_TIMESTAMP()")
        self.validate_identity("LOCALTIMESTAMP(3)", "CURRENT_TIMESTAMP(3)")

        self.validate_all(
            "SELECT DATE_FROM_PARTS(2026, 1, 100)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2026, 1, 100)",
                "duckdb": "SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (1 - 1) MONTH + INTERVAL (100 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2026, 14, 32)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2026, 14, 32)",
                "duckdb": "SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (14 - 1) MONTH + INTERVAL (32 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2026, 0, 0)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2026, 0, 0)",
                "duckdb": "SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (0 - 1) MONTH + INTERVAL (0 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2026, -14, -32)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2026, -14, -32)",
                "duckdb": "SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (-14 - 1) MONTH + INTERVAL (-32 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2024, 1, 60)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2024, 1, 60)",
                "duckdb": "SELECT CAST(MAKE_DATE(2024, 1, 1) + INTERVAL (1 - 1) MONTH + INTERVAL (60 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2026, NULL, 100)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2026, NULL, 100)",
                "duckdb": "SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (NULL - 1) MONTH + INTERVAL (100 - 1) DAY AS DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_FROM_PARTS(2024 + 2, 1 + 2, 2 + 3)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(2024 + 2, 1 + 2, 2 + 3)",
                "duckdb": "SELECT CAST(MAKE_DATE(2024 + 2, 1, 1) + INTERVAL ((1 + 2) - 1) MONTH + INTERVAL ((2 + 3) - 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT DATE_FROM_PARTS(year, month, date)",
            write={
                "snowflake": "SELECT DATE_FROM_PARTS(year, month, date)",
                "duckdb": "SELECT CAST(MAKE_DATE(year, 1, 1) + INTERVAL (month - 1) MONTH + INTERVAL (date - 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "EQUAL_NULL(a, b)",
            write={
                "snowflake": "EQUAL_NULL(a, b)",
                "duckdb": "a IS NOT DISTINCT FROM b",
            },
        )

    def test_null_treatment(self):
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1 RESPECT NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1 IGNORE NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            "SELECT * FROM foo WHERE 'str' IN (SELECT value FROM TABLE(FLATTEN(INPUT => vals)) AS _u(seq, key, path, index, value, this))",
            read={
                "bigquery": "SELECT * FROM foo WHERE 'str' IN UNNEST(vals)",
            },
            write={
                "snowflake": "SELECT * FROM foo WHERE 'str' IN (SELECT value FROM TABLE(FLATTEN(INPUT => vals)) AS _u(seq, key, path, index, value, this))",
            },
        )

    def test_staged_files(self):
        # Ensure we don't treat staged file paths as identifiers (i.e. they're not normalized)
        staged_file = parse_one("SELECT * FROM @foo", read="snowflake")
        self.assertEqual(
            normalize_identifiers(staged_file, dialect="snowflake").sql(dialect="snowflake"),
            staged_file.sql(dialect="snowflake"),
        )

        self.validate_identity('SELECT * FROM @"mystage"')
        self.validate_identity('SELECT * FROM @"myschema"."mystage"/file.gz')
        self.validate_identity('SELECT * FROM @"my_DB"."schEMA1".mystage/file.gz')
        self.validate_identity("SELECT metadata$filename FROM @s1/")
        self.validate_identity("SELECT * FROM @~")
        self.validate_identity("SELECT * FROM @~/some/path/to/file.csv")
        self.validate_identity("SELECT * FROM @mystage")
        self.validate_identity("SELECT * FROM '@mystage'")
        self.validate_identity("SELECT * FROM @namespace.mystage/path/to/file.json.gz")
        self.validate_identity("SELECT * FROM @namespace.%table_name/path/to/file.json.gz")
        self.validate_identity("SELECT * FROM '@external/location' (FILE_FORMAT => 'path.to.csv')")
        self.validate_identity("PUT file:///dir/tmp.csv @%table", check_command_warning=True)
        self.validate_identity("SELECT * FROM (SELECT a FROM @foo)")
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM '@external/location' (FILE_FORMAT => 'path.to.csv'))"
        )
        self.validate_identity(
            "SELECT * FROM @foo/bar (FILE_FORMAT => ds_sandbox.test.my_csv_format, PATTERN => 'test') AS bla"
        )
        self.validate_identity(
            "SELECT t.$1, t.$2 FROM @mystage1 (FILE_FORMAT => 'myformat', PATTERN => '.*data.*[.]csv.gz') AS t"
        )
        self.validate_identity(
            "SELECT parse_json($1):a.b FROM @mystage2/data1.json.gz",
            "SELECT GET_PATH(PARSE_JSON($1), 'a.b') FROM @mystage2/data1.json.gz",
        )
        self.validate_identity(
            "SELECT * FROM @mystage t (c1)",
            "SELECT * FROM @mystage AS t(c1)",
        )
        self.validate_identity(
            "SELECT * FROM @foo/bar (PATTERN => 'test', FILE_FORMAT => ds_sandbox.test.my_csv_format) AS bla",
            "SELECT * FROM @foo/bar (FILE_FORMAT => ds_sandbox.test.my_csv_format, PATTERN => 'test') AS bla",
        )

        self.validate_identity(
            "SELECT * FROM @test.public.thing/location/somefile.csv( FILE_FORMAT => 'fmt' )",
            "SELECT * FROM @test.public.thing/location/somefile.csv (FILE_FORMAT => 'fmt')",
        )

    def test_sample(self):
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE SYSTEM (3) SEED (82)")
        self.validate_identity(
            "SELECT a FROM test PIVOT(SUM(x) FOR y IN ('z', 'q')) AS x TABLESAMPLE BERNOULLI (0.1)"
        )
        self.validate_identity(
            "SELECT i, j FROM table1 AS t1 INNER JOIN table2 AS t2 TABLESAMPLE BERNOULLI (50) WHERE t2.j = t1.i"
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE BERNOULLI (1)"
        )
        self.validate_identity(
            "SELECT * FROM testtable TABLESAMPLE (10 ROWS)",
            "SELECT * FROM testtable TABLESAMPLE BERNOULLI (10 ROWS)",
        )
        self.validate_identity(
            "SELECT * FROM testtable TABLESAMPLE (100)",
            "SELECT * FROM testtable TABLESAMPLE BERNOULLI (100)",
        )
        self.validate_identity(
            "SELECT * FROM testtable SAMPLE (10)",
            "SELECT * FROM testtable TABLESAMPLE BERNOULLI (10)",
        )
        self.validate_identity(
            "SELECT * FROM testtable SAMPLE ROW (0)",
            "SELECT * FROM testtable TABLESAMPLE ROW (0)",
        )
        self.validate_identity(
            "SELECT a FROM test SAMPLE BLOCK (0.5) SEED (42)",
            "SELECT a FROM test TABLESAMPLE BLOCK (0.5) SEED (42)",
        )
        self.validate_identity(
            "SELECT user_id, value FROM table_name SAMPLE BERNOULLI ($s) SEED (0)",
            "SELECT user_id, value FROM table_name TABLESAMPLE BERNOULLI ($s) SEED (0)",
        )

        self.validate_all(
            "SELECT * FROM example TABLESAMPLE BERNOULLI (3) SEED (82)",
            read={
                "duckdb": "SELECT * FROM example TABLESAMPLE BERNOULLI (3 PERCENT) REPEATABLE (82)",
            },
            write={
                "databricks": "SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)",
                "duckdb": "SELECT * FROM example TABLESAMPLE BERNOULLI (3 PERCENT) REPEATABLE (82)",
                "snowflake": "SELECT * FROM example TABLESAMPLE BERNOULLI (3) SEED (82)",
            },
        )
        self.validate_all(
            "SELECT * FROM test AS _tmp TABLESAMPLE (5)",
            write={
                "postgres": "SELECT * FROM test AS _tmp TABLESAMPLE BERNOULLI (5)",
                "snowflake": "SELECT * FROM test AS _tmp TABLESAMPLE BERNOULLI (5)",
            },
        )
        self.validate_all(
            """
            SELECT i, j
                FROM
                     table1 AS t1 SAMPLE (25)     -- 25% of rows in table1
                         INNER JOIN
                     table2 AS t2 SAMPLE (50)     -- 50% of rows in table2
                WHERE t2.j = t1.i""",
            write={
                "snowflake": "SELECT i, j FROM table1 AS t1 TABLESAMPLE BERNOULLI (25) /* 25% of rows in table1 */ INNER JOIN table2 AS t2 TABLESAMPLE BERNOULLI (50) /* 50% of rows in table2 */ WHERE t2.j = t1.i",
            },
        )
        self.validate_all(
            "SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992)",
            write={
                "snowflake": "SELECT * FROM testtable TABLESAMPLE BLOCK (0.012) SEED (99992)",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT * FROM t1 join t2 on t1.a = t2.c) SAMPLE (1)",
            write={
                "snowflake": "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE BERNOULLI (1)",
                "spark": "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1 PERCENT)",
            },
        )
        self.validate_all(
            "TO_DOUBLE(expr)",
            write={
                "snowflake": "TO_DOUBLE(expr)",
                "duckdb": "CAST(expr AS DOUBLE)",
            },
        )
        self.validate_all(
            "TO_DOUBLE(expr, fmt)",
            write={
                "snowflake": "TO_DOUBLE(expr, fmt)",
                "duckdb": UnsupportedError,
            },
        )

    def test_timestamps(self):
        self.validate_identity("SELECT CAST('12:00:00' AS TIME)")
        self.validate_identity("SELECT DATE_PART(month, a)")

        self.validate_identity(
            "SELECT DATE_PART(year FROM CAST('2024-04-08' AS DATE))",
            "SELECT DATE_PART(year, CAST('2024-04-08' AS DATE))",
        ).expressions[0].assert_is(exp.Extract)
        self.validate_identity(
            "SELECT DATE_PART('month' FROM CAST('2024-04-08' AS DATE))",
            "SELECT DATE_PART('month', CAST('2024-04-08' AS DATE))",
        ).expressions[0].assert_is(exp.Extract)
        self.validate_identity(
            "SELECT DATE_PART(day FROM a)", "SELECT DATE_PART(day, a)"
        ).expressions[0].assert_is(exp.Extract)

        for data_type in (
            "TIMESTAMP",
            "TIMESTAMPLTZ",
            "TIMESTAMPNTZ",
        ):
            self.validate_identity(f"CAST(a AS {data_type})")

        self.validate_identity("CAST(a AS TIMESTAMP_NTZ)", "CAST(a AS TIMESTAMPNTZ)")
        self.validate_identity("CAST(a AS TIMESTAMP_LTZ)", "CAST(a AS TIMESTAMPLTZ)")

        self.validate_all(
            "SELECT a::TIMESTAMP_LTZ(9)",
            write={
                "snowflake": "SELECT CAST(a AS TIMESTAMPLTZ(9))",
            },
        )
        self.validate_all(
            "SELECT a::TIMESTAMPLTZ",
            write={
                "snowflake": "SELECT CAST(a AS TIMESTAMPLTZ)",
            },
        )
        self.validate_all(
            "SELECT a::TIMESTAMP WITH LOCAL TIME ZONE",
            write={
                "snowflake": "SELECT CAST(a AS TIMESTAMPLTZ)",
            },
        )
        self.validate_all(
            "SELECT EXTRACT('month', a)",
            write={
                "snowflake": "SELECT DATE_PART('month', a)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('month', a)",
            write={
                "snowflake": "SELECT DATE_PART('month', a)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(month, a::DATETIME)",
            write={
                "snowflake": "SELECT DATE_PART(month, CAST(a AS DATETIME))",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(epoch_second, foo) as ddate from table_name",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_SECOND, foo) AS ddate FROM table_name",
                "duckdb": "SELECT CAST(EPOCH(foo) AS BIGINT) AS ddate FROM table_name",
                "presto": "SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) AS ddate FROM table_name",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(epoch_milliseconds, foo) as ddate from table_name",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH_MILLISECOND, foo) AS ddate FROM table_name",
                "duckdb": "SELECT EPOCH_MS(foo) AS ddate FROM table_name",
                "presto": "SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) * 1000 AS ddate FROM table_name",
            },
        )
        self.validate_all(
            "DATEADD(DAY, 5, CAST('2008-12-25' AS DATE))",
            read={
                "snowflake": "TIMESTAMPADD(DAY, 5, CAST('2008-12-25' AS DATE))",
            },
            write={
                "bigquery": "DATE_ADD(CAST('2008-12-25' AS DATE), INTERVAL 5 DAY)",
                "snowflake": "DATEADD(DAY, 5, CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_identity(
            "DATEDIFF(DAY, CAST('2007-12-25' AS DATE), CAST('2008-12-25' AS DATE))"
        )
        self.validate_identity(
            "TIMEDIFF(DAY, CAST('2007-12-25' AS DATE), CAST('2008-12-25' AS DATE))",
            "DATEDIFF(DAY, CAST('2007-12-25' AS DATE), CAST('2008-12-25' AS DATE))",
        )
        self.validate_identity(
            "TIMESTAMPDIFF(DAY, CAST('2007-12-25' AS DATE), CAST('2008-12-25' AS DATE))",
            "DATEDIFF(DAY, CAST('2007-12-25' AS DATE), CAST('2008-12-25' AS DATE))",
        )

        # Test DATEDIFF with WEEK unit - week boundary crossing
        self.validate_all(
            "DATEDIFF(WEEK, '2024-12-13', '2024-12-17')",
            write={
                "duckdb": "DATE_DIFF('WEEK', DATE_TRUNC('WEEK', CAST('2024-12-13' AS DATE)), DATE_TRUNC('WEEK', CAST('2024-12-17' AS DATE)))",
                "snowflake": "DATEDIFF(WEEK, '2024-12-13', '2024-12-17')",
            },
        )
        self.validate_all(
            "DATEDIFF(WEEK, '2024-12-15', '2024-12-16')",
            write={
                "duckdb": "DATE_DIFF('WEEK', DATE_TRUNC('WEEK', CAST('2024-12-15' AS DATE)), DATE_TRUNC('WEEK', CAST('2024-12-16' AS DATE)))",
                "snowflake": "DATEDIFF(WEEK, '2024-12-15', '2024-12-16')",
            },
        )

        # Test DATEDIFF with other date parts - should not use DATE_TRUNC
        self.validate_all(
            "DATEDIFF(YEAR, '2020-01-15', '2023-06-20')",
            write={
                "duckdb": "DATE_DIFF('YEAR', CAST('2020-01-15' AS DATE), CAST('2023-06-20' AS DATE))",
                "snowflake": "DATEDIFF(YEAR, '2020-01-15', '2023-06-20')",
            },
        )
        self.validate_all(
            "DATEDIFF(MONTH, '2020-01-15', '2023-06-20')",
            write={
                "duckdb": "DATE_DIFF('MONTH', CAST('2020-01-15' AS DATE), CAST('2023-06-20' AS DATE))",
                "snowflake": "DATEDIFF(MONTH, '2020-01-15', '2023-06-20')",
            },
        )
        self.validate_all(
            "DATEDIFF(QUARTER, '2020-01-15', '2023-06-20')",
            write={
                "duckdb": "DATE_DIFF('QUARTER', CAST('2020-01-15' AS DATE), CAST('2023-06-20' AS DATE))",
                "snowflake": "DATEDIFF(QUARTER, '2020-01-15', '2023-06-20')",
            },
        )

        # Test DATEDIFF with NANOSECOND - DuckDB uses EPOCH_NS since DATE_DIFF doesn't support NANOSECOND
        self.validate_all(
            "DATEDIFF(NANOSECOND, '2023-01-01 10:00:00.000000000', '2023-01-01 10:00:00.123456789')",
            write={
                "duckdb": "EPOCH_NS(CAST('2023-01-01 10:00:00.123456789' AS TIMESTAMP_NS)) - EPOCH_NS(CAST('2023-01-01 10:00:00.000000000' AS TIMESTAMP_NS))",
                "snowflake": "DATEDIFF(NANOSECOND, '2023-01-01 10:00:00.000000000', '2023-01-01 10:00:00.123456789')",
            },
        )

        # Test DATEDIFF with NANOSECOND on columns
        self.validate_all(
            "DATEDIFF(NANOSECOND, start_time, end_time)",
            write={
                "duckdb": "EPOCH_NS(CAST(end_time AS TIMESTAMP_NS)) - EPOCH_NS(CAST(start_time AS TIMESTAMP_NS))",
                "snowflake": "DATEDIFF(NANOSECOND, start_time, end_time)",
            },
        )

        # Test DATEADD with NANOSECOND - DuckDB uses MAKE_TIMESTAMP_NS since INTERVAL doesn't support NANOSECOND
        self.validate_all(
            "DATEADD(NANOSECOND, 123456789, '2023-01-01 10:00:00.000000000')",
            write={
                "duckdb": "MAKE_TIMESTAMP_NS(EPOCH_NS(CAST('2023-01-01 10:00:00.000000000' AS TIMESTAMP_NS)) + 123456789)",
                "snowflake": "DATEADD(NANOSECOND, 123456789, '2023-01-01 10:00:00.000000000')",
            },
        )

        # Test DATEADD with NANOSECOND on columns
        self.validate_all(
            "DATEADD(NANOSECOND, nano_offset, timestamp_col)",
            write={
                "duckdb": "MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(timestamp_col AS TIMESTAMP_NS)) + nano_offset)",
                "snowflake": "DATEADD(NANOSECOND, nano_offset, timestamp_col)",
            },
        )

        # Test negative NANOSECOND values (subtraction)
        self.validate_all(
            "DATEADD(NANOSECOND, -123456789, '2023-01-01 10:00:00.500000000')",
            write={
                "duckdb": "MAKE_TIMESTAMP_NS(EPOCH_NS(CAST('2023-01-01 10:00:00.500000000' AS TIMESTAMP_NS)) + -123456789)",
                "snowflake": "DATEADD(NANOSECOND, -123456789, '2023-01-01 10:00:00.500000000')",
            },
        )

        # Test TIMESTAMPDIFF with NANOSECOND - Snowflake parser converts to DATEDIFF
        self.validate_all(
            "TIMESTAMPDIFF(NANOSECOND, '2023-01-01 10:00:00.000000000', '2023-01-01 10:00:00.123456789')",
            write={
                "duckdb": "EPOCH_NS(CAST('2023-01-01 10:00:00.123456789' AS TIMESTAMP_NS)) - EPOCH_NS(CAST('2023-01-01 10:00:00.000000000' AS TIMESTAMP_NS))",
                "snowflake": "DATEDIFF(NANOSECOND, '2023-01-01 10:00:00.000000000', '2023-01-01 10:00:00.123456789')",
            },
        )

        # Test TIMESTAMPADD with NANOSECOND - Snowflake parser converts to DATEADD
        self.validate_all(
            "TIMESTAMPADD(NANOSECOND, 123456789, '2023-01-01 10:00:00.000000000')",
            write={
                "duckdb": "MAKE_TIMESTAMP_NS(EPOCH_NS(CAST('2023-01-01 10:00:00.000000000' AS TIMESTAMP_NS)) + 123456789)",
                "snowflake": "DATEADD(NANOSECOND, 123456789, '2023-01-01 10:00:00.000000000')",
            },
        )

        self.validate_identity("DATEADD(y, 5, x)", "DATEADD(YEAR, 5, x)")
        self.validate_identity("DATEADD(y, 5, x)", "DATEADD(YEAR, 5, x)")
        self.validate_identity("DATE_PART(yyy, x)", "DATE_PART(YEAR, x)")
        self.validate_identity("DATE_TRUNC(yr, x)", "DATE_TRUNC('YEAR', x)")
        self.validate_all(
            "DATE_TRUNC('YEAR', CAST('2024-06-15' AS DATE))",
            write={
                "snowflake": "DATE_TRUNC('YEAR', CAST('2024-06-15' AS DATE))",
                "duckdb": "DATE_TRUNC('YEAR', CAST('2024-06-15' AS DATE))",
            },
        )
        self.validate_all(
            "DATE_TRUNC('HOUR', CAST('2026-01-01 00:00:00' AS TIMESTAMP))",
            write={
                "snowflake": "DATE_TRUNC('HOUR', CAST('2026-01-01 00:00:00' AS TIMESTAMP))",
                "duckdb": "DATE_TRUNC('HOUR', CAST('2026-01-01 00:00:00' AS TIMESTAMP))",
            },
        )
        # Snowflake's DATE_TRUNC return type matches type of the expresison
        # DuckDB's DATE_TRUNC return type matches type of granularity part.
        # In Snowflake --> DuckDB, DATE_TRUNC(date_part, timestamp) should be cast to timestamp to preserve Snowflake behavior.
        self.validate_all(
            "DATE_TRUNC(YEAR, TIMESTAMP '2026-01-01 00:00:00')",
            write={
                "snowflake": "DATE_TRUNC('YEAR', CAST('2026-01-01 00:00:00' AS TIMESTAMP))",
                "duckdb": "CAST(DATE_TRUNC('YEAR', CAST('2026-01-01 00:00:00' AS TIMESTAMP)) AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "DATE_TRUNC(MONTH, CAST('2024-06-15 14:23:45' AS TIMESTAMPTZ))",
            write={
                "snowflake": "DATE_TRUNC('MONTH', CAST('2024-06-15 14:23:45' AS TIMESTAMPTZ))",
                "duckdb": "CAST(DATE_TRUNC('MONTH', CAST('2024-06-15 14:23:45' AS TIMESTAMPTZ)) AS TIMESTAMPTZ)",
            },
        )
        self.validate_all(
            "DATE_TRUNC('WEEK', CURRENT_DATE)",
            write={
                "snowflake": "DATE_TRUNC('WEEK', CURRENT_DATE)",
                "duckdb": "DATE_TRUNC('WEEK', CURRENT_DATE)",
            },
        )

        # In Snowflake --> DuckDB, DATE_TRUNC(time_part, date) should be cast to date to preserve Snowflake behavior.
        self.validate_all(
            "DATE_TRUNC('HOUR', CAST('2026-01-01' AS DATE))",
            write={
                "snowflake": "DATE_TRUNC('HOUR', CAST('2026-01-01' AS DATE))",
                "duckdb": "CAST(DATE_TRUNC('HOUR', CAST('2026-01-01' AS DATE)) AS DATE)",
            },
        )

        # DuckDB does not support DATE_TRUNC(time_part, time), so we add a dummy date to generate DATE_TRUNC(time_part, date) --> DATE in DuckDB
        # Then it is casted to a time (HH:MM:SS) to match Snowflake.
        self.validate_all(
            "DATE_TRUNC('HOUR', CAST('14:23:45.123456' AS TIME))",
            write={
                "snowflake": "DATE_TRUNC('HOUR', CAST('14:23:45.123456' AS TIME))",
                "duckdb": "CAST(DATE_TRUNC('HOUR', CAST('1970-01-01' AS DATE) + CAST('14:23:45.123456' AS TIME)) AS TIME)",
            },
        )

        self.validate_all(
            "DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "snowflake": "TO_DATE(x)",
            },
        )

        self.validate_all(
            "DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE('01-01-2000', 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME('01-01-2000', '%m-%d-%Y') AS DATE)",
            },
        )

        self.validate_identity("SELECT TO_TIME(x) FROM t")
        self.validate_all(
            "SELECT TO_TIME('12:05:00')",
            write={
                "bigquery": "SELECT CAST('12:05:00' AS TIME)",
                "snowflake": "SELECT CAST('12:05:00' AS TIME)",
                "duckdb": "SELECT CAST('12:05:00' AS TIME)",
            },
        )
        self.validate_all(
            "SELECT TO_TIME('2024-01-15 14:30:00'::TIMESTAMP)",
            write={
                "bigquery": "SELECT TIME(CAST('2024-01-15 14:30:00' AS DATETIME))",
                "snowflake": "SELECT TO_TIME(CAST('2024-01-15 14:30:00' AS TIMESTAMP))",
                "duckdb": "SELECT CAST(CAST('2024-01-15 14:30:00' AS TIMESTAMP) AS TIME)",
            },
        )
        self.validate_all(
            "SELECT TO_TIME(CONVERT_TIMEZONE('UTC', 'US/Pacific', '2024-08-06 09:10:00.000')) AS pst_time",
            write={
                "snowflake": "SELECT TO_TIME(CONVERT_TIMEZONE('UTC', 'US/Pacific', '2024-08-06 09:10:00.000')) AS pst_time",
                "duckdb": "SELECT CAST(CAST('2024-08-06 09:10:00.000' AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific' AS TIME) AS pst_time",
            },
        )
        self.validate_all(
            "SELECT TO_TIME('11.15.00', 'hh24.mi.ss')",
            write={
                "snowflake": "SELECT TO_TIME('11.15.00', 'hh24.mi.ss')",
                "duckdb": "SELECT CAST(STRPTIME('11.15.00', '%H.%M.%S') AS TIME)",
            },
        )
        self.validate_all(
            "SELECT TO_TIME('093000', 'HH24MISS')",
            write={
                "duckdb": "SELECT CAST(STRPTIME('093000', '%H%M%S') AS TIME)",
                "snowflake": "SELECT TO_TIME('093000', 'hh24miss')",
            },
        )
        self.validate_all(
            "SELECT TRY_TO_TIME('093000', 'HH24MISS')",
            write={
                "snowflake": "SELECT TRY_TO_TIME('093000', 'hh24miss')",
                "duckdb": "SELECT TRY_CAST(TRY_STRPTIME('093000', '%H%M%S') AS TIME)",
            },
        )
        self.validate_all(
            "SELECT TRY_TO_TIME('11.15.00')",
            write={
                "snowflake": "SELECT TRY_CAST('11.15.00' AS TIME)",
                "duckdb": "SELECT TRY_CAST('11.15.00' AS TIME)",
            },
        )
        self.validate_all(
            "SELECT TRY_TO_TIME('11.15.00', 'hh24.mi.ss')",
            write={
                "snowflake": "SELECT TRY_TO_TIME('11.15.00', 'hh24.mi.ss')",
                "duckdb": "SELECT TRY_CAST(TRY_STRPTIME('11.15.00', '%H.%M.%S') AS TIME)",
            },
        )

    def test_to_date(self):
        self.validate_identity("TO_DATE('12345')").assert_is(exp.Anonymous)

        self.validate_identity("TO_DATE(x)").assert_is(exp.TsOrDsToDate)

        self.validate_all(
            "TO_DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE('01-01-2000', 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME('01-01-2000', '%m-%d-%Y') AS DATE)",
            },
        )

        self.validate_all(
            "TO_DATE(x, 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE(x, 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME(x, '%m-%d-%Y') AS DATE)",
            },
        )

        self.validate_identity(
            "SELECT TO_DATE('2019-02-28') + INTERVAL '1 day, 1 year'",
            "SELECT CAST('2019-02-28' AS DATE) + INTERVAL '1 day, 1 year'",
        )

        self.validate_identity("TRY_TO_DATE(x)").assert_is(exp.TsOrDsToDate)

        self.validate_all(
            "TRY_TO_DATE('2024-01-31')",
            write={
                "snowflake": "TRY_CAST('2024-01-31' AS DATE)",
                "duckdb": "TRY_CAST('2024-01-31' AS DATE)",
            },
        )
        self.validate_identity("TRY_TO_DATE('2024-01-31', 'AUTO')")

        self.validate_all(
            "TRY_TO_DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TRY_TO_DATE('01-01-2000', 'mm-DD-yyyy')",
                "duckdb": "CAST(CAST(TRY_STRPTIME('01-01-2000', '%m-%d-%Y') AS TIMESTAMP) AS DATE)",
            },
        )

        for i in range(1, 10):
            fractional_format = "ff" + str(i)
            duck_db_format = "%n"
            if i == 3:
                duck_db_format = "%g"
            elif i == 6:
                duck_db_format = "%f"
            with self.subTest(f"Testing snowflake {fractional_format} format"):
                self.validate_all(
                    f"TRY_TO_DATE('2013-04-28T20:57:01', 'yyyy-mm-DDThh24:mi:ss.{fractional_format}')",
                    write={
                        "snowflake": f"TRY_TO_DATE('2013-04-28T20:57:01', 'yyyy-mm-DDThh24:mi:ss.{fractional_format}')",
                        "duckdb": f"CAST(CAST(TRY_STRPTIME('2013-04-28T20:57:01', '%Y-%m-%dT%H:%M:%S.{duck_db_format}') AS TIMESTAMP) AS DATE)",
                    },
                )

        self.validate_all(
            "TRY_TO_DATE('2013-04-28T20:57:01.888', 'yyyy-mm-DDThh24:mi:ss.ff')",
            write={
                "snowflake": "TRY_TO_DATE('2013-04-28T20:57:01.888', 'yyyy-mm-DDThh24:mi:ss.ff9')",
                "duckdb": "CAST(CAST(TRY_STRPTIME('2013-04-28T20:57:01.888', '%Y-%m-%dT%H:%M:%S.%n') AS TIMESTAMP) AS DATE)",
            },
        )

        tz_to_format = {
            "tzh:tzm": "+07:00",
            "tzhtzm": "+0700",
            "tzh": "+07",
        }

        for tz_format, tz in tz_to_format.items():
            with self.subTest(f"Testing snowflake {tz_format} timezone format"):
                self.validate_all(
                    f"TRY_TO_DATE('2013-04-28 20:57 {tz}', 'YYYY-MM-DD HH24:MI {tz_format}')",
                    write={
                        "snowflake": f"TRY_TO_DATE('2013-04-28 20:57 {tz}', 'yyyy-mm-DD hh24:mi {tz_format}')",
                        "duckdb": f"CAST(CAST(TRY_STRPTIME('2013-04-28 20:57 {tz}', '%Y-%m-%d %H:%M %z') AS TIMESTAMP) AS DATE)",
                    },
                )

        self.validate_all(
            """TRY_TO_DATE('2013-04-28T20:57', 'YYYY-MM-DD"T"HH24:MI:SS')""",
            write={
                "snowflake": "TRY_TO_DATE('2013-04-28T20:57', 'yyyy-mm-DDThh24:mi:ss')",
                "duckdb": "CAST(CAST(TRY_STRPTIME('2013-04-28T20:57', '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS DATE)",
            },
        )

    def test_semi_structured_types(self):
        self.validate_identity("SELECT CAST(a AS VARIANT)")
        self.validate_identity("SELECT CAST(a AS ARRAY)")

        self.validate_all(
            "SELECT a::VARIANT",
            write={
                "snowflake": "SELECT CAST(a AS VARIANT)",
                "tsql": "SELECT CAST(a AS SQL_VARIANT)",
            },
        )
        self.validate_all(
            "ARRAY_CONSTRUCT(0, 1, 2)",
            write={
                "snowflake": "[0, 1, 2]",
                "bigquery": "[0, 1, 2]",
                "duckdb": "[0, 1, 2]",
                "presto": "ARRAY[0, 1, 2]",
                "spark": "ARRAY(0, 1, 2)",
            },
        )
        self.validate_all(
            "SELECT a::OBJECT",
            write={
                "snowflake": "SELECT CAST(a AS OBJECT)",
            },
        )

    def test_next_day(self):
        self.validate_all(
            "SELECT NEXT_DAY(CAST('2024-01-01' AS DATE), 'Monday')",
            write={
                "snowflake": "SELECT NEXT_DAY(CAST('2024-01-01' AS DATE), 'Monday')",
                "duckdb": "SELECT CAST(CAST('2024-01-01' AS DATE) + INTERVAL ((((1 - ISODOW(CAST('2024-01-01' AS DATE))) + 6) % 7) + 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT NEXT_DAY(CAST('2024-01-05' AS DATE), 'Friday')",
            write={
                "snowflake": "SELECT NEXT_DAY(CAST('2024-01-05' AS DATE), 'Friday')",
                "duckdb": "SELECT CAST(CAST('2024-01-05' AS DATE) + INTERVAL ((((5 - ISODOW(CAST('2024-01-05' AS DATE))) + 6) % 7) + 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT NEXT_DAY(CAST('2024-01-05' AS DATE), 'WE')",
            write={
                "snowflake": "SELECT NEXT_DAY(CAST('2024-01-05' AS DATE), 'WE')",
                "duckdb": "SELECT CAST(CAST('2024-01-05' AS DATE) + INTERVAL ((((3 - ISODOW(CAST('2024-01-05' AS DATE))) + 6) % 7) + 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT NEXT_DAY(CAST('2024-01-01 10:30:45' AS TIMESTAMP), 'Friday')",
            write={
                "snowflake": "SELECT NEXT_DAY(CAST('2024-01-01 10:30:45' AS TIMESTAMP), 'Friday')",
                "duckdb": "SELECT CAST(CAST('2024-01-01 10:30:45' AS TIMESTAMP) + INTERVAL ((((5 - ISODOW(CAST('2024-01-01 10:30:45' AS TIMESTAMP))) + 6) % 7) + 1) DAY AS DATE)",
            },
        )

        self.validate_all(
            "SELECT NEXT_DAY(CAST('2024-01-01' AS DATE), day_column)",
            write={
                "snowflake": "SELECT NEXT_DAY(CAST('2024-01-01' AS DATE), day_column)",
                "duckdb": "SELECT CAST(CAST('2024-01-01' AS DATE) + INTERVAL ((((CASE WHEN STARTS_WITH(UPPER(day_column), 'MO') THEN 1 WHEN STARTS_WITH(UPPER(day_column), 'TU') THEN 2 WHEN STARTS_WITH(UPPER(day_column), 'WE') THEN 3 WHEN STARTS_WITH(UPPER(day_column), 'TH') THEN 4 WHEN STARTS_WITH(UPPER(day_column), 'FR') THEN 5 WHEN STARTS_WITH(UPPER(day_column), 'SA') THEN 6 WHEN STARTS_WITH(UPPER(day_column), 'SU') THEN 7 END - ISODOW(CAST('2024-01-01' AS DATE))) + 6) % 7) + 1) DAY AS DATE)",
            },
        )

    def test_previous_day(self):
        self.validate_all(
            "SELECT PREVIOUS_DAY(DATE '2024-01-15', 'Monday')",
            write={
                "duckdb": "SELECT CAST(CAST('2024-01-15' AS DATE) - INTERVAL ((((ISODOW(CAST('2024-01-15' AS DATE)) - 1) + 6) % 7) + 1) DAY AS DATE)",
                "snowflake": "SELECT PREVIOUS_DAY(CAST('2024-01-15' AS DATE), 'Monday')",
            },
        )

        self.validate_all(
            "SELECT PREVIOUS_DAY(DATE '2024-01-15', 'Fr')",
            write={
                "duckdb": "SELECT CAST(CAST('2024-01-15' AS DATE) - INTERVAL ((((ISODOW(CAST('2024-01-15' AS DATE)) - 5) + 6) % 7) + 1) DAY AS DATE)",
                "snowflake": "SELECT PREVIOUS_DAY(CAST('2024-01-15' AS DATE), 'Fr')",
            },
        )

        self.validate_all(
            "SELECT PREVIOUS_DAY(TIMESTAMP '2024-01-15 10:30:45', 'Monday')",
            write={
                "duckdb": "SELECT CAST(CAST('2024-01-15 10:30:45' AS TIMESTAMP) - INTERVAL ((((ISODOW(CAST('2024-01-15 10:30:45' AS TIMESTAMP)) - 1) + 6) % 7) + 1) DAY AS DATE)",
                "snowflake": "SELECT PREVIOUS_DAY(CAST('2024-01-15 10:30:45' AS TIMESTAMP), 'Monday')",
            },
        )

        self.validate_all(
            "SELECT PREVIOUS_DAY(DATE '2024-01-15', day_column)",
            write={
                "duckdb": "SELECT CAST(CAST('2024-01-15' AS DATE) - INTERVAL ((((ISODOW(CAST('2024-01-15' AS DATE)) - CASE WHEN STARTS_WITH(UPPER(day_column), 'MO') THEN 1 WHEN STARTS_WITH(UPPER(day_column), 'TU') THEN 2 WHEN STARTS_WITH(UPPER(day_column), 'WE') THEN 3 WHEN STARTS_WITH(UPPER(day_column), 'TH') THEN 4 WHEN STARTS_WITH(UPPER(day_column), 'FR') THEN 5 WHEN STARTS_WITH(UPPER(day_column), 'SA') THEN 6 WHEN STARTS_WITH(UPPER(day_column), 'SU') THEN 7 END) + 6) % 7) + 1) DAY AS DATE)",
                "snowflake": "SELECT PREVIOUS_DAY(CAST('2024-01-15' AS DATE), day_column)",
            },
        )

    def test_historical_data(self):
        self.validate_identity("SELECT * FROM my_table AT (STATEMENT => $query_id_var)")
        self.validate_identity("SELECT * FROM my_table AT (OFFSET => -60 * 5)")
        self.validate_identity("SELECT * FROM my_table BEFORE (STATEMENT => $query_id_var)")
        self.validate_identity("SELECT * FROM my_table BEFORE (OFFSET => -60 * 5)")
        self.validate_identity("CREATE SCHEMA restored_schema CLONE my_schema AT (OFFSET => -3600)")
        self.validate_identity(
            "CREATE TABLE restored_table CLONE my_table AT (TIMESTAMP => CAST('Sat, 09 May 2015 01:01:00 +0300' AS TIMESTAMPTZ))",
        )
        self.validate_identity(
            "CREATE DATABASE restored_db CLONE my_db BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')"
        )
        self.validate_identity(
            "SELECT * FROM my_table AT (TIMESTAMP => TO_TIMESTAMP(1432669154242, 3))"
        )
        self.validate_identity(
            "SELECT * FROM my_table AT (OFFSET => -60 * 5) AS T WHERE T.flag = 'valid'"
        )
        self.validate_identity(
            "SELECT * FROM my_table AT (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')"
        )
        self.validate_identity(
            "SELECT * FROM my_table BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')"
        )
        self.validate_identity(
            "SELECT * FROM my_table AT (TIMESTAMP => 'Fri, 01 May 2015 16:20:00 -0700'::timestamp)",
            "SELECT * FROM my_table AT (TIMESTAMP => CAST('Fri, 01 May 2015 16:20:00 -0700' AS TIMESTAMP))",
        )
        self.validate_identity(
            "SELECT * FROM my_table AT(TIMESTAMP => 'Fri, 01 May 2015 16:20:00 -0700'::timestamp_tz)",
            "SELECT * FROM my_table AT (TIMESTAMP => CAST('Fri, 01 May 2015 16:20:00 -0700' AS TIMESTAMPTZ))",
        )
        self.validate_identity(
            "SELECT * FROM my_table BEFORE (TIMESTAMP => 'Fri, 01 May 2015 16:20:00 -0700'::timestamp_tz);",
            "SELECT * FROM my_table BEFORE (TIMESTAMP => CAST('Fri, 01 May 2015 16:20:00 -0700' AS TIMESTAMPTZ))",
        )
        self.validate_identity(
            """
            SELECT oldt.* , newt.*
            FROM my_table BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS oldt
            FULL OUTER JOIN my_table AT(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS newt
            ON oldt.id = newt.id
            WHERE oldt.id IS NULL OR newt.id IS NULL;
            """,
            "SELECT oldt.*, newt.* FROM my_table BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS oldt FULL OUTER JOIN my_table AT (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS newt ON oldt.id = newt.id WHERE oldt.id IS NULL OR newt.id IS NULL",
        )

        # Make sure that the historical data keywords can still be used as aliases
        for historical_data_prefix in ("AT", "BEFORE", "END", "CHANGES"):
            for schema_suffix in ("", "(col)"):
                with self.subTest(
                    f"Testing historical data prefix alias: {historical_data_prefix}{schema_suffix}"
                ):
                    self.validate_identity(
                        f"SELECT * FROM foo {historical_data_prefix}{schema_suffix}",
                        f"SELECT * FROM foo AS {historical_data_prefix}{schema_suffix}",
                    )

    def test_ddl(self):
        for constraint_prefix in ("WITH ", ""):
            with self.subTest(f"Constraint prefix: {constraint_prefix}"):
                self.validate_identity(
                    f"CREATE TABLE t (id INT {constraint_prefix}MASKING POLICY p.q.r)",
                    "CREATE TABLE t (id INT MASKING POLICY p.q.r)",
                )
                self.validate_identity(
                    f"CREATE TABLE t (id INT {constraint_prefix}MASKING POLICY p USING (c1, c2, c3))",
                    "CREATE TABLE t (id INT MASKING POLICY p USING (c1, c2, c3))",
                )
                self.validate_identity(
                    f"CREATE TABLE t (id INT {constraint_prefix}PROJECTION POLICY p.q.r)",
                    "CREATE TABLE t (id INT PROJECTION POLICY p.q.r)",
                )
                self.validate_identity(
                    f"CREATE TABLE t (id INT {constraint_prefix}TAG (key1='value_1', key2='value_2'))",
                    "CREATE TABLE t (id INT TAG (key1='value_1', key2='value_2'))",
                )

        self.validate_identity("CREATE OR REPLACE TABLE foo COPY GRANTS USING TEMPLATE (SELECT 1)")
        self.validate_identity("USE SECONDARY ROLES ALL")
        self.validate_identity("USE SECONDARY ROLES NONE")
        self.validate_identity("USE SECONDARY ROLES a, b, c")
        self.validate_identity("CREATE SECURE VIEW table1 AS (SELECT a FROM table2)")
        self.validate_identity("CREATE OR REPLACE VIEW foo (uid) COPY GRANTS AS (SELECT 1)")
        self.validate_identity("CREATE TABLE geospatial_table (id INT, g GEOGRAPHY)")
        self.validate_identity("CREATE MATERIALIZED VIEW a COMMENT='...' AS SELECT 1 FROM x")
        self.validate_identity("CREATE DATABASE mytestdb_clone CLONE mytestdb")
        self.validate_identity("CREATE SCHEMA mytestschema_clone CLONE testschema")
        self.validate_identity("CREATE TABLE IDENTIFIER('foo') (COLUMN1 VARCHAR, COLUMN2 VARCHAR)")
        self.validate_identity("CREATE TABLE IDENTIFIER($foo) (col1 VARCHAR, col2 VARCHAR)")
        self.validate_identity("CREATE TAG cost_center ALLOWED_VALUES 'a', 'b'")
        self.validate_identity("CREATE WAREHOUSE x").this.assert_is(exp.Identifier)
        self.validate_identity("CREATE STREAMLIT x").this.assert_is(exp.Identifier)
        self.validate_identity(
            "CREATE TEMPORARY STAGE stage1 FILE_FORMAT=(TYPE=PARQUET)"
        ).this.assert_is(exp.Table)
        self.validate_identity(
            "CREATE STAGE stage1 FILE_FORMAT='format1'",
            "CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME='format1')",
        )
        self.validate_identity("CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=stage1.format1)")
        self.validate_identity("CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME='stage1.format1')")
        self.validate_identity(
            "CREATE STAGE stage1 FILE_FORMAT=schema1.format1",
            "CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=schema1.format1)",
        )
        with self.assertRaises(ParseError):
            self.parse_one("CREATE STAGE stage1 FILE_FORMAT=123", dialect="snowflake")
        self.validate_identity(
            "CREATE STAGE s1 URL='s3://bucket-123' FILE_FORMAT=(TYPE='JSON') CREDENTIALS=(aws_key_id='test' aws_secret_key='test')"
        )
        self.validate_identity(
            "CREATE OR REPLACE TAG IF NOT EXISTS cost_center COMMENT='cost_center tag'"
        ).this.assert_is(exp.Identifier)
        self.validate_identity(
            "CREATE TEMPORARY FILE FORMAT fileformat1 TYPE=PARQUET COMPRESSION=auto"
        ).this.assert_is(exp.Table)
        self.validate_identity(
            "CREATE DYNAMIC TABLE product (pre_tax_profit, taxes, after_tax_profit) TARGET_LAG='20 minutes' WAREHOUSE=mywh AS SELECT revenue - cost, (revenue - cost) * tax_rate, (revenue - cost) * (1.0 - tax_rate) FROM staging_table"
        )
        self.validate_identity(
            "ALTER TABLE db_name.schmaName.tblName ADD COLUMN_1 VARCHAR NOT NULL TAG (key1='value_1')"
        )
        self.validate_identity(
            "DROP FUNCTION my_udf (OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN)))"
        )
        self.validate_identity(
            "CREATE TABLE orders_clone_restore CLONE orders AT (TIMESTAMP => TO_TIMESTAMP_TZ('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss'))"
        )
        self.validate_identity(
            "CREATE TABLE orders_clone_restore CLONE orders BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')"
        )
        self.validate_identity(
            "CREATE SCHEMA mytestschema_clone_restore CLONE testschema BEFORE (TIMESTAMP => TO_TIMESTAMP(40 * 365 * 86400))"
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE EXAMPLE_DB.DEMO.USERS (ID DECIMAL(38, 0) NOT NULL, PRIMARY KEY (ID), FOREIGN KEY (CITY_CODE) REFERENCES EXAMPLE_DB.DEMO.CITIES (CITY_CODE))"
        )
        self.validate_identity(
            "CREATE ICEBERG TABLE my_iceberg_table (amount ARRAY(INT)) CATALOG='SNOWFLAKE' EXTERNAL_VOLUME='my_external_volume' BASE_LOCATION='my/relative/path/from/extvol'"
        )
        self.validate_identity(
            """CREATE OR REPLACE FUNCTION ibis_udfs.public.object_values("obj" OBJECT) RETURNS ARRAY LANGUAGE JAVASCRIPT RETURNS NULL ON NULL INPUT AS ' return Object.values(obj) '"""
        )
        self.validate_identity(
            """CREATE OR REPLACE FUNCTION ibis_udfs.public.object_values("obj" OBJECT) RETURNS ARRAY LANGUAGE JAVASCRIPT STRICT AS ' return Object.values(obj) '"""
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE TEST (SOME_REF DECIMAL(38, 0) NOT NULL FOREIGN KEY REFERENCES SOME_OTHER_TABLE (ID))"
        )
        self.validate_identity(
            "CREATE OR REPLACE FUNCTION my_udf(location OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN))) RETURNS VARCHAR AS $$ SELECT 'foo' $$",
            "CREATE OR REPLACE FUNCTION my_udf(location OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN))) RETURNS VARCHAR AS ' SELECT \\'foo\\' '",
        )
        self.validate_identity(
            "CREATE OR REPLACE FUNCTION my_udtf(foo BOOLEAN) RETURNS TABLE(col1 ARRAY(INT)) AS $$ WITH t AS (SELECT CAST([1, 2, 3] AS ARRAY(INT)) AS c) SELECT c FROM t $$",
            "CREATE OR REPLACE FUNCTION my_udtf(foo BOOLEAN) RETURNS TABLE (col1 ARRAY(INT)) AS ' WITH t AS (SELECT CAST([1, 2, 3] AS ARRAY(INT)) AS c) SELECT c FROM t '",
        )
        self.validate_identity(
            "CREATE SEQUENCE seq1 WITH START=1, INCREMENT=1 ORDER",
            "CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 ORDER",
        )
        self.validate_identity(
            "CREATE SEQUENCE seq1 WITH START=1 INCREMENT=1 ORDER",
            "CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 ORDER",
        )
        self.validate_identity(
            """create external table et2(
  col1 date as (parse_json(metadata$external_table_partition):COL1::date),
  col2 varchar as (parse_json(metadata$external_table_partition):COL2::varchar),
  col3 number as (parse_json(metadata$external_table_partition):COL3::number))
  partition by (col1,col2,col3)
  location=@s2/logs/
  partition_type = user_specified
  file_format = (type = parquet compression = gzip binary_as_text = false)""",
            "CREATE EXTERNAL TABLE et2 (col1 DATE AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL1') AS DATE)), col2 VARCHAR AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL2') AS VARCHAR)), col3 DECIMAL(38, 0) AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL3') AS DECIMAL(38, 0)))) PARTITION BY (col1, col2, col3) LOCATION=@s2/logs/ partition_type=user_specified FILE_FORMAT=(type=parquet compression=gzip binary_as_text=FALSE)",
        )

        self.validate_all(
            "CREATE TABLE orders_clone CLONE orders",
            read={
                "bigquery": "CREATE TABLE orders_clone CLONE orders",
            },
            write={
                "bigquery": "CREATE TABLE orders_clone CLONE orders",
                "snowflake": "CREATE TABLE orders_clone CLONE orders",
            },
        )
        self.validate_all(
            "CREATE OR REPLACE TRANSIENT TABLE a (id INT)",
            read={
                "postgres": "CREATE OR REPLACE TRANSIENT TABLE a (id INT)",
                "snowflake": "CREATE OR REPLACE TRANSIENT TABLE a (id INT)",
            },
            write={
                "postgres": "CREATE OR REPLACE TABLE a (id INT)",
                "mysql": "CREATE OR REPLACE TABLE a (id INT)",
                "snowflake": "CREATE OR REPLACE TRANSIENT TABLE a (id INT)",
            },
        )
        self.validate_all(
            "CREATE TABLE a (b INT)",
            read={"teradata": "CREATE MULTISET TABLE a (b INT)"},
            write={"snowflake": "CREATE TABLE a (b INT)"},
        )

        self.validate_identity("CREATE TABLE a TAG (key1='value_1', key2='value_2')")
        self.validate_all(
            "CREATE TABLE a TAG (key1='value_1')",
            read={
                "snowflake": "CREATE TABLE a WITH TAG (key1='value_1')",
            },
        )

        for action in ("SET", "DROP"):
            with self.subTest(f"ALTER COLUMN {action} NOT NULL"):
                self.validate_all(
                    f"""
                        ALTER TABLE a
                        ALTER COLUMN my_column {action} NOT NULL;
                    """,
                    write={
                        "snowflake": f"ALTER TABLE a ALTER COLUMN my_column {action} NOT NULL",
                        "duckdb": f"ALTER TABLE a ALTER COLUMN my_column {action} NOT NULL",
                        "postgres": f"ALTER TABLE a ALTER COLUMN my_column {action} NOT NULL",
                    },
                )

    def test_user_defined_functions(self):
        self.validate_all(
            "CREATE FUNCTION a(x DATE, y BIGINT) RETURNS ARRAY LANGUAGE JAVASCRIPT AS $$ SELECT 1 $$",
            write={
                "snowflake": "CREATE FUNCTION a(x DATE, y BIGINT) RETURNS ARRAY LANGUAGE JAVASCRIPT AS ' SELECT 1 '",
            },
        )
        self.validate_all(
            "CREATE FUNCTION a() RETURNS TABLE (b INT) AS 'SELECT 1'",
            write={
                "snowflake": "CREATE FUNCTION a() RETURNS TABLE (b INT) AS 'SELECT 1'",
                "bigquery": "CREATE TABLE FUNCTION a() RETURNS TABLE <b INT64> AS SELECT 1",
            },
        )
        self.validate_all(
            "CREATE FUNCTION a() RETURNS INT IMMUTABLE AS 'SELECT 1'",
            write={
                "snowflake": "CREATE FUNCTION a() RETURNS INT IMMUTABLE AS 'SELECT 1'",
            },
        )

    def test_stored_procedures(self):
        self.validate_identity("CALL a.b.c(x, y)", check_command_warning=True)
        self.validate_identity(
            "CREATE PROCEDURE a.b.c(x INT, y VARIANT) RETURNS OBJECT EXECUTE AS CALLER AS 'BEGIN SELECT 1; END;'"
        )

    def test_table_function(self):
        self.validate_identity("SELECT * FROM TABLE('MYTABLE')")
        self.validate_identity("SELECT * FROM TABLE($MYVAR)")
        self.validate_identity("SELECT * FROM TABLE(?)")
        self.validate_identity("SELECT * FROM TABLE(:BINDING)")
        self.validate_identity("SELECT * FROM TABLE($MYVAR) WHERE COL1 = 10")
        self.validate_identity("SELECT * FROM TABLE('t1') AS f")
        self.validate_identity("SELECT * FROM (TABLE('t1') CROSS JOIN TABLE('t2'))")
        self.validate_identity("SELECT * FROM TABLE('t1'), LATERAL (SELECT * FROM t2)")
        self.validate_identity("SELECT * FROM TABLE('t1') UNION ALL SELECT * FROM TABLE('t2')")
        self.validate_identity("SELECT * FROM TABLE('t1') TABLESAMPLE BERNOULLI (20.3)")
        self.validate_identity("""SELECT * FROM TABLE('MYDB."MYSCHEMA"."MYTABLE"')""")
        self.validate_identity(
            'SELECT * FROM TABLE($$MYDB. "MYSCHEMA"."MYTABLE"$$)',
            """SELECT * FROM TABLE('MYDB. "MYSCHEMA"."MYTABLE"')""",
        )

    def test_flatten(self):
        self.assertEqual(
            exp.select(exp.Explode(this=exp.column("x")).as_("y", quoted=True)).sql(
                "snowflake", pretty=True
            ),
            """SELECT
  IFF(_u.pos = _u_2.pos_2, _u_2."y", NULL) AS "y"
FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (
  GREATEST(ARRAY_SIZE(x)) - 1
) + 1))) AS _u(seq, key, path, index, pos, this)
CROSS JOIN TABLE(FLATTEN(INPUT => x)) AS _u_2(seq, key, path, pos_2, "y", this)
WHERE
  _u.pos = _u_2.pos_2
  OR (
    _u.pos > (
      ARRAY_SIZE(x) - 1
    ) AND _u_2.pos_2 = (
      ARRAY_SIZE(x) - 1
    )
  )""",
        )

        self.validate_all(
            """
            select
              dag_report.acct_id,
              dag_report.report_date,
              dag_report.report_uuid,
              dag_report.airflow_name,
              dag_report.dag_id,
              f.value::varchar as operator
            from cs.telescope.dag_report,
            table(flatten(input=>split(operators, ','))) f
            """,
            write={
                "snowflake": """SELECT
  dag_report.acct_id,
  dag_report.report_date,
  dag_report.report_uuid,
  dag_report.airflow_name,
  dag_report.dag_id,
  CAST(f.value AS VARCHAR) AS operator
FROM cs.telescope.dag_report, TABLE(FLATTEN(input => SPLIT(operators, ','))) AS f"""
            },
            pretty=True,
        )
        self.validate_all(
            """
            SELECT
              uc.user_id,
              uc.start_ts AS ts,
              CASE
                WHEN uc.start_ts::DATE >= '2023-01-01' AND uc.country_code IN ('US') AND uc.user_id NOT IN (
                  SELECT DISTINCT
                    _id
                  FROM
                    users,
                    LATERAL FLATTEN(INPUT => PARSE_JSON(flags)) datasource
                  WHERE datasource.value:name = 'something'
                )
                  THEN 'Sample1'
                  ELSE 'Sample2'
              END AS entity
            FROM user_countries AS uc
            LEFT JOIN (
              SELECT user_id, MAX(IFF(service_entity IS NULL,1,0)) AS le_null
              FROM accepted_user_agreements
              GROUP BY 1
            ) AS aua
              ON uc.user_id = aua.user_id
            """,
            write={
                "snowflake": """SELECT
  uc.user_id,
  uc.start_ts AS ts,
  CASE
    WHEN CAST(uc.start_ts AS DATE) >= '2023-01-01'
    AND uc.country_code IN ('US')
    AND uc.user_id <> ALL (
      SELECT DISTINCT
        _id
      FROM users, LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS datasource(SEQ, KEY, PATH, INDEX, VALUE, THIS)
      WHERE
        GET_PATH(datasource.value, 'name') = 'something'
    )
    THEN 'Sample1'
    ELSE 'Sample2'
  END AS entity
FROM user_countries AS uc
LEFT JOIN (
  SELECT
    user_id,
    MAX(IFF(service_entity IS NULL, 1, 0)) AS le_null
  FROM accepted_user_agreements
  GROUP BY
    1
) AS aua
  ON uc.user_id = aua.user_id
CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (
  GREATEST(ARRAY_SIZE(INPUT => PARSE_JSON(flags))) - 1
) + 1))) AS _u(seq, key, path, index, pos, this)
CROSS JOIN TABLE(FLATTEN(INPUT => PARSE_JSON(flags))) AS _u_2(seq, key, path, pos_2, entity, this)
WHERE
  _u.pos = _u_2.pos_2
  OR (
    _u.pos > (
      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1
    )
    AND _u_2.pos_2 = (
      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1
    )
  )""",
            },
            pretty=True,
        )

        # All examples from https://docs.snowflake.com/en/sql-reference/functions/flatten.html#syntax
        self.validate_all(
            "SELECT * FROM TABLE(FLATTEN(input => parse_json('[1, ,77]'))) f",
            write={
                "snowflake": "SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('[1, ,77]'))) AS f"
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('{"a":1, "b":[77,88]}'), outer => TRUE)) AS f"""
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), path => 'b')) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('{"a":1, "b":[77,88]}'), path => 'b')) AS f"""
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('[]'))) f""",
            write={"snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('[]'))) AS f"""},
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('[]'), outer => true)) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('[]'), outer => TRUE)) AS f"""
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88], "c": {"d":"X"}}'))) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'))) AS f"""
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88], "c": {"d":"X"}}'), recursive => true)) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), recursive => TRUE)) AS f"""
            },
        )

        self.validate_all(
            """SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88], "c": {"d":"X"}}'), recursive => true, mode => 'object')) f""",
            write={
                "snowflake": """SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), recursive => TRUE, mode => 'object')) AS f"""
            },
        )

        self.validate_all(
            """
            SELECT id as "ID",
              f.value AS "Contact",
              f1.value:type AS "Type",
              f1.value:content AS "Details"
            FROM persons p,
              lateral flatten(input => p.c, path => 'contact') f,
              lateral flatten(input => f.value:business) f1
            """,
            write={
                "snowflake": """SELECT
  id AS "ID",
  f.value AS "Contact",
  GET_PATH(f1.value, 'type') AS "Type",
  GET_PATH(f1.value, 'content') AS "Details"
FROM persons AS p, LATERAL FLATTEN(input => p.c, path => 'contact') AS f(SEQ, KEY, PATH, INDEX, VALUE, THIS), LATERAL FLATTEN(input => GET_PATH(f.value, 'business')) AS f1(SEQ, KEY, PATH, INDEX, VALUE, THIS)""",
            },
            pretty=True,
        )

        self.validate_all(
            """
            SELECT id as "ID",
              value AS "Contact"
            FROM persons p,
              lateral flatten(input => p.c, path => 'contact')
            """,
            write={
                "snowflake": """SELECT
  id AS "ID",
  value AS "Contact"
FROM persons AS p, LATERAL FLATTEN(input => p.c, path => 'contact') AS _flattened(SEQ, KEY, PATH, INDEX, VALUE, THIS)""",
            },
            pretty=True,
        )

    def test_minus(self):
        self.validate_all(
            "SELECT 1 EXCEPT SELECT 1",
            read={
                "oracle": "SELECT 1 MINUS SELECT 1",
                "snowflake": "SELECT 1 MINUS SELECT 1",
            },
        )

    def test_values(self):
        select = exp.select("*").from_("values (map(['a'], [1]))")
        self.assertEqual(select.sql("snowflake"), "SELECT * FROM (SELECT OBJECT_CONSTRUCT('a', 1))")

        self.validate_all(
            'SELECT "c0", "c1" FROM (VALUES (1, 2), (3, 4)) AS "t0"("c0", "c1")',
            read={
                "spark": "SELECT `c0`, `c1` FROM (VALUES (1, 2), (3, 4)) AS `t0`(`c0`, `c1`)",
            },
        )
        self.validate_all(
            """SELECT $1 AS "_1" FROM VALUES ('a'), ('b')""",
            write={
                "snowflake": """SELECT $1 AS "_1" FROM (VALUES ('a'), ('b'))""",
                "spark": """SELECT ${1} AS `_1` FROM VALUES ('a'), ('b')""",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT OBJECT_CONSTRUCT('a', 1) AS x) AS t",
            read={
                "duckdb": "SELECT * FROM (VALUES ({'a': 1})) AS t(x)",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT OBJECT_CONSTRUCT('a', 1) AS x UNION ALL SELECT OBJECT_CONSTRUCT('a', 2)) AS t",
            read={
                "duckdb": "SELECT * FROM (VALUES ({'a': 1}), ({'a': 2})) AS t(x)",
            },
        )

    def test_describe(self):
        self.validate_identity("DESCRIBE SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM")
        self.validate_identity(
            "DESC SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM",
            "DESCRIBE SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM",
        )

        self.validate_all(
            "DESCRIBE TABLE db.table",
            write={
                "snowflake": "DESCRIBE TABLE db.table",
                "spark": "DESCRIBE db.table",
            },
        )
        self.validate_all(
            "DESCRIBE db.table",
            write={
                "snowflake": "DESCRIBE TABLE db.table",
                "spark": "DESCRIBE db.table",
            },
        )
        self.validate_all(
            "DESC TABLE db.table",
            write={
                "snowflake": "DESCRIBE TABLE db.table",
                "spark": "DESCRIBE db.table",
            },
        )
        self.validate_all(
            "DESC VIEW db.table",
            write={
                "snowflake": "DESCRIBE VIEW db.table",
                "spark": "DESCRIBE db.table",
            },
        )
        self.validate_all(
            "ENDSWITH('abc', 'c')",
            read={
                "bigquery": "ENDS_WITH('abc', 'c')",
                "clickhouse": "endsWith('abc', 'c')",
                "databricks": "ENDSWITH('abc', 'c')",
                "duckdb": "ENDS_WITH('abc', 'c')",
                "presto": "ENDS_WITH('abc', 'c')",
                "spark": "ENDSWITH('abc', 'c')",
            },
            write={
                "bigquery": "ENDS_WITH('abc', 'c')",
                "clickhouse": "endsWith('abc', 'c')",
                "databricks": "ENDSWITH('abc', 'c')",
                "duckdb": "ENDS_WITH('abc', 'c')",
                "presto": "ENDS_WITH('abc', 'c')",
                "snowflake": "ENDSWITH('abc', 'c')",
                "spark": "ENDSWITH('abc', 'c')",
            },
        )

    def test_parse_like_any(self):
        for keyword in ("LIKE", "ILIKE"):
            ast = self.validate_identity(f"a {keyword} ANY FUN('foo')")

            ast.sql()  # check that this doesn't raise

    @mock.patch("sqlglot.generator.logger")
    def test_regexp_substr(self, logger):
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern, pos, occ, params, group)",
            write={
                "bigquery": "REGEXP_EXTRACT(subject, pattern, pos, occ)",
                "hive": "REGEXP_EXTRACT(subject, pattern, group)",
                "presto": 'REGEXP_EXTRACT(subject, pattern, "group")',
                "snowflake": "REGEXP_SUBSTR(subject, pattern, pos, occ, params, group)",
                "spark": "REGEXP_EXTRACT(subject, pattern, group)",
            },
        )
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern)",
            read={
                "bigquery": "REGEXP_EXTRACT(subject, pattern)",
            },
            write={
                "bigquery": "REGEXP_EXTRACT(subject, pattern)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern)",
            },
        )
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', 1)",
            read={
                "hive": "REGEXP_EXTRACT(subject, pattern)",
                "spark2": "REGEXP_EXTRACT(subject, pattern)",
                "spark": "REGEXP_EXTRACT(subject, pattern)",
                "databricks": "REGEXP_EXTRACT(subject, pattern)",
            },
            write={
                "hive": "REGEXP_EXTRACT(subject, pattern)",
                "spark2": "REGEXP_EXTRACT(subject, pattern)",
                "spark": "REGEXP_EXTRACT(subject, pattern)",
                "databricks": "REGEXP_EXTRACT(subject, pattern)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', 1)",
            },
        )
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', group)",
            read={
                "duckdb": "REGEXP_EXTRACT(subject, pattern, group)",
                "hive": "REGEXP_EXTRACT(subject, pattern, group)",
                "presto": "REGEXP_EXTRACT(subject, pattern, group)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', group)",
                "spark": "REGEXP_EXTRACT(subject, pattern, group)",
            },
        )

        self.validate_identity(
            "REGEXP_SUBSTR_ALL(subject, pattern, pos, occ, param, group)",
            "REGEXP_EXTRACT_ALL(subject, pattern, pos, occ, param, group)",
        )

        self.validate_identity("SELECT SEARCH((play, line), 'dream')")
        self.validate_identity("SELECT SEARCH(line, 'king', ANALYZER => 'UNICODE_ANALYZER')")
        self.validate_identity("SELECT SEARCH(character, 'king queen', SEARCH_MODE => 'AND')")
        self.validate_identity(
            "SELECT SEARCH(line, 'king', ANALYZER => 'UNICODE_ANALYZER', SEARCH_MODE => 'OR')"
        )

        # AST validation tests - verify argument mapping
        ast = self.validate_identity("SELECT SEARCH(line, 'king')")
        search_ast = ast.find(exp.Search)
        self.assertEqual(list(search_ast.args), ["this", "expression"])
        self.assertIsNone(search_ast.args.get("analyzer"))
        self.assertIsNone(search_ast.args.get("search_mode"))

        ast = self.validate_identity("SELECT SEARCH(line, 'king', ANALYZER => 'UNICODE_ANALYZER')")
        search_ast = ast.find(exp.Search)
        self.assertIsNotNone(search_ast.args.get("analyzer"))
        self.assertIsNone(search_ast.args.get("search_mode"))

        ast = self.validate_identity("SELECT SEARCH(character, 'king queen', SEARCH_MODE => 'AND')")
        search_ast = ast.find(exp.Search)
        self.assertIsNone(search_ast.args.get("analyzer"))
        self.assertIsNotNone(search_ast.args.get("search_mode"))

        # Test with arguments in different order (search_mode first, then analyzer)
        ast = self.validate_identity(
            "SELECT SEARCH(line, 'king', SEARCH_MODE => 'AND', ANALYZER => 'PATTERN_ANALYZER')",
            "SELECT SEARCH(line, 'king', ANALYZER => 'PATTERN_ANALYZER', SEARCH_MODE => 'AND')",
        )
        search_ast = ast.find(exp.Search)
        self.assertEqual(list(search_ast.args), ["this", "expression", "search_mode", "analyzer"])
        analyzer = search_ast.args.get("analyzer")
        self.assertIsNotNone(analyzer)
        search_mode = search_ast.args.get("search_mode")
        self.assertIsNotNone(search_mode)

        self.validate_identity("SELECT SEARCH_IP(col, '192.168.0.0')").selects[0].assert_is(
            exp.SearchIp
        )

        self.validate_identity("SELECT REGEXP_COUNT('hello world', 'l ')")
        self.validate_identity("SELECT REGEXP_COUNT('hello world', 'l', 1)")
        self.validate_identity("SELECT REGEXP_COUNT('hello world', 'l', 1, 'i')")

    @mock.patch("sqlglot.generator.logger")
    def test_regexp_replace(self, logger):
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern)",
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, '')",
                "duckdb": "REGEXP_REPLACE(subject, pattern, '', 'g')",
                "hive": "REGEXP_REPLACE(subject, pattern, '')",
                "snowflake": "REGEXP_REPLACE(subject, pattern, '')",
                "spark": "REGEXP_REPLACE(subject, pattern, '')",
            },
        )
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern, replacement)",
            read={
                "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement)",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement)",
            },
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement, 'g')",
                "postgres": "REGEXP_REPLACE(subject, pattern, replacement, 'g')",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement)",
            },
        )
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern, replacement, position)",
            read={
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
            },
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement, 'g')",
                "postgres": "REGEXP_REPLACE(subject, pattern, replacement, position, 'g')",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
            },
        )
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, 'c')",
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement, 'c')",
                "postgres": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, 'c')",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, 'c')",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
            },
        )

        self.validate_all(
            "REGEXP_REPLACE(subject, pattern, replacement, 1, 0, 'c')",
            write={
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, 1, 0, 'c')",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement, 'cg')",
                "postgres": "REGEXP_REPLACE(subject, pattern, replacement, 1, 0, 'cg')",
            },
        )

    def test_replace(self):
        self.validate_all(
            "REPLACE(subject, pattern)",
            write={
                "bigquery": "REPLACE(subject, pattern, '')",
                "duckdb": "REPLACE(subject, pattern, '')",
                "hive": "REPLACE(subject, pattern, '')",
                "snowflake": "REPLACE(subject, pattern, '')",
                "spark": "REPLACE(subject, pattern, '')",
            },
        )
        self.validate_all(
            "REPLACE(subject, pattern, replacement)",
            read={
                "bigquery": "REPLACE(subject, pattern, replacement)",
                "duckdb": "REPLACE(subject, pattern, replacement)",
                "hive": "REPLACE(subject, pattern, replacement)",
                "spark": "REPLACE(subject, pattern, replacement)",
            },
            write={
                "bigquery": "REPLACE(subject, pattern, replacement)",
                "duckdb": "REPLACE(subject, pattern, replacement)",
                "hive": "REPLACE(subject, pattern, replacement)",
                "snowflake": "REPLACE(subject, pattern, replacement)",
                "spark": "REPLACE(subject, pattern, replacement)",
            },
        )

    def test_match_recognize(self):
        for window_frame in ("", "FINAL ", "RUNNING "):
            for row in (
                "ONE ROW PER MATCH",
                "ALL ROWS PER MATCH",
                "ALL ROWS PER MATCH SHOW EMPTY MATCHES",
                "ALL ROWS PER MATCH OMIT EMPTY MATCHES",
                "ALL ROWS PER MATCH WITH UNMATCHED ROWS",
            ):
                for after in (
                    "AFTER MATCH SKIP",
                    "AFTER MATCH SKIP PAST LAST ROW",
                    "AFTER MATCH SKIP TO NEXT ROW",
                    "AFTER MATCH SKIP TO FIRST x",
                    "AFTER MATCH SKIP TO LAST x",
                ):
                    with self.subTest(
                        f"MATCH_RECOGNIZE with window frame {window_frame}, rows {row}, after {after}: "
                    ):
                        self.validate_identity(
                            f"""SELECT
  *
FROM x
MATCH_RECOGNIZE (
  PARTITION BY a, b
  ORDER BY
    x DESC
  MEASURES
    {window_frame}y AS b
  {row}
  {after}
  PATTERN (^ S1 S2*? ( {{- S3 -}} S4 )+ | PERMUTE(S1, S2){{1,2}} $)
  DEFINE
    x AS y
)""",
                            pretty=True,
                        )

    def test_show_users(self):
        self.validate_identity("SHOW USERS")
        self.validate_identity("SHOW TERSE USERS")
        self.validate_identity("SHOW USERS LIKE '_foo%' STARTS WITH 'bar' LIMIT 5 FROM 'baz'")

    def test_show_databases(self):
        self.validate_identity("SHOW TERSE DATABASES")
        self.validate_identity(
            "SHOW TERSE DATABASES HISTORY LIKE 'foo' STARTS WITH 'bla' LIMIT 5 FROM 'bob' WITH PRIVILEGES USAGE, MODIFY"
        )

        ast = parse_one("SHOW DATABASES IN ACCOUNT", read="snowflake")
        self.assertEqual(ast.this, "DATABASES")
        self.assertEqual(ast.args.get("scope_kind"), "ACCOUNT")

    def test_show_file_formats(self):
        self.validate_identity("SHOW FILE FORMATS")
        self.validate_identity("SHOW FILE FORMATS LIKE 'foo' IN DATABASE db1")
        self.validate_identity("SHOW FILE FORMATS LIKE 'foo' IN SCHEMA db1.schema1")

        ast = parse_one("SHOW FILE FORMATS IN ACCOUNT", read="snowflake")
        self.assertEqual(ast.this, "FILE FORMATS")
        self.assertEqual(ast.args.get("scope_kind"), "ACCOUNT")

    def test_show_functions(self):
        self.validate_identity("SHOW FUNCTIONS")
        self.validate_identity("SHOW FUNCTIONS LIKE 'foo' IN CLASS bla")

        ast = parse_one("SHOW FUNCTIONS IN ACCOUNT", read="snowflake")
        self.assertEqual(ast.this, "FUNCTIONS")
        self.assertEqual(ast.args.get("scope_kind"), "ACCOUNT")

    def test_show_procedures(self):
        self.validate_identity("SHOW PROCEDURES")
        self.validate_identity("SHOW PROCEDURES LIKE 'foo' IN APPLICATION app")
        self.validate_identity("SHOW PROCEDURES LIKE 'foo' IN APPLICATION PACKAGE pkg")

        ast = parse_one("SHOW PROCEDURES IN ACCOUNT", read="snowflake")
        self.assertEqual(ast.this, "PROCEDURES")
        self.assertEqual(ast.args.get("scope_kind"), "ACCOUNT")

    def test_show_stages(self):
        self.validate_identity("SHOW STAGES")
        self.validate_identity("SHOW STAGES LIKE 'foo' IN DATABASE db1")
        self.validate_identity("SHOW STAGES LIKE 'foo' IN SCHEMA db1.schema1")

        ast = parse_one("SHOW STAGES IN ACCOUNT", read="snowflake")
        self.assertEqual(ast.this, "STAGES")
        self.assertEqual(ast.args.get("scope_kind"), "ACCOUNT")

    def test_show_warehouses(self):
        self.validate_identity("SHOW WAREHOUSES")
        self.validate_identity("SHOW WAREHOUSES LIKE 'foo' WITH PRIVILEGES USAGE, MODIFY")

        ast = parse_one("SHOW WAREHOUSES", read="snowflake")
        self.assertEqual(ast.this, "WAREHOUSES")

    def test_show_schemas(self):
        self.validate_identity(
            "show terse schemas in database db1 starts with 'a' limit 10 from 'b'",
            "SHOW TERSE SCHEMAS IN DATABASE db1 STARTS WITH 'a' LIMIT 10 FROM 'b'",
        )

        ast = parse_one("SHOW SCHEMAS IN DATABASE db1", read="snowflake")
        self.assertEqual(ast.args.get("scope_kind"), "DATABASE")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), "db1")

    def test_show_objects(self):
        self.validate_identity(
            "show terse objects in schema db1.schema1 starts with 'a' limit 10 from 'b'",
            "SHOW TERSE OBJECTS IN SCHEMA db1.schema1 STARTS WITH 'a' LIMIT 10 FROM 'b'",
        )
        self.validate_identity(
            "show terse objects in db1.schema1 starts with 'a' limit 10 from 'b'",
            "SHOW TERSE OBJECTS IN SCHEMA db1.schema1 STARTS WITH 'a' LIMIT 10 FROM 'b'",
        )

        ast = parse_one("SHOW OBJECTS IN db1.schema1", read="snowflake")
        self.assertEqual(ast.args.get("scope_kind"), "SCHEMA")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), "db1.schema1")

    def test_show_columns(self):
        self.validate_identity("SHOW COLUMNS")
        self.validate_identity("SHOW COLUMNS IN TABLE dt_test")
        self.validate_identity("SHOW COLUMNS LIKE '_foo%' IN TABLE dt_test")
        self.validate_identity("SHOW COLUMNS IN VIEW")
        self.validate_identity("SHOW COLUMNS LIKE '_foo%' IN VIEW dt_test")

        ast = parse_one("SHOW COLUMNS LIKE '_testing%' IN dt_test", read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), "dt_test")
        self.assertEqual(ast.find(exp.Literal).sql(dialect="snowflake"), "'_testing%'")

    def test_show_tables(self):
        self.validate_identity(
            "SHOW TABLES LIKE 'line%' IN tpch.public",
            "SHOW TABLES LIKE 'line%' IN SCHEMA tpch.public",
        )
        self.validate_identity(
            "SHOW TABLES HISTORY IN tpch.public",
            "SHOW TABLES HISTORY IN SCHEMA tpch.public",
        )
        self.validate_identity(
            "show terse tables in schema db1.schema1 starts with 'a' limit 10 from 'b'",
            "SHOW TERSE TABLES IN SCHEMA db1.schema1 STARTS WITH 'a' LIMIT 10 FROM 'b'",
        )
        self.validate_identity(
            "show terse tables in db1.schema1 starts with 'a' limit 10 from 'b'",
            "SHOW TERSE TABLES IN SCHEMA db1.schema1 STARTS WITH 'a' LIMIT 10 FROM 'b'",
        )

        ast = parse_one("SHOW TABLES IN db1.schema1", read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), "db1.schema1")

    def test_show_primary_keys(self):
        self.validate_identity("SHOW PRIMARY KEYS")
        self.validate_identity("SHOW PRIMARY KEYS IN ACCOUNT")
        self.validate_identity("SHOW PRIMARY KEYS IN DATABASE")
        self.validate_identity("SHOW PRIMARY KEYS IN DATABASE foo")
        self.validate_identity("SHOW PRIMARY KEYS IN TABLE")
        self.validate_identity("SHOW PRIMARY KEYS IN TABLE foo")
        self.validate_identity(
            'SHOW PRIMARY KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW PRIMARY KEYS IN TABLE "TEST"."PUBLIC"."foo"',
        )
        self.validate_identity(
            'SHOW TERSE PRIMARY KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW PRIMARY KEYS IN TABLE "TEST"."PUBLIC"."foo"',
        )

        ast = parse_one('SHOW PRIMARY KEYS IN "TEST"."PUBLIC"."foo"', read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), '"TEST"."PUBLIC"."foo"')

    def test_show_views(self):
        self.validate_identity("SHOW TERSE VIEWS")
        self.validate_identity("SHOW VIEWS")
        self.validate_identity("SHOW VIEWS LIKE 'foo%'")
        self.validate_identity("SHOW VIEWS IN ACCOUNT")
        self.validate_identity("SHOW VIEWS IN DATABASE")
        self.validate_identity("SHOW VIEWS IN DATABASE foo")
        self.validate_identity("SHOW VIEWS IN SCHEMA foo")
        self.validate_identity(
            "SHOW VIEWS IN foo",
            "SHOW VIEWS IN SCHEMA foo",
        )

        ast = parse_one("SHOW VIEWS IN db1.schema1", read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), "db1.schema1")

    def test_show_unique_keys(self):
        self.validate_identity("SHOW UNIQUE KEYS")
        self.validate_identity("SHOW UNIQUE KEYS IN ACCOUNT")
        self.validate_identity("SHOW UNIQUE KEYS IN DATABASE")
        self.validate_identity("SHOW UNIQUE KEYS IN DATABASE foo")
        self.validate_identity("SHOW UNIQUE KEYS IN TABLE")
        self.validate_identity("SHOW UNIQUE KEYS IN TABLE foo")
        self.validate_identity(
            'SHOW UNIQUE KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW UNIQUE KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
        )
        self.validate_identity(
            'SHOW TERSE UNIQUE KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW UNIQUE KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
        )

        ast = parse_one('SHOW UNIQUE KEYS IN "TEST"."PUBLIC"."foo"', read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), '"TEST"."PUBLIC"."foo"')

    def test_show_imported_keys(self):
        self.validate_identity("SHOW IMPORTED KEYS")
        self.validate_identity("SHOW IMPORTED KEYS IN ACCOUNT")
        self.validate_identity("SHOW IMPORTED KEYS IN DATABASE")
        self.validate_identity("SHOW IMPORTED KEYS IN DATABASE foo")
        self.validate_identity("SHOW IMPORTED KEYS IN TABLE")
        self.validate_identity("SHOW IMPORTED KEYS IN TABLE foo")
        self.validate_identity(
            'SHOW IMPORTED KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW IMPORTED KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
        )
        self.validate_identity(
            'SHOW TERSE IMPORTED KEYS IN "TEST"."PUBLIC"."foo"',
            'SHOW IMPORTED KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
        )

        ast = parse_one('SHOW IMPORTED KEYS IN "TEST"."PUBLIC"."foo"', read="snowflake")
        self.assertEqual(ast.find(exp.Table).sql(dialect="snowflake"), '"TEST"."PUBLIC"."foo"')

    def test_show_sequences(self):
        self.validate_identity("SHOW TERSE SEQUENCES")
        self.validate_identity("SHOW SEQUENCES")
        self.validate_identity("SHOW SEQUENCES LIKE '_foo%' IN ACCOUNT")
        self.validate_identity("SHOW SEQUENCES LIKE '_foo%' IN DATABASE")
        self.validate_identity("SHOW SEQUENCES LIKE '_foo%' IN DATABASE foo")
        self.validate_identity("SHOW SEQUENCES LIKE '_foo%' IN SCHEMA")
        self.validate_identity("SHOW SEQUENCES LIKE '_foo%' IN SCHEMA foo")
        self.validate_identity(
            "SHOW SEQUENCES LIKE '_foo%' IN foo",
            "SHOW SEQUENCES LIKE '_foo%' IN SCHEMA foo",
        )

        ast = parse_one("SHOW SEQUENCES IN dt_test", read="snowflake")
        self.assertEqual(ast.args.get("scope_kind"), "SCHEMA")

    def test_storage_integration(self):
        self.validate_identity(
            """CREATE STORAGE INTEGRATION s3_int
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER='S3'
STORAGE_AWS_ROLE_ARN='arn:aws:iam::001234567890:role/myrole'
ENABLED=TRUE
STORAGE_ALLOWED_LOCATIONS=('s3://mybucket1/path1/', 's3://mybucket2/path2/')""",
            pretty=True,
        ).this.assert_is(exp.Identifier)

    def test_swap(self):
        ast = parse_one("ALTER TABLE a SWAP WITH b", read="snowflake")
        assert isinstance(ast, exp.Alter)
        assert isinstance(ast.args["actions"][0], exp.SwapTable)

    def test_try_cast(self):
        self.validate_all("TRY_CAST('foo' AS VARCHAR)", read={"hive": "CAST('foo' AS STRING)"})
        self.validate_all("CAST(5 + 5 AS VARCHAR)", read={"hive": "CAST(5 + 5 AS STRING)"})
        self.validate_all(
            "CAST(TRY_CAST('2020-01-01' AS DATE) AS VARCHAR)",
            read={
                "hive": "CAST(CAST('2020-01-01' AS DATE) AS STRING)",
                "snowflake": "CAST(TRY_CAST('2020-01-01' AS DATE) AS VARCHAR)",
            },
        )
        self.validate_all(
            "TRY_CAST('val' AS VARCHAR)",
            read={
                "hive": "CAST('val' AS STRING)",
                "snowflake": "TRY_CAST('val' AS VARCHAR)",
            },
        )
        self.validate_identity("SELECT TRY_CAST(x AS DOUBLE)")
        self.validate_identity(
            "SELECT TRY_CAST(FOO() AS TEXT)", "SELECT TRY_CAST(FOO() AS VARCHAR)"
        )

        expression = parse_one("SELECT CAST(t.x AS STRING) FROM t", read="hive")

        for value_type in ("string", "int"):
            with self.subTest(
                f"Testing Hive -> Snowflake CAST/TRY_CAST conversion for {value_type}"
            ):
                func = "TRY_CAST" if value_type == "string" else "CAST"

                expression = annotate_types(expression, schema={"t": {"x": value_type}})
                self.assertEqual(
                    expression.sql(dialect="snowflake"), f"SELECT {func}(t.x AS VARCHAR) FROM t"
                )

    def test_decfloat(self):
        self.validate_all(
            "SELECT CAST(1.5 AS DECFLOAT)",
            write={
                "snowflake": "SELECT CAST(1.5 AS DECFLOAT)",
                "duckdb": "SELECT CAST(1.5 AS DECIMAL(38, 5))",
            },
        )
        self.validate_all(
            "CREATE TABLE t (x DECFLOAT)",
            write={
                "snowflake": "CREATE TABLE t (x DECFLOAT)",
                "duckdb": "CREATE TABLE t (x DECIMAL(38, 5))",
            },
        )

    def test_copy(self):
        self.validate_identity("COPY INTO test (c1) FROM (SELECT $1.c1 FROM @mystage)")
        self.validate_identity(
            """COPY INTO temp FROM @random_stage/path/ FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('str1', 'str2') FIELD_OPTIONALLY_ENCLOSED_BY='"' TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9' DATE_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9' BINARY_FORMAT=BASE64) VALIDATION_MODE = 'RETURN_3_ROWS'"""
        )
        self.validate_identity(
            """COPY INTO load1 FROM @%load1/data1/ CREDENTIALS = (AWS_KEY_ID='id' AWS_SECRET_KEY='key' AWS_TOKEN='token') FILES = ('test1.csv', 'test2.csv') FORCE = TRUE"""
        )
        self.validate_identity(
            """COPY INTO mytable FROM 'azure://myaccount.blob.core.windows.net/mycontainer/data/files' CREDENTIALS = (AZURE_SAS_TOKEN='token') ENCRYPTION = (TYPE='AZURE_CSE' MASTER_KEY='kPx...') FILE_FORMAT = (FORMAT_NAME=my_csv_format)"""
        )
        self.validate_identity(
            """COPY INTO mytable (col1, col2) FROM 's3://mybucket/data/files' STORAGE_INTEGRATION = "storage" ENCRYPTION = (TYPE='NONE' MASTER_KEY='key') FILES = ('file1', 'file2') PATTERN = 'pattern' FILE_FORMAT = (FORMAT_NAME=my_csv_format NULL_IF=('')) PARSE_HEADER = TRUE"""
        )
        self.validate_identity(
            """COPY INTO @my_stage/result/data FROM (SELECT * FROM orderstiny) FILE_FORMAT = (TYPE='csv')"""
        )
        self.validate_identity("COPY INTO mytable FILE_FORMAT = (TYPE='csv')")
        self.validate_identity(
            """COPY INTO MY_DATABASE.MY_SCHEMA.MY_TABLE FROM @MY_DATABASE.MY_SCHEMA.MY_STAGE/my_path FILE_FORMAT = (FORMAT_NAME=MY_DATABASE.MY_SCHEMA.MY_FILE_FORMAT)"""
        )
        self.validate_all(
            """COPY INTO 's3://example/data.csv'
    FROM EXTRA.EXAMPLE.TABLE
    CREDENTIALS = ()
    FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE NULL_IF = ('') FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    HEADER = TRUE
    OVERWRITE = TRUE
    SINGLE = TRUE
            """,
            write={
                "": """COPY INTO 's3://example/data.csv'
FROM EXTRA.EXAMPLE.TABLE
CREDENTIALS = () WITH (
  FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(
    ''
  ) FIELD_OPTIONALLY_ENCLOSED_BY='"'),
  HEADER TRUE,
  OVERWRITE TRUE,
  SINGLE TRUE
)""",
                "snowflake": """COPY INTO 's3://example/data.csv'
FROM EXTRA.EXAMPLE.TABLE
CREDENTIALS = ()
FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(
  ''
) FIELD_OPTIONALLY_ENCLOSED_BY='"')
HEADER = TRUE
OVERWRITE = TRUE
SINGLE = TRUE""",
            },
            pretty=True,
        )
        self.validate_all(
            """COPY INTO 's3://example/data.csv'
    FROM EXTRA.EXAMPLE.TABLE
    STORAGE_INTEGRATION = S3_INTEGRATION
    FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"')
    HEADER = TRUE
    OVERWRITE = TRUE
    SINGLE = TRUE
            """,
            write={
                "": """COPY INTO 's3://example/data.csv' FROM EXTRA.EXAMPLE.TABLE STORAGE_INTEGRATION = S3_INTEGRATION WITH (FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"'), HEADER TRUE, OVERWRITE TRUE, SINGLE TRUE)""",
                "snowflake": """COPY INTO 's3://example/data.csv' FROM EXTRA.EXAMPLE.TABLE STORAGE_INTEGRATION = S3_INTEGRATION FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"') HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE""",
            },
        )

        copy_ast = parse_one(
            """COPY INTO 's3://example/contacts.csv' FROM db.tbl STORAGE_INTEGRATION = PROD_S3_SIDETRADE_INTEGRATION FILE_FORMAT = (FORMAT_NAME=my_csv_format TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"') MATCH_BY_COLUMN_NAME = CASE_SENSITIVE OVERWRITE = TRUE SINGLE = TRUE INCLUDE_METADATA = (col1 = METADATA$START_SCAN_TIME)""",
            read="snowflake",
        )
        self.assertEqual(
            quote_identifiers(copy_ast, dialect="snowflake").sql(dialect="snowflake"),
            """COPY INTO 's3://example/contacts.csv' FROM "db"."tbl" STORAGE_INTEGRATION = "PROD_S3_SIDETRADE_INTEGRATION" FILE_FORMAT = (FORMAT_NAME="my_csv_format" TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"') MATCH_BY_COLUMN_NAME = CASE_SENSITIVE OVERWRITE = TRUE SINGLE = TRUE INCLUDE_METADATA = ("col1" = "METADATA$START_SCAN_TIME")""",
        )

    def test_put_to_stage(self):
        self.validate_identity('PUT \'file:///dir/tmp.csv\' @"my_DB"."schEMA1"."MYstage"')

        # PUT with file path and stage ref containing spaces (wrapped in single quotes)
        ast = parse_one("PUT 'file://my file.txt' '@s1/my folder'", read="snowflake")
        self.assertIsInstance(ast, exp.Put)
        self.assertEqual(ast.this, exp.Literal(this="file://my file.txt", is_string=True))
        self.assertEqual(ast.args["target"], exp.Var(this="'@s1/my folder'"))
        self.assertEqual(ast.sql("snowflake"), "PUT 'file://my file.txt' '@s1/my folder'")

        # expression with additional properties
        ast = parse_one(
            "PUT 'file:///tmp/my.txt' @stage1/folder PARALLEL = 1 AUTO_COMPRESS=false source_compression=gzip OVERWRITE=TRUE",
            read="snowflake",
        )
        self.assertIsInstance(ast, exp.Put)
        self.assertEqual(ast.this, exp.Literal(this="file:///tmp/my.txt", is_string=True))
        self.assertEqual(ast.args["target"], exp.Var(this="@stage1/folder"))
        properties = ast.args.get("properties")
        props_dict = {prop.this.this: prop.args["value"].this for prop in properties.expressions}
        self.assertEqual(
            props_dict,
            {
                "PARALLEL": "1",
                "AUTO_COMPRESS": False,
                "source_compression": "gzip",
                "OVERWRITE": True,
            },
        )

        # validate identity for different args and properties
        self.validate_identity("PUT 'file:///dir/tmp.csv' @s1/test")

        # the unquoted URI variant is not fully supported yet
        self.validate_identity("PUT file:///dir/tmp.csv @%table", check_command_warning=True)
        self.validate_identity(
            "PUT file:///dir/tmp.csv @s1/test PARALLEL=1 AUTO_COMPRESS=FALSE source_compression=gzip OVERWRITE=TRUE",
            check_command_warning=True,
        )

    def test_get_from_stage(self):
        self.validate_identity('GET @"my_DB"."schEMA1"."MYstage" \'file:///dir/tmp.csv\'')
        self.validate_identity("GET @s1/test 'file:///dir/tmp.csv'").assert_is(exp.Get)

        # GET with file path and stage ref containing spaces (wrapped in single quotes)
        ast = parse_one("GET '@s1/my folder' 'file://my file.txt'", read="snowflake")
        self.assertIsInstance(ast, exp.Get)
        self.assertEqual(ast.args["target"], exp.Var(this="'@s1/my folder'"))
        self.assertEqual(ast.this, exp.Literal(this="file://my file.txt", is_string=True))
        self.assertEqual(ast.sql("snowflake"), "GET '@s1/my folder' 'file://my file.txt'")

        # expression with additional properties
        ast = parse_one("GET @stage1/folder 'file:///tmp/my.txt' PARALLEL = 1", read="snowflake")
        self.assertIsInstance(ast, exp.Get)
        self.assertEqual(ast.args["target"], exp.Var(this="@stage1/folder"))
        self.assertEqual(ast.this, exp.Literal(this="file:///tmp/my.txt", is_string=True))
        properties = ast.args.get("properties")
        props_dict = {prop.this.this: prop.args["value"].this for prop in properties.expressions}
        self.assertEqual(props_dict, {"PARALLEL": "1"})

        # the unquoted URI variant is not fully supported yet
        self.validate_identity("GET @%table file:///dir/tmp.csv", check_command_warning=True)
        self.validate_identity(
            "GET @s1/test file:///dir/tmp.csv PARALLEL=1",
            check_command_warning=True,
        )

    def test_querying_semi_structured_data(self):
        self.validate_identity("SELECT $1")
        self.validate_identity("SELECT $1.elem")

        self.validate_identity("SELECT $1:a.b", "SELECT GET_PATH($1, 'a.b')")
        self.validate_identity("SELECT t.$23:a.b", "SELECT GET_PATH(t.$23, 'a.b')")
        self.validate_identity("SELECT t.$17:a[0].b[0].c", "SELECT GET_PATH(t.$17, 'a[0].b[0].c')")

        self.validate_all(
            """
            SELECT col:"customer's department"
            """,
            write={
                "snowflake": """SELECT GET_PATH(col, '["customer\\'s department"]')""",
                "postgres": "SELECT JSON_EXTRACT_PATH(col, 'customer''s department')",
            },
        )

    def test_alter_set_unset(self):
        self.validate_identity("ALTER TABLE tbl SET DATA_RETENTION_TIME_IN_DAYS=1")
        self.validate_identity("ALTER TABLE tbl SET DEFAULT_DDL_COLLATION='test'")
        self.validate_identity("ALTER TABLE foo SET COMMENT='bar'")
        self.validate_identity("ALTER TABLE foo SET CHANGE_TRACKING=FALSE")
        self.validate_identity("ALTER TABLE table1 SET TAG foo.bar = 'baz'")
        self.validate_identity("ALTER TABLE IF EXISTS foo SET TAG a = 'a', b = 'b', c = 'c'")
        self.validate_identity(
            """ALTER TABLE tbl SET STAGE_FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"' TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9' DATE_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9' BINARY_FORMAT=BASE64)""",
        )
        self.validate_identity(
            """ALTER TABLE tbl SET STAGE_COPY_OPTIONS = (ON_ERROR=SKIP_FILE SIZE_LIMIT=5 PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_SENSITIVE)"""
        )

        self.validate_identity("ALTER TABLE foo UNSET TAG a, b, c")
        self.validate_identity("ALTER TABLE foo UNSET DATA_RETENTION_TIME_IN_DAYS, CHANGE_TRACKING")

    def test_alter_session(self):
        expr = self.validate_identity(
            "ALTER SESSION SET autocommit = FALSE, QUERY_TAG = 'qtag', JSON_INDENT = 1"
        )
        self.assertEqual(
            expr.find(exp.AlterSession),
            exp.AlterSession(
                expressions=[
                    exp.SetItem(
                        this=exp.EQ(
                            this=exp.Column(this=exp.Identifier(this="autocommit", quoted=False)),
                            expression=exp.Boolean(this=False),
                        ),
                    ),
                    exp.SetItem(
                        this=exp.EQ(
                            this=exp.Column(this=exp.Identifier(this="QUERY_TAG", quoted=False)),
                            expression=exp.Literal(this="qtag", is_string=True),
                        ),
                    ),
                    exp.SetItem(
                        this=exp.EQ(
                            this=exp.Column(this=exp.Identifier(this="JSON_INDENT", quoted=False)),
                            expression=exp.Literal(this="1", is_string=False),
                        ),
                    ),
                ],
                unset=False,
            ),
        )

        expr = self.validate_identity("ALTER SESSION UNSET autocommit, QUERY_TAG")
        self.assertEqual(
            expr.find(exp.AlterSession),
            exp.AlterSession(
                expressions=[
                    exp.SetItem(this=exp.Identifier(this="autocommit", quoted=False)),
                    exp.SetItem(this=exp.Identifier(this="QUERY_TAG", quoted=False)),
                ],
                unset=True,
            ),
        )

    def test_from_changes(self):
        self.validate_identity(
            """SELECT C1 FROM t1 CHANGES (INFORMATION => APPEND_ONLY) AT (STREAM => 's1') END (TIMESTAMP => $ts2)"""
        )
        self.validate_identity(
            """SELECT C1 FROM t1 CHANGES (INFORMATION => APPEND_ONLY) BEFORE (STATEMENT => 'STMT_ID') END (TIMESTAMP => $ts2)"""
        )
        self.validate_identity(
            """SELECT 1 FROM some_table CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => TO_TIMESTAMP_TZ('2024-07-01 00:00:00+00:00')) END (TIMESTAMP => TO_TIMESTAMP_TZ('2024-07-01 14:28:59.999999+00:00'))""",
            """SELECT 1 FROM some_table CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => CAST('2024-07-01 00:00:00+00:00' AS TIMESTAMPTZ)) END (TIMESTAMP => CAST('2024-07-01 14:28:59.999999+00:00' AS TIMESTAMPTZ))""",
        )

    def test_grant(self):
        grant_cmds = [
            "GRANT SELECT ON FUTURE TABLES IN DATABASE d1 TO ROLE r1",
            "GRANT INSERT, DELETE ON FUTURE TABLES IN SCHEMA d1.s1 TO ROLE r2",
            "GRANT SELECT ON ALL TABLES IN SCHEMA mydb.myschema to ROLE analyst",
            "GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema TO ROLE role1",
            "GRANT CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema TO DATABASE ROLE mydb.dr1",
        ]

        for sql in grant_cmds:
            with self.subTest(f"Testing Snowflake's GRANT command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

        self.validate_identity(
            "GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.ADD5(number) TO ROLE analyst"
        )

    def test_revoke(self):
        revoke_cmds = [
            "REVOKE SELECT ON FUTURE TABLES IN DATABASE d1 FROM ROLE r1",
            "REVOKE INSERT, DELETE ON FUTURE TABLES IN SCHEMA d1.s1 FROM ROLE r2",
            "REVOKE SELECT ON ALL TABLES IN SCHEMA mydb.myschema FROM ROLE analyst",
            "REVOKE SELECT, INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema FROM ROLE role1",
            "REVOKE CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema FROM DATABASE ROLE mydb.dr1",
        ]

        for sql in revoke_cmds:
            with self.subTest(f"Testing Snowflake's REVOKE command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

        self.validate_identity(
            "REVOKE ALL PRIVILEGES ON FUNCTION mydb.myschema.ADD5(number) FROM ROLE analyst"
        )

    def test_window_function_arg(self):
        query = "SELECT * FROM TABLE(db.schema.FUNC(a) OVER ())"

        ast = self.parse_one(query)
        window = ast.find(exp.Window)

        self.assertEqual(ast.sql("snowflake"), query)
        self.assertEqual(len(list(ast.find_all(exp.Column))), 1)
        self.assertEqual(window.this.sql("snowflake"), "db.schema.FUNC(a)")

    def test_offset_without_limit(self):
        self.validate_all(
            "SELECT 1 ORDER BY 1 LIMIT NULL OFFSET 0",
            read={
                "trino": "SELECT 1 ORDER BY 1 OFFSET 0",
            },
        )

    def test_listagg(self):
        self.validate_identity("LISTAGG(data['some_field'], ',')")

        for distinct in ("", "DISTINCT "):
            self.validate_all(
                f"SELECT LISTAGG({distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t",
                read={
                    "trino": f"SELECT LISTAGG({distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t",
                    "duckdb": f"SELECT LISTAGG({distinct}col, '|SEPARATOR|' ORDER BY col2) FROM t",
                },
                write={
                    "snowflake": f"SELECT LISTAGG({distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t",
                    "trino": f"SELECT LISTAGG({distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t",
                    "duckdb": f"SELECT LISTAGG({distinct}col, '|SEPARATOR|' ORDER BY col2) FROM t",
                },
            )

    def test_rely_options(self):
        for option in ("NORELY", "RELY"):
            self.validate_identity(
                f"CREATE TABLE t (col1 INT PRIMARY KEY {option}, col2 INT UNIQUE {option}, col3 INT NOT NULL FOREIGN KEY REFERENCES other_t (id) {option})"
            )
            self.validate_identity(
                f"CREATE TABLE t (col1 INT, col2 INT, col3 INT, PRIMARY KEY (col1) {option}, UNIQUE (col1, col2) {option}, FOREIGN KEY (col3) REFERENCES other_t (id) {option})"
            )

    def test_parameter(self):
        expr = self.validate_identity("SELECT :1")
        self.assertEqual(expr.find(exp.Placeholder), exp.Placeholder(this="1"))
        self.validate_identity("SELECT :1, :2")
        self.validate_identity("SELECT :1 + :2")

    def test_max_by_min_by(self):
        max_by = self.validate_identity("MAX_BY(DISTINCT selected_col, filtered_col)")
        min_by = self.validate_identity("MIN_BY(DISTINCT selected_col, filtered_col)")

        for node in (max_by, min_by):
            self.assertEqual(len(node.this.expressions), 1)
            self.assertIsInstance(node.expression, exp.Column)

        # Test 3-argument case (returns array)
        max_by_3 = self.validate_identity("MAX_BY(selected_col, filtered_col, 5)")
        min_by_3 = self.validate_identity("MIN_BY(selected_col, filtered_col, 3)")

        for node in (max_by_3, min_by_3):
            with self.subTest(f"Checking  count arg of {node.sql('snowflake')}"):
                self.assertIsNotNone(node.args.get("count"))

        self.validate_all(
            "SELECT MAX_BY(a, b) FROM t",
            write={
                "snowflake": "SELECT MAX_BY(a, b) FROM t",
                "duckdb": "SELECT ARG_MAX(a, b) FROM t",
            },
        )
        self.validate_all(
            "SELECT MIN_BY(a, b) FROM t",
            write={
                "snowflake": "SELECT MIN_BY(a, b) FROM t",
                "duckdb": "SELECT ARG_MIN(a, b) FROM t",
            },
        )

    def test_create_view_copy_grants(self):
        # for normal views, 'COPY GRANTS' goes *after* the column list. ref: https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax
        self.validate_identity(
            "CREATE OR REPLACE VIEW FOO (A, B) COPY GRANTS AS SELECT A, B FROM TBL"
        )

        # for materialized views, 'COPY GRANTS' must go *before* the column list or an error will be thrown. ref: https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#syntax
        self.validate_identity(
            "CREATE OR REPLACE MATERIALIZED VIEW FOO COPY GRANTS (A, B) AS SELECT A, B FROM TBL"
        )

        # check that only 'COPY GRANTS' goes before the column list and other properties still go after
        self.validate_identity(
            "CREATE OR REPLACE MATERIALIZED VIEW FOO COPY GRANTS (A, B) COMMENT='foo' TAG (a='b') AS SELECT A, B FROM TBL"
        )

        # no COPY GRANTS
        self.validate_identity("CREATE OR REPLACE VIEW FOO (A, B) AS SELECT A, B FROM TBL")
        self.validate_identity(
            "CREATE OR REPLACE MATERIALIZED VIEW FOO (A, B) AS SELECT A, B FROM TBL"
        )

    def test_semantic_view(self):
        for dimensions, metrics, where, facts in [
            (None, None, None, None),
            (None, None, None, "a.a"),
            ("DATE_PART('year', a.b)", None, None, None),
            (None, "a.b, a.c", None, None),
            (None, None, None, "a.d, a.e"),
            ("a.b, a.c", "a.b, a.c", None, None),
            ("a.b", "a.b, a.c", "a.c > 5", None),
            ("a.b", None, "a.c > 5", "a.d"),
        ]:
            with self.subTest(
                f"Testing Snowflake's SEMANTIC_VIEW command statement: {dimensions}, {metrics}, {facts} {where}"
            ):
                dimensions_str = f" DIMENSIONS {dimensions}" if dimensions else ""
                metrics_str = f" METRICS {metrics}" if metrics else ""
                fact_str = f" FACTS {facts}" if facts else ""
                where_str = f" WHERE {where}" if where else ""

                self.validate_identity(
                    f"SELECT * FROM SEMANTIC_VIEW(tbl{metrics_str}{dimensions_str}{fact_str}{where_str}) ORDER BY foo"
                )
                self.validate_identity(
                    f"SELECT * FROM SEMANTIC_VIEW(tbl{dimensions_str}{fact_str}{metrics_str}{where_str})",
                    f"SELECT * FROM SEMANTIC_VIEW(tbl{metrics_str}{dimensions_str}{fact_str}{where_str})",
                )

        self.validate_identity(
            "SELECT * FROM SEMANTIC_VIEW(foo METRICS a.b, a.c DIMENSIONS a.b, a.c WHERE a.b > '1995-01-01')",
            """SELECT
  *
FROM SEMANTIC_VIEW(
  foo
  METRICS a.b, a.c
  DIMENSIONS a.b, a.c
  WHERE a.b > '1995-01-01'
)""",
            pretty=True,
        )

    def test_get_extract(self):
        self.validate_all(
            "SELECT GET([4, 5, 6], 1)",
            write={
                "snowflake": "SELECT GET([4, 5, 6], 1)",
                "duckdb": "SELECT [4, 5, 6][2]",
            },
        )

        self.validate_all(
            "SELECT GET(col::MAP(INTEGER, VARCHAR), 1)",
            write={
                "snowflake": "SELECT GET(CAST(col AS MAP(INT, VARCHAR)), 1)",
                "duckdb": "SELECT CAST(col AS MAP(INT, TEXT))[1]",
            },
        )

        self.validate_all(
            "SELECT GET(v, 'field')",
            write={
                "snowflake": "SELECT GET(v, 'field')",
                "duckdb": "SELECT v -> '$.field'",
            },
        )

        self.validate_identity("GET(foo, bar)").assert_is(exp.GetExtract)

    def test_create_sequence(self):
        self.validate_identity(
            "CREATE SEQUENCE seq  START=5 comment = 'foo' INCREMENT=10",
            "CREATE SEQUENCE seq COMMENT='foo' START WITH 5 INCREMENT BY 10",
        )
        self.validate_all(
            "CREATE SEQUENCE seq WITH START=1 INCREMENT=1",
            write={
                "snowflake": "CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1",
                "duckdb": "CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1",
            },
        )

    def test_bit_aggs(self):
        bit_and_funcs = ["BITANDAGG", "BITAND_AGG", "BIT_AND_AGG", "BIT_ANDAGG"]
        bit_or_funcs = ["BITORAGG", "BITOR_AGG", "BIT_OR_AGG", "BIT_ORAGG"]
        bit_xor_funcs = ["BITXORAGG", "BITXOR_AGG", "BIT_XOR_AGG", "BIT_XORAGG"]
        for bit_func in (bit_and_funcs, bit_or_funcs, bit_xor_funcs):
            for name in bit_func:
                with self.subTest(f"Testing Snowflakes {name}"):
                    self.validate_identity(f"{name}(x)", f"{bit_func[0]}(x)")

    def test_bitmap_or_agg(self):
        self.validate_identity("BITMAP_OR_AGG(x)")

    def test_md5_functions(self):
        self.validate_identity("MD5_HEX(col)", "MD5(col)")
        self.validate_identity("MD5(col)")
        self.validate_identity("MD5_BINARY(col)")
        self.validate_identity("MD5_NUMBER_LOWER64(col)")
        self.validate_identity("MD5_NUMBER_UPPER64(col)")

    def test_model_attribute(self):
        self.validate_identity("SELECT model!mladmin")
        self.validate_identity("SELECT model!PREDICT(1)")
        self.validate_identity("SELECT m!PREDICT(INPUT_DATA => {*}) AS p FROM tbl")
        self.validate_identity("SELECT m!PREDICT(INPUT_DATA => {tbl.*}) AS p FROM tbl")
        self.validate_identity("x.y.z!PREDICT(foo, bar, baz, bla)")
        self.validate_identity(
            "SELECT * FROM TABLE(model_trained_with_labeled_data!DETECT_ANOMALIES(INPUT_DATA => TABLE(view_with_data_to_analyze), TIMESTAMP_COLNAME => 'date', TARGET_COLNAME => 'sales', CONFIG_OBJECT => OBJECT_CONSTRUCT('prediction_interval', 0.99)))"
        )

    def test_set_item_kind_attribute(self):
        expr = parse_one("ALTER SESSION SET autocommit = FALSE", read="snowflake")
        set_item = expr.find(exp.SetItem)
        self.assertIsNotNone(set_item)
        self.assertIsNone(set_item.args.get("kind"))

        expr = parse_one("SET a = 1", read="snowflake")
        set_item = expr.find(exp.SetItem)
        self.assertIsNotNone(set_item)
        self.assertEqual(set_item.args.get("kind"), "VARIABLE")

    def test_round(self):
        self.validate_all(
            "SELECT ROUND(2.25) AS value",
            write={
                "snowflake": "SELECT ROUND(2.25) AS value",
                "duckdb": "SELECT ROUND(2.25) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(2.25, 1) AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1) AS value",
                "duckdb": "SELECT ROUND(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(EXPR => 2.25, SCALE => 1) AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1) AS value",
                "duckdb": "SELECT ROUND(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(SCALE => 1, EXPR => 2.25) AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1) AS value",
                "duckdb": "SELECT ROUND(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(2.25, 1, 'HALF_AWAY_FROM_ZERO') AS value",
            write={
                "snowflake": """SELECT ROUND(2.25, 1, 'HALF_AWAY_FROM_ZERO') AS value""",
                "duckdb": "SELECT ROUND(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(EXPR => 2.25, SCALE => 1, ROUNDING_MODE => 'HALF_AWAY_FROM_ZERO') AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1, 'HALF_AWAY_FROM_ZERO') AS value",
                "duckdb": "SELECT ROUND(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(2.25, 1, 'HALF_TO_EVEN') AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1, 'HALF_TO_EVEN') AS value",
                "duckdb": "SELECT ROUND_EVEN(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(ROUNDING_MODE => 'HALF_TO_EVEN', EXPR => 2.25, SCALE => 1) AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1, 'HALF_TO_EVEN') AS value",
                "duckdb": "SELECT ROUND_EVEN(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(SCALE => 1, EXPR => 2.25, , ROUNDING_MODE => 'HALF_TO_EVEN') AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1, 'HALF_TO_EVEN') AS value",
                "duckdb": "SELECT ROUND_EVEN(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(EXPR => 2.25, SCALE => 1, ROUNDING_MODE => 'HALF_TO_EVEN') AS value",
            write={
                "snowflake": "SELECT ROUND(2.25, 1, 'HALF_TO_EVEN') AS value",
                "duckdb": "SELECT ROUND_EVEN(2.25, 1) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(2.256, 1.8) AS value",
            write={
                "snowflake": "SELECT ROUND(2.256, 1.8) AS value",
                "duckdb": "SELECT ROUND(2.256, CAST(1.8 AS INT)) AS value",
            },
        )

        self.validate_all(
            "SELECT ROUND(2.256, CAST(1.8 AS DECIMAL(38, 0))) AS value",
            write={
                "snowflake": "SELECT ROUND(2.256, CAST(1.8 AS DECIMAL(38, 0))) AS value",
                "duckdb": "SELECT ROUND(2.256, CAST(CAST(1.8 AS DECIMAL(38, 0)) AS INT)) AS value",
            },
        )

    def test_get_bit(self):
        self.validate_all(
            "SELECT GETBIT(11, 1)",
            write={
                "snowflake": "SELECT GETBIT(11, 1)",
                "databricks": "SELECT GETBIT(11, 1)",
                "redshift": "SELECT GETBIT(11, 1)",
            },
        )
        expr = self.validate_identity("GETBIT(11, 1)")
        annotated = annotate_types(expr, dialect="snowflake")

        self.assertEqual(annotated.sql("duckdb"), "(11 >> 1) & 1")
        self.assertEqual(annotated.sql("postgres"), "11 >> 1 & 1")

    def test_to_binary(self):
        expr = self.validate_identity("TO_BINARY('48454C50', 'HEX')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "UNHEX('48454C50')")

        expr = self.validate_identity("TO_BINARY('48454C50')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "UNHEX('48454C50')")

        expr = self.validate_identity("TO_BINARY('TEST', 'UTF-8')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "ENCODE('TEST')")

        expr = self.validate_identity("TO_BINARY('SEVMUA==', 'BASE64')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "FROM_BASE64('SEVMUA==')")

        expr = self.validate_identity("TRY_TO_BINARY('48454C50', 'HEX')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "TRY(UNHEX('48454C50'))")

        expr = self.validate_identity("TRY_TO_BINARY('48454C50')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "TRY(UNHEX('48454C50'))")

        expr = self.validate_identity("TRY_TO_BINARY('Hello', 'UTF-8')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "TRY(ENCODE('Hello'))")

        expr = self.validate_identity("TRY_TO_BINARY('SGVsbG8=', 'BASE64')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "TRY(FROM_BASE64('SGVsbG8='))")

        expr = self.validate_identity("TRY_TO_BINARY('Hello', 'UTF-16')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "NULL")

    def test_reverse(self):
        # Test REVERSE with TO_BINARY (BLOB type) - UTF-8 format
        expr = self.validate_identity("REVERSE(TO_BINARY('ABC', 'UTF-8'))")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"), "CAST(REVERSE(CAST(ENCODE('ABC') AS TEXT)) AS BLOB)"
        )

        # Test REVERSE with TO_BINARY - HEX format
        expr = self.validate_identity("REVERSE(TO_BINARY('414243', 'HEX'))")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "CAST(REVERSE(CAST(UNHEX('414243') AS TEXT)) AS BLOB)",
        )

        # Test REVERSE with HEX_DECODE_BINARY
        expr = self.validate_identity("REVERSE(HEX_DECODE_BINARY('414243'))")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "CAST(REVERSE(CAST(UNHEX('414243') AS TEXT)) AS BLOB)",
        )

        # Test REVERSE with VARCHAR (should not add casts)
        expr = self.validate_identity("REVERSE('ABC')")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "REVERSE('ABC')")

    def test_float_interval(self):
        # Test TIMEADD with float interval value - DuckDB INTERVAL requires integers
        expr = self.validate_identity("TIMEADD(HOUR, 2.5, CAST('10:30:00' AS TIME))")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "CAST('10:30:00' AS TIME) + INTERVAL (CAST(ROUND(2.5) AS INT)) HOUR",
        )

        # Test DATEADD with decimal interval value
        expr = self.validate_identity(
            "DATEADD(HOUR, CAST(3.8 AS DECIMAL(10, 2)), CAST('2024-01-01 10:00:00' AS TIMESTAMP))"
        )
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "CAST('2024-01-01 10:00:00' AS TIMESTAMP) + INTERVAL (CAST(ROUND(CAST(3.8 AS DECIMAL(10, 2))) AS INT)) HOUR",
        )

        # Test TIMESTAMPADD with float interval value - Snowflake parser converts to DATEADD
        expr = self.parse_one(
            "TIMESTAMPADD(MINUTE, 30.9, CAST('2024-01-01 10:00:00' AS TIMESTAMP))",
            dialect="snowflake",
        )
        self.assertEqual(
            expr.sql("snowflake"), "DATEADD(MINUTE, 30.9, CAST('2024-01-01 10:00:00' AS TIMESTAMP))"
        )
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "CAST('2024-01-01 10:00:00' AS TIMESTAMP) + INTERVAL (CAST(ROUND(30.9) AS INT)) MINUTE",
        )

    def test_transpile_bitwise_ops(self):
        # Binary bitwise operations
        expr = self.parse_one("SELECT BITOR(x'FF', x'0F')", dialect="snowflake")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "SELECT CAST(CAST(UNHEX('FF') AS BIT) | CAST(UNHEX('0F') AS BIT) AS BLOB)",
        )
        self.assertEqual(annotated.sql("snowflake"), "SELECT BITOR(x'FF', x'0F')")

        expr = self.parse_one("SELECT BITAND(x'FF', x'0F')", dialect="snowflake")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "SELECT CAST(CAST(UNHEX('FF') AS BIT) & CAST(UNHEX('0F') AS BIT) AS BLOB)",
        )
        self.assertEqual(annotated.sql("snowflake"), "SELECT BITAND(x'FF', x'0F')")

        expr = self.parse_one("SELECT BITXOR(x'FF', x'0F')", dialect="snowflake")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(
            annotated.sql("duckdb"),
            "SELECT CAST(XOR(CAST(UNHEX('FF') AS BIT), CAST(UNHEX('0F') AS BIT)) AS BLOB)",
        )
        self.assertEqual(annotated.sql("snowflake"), "SELECT BITXOR(x'FF', x'0F')")

        expr = self.parse_one("SELECT BITNOT(x'FF')", dialect="snowflake")
        annotated = annotate_types(expr, dialect="snowflake")
        self.assertEqual(annotated.sql("duckdb"), "SELECT CAST(~CAST(UNHEX('FF') AS BIT) AS BLOB)")
        self.assertEqual(annotated.sql("snowflake"), "SELECT BITNOT(x'FF')")

    def test_quoting(self):
        self.assertEqual(
            parse_one("select a, B from DUAL", dialect="snowflake").sql(
                dialect="snowflake", identify="safe"
            ),
            'SELECT a, "B" FROM DUAL',
        )

    def test_floor(self):
        self.validate_all(
            "SELECT FLOOR(1.753, 2)",
            write={"duckdb": "SELECT ROUND(FLOOR(1.753 * POWER(10, 2)) / POWER(10, 2), 2)"},
        )
        self.validate_all(
            "SELECT FLOOR(123.45, -1)",
            write={"duckdb": "SELECT ROUND(FLOOR(123.45 * POWER(10, -1)) / POWER(10, -1), -1)"},
        )
        self.validate_all(
            "SELECT FLOOR(a + b, 2)",
            write={"duckdb": "SELECT ROUND(FLOOR((a + b) * POWER(10, 2)) / POWER(10, 2), 2)"},
        )
        self.validate_all(
            "SELECT FLOOR(1.234, 1.5)",
            write={
                "duckdb": "SELECT ROUND(FLOOR(1.234 * POWER(10, CAST(1.5 AS INT))) / POWER(10, CAST(1.5 AS INT)), CAST(1.5 AS INT))"
            },
        )

    def test_seq_functions(self):
        # SEQ1 - 1-byte sequences
        self.validate_all(
            "SELECT SEQ1() FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 FROM test",
                "snowflake": "SELECT SEQ1() FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ1(0) FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 FROM test",
                "snowflake": "SELECT SEQ1(0) FROM test",
            },
        )
        # 1 means it's signed parameter, which affects wrap-around behavior
        self.validate_all(
            "SELECT SEQ1(1) FROM test",
            write={
                "duckdb": "SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 >= 128 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 - 256 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 END) FROM test",
                "snowflake": "SELECT SEQ1(1) FROM test",
            },
        )

        # SEQ2 - 2-byte sequences
        self.validate_all(
            "SELECT SEQ2() FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 FROM test",
                "snowflake": "SELECT SEQ2() FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ2(0) FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 FROM test",
                "snowflake": "SELECT SEQ2(0) FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ2(1) FROM test",
            write={
                "duckdb": "SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 >= 32768 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 - 65536 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 END) FROM test",
                "snowflake": "SELECT SEQ2(1) FROM test",
            },
        )

        # SEQ4 - 4-byte sequences
        self.validate_all(
            "SELECT SEQ4() FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 FROM test",
                "snowflake": "SELECT SEQ4() FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ4(0) FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 FROM test",
                "snowflake": "SELECT SEQ4(0) FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ4(1) FROM test",
            write={
                "duckdb": "SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 >= 2147483648 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 - 4294967296 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 END) FROM test",
                "snowflake": "SELECT SEQ4(1) FROM test",
            },
        )

        # SEQ8 - 8-byte sequences
        self.validate_all(
            "SELECT SEQ8() FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 FROM test",
                "snowflake": "SELECT SEQ8() FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ8(0) FROM test",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 FROM test",
                "snowflake": "SELECT SEQ8(0) FROM test",
            },
        )
        self.validate_all(
            "SELECT SEQ8(1) FROM test",
            write={
                "duckdb": "SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 >= 9223372036854775808 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 - 18446744073709551616 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 END) FROM test",
                "snowflake": "SELECT SEQ8(1) FROM test",
            },
        )

    def test_generator(self):
        # Basic ROWCOUNT transpilation
        self.validate_all(
            "SELECT 1 FROM TABLE(GENERATOR(ROWCOUNT => 5))",
            write={
                "duckdb": "SELECT 1 FROM RANGE(5)",
                "snowflake": "SELECT 1 FROM TABLE(GENERATOR(ROWCOUNT => 5))",
            },
        )

        # GENERATOR with SEQ functions - the common use case
        self.validate_all(
            "SELECT SEQ8() FROM TABLE(GENERATOR(ROWCOUNT => 5))",
            write={
                "duckdb": "SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 FROM RANGE(5)",
                "snowflake": "SELECT SEQ8() FROM TABLE(GENERATOR(ROWCOUNT => 5))",
            },
        )

        # GENERATOR with JOIN in parenthesized construct - preserves joins
        self.validate_all(
            "SELECT * FROM (TABLE(GENERATOR(ROWCOUNT => 5)) JOIN other ON 1 = 1)",
            write={
                "duckdb": "SELECT * FROM (RANGE(5) JOIN other ON 1 = 1)",
                "snowflake": "SELECT * FROM (TABLE(GENERATOR(ROWCOUNT => 5)) JOIN other ON 1 = 1)",
            },
        )

    def test_ceil(self):
        self.validate_all(
            "SELECT CEIL(1.753, 2)",
            write={"duckdb": "SELECT ROUND(CEIL(1.753 * POWER(10, 2)) / POWER(10, 2), 2)"},
        )
        self.validate_all(
            "SELECT CEIL(123.45, -1)",
            write={"duckdb": "SELECT ROUND(CEIL(123.45 * POWER(10, -1)) / POWER(10, -1), -1)"},
        )
        self.validate_all(
            "SELECT CEIL(a + b, 2)",
            write={"duckdb": "SELECT ROUND(CEIL((a + b) * POWER(10, 2)) / POWER(10, 2), 2)"},
        )
        self.validate_all(
            "SELECT CEIL(1.234, 1.5)",
            write={
                "duckdb": "SELECT ROUND(CEIL(1.234 * POWER(10, CAST(1.5 AS INT))) / POWER(10, CAST(1.5 AS INT)), CAST(1.5 AS INT))"
            },
        )

    def test_corr(self):
        self.validate_all(
            "SELECT CORR(a, b)",
            read={
                "snowflake": "SELECT CORR(a, b)",
                "postgres": "SELECT CORR(a, b)",
            },
            write={
                "snowflake": "SELECT CORR(a, b)",
                "postgres": "SELECT CORR(a, b)",
                "duckdb": "SELECT CASE WHEN ISNAN(CORR(a, b)) THEN NULL ELSE CORR(a, b) END",
            },
        )
        self.validate_all(
            "SELECT CORR(a, b) OVER (PARTITION BY c)",
            read={
                "snowflake": "SELECT CORR(a, b) OVER (PARTITION BY c)",
                "postgres": "SELECT CORR(a, b) OVER (PARTITION BY c)",
            },
            write={
                "snowflake": "SELECT CORR(a, b) OVER (PARTITION BY c)",
                "postgres": "SELECT CORR(a, b) OVER (PARTITION BY c)",
                "duckdb": "SELECT CASE WHEN ISNAN(CORR(a, b) OVER (PARTITION BY c)) THEN NULL ELSE CORR(a, b) OVER (PARTITION BY c) END",
            },
        )

        self.validate_all(
            "SELECT CORR(a, b) FILTER(WHERE c > 0)",
            write={
                "duckdb": "SELECT CASE WHEN ISNAN(CORR(a, b) FILTER(WHERE c > 0)) THEN NULL ELSE CORR(a, b) FILTER(WHERE c > 0) END",
            },
        )
        self.validate_all(
            "SELECT CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)",
            write={
                "duckdb": "SELECT CASE WHEN ISNAN(CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)) THEN NULL ELSE CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d) END",
            },
        )

    def test_encryption_functions(self):
        # ENCRYPT
        self.validate_identity("ENCRYPT(value, 'passphrase')")
        self.validate_identity("ENCRYPT(value, 'passphrase', 'aad')")
        self.validate_identity("ENCRYPT(value, 'passphrase', 'aad', 'AES-GCM')")

        # ENCRYPT_RAW
        self.validate_identity("ENCRYPT_RAW(value, key, iv)")
        self.validate_identity("ENCRYPT_RAW(value, key, iv, aad)")
        self.validate_identity("ENCRYPT_RAW(value, key, iv, aad, 'AES-GCM')")

        # DECRYPT
        self.validate_identity("DECRYPT(encrypted, 'passphrase')")
        self.validate_identity("DECRYPT(encrypted, 'passphrase', 'aad')")
        self.validate_identity("DECRYPT(encrypted, 'passphrase', 'aad', 'AES-GCM')")

        # DECRYPT_RAW
        self.validate_identity("DECRYPT_RAW(encrypted, key, iv)")
        self.validate_identity("DECRYPT_RAW(encrypted, key, iv, aad)")
        self.validate_identity("DECRYPT_RAW(encrypted, key, iv, aad, 'AES-GCM')")
        self.validate_identity("DECRYPT_RAW(encrypted, key, iv, aad, 'AES-GCM', aead)")

        # TRY_DECRYPT (parses as Decrypt with safe=True)
        self.validate_identity("TRY_DECRYPT(encrypted, 'passphrase')")
        self.validate_identity("TRY_DECRYPT(encrypted, 'passphrase', 'aad')")
        self.validate_identity("TRY_DECRYPT(encrypted, 'passphrase', 'aad', 'AES-GCM')")

        # TRY_DECRYPT_RAW (parses as DecryptRaw with safe=True)
        self.validate_identity("TRY_DECRYPT_RAW(encrypted, key, iv)")
        self.validate_identity("TRY_DECRYPT_RAW(encrypted, key, iv, aad)")
        self.validate_identity("TRY_DECRYPT_RAW(encrypted, key, iv, aad, 'AES-GCM')")
        self.validate_identity("TRY_DECRYPT_RAW(encrypted, key, iv, aad, 'AES-GCM', aead)")

    def test_update_statement(self):
        self.validate_identity("UPDATE test SET t = 1 FROM t1")
        self.validate_identity("UPDATE test SET t = 1 FROM t2 JOIN t3 ON t2.id = t3.id")
        self.validate_identity(
            "UPDATE test SET t = 1 FROM (SELECT id FROM test2) AS t2 JOIN test3 AS t3 ON t2.id = t3.id"
        )

        self.validate_identity(
            "UPDATE sometesttable u FROM (SELECT 5195 AS new_count, '01bee1e5-0000-d31e-0000-e80ef02b9f27' query_id ) b SET qry_hash_count = new_count WHERE u.sample_query_id  = b.query_id",
            "UPDATE sometesttable AS u SET qry_hash_count = new_count FROM (SELECT 5195 AS new_count, '01bee1e5-0000-d31e-0000-e80ef02b9f27' AS query_id) AS b WHERE u.sample_query_id = b.query_id",
        )

    def test_type_sensitive_bitshift_transpilation(self):
        ast = annotate_types(self.parse_one("SELECT BITSHIFTLEFT(X'FF', 4)"), dialect="snowflake")
        self.assertEqual(ast.sql("duckdb"), "SELECT CAST(CAST(UNHEX('FF') AS BIT) << 4 AS BLOB)")

        ast = annotate_types(self.parse_one("SELECT BITSHIFTRIGHT(X'FF', 4)"), dialect="snowflake")
        self.assertEqual(ast.sql("duckdb"), "SELECT CAST(CAST(UNHEX('FF') AS BIT) >> 4 AS BLOB)")

    def test_array_flatten(self):
        # String array flattening
        self.validate_all(
            "SELECT ARRAY_FLATTEN([['a', 'b'], ['c', 'd', 'e']])",
            write={
                "snowflake": "SELECT ARRAY_FLATTEN([['a', 'b'], ['c', 'd', 'e']])",
                "duckdb": "SELECT FLATTEN([['a', 'b'], ['c', 'd', 'e']])",
                "starrocks": "SELECT ARRAY_FLATTEN([['a', 'b'], ['c', 'd', 'e']])",
            },
        )

        # Nested arrays (single level flattening)
        self.validate_all(
            "SELECT ARRAY_FLATTEN([[[1, 2], [3]], [[4], [5]]])",
            write={
                "snowflake": "SELECT ARRAY_FLATTEN([[[1, 2], [3]], [[4], [5]]])",
                "duckdb": "SELECT FLATTEN([[[1, 2], [3]], [[4], [5]]])",
            },
        )

        # Array with NULL elements
        self.validate_all(
            "SELECT ARRAY_FLATTEN([[1, NULL, 3], [4]])",
            write={
                "snowflake": "SELECT ARRAY_FLATTEN([[1, NULL, 3], [4]])",
                "duckdb": "SELECT FLATTEN([[1, NULL, 3], [4]])",
            },
        )

        # Empty arrays
        self.validate_all(
            "SELECT ARRAY_FLATTEN([[]])",
            write={
                "snowflake": "SELECT ARRAY_FLATTEN([[]])",
                "duckdb": "SELECT FLATTEN([[]])",
            },
        )

    def test_directed_joins(self):
        self.validate_identity("SELECT * FROM a CROSS DIRECTED JOIN b USING (id)")
        self.validate_identity("SELECT * FROM a INNER DIRECTED JOIN b USING (id)")
        self.validate_identity("SELECT * FROM a NATURAL INNER DIRECTED JOIN b USING (id)")

        for join_side in ("LEFT", "RIGHT", "FULL"):
            for outer in ("", " OUTER"):
                for natural in ("", "NATURAL "):
                    prefix = natural + join_side + outer + " DIRECTED"
                    with self.subTest(f"Testing {prefix} JOIN"):
                        self.validate_identity(f"SELECT * FROM a {prefix} JOIN b USING (id)")
