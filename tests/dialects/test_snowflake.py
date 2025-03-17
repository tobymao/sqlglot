from unittest import mock

from sqlglot import UnsupportedError, exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    maxDiff = None
    dialect = "snowflake"

    def test_snowflake(self):
        self.assertEqual(
            # Ensures we don't fail when generating ParseJSON with the `safe` arg set to `True`
            self.validate_identity("""SELECT TRY_PARSE_JSON('{"x: 1}')""").sql(),
            """SELECT PARSE_JSON('{"x: 1}')""",
        )

        expr = parse_one("SELECT APPROX_TOP_K(C4, 3, 5) FROM t")
        expr.selects[0].assert_is(exp.AggFunc)
        self.assertEqual(expr.sql(dialect="snowflake"), "SELECT APPROX_TOP_K(C4, 3, 5) FROM t")

        self.validate_identity("INSERT INTO test VALUES (x'48FAF43B0AFCEF9B63EE3A93EE2AC2')")
        self.validate_identity("exclude := [foo]")
        self.validate_identity("SELECT CAST([1, 2, 3] AS VECTOR(FLOAT, 3))")
        self.validate_identity("SELECT CONNECT_BY_ROOT test AS test_column_alias")
        self.validate_identity("SELECT number").selects[0].assert_is(exp.Column)
        self.validate_identity("INTERVAL '4 years, 5 months, 3 hours'")
        self.validate_identity("ALTER TABLE table1 CLUSTER BY (name DESC)")
        self.validate_identity("SELECT rename, replace")
        self.validate_identity("SELECT TIMEADD(HOUR, 2, CAST('09:05:03' AS TIME))")
        self.validate_identity("SELECT CAST(OBJECT_CONSTRUCT('a', 1) AS MAP(VARCHAR, INT))")
        self.validate_identity("SELECT CAST(OBJECT_CONSTRUCT('a', 1) AS OBJECT(a CHAR NOT NULL))")
        self.validate_identity("SELECT CAST([1, 2, 3] AS ARRAY(INT))")
        self.validate_identity("SELECT CAST(obj AS OBJECT(x CHAR) RENAME FIELDS)")
        self.validate_identity("SELECT CAST(obj AS OBJECT(x CHAR, y VARCHAR) ADD FIELDS)")
        self.validate_identity("SELECT TO_TIMESTAMP(123.4)").selects[0].assert_is(exp.Anonymous)
        self.validate_identity("SELECT TO_TIMESTAMP(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_NTZ(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_LTZ(x) FROM t")
        self.validate_identity("SELECT TO_TIMESTAMP_TZ(x) FROM t")
        self.validate_identity("TO_DECIMAL(expr, fmt, precision, scale)")
        self.validate_identity("ALTER TABLE authors ADD CONSTRAINT c1 UNIQUE (id, email)")
        self.validate_identity("RM @parquet_stage", check_command_warning=True)
        self.validate_identity("REMOVE @parquet_stage", check_command_warning=True)
        self.validate_identity("SELECT TIMESTAMP_FROM_PARTS(d, t)")
        self.validate_identity("SELECT GET_PATH(v, 'attr[0].name') FROM vartab")
        self.validate_identity("SELECT TO_ARRAY(CAST(x AS ARRAY))")
        self.validate_identity("SELECT TO_ARRAY(CAST(['test'] AS VARIANT))")
        self.validate_identity("SELECT ARRAY_UNIQUE_AGG(x)")
        self.validate_identity("SELECT OBJECT_CONSTRUCT()")
        self.validate_identity("SELECT DAYOFMONTH(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT DAYOFYEAR(CURRENT_TIMESTAMP())")
        self.validate_identity("WEEKOFYEAR(tstamp)")
        self.validate_identity("SELECT QUARTER(CURRENT_TIMESTAMP())")
        self.validate_identity("SELECT SUM(amount) FROM mytable GROUP BY ALL")
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
        self.validate_identity("CREATE TABLE foo (bar FLOAT AUTOINCREMENT START 0 INCREMENT 1)")
        self.validate_identity("COMMENT IF EXISTS ON TABLE foo IS 'bar'")
        self.validate_identity("SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', col)")
        self.validate_identity("ALTER TABLE a SWAP WITH b")
        self.validate_identity("SELECT MATCH_CONDITION")
        self.validate_identity("SELECT * REPLACE (CAST(col AS TEXT) AS scol) FROM t")
        self.validate_identity("1 /* /* */")
        self.validate_identity("TO_TIMESTAMP(col, fmt)")
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
            """SELECT TO_TIMESTAMP('2025-01-16T14:45:30.123+0500', 'yyyy-mm-DD"T"hh24:mi:ss.ff3TZHTZM')"""
        )
        self.validate_identity(
            "SELECT 1 put",
            "SELECT 1 AS put",
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
            "SELECT CAST(GET_PATH(a, 'from') AS TEXT), GET_PATH(a, 'from') || ' test'",
        )
        self.validate_identity(
            "SELECT a:select",
            "SELECT GET_PATH(a, 'select')",
        )
        self.validate_identity("x:from", "GET_PATH(x, 'from')")
        self.validate_identity(
            "value:values::string::int",
            "CAST(CAST(GET_PATH(value, 'values') AS TEXT) AS INT)",
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
            "SELECT * FROM unnest(x) with ordinality",
            "SELECT * FROM TABLE(FLATTEN(INPUT => x)) AS _u(seq, key, path, index, value, this)",
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
            "CREATE TABLE x (y NUMBER IDENTITY START 0 INCREMENT 1)",
            "CREATE TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)",
        )
        self.validate_identity(
            "ALTER TABLE foo ADD COLUMN id INT identity(1, 1)",
            "ALTER TABLE foo ADD COLUMN id INT AUTOINCREMENT START 1 INCREMENT 1",
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
        self.validate_all(
            "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
            write={
                "duckdb": "SELECT MAKE_TIME(12, 34, 56 + (987654321 / 1000000000.0))",
                "snowflake": "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
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
                "databricks": """SELECT '{"fruit":"banana"}':fruit""",
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
                "duckdb": "SELECT BOOL_OR(c1), BOOL_OR(c2) FROM test",
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
                "duckdb": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "oracle": "SELECT MIN(c1), MIN(c2) FROM test",
                "postgres": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "snowflake": "SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test",
                "spark": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "sqlite": "SELECT MIN(c1), MIN(c2) FROM test",
                "mysql": "SELECT MIN(c1), MIN(c2) FROM test",
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
                "CORR",
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
        self.validate_identity(
            "TO_CHAR(foo::DATE, 'yyyy')", "TO_CHAR(CAST(CAST(foo AS DATE) AS TIMESTAMP), 'yyyy')"
        )
        self.validate_all(
            "TO_CHAR(foo::TIMESTAMP, 'YYYY-MM')",
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
                "postgres": "x ^ 2",
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
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            write={
                "bigquery": "SELECT TIMESTAMP_MILLIS(1659981729000)",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000, 3)",
                "spark": "SELECT TIMESTAMP_MILLIS(1659981729000)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(16599817290000, 4)",
            write={
                "bigquery": "SELECT TIMESTAMP_SECONDS(CAST(16599817290000 / POWER(10, 4) AS INT64))",
                "snowflake": "SELECT TO_TIMESTAMP(16599817290000, 4)",
                "spark": "SELECT TIMESTAMP_SECONDS(16599817290000 / POWER(10, 4))",
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
                "duckdb": "SELECT TO_TIMESTAMP(1659981729000000000 / POWER(10, 9))",
                "presto": "SELECT FROM_UNIXTIME(CAST(1659981729000000000 AS DOUBLE) / POW(10, 9))",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
                "spark": "SELECT TIMESTAMP_SECONDS(1659981729000000000 / POWER(10, 9))",
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
                "bigquery": "SELECT PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', '04/05/2013 01:02:03')",
                "snowflake": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/DD/yyyy hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'MM/dd/yyyy HH:mm:ss')",
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
                "": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d ELSE e END",
                "snowflake": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d ELSE e END",
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

        self.validate_identity("EDITDISTANCE(col1, col2)")
        self.validate_all(
            "EDITDISTANCE(col1, col2, 3)",
            write={
                "bigquery": "EDIT_DISTANCE(col1, col2, max_distance => 3)",
                "postgres": "LEVENSHTEIN_LESS_EQUAL(col1, col2, 3)",
                "snowflake": "EDITDISTANCE(col1, col2, 3)",
            },
        )
        self.validate_identity("SELECT BITOR(a, b)")
        self.validate_identity("SELECT BIT_OR(a, b)", "SELECT BITOR(a, b)")
        self.validate_identity("SELECT BITOR(a, b, 'LEFT')")
        self.validate_identity("SELECT BITXOR(a, b, 'LEFT')")
        self.validate_identity("SELECT BIT_XOR(a, b)", "SELECT BITXOR(a, b)")
        self.validate_identity("SELECT BIT_XOR(a, b, 'LEFT')", "SELECT BITXOR(a, b, 'LEFT')")
        self.validate_identity("SELECT BITSHIFTLEFT(a, 1)")
        self.validate_identity("SELECT BIT_SHIFTLEFT(a, 1)", "SELECT BITSHIFTLEFT(a, 1)")
        self.validate_identity("SELECT BIT_SHIFTRIGHT(a, 1)", "SELECT BITSHIFTRIGHT(a, 1)")

        self.validate_identity("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT)")

        self.validate_all(
            "SELECT HEX_DECODE_BINARY('65')",
            write={
                "bigquery": "SELECT FROM_HEX('65')",
                "duckdb": "SELECT UNHEX('65')",
                "snowflake": "SELECT HEX_DECODE_BINARY('65')",
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

    def test_staged_files(self):
        # Ensure we don't treat staged file paths as identifiers (i.e. they're not normalized)
        staged_file = parse_one("SELECT * FROM @foo", read="snowflake")
        self.assertEqual(
            normalize_identifiers(staged_file, dialect="snowflake").sql(dialect="snowflake"),
            staged_file.sql(dialect="snowflake"),
        )

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
                "snowflake": "SELECT EXTRACT(epoch_second FROM CAST(foo AS TIMESTAMP)) AS ddate FROM table_name",
                "presto": "SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) AS ddate FROM table_name",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(epoch_milliseconds, foo) as ddate from table_name",
            write={
                "snowflake": "SELECT EXTRACT(epoch_second FROM CAST(foo AS TIMESTAMP)) * 1000 AS ddate FROM table_name",
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

        self.validate_identity("DATEADD(y, 5, x)", "DATEADD(YEAR, 5, x)")
        self.validate_identity("DATEADD(y, 5, x)", "DATEADD(YEAR, 5, x)")
        self.validate_identity("DATE_PART(yyy, x)", "DATE_PART(YEAR, x)")
        self.validate_identity("DATE_TRUNC(yr, x)", "DATE_TRUNC('YEAR', x)")

        self.validate_identity("TO_DATE('12345')").assert_is(exp.Anonymous)

        self.validate_identity(
            "SELECT TO_DATE('2019-02-28') + INTERVAL '1 day, 1 year'",
            "SELECT CAST('2019-02-28' AS DATE) + INTERVAL '1 day, 1 year'",
        )

        self.validate_identity("TO_DATE(x)").assert_is(exp.TsOrDsToDate)
        self.validate_identity("TRY_TO_DATE(x)").assert_is(exp.TsOrDsToDate)

        self.validate_all(
            "DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "snowflake": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "TO_DATE(x, 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE(x, 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME(x, '%m-%d-%Y') AS DATE)",
            },
        )
        self.validate_all(
            "DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE('01-01-2000', 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME('01-01-2000', '%m-%d-%Y') AS DATE)",
            },
        )
        self.validate_all(
            "TO_DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TO_DATE('01-01-2000', 'mm-DD-yyyy')",
                "duckdb": "CAST(STRPTIME('01-01-2000', '%m-%d-%Y') AS DATE)",
            },
        )
        self.validate_all(
            "TRY_TO_DATE('01-01-2000', 'MM-DD-YYYY')",
            write={
                "snowflake": "TRY_TO_DATE('01-01-2000', 'mm-DD-yyyy')",
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
            "SELECT TRY_TO_TIME('11.15.00', 'hh24.mi.ss')",
            write={
                "snowflake": "SELECT TRY_TO_TIME('11.15.00', 'hh24.mi.ss')",
                "duckdb": "SELECT CAST(STRPTIME('11.15.00', '%H.%M.%S') AS TIME)",
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
            "CREATE OR REPLACE TAG IF NOT EXISTS cost_center COMMENT='cost_center tag'"
        ).this.assert_is(exp.Identifier)
        self.validate_identity(
            "CREATE DYNAMIC TABLE product (pre_tax_profit, taxes, after_tax_profit) TARGET_LAG='20 minutes' WAREHOUSE=mywh AS SELECT revenue - cost, (revenue - cost) * tax_rate, (revenue - cost) * (1.0 - tax_rate) FROM staging_table"
        )
        self.validate_identity(
            "ALTER TABLE db_name.schmaName.tblName ADD COLUMN COLUMN_1 VARCHAR NOT NULL TAG (key1='value_1')"
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
            "CREATE TABLE a (x DATE, y BIGINT) PARTITION BY (x) integration='q' auto_refresh=TRUE file_format=(type = parquet)"
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
            "CREATE SEQUENCE seq1 START=1 INCREMENT BY 1 ORDER",
        )
        self.validate_identity(
            "CREATE SEQUENCE seq1 WITH START=1 INCREMENT=1 ORDER",
            "CREATE SEQUENCE seq1 START=1 INCREMENT=1 ORDER",
        )
        self.validate_identity(
            """create external table et2(
  col1 date as (parse_json(metadata$external_table_partition):COL1::date),
  col2 varchar as (parse_json(metadata$external_table_partition):COL2::varchar),
  col3 number as (parse_json(metadata$external_table_partition):COL3::number))
  partition by (col1,col2,col3)
  location=@s2/logs/
  partition_type = user_specified
  file_format = (type = parquet)""",
            "CREATE EXTERNAL TABLE et2 (col1 DATE AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL1') AS DATE)), col2 VARCHAR AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL2') AS VARCHAR)), col3 DECIMAL(38, 0) AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL3') AS DECIMAL(38, 0)))) LOCATION @s2/logs/ PARTITION BY (col1, col2, col3) partition_type=user_specified file_format=(type = parquet)",
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

        self.assertIsNotNone(
            self.validate_identity("CREATE TABLE foo (bar INT AS (foo))").find(
                exp.TransformColumnConstraint
            )
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

    def test_describe_table(self):
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

    def test_parse_like_any(self):
        like = parse_one("a LIKE ANY fun('foo')", read="snowflake")
        ilike = parse_one("a ILIKE ANY fun('foo')", read="snowflake")

        self.assertIsInstance(like, exp.LikeAny)
        self.assertIsInstance(ilike, exp.ILikeAny)
        like.sql()  # check that this doesn't raise

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
            "REGEXP_SUBSTR_ALL(subject, pattern)",
            "REGEXP_EXTRACT_ALL(subject, pattern)",
        )

    @mock.patch("sqlglot.generator.logger")
    def test_regexp_replace(self, logger):
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern)",
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, '')",
                "duckdb": "REGEXP_REPLACE(subject, pattern, '')",
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
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement)",
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
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement)",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
            },
        )
        self.validate_all(
            "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, parameters)",
            write={
                "bigquery": "REGEXP_REPLACE(subject, pattern, replacement)",
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement, parameters)",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, parameters)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
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
        self.validate_identity("SELECT TRY_CAST(x AS DOUBLE)")

        self.validate_all("TRY_CAST('foo' AS TEXT)", read={"hive": "CAST('foo' AS STRING)"})
        self.validate_all("CAST(5 + 5 AS TEXT)", read={"hive": "CAST(5 + 5 AS STRING)"})
        self.validate_all(
            "CAST(TRY_CAST('2020-01-01' AS DATE) AS TEXT)",
            read={
                "hive": "CAST(CAST('2020-01-01' AS DATE) AS STRING)",
                "snowflake": "CAST(TRY_CAST('2020-01-01' AS DATE) AS TEXT)",
            },
        )
        self.validate_all(
            "TRY_CAST(x AS TEXT)",
            read={
                "hive": "CAST(x AS STRING)",
                "snowflake": "TRY_CAST(x AS TEXT)",
            },
        )

        from sqlglot.optimizer.annotate_types import annotate_types

        expression = parse_one("SELECT CAST(t.x AS STRING) FROM t", read="hive")

        expression = annotate_types(expression, schema={"t": {"x": "string"}})
        self.assertEqual(expression.sql(dialect="snowflake"), "SELECT TRY_CAST(t.x AS TEXT) FROM t")

        expression = annotate_types(expression, schema={"t": {"x": "int"}})
        self.assertEqual(expression.sql(dialect="snowflake"), "SELECT CAST(t.x AS TEXT) FROM t")

        # We can't infer FOO's type since it's a UDF in this case, so we don't get rid of TRY_CAST
        expression = parse_one("SELECT TRY_CAST(FOO() AS TEXT)", read="snowflake")

        expression = annotate_types(expression)
        self.assertEqual(expression.sql(dialect="snowflake"), "SELECT TRY_CAST(FOO() AS TEXT)")

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
        # PUT with file path and stage ref containing spaces (wrapped in single quotes)
        ast = parse_one("PUT 'file://my file.txt' '@s1/my folder'", read="snowflake")
        self.assertIsInstance(ast, exp.Put)
        self.assertEqual(ast.this, exp.Literal(this="file://my file.txt", is_string=True))
        self.assertEqual(ast.args["target"], exp.Var(this="@s1/my folder"))

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
