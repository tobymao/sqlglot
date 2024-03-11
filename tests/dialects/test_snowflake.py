from unittest import mock

from sqlglot import UnsupportedError, exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    maxDiff = None
    dialect = "snowflake"

    def test_snowflake(self):
        self.validate_identity("ALTER TABLE table1 CLUSTER BY (name DESC)")
        self.validate_identity(
            "INSERT OVERWRITE TABLE t SELECT 1", "INSERT OVERWRITE INTO t SELECT 1"
        )
        self.validate_identity("SELECT rename, replace")
        expr = parse_one("SELECT APPROX_TOP_K(C4, 3, 5) FROM t")
        expr.selects[0].assert_is(exp.AggFunc)
        self.assertEqual(expr.sql(dialect="snowflake"), "SELECT APPROX_TOP_K(C4, 3, 5) FROM t")

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
        self.validate_identity("LISTAGG(data['some_field'], ',')")
        self.validate_identity("WEEKOFYEAR(tstamp)")
        self.validate_identity("SELECT SUM(amount) FROM mytable GROUP BY ALL")
        self.validate_identity("WITH x AS (SELECT 1 AS foo) SELECT foo FROM IDENTIFIER('x')")
        self.validate_identity("WITH x AS (SELECT 1 AS foo) SELECT IDENTIFIER('foo') FROM x")
        self.validate_identity("INITCAP('iqamqinterestedqinqthisqtopic', 'q')")
        self.validate_identity("CAST(x AS GEOMETRY)")
        self.validate_identity("OBJECT_CONSTRUCT(*)")
        self.validate_identity("SELECT TO_DATE('2019-02-28') + INTERVAL '1 day, 1 year'")
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
        self.validate_identity("ALTER TABLE IF EXISTS foo SET TAG a = 'a', b = 'b', c = 'c'")
        self.validate_identity("ALTER TABLE foo UNSET TAG a, b, c")
        self.validate_identity("ALTER TABLE foo SET COMMENT = 'bar'")
        self.validate_identity("ALTER TABLE foo SET CHANGE_TRACKING = FALSE")
        self.validate_identity("ALTER TABLE foo UNSET DATA_RETENTION_TIME_IN_DAYS, CHANGE_TRACKING")
        self.validate_identity("COMMENT IF EXISTS ON TABLE foo IS 'bar'")
        self.validate_identity("SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', col)")
        self.validate_identity("REGEXP_REPLACE('target', 'pattern', '\n')")
        self.validate_identity("ALTER TABLE a SWAP WITH b")
        self.validate_identity(
            'DESCRIBE TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."WEB_SITE" type=stage'
        )
        self.validate_identity(
            "SELECT a FROM test PIVOT(SUM(x) FOR y IN ('z', 'q')) AS x TABLESAMPLE (0.1)"
        )
        self.validate_identity(
            "SELECT a:from::STRING, a:from || ' test' ",
            "SELECT CAST(GET_PATH(a, 'from') AS TEXT), GET_PATH(a, 'from') || ' test'",
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
            "SELECT user_id, value FROM table_name SAMPLE ($s) SEED (0)",
            "SELECT user_id, value FROM table_name TABLESAMPLE ($s) SEED (0)",
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
            "SELECT * FROM foo at",
            "SELECT * FROM foo AS at",
        )
        self.validate_identity(
            "SELECT * FROM foo before",
            "SELECT * FROM foo AS before",
        )
        self.validate_identity(
            "SELECT * FROM foo at (col)",
            "SELECT * FROM foo AS at(col)",
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
            "SELECT TO_TIMESTAMP(x) FROM t",
            "SELECT CAST(x AS TIMESTAMPNTZ) FROM t",
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
            "SELECT * FROM example TABLESAMPLE (3) SEED (82)",
            read={
                "databricks": "SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)",
                "duckdb": "SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)",
            },
            write={
                "databricks": "SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)",
                "duckdb": "SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)",
                "snowflake": "SELECT * FROM example TABLESAMPLE (3) SEED (82)",
            },
        )
        self.validate_all(
            "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
            write={
                "duckdb": "SELECT MAKE_TIME(12, 34, 56 + (987654321 / 1000000000.0))",
                "snowflake": "SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)",
            read={
                "duckdb": "SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00)",
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
                "duckdb": """SELECT JSON('{"fruit":"banana"}') -> '$.fruit'""",
                "mysql": """SELECT JSON_EXTRACT('{"fruit":"banana"}', '$.fruit')""",
                "presto": """SELECT JSON_EXTRACT(JSON_PARSE('{"fruit":"banana"}'), '$.fruit')""",
                "snowflake": """SELECT GET_PATH(PARSE_JSON('{"fruit":"banana"}'), 'fruit')""",
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
                "snowflake": "SELECT DATE_PART('year', CAST('2020-01-01' AS TIMESTAMPNTZ))",
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
                "postgres": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "snowflake": "SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test",
                "spark": "SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test",
                "sqlite": "SELECT MIN(c1), MIN(c2) FROM test",
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
                "snowflake": "IFF(bar = 0, 0, foo / bar)",
                "sqlite": "IIF(bar = 0, 0, CAST(foo AS REAL) / bar)",
                "presto": "IF(bar = 0, 0, CAST(foo AS DOUBLE) / bar)",
                "spark": "IF(bar = 0, 0, foo / bar)",
                "hive": "IF(bar = 0, 0, foo / bar)",
                "duckdb": "CASE WHEN bar = 0 THEN 0 ELSE foo / bar END",
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
            "CREATE OR REPLACE TEMPORARY TABLE x (y NUMBER IDENTITY(0, 1))",
            write={
                "snowflake": "CREATE OR REPLACE TEMPORARY TABLE x (y DECIMAL AUTOINCREMENT START 0 INCREMENT 1)",
            },
        )
        self.validate_all(
            "CREATE TEMPORARY TABLE x (y NUMBER AUTOINCREMENT(0, 1))",
            write={
                "snowflake": "CREATE TEMPORARY TABLE x (y DECIMAL AUTOINCREMENT START 0 INCREMENT 1)",
            },
        )
        self.validate_all(
            "CREATE TABLE x (y NUMBER IDENTITY START 0 INCREMENT 1)",
            write={
                "snowflake": "CREATE TABLE x (y DECIMAL AUTOINCREMENT START 0 INCREMENT 1)",
            },
        )
        self.validate_all(
            "ALTER TABLE foo ADD COLUMN id INT identity(1, 1)",
            write={
                "snowflake": "ALTER TABLE foo ADD COLUMN id INT AUTOINCREMENT START 1 INCREMENT 1",
            },
        )
        self.validate_all(
            "SELECT DAYOFWEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)",
            write={
                "snowflake": "SELECT DAYOFWEEK(CAST('2016-01-02T23:39:20.123-07:00' AS TIMESTAMPNTZ))",
            },
        )
        self.validate_all(
            "SELECT * FROM xxx WHERE col ilike '%Don''t%'",
            write={
                "snowflake": "SELECT * FROM xxx WHERE col ILIKE '%Don\\'t%'",
            },
        )
        self.validate_all(
            "SELECT * EXCLUDE a, b FROM xxx",
            write={
                "snowflake": "SELECT * EXCLUDE (a), b FROM xxx",
            },
        )
        self.validate_all(
            "SELECT * RENAME a AS b, c AS d FROM xxx",
            write={
                "snowflake": "SELECT * RENAME (a AS b), c AS d FROM xxx",
            },
        )
        self.validate_all(
            "SELECT * EXCLUDE (a, b) RENAME (c AS d, E AS F) FROM xxx",
            read={
                "duckdb": "SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx",
            },
            write={
                "snowflake": "SELECT * EXCLUDE (a, b) RENAME (c AS d, E AS F) FROM xxx",
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
                "bigquery": "SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2013-04-05 01:02:03')",
                "snowflake": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-mm-DD hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-MM-dd HH:mm:ss')",
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
        self.validate_identity(
            'COPY INTO NEW_TABLE ("foo", "bar") FROM (SELECT $1, $2, $3, $4 FROM @%old_table)',
            check_command_warning=True,
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

    def test_sample(self):
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE (100)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE SYSTEM (3) SEED (82)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE (10 ROWS)")
        self.validate_identity(
            "SELECT i, j FROM table1 AS t1 INNER JOIN table2 AS t2 TABLESAMPLE (50) WHERE t2.j = t1.i"
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1)"
        )
        self.validate_identity(
            "SELECT * FROM testtable SAMPLE (10)",
            "SELECT * FROM testtable TABLESAMPLE (10)",
        )
        self.validate_identity(
            "SELECT * FROM testtable SAMPLE ROW (0)",
            "SELECT * FROM testtable TABLESAMPLE ROW (0)",
        )
        self.validate_identity(
            "SELECT a FROM test SAMPLE BLOCK (0.5) SEED (42)",
            "SELECT a FROM test TABLESAMPLE BLOCK (0.5) SEED (42)",
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
                "snowflake": "SELECT i, j FROM table1 AS t1 TABLESAMPLE (25) /* 25% of rows in table1 */ INNER JOIN table2 AS t2 TABLESAMPLE (50) /* 50% of rows in table2 */ WHERE t2.j = t1.i",
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
                "snowflake": "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1)",
                "spark": "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1 PERCENT)",
            },
        )

    def test_timestamps(self):
        self.validate_identity("SELECT CAST('12:00:00' AS TIME)")
        self.validate_identity("SELECT DATE_PART(month, a)")

        self.validate_all(
            "SELECT CAST(a AS TIMESTAMP)",
            write={
                "snowflake": "SELECT CAST(a AS TIMESTAMPNTZ)",
            },
        )
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
                "snowflake": "SELECT EXTRACT(epoch_second FROM CAST(foo AS TIMESTAMPNTZ)) AS ddate FROM table_name",
                "presto": "SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) AS ddate FROM table_name",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(epoch_milliseconds, foo) as ddate from table_name",
            write={
                "snowflake": "SELECT EXTRACT(epoch_second FROM CAST(foo AS TIMESTAMPNTZ)) * 1000 AS ddate FROM table_name",
                "presto": "SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) * 1000 AS ddate FROM table_name",
            },
        )
        self.validate_all(
            "DATEADD(DAY, 5, CAST('2008-12-25' AS DATE))",
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
            "SELECT * FROM my_table AT (TIMESTAMP => CAST('Fri, 01 May 2015 16:20:00 -0700' AS TIMESTAMPNTZ))",
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

    def test_ddl(self):
        self.validate_identity(
            """create external table et2(
  col1 date as (parse_json(metadata$external_table_partition):COL1::date),
  col2 varchar as (parse_json(metadata$external_table_partition):COL2::varchar),
  col3 number as (parse_json(metadata$external_table_partition):COL3::number))
  partition by (col1,col2,col3)
  location=@s2/logs/
  partition_type = user_specified
  file_format = (type = parquet)""",
            "CREATE EXTERNAL TABLE et2 (col1 DATE AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL1') AS DATE)), col2 VARCHAR AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL2') AS VARCHAR)), col3 DECIMAL AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), 'COL3') AS DECIMAL))) LOCATION @s2/logs/ PARTITION BY (col1, col2, col3) partition_type=user_specified file_format=(type = parquet)",
        )
        self.validate_identity("CREATE OR REPLACE VIEW foo (uid) COPY GRANTS AS (SELECT 1)")
        self.validate_identity("CREATE TABLE geospatial_table (id INT, g GEOGRAPHY)")
        self.validate_identity("CREATE MATERIALIZED VIEW a COMMENT='...' AS SELECT 1 FROM x")
        self.validate_identity("CREATE DATABASE mytestdb_clone CLONE mytestdb")
        self.validate_identity("CREATE SCHEMA mytestschema_clone CLONE testschema")
        self.validate_identity("CREATE TABLE IDENTIFIER('foo') (COLUMN1 VARCHAR, COLUMN2 VARCHAR)")
        self.validate_identity("CREATE TABLE IDENTIFIER($foo) (col1 VARCHAR, col2 VARCHAR)")
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

    def test_table_literal(self):
        # All examples from https://docs.snowflake.com/en/sql-reference/literals-table.html
        self.validate_all(
            r"""SELECT * FROM TABLE('MYTABLE')""",
            write={"snowflake": r"""SELECT * FROM TABLE('MYTABLE')"""},
        )

        self.validate_all(
            r"""SELECT * FROM TABLE('MYDB."MYSCHEMA"."MYTABLE"')""",
            write={"snowflake": r"""SELECT * FROM TABLE('MYDB."MYSCHEMA"."MYTABLE"')"""},
        )

        # Per Snowflake documentation at https://docs.snowflake.com/en/sql-reference/literals-table.html
        # one can use either a  " ' " or " $$ " to enclose the object identifier.
        # Capturing the single tokens seems like lot of work. Hence adjusting tests to use these interchangeably,
        self.validate_all(
            r"""SELECT * FROM TABLE($$MYDB. "MYSCHEMA"."MYTABLE"$$)""",
            write={"snowflake": r"""SELECT * FROM TABLE('MYDB. "MYSCHEMA"."MYTABLE"')"""},
        )

        self.validate_all(
            r"""SELECT * FROM TABLE($MYVAR)""",
            write={"snowflake": r"""SELECT * FROM TABLE($MYVAR)"""},
        )

        self.validate_all(
            r"""SELECT * FROM TABLE(?)""",
            write={"snowflake": r"""SELECT * FROM TABLE(?)"""},
        )

        self.validate_all(
            r"""SELECT * FROM TABLE(:BINDING)""",
            write={"snowflake": r"""SELECT * FROM TABLE(:BINDING)"""},
        )

        self.validate_all(
            r"""SELECT * FROM TABLE($MYVAR) WHERE COL1 = 10""",
            write={"snowflake": r"""SELECT * FROM TABLE($MYVAR) WHERE COL1 = 10"""},
        )

    def test_flatten(self):
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
                "presto": "REGEXP_EXTRACT(subject, pattern, group)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern, pos, occ, params, group)",
                "spark": "REGEXP_EXTRACT(subject, pattern, group)",
            },
        )
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern)",
            read={
                "bigquery": "REGEXP_EXTRACT(subject, pattern)",
                "hive": "REGEXP_EXTRACT(subject, pattern)",
                "presto": "REGEXP_EXTRACT(subject, pattern)",
                "spark": "REGEXP_EXTRACT(subject, pattern)",
            },
            write={
                "bigquery": "REGEXP_EXTRACT(subject, pattern)",
                "hive": "REGEXP_EXTRACT(subject, pattern)",
                "presto": "REGEXP_EXTRACT(subject, pattern)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern)",
                "spark": "REGEXP_EXTRACT(subject, pattern)",
            },
        )
        self.validate_all(
            "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', group)",
            read={
                "bigquery": "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', group)",
                "duckdb": "REGEXP_EXTRACT(subject, pattern, group)",
                "hive": "REGEXP_EXTRACT(subject, pattern, group)",
                "presto": "REGEXP_EXTRACT(subject, pattern, group)",
                "snowflake": "REGEXP_SUBSTR(subject, pattern, 1, 1, 'c', group)",
                "spark": "REGEXP_EXTRACT(subject, pattern, group)",
            },
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
                "duckdb": "REGEXP_REPLACE(subject, pattern, replacement)",
                "hive": "REGEXP_REPLACE(subject, pattern, replacement)",
                "snowflake": "REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, parameters)",
                "spark": "REGEXP_REPLACE(subject, pattern, replacement, position)",
            },
        )

    def test_match_recognize(self):
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
                self.validate_identity(
                    f"""SELECT
  *
FROM x
MATCH_RECOGNIZE (
  PARTITION BY a, b
  ORDER BY
    x DESC
  MEASURES
    y AS b
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
        )

    def test_swap(self):
        ast = parse_one("ALTER TABLE a SWAP WITH b", read="snowflake")
        assert isinstance(ast, exp.AlterTable)
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
