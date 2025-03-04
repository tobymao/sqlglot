from sqlglot import exp, parse_one, transpile
from tests.dialects.test_dialect import Validator


class TestRedshift(Validator):
    dialect = "redshift"

    def test_redshift(self):
        self.validate_all(
            "SELECT SPLIT_TO_ARRAY('12,345,6789')",
            write={
                "postgres": "SELECT STRING_TO_ARRAY('12,345,6789', ',')",
                "redshift": "SELECT SPLIT_TO_ARRAY('12,345,6789', ',')",
            },
        )
        self.validate_all(
            "GETDATE()",
            read={
                "duckdb": "CURRENT_TIMESTAMP",
            },
            write={
                "duckdb": "CURRENT_TIMESTAMP",
                "redshift": "GETDATE()",
            },
        )
        self.validate_all(
            """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm', 'barn', 'color')""",
            write={
                "bigquery": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "databricks": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}':farm.barn.color""",
                "duckdb": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm.barn.color'""",
                "postgres": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm', 'barn', 'color')""",
                "presto": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "redshift": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm', 'barn', 'color')""",
                "spark": """SELECT GET_JSON_OBJECT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "sqlite": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm.barn.color'""",
            },
        )
        self.validate_all(
            "LISTAGG(sellerid, ', ')",
            read={
                "duckdb": "STRING_AGG(sellerid, ', ')",
            },
            write={
                # GROUP_CONCAT, LISTAGG and STRING_AGG are aliases in DuckDB
                "duckdb": "LISTAGG(sellerid, ', ')",
                "redshift": "LISTAGG(sellerid, ', ')",
            },
        )
        self.validate_all(
            "SELECT APPROXIMATE COUNT(DISTINCT y)",
            read={
                "spark": "SELECT APPROX_COUNT_DISTINCT(y)",
            },
            write={
                "redshift": "SELECT APPROXIMATE COUNT(DISTINCT y)",
                "spark": "SELECT APPROX_COUNT_DISTINCT(y)",
            },
        )
        self.validate_all(
            "x ~* 'pat'",
            write={
                "redshift": "x ~* 'pat'",
                "snowflake": "REGEXP_LIKE(x, 'pat', 'i')",
            },
        )
        self.validate_all(
            "SELECT CAST('01:03:05.124' AS TIME(2) WITH TIME ZONE)",
            read={
                "postgres": "SELECT CAST('01:03:05.124' AS TIMETZ(2))",
            },
            write={
                "postgres": "SELECT CAST('01:03:05.124' AS TIMETZ(2))",
                "redshift": "SELECT CAST('01:03:05.124' AS TIME(2) WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "SELECT CAST('2020-02-02 01:03:05.124' AS TIMESTAMP(2) WITH TIME ZONE)",
            read={
                "postgres": "SELECT CAST('2020-02-02 01:03:05.124' AS TIMESTAMPTZ(2))",
            },
            write={
                "postgres": "SELECT CAST('2020-02-02 01:03:05.124' AS TIMESTAMPTZ(2))",
                "redshift": "SELECT CAST('2020-02-02 01:03:05.124' AS TIMESTAMP(2) WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "SELECT INTERVAL '5 DAYS'",
            read={
                "": "SELECT INTERVAL '5' days",
            },
        )
        self.validate_all(
            "SELECT ADD_MONTHS('2008-03-31', 1)",
            write={
                "bigquery": "SELECT DATE_ADD(CAST('2008-03-31' AS DATETIME), INTERVAL 1 MONTH)",
                "duckdb": "SELECT CAST('2008-03-31' AS TIMESTAMP) + INTERVAL 1 MONTH",
                "redshift": "SELECT DATEADD(MONTH, 1, '2008-03-31')",
                "trino": "SELECT DATE_ADD('MONTH', 1, CAST('2008-03-31' AS TIMESTAMP))",
                "tsql": "SELECT DATEADD(MONTH, 1, CAST('2008-03-31' AS DATETIME2))",
            },
        )
        self.validate_all(
            "SELECT STRTOL('abc', 16)",
            read={
                "trino": "SELECT FROM_BASE('abc', 16)",
            },
            write={
                "redshift": "SELECT STRTOL('abc', 16)",
                "trino": "SELECT FROM_BASE('abc', 16)",
            },
        )
        self.validate_all(
            "SELECT SNAPSHOT, type",
            write={
                "": "SELECT SNAPSHOT, type",
                "redshift": 'SELECT "SNAPSHOT", "type"',
            },
        )

        self.validate_all(
            "x is true",
            write={
                "redshift": "x IS TRUE",
                "presto": "x",
            },
        )
        self.validate_all(
            "x is false",
            write={
                "redshift": "x IS FALSE",
                "presto": "NOT x",
            },
        )
        self.validate_all(
            "x is not false",
            write={
                "redshift": "NOT x IS FALSE",
                "presto": "NOT NOT x",
            },
        )
        self.validate_all(
            "LEN(x)",
            write={
                "redshift": "LENGTH(x)",
                "presto": "LENGTH(x)",
            },
        )
        self.validate_all(
            "x LIKE 'abc' || '%'",
            read={
                "duckdb": "STARTS_WITH(x, 'abc')",
            },
            write={
                "redshift": "x LIKE 'abc' || '%'",
            },
        )

        self.validate_all(
            "SELECT SYSDATE",
            write={
                "": "SELECT CURRENT_TIMESTAMP()",
                "postgres": "SELECT CURRENT_TIMESTAMP",
                "redshift": "SELECT SYSDATE",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(minute, timestamp '2023-01-04 04:05:06.789')",
            write={
                "postgres": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "redshift": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "snowflake": "SELECT DATE_PART(minute, CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(month, date '20220502')",
            write={
                "postgres": "SELECT EXTRACT(month FROM CAST('20220502' AS DATE))",
                "redshift": "SELECT EXTRACT(month FROM CAST('20220502' AS DATE))",
                "snowflake": "SELECT DATE_PART(month, CAST('20220502' AS DATE))",
            },
        )
        self.validate_all(
            'create table "group" ("col" char(10))',
            write={
                "redshift": 'CREATE TABLE "group" ("col" CHAR(10))',
                "mysql": "CREATE TABLE `group` (`col` CHAR(10))",
            },
        )
        self.validate_all(
            'create table if not exists city_slash_id("city/id" integer not null, state char(2) not null)',
            write={
                "redshift": 'CREATE TABLE IF NOT EXISTS city_slash_id ("city/id" INTEGER NOT NULL, state CHAR(2) NOT NULL)',
                "presto": 'CREATE TABLE IF NOT EXISTS city_slash_id ("city/id" INTEGER NOT NULL, state CHAR(2) NOT NULL)',
            },
        )
        self.validate_all(
            "SELECT ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT(10 20)')::geography)",
            write={
                "redshift": "SELECT ST_ASEWKT(CAST(ST_GEOMFROMEWKT('SRID=4326;POINT(10 20)') AS GEOGRAPHY))",
                "bigquery": "SELECT ST_AsEWKT(CAST(ST_GeomFromEWKT('SRID=4326;POINT(10 20)') AS GEOGRAPHY))",
            },
        )
        self.validate_all(
            "SELECT ST_AsEWKT(ST_GeogFromText('LINESTRING(110 40, 2 3, -10 80, -7 9)')::geometry)",
            write={
                "redshift": "SELECT ST_ASEWKT(CAST(ST_GEOGFROMTEXT('LINESTRING(110 40, 2 3, -10 80, -7 9)') AS GEOMETRY))",
            },
        )
        self.validate_all(
            "SELECT 'abc'::BINARY",
            write={
                "redshift": "SELECT CAST('abc' AS VARBYTE)",
            },
        )
        self.validate_all(
            "CREATE TABLE a (b BINARY VARYING(10))",
            write={
                "redshift": "CREATE TABLE a (b VARBYTE(10))",
            },
        )
        self.validate_all(
            "SELECT 'abc'::CHARACTER",
            write={
                "redshift": "SELECT CAST('abc' AS CHAR)",
            },
        )
        self.validate_all(
            "SELECT DISTINCT ON (a) a, b FROM x ORDER BY c DESC",
            write={
                "bigquery": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "databricks": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "drill": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "hive": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "mysql": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "oracle": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) _t WHERE _row_number = 1",
                "presto": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "redshift": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "snowflake": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "spark": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "sqlite": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "starrocks": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "tableau": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "teradata": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "trino": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "tsql": "SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
            },
        )
        self.validate_all(
            "DECODE(x, a, b, c, d)",
            write={
                "": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d END",
                "oracle": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d END",
                "redshift": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d END",
                "snowflake": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d END",
                "spark": "CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d END",
            },
        )
        self.validate_all(
            "NVL(a, b, c, d)",
            write={
                "redshift": "COALESCE(a, b, c, d)",
                "mysql": "COALESCE(a, b, c, d)",
                "postgres": "COALESCE(a, b, c, d)",
            },
        )

        self.validate_identity(
            "DATEDIFF(days, a, b)",
            "DATEDIFF(DAY, a, b)",
        )

        self.validate_all(
            "DATEDIFF('day', a, b)",
            write={
                "bigquery": "DATE_DIFF(CAST(b AS DATETIME), CAST(a AS DATETIME), DAY)",
                "duckdb": "DATE_DIFF('DAY', CAST(a AS TIMESTAMP), CAST(b AS TIMESTAMP))",
                "hive": "DATEDIFF(b, a)",
                "redshift": "DATEDIFF(DAY, a, b)",
                "presto": "DATE_DIFF('DAY', CAST(a AS TIMESTAMP), CAST(b AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT DATEADD(month, 18, '2008-02-28')",
            write={
                "bigquery": "SELECT DATE_ADD(CAST('2008-02-28' AS DATETIME), INTERVAL 18 MONTH)",
                "duckdb": "SELECT CAST('2008-02-28' AS TIMESTAMP) + INTERVAL 18 MONTH",
                "hive": "SELECT ADD_MONTHS('2008-02-28', 18)",
                "mysql": "SELECT DATE_ADD('2008-02-28', INTERVAL 18 MONTH)",
                "postgres": "SELECT CAST('2008-02-28' AS TIMESTAMP) + INTERVAL '18 MONTH'",
                "presto": "SELECT DATE_ADD('MONTH', 18, CAST('2008-02-28' AS TIMESTAMP))",
                "redshift": "SELECT DATEADD(MONTH, 18, '2008-02-28')",
                "snowflake": "SELECT DATEADD(MONTH, 18, CAST('2008-02-28' AS TIMESTAMP))",
                "tsql": "SELECT DATEADD(MONTH, 18, CAST('2008-02-28' AS DATETIME2))",
                "spark": "SELECT DATE_ADD(MONTH, 18, '2008-02-28')",
                "spark2": "SELECT ADD_MONTHS('2008-02-28', 18)",
                "databricks": "SELECT DATE_ADD(MONTH, 18, '2008-02-28')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(week, '2009-01-01', '2009-12-31')",
            write={
                "bigquery": "SELECT DATE_DIFF(CAST('2009-12-31' AS DATETIME), CAST('2009-01-01' AS DATETIME), WEEK)",
                "duckdb": "SELECT DATE_DIFF('WEEK', CAST('2009-01-01' AS TIMESTAMP), CAST('2009-12-31' AS TIMESTAMP))",
                "hive": "SELECT CAST(DATEDIFF('2009-12-31', '2009-01-01') / 7 AS INT)",
                "postgres": "SELECT CAST(EXTRACT(days FROM (CAST('2009-12-31' AS TIMESTAMP) - CAST('2009-01-01' AS TIMESTAMP))) / 7 AS BIGINT)",
                "presto": "SELECT DATE_DIFF('WEEK', CAST('2009-01-01' AS TIMESTAMP), CAST('2009-12-31' AS TIMESTAMP))",
                "redshift": "SELECT DATEDIFF(WEEK, '2009-01-01', '2009-12-31')",
                "snowflake": "SELECT DATEDIFF(WEEK, '2009-01-01', '2009-12-31')",
                "tsql": "SELECT DATEDIFF(WEEK, '2009-01-01', '2009-12-31')",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(EPOCH FROM CURRENT_DATE)",
            write={
                "snowflake": "SELECT DATE_PART(EPOCH, CURRENT_DATE)",
                "redshift": "SELECT EXTRACT(EPOCH FROM CURRENT_DATE)",
            },
        )

    def test_identity(self):
        self.validate_identity("ALTER TABLE table_name ALTER COLUMN bla TYPE VARCHAR")
        self.validate_identity("SELECT CAST(value AS FLOAT(8))")
        self.validate_identity("1 div", "1 AS div")
        self.validate_identity("LISTAGG(DISTINCT foo, ', ')")
        self.validate_identity("CREATE MATERIALIZED VIEW orders AUTO REFRESH YES AS SELECT 1")
        self.validate_identity("SELECT DATEADD(DAY, 1, 'today')")
        self.validate_identity("SELECT * FROM #x")
        self.validate_identity("SELECT INTERVAL '5 DAY'")
        self.validate_identity("foo$")
        self.validate_identity("CAST('bla' AS SUPER)")
        self.validate_identity("CREATE TABLE real1 (realcol REAL)")
        self.validate_identity("CAST('foo' AS HLLSKETCH)")
        self.validate_identity("'abc' SIMILAR TO '(b|c)%'")
        self.validate_identity("CREATE TABLE datetable (start_date DATE, end_date DATE)")
        self.validate_identity("SELECT APPROXIMATE AS y")
        self.validate_identity("CREATE TABLE t (c BIGINT IDENTITY(0, 1))")
        self.validate_identity(
            "SELECT * FROM venue WHERE (venuecity, venuestate) IN (('Miami', 'FL'), ('Tampa', 'FL')) ORDER BY venueid"
        )
        self.validate_identity(
            """SELECT tablename, "column" FROM pg_table_def WHERE "column" LIKE '%start\\\\_%' LIMIT 5"""
        )
        self.validate_identity(
            """SELECT JSON_EXTRACT_PATH_TEXT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"star"}', 'f4', 'f6', TRUE)"""
        )
        self.validate_identity(
            'DATE_PART(year, "somecol")',
            'EXTRACT(year FROM "somecol")',
        ).this.assert_is(exp.Var)
        self.validate_identity(
            "SELECT CONCAT('abc', 'def')",
            "SELECT 'abc' || 'def'",
        )
        self.validate_identity(
            "SELECT CONCAT_WS('DELIM', 'abc', 'def', 'ghi')",
            "SELECT 'abc' || 'DELIM' || 'def' || 'DELIM' || 'ghi'",
        )
        self.validate_identity(
            "SELECT TOP 1 x FROM y",
            "SELECT x FROM y LIMIT 1",
        )
        self.validate_identity(
            "SELECT DATE_DIFF('month', CAST('2020-02-29 00:00:00' AS TIMESTAMP), CAST('2020-03-02 00:00:00' AS TIMESTAMP))",
            "SELECT DATEDIFF(MONTH, CAST('2020-02-29 00:00:00' AS TIMESTAMP), CAST('2020-03-02 00:00:00' AS TIMESTAMP))",
        )
        self.validate_identity(
            "SELECT * FROM x WHERE y = DATEADD('month', -1, DATE_TRUNC('month', (SELECT y FROM #temp_table)))",
            "SELECT * FROM x WHERE y = DATEADD(MONTH, -1, DATE_TRUNC('MONTH', (SELECT y FROM #temp_table)))",
        )
        self.validate_identity(
            "SELECT 'a''b'",
            "SELECT 'a\\'b'",
        )
        self.validate_identity(
            "CREATE TABLE t (c BIGINT GENERATED BY DEFAULT AS IDENTITY (0, 1))",
            "CREATE TABLE t (c BIGINT IDENTITY(0, 1))",
        )
        self.validate_identity(
            "SELECT DATEADD(HOUR, 0, CAST('2020-02-02 01:03:05.124' AS TIMESTAMP))"
        )
        self.validate_identity(
            "SELECT DATEDIFF(SECOND, '2020-02-02 00:00:00.000', '2020-02-02 01:03:05.124')"
        )
        self.validate_identity(
            "CREATE OR REPLACE VIEW v1 AS SELECT id, AVG(average_metric1) AS m1, AVG(average_metric2) AS m2 FROM t GROUP BY id WITH NO SCHEMA BINDING"
        )
        self.validate_identity(
            "SELECT caldate + INTERVAL '1 SECOND' AS dateplus FROM date WHERE caldate = '12-31-2008'"
        )
        self.validate_identity(
            "SELECT COUNT(*) FROM event WHERE eventname LIKE '%Ring%' OR eventname LIKE '%Die%'"
        )
        self.validate_identity(
            "CREATE TABLE SOUP (LIKE other_table) DISTKEY(soup1) SORTKEY(soup2) DISTSTYLE ALL"
        )
        self.validate_identity(
            "CREATE TABLE sales (salesid INTEGER NOT NULL) DISTKEY(listid) COMPOUND SORTKEY(listid, sellerid) DISTSTYLE AUTO"
        )
        self.validate_identity(
            "COPY customer FROM 's3://mybucket/customer' IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole' REGION 'us-east-1' FORMAT orc",
        )
        self.validate_identity(
            "COPY customer FROM 's3://mybucket/mydata' CREDENTIALS 'aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>;master_symmetric_key=<root-key>' emptyasnull blanksasnull timeformat 'YYYY-MM-DD HH:MI:SS'"
        )
        self.validate_identity(
            "UNLOAD ('select * from venue') TO 's3://mybucket/unload/' IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE TABLE SOUP (SOUP1 VARCHAR(50) NOT NULL ENCODE ZSTD, SOUP2 VARCHAR(70) NULL ENCODE DELTA)"
        )
        self.validate_identity(
            "SELECT DATEADD('day', ndays, caldate)",
            "SELECT DATEADD(DAY, ndays, caldate)",
        )
        self.validate_identity(
            "CONVERT(INT, x)",
            "CAST(x AS INTEGER)",
        )
        self.validate_identity(
            "SELECT DATE_ADD('day', 1, DATE('2023-01-01'))",
            "SELECT DATEADD(DAY, 1, DATE('2023-01-01'))",
        )

        self.validate_identity(
            """SELECT
  c_name,
  orders.o_orderkey AS orderkey,
  index AS orderkey_index
FROM customer_orders_lineitem AS c, c.c_orders AS orders AT index
ORDER BY
  orderkey_index""",
            pretty=True,
        )
        self.validate_identity(
            "SELECT attr AS attr, JSON_TYPEOF(val) AS value_type FROM customer_orders_lineitem AS c, UNPIVOT c.c_orders[0] WHERE c_custkey = 9451"
        )
        self.validate_identity(
            "SELECT attr AS attr, JSON_TYPEOF(val) AS value_type FROM customer_orders_lineitem AS c, UNPIVOT c.c_orders AS val AT attr WHERE c_custkey = 9451"
        )
        self.validate_identity("SELECT JSON_PARSE('[]')")

        self.validate_identity("SELECT ARRAY(1, 2, 3)")
        self.validate_identity("SELECT ARRAY[1, 2, 3]")

        self.validate_identity(
            """SELECT CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000')""",
            """SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', '2024-08-06 09:10:00.000')""",
        )

    def test_values(self):
        # Test crazy-sized VALUES clause to UNION ALL conversion to ensure we don't get RecursionError
        values = [str(v) for v in range(0, 10000)]
        values_query = f"SELECT * FROM (VALUES {', '.join('(' + v + ')' for v in values)})"
        union_query = f"SELECT * FROM ({' UNION ALL '.join('SELECT ' + v for v in values)})"
        self.assertEqual(transpile(values_query, write="redshift")[0], union_query)

        values_sql = transpile("SELECT * FROM (VALUES (1), (2))", write="redshift", pretty=True)[0]
        self.assertEqual(
            values_sql,
            """SELECT
  *
FROM (
  SELECT
    1
  UNION ALL
  SELECT
    2
)""",
        )

        self.validate_identity("INSERT INTO t (a) VALUES (1), (2), (3)")
        self.validate_identity("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)")

        self.validate_all(
            "SELECT * FROM (SELECT 1, 2) AS t",
            read={
                "": "SELECT * FROM (VALUES (1, 2)) AS t",
            },
            write={
                "mysql": "SELECT * FROM (SELECT 1, 2) AS t",
                "presto": "SELECT * FROM (SELECT 1, 2) AS t",
            },
        )
        self.validate_all(
            "SELECT * FROM (SELECT 1 AS id) AS t1 CROSS JOIN (SELECT 1 AS id) AS t2",
            read={
                "": "SELECT * FROM (VALUES (1)) AS t1(id) CROSS JOIN (VALUES (1)) AS t2(id)",
            },
        )
        self.validate_all(
            "SELECT a, b FROM (SELECT 1 AS a, 2 AS b) AS t",
            read={
                "": "SELECT a, b FROM (VALUES (1, 2)) AS t (a, b)",
            },
        )
        self.validate_all(
            'SELECT a, b FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4) AS "t"',
            read={
                "": 'SELECT a, b FROM (VALUES (1, 2), (3, 4)) AS "t" (a, b)',
            },
        )
        self.validate_all(
            "SELECT a, b FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6 UNION ALL SELECT 7, 8) AS t",
            read={
                "": "SELECT a, b FROM (VALUES (1, 2), (3, 4), (5, 6), (7, 8)) AS t (a, b)",
            },
        )
        self.validate_all(
            "INSERT INTO t (a, b) SELECT a, b FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4) AS t",
            read={
                "": "INSERT INTO t(a, b) SELECT a, b FROM (VALUES (1, 2), (3, 4)) AS t (a, b)",
            },
        )
        self.validate_identity("CREATE TABLE table_backup BACKUP NO AS SELECT * FROM event")
        self.validate_identity("CREATE TABLE table_backup BACKUP YES AS SELECT * FROM event")
        self.validate_identity("CREATE TABLE table_backup (i INTEGER, b VARCHAR) BACKUP NO")
        self.validate_identity("CREATE TABLE table_backup (i INTEGER, b VARCHAR) BACKUP YES")
        self.validate_identity(
            "select foo, bar from table_1 minus select foo, bar from table_2",
            "SELECT foo, bar FROM table_1 EXCEPT SELECT foo, bar FROM table_2",
        )

    def test_create_table_like(self):
        self.validate_identity(
            "CREATE TABLE SOUP (LIKE other_table) DISTKEY(soup1) SORTKEY(soup2) DISTSTYLE ALL"
        )

        self.validate_all(
            "CREATE TABLE t1 (LIKE t2)",
            write={
                "postgres": "CREATE TABLE t1 (LIKE t2)",
                "presto": "CREATE TABLE t1 (LIKE t2)",
                "redshift": "CREATE TABLE t1 (LIKE t2)",
                "trino": "CREATE TABLE t1 (LIKE t2)",
            },
        )
        self.validate_all(
            "CREATE TABLE t1 (col VARCHAR, LIKE t2)",
            write={
                "postgres": "CREATE TABLE t1 (col VARCHAR, LIKE t2)",
                "presto": "CREATE TABLE t1 (col VARCHAR, LIKE t2)",
                "redshift": "CREATE TABLE t1 (col VARCHAR, LIKE t2)",
                "trino": "CREATE TABLE t1 (col VARCHAR, LIKE t2)",
            },
        )

    def test_alter_table(self):
        self.validate_identity("ALTER TABLE s.t ALTER SORTKEY (c)")
        self.validate_identity("ALTER TABLE t ALTER SORTKEY AUTO")
        self.validate_identity("ALTER TABLE t ALTER SORTKEY NONE")
        self.validate_identity("ALTER TABLE t ALTER SORTKEY (c1, c2)")
        self.validate_identity("ALTER TABLE t ALTER SORTKEY (c1, c2)")
        self.validate_identity("ALTER TABLE t ALTER COMPOUND SORTKEY (c1, c2)")
        self.validate_identity("ALTER TABLE t ALTER DISTSTYLE ALL")
        self.validate_identity("ALTER TABLE t ALTER DISTSTYLE EVEN")
        self.validate_identity("ALTER TABLE t ALTER DISTSTYLE AUTO")
        self.validate_identity("ALTER TABLE t ALTER DISTSTYLE KEY DISTKEY c")
        self.validate_identity("ALTER TABLE t SET TABLE PROPERTIES ('a' = '5', 'b' = 'c')")
        self.validate_identity("ALTER TABLE t SET LOCATION 's3://bucket/folder/'")
        self.validate_identity("ALTER TABLE t SET FILE FORMAT AVRO")
        self.validate_identity(
            "ALTER TABLE t ALTER DISTKEY c",
            "ALTER TABLE t ALTER DISTSTYLE KEY DISTKEY c",
        )

        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "spark": "ALTER TABLE db.t1 RENAME TO db.t2",
                "redshift": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )

    def test_varchar_max(self):
        self.validate_all(
            'CREATE TABLE "TEST" ("cola" VARCHAR(MAX))',
            read={
                "redshift": "CREATE TABLE TEST (cola VARCHAR(max))",
                "tsql": "CREATE TABLE TEST (cola VARCHAR(max))",
            },
            write={
                "redshift": 'CREATE TABLE "TEST" ("cola" VARCHAR(MAX))',
            },
            identify=True,
        )

    def test_no_schema_binding(self):
        self.validate_all(
            "CREATE OR REPLACE VIEW v1 AS SELECT cola, colb FROM t1 WITH NO SCHEMA BINDING",
            write={
                "redshift": "CREATE OR REPLACE VIEW v1 AS SELECT cola, colb FROM t1 WITH NO SCHEMA BINDING",
            },
        )

    def test_column_unnesting(self):
        self.validate_identity("SELECT c.*, o FROM bloo AS c, c.c_orders AS o")
        self.validate_identity(
            "SELECT c.*, o, l FROM bloo AS c, c.c_orders AS o, o.o_lineitems AS l"
        )

        ast = parse_one("SELECT * FROM t.t JOIN t.c1 ON c1.c2 = t.c3", read="redshift")
        ast.args["from"].this.assert_is(exp.Table)
        ast.args["joins"][0].this.assert_is(exp.Table)
        self.assertEqual(ast.sql("redshift"), "SELECT * FROM t.t JOIN t.c1 ON c1.c2 = t.c3")

        ast = parse_one("SELECT * FROM t AS t CROSS JOIN t.c1", read="redshift")
        ast.args["from"].this.assert_is(exp.Table)
        ast.args["joins"][0].this.assert_is(exp.Unnest)
        self.assertEqual(ast.sql("redshift"), "SELECT * FROM t AS t CROSS JOIN t.c1")

        ast = parse_one(
            "SELECT * FROM x AS a, a.b AS c, c.d.e AS f, f.g.h.i.j.k AS l", read="redshift"
        )
        joins = ast.args["joins"]
        ast.args["from"].this.assert_is(exp.Table)
        joins[0].this.assert_is(exp.Unnest)
        joins[1].this.assert_is(exp.Unnest)
        joins[2].this.assert_is(exp.Unnest).expressions[0].assert_is(exp.Dot)
        self.assertEqual(
            ast.sql("redshift"), "SELECT * FROM x AS a, a.b AS c, c.d.e AS f, f.g.h.i.j.k AS l"
        )

    def test_join_markers(self):
        self.validate_identity(
            "select a.foo, b.bar, a.baz from a, b where a.baz = b.baz (+)",
            "SELECT a.foo, b.bar, a.baz FROM a, b WHERE a.baz = b.baz (+)",
        )

    def test_time(self):
        self.validate_all(
            "TIME_TO_STR(a, '%Y-%m-%d %H:%M:%S.%f')",
            write={"redshift": "TO_CHAR(a, 'YYYY-MM-DD HH24:MI:SS.US')"},
        )

    def test_grant(self):
        grant_cmds = [
            "GRANT SELECT ON ALL TABLES IN SCHEMA qa_tickit TO fred",
            "GRANT USAGE ON DATASHARE salesshare TO NAMESPACE '13b8833d-17c6-4f16-8fe4-1a018f5ed00d'",
            "GRANT USAGE FOR SCHEMAS IN DATABASE Sales_db TO ROLE Sales",
            "GRANT EXECUTE FOR FUNCTIONS IN SCHEMA Sales_schema TO bob",
            "GRANT SELECT FOR TABLES IN DATABASE Sales_db TO alice WITH GRANT OPTION",
            "GRANT ALL FOR TABLES IN SCHEMA ShareSchema DATABASE ShareDb TO ROLE Sales",
            "GRANT ASSUMEROLE ON 'arn:aws:iam::123456789012:role/Redshift-Exfunc' TO reg_user1 FOR EXTERNAL FUNCTION",
            "GRANT ROLE sample_role1 TO ROLE sample_role2",
        ]

        for sql in grant_cmds:
            with self.subTest(f"Testing Redshift's GRANT command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

        self.validate_identity("GRANT SELECT ON TABLE sales TO fred")
        self.validate_identity("GRANT ALL ON SCHEMA qa_tickit TO GROUP qa_users")
        self.validate_identity("GRANT ALL ON TABLE qa_tickit.sales TO GROUP qa_users")
        self.validate_identity(
            "GRANT ALL ON TABLE qa_tickit.sales TO GROUP qa_users, GROUP ro_users"
        )
        self.validate_identity("GRANT ALL ON view_date TO view_user")
        self.validate_identity(
            "GRANT SELECT(cust_name, cust_phone), UPDATE(cust_contact_preference) ON cust_profile TO GROUP sales_group"
        )
        self.validate_identity(
            "GRANT ALL(cust_name, cust_phone, cust_contact_preference) ON cust_profile TO GROUP sales_admin"
        )
        self.validate_identity("GRANT USAGE ON DATABASE sales_db TO Bob")
        self.validate_identity("GRANT USAGE ON SCHEMA sales_schema TO ROLE Analyst_role")
        self.validate_identity("GRANT SELECT ON sales_db.sales_schema.tickit_sales_redshift TO Bob")

    def test_analyze(self):
        self.validate_identity("ANALYZE TBL(col1, col2)")
        self.validate_identity("ANALYZE VERBOSE TBL")
        self.validate_identity("ANALYZE TBL PREDICATE COLUMNS")
        self.validate_identity("ANALYZE TBL ALL COLUMNS")
