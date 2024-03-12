from sqlglot import exp, parse_one, transpile
from tests.dialects.test_dialect import Validator


class TestRedshift(Validator):
    dialect = "redshift"

    def test_redshift(self):
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
                "databricks": """SELECT GET_JSON_OBJECT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
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
                # GROUP_CONCAT and STRING_AGG are aliases in DuckDB
                "duckdb": "GROUP_CONCAT(sellerid, ', ')",
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
                "snowflake": "SELECT DATE_PART(minute, CAST('2023-01-04 04:05:06.789' AS TIMESTAMPNTZ))",
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
            "SELECT 'abc'::CHARACTER",
            write={
                "redshift": "SELECT CAST('abc' AS CHAR)",
            },
        )
        self.validate_all(
            "SELECT * FROM venue WHERE (venuecity, venuestate) IN (('Miami', 'FL'), ('Tampa', 'FL')) ORDER BY venueid",
            write={
                "redshift": "SELECT * FROM venue WHERE (venuecity, venuestate) IN (('Miami', 'FL'), ('Tampa', 'FL')) ORDER BY venueid",
            },
        )
        self.validate_all(
            'SELECT tablename, "column" FROM pg_table_def WHERE "column" LIKE \'%start\\_%\' LIMIT 5',
            write={
                "redshift": 'SELECT tablename, "column" FROM pg_table_def WHERE "column" LIKE \'%start\\_%\' LIMIT 5'
            },
        )
        self.validate_all(
            "SELECT DISTINCT ON (a) a, b FROM x ORDER BY c DESC",
            write={
                "bigquery": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "databricks": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "drill": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "hive": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "mysql": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "oracle": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) _t WHERE _row_number = 1",
                "presto": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "redshift": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "snowflake": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "spark": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "sqlite": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "starrocks": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY CASE WHEN c IS NULL THEN 1 ELSE 0 END DESC, c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "tableau": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "teradata": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
                "trino": "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC NULLS FIRST) AS _row_number FROM x) AS _t WHERE _row_number = 1",
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
                "snowflake": "SELECT DATEADD(MONTH, 18, CAST('2008-02-28' AS TIMESTAMPNTZ))",
                "tsql": "SELECT DATEADD(MONTH, 18, CAST('2008-02-28' AS DATETIME2))",
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

    def test_identity(self):
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
            """SELECT JSON_EXTRACT_PATH_TEXT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"star"}', 'f4', 'f6', TRUE)"""
        )
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
            "COPY customer FROM 's3://mybucket/customer' IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'",
            check_command_warning=True,
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

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "spark": "ALTER TABLE db.t1 RENAME TO db.t2",
                "redshift": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )

    def test_varchar_max(self):
        self.validate_all(
            "CREATE TABLE TEST (cola VARCHAR(MAX))",
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
