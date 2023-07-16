from unittest import mock

from sqlglot import UnsupportedError, exp, parse_one
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    dialect = "snowflake"

    def test_snowflake(self):
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
        self.validate_identity("PUT file:///dir/tmp.csv @%table")
        self.validate_identity("CREATE TABLE foo (bar FLOAT AUTOINCREMENT START 0 INCREMENT 1)")
        self.validate_identity("ALTER TABLE IF EXISTS foo SET TAG a = 'a', b = 'b', c = 'c'")
        self.validate_identity("ALTER TABLE foo UNSET TAG a, b, c")
        self.validate_identity("ALTER TABLE foo SET COMMENT = 'bar'")
        self.validate_identity("ALTER TABLE foo SET CHANGE_TRACKING = FALSE")
        self.validate_identity("ALTER TABLE foo UNSET DATA_RETENTION_TIME_IN_DAYS, CHANGE_TRACKING")
        self.validate_identity("COMMENT IF EXISTS ON TABLE foo IS 'bar'")
        self.validate_identity("SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', col)")
        self.validate_identity(
            'COPY INTO NEW_TABLE ("foo", "bar") FROM (SELECT $1, $2, $3, $4 FROM @%old_table)'
        )
        self.validate_identity(
            "SELECT state, city, SUM(retail_price * quantity) AS gross_revenue FROM sales GROUP BY ALL"
        )

        self.validate_all("CAST(x AS BYTEINT)", write={"snowflake": "CAST(x AS INT)"})
        self.validate_all("CAST(x AS CHAR VARYING)", write={"snowflake": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS CHARACTER VARYING)", write={"snowflake": "CAST(x AS VARCHAR)"})
        self.validate_all("CAST(x AS NCHAR VARYING)", write={"snowflake": "CAST(x AS VARCHAR)"})
        self.validate_all(
            "SELECT * FROM (VALUES (0) foo(bar))",
            write={"snowflake": "SELECT * FROM (VALUES (0)) AS foo(bar)"},
        )
        self.validate_all(
            "OBJECT_CONSTRUCT(a, b, c, d)",
            read={
                "": "STRUCT(a as b, c as d)",
            },
            write={
                "duckdb": "{'a': b, 'c': d}",
                "snowflake": "OBJECT_CONSTRUCT(a, b, c, d)",
            },
        )
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
                "sqlite": "CASE WHEN bar = 0 THEN 0 ELSE foo / bar END",
                "presto": "IF(bar = 0, 0, foo / bar)",
                "spark": "IF(bar = 0, 0, foo / bar)",
                "hive": "IF(bar = 0, 0, foo / bar)",
                "duckdb": "CASE WHEN bar = 0 THEN 0 ELSE foo / bar END",
            },
        )
        self.validate_all(
            "ZEROIFNULL(foo)",
            write={
                "snowflake": "IFF(foo IS NULL, 0, foo)",
                "sqlite": "CASE WHEN foo IS NULL THEN 0 ELSE foo END",
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
                "sqlite": "CASE WHEN foo = 0 THEN NULL ELSE foo END",
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
                "snowflake": "SELECT * EXCLUDE (a, b) FROM xxx",
            },
        )
        self.validate_all(
            "SELECT * RENAME a AS b, c AS d FROM xxx",
            write={
                "snowflake": "SELECT * RENAME (a AS b, c AS d) FROM xxx",
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
            'x:a:"b c"',
            write={
                "duckdb": "x['a']['b c']",
                "hive": "x['a']['b c']",
                "presto": "x['a']['b c']",
                "snowflake": "x['a']['b c']",
                "spark": "x['a']['b c']",
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
                "bigquery": "SELECT UNIX_TO_TIME(1659981729)",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729)",
                "spark": "SELECT CAST(FROM_UNIXTIME(1659981729) AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            write={
                "bigquery": "SELECT UNIX_TO_TIME(1659981729000, 'millis')",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000, 3)",
                "spark": "SELECT TIMESTAMP_MILLIS(1659981729000)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('1659981729')",
            write={
                "bigquery": "SELECT UNIX_TO_TIME('1659981729')",
                "snowflake": "SELECT TO_TIMESTAMP('1659981729')",
                "spark": "SELECT CAST(FROM_UNIXTIME('1659981729') AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
            write={
                "bigquery": "SELECT UNIX_TO_TIME(1659981729000000000, 'micros')",
                "snowflake": "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
                "spark": "SELECT TIMESTAMP_MICROS(1659981729000000000)",
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
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
                "snowflake": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
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
            write={
                "spark": "ARRAY_JOIN(x, '')",
                "snowflake": "ARRAY_TO_STRING(x, '')",
            },
        )
        self.validate_all(
            "TO_ARRAY(x)",
            write={
                "spark": "ARRAY(x)",
                "snowflake": "[x]",
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
            "SELECT NVL2(a, b, c)",
            write={
                "snowflake": "SELECT NVL2(a, b, c)",
            },
        )
        self.validate_all(
            "SELECT $$a$$",
            write={
                "snowflake": "SELECT 'a'",
            },
        )
        self.validate_all(
            r"SELECT $$a ' \ \t \x21 z $ $$",
            write={
                "snowflake": r"SELECT 'a \' \ \t \x21 z $ '",
            },
        )
        self.validate_identity("REGEXP_REPLACE('target', 'pattern', '\n')")
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
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1 RESPECT NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
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
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1 IGNORE NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )
        self.validate_all(
            r"SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1",
            write={
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
            },
        )

    def test_sample(self):
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE (100)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE SYSTEM (3) SEED (82)")
        self.validate_identity("SELECT * FROM testtable TABLESAMPLE (10 ROWS)")
        self.validate_identity("SELECT * FROM testtable SAMPLE (10)")
        self.validate_identity("SELECT * FROM testtable SAMPLE ROW (0)")
        self.validate_identity("SELECT a FROM test SAMPLE BLOCK (0.5) SEED (42)")
        self.validate_identity(
            "SELECT i, j FROM table1 AS t1 INNER JOIN table2 AS t2 TABLESAMPLE (50) WHERE t2.j = t1.i"
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1)"
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
                "snowflake": "SELECT i, j FROM table1 AS t1 SAMPLE (25) /* 25% of rows in table1 */ INNER JOIN table2 AS t2 SAMPLE (50) /* 50% of rows in table2 */ WHERE t2.j = t1.i",
            },
        )
        self.validate_all(
            "SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992)",
            write={
                "snowflake": "SELECT * FROM testtable SAMPLE BLOCK (0.012) SEED (99992)",
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
                "duckdb": "LIST_VALUE(0, 1, 2)",
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

    def test_ddl(self):
        self.validate_identity("CREATE OR REPLACE VIEW foo (uid) COPY GRANTS AS (SELECT 1)")
        self.validate_identity("CREATE TABLE geospatial_table (id INT, g GEOGRAPHY)")
        self.validate_identity("CREATE MATERIALIZED VIEW a COMMENT='...' AS SELECT 1 FROM x")
        self.validate_identity("CREATE DATABASE mytestdb_clone CLONE mytestdb")
        self.validate_identity("CREATE SCHEMA mytestschema_clone CLONE testschema")
        self.validate_identity("CREATE TABLE orders_clone CLONE orders")
        self.validate_identity("CREATE TABLE IDENTIFIER('foo') (COLUMN1 VARCHAR, COLUMN2 VARCHAR)")
        self.validate_identity("CREATE TABLE IDENTIFIER($foo) (col1 VARCHAR, col2 VARCHAR)")
        self.validate_identity(
            "CREATE TABLE orders_clone_restore CLONE orders AT (TIMESTAMP => TO_TIMESTAMP_TZ('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss'))"
        )
        self.validate_identity(
            "CREATE TABLE orders_clone_restore CLONE orders BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')"
        )
        self.validate_identity(
            "CREATE TABLE a (x DATE, y BIGINT) WITH (PARTITION BY (x), integration='q', auto_refresh=TRUE, file_format=(type = parquet))"
        )
        self.validate_identity(
            "CREATE SCHEMA mytestschema_clone_restore CLONE testschema BEFORE (TIMESTAMP => TO_TIMESTAMP(40 * 365 * 86400))"
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE EXAMPLE_DB.DEMO.USERS (ID DECIMAL(38, 0) NOT NULL, PRIMARY KEY (ID), FOREIGN KEY (CITY_CODE) REFERENCES EXAMPLE_DB.DEMO.CITIES (CITY_CODE))"
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
        self.validate_identity("CALL a.b.c(x, y)")
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
  f1.value['type'] AS "Type",
  f1.value['content'] AS "Details"
FROM persons AS p, LATERAL FLATTEN(input => p.c, path => 'contact') AS f, LATERAL FLATTEN(input => f.value['business']) AS f1""",
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
                "spark": """SELECT @1 AS `_1` FROM VALUES ('a'), ('b')""",
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
