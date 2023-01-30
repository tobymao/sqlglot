from sqlglot import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    dialect = "snowflake"

    def test_snowflake(self):
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
            "SELECT * EXCLUDE a, b RENAME (c AS d, E as F) FROM xxx",
            write={
                "snowflake": "SELECT * EXCLUDE (a, b) RENAME (c AS d, E AS F) FROM xxx",
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
                "spark": "SELECT FROM_UNIXTIME(1659981729)",
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
                "spark": "SELECT FROM_UNIXTIME('1659981729')",
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
                "snowflake": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-mm-dd hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read={
                "bigquery": "SELECT PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', '04/05/2013 01:02:03')",
                "duckdb": "SELECT STRPTIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S')",
                "snowflake": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            },
            write={
                "bigquery": "SELECT PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', '04/05/2013 01:02:03')",
                "snowflake": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'MM/dd/yyyy HH:mm:ss')",
            },
        )

        self.validate_all(
            "SELECT IFF(TRUE, 'true', 'false')",
            write={
                "snowflake": "SELECT IFF(TRUE, 'true', 'false')",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
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
                "snowflake": r"SELECT 'a \' \\ \\t \\x21 z $ '",
            },
        )
        self.validate_identity("SELECT REGEXP_LIKE(a, b, c)")
        self.validate_all(
            "SELECT RLIKE(a, b)",
            write={
                "snowflake": "SELECT REGEXP_LIKE(a, b)",
            },
        )
        self.validate_all(
            "SELECT a FROM test SAMPLE BLOCK (0.5) SEED (42)",
            write={
                "snowflake": "SELECT a FROM test TABLESAMPLE BLOCK (0.5) SEED (42)",
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
            "DECODE(x, a, b, c, d)",
            read={
                "": "MATCHES(x, a, b, c, d)",
            },
            write={
                "": "MATCHES(x, a, b, c, d)",
                "oracle": "DECODE(x, a, b, c, d)",
                "snowflake": "DECODE(x, a, b, c, d)",
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
                "snowflake": r"SELECT FIRST_VALUE(TABLE1.COLUMN1) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1"
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

    def test_timestamps(self):
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
        self.validate_identity("SELECT EXTRACT(month FROM a)")
        self.validate_all(
            "SELECT EXTRACT('month', a)",
            write={
                "snowflake": "SELECT EXTRACT('month' FROM a)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('month', a)",
            write={
                "snowflake": "SELECT EXTRACT('month' FROM a)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART(month, a::DATETIME)",
            write={
                "snowflake": "SELECT EXTRACT(month FROM CAST(a AS DATETIME))",
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

    def test_semi_structured_types(self):
        self.validate_identity("SELECT CAST(a AS VARIANT)")
        self.validate_all(
            "SELECT a::VARIANT",
            write={
                "snowflake": "SELECT CAST(a AS VARIANT)",
                "tsql": "SELECT CAST(a AS SQL_VARIANT)",
            },
        )
        self.validate_identity("SELECT CAST(a AS ARRAY)")
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
        self.validate_identity(
            "CREATE TABLE a (x DATE, y BIGINT) WITH (PARTITION BY (x), integration='q', auto_refresh=TRUE, file_format=(type = parquet))"
        )
        self.validate_identity("CREATE MATERIALIZED VIEW a COMMENT='...' AS SELECT 1 FROM x")
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
            'SELECT c0, c1 FROM (VALUES (1, 2), (3, 4)) AS "t0"(c0, c1)',
            read={
                "spark": "SELECT `c0`, `c1` FROM (VALUES (1, 2), (3, 4)) AS `t0`(`c0`, `c1`)",
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
  MEASURES y AS b
  {row}
  {after}
  PATTERN (^ S1 S2*? ( {{- S3 -}} S4 )+ | PERMUTE(S1, S2){{1,2}} $)
  DEFINE x AS y
)""",
                    pretty=True,
                )
