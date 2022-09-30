from sqlglot import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    dialect = "snowflake"

    def test_snowflake(self):
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
                "bigquery": "SELECT STR_TO_TIME('2013-04-05 01:02:03', '%Y-%m-%d %H:%M:%S')",
                "snowflake": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-mm-dd hh24:mi:ss')",
                "spark": "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read={
                "bigquery": "SELECT STR_TO_TIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S')",
                "duckdb": "SELECT STRPTIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S')",
                "snowflake": "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            },
            write={
                "bigquery": "SELECT STR_TO_TIME('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S')",
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
            "SELECT DATE_PART(month FROM a::DATETIME)",
            write={
                "snowflake": "SELECT EXTRACT(month FROM CAST(a AS DATETIME))",
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
