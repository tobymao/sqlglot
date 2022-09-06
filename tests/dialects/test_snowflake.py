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
