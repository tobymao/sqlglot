from tests.dialects.test_dialect import Validator

from sqlglot import exp


class TestHive(Validator):
    dialect = "hive"

    def test_bits(self):
        self.validate_all(
            "x & 1",
            read={
                "duckdb": "x & 1",
                "presto": "BITWISE_AND(x, 1)",
                "spark": "x & 1",
            },
            write={
                "duckdb": "x & 1",
                "hive": "x & 1",
                "presto": "BITWISE_AND(x, 1)",
                "spark": "x & 1",
            },
        )
        self.validate_all(
            "x & 1 > 0",
            read={
                "duckdb": "x & 1 > 0",
                "presto": "BITWISE_AND(x, 1) > 0",
                "spark": "x & 1 > 0",
            },
            write={
                "duckdb": "x & 1 > 0",
                "presto": "BITWISE_AND(x, 1) > 0",
                "hive": "x & 1 > 0",
                "spark": "x & 1 > 0",
            },
        )
        self.validate_all(
            "~x",
            read={
                "duckdb": "~x",
                "presto": "BITWISE_NOT(x)",
                "spark": "~x",
            },
            write={
                "duckdb": "~x",
                "hive": "~x",
                "presto": "BITWISE_NOT(x)",
                "spark": "~x",
            },
        )
        self.validate_all(
            "x | 1",
            read={
                "duckdb": "x | 1",
                "presto": "BITWISE_OR(x, 1)",
                "spark": "x | 1",
            },
            write={
                "duckdb": "x | 1",
                "hive": "x | 1",
                "presto": "BITWISE_OR(x, 1)",
                "spark": "x | 1",
            },
        )
        self.validate_all(
            "x << 1",
            read={
                "spark": "SHIFTLEFT(x, 1)",
            },
            write={
                "duckdb": "x << 1",
                "presto": "BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)",
                "hive": "x << 1",
                "spark": "SHIFTLEFT(x, 1)",
            },
        )
        self.validate_all(
            "x >> 1",
            read={
                "spark": "SHIFTRIGHT(x, 1)",
            },
            write={
                "duckdb": "x >> 1",
                "presto": "BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
                "hive": "x >> 1",
                "spark": "SHIFTRIGHT(x, 1)",
            },
        )

    def test_cast(self):
        self.validate_all(
            "1s",
            write={
                "duckdb": "TRY_CAST(1 AS SMALLINT)",
                "presto": "TRY_CAST(1 AS SMALLINT)",
                "hive": "CAST(1 AS SMALLINT)",
                "spark": "CAST(1 AS SMALLINT)",
            },
        )
        self.validate_all(
            "1S",
            write={
                "duckdb": "TRY_CAST(1 AS SMALLINT)",
                "presto": "TRY_CAST(1 AS SMALLINT)",
                "hive": "CAST(1 AS SMALLINT)",
                "spark": "CAST(1 AS SMALLINT)",
            },
        )
        self.validate_all(
            "1Y",
            write={
                "duckdb": "TRY_CAST(1 AS TINYINT)",
                "presto": "TRY_CAST(1 AS TINYINT)",
                "hive": "CAST(1 AS TINYINT)",
                "spark": "CAST(1 AS TINYINT)",
            },
        )
        self.validate_all(
            "1L",
            write={
                "duckdb": "TRY_CAST(1 AS BIGINT)",
                "presto": "TRY_CAST(1 AS BIGINT)",
                "hive": "CAST(1 AS BIGINT)",
                "spark": "CAST(1 AS BIGINT)",
            },
        )
        self.validate_all(
            "1.0bd",
            write={
                "duckdb": "TRY_CAST(1.0 AS DECIMAL)",
                "presto": "TRY_CAST(1.0 AS DECIMAL)",
                "hive": "CAST(1.0 AS DECIMAL)",
                "spark": "CAST(1.0 AS DECIMAL)",
            },
        )
        self.validate_all(
            "CAST(1 AS INT)",
            read={
                "presto": "TRY_CAST(1 AS INT)",
            },
            write={
                "duckdb": "TRY_CAST(1 AS INT)",
                "presto": "TRY_CAST(1 AS INTEGER)",
                "hive": "CAST(1 AS INT)",
                "spark": "CAST(1 AS INT)",
            },
        )

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            write={
                "duckdb": "CREATE TABLE x (w TEXT)",  # Partition columns should exist in table
                "presto": "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY=ARRAY['y', 'z'])",
                "hive": "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
                "spark": "CREATE TABLE x (w STRING, y INT, z INT) PARTITIONED BY (y, z)",
            },
        )
        self.validate_all(
            "CREATE TABLE test STORED AS parquet TBLPROPERTIES ('x'='1', 'Z'='2') AS SELECT 1",
            write={
                "duckdb": "CREATE TABLE test AS SELECT 1",
                "presto": "CREATE TABLE test WITH (FORMAT='PARQUET', x='1', Z='2') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET TBLPROPERTIES ('x'='1', 'Z'='2') AS SELECT 1",
                "spark": "CREATE TABLE test USING PARQUET TBLPROPERTIES ('x'='1', 'Z'='2') AS SELECT 1",
            },
        )

        self.validate_identity(
            """CREATE EXTERNAL TABLE x (y INT) ROW FORMAT SERDE 'serde' ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' WITH SERDEPROPERTIES ('input.regex'='')""",
        )
        self.validate_identity(
            """CREATE EXTERNAL TABLE `my_table` (`a7` ARRAY<DATE>) ROW FORMAT SERDE 'a' STORED AS INPUTFORMAT 'b' OUTPUTFORMAT 'c' LOCATION 'd' TBLPROPERTIES ('e'='f')"""
        )
        self.validate_identity("CREATE EXTERNAL TABLE X (y INT) STORED BY 'x'")
        self.validate_identity("ALTER VIEW v1 AS SELECT x, UPPER(s) AS s FROM t2")
        self.validate_identity("ALTER VIEW v1 (c1, c2) AS SELECT x, UPPER(s) AS s FROM t2")
        self.validate_identity(
            "ALTER VIEW v7 (c1 COMMENT 'Comment for c1', c2) AS SELECT t1.c1, t1.c2 FROM t1"
        )
        self.validate_identity("ALTER VIEW db1.v1 RENAME TO db2.v2")
        self.validate_identity("ALTER VIEW v1 SET TBLPROPERTIES ('tblp1'='1', 'tblp2'='2')")
        self.validate_identity(
            "ALTER VIEW v1 UNSET TBLPROPERTIES ('tblp1', 'tblp2')", check_command_warning=True
        )

    def test_lateral_view(self):
        self.validate_all(
            "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
            write={
                "presto": "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)",
                "duckdb": "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)",
                "hive": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
                "spark": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
            },
        )
        self.validate_all(
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
                "duckdb": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
                "hive": "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            },
        )
        self.validate_all(
            "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t(a)",
                "hive": "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
                "spark": "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            },
        )
        self.validate_all(
            "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t(a)",
                "duckdb": "SELECT a FROM x CROSS JOIN UNNEST([y]) AS t(a)",
                "hive": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
            },
        )

    def test_quotes(self):
        self.validate_all(
            "'\\''",
            write={
                "duckdb": "''''",
                "presto": "''''",
                "hive": "'\\''",
                "spark": "'\\''",
            },
        )
        self.validate_all(
            "'\"x\"'",
            write={
                "duckdb": "'\"x\"'",
                "presto": "'\"x\"'",
                "hive": "'\"x\"'",
                "spark": "'\"x\"'",
            },
        )
        self.validate_all(
            "\"'x'\"",
            write={
                "duckdb": "'''x'''",
                "presto": "'''x'''",
                "hive": "'\\'x\\''",
                "spark": "'\\'x\\''",
            },
        )
        self.validate_all(
            "'\\\\\\\\a'",
            read={
                "drill": "'\\\\\\\\a'",
                "duckdb": "'\\\\a'",
                "presto": "'\\\\a'",
            },
            write={
                "drill": "'\\\\\\\\a'",
                "duckdb": "'\\\\a'",
                "hive": "'\\\\\\\\a'",
                "presto": "'\\\\a'",
                "spark": "'\\\\\\\\a'",
            },
        )

    def test_regex(self):
        self.validate_all(
            "a RLIKE 'x'",
            write={
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "hive": "a RLIKE 'x'",
                "spark": "a RLIKE 'x'",
            },
        )

        self.validate_all(
            "a REGEXP 'x'",
            write={
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "hive": "a RLIKE 'x'",
                "spark": "a RLIKE 'x'",
            },
        )

    def test_time(self):
        self.validate_all(
            "(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) * 1000",
            read={
                "presto": "DATE_DIFF('millisecond', x, y)",
            },
        )
        self.validate_all(
            "UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)",
            read={
                "presto": "DATE_DIFF('second', x, y)",
            },
        )
        self.validate_all(
            "(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) / 60",
            read={
                "presto": "DATE_DIFF('minute', x, y)",
            },
        )
        self.validate_all(
            "(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) / 3600",
            read={
                "presto": "DATE_DIFF('hour', x, y)",
            },
        )
        self.validate_all(
            "DATEDIFF(a, b)",
            write={
                "duckdb": "DATE_DIFF('DAY', CAST(b AS DATE), CAST(a AS DATE))",
                "presto": "DATE_DIFF('DAY', CAST(CAST(b AS TIMESTAMP) AS DATE), CAST(CAST(a AS TIMESTAMP) AS DATE))",
                "hive": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
                "spark": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
                "": "DATEDIFF(CAST(a AS DATE), CAST(b AS DATE))",
            },
        )
        self.validate_all(
            """from_unixtime(x, "yyyy-MM-dd'T'HH")""",
            write={
                "duckdb": "STRFTIME(TO_TIMESTAMP(x), '%Y-%m-%d''T''%H')",
                "presto": "DATE_FORMAT(FROM_UNIXTIME(x), '%Y-%m-%d''T''%H')",
                "hive": "FROM_UNIXTIME(x, 'yyyy-MM-dd\\'T\\'HH')",
                "spark": "FROM_UNIXTIME(x, 'yyyy-MM-dd\\'T\\'HH')",
            },
        )
        self.validate_all(
            "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            write={
                "bigquery": "FORMAT_DATE('%Y-%m-%d %H:%M:%S', CAST('2020-01-01' AS DATETIME))",
                "duckdb": "STRFTIME(CAST('2020-01-01' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_FORMAT(CAST('2020-01-01' AS TIMESTAMP), '%Y-%m-%d %T')",
                "hive": "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
                "spark": "DATE_FORMAT('2020-01-01', 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "DATE_ADD('2020-01-01', 1)",
            write={
                "": "TS_OR_DS_ADD('2020-01-01', 1, DAY)",
                "bigquery": "DATE_ADD(CAST(CAST('2020-01-01' AS DATETIME) AS DATE), INTERVAL 1 DAY)",
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD('2020-01-01', 1)",
                "presto": "DATE_ADD('DAY', 1, CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE))",
                "redshift": "DATEADD(DAY, 1, '2020-01-01')",
                "snowflake": "DATEADD(DAY, 1, CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE))",
                "spark": "DATE_ADD('2020-01-01', 1)",
                "tsql": "DATEADD(DAY, 1, CAST(CAST('2020-01-01' AS DATETIME2) AS DATE))",
            },
        )
        self.validate_all(
            "DATE_SUB('2020-01-01', 1)",
            write={
                "": "TS_OR_DS_ADD('2020-01-01', 1 * -1, DAY)",
                "bigquery": "DATE_ADD(CAST(CAST('2020-01-01' AS DATETIME) AS DATE), INTERVAL (1 * -1) DAY)",
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL (1 * -1) DAY",
                "hive": "DATE_ADD('2020-01-01', 1 * -1)",
                "presto": "DATE_ADD('DAY', 1 * -1, CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE))",
                "redshift": "DATEADD(DAY, 1 * -1, '2020-01-01')",
                "snowflake": "DATEADD(DAY, 1 * -1, CAST(CAST('2020-01-01' AS TIMESTAMP) AS DATE))",
                "spark": "DATE_ADD('2020-01-01', 1 * -1)",
                "tsql": "DATEADD(DAY, 1 * -1, CAST(CAST('2020-01-01' AS DATETIME2) AS DATE))",
            },
        )
        self.validate_all("DATE_ADD('2020-01-01', -1)", read={"": "DATE_SUB('2020-01-01', 1)"})
        self.validate_all("DATE_ADD(a, b * -1)", read={"": "DATE_SUB(a, b)"})
        self.validate_all(
            "ADD_MONTHS('2020-01-01', -2)", read={"": "DATE_SUB('2020-01-01', 2, month)"}
        )
        self.validate_all(
            "DATEDIFF(TO_DATE(y), x)",
            write={
                "duckdb": "DATE_DIFF('DAY', CAST(x AS DATE), CAST(y AS DATE))",
                "presto": "DATE_DIFF('DAY', CAST(CAST(x AS TIMESTAMP) AS DATE), CAST(CAST(CAST(CAST(y AS TIMESTAMP) AS DATE) AS TIMESTAMP) AS DATE))",
                "hive": "DATEDIFF(TO_DATE(y), TO_DATE(x))",
                "spark": "DATEDIFF(TO_DATE(y), TO_DATE(x))",
                "": "DATEDIFF(CAST(y AS DATE), CAST(x AS DATE))",
            },
        )
        self.validate_all(
            "UNIX_TIMESTAMP(x)",
            write={
                "duckdb": "EPOCH(STRPTIME(x, '%Y-%m-%d %H:%M:%S'))",
                "presto": "TO_UNIXTIME(COALESCE(TRY(DATE_PARSE(CAST(x AS VARCHAR), '%Y-%m-%d %T')), PARSE_DATETIME(DATE_FORMAT(x, '%Y-%m-%d %T'), 'yyyy-MM-dd HH:mm:ss')))",
                "hive": "UNIX_TIMESTAMP(x)",
                "spark": "UNIX_TIMESTAMP(x)",
                "": "STR_TO_UNIX(x, '%Y-%m-%d %H:%M:%S')",
            },
        )

        for unit in ("DAY", "MONTH", "YEAR"):
            self.validate_all(
                f"{unit}(x)",
                write={
                    "duckdb": f"{unit}(CAST(x AS DATE))",
                    "presto": f"{unit}(CAST(CAST(x AS TIMESTAMP) AS DATE))",
                    "hive": f"{unit}(TO_DATE(x))",
                    "spark": f"{unit}(TO_DATE(x))",
                },
            )

    def test_order_by(self):
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            },
        )

    def test_hive(self):
        self.validate_identity("SELECT * FROM t WHERE col IN ('stream')")
        self.validate_identity("SET hiveconf:some_var = 5", check_command_warning=True)
        self.validate_identity("(VALUES (1 AS a, 2 AS b, 3))")
        self.validate_identity("SELECT * FROM my_table TIMESTAMP AS OF DATE_ADD(CURRENT_DATE, -1)")
        self.validate_identity("SELECT * FROM my_table VERSION AS OF DATE_ADD(CURRENT_DATE, -1)")
        self.validate_identity(
            "SELECT WEEKOFYEAR('2024-05-22'), DAYOFMONTH('2024-05-22'), DAYOFWEEK('2024-05-22')"
        )
        self.validate_identity(
            "SELECT ROW() OVER (DISTRIBUTE BY x SORT BY y)",
            "SELECT ROW() OVER (PARTITION BY x ORDER BY y)",
        )
        self.validate_identity("SELECT transform")
        self.validate_identity("SELECT * FROM test DISTRIBUTE BY y SORT BY x DESC ORDER BY l")
        self.validate_identity(
            "SELECT * FROM test WHERE RAND() <= 0.1 DISTRIBUTE BY RAND() SORT BY RAND()"
        )
        self.validate_identity("(SELECT 1 UNION SELECT 2) DISTRIBUTE BY z")
        self.validate_identity("(SELECT 1 UNION SELECT 2) DISTRIBUTE BY z SORT BY x")
        self.validate_identity("(SELECT 1 UNION SELECT 2) CLUSTER BY y DESC")
        self.validate_identity("SELECT * FROM test CLUSTER BY y")

        self.validate_identity("(SELECT 1 UNION SELECT 2) SORT BY z")
        self.validate_identity(
            "INSERT OVERWRITE TABLE zipcodes PARTITION(state = '0') VALUES (896, 'US', 'TAMPA', 33607)"
        )
        self.validate_identity(
            "INSERT OVERWRITE TABLE zipcodes PARTITION(state = 0) VALUES (896, 'US', 'TAMPA', 33607)"
        )
        self.validate_identity(
            "INSERT OVERWRITE DIRECTORY 'x' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' LINES TERMINATED BY '' STORED AS TEXTFILE SELECT * FROM `a`.`b`"
        )
        self.validate_identity(
            "SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, b, GROUPING SETS ((a, b), a)"
        )
        self.validate_identity(
            "SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, b, GROUPING SETS ((t.a, b), a)"
        )
        self.validate_identity(
            "SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, FOO(b), GROUPING SETS ((a, FOO(b)), a)"
        )
        self.validate_identity(
            "SELECT key, value, GROUPING__ID, COUNT(*) FROM T1 GROUP BY key, value WITH CUBE"
        )
        self.validate_identity(
            "SELECT key, value, GROUPING__ID, COUNT(*) FROM T1 GROUP BY key, value WITH ROLLUP"
        )
        self.validate_identity(
            "TRUNCATE TABLE t1 PARTITION(age = 10, name = 'test', address = 'abc')"
        )

        self.validate_all(
            "SELECT ${hiveconf:some_var}",
            write={
                "hive": "SELECT ${hiveconf:some_var}",
                "spark": "SELECT ${hiveconf:some_var}",
            },
        )
        self.validate_all(
            "SELECT A.1a AS b FROM test_a AS A",
            write={
                "spark": "SELECT A.1a AS b FROM test_a AS A",
            },
        )
        self.validate_all(
            "SELECT 1_a AS a FROM test_table",
            write={
                "spark": "SELECT 1_a AS a FROM test_table",
                "trino": 'SELECT "1_a" AS a FROM test_table',
            },
        )
        self.validate_all(
            "SELECT a_b AS 1_a FROM test_table",
            write={
                "spark": "SELECT a_b AS 1_a FROM test_table",
            },
        )
        self.validate_all(
            "SELECT 1a_1a FROM test_a",
            write={
                "spark": "SELECT 1a_1a FROM test_a",
            },
        )
        self.validate_all(
            "SELECT 1a AS 1a_1a FROM test_a",
            write={
                "spark": "SELECT 1a AS 1a_1a FROM test_a",
            },
        )
        self.validate_all(
            "CREATE TABLE test_table (1a STRING)",
            write={
                "spark": "CREATE TABLE test_table (1a STRING)",
            },
        )
        self.validate_all(
            "CREATE TABLE test_table2 (1a_1a STRING)",
            write={
                "spark": "CREATE TABLE test_table2 (1a_1a STRING)",
            },
        )
        self.validate_all(
            "PERCENTILE(x, 0.5)",
            write={
                "duckdb": "QUANTILE(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "hive": "PERCENTILE(x, 0.5)",
                "spark": "PERCENTILE(x, 0.5)",
            },
        )
        self.validate_all(
            "PERCENTILE_APPROX(x, 0.5)",
            read={
                "hive": "PERCENTILE_APPROX(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "duckdb": "APPROX_QUANTILE(x, 0.5)",
                "spark": "PERCENTILE_APPROX(x, 0.5)",
            },
            write={
                "hive": "PERCENTILE_APPROX(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "duckdb": "APPROX_QUANTILE(x, 0.5)",
                "spark": "PERCENTILE_APPROX(x, 0.5)",
            },
        )
        self.validate_all(
            "APPROX_COUNT_DISTINCT(a)",
            write={
                "bigquery": "APPROX_COUNT_DISTINCT(a)",
                "duckdb": "APPROX_COUNT_DISTINCT(a)",
                "presto": "APPROX_DISTINCT(a)",
                "hive": "APPROX_COUNT_DISTINCT(a)",
                "snowflake": "APPROX_COUNT_DISTINCT(a)",
                "spark": "APPROX_COUNT_DISTINCT(a)",
            },
        )
        self.validate_all(
            "ARRAY_CONTAINS(x, 1)",
            read={
                "duckdb": "LIST_HAS(x, 1)",
                "snowflake": "ARRAY_CONTAINS(1, x)",
            },
            write={
                "duckdb": "ARRAY_CONTAINS(x, 1)",
                "presto": "CONTAINS(x, 1)",
                "hive": "ARRAY_CONTAINS(x, 1)",
                "spark": "ARRAY_CONTAINS(x, 1)",
                "snowflake": "ARRAY_CONTAINS(1, x)",
            },
        )
        self.validate_all(
            "SIZE(x)",
            write={
                "duckdb": "ARRAY_LENGTH(x)",
                "presto": "CARDINALITY(x)",
                "hive": "SIZE(x)",
                "spark": "SIZE(x)",
            },
        )
        self.validate_all(
            "LOCATE('a', x)",
            write={
                "duckdb": "STRPOS(x, 'a')",
                "presto": "STRPOS(x, 'a')",
                "hive": "LOCATE('a', x)",
                "spark": "LOCATE('a', x)",
            },
        )
        self.validate_all(
            "LOCATE('a', x, 3)",
            write={
                "duckdb": "CASE WHEN STRPOS(SUBSTRING(x, 3), 'a') = 0 THEN 0 ELSE STRPOS(SUBSTRING(x, 3), 'a') + 3 - 1 END",
                "presto": "IF(STRPOS(SUBSTRING(x, 3), 'a') = 0, 0, STRPOS(SUBSTRING(x, 3), 'a') + 3 - 1)",
                "hive": "LOCATE('a', x, 3)",
                "spark": "LOCATE('a', x, 3)",
            },
        )
        self.validate_all(
            "INITCAP('new york')",
            write={
                "duckdb": "INITCAP('new york')",
                "presto": r"REGEXP_REPLACE('new york', '(\w)(\w*)', x -> UPPER(x[1]) || LOWER(x[2]))",
                "hive": "INITCAP('new york')",
                "spark": "INITCAP('new york')",
            },
        )
        self.validate_all(
            "SELECT * FROM x.z TABLESAMPLE(10 PERCENT) y",
            write={
                "hive": "SELECT * FROM x.z TABLESAMPLE (10 PERCENT) AS y",
                "spark": "SELECT * FROM x.z TABLESAMPLE (10 PERCENT) AS y",
            },
        )
        self.validate_all(
            "SELECT SORT_ARRAY(x)",
            write={
                "duckdb": "SELECT ARRAY_SORT(x)",
                "presto": "SELECT ARRAY_SORT(x)",
                "hive": "SELECT SORT_ARRAY(x)",
                "spark": "SELECT SORT_ARRAY(x)",
            },
        )
        self.validate_all(
            "SELECT SORT_ARRAY(x, FALSE)",
            read={
                "duckdb": "SELECT ARRAY_REVERSE_SORT(x)",
                "spark": "SELECT SORT_ARRAY(x, FALSE)",
            },
            write={
                "duckdb": "SELECT ARRAY_REVERSE_SORT(x)",
                "presto": "SELECT ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SELECT SORT_ARRAY(x, FALSE)",
                "spark": "SELECT SORT_ARRAY(x, FALSE)",
            },
        )
        self.validate_all(
            "GET_JSON_OBJECT(x, '$.name')",
            write={
                "presto": "JSON_EXTRACT_SCALAR(x, '$.name')",
                "hive": "GET_JSON_OBJECT(x, '$.name')",
                "spark": "GET_JSON_OBJECT(x, '$.name')",
            },
        )
        self.validate_all(
            "MAP(a, b, c, d)",
            read={
                "": "VAR_MAP(a, b, c, d)",
                "clickhouse": "map(a, b, c, d)",
                "duckdb": "MAP([a, c], [b, d])",
                "hive": "MAP(a, b, c, d)",
                "presto": "MAP(ARRAY[a, c], ARRAY[b, d])",
                "spark": "MAP(a, b, c, d)",
            },
            write={
                "": "MAP(ARRAY(a, c), ARRAY(b, d))",
                "clickhouse": "map(a, b, c, d)",
                "duckdb": "MAP([a, c], [b, d])",
                "presto": "MAP(ARRAY[a, c], ARRAY[b, d])",
                "hive": "MAP(a, b, c, d)",
                "spark": "MAP(a, b, c, d)",
                "snowflake": "OBJECT_CONSTRUCT(a, b, c, d)",
            },
        )
        self.validate_all(
            "MAP(a, b)",
            write={
                "duckdb": "MAP([a], [b])",
                "presto": "MAP(ARRAY[a], ARRAY[b])",
                "hive": "MAP(a, b)",
                "spark": "MAP(a, b)",
                "snowflake": "OBJECT_CONSTRUCT(a, b)",
            },
        )
        self.validate_all(
            "LOG(10)",
            write={
                "duckdb": "LN(10)",
                "presto": "LN(10)",
                "hive": "LN(10)",
                "spark": "LN(10)",
            },
        )
        self.validate_all(
            'ds = "2020-01-01"',
            write={
                "duckdb": "ds = '2020-01-01'",
                "presto": "ds = '2020-01-01'",
                "hive": "ds = '2020-01-01'",
                "spark": "ds = '2020-01-01'",
            },
        )
        self.validate_all(
            "ds = \"1''2\"",
            write={
                "duckdb": "ds = '1''''2'",
                "presto": "ds = '1''''2'",
                "hive": "ds = '1\\'\\'2'",
                "spark": "ds = '1\\'\\'2'",
            },
        )
        self.validate_all(
            "x == 1",
            write={
                "duckdb": "x = 1",
                "presto": "x = 1",
                "hive": "x = 1",
                "spark": "x = 1",
            },
        )
        self.validate_all(
            "x div y",
            write={
                "duckdb": "x // y",
                "presto": "CAST(CAST(x AS DOUBLE) / y AS INTEGER)",
                "hive": "CAST(x / y AS INT)",
                "spark": "CAST(x / y AS INT)",
            },
        )
        self.validate_all(
            "COLLECT_LIST(x)",
            read={
                "presto": "ARRAY_AGG(x)",
            },
            write={
                "duckdb": "ARRAY_AGG(x) FILTER(WHERE x IS NOT NULL)",
                "presto": "ARRAY_AGG(x) FILTER(WHERE x IS NOT NULL)",
                "hive": "COLLECT_LIST(x)",
                "spark": "COLLECT_LIST(x)",
            },
        )
        self.validate_all(
            "COLLECT_SET(x)",
            read={
                "doris": "COLLECT_SET(x)",
                "presto": "SET_AGG(x)",
                "snowflake": "ARRAY_UNIQUE_AGG(x)",
            },
            write={
                "doris": "COLLECT_SET(x)",
                "hive": "COLLECT_SET(x)",
                "presto": "SET_AGG(x)",
                "snowflake": "ARRAY_UNIQUE_AGG(x)",
                "spark": "COLLECT_SET(x)",
                "trino": "ARRAY_AGG(DISTINCT x)",
            },
        )
        self.validate_all(
            "SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo",
            read={
                "presto": "SELECT * FROM x AS foo TABLESAMPLE BERNOULLI (1)",
                "snowflake": "SELECT * FROM x AS foo TABLESAMPLE (1)",
            },
            write={
                "hive": "SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo",
                "snowflake": "SELECT * FROM x AS foo TABLESAMPLE (1)",
                "spark": "SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo",
            },
        )
        self.validate_all(
            "SELECT a, SUM(c) FROM t GROUP BY a, DATE_FORMAT(b, 'yyyy'), GROUPING SETS ((a, DATE_FORMAT(b, 'yyyy')), a)",
            write={
                "hive": "SELECT a, SUM(c) FROM t GROUP BY a, DATE_FORMAT(b, 'yyyy'), GROUPING SETS ((a, DATE_FORMAT(b, 'yyyy')), a)",
            },
        )
        self.validate_all(
            "SELECT TRUNC(CAST(ds AS TIMESTAMP), 'MONTH')",
            read={
                "hive": "SELECT TRUNC(CAST(ds AS TIMESTAMP), 'MONTH')",
                "presto": "SELECT DATE_TRUNC('MONTH', CAST(ds AS TIMESTAMP))",
            },
            write={
                "presto": "SELECT DATE_TRUNC('MONTH', TRY_CAST(ds AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
            read={
                "hive": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark2": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "databricks": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
            },
            write={
                "hive": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark2": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "databricks": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "presto": "REGEXP_EXTRACT('abc', '(a)(b)(c)', 1)",
                "trino": "REGEXP_EXTRACT('abc', '(a)(b)(c)', 1)",
                "duckdb": "REGEXP_EXTRACT('abc', '(a)(b)(c)', 1)",
            },
        )

        self.validate_identity("EXISTS(col, x -> x % 2 = 0)").assert_is(exp.Exists)

        self.validate_all(
            "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
            read={
                "hive": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
                "spark2": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
                "spark": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
                "databricks": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
            },
            write={
                "spark2": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
                "spark": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
                "databricks": "SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)",
            },
        )

        self.validate_identity("SELECT 1_2")

        self.validate_all(
            "SELECT MAP(*), STRUCT(*) FROM t",
            read={
                "hive": "SELECT MAP(*), STRUCT(*) FROM t",
                "spark2": "SELECT MAP(*), STRUCT(*) FROM t",
                "spark": "SELECT MAP(*), STRUCT(*) FROM t",
                "databricks": "SELECT MAP(*), STRUCT(*) FROM t",
            },
            write={
                "spark2": "SELECT MAP(*), STRUCT(*) FROM t",
                "spark": "SELECT MAP(*), STRUCT(*) FROM t",
                "databricks": "SELECT MAP(*), STRUCT(*) FROM t",
            },
        )

    def test_escapes(self) -> None:
        self.validate_identity("'\n'", "'\\n'")
        self.validate_identity("'\\n'")
        self.validate_identity("'\\\n'", "'\\\\\\n'")
        self.validate_identity("'\\\\n'")
        self.validate_identity("''")
        self.validate_identity("'\\\\'")
        self.validate_identity("'\\\\z'")

    def test_data_type(self):
        self.validate_all(
            "CAST(a AS BIT)",
            write={
                "hive": "CAST(a AS BOOLEAN)",
            },
        )
