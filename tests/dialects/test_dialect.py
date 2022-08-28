import unittest

from sqlglot import (
    Dialect,
    Dialects,
    ErrorLevel,
    UnsupportedError,
    transpile,
    parse_one,
)


class Validator(unittest.TestCase):
    dialect = None

    def validate(self, sql, target, **kwargs):
        self.assertEqual(transpile(sql, **kwargs)[0], target)

    def validate_identity(self, sql):
        self.assertEqual(transpile(sql, read=self.dialect, write=self.dialect)[0], sql)

    def validate_all(self, sql, read=None, write=None):
        """
        Validate that:
        1. Everything in `read` transpiles to `sql`
        2. `sql` transpiles to everything in `write`

        Args:
            sql (str): Main SQL expression
            dialect (str): dialect of `sql`
            read (dict): Mapping of dialect -> SQL
            write (dict): Mapping of dialect -> SQL
        """
        expression = parse_one(sql, read=self.dialect)

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                self.assertEqual(
                    parse_one(read_sql, read_dialect).sql(
                        self.dialect, unsupported_level=ErrorLevel.IGNORE
                    ),
                    sql,
                )

        for write_dialect, write_sql in (write or {}).items():
            with self.subTest(f"{sql} -> {write_dialect}"):
                if write_sql is UnsupportedError:
                    with self.assertRaises(UnsupportedError):
                        expression.sql(
                            write_dialect, unsupported_level=ErrorLevel.RAISE
                        )
                else:
                    self.assertEqual(
                        expression.sql(
                            write_dialect, unsupported_level=ErrorLevel.IGNORE
                        ),
                        write_sql,
                    )


class TestDialect(Validator):
    maxDiff = None

    def test_enum(self):
        for dialect in Dialects:
            self.assertIsNotNone(Dialect[dialect])
            self.assertIsNotNone(Dialect.get(dialect))
            self.assertIsNotNone(Dialect.get_or_raise(dialect))
            self.assertIsNotNone(Dialect[dialect.value])

    def test_postgres(self):
        self.validate(
            "SELECT CAST(`a`.`b` AS DOUBLE) FROM foo",
            "SELECT CAST(`a`.`b` AS DOUBLE PRECISION) FROM foo",
            read="postgres",
            write="postgres",
        )
        self.validate(
            "CREATE TABLE x (a BYTEA)",
            "CREATE TABLE x (a BINARY)",
            read="postgres",
            write="hive",
        )

        self.validate(
            "CREATE TABLE x (a UUID)",
            "CREATE TABLE x (a UUID)",
            read="postgres",
            write="hive",
        )

        self.validate(
            "CREATE TABLE x (a INT SERIAL)",
            "CREATE TABLE x (a INTEGER AUTOINCREMENT)",
            read="postgres",
            write="sqlite",
        )
        self.validate(
            "CREATE TABLE x (a INTEGER AUTOINCREMENT)",
            "CREATE TABLE x (a INT SERIAL)",
            read="sqlite",
            write="postgres",
        )

        self.validate(
            "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            "CURRENT_DATE - INTERVAL '1' DAY",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "DATE_ADD(CURRENT_DATE(), INTERVAL 1 + 3 DAY)",
            "CURRENT_DATE + INTERVAL '4' DAY",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "CURRENT_DATE('UTC')",
            "CURRENT_DATE AT TIME ZONE 'UTC'",
            read="bigquery",
            write="postgres",
        )

        self.validate(
            "CURRENT_TIMESTAMP()",
            "CURRENT_TIMESTAMP",
            read="bigquery",
            write="postgres",
        )

        for read, write in [(None, "postgres")]:
            for a, b in [
                ("JSON_EXTRACT(x, 'y')", "x->'y'"),
                ("JSON_EXTRACT_SCALAR(x, 'y')", "x->>'y'"),
                ("JSONB_EXTRACT(x, 'y')", "x#>'y'"),
                ("JSONB_EXTRACT_SCALAR(x, 'y')", "x#>>'y'"),
            ]:
                self.validate(a, b, read=read, write=write, identity=False)
                self.validate(b, a, read=write, write=read, identity=False)

        self.validate(
            "x->'1'",
            "x->'1'",
            read="postgres",
            write="sqlite",
        )
        self.validate(
            "x#>'1'",
            "x->'1'",
            read="postgres",
            write="sqlite",
        )

        self.validate(
            "STRFTIME(x, '%y-%-m-%S')",
            "TO_CHAR(x, 'YY-FMMM-SS')",
            read="duckdb",
            write="postgres",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "DATE_ADD(x, y, 'day')",
                write="postgres",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT * FROM x FETCH 1 ROW",
            "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
            read="postgres",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
            read="postgres",
            write="postgres",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname NULLS FIRST",
            read="spark",
            write="postgres",
        )

    def test_spark(self):
        self.validate(
            'SELECT "a"."b" FROM "foo"',
            "SELECT `a`.`b` FROM `foo`",
            write="spark",
        )

        self.validate("CAST(a AS TEXT)", "CAST(a AS STRING)", write="spark")
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            "SELECT CAST(`a`.`b` AS SHORT) FROM foo",
            read="spark",
        )
        self.validate(
            'SELECT "a"."b" FROM foo',
            "SELECT `a`.`b` FROM `foo`",
            write="spark",
            identify=True,
        )
        self.validate(
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            "SELECT APPROX_DISTINCT(a) FROM foo",
            read="spark",
            write="presto",
        )
        self.validate(
            "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            "CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION 's3://z'",
            read="spark",
            write="spark",
        )
        self.validate(
            "CREATE TABLE test STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (FORMAT = 'PARQUET') AS SELECT 1",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE test USING ICEBERG STORED AS PARQUET AS SELECT 1",
            "CREATE TABLE test WITH (TABLE_FORMAT = 'ICEBERG', FORMAT = 'PARQUET') AS SELECT 1",
            read="spark",
            write="presto",
        )

        self.validate("ARRAY(0, 1, 2)", "ARRAY[0, 1, 2]", read="spark", write="presto")
        self.validate(
            "ARRAY(0, 1, 2)", "LIST_VALUE(0, 1, 2)", read="spark", write="duckdb"
        )
        self.validate(
            "SELECT /*+ COALESCE(3) */ * FROM x",
            "SELECT /*+ COALESCE(3) */ * FROM x",
            read="spark",
        )
        self.validate(
            "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
            "SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x",
            read="spark",
        )
        self.validate(
            "x IN ('a', 'a''b')", "x IN ('a', 'a\\'b')", read="presto", write="spark"
        )

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", "x.`abc`", read="duckdb", write="spark"
        )
        self.validate(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            "x.`y`.`abc`",
            read="duckdb",
            write="spark",
        )

        self.validate(
            "MONTH('2021-03-01')",
            "MONTH(CAST('2021-03-01' AS DATE))",
            read="spark",
            write="duckdb",
        )
        self.validate(
            "YEAR('2021-03-01')",
            "YEAR(CAST('2021-03-01' AS DATE))",
            read="spark",
            write="duckdb",
        )
        self.validate("MONTH(x)", "MONTH(x)", read="duckdb", write="spark")

        self.validate("'\u6bdb'", "'毛'", read="spark")

        self.validate(
            "SELECT LEFT(x, 2), RIGHT(x, 2)",
            "SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - 2 + 1, 2)",
            read="spark",
            write="presto",
        )
        self.validate(
            "ARRAY_SUM(ARRAY(1, 2))",
            "AGGREGATE(ARRAY(1, 2), 0, (acc, x) -> acc + x, acc -> acc)",
            write="spark",
        )
        self.validate(
            "REDUCE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            "AGGREGATE(x, 0, (acc, x) -> acc + x, acc -> acc)",
            write="spark",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100 ) SELECT sum(n) FROM t",
                read="presto",
                write="spark",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT a FROM x CROSS JOIN UNNEST(y) AS t (a)",
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            write="spark",
        )
        self.validate(
            "SELECT a, b FROM x CROSS JOIN UNNEST(y, z) AS t (a, b)",
            "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) t AS b",
            write="spark",
        )
        self.validate(
            "SELECT a FROM x CROSS JOIN UNNEST(y) WITH ORDINALITY AS t (a)",
            "SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS a",
            write="spark",
        )

        self.validate(
            "MAP(a, b)",
            "MAP_FROM_ARRAYS(a, b)",
            read="presto",
            write="spark",
        )

        self.validate(
            "MAP(ARRAY[1], ARRAY[2])",
            "MAP_FROM_ARRAYS(ARRAY(1), ARRAY(2))",
            read="presto",
            write="spark",
        )
        self.validate(
            "MAP_FROM_ARRAYS(ARRAY(1), c)",
            "MAP(ARRAY[1], c)",
            read="spark",
            write="presto",
        )

        self.validate(
            "SELECT SORT_ARRAY(x, FALSE)",
            "SELECT SORT_ARRAY(x, FALSE)",
            read="hive",
            write="spark",
        )
        self.validate(
            "SELECT SORT_ARRAY(x, TRUE)",
            "SELECT SORT_ARRAY(x, TRUE)",
            read="hive",
            write="spark",
        )
        self.validate(
            "SELECT SORT_ARRAY(x, TRUE)",
            "SELECT SORT_ARRAY(x, TRUE)",
            read="spark",
            write="hive",
        )
        self.validate(
            "SELECT ARRAY_SORT(x)",
            "SELECT SORT_ARRAY(x)",
            read="spark",
            write="hive",
        )

        self.validate(
            "ARRAY_FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            write="spark",
        )

        self.validate(
            "FILTER(the_array, x -> x > 0)",
            "FILTER(the_array, x -> x > 0)",
            read="spark",
            write="presto",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)",
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "CREATE TABLE db.example_table (col_a array<int>, col_b array<array<int>>)",
            "CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)",
            read="spark",
            write="spark",
        )

        self.validate(
            "SELECT 4 << 1",
            "SELECT SHIFTLEFT(4, 1)",
            read="hive",
            write="spark",
        )

        self.validate(
            "SELECT 4 >> 1",
            "SELECT SHIFTRIGHT(4, 1)",
            read="hive",
            write="spark",
        )

        self.validate(
            "SELECT SHIFTRIGHT(4, 1)",
            "SELECT 4 >> 1",
            read="spark",
            write="hive",
        )

        self.validate(
            "SELECT SHIFTLEFT(4, 1)",
            "SELECT 4 << 1",
            read="spark",
            write="hive",
        )
        self.validate(
            "SELECT * FROM VALUES ('x'), ('y') AS t(z)",
            "SELECT * FROM (VALUES ('x'), ('y')) AS t(z)",
            write="spark",
        )

        self.validate(
            """CREATE TABLE blah (col_a INT) COMMENT "Test comment: blah" PARTITIONED BY (date STRING) STORED AS ICEBERG TBLPROPERTIES('x' = '1')""",
            """CREATE TABLE blah (
  col_a INT
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date STRING
)
STORED AS ICEBERG
TBLPROPERTIES (
  'x' = '1'
)""",
            read="spark",
            write="spark",
            pretty=True,
        )

        self.validate(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
            "CREATE TABLE z (a INT) COMMENT 'x'",
            read="mysql",
            write="spark",
        )

        self.validate(
            "CREATE TABLE a (x BINARY)",
            "CREATE TABLE a (x BINARY)",
            read="spark",
            write="spark",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            read="spark",
            write="spark",
        )

        self.validate(
            "TO_DATE(x, 'yyyy-MM-dd')",
            "CAST(SUBSTR(CAST(x AS VARCHAR), 1, 10) AS DATE)",
            read="spark",
            write="presto",
        )
        self.validate(
            "TO_DATE(x, 'yyyy')",
            "CAST(DATE_PARSE(x, '%Y') AS DATE)",
            read="spark",
            write="presto",
        )
        self.validate(
            "TO_DATE(x, 'yyyy')",
            "CAST(STRPTIME(x, '%Y') AS DATE)",
            read="spark",
            write="duckdb",
        )
        self.validate(
            "SELECT * FROM a UNION SELECT * FROM b",
            "SELECT * FROM a UNION SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
            "SELECT * FROM a UNION SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            write="spark",
        )
        self.validate(
            "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            write="spark",
        )

    def test_snowflake(self):
        self.validate(
            'x:a:"b c"',
            "x['a']['b c']",
            read="snowflake",
        )
        self.validate(
            "CAST(x AS DOUBLE PRECISION)",
            "CAST(x AS DOUBLE)",
            read="snowflake",
        )

        self.validate(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            read="snowflake",
        )
        self.validate(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS FIRST LIMIT 10",
            read="bigquery",
            write="snowflake",
        )
        self.validate(
            "SELECT a FROM test QUALIFY z ORDER BY a LIMIT 10",
            "SELECT a FROM test QUALIFY z ORDER BY a NULLS FIRST LIMIT 10",
            read="bigquery",
            write="snowflake",
        )
        self.validate(
            "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            "SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729)",
            "SELECT TO_TIMESTAMP(1659981729)",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('1659981729')",
            "SELECT TO_TIMESTAMP('1659981729')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729000, 3)",
            "SELECT TIMESTAMP_MILLIS(1659981729000)",
            read="snowflake",
            write="spark",
        )
        self.validate(
            "SELECT TO_TIMESTAMP(1659981729000000000, 9)",
            "SELECT TIMESTAMP_MICROS(1659981729000000000)",
            read="snowflake",
            write="spark",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('2013-04-05 01:02:03')",
            "SELECT TO_TIMESTAMP('2013-04-05 01:02:03', 'yyyy-mm-dd hh24:mi:ss')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read="snowflake",
        )
        self.validate(
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'MM/dd/yyyy HH:mm:ss')",
            read="snowflake",
            write="spark",
        )
        self.validate(
            "SELECT strptime('04/05/2013 01:02:03', '%m/%d/%Y %H:%M:%S');",
            "SELECT TO_TIMESTAMP('04/05/2013 01:02:03', 'mm/dd/yyyy hh24:mi:ss')",
            read="duckdb",
            write="snowflake",
        )
        self.validate(
            "SELECT IFF(TRUE, 'true', 'false')",
            "SELECT IFF(TRUE, 'true', 'false')",
            read="snowflake",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
            read="snowflake",
            write="snowflake",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname NULLS FIRST",
            read="spark",
            write="snowflake",
        )

        self.validate(
            "SELECT ARRAYAGG(DISTINCT a)",
            "SELECT COLLECT_LIST(DISTINCT a)",
            read="snowflake",
            write="spark",
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                write="snowflake",
                unsupported_level=ErrorLevel.RAISE,
            )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                write="snowflake",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate(
            "SELECT * FROM a UNION SELECT * FROM b",
            "SELECT * FROM a UNION SELECT * FROM b",
            write="snowflake",
        )

        self.validate(
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            write="snowflake",
        )

        self.validate(
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            write="snowflake",
        )

        self.validate(
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            write="snowflake",
        )

    def test_sqlite(self):
        self.validate(
            """
            CREATE TABLE "Track"
            (
                CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
                FOREIGN KEY ("AlbumId") REFERENCES "Album" ("AlbumId")
                    ON DELETE NO ACTION ON UPDATE NO ACTION,
                FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
                FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
            )
            """,
            """CREATE TABLE "Track" (
  CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
  FOREIGN KEY ("AlbumId") REFERENCES "Album"("AlbumId") ON DELETE NO ACTION ON UPDATE NO ACTION,
  FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
)""",
            read="sqlite",
            write="sqlite",
            pretty=True,
        )
        self.validate(
            "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
            'SELECT CAST("a"."b" AS INTEGER) FROM foo',
            read="sqlite",
            write="sqlite",
        )
        self.validate(
            "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            'SELECT CAST("a"."b" AS INTEGER) FROM foo',
            read="sqlite",
            write="sqlite",
        )
        self.validate(
            "CAST(`a`.`b` AS INT)",
            'CAST("a"."b" AS INTEGER)',
            read="spark",
            write="sqlite",
        )

        self.validate(
            "LEVENSHTEIN(col1, col2)", "EDITDIST3(col1, col2)", write="sqlite"
        )

        self.validate(
            "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
            read="sqlite",
            write="mysql",
        )

        self.validate(
            "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
            "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
            read="mysql",
            write="sqlite",
        )
        self.validate(
            """CREATE TABLE "x" ("Name" NVARCHAR(200) NOT NULL)""",
            """CREATE TABLE "x" ("Name" TEXT(200) NOT NULL)""",
            read="sqlite",
            write="sqlite",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            read="sqlite",
            write="sqlite",
        )

    def test_tableau(self):
        self.validate(
            "IF(x = 'a', y, NULL)",
            "IF x = 'a' THEN y ELSE NULL END",
            write="tableau",
        )
        self.validate(
            "COALESCE(a, 0)",
            "IFNULL(a, 0)",
            write="tableau",
        )
        self.validate(
            "COUNT(DISTINCT(a))",
            "COUNTD((a))",
            write="tableau",
        )
        self.validate(
            "COUNT(a)",
            "COUNT(a)",
            write="tableau",
        )
        self.validate(
            "COUNT(DISTINCT x)",
            "COUNTD(x)",
            write="tableau",
        )

    def test_clickhouse(self):
        self.validate(
            "dictGet(x, 'y')",
            "dictGet(x, 'y')",
            write="clickhouse",
        )
        self.validate(
            "CAST(1 AS NULLABLE(INT64))",
            "CAST(1 AS NULLABLE(BIGINT))",
            read="clickhouse",
        )

        self.validate(
            "select * from x final",
            "SELECT * FROM x FINAL",
            read="clickhouse",
        )
        self.validate(
            "select * from x y final",
            "SELECT * FROM x AS y FINAL",
            read="clickhouse",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
            read="clickhouse",
            write="clickhouse",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
            read="spark",
            write="clickhouse",
        )

    def test_cast(self):
        self.validate_all(
            "CAST(a AS TEXT)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS TEXT)",
                "hive": "CAST(a AS STRING)",
                "presto": "CAST(a AS VARCHAR)",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS STRING)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS TEXT)",
                "hive": "CAST(a AS STRING)",
                "presto": "CAST(a AS VARCHAR)",
                "snowflake": "CAST(a AS TEXT)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS STRING)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR)",
            write={
                "bigquery": "CAST(a AS STRING)",
                "duckdb": "CAST(a AS TEXT)",
                "mysql": "CAST(a AS VARCHAR)",
                "hive": "CAST(a AS STRING)",
                "oracle": "CAST(a AS VARCHAR2)",
                "presto": "CAST(a AS VARCHAR)",
                "snowflake": "CAST(a AS VARCHAR)",
                "spark": "CAST(a AS STRING)",
                "starrocks": "CAST(a AS VARCHAR)",
            },
        )
        self.validate_all(
            "CAST(a AS VARCHAR(3))",
            write={
                "bigquery": "CAST(a AS STRING(3))",
                "duckdb": "CAST(a AS TEXT(3))",
                "mysql": "CAST(a AS VARCHAR(3))",
                "hive": "CAST(a AS VARCHAR(3))",
                "oracle": "CAST(a AS VARCHAR2(3))",
                "presto": "CAST(a AS VARCHAR(3))",
                "snowflake": "CAST(a AS VARCHAR(3))",
                "spark": "CAST(a AS VARCHAR(3))",
                "starrocks": "CAST(a AS VARCHAR(3))",
            },
        )
        self.validate_all(
            "CAST(a AS SMALLINT)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "duckdb": "CAST(a AS SMALLINT)",
                "mysql": "CAST(a AS SMALLINT)",
                "hive": "CAST(a AS SMALLINT)",
                "oracle": "CAST(a AS NUMBER)",
                "presto": "CAST(a AS SMALLINT)",
                "snowflake": "CAST(a AS SMALLINT)",
                "spark": "CAST(a AS SHORT)",
                "starrocks": "CAST(a AS SMALLINT)",
            },
        )
        self.validate_all(
            "CAST(a AS TIMESTAMP)", write={"starrocks": "CAST(a AS DATETIME)"}
        )
        self.validate_all(
            "CAST(a AS TIMESTAMPTZ)", write={"starrocks": "CAST(a AS DATETIME)"}
        )
        self.validate_all("CAST(a AS TINYINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS SMALLINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS BIGINT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all("CAST(a AS INT)", write={"oracle": "CAST(a AS NUMBER)"})
        self.validate_all(
            "CAST(a AS DECIMAL)",
            read={"oracle": "CAST(a AS NUMBER)"},
            write={"oracle": "CAST(a AS NUMBER)"},
        )

    def test_time(self):
        self.validate_all(
            "STR_TO_TIME(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
            },
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS TIMESTAMP)",
                "presto": "DATE_PARSE(x, '%Y-%m-%dT%H:%i:%S')",
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            write={
                "duckdb": "STRPTIME('2020-01-01', '%Y-%m-%d')",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d')",
                "spark": "TO_TIMESTAMP('2020-01-01', 'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "STR_TO_TIME(x, '%y')",
            write={
                "duckdb": "STRPTIME(x, '%y')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yy')) AS TIMESTAMP)",
                "presto": "DATE_PARSE(x, '%y')",
                "spark": "TO_TIMESTAMP(x, 'yy')",
            },
        )
        self.validate_all(
            "STR_TO_UNIX('2020-01-01', '%Y-%M-%d')",
            write={
                "duckdb": "EPOCH(STRPTIME('2020-01-01', '%Y-%M-%d'))",
                "hive": "UNIX_TIMESTAMP('2020-01-01', 'yyyy-mm-dd')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%i-%d'))",
            },
        )
        self.validate_all(
            "TIME_STR_TO_DATE('2020-01-01')",
            write={
                "duckdb": "CAST('2020-01-01' AS DATE)",
                "hive": "TO_DATE('2020-01-01')",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%s')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_TIME('2020-01-01')",
            write={
                "duckdb": "CAST('2020-01-01' AS TIMESTAMP)",
                "hive": "CAST('2020-01-01' AS TIMESTAMP)",
                "presto": "DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%s')",
            },
        )
        self.validate_all(
            "TIME_STR_TO_UNIX('2020-01-01')",
            write={
                "duckdb": "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
                "hive": "UNIX_TIMESTAMP('2020-01-01')",
                "presto": "TO_UNIXTIME(DATE_PARSE('2020-01-01', '%Y-%m-%d %H:%i:%S'))",
            },
        )
        self.validate_all(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            write={
                "duckdb": "STRFTIME(x, '%Y-%m-%d')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d')",
            },
        )
        self.validate_all(
            "TIME_TO_TIME_STR(x)",
            write={
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
            },
        )
        self.validate_all(
            "TIME_TO_UNIX(x)",
            write={
                "duckdb": "EPOCH(x)",
                "hive": "UNIX_TIMESTAMP(x)",
                "presto": "TO_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE_STR(x)",
            write={
                "duckdb": "SUBSTRING(CAST(x AS TEXT), 1, 10)",
                "hive": "SUBSTRING(CAST(x AS STRING), 1, 10)",
                "presto": "SUBSTRING(CAST(x AS VARCHAR), 1, 10)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(SUBSTR(CAST(x AS VARCHAR), 1, 10) AS DATE)",
            },
        )
        self.validate_all(
            "TS_OR_DS_TO_DATE(x, '%-d')",
            write={
                "duckdb": "CAST(STRPTIME(x, '%-d') AS DATE)",
                "hive": "TO_DATE(x, 'd')",
                "presto": "CAST(DATE_PARSE(x, '%e') AS DATE)",
                "spark": "TO_DATE(x, 'd')",
            },
        )
        self.validate_all(
            "UNIX_TO_STR(x, y)",
            write={
                "duckdb": "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), y)",
                "hive": "FROM_UNIXTIME(x, y)",
                "presto": "DATE_FORMAT(FROM_UNIXTIME(x), y)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME(x)",
            write={
                "duckdb": "TO_TIMESTAMP(CAST(x AS BIGINT))",
                "hive": "FROM_UNIXTIME(x)",
                "presto": "FROM_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "UNIX_TO_TIME_STR(x)",
            write={
                "duckdb": "CAST(TO_TIMESTAMP(CAST(x AS BIGINT)) AS TEXT)",
                "hive": "FROM_UNIXTIME(x)",
                "presto": "CAST(FROM_UNIXTIME(x) AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DATE_STR(x)",
            write={
                "duckdb": "CAST(x AS TEXT)",
                "hive": "CAST(x AS STRING)",
                "presto": "CAST(x AS VARCHAR)",
            },
        )
        self.validate_all(
            "DATE_TO_DI(x)",
            write={
                "duckdb": "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
                "hive": "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
                "presto": "CAST(DATE_FORMAT(x, '%Y%m%d') AS INT)",
            },
        )
        self.validate_all(
            "DI_TO_DATE(x)",
            write={
                "duckdb": "CAST(STRPTIME(CAST(x AS TEXT), '%Y%m%d') AS DATE)",
                "hive": "TO_DATE(CAST(x AS STRING), 'yyyyMMdd')",
                "presto": "CAST(DATE_PARSE(CAST(x AS VARCHAR), '%Y%m%d') AS DATE)",
            },
        )
        self.validate_all(
            "TS_OR_DI_TO_DI(x)",
            write={
                "duckdb": "CAST(SUBSTR(REPLACE(CAST(x AS TEXT), '-', ''), 1, 8) AS INT)",
                "hive": "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
                "presto": "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
                "spark": "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            },
        )
        self.validate_all(
            "DATE_ADD(x, 1, 'day')",
            read={
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 'day')",
                "duckdb": "x + INTERVAL 1 day",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "postgres": "x + INTERVAL '1' 'day'",
                "presto": "DATE_ADD('day', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_ADD(x, 1)",
            write={
                "bigquery": "DATE_ADD(x, INTERVAL 1 'day')",
                "duckdb": "x + INTERVAL 1 DAY",
                "hive": "DATE_ADD(x, 1)",
                "mysql": "DATE_ADD(x, INTERVAL 1 DAY)",
                "presto": "DATE_ADD('day', 1, x)",
                "spark": "DATE_ADD(x, 1)",
                "starrocks": "DATE_ADD(x, INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'day')",
            write={
                "mysql": "DATE(x)",
                "starrocks": "DATE(x)",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'week')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'month')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'quarter')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'year')",
            write={
                "mysql": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
                "starrocks": "STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')",
            },
        )
        self.validate_all(
            "DATE_TRUNC(x, 'millenium')",
            write={
                "mysql": UnsupportedError,
                "starrocks": UnsupportedError,
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%dT%H:%M:%S')",
            read={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
            },
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%dT%H:%i:%S')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yyyy-MM-ddTHH:mm:ss')) AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%dT%H:%i:%S') AS DATE)",
                "spark": "TO_DATE(x, 'yyyy-MM-ddTHH:mm:ss')",
            },
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%d')",
            write={
                "mysql": "STR_TO_DATE(x, '%Y-%m-%d')",
                "starrocks": "STR_TO_DATE(x, '%Y-%m-%d')",
                "hive": "CAST(x AS DATE)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)",
                "spark": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "DATE_STR_TO_DATE(x)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "hive": "TO_DATE(x)",
                "presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)",
                "spark": "TO_DATE(x)",
            },
        )
        self.validate_all(
            "TS_OR_DS_ADD('2021-02-01', 1, 'DAY')",
            write={
                "duckdb": "CAST('2021-02-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD('2021-02-01', 1)",
                "presto": "DATE_ADD('DAY', 1, DATE_PARSE(SUBSTR('2021-02-01', 1, 10), '%Y-%m-%d'))",
                "spark": "DATE_ADD('2021-02-01', 1)",
            },
        )
        self.validate_all(
            "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            write={
                "duckdb": "CAST('2020-01-01' AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
                "presto": "DATE_ADD('day', 1, CAST('2020-01-01' AS DATE))",
                "spark": "DATE_ADD(CAST('2020-01-01' AS DATE), 1)",
            },
        )

        for unit in ("DAY", "MONTH", "YEAR"):
            self.validate_all(
                f"{unit}(x)",
                read={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "bigquery",
                        "duckdb",
                        "mysql",
                        "presto",
                        "starrocks",
                    )
                },
                write={
                    dialect: f"{unit}(x)"
                    for dialect in (
                        "bigquery",
                        "duckdb",
                        "mysql",
                        "presto",
                        "hive",
                        "spark",
                        "starrocks",
                    )
                },
            )

    def test_array(self):
        self.validate_all(
            "ARRAY(0, 1, 2)",
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "LIST_VALUE(0, 1, 2)",
                "presto": "ARRAY[0, 1, 2]",
                "spark": "ARRAY(0, 1, 2)",
            },
        )
        self.validate_all(
            "ARRAY_SIZE(x)",
            write={
                "bigquery": "ARRAY_LENGTH(x)",
                "duckdb": "ARRAY_LENGTH(x)",
                "presto": "CARDINALITY(x)",
                "spark": "SIZE(x)",
            },
        )
        self.validate_all(
            "ARRAY_SUM(ARRAY(1, 2))",
            write={
                "trino": "REDUCE(ARRAY[1, 2], 0, (acc, x) -> acc + x, acc -> acc)",
                "duckdb": "LIST_SUM(LIST_VALUE(1, 2))",
                "hive": "ARRAY_SUM(ARRAY(1, 2))",
                "presto": "ARRAY_SUM(ARRAY[1, 2])",
                "spark": "AGGREGATE(ARRAY(1, 2), 0, (acc, x) -> acc + x, acc -> acc)",
            },
        )

    def test_order_by(self):
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "bigquery": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname NULLS FIRST",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            },
        )

    def test_operators(self):
        self.validate_all(
            "x ILIKE '%y'",
            read={
                "clickhouse": "x ILIKE '%y'",
                "duckdb": "x ILIKE '%y'",
                "postgres": "x ILIKE '%y'",
                "snowflake": "x ILIKE '%y'",
            },
            write={
                "bigquery": "LOWER(x) LIKE '%y'",
                "clickhouse": "x ILIKE '%y'",
                "duckdb": "x ILIKE '%y'",
                "hive": "LOWER(x) LIKE '%y'",
                "mysql": "LOWER(x) LIKE '%y'",
                "oracle": "LOWER(x) LIKE '%y'",
                "postgres": "x ILIKE '%y'",
                "presto": "LOWER(x) LIKE '%y'",
                "snowflake": "x ILIKE '%y'",
                "spark": "LOWER(x) LIKE '%y'",
                "sqlite": "LOWER(x) LIKE '%y'",
                "starrocks": "LOWER(x) LIKE '%y'",
                "trino": "LOWER(x) LIKE '%y'",
            },
        )
        self.validate_all(
            "SELECT * FROM a ORDER BY col_a NULLS LAST",
            write={
                "mysql": UnsupportedError,
                "starrocks": UnsupportedError,
            },
        )
        self.validate_all(
            "STR_POSITION(x, 'a')",
            write={
                "duckdb": "STRPOS(x, 'a')",
                "presto": "STRPOS(x, 'a')",
                "spark": "LOCATE('a', x)",
            },
        )
        self.validate_all(
            "CONCAT_WS('-', 'a', 'b')",
            write={
                "duckdb": "CONCAT_WS('-', 'a', 'b')",
                "presto": "ARRAY_JOIN(ARRAY['a', 'b'], '-')",
                "hive": "CONCAT_WS('-', 'a', 'b')",
                "spark": "CONCAT_WS('-', 'a', 'b')",
            },
        )

        self.validate_all(
            "CONCAT_WS('-', x)",
            write={
                "duckdb": "CONCAT_WS('-', x)",
                "presto": "ARRAY_JOIN(x, '-')",
                "hive": "CONCAT_WS('-', x)",
                "spark": "CONCAT_WS('-', x)",
            },
        )
        self.validate_all(
            "IF(x > 1, 1, 0)",
            write={
                "duckdb": "CASE WHEN x > 1 THEN 1 ELSE 0 END",
                "presto": "IF(x > 1, 1, 0)",
                "hive": "IF(x > 1, 1, 0)",
                "spark": "IF(x > 1, 1, 0)",
                "tableau": "IF x > 1 THEN 1 ELSE 0 END",
            },
        )
        self.validate_all(
            "CASE WHEN 1 THEN x ELSE 0 END",
            write={
                "duckdb": "CASE WHEN 1 THEN x ELSE 0 END",
                "presto": "CASE WHEN 1 THEN x ELSE 0 END",
                "hive": "CASE WHEN 1 THEN x ELSE 0 END",
                "spark": "CASE WHEN 1 THEN x ELSE 0 END",
                "tableau": "CASE WHEN 1 THEN x ELSE 0 END",
            },
        )
        self.validate_all(
            "x[y]",
            write={
                "duckdb": "x[y]",
                "presto": "x[y]",
                "hive": "x[y]",
                "spark": "x[y]",
            },
        )
        self.validate_all(
            """'["x"]'""",
            write={
                "duckdb": """'["x"]'""",
                "presto": """'["x"]'""",
                "hive": """'["x"]'""",
                "spark": """'["x"]'""",
            },
        )

        self.validate_all(
            'true or null as "foo"',
            write={
                "bigquery": "TRUE OR NULL AS `foo`",
                "duckdb": 'TRUE OR NULL AS "foo"',
                "presto": 'TRUE OR NULL AS "foo"',
                "hive": "TRUE OR NULL AS `foo`",
                "spark": "TRUE OR NULL AS `foo`",
            },
        )
        self.validate_all(
            "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) as foo FROM baz",
            write={
                "bigquery": "SELECT CASE WHEN COALESCE(bar, 0) = 1 THEN TRUE ELSE FALSE END AS foo FROM baz",
                "duckdb": "SELECT CASE WHEN COALESCE(bar, 0) = 1 THEN TRUE ELSE FALSE END AS foo FROM baz",
                "presto": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "hive": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
                "spark": "SELECT IF(COALESCE(bar, 0) = 1, TRUE, FALSE) AS foo FROM baz",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(col1, col2)",
            write={
                "duckdb": "LEVENSHTEIN(col1, col2)",
                "presto": "LEVENSHTEIN_DISTANCE(col1, col2)",
                "hive": "LEVENSHTEIN(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
            },
        )
        self.validate_all(
            "LEVENSHTEIN(coalesce(col1, col2), coalesce(col2, col1))",
            write={
                "duckdb": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
                "presto": "LEVENSHTEIN_DISTANCE(COALESCE(col1, col2), COALESCE(col2, col1))",
                "hive": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
                "spark": "LEVENSHTEIN(COALESCE(col1, col2), COALESCE(col2, col1))",
            },
        )
        self.validate_all(
            "ARRAY_FILTER(the_array, x -> x > 0)",
            write={
                "presto": "FILTER(the_array, x -> x > 0)",
                "hive": "FILTER(the_array, x -> x > 0)",
                "spark": "FILTER(the_array, x -> x > 0)",
            },
        )
        self.validate_all(
            "SELECT a AS b FROM x GROUP BY b",
            write={
                "duckdb": "SELECT a AS b FROM x GROUP BY b",
                "presto": "SELECT a AS b FROM x GROUP BY 1",
                "hive": "SELECT a AS b FROM x GROUP BY 1",
                "oracle": "SELECT a AS b FROM x GROUP BY 1",
                "spark": "SELECT a AS b FROM x GROUP BY 1",
            },
        )
        self.validate_all(
            "SELECT x FROM y LIMIT 10",
            read={
                "oracle": "SELECT TOP 10 x FROM y",
            },
        )
        self.validate_all(
            "SELECT * FROM a UNION SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a UNION DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION SELECT * FROM b",
                "presto": "SELECT * FROM a UNION SELECT * FROM b",
                "spark": "SELECT * FROM a UNION SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a UNION ALL SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "duckdb": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "presto": "SELECT * FROM a UNION ALL SELECT * FROM b",
                "spark": "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a INTERSECT SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "presto": "SELECT * FROM a INTERSECT SELECT * FROM b",
                "spark": "SELECT * FROM a INTERSECT SELECT * FROM b",
            },
        )
        self.validate_all(
            "SELECT * FROM a EXCEPT SELECT * FROM b",
            read={
                "bigquery": "SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b",
                "duckdb": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "presto": "SELECT * FROM a EXCEPT SELECT * FROM b",
                "spark": "SELECT * FROM a EXCEPT SELECT * FROM b",
            },
        )
