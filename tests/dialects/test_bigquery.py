from unittest import mock
import datetime
import pytz

from sqlglot import (
    ErrorLevel,
    ParseError,
    TokenError,
    UnsupportedError,
    exp,
    parse,
    transpile,
    parse_one,
)
from sqlglot.helper import logger as helper_logger
from sqlglot.parser import logger as parser_logger
from tests.dialects.test_dialect import Validator


class TestBigQuery(Validator):
    dialect = "bigquery"
    maxDiff = None

    def test_bigquery(self):
        self.validate_all(
            "EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00))",
            write={
                "bigquery": "EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00))",
                "duckdb": "EXTRACT(HOUR FROM MAKE_TIMESTAMP(2008, 12, 25, 15, 30, 00))",
                "snowflake": "DATE_PART(HOUR, TIMESTAMP_FROM_PARTS(2008, 12, 25, 15, 30, 00))",
            },
        )
        self.validate_identity(
            """CREATE TEMPORARY FUNCTION FOO()
RETURNS STRING
LANGUAGE js AS
'return "Hello world!"'""",
            pretty=True,
        )
        self.validate_identity(
            "[a, a(1, 2,3,4444444444444444, tttttaoeunthaoentuhaoentuheoantu, toheuntaoheutnahoeunteoahuntaoeh), b(3, 4,5), c, d, tttttttttttttttteeeeeeeeeeeeeett, 12312312312]",
            """[
  a,
  a(
    1,
    2,
    3,
    4444444444444444,
    tttttaoeunthaoentuhaoentuheoantu,
    toheuntaoheutnahoeunteoahuntaoeh
  ),
  b(3, 4, 5),
  c,
  d,
  tttttttttttttttteeeeeeeeeeeeeett,
  12312312312
]""",
            pretty=True,
        )

        self.validate_all(
            "SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 as a, 'abc' AS b), STRUCT(str_col AS abc)",
            write={
                "bigquery": "SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)",
                "duckdb": "SELECT {'_0': 1, '_1': 2, '_2': 3}, {}, {'_0': 'abc'}, {'_0': 1, '_1': t.str_col}, {'a': 1, 'b': 'abc'}, {'abc': str_col}",
                "hive": "SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1, 'abc'), STRUCT(str_col)",
                "spark2": "SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)",
                "spark": "SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)",
                "snowflake": "SELECT OBJECT_CONSTRUCT('_0', 1, '_1', 2, '_2', 3), OBJECT_CONSTRUCT(), OBJECT_CONSTRUCT('_0', 'abc'), OBJECT_CONSTRUCT('_0', 1, '_1', t.str_col), OBJECT_CONSTRUCT('a', 1, 'b', 'abc'), OBJECT_CONSTRUCT('abc', str_col)",
                # fallback to unnamed without type inference
                "trino": "SELECT ROW(1, 2, 3), ROW(), ROW('abc'), ROW(1, t.str_col), CAST(ROW(1, 'abc') AS ROW(a INTEGER, b VARCHAR)), ROW(str_col)",
            },
        )
        self.validate_all(
            "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%z', x)",
            write={
                "bigquery": "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%z', x)",
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S.%f%z')",
            },
        )
        self.validate_identity(
            "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', x)",
            "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', x)",
        )

        for prefix in ("c.db.", "db.", ""):
            with self.subTest(f"Parsing {prefix}INFORMATION_SCHEMA.X into a Table"):
                table = self.parse_one(f"`{prefix}INFORMATION_SCHEMA.X`", into=exp.Table)
                this = table.this

                self.assertIsInstance(this, exp.Identifier)
                self.assertTrue(this.quoted)
                self.assertEqual(this.name, "INFORMATION_SCHEMA.X")

        table = self.parse_one("x-0._y.z", into=exp.Table)
        self.assertEqual(table.catalog, "x-0")
        self.assertEqual(table.db, "_y")
        self.assertEqual(table.name, "z")

        table = self.parse_one("x-0._y", into=exp.Table)
        self.assertEqual(table.db, "x-0")
        self.assertEqual(table.name, "_y")

        self.validate_identity("SELECT * FROM x-0.y")
        self.assertEqual(exp.to_table("`a.b`.`c.d`", dialect="bigquery").sql(), '"a"."b"."c"."d"')
        self.assertEqual(exp.to_table("`x`.`y.z`", dialect="bigquery").sql(), '"x"."y"."z"')
        self.assertEqual(exp.to_table("`x.y.z`", dialect="bigquery").sql(), '"x"."y"."z"')
        self.assertEqual(exp.to_table("`x.y.z`", dialect="bigquery").sql("bigquery"), "`x.y.z`")
        self.assertEqual(exp.to_table("`x`.`y`", dialect="bigquery").sql("bigquery"), "`x`.`y`")

        column = self.validate_identity("SELECT `db.t`.`c` FROM `db.t`").selects[0]
        self.assertEqual(len(column.parts), 3)

        select_with_quoted_udf = self.validate_identity("SELECT `p.d.UdF`(data) FROM `p.d.t`")
        self.assertEqual(select_with_quoted_udf.selects[0].name, "p.d.UdF")

        self.validate_identity("SELECT ARRAY_CONCAT([1])")
        self.validate_identity("SELECT * FROM READ_CSV('bla.csv')")
        self.validate_identity("CAST(x AS STRUCT<list ARRAY<INT64>>)")
        self.validate_identity("assert.true(1 = 1)")
        self.validate_identity("SELECT jsondoc['some_key']")
        self.validate_identity("SELECT `p.d.UdF`(data).* FROM `p.d.t`")
        self.validate_identity("SELECT * FROM `my-project.my-dataset.my-table`")
        self.validate_identity("CREATE OR REPLACE TABLE `a.b.c` CLONE `a.b.d`")
        self.validate_identity("SELECT x, 1 AS y GROUP BY 1 ORDER BY 1")
        self.validate_identity("SELECT * FROM x.*")
        self.validate_identity("SELECT * FROM x.y*")
        self.validate_identity("CASE A WHEN 90 THEN 'red' WHEN 50 THEN 'blue' ELSE 'green' END")
        self.validate_identity("CREATE SCHEMA x DEFAULT COLLATE 'en'")
        self.validate_identity("CREATE TABLE x (y INT64) DEFAULT COLLATE 'en'")
        self.validate_identity("PARSE_JSON('{}', wide_number_mode => 'exact')")
        self.validate_identity("FOO(values)")
        self.validate_identity("STRUCT(values AS value)")
        self.validate_identity("ARRAY_AGG(x IGNORE NULLS LIMIT 1)")
        self.validate_identity("ARRAY_AGG(x IGNORE NULLS ORDER BY x LIMIT 1)")
        self.validate_identity("ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x LIMIT 1)")
        self.validate_identity("ARRAY_AGG(x IGNORE NULLS)")
        self.validate_identity("ARRAY_AGG(DISTINCT x IGNORE NULLS HAVING MAX x ORDER BY x LIMIT 1)")
        self.validate_identity("SELECT * FROM dataset.my_table TABLESAMPLE SYSTEM (10 PERCENT)")
        self.validate_identity("TIME('2008-12-25 15:30:00+08')")
        self.validate_identity("TIME('2008-12-25 15:30:00+08', 'America/Los_Angeles')")
        self.validate_identity("SELECT test.Unknown FROM test")
        self.validate_identity(r"SELECT '\n\r\a\v\f\t'")
        self.validate_identity("SELECT * FROM tbl FOR SYSTEM_TIME AS OF z")
        self.validate_identity("SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008', 'UTC')")
        self.validate_identity("SELECT ANY_VALUE(fruit HAVING MAX sold) FROM fruits")
        self.validate_identity("SELECT ANY_VALUE(fruit HAVING MIN sold) FROM fruits")
        self.validate_identity("SELECT `project-id`.udfs.func(call.dir)")
        self.validate_identity("SELECT CAST(CURRENT_DATE AS STRING FORMAT 'DAY') AS current_day")
        self.validate_identity("SAFE_CAST(encrypted_value AS STRING FORMAT 'BASE64')")
        self.validate_identity("CAST(encrypted_value AS STRING FORMAT 'BASE64')")
        self.validate_identity("DATE(2016, 12, 25)")
        self.validate_identity("DATE(CAST('2016-12-25 23:59:59' AS DATETIME))")
        self.validate_identity("SELECT foo IN UNNEST(bar) AS bla")
        self.validate_identity("SELECT * FROM x-0.a")
        self.validate_identity("SELECT * FROM pivot CROSS JOIN foo")
        self.validate_identity("SAFE_CAST(x AS STRING)")
        self.validate_identity("SELECT * FROM a-b-c.mydataset.mytable")
        self.validate_identity("SELECT * FROM abc-def-ghi")
        self.validate_identity("SELECT * FROM a-b-c")
        self.validate_identity("SELECT * FROM my-table")
        self.validate_identity("SELECT * FROM my-project.mydataset.mytable")
        self.validate_identity("SELECT * FROM pro-ject_id.c.d CROSS JOIN foo-bar")
        self.validate_identity("SELECT * FROM foo.bar.25", "SELECT * FROM foo.bar.`25`")
        self.validate_identity("SELECT * FROM foo.bar.25_", "SELECT * FROM foo.bar.`25_`")
        self.validate_identity("SELECT * FROM foo.bar.25x a", "SELECT * FROM foo.bar.`25x` AS a")
        self.validate_identity("SELECT * FROM foo.bar.25ab c", "SELECT * FROM foo.bar.`25ab` AS c")
        self.validate_identity("x <> ''")
        self.validate_identity("DATE_TRUNC(col, WEEK(MONDAY))")
        self.validate_identity("DATE_TRUNC(col, MONTH, 'UTC+8')")
        self.validate_identity("SELECT b'abc'")
        self.validate_identity("SELECT AS STRUCT 1 AS a, 2 AS b")
        self.validate_identity("SELECT DISTINCT AS STRUCT 1 AS a, 2 AS b")
        self.validate_identity("SELECT AS VALUE STRUCT(1 AS a, 2 AS b)")
        self.validate_identity("SELECT * FROM q UNPIVOT(values FOR quarter IN (b, c))")
        self.validate_identity("""CREATE TABLE x (a STRUCT<values ARRAY<INT64>>)""")
        self.validate_identity("""CREATE TABLE x (a STRUCT<b STRING OPTIONS (description='b')>)""")
        self.validate_identity("CAST(x AS TIMESTAMP)")
        self.validate_identity("BEGIN DECLARE y INT64", check_command_warning=True)
        self.validate_identity("BEGIN TRANSACTION")
        self.validate_identity("COMMIT TRANSACTION")
        self.validate_identity("ROLLBACK TRANSACTION")
        self.validate_identity("CAST(x AS BIGNUMERIC)")
        self.validate_identity("SELECT y + 1 FROM x GROUP BY y + 1 ORDER BY 1")
        self.validate_identity("SELECT TIMESTAMP_SECONDS(2) AS t")
        self.validate_identity("SELECT TIMESTAMP_MILLIS(2) AS t")
        self.validate_identity("UPDATE x SET y = NULL")
        self.validate_identity("LOG(n, b)")
        self.validate_identity("SELECT COUNT(x RESPECT NULLS)")
        self.validate_identity("SELECT LAST_VALUE(x IGNORE NULLS) OVER y AS x")
        self.validate_identity("SELECT ARRAY((SELECT AS STRUCT 1 AS a, 2 AS b))")
        self.validate_identity("SELECT ARRAY((SELECT AS STRUCT 1 AS a, 2 AS b) LIMIT 10)")
        self.validate_identity("CAST(x AS CHAR)", "CAST(x AS STRING)")
        self.validate_identity("CAST(x AS NCHAR)", "CAST(x AS STRING)")
        self.validate_identity("CAST(x AS NVARCHAR)", "CAST(x AS STRING)")
        self.validate_identity("CAST(x AS TIMESTAMPTZ)", "CAST(x AS TIMESTAMP)")
        self.validate_identity("CAST(x AS RECORD)", "CAST(x AS STRUCT)")
        self.validate_identity("SELECT * FROM x WHERE x.y >= (SELECT MAX(a) FROM b-c) - 20")
        self.validate_identity(
            "SELECT cars, apples FROM some_table PIVOT(SUM(total_counts) FOR products IN ('general.cars' AS cars, 'food.apples' AS apples))"
        )
        self.validate_identity(
            "MERGE INTO dataset.NewArrivals USING (SELECT * FROM UNNEST([('microwave', 10, 'warehouse #1'), ('dryer', 30, 'warehouse #1'), ('oven', 20, 'warehouse #2')])) ON FALSE WHEN NOT MATCHED THEN INSERT ROW WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )
        self.validate_identity(
            "SELECT * FROM test QUALIFY a IS DISTINCT FROM b WINDOW c AS (PARTITION BY d)"
        )
        self.validate_identity(
            "FOR record IN (SELECT word, word_count FROM bigquery-public-data.samples.shakespeare LIMIT 5) DO SELECT record.word, record.word_count"
        )
        self.validate_identity(
            "DATE(CAST('2016-12-25 05:30:00+07' AS DATETIME), 'America/Los_Angeles')"
        )
        self.validate_identity(
            """CREATE TABLE x (a STRING OPTIONS (description='x')) OPTIONS (table_expiration_days=1)"""
        )
        self.validate_identity(
            "SELECT * FROM (SELECT * FROM `t`) AS a UNPIVOT((c) FOR c_name IN (v1, v2))"
        )
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM bla EXCEPT DISTINCT (SELECT * FROM bar) LIMIT 0"
        )
        self.validate_identity(
            "SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)"
        )
        self.validate_identity(
            "SELECT item, purchases, LAST_VALUE(item) OVER (item_window ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce WINDOW item_window AS (ORDER BY purchases)"
        )
        self.validate_identity(
            "SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)",
        )
        self.validate_identity(
            "CREATE OR REPLACE VIEW test (tenant_id OPTIONS (description='Test description on table creation')) AS SELECT 1 AS tenant_id, 1 AS customer_id",
        )
        self.validate_identity(
            "ARRAY(SELECT AS STRUCT e.x AS y, e.z AS bla FROM UNNEST(bob))::ARRAY<STRUCT<y STRING, bro NUMERIC>>",
            "CAST(ARRAY(SELECT AS STRUCT e.x AS y, e.z AS bla FROM UNNEST(bob)) AS ARRAY<STRUCT<y STRING, bro NUMERIC>>)",
        )
        self.validate_identity(
            "SELECT * FROM `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW`",
            "SELECT * FROM `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW` AS `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW`",
        )
        self.validate_identity(
            "SELECT * FROM region_or_dataset.INFORMATION_SCHEMA.TABLES",
            "SELECT * FROM region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS TABLES",
        )
        self.validate_identity(
            "SELECT * FROM region_or_dataset.INFORMATION_SCHEMA.TABLES AS some_name",
            "SELECT * FROM region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS some_name",
        )
        self.validate_identity(
            "SELECT * FROM proj.region_or_dataset.INFORMATION_SCHEMA.TABLES",
            "SELECT * FROM proj.region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS TABLES",
        )
        self.validate_identity(
            "CREATE VIEW `d.v` OPTIONS (expiration_timestamp=TIMESTAMP '2020-01-02T04:05:06.007Z') AS SELECT 1 AS c",
            "CREATE VIEW `d.v` OPTIONS (expiration_timestamp=CAST('2020-01-02T04:05:06.007Z' AS TIMESTAMP)) AS SELECT 1 AS c",
        )
        self.validate_identity(
            "SELECT ARRAY(SELECT AS STRUCT 1 a, 2 b)",
            "SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b)",
        )
        self.validate_identity(
            "select array_contains([1, 2, 3], 1)",
            "SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS _col WHERE _col = 1)",
        )
        self.validate_identity(
            "SELECT SPLIT(foo)",
            "SELECT SPLIT(foo, ',')",
        )
        self.validate_identity(
            "SELECT 1 AS hash",
            "SELECT 1 AS `hash`",
        )
        self.validate_identity(
            "SELECT 1 AS at",
            "SELECT 1 AS `at`",
        )
        self.validate_identity(
            'x <> ""',
            "x <> ''",
        )
        self.validate_identity(
            'x <> """"""',
            "x <> ''",
        )
        self.validate_identity(
            "x <> ''''''",
            "x <> ''",
        )
        self.validate_identity(
            "SELECT a overlaps",
            "SELECT a AS overlaps",
        )
        self.validate_identity(
            "SELECT y + 1 z FROM x GROUP BY y + 1 ORDER BY z",
            "SELECT y + 1 AS z FROM x GROUP BY z ORDER BY z",
        )
        self.validate_identity(
            "SELECT y + 1 z FROM x GROUP BY y + 1",
            "SELECT y + 1 AS z FROM x GROUP BY y + 1",
        )
        self.validate_identity(
            """SELECT JSON '"foo"' AS json_data""",
            """SELECT PARSE_JSON('"foo"') AS json_data""",
        )
        self.validate_identity(
            "SELECT * FROM UNNEST(x) WITH OFFSET EXCEPT DISTINCT SELECT * FROM UNNEST(y) WITH OFFSET",
            "SELECT * FROM UNNEST(x) WITH OFFSET AS offset EXCEPT DISTINCT SELECT * FROM UNNEST(y) WITH OFFSET AS offset",
        )
        self.validate_identity(
            "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) d, COUNT(*) e FOR c IN ('x', 'y'))",
            "SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) AS d, COUNT(*) AS e FOR c IN ('x', 'y'))",
        )
        self.validate_identity(
            "SELECT CAST(1 AS BYTEINT)",
            "SELECT CAST(1 AS INT64)",
        )

        self.validate_all(
            "SELECT DATE_SUB(DATE '2008-12-25', INTERVAL 5 DAY)",
            write={
                "bigquery": "SELECT DATE_SUB(CAST('2008-12-25' AS DATE), INTERVAL '5' DAY)",
                "duckdb": "SELECT CAST('2008-12-25' AS DATE) - INTERVAL '5' DAY",
                "snowflake": "SELECT DATEADD(DAY, '5' * -1, CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "EDIT_DISTANCE(col1, col2, max_distance => 3)",
            write={
                "bigquery": "EDIT_DISTANCE(col1, col2, max_distance => 3)",
                "clickhouse": UnsupportedError,
                "databricks": UnsupportedError,
                "drill": UnsupportedError,
                "duckdb": UnsupportedError,
                "hive": UnsupportedError,
                "postgres": "LEVENSHTEIN_LESS_EQUAL(col1, col2, 3)",
                "presto": UnsupportedError,
                "snowflake": "EDITDISTANCE(col1, col2, 3)",
                "spark": UnsupportedError,
                "spark2": UnsupportedError,
                "sqlite": UnsupportedError,
            },
        )
        self.validate_all(
            "EDIT_DISTANCE(a, b)",
            write={
                "bigquery": "EDIT_DISTANCE(a, b)",
                "duckdb": "LEVENSHTEIN(a, b)",
            },
        )
        self.validate_all(
            "SAFE_CAST(some_date AS DATE FORMAT 'DD MONTH YYYY')",
            write={
                "bigquery": "SAFE_CAST(some_date AS DATE FORMAT 'DD MONTH YYYY')",
                "duckdb": "CAST(TRY_STRPTIME(some_date, '%d %B %Y') AS DATE)",
            },
        )
        self.validate_all(
            "SAFE_CAST(some_date AS DATE FORMAT 'YYYY-MM-DD') AS some_date",
            write={
                "bigquery": "SAFE_CAST(some_date AS DATE FORMAT 'YYYY-MM-DD') AS some_date",
                "duckdb": "CAST(TRY_STRPTIME(some_date, '%Y-%m-%d') AS DATE) AS some_date",
            },
        )
        self.validate_all(
            "SELECT t.c1, h.c2, s.c3 FROM t1 AS t, UNNEST(t.t2) AS h, UNNEST(h.t3) AS s",
            write={
                "bigquery": "SELECT t.c1, h.c2, s.c3 FROM t1 AS t, UNNEST(t.t2) AS h, UNNEST(h.t3) AS s",
                "duckdb": "SELECT t.c1, h.c2, s.c3 FROM t1 AS t, UNNEST(t.t2) AS _t0(h), UNNEST(h.t3) AS _t1(s)",
            },
        )
        self.validate_all(
            "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%z', x)",
            write={
                "bigquery": "PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%z', x)",
                "duckdb": "STRPTIME(x, '%Y-%m-%dT%H:%M:%S.%f%z')",
            },
        )
        self.validate_all(
            "SELECT results FROM Coordinates, Coordinates.position AS results",
            write={
                "bigquery": "SELECT results FROM Coordinates, UNNEST(Coordinates.position) AS results",
                "presto": "SELECT results FROM Coordinates, UNNEST(Coordinates.position) AS _t0(results)",
            },
        )
        self.validate_all(
            "SELECT results FROM Coordinates, `Coordinates.position` AS results",
            write={
                "bigquery": "SELECT results FROM Coordinates, `Coordinates.position` AS results",
                "presto": 'SELECT results FROM Coordinates, "Coordinates"."position" AS results',
            },
        )
        self.validate_all(
            "SELECT results FROM Coordinates AS c, UNNEST(c.position) AS results",
            read={
                "presto": "SELECT results FROM Coordinates AS c, UNNEST(c.position) AS _t(results)",
                "redshift": "SELECT results FROM Coordinates AS c, c.position AS results",
            },
            write={
                "bigquery": "SELECT results FROM Coordinates AS c, UNNEST(c.position) AS results",
                "presto": "SELECT results FROM Coordinates AS c, UNNEST(c.position) AS _t0(results)",
                "redshift": "SELECT results FROM Coordinates AS c, c.position AS results",
            },
        )
        self.validate_all(
            "TIMESTAMP(x)",
            write={
                "bigquery": "TIMESTAMP(x)",
                "duckdb": "CAST(x AS TIMESTAMPTZ)",
                "snowflake": "CAST(x AS TIMESTAMPTZ)",
                "presto": "CAST(x AS TIMESTAMP WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP('2008-12-25 15:30:00', 'America/Los_Angeles')",
            write={
                "bigquery": "SELECT TIMESTAMP('2008-12-25 15:30:00', 'America/Los_Angeles')",
                "duckdb": "SELECT CAST('2008-12-25 15:30:00' AS TIMESTAMP) AT TIME ZONE 'America/Los_Angeles'",
                "snowflake": "SELECT CONVERT_TIMEZONE('America/Los_Angeles', CAST('2008-12-25 15:30:00' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT SUM(x IGNORE NULLS) AS x",
            read={
                "bigquery": "SELECT SUM(x IGNORE NULLS) AS x",
                "duckdb": "SELECT SUM(x IGNORE NULLS) AS x",
                "postgres": "SELECT SUM(x) IGNORE NULLS AS x",
                "spark": "SELECT SUM(x) IGNORE NULLS AS x",
                "snowflake": "SELECT SUM(x) IGNORE NULLS AS x",
            },
            write={
                "bigquery": "SELECT SUM(x IGNORE NULLS) AS x",
                "duckdb": "SELECT SUM(x) AS x",
                "postgres": "SELECT SUM(x) IGNORE NULLS AS x",
                "spark": "SELECT SUM(x) IGNORE NULLS AS x",
                "snowflake": "SELECT SUM(x) IGNORE NULLS AS x",
            },
        )
        self.validate_all(
            "SELECT SUM(x RESPECT NULLS) AS x",
            read={
                "bigquery": "SELECT SUM(x RESPECT NULLS) AS x",
                "duckdb": "SELECT SUM(x RESPECT NULLS) AS x",
                "postgres": "SELECT SUM(x) RESPECT NULLS AS x",
                "spark": "SELECT SUM(x) RESPECT NULLS AS x",
                "snowflake": "SELECT SUM(x) RESPECT NULLS AS x",
            },
            write={
                "bigquery": "SELECT SUM(x RESPECT NULLS) AS x",
                "duckdb": "SELECT SUM(x RESPECT NULLS) AS x",
                "postgres": "SELECT SUM(x) RESPECT NULLS AS x",
                "spark": "SELECT SUM(x) RESPECT NULLS AS x",
                "snowflake": "SELECT SUM(x) RESPECT NULLS AS x",
            },
        )
        self.validate_all(
            "SELECT PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER ()",
            write={
                "bigquery": "SELECT PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER ()",
                "duckdb": "SELECT QUANTILE_CONT(x, 0.5 RESPECT NULLS) OVER ()",
                "spark": "SELECT PERCENTILE_CONT(x, 0.5) RESPECT NULLS OVER ()",
            },
        )
        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 10) AS x",
            write={
                "bigquery": "SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 10) AS x",
                "duckdb": "SELECT ARRAY_AGG(DISTINCT x ORDER BY a NULLS FIRST, b DESC LIMIT 10) AS x",
                "spark": "SELECT COLLECT_LIST(DISTINCT x ORDER BY a, b DESC LIMIT 10) IGNORE NULLS AS x",
            },
        )
        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 1, 10) AS x",
            write={
                "bigquery": "SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 1, 10) AS x",
                "duckdb": "SELECT ARRAY_AGG(DISTINCT x ORDER BY a NULLS FIRST, b DESC LIMIT 1, 10) AS x",
                "spark": "SELECT COLLECT_LIST(DISTINCT x ORDER BY a, b DESC LIMIT 1, 10) IGNORE NULLS AS x",
            },
        )
        self.validate_all(
            "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 'semester_1', (Q3, Q4) AS 'semester_2'))",
            read={
                "spark": "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS semester_1, (Q3, Q4) AS semester_2))",
            },
            write={
                "bigquery": "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 'semester_1', (Q3, Q4) AS 'semester_2'))",
                "spark": "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS semester_1, (Q3, Q4) AS semester_2))",
            },
        )
        self.validate_all(
            "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 1, (Q3, Q4) AS 2))",
            write={
                "bigquery": "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 1, (Q3, Q4) AS 2))",
                "spark": "SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS `1`, (Q3, Q4) AS `2`))",
            },
        )
        self.validate_all(
            "SELECT UNIX_DATE(DATE '2008-12-25')",
            write={
                "bigquery": "SELECT UNIX_DATE(CAST('2008-12-25' AS DATE))",
                "duckdb": "SELECT DATE_DIFF('DAY', CAST('1970-01-01' AS DATE), CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), MONTH)",
            read={
                "snowflake": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), MONS)",
            },
            write={
                "bigquery": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), MONTH)",
                "duckdb": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE))",
                "clickhouse": "SELECT LAST_DAY(CAST('2008-11-25' AS Nullable(DATE)))",
                "mysql": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE))",
                "oracle": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE))",
                "postgres": "SELECT CAST(DATE_TRUNC('MONTH', CAST('2008-11-25' AS DATE)) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)",
                "presto": "SELECT LAST_DAY_OF_MONTH(CAST('2008-11-25' AS DATE))",
                "redshift": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE))",
                "snowflake": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), MONTH)",
                "spark": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE))",
                "tsql": "SELECT EOMONTH(CAST('2008-11-25' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), QUARTER)",
            read={
                "snowflake": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), QUARTER)",
            },
            write={
                "duckdb": UnsupportedError,
                "bigquery": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), QUARTER)",
                "snowflake": "SELECT LAST_DAY(CAST('2008-11-25' AS DATE), QUARTER)",
            },
        )
        self.validate_all(
            "CAST(x AS DATETIME)",
            read={
                "": "x::timestamp",
            },
        )
        self.validate_all(
            "SELECT TIME(15, 30, 00)",
            read={
                "duckdb": "SELECT MAKE_TIME(15, 30, 00)",
                "mysql": "SELECT MAKETIME(15, 30, 00)",
                "postgres": "SELECT MAKE_TIME(15, 30, 00)",
                "snowflake": "SELECT TIME_FROM_PARTS(15, 30, 00)",
            },
            write={
                "bigquery": "SELECT TIME(15, 30, 00)",
                "duckdb": "SELECT MAKE_TIME(15, 30, 00)",
                "mysql": "SELECT MAKETIME(15, 30, 00)",
                "postgres": "SELECT MAKE_TIME(15, 30, 00)",
                "snowflake": "SELECT TIME_FROM_PARTS(15, 30, 00)",
                "tsql": "SELECT TIMEFROMPARTS(15, 30, 00, 0, 0)",
            },
        )
        self.validate_all(
            "SELECT TIME('2008-12-25 15:30:00')",
            write={
                "bigquery": "SELECT TIME('2008-12-25 15:30:00')",
                "duckdb": "SELECT CAST('2008-12-25 15:30:00' AS TIME)",
                "mysql": "SELECT CAST('2008-12-25 15:30:00' AS TIME)",
                "postgres": "SELECT CAST('2008-12-25 15:30:00' AS TIME)",
                "redshift": "SELECT CAST('2008-12-25 15:30:00' AS TIME)",
                "spark": "SELECT CAST('2008-12-25 15:30:00' AS TIMESTAMP)",
                "tsql": "SELECT CAST('2008-12-25 15:30:00' AS TIME)",
            },
        )
        self.validate_all(
            "SELECT COUNTIF(x)",
            read={
                "clickhouse": "SELECT countIf(x)",
                "duckdb": "SELECT COUNT_IF(x)",
            },
            write={
                "bigquery": "SELECT COUNTIF(x)",
                "clickhouse": "SELECT countIf(x)",
                "duckdb": "SELECT COUNT_IF(x)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP_DIFF(TIMESTAMP_SECONDS(60), TIMESTAMP_SECONDS(0), minute)",
            write={
                "bigquery": "SELECT TIMESTAMP_DIFF(TIMESTAMP_SECONDS(60), TIMESTAMP_SECONDS(0), MINUTE)",
                "databricks": "SELECT TIMESTAMPDIFF(MINUTE, CAST(FROM_UNIXTIME(0) AS TIMESTAMP), CAST(FROM_UNIXTIME(60) AS TIMESTAMP))",
                "duckdb": "SELECT DATE_DIFF('MINUTE', TO_TIMESTAMP(0), TO_TIMESTAMP(60))",
                "snowflake": "SELECT TIMESTAMPDIFF(MINUTE, TO_TIMESTAMP(0), TO_TIMESTAMP(60))",
            },
        )
        self.validate_all(
            "TIMESTAMP_DIFF(a, b, MONTH)",
            read={
                "bigquery": "TIMESTAMP_DIFF(a, b, month)",
                "databricks": "TIMESTAMPDIFF(month, b, a)",
                "mysql": "TIMESTAMPDIFF(month, b, a)",
            },
            write={
                "databricks": "TIMESTAMPDIFF(MONTH, b, a)",
                "mysql": "TIMESTAMPDIFF(MONTH, b, a)",
                "snowflake": "TIMESTAMPDIFF(MONTH, b, a)",
            },
        )

        self.validate_all(
            "SELECT TIMESTAMP_MICROS(x)",
            read={
                "duckdb": "SELECT MAKE_TIMESTAMP(x)",
                "spark": "SELECT TIMESTAMP_MICROS(x)",
            },
            write={
                "bigquery": "SELECT TIMESTAMP_MICROS(x)",
                "duckdb": "SELECT MAKE_TIMESTAMP(x)",
                "snowflake": "SELECT TO_TIMESTAMP(x, 6)",
                "spark": "SELECT TIMESTAMP_MICROS(x)",
            },
        )
        self.validate_all(
            "SELECT * FROM t WHERE EXISTS(SELECT * FROM unnest(nums) AS x WHERE x > 1)",
            write={
                "bigquery": "SELECT * FROM t WHERE EXISTS(SELECT * FROM UNNEST(nums) AS x WHERE x > 1)",
                "duckdb": "SELECT * FROM t WHERE EXISTS(SELECT * FROM UNNEST(nums) AS _t0(x) WHERE x > 1)",
            },
        )
        self.validate_all(
            "NULL",
            read={
                "duckdb": "NULL = a",
                "postgres": "a = NULL",
            },
        )
        self.validate_all(
            "SELECT '\\n'",
            read={
                "bigquery": "SELECT '''\n'''",
            },
            write={
                "bigquery": "SELECT '\\n'",
                "postgres": "SELECT '\n'",
            },
        )
        self.validate_all(
            "TRIM(item, '*')",
            read={
                "snowflake": "TRIM(item, '*')",
                "spark": "TRIM('*', item)",
            },
            write={
                "bigquery": "TRIM(item, '*')",
                "snowflake": "TRIM(item, '*')",
                "spark": "TRIM('*' FROM item)",
            },
        )
        self.validate_all(
            "CREATE OR REPLACE TABLE `a.b.c` COPY `a.b.d`",
            write={
                "bigquery": "CREATE OR REPLACE TABLE `a.b.c` COPY `a.b.d`",
                "snowflake": 'CREATE OR REPLACE TABLE "a"."b"."c" CLONE "a"."b"."d"',
            },
        )
        (
            self.validate_all(
                "SELECT DATETIME_DIFF('2023-01-01T00:00:00', '2023-01-01T05:00:00', MILLISECOND)",
                write={
                    "bigquery": "SELECT DATETIME_DIFF('2023-01-01T00:00:00', '2023-01-01T05:00:00', MILLISECOND)",
                    "databricks": "SELECT TIMESTAMPDIFF(MILLISECOND, '2023-01-01T05:00:00', '2023-01-01T00:00:00')",
                    "snowflake": "SELECT TIMESTAMPDIFF(MILLISECOND, '2023-01-01T05:00:00', '2023-01-01T00:00:00')",
                },
            ),
        )
        (
            self.validate_all(
                "SELECT DATETIME_ADD('2023-01-01T00:00:00', INTERVAL 1 MILLISECOND)",
                write={
                    "bigquery": "SELECT DATETIME_ADD('2023-01-01T00:00:00', INTERVAL '1' MILLISECOND)",
                    "databricks": "SELECT TIMESTAMPADD(MILLISECOND, '1', '2023-01-01T00:00:00')",
                    "duckdb": "SELECT CAST('2023-01-01T00:00:00' AS TIMESTAMP) + INTERVAL '1' MILLISECOND",
                    "snowflake": "SELECT TIMESTAMPADD(MILLISECOND, '1', '2023-01-01T00:00:00')",
                },
            ),
        )
        (
            self.validate_all(
                "SELECT DATETIME_SUB('2023-01-01T00:00:00', INTERVAL 1 MILLISECOND)",
                write={
                    "bigquery": "SELECT DATETIME_SUB('2023-01-01T00:00:00', INTERVAL '1' MILLISECOND)",
                    "databricks": "SELECT TIMESTAMPADD(MILLISECOND, '1' * -1, '2023-01-01T00:00:00')",
                    "duckdb": "SELECT CAST('2023-01-01T00:00:00' AS TIMESTAMP) - INTERVAL '1' MILLISECOND",
                },
            ),
        )
        (
            self.validate_all(
                "SELECT DATETIME_TRUNC('2023-01-01T01:01:01', HOUR)",
                write={
                    "bigquery": "SELECT DATETIME_TRUNC('2023-01-01T01:01:01', HOUR)",
                    "databricks": "SELECT DATE_TRUNC('HOUR', '2023-01-01T01:01:01')",
                    "duckdb": "SELECT DATE_TRUNC('HOUR', CAST('2023-01-01T01:01:01' AS TIMESTAMP))",
                },
            ),
        )
        self.validate_all("LEAST(x, y)", read={"sqlite": "MIN(x, y)"})
        self.validate_all(
            'SELECT TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)',
            write={
                "bigquery": "SELECT TIMESTAMP_ADD(CAST('2008-12-25 15:30:00+00' AS TIMESTAMP), INTERVAL '10' MINUTE)",
                "databricks": "SELECT DATE_ADD(MINUTE, '10', CAST('2008-12-25 15:30:00+00' AS TIMESTAMP))",
                "mysql": "SELECT DATE_ADD(TIMESTAMP('2008-12-25 15:30:00+00'), INTERVAL '10' MINUTE)",
                "spark": "SELECT DATE_ADD(MINUTE, '10', CAST('2008-12-25 15:30:00+00' AS TIMESTAMP))",
                "snowflake": "SELECT TIMESTAMPADD(MINUTE, '10', CAST('2008-12-25 15:30:00+00' AS TIMESTAMPTZ))",
            },
        )
        self.validate_all(
            'SELECT TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)',
            write={
                "bigquery": "SELECT TIMESTAMP_SUB(CAST('2008-12-25 15:30:00+00' AS TIMESTAMP), INTERVAL '10' MINUTE)",
                "mysql": "SELECT DATE_SUB(TIMESTAMP('2008-12-25 15:30:00+00'), INTERVAL '10' MINUTE)",
                "snowflake": "SELECT TIMESTAMPADD(MINUTE, '10' * -1, CAST('2008-12-25 15:30:00+00' AS TIMESTAMPTZ))",
            },
        )
        self.validate_all(
            'SELECT TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL col MINUTE)',
            write={
                "bigquery": "SELECT TIMESTAMP_SUB(CAST('2008-12-25 15:30:00+00' AS TIMESTAMP), INTERVAL col MINUTE)",
                "snowflake": "SELECT TIMESTAMPADD(MINUTE, col * -1, CAST('2008-12-25 15:30:00+00' AS TIMESTAMPTZ))",
            },
        )
        self.validate_all(
            "SELECT TIME_ADD(CAST('09:05:03' AS TIME), INTERVAL 2 HOUR)",
            write={
                "bigquery": "SELECT TIME_ADD(CAST('09:05:03' AS TIME), INTERVAL '2' HOUR)",
                "duckdb": "SELECT CAST('09:05:03' AS TIME) + INTERVAL '2' HOUR",
            },
        )
        self.validate_all(
            "LOWER(TO_HEX(x))",
            write={
                "": "LOWER(HEX(x))",
                "bigquery": "TO_HEX(x)",
                "clickhouse": "LOWER(HEX(x))",
                "duckdb": "LOWER(HEX(x))",
                "hive": "LOWER(HEX(x))",
                "mysql": "LOWER(HEX(x))",
                "spark": "LOWER(HEX(x))",
                "sqlite": "LOWER(HEX(x))",
                "presto": "LOWER(TO_HEX(x))",
                "trino": "LOWER(TO_HEX(x))",
            },
        )
        self.validate_all(
            "TO_HEX(x)",
            read={
                "": "LOWER(HEX(x))",
                "clickhouse": "LOWER(HEX(x))",
                "duckdb": "LOWER(HEX(x))",
                "hive": "LOWER(HEX(x))",
                "mysql": "LOWER(HEX(x))",
                "spark": "LOWER(HEX(x))",
                "sqlite": "LOWER(HEX(x))",
                "presto": "LOWER(TO_HEX(x))",
                "trino": "LOWER(TO_HEX(x))",
            },
            write={
                "": "LOWER(HEX(x))",
                "bigquery": "TO_HEX(x)",
                "clickhouse": "LOWER(HEX(x))",
                "duckdb": "LOWER(HEX(x))",
                "hive": "LOWER(HEX(x))",
                "mysql": "LOWER(HEX(x))",
                "presto": "LOWER(TO_HEX(x))",
                "spark": "LOWER(HEX(x))",
                "sqlite": "LOWER(HEX(x))",
                "trino": "LOWER(TO_HEX(x))",
            },
        )
        self.validate_all(
            "UPPER(TO_HEX(x))",
            read={
                "": "HEX(x)",
                "clickhouse": "HEX(x)",
                "duckdb": "HEX(x)",
                "hive": "HEX(x)",
                "mysql": "HEX(x)",
                "presto": "TO_HEX(x)",
                "spark": "HEX(x)",
                "sqlite": "HEX(x)",
                "trino": "TO_HEX(x)",
            },
            write={
                "": "HEX(x)",
                "bigquery": "UPPER(TO_HEX(x))",
                "clickhouse": "HEX(x)",
                "duckdb": "HEX(x)",
                "hive": "HEX(x)",
                "mysql": "HEX(x)",
                "presto": "TO_HEX(x)",
                "spark": "HEX(x)",
                "sqlite": "HEX(x)",
                "trino": "TO_HEX(x)",
            },
        )
        self.validate_all(
            "MD5(x)",
            read={
                "clickhouse": "MD5(x)",
                "presto": "MD5(x)",
                "trino": "MD5(x)",
            },
            write={
                "": "MD5_DIGEST(x)",
                "bigquery": "MD5(x)",
                "clickhouse": "MD5(x)",
                "hive": "UNHEX(MD5(x))",
                "presto": "MD5(x)",
                "spark": "UNHEX(MD5(x))",
                "trino": "MD5(x)",
            },
        )
        self.validate_all(
            "SELECT TO_HEX(MD5(some_string))",
            read={
                "duckdb": "SELECT MD5(some_string)",
                "spark": "SELECT MD5(some_string)",
                "clickhouse": "SELECT LOWER(HEX(MD5(some_string)))",
                "presto": "SELECT LOWER(TO_HEX(MD5(some_string)))",
                "trino": "SELECT LOWER(TO_HEX(MD5(some_string)))",
            },
            write={
                "": "SELECT MD5(some_string)",
                "bigquery": "SELECT TO_HEX(MD5(some_string))",
                "duckdb": "SELECT MD5(some_string)",
                "clickhouse": "SELECT LOWER(HEX(MD5(some_string)))",
                "presto": "SELECT LOWER(TO_HEX(MD5(some_string)))",
                "trino": "SELECT LOWER(TO_HEX(MD5(some_string)))",
            },
        )
        self.validate_all(
            "SHA1(x)",
            read={
                "clickhouse": "SHA1(x)",
                "presto": "SHA1(x)",
                "trino": "SHA1(x)",
            },
            write={
                "clickhouse": "SHA1(x)",
                "bigquery": "SHA1(x)",
                "": "SHA(x)",
                "presto": "SHA1(x)",
                "trino": "SHA1(x)",
            },
        )
        self.validate_all(
            "SHA1(x)",
            write={
                "bigquery": "SHA1(x)",
                "": "SHA(x)",
            },
        )
        self.validate_all(
            "SHA256(x)",
            read={
                "clickhouse": "SHA256(x)",
                "presto": "SHA256(x)",
                "trino": "SHA256(x)",
                "postgres": "SHA256(x)",
                "duckdb": "SHA256(x)",
            },
            write={
                "bigquery": "SHA256(x)",
                "spark2": "SHA2(x, 256)",
                "clickhouse": "SHA256(x)",
                "postgres": "SHA256(x)",
                "presto": "SHA256(x)",
                "redshift": "SHA2(x, 256)",
                "trino": "SHA256(x)",
                "duckdb": "SHA256(x)",
                "snowflake": "SHA2(x, 256)",
            },
        )
        self.validate_all(
            "SHA512(x)",
            read={
                "clickhouse": "SHA512(x)",
                "presto": "SHA512(x)",
                "trino": "SHA512(x)",
            },
            write={
                "clickhouse": "SHA512(x)",
                "bigquery": "SHA512(x)",
                "spark2": "SHA2(x, 512)",
                "presto": "SHA512(x)",
                "trino": "SHA512(x)",
            },
        )
        self.validate_all(
            "SELECT CAST('20201225' AS TIMESTAMP FORMAT 'YYYYMMDD' AT TIME ZONE 'America/New_York')",
            write={"bigquery": "SELECT PARSE_TIMESTAMP('%Y%m%d', '20201225', 'America/New_York')"},
        )
        self.validate_all(
            "SELECT CAST('20201225' AS TIMESTAMP FORMAT 'YYYYMMDD')",
            write={"bigquery": "SELECT PARSE_TIMESTAMP('%Y%m%d', '20201225')"},
        )
        self.validate_all(
            "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string",
            write={
                "bigquery": "SELECT CAST(CAST('2008-12-25 00:00:00+00:00' AS TIMESTAMP) AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string",
            },
        )
        self.validate_all(
            "SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string",
            write={
                "bigquery": "SELECT CAST(CAST('2008-12-25 00:00:00+00:00' AS TIMESTAMP) AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string",
            },
        )
        self.validate_all(
            "WITH cte AS (SELECT [1, 2, 3] AS arr) SELECT IF(pos = pos_2, col, NULL) AS col FROM cte CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(arr)) - 1)) AS pos CROSS JOIN UNNEST(arr) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(arr) - 1) AND pos_2 = (ARRAY_LENGTH(arr) - 1))",
            read={
                "spark": "WITH cte AS (SELECT ARRAY(1, 2, 3) AS arr) SELECT EXPLODE(arr) FROM cte"
            },
        )
        self.validate_all(
            "SELECT IF(pos = pos_2, col, NULL) AS col FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], []))) - 1)) AS pos CROSS JOIN UNNEST(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1) AND pos_2 = (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1))",
            read={"spark": "select explode_outer([])"},
        )
        self.validate_all(
            "SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_2, pos_2, NULL) AS pos_2 FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], []))) - 1)) AS pos CROSS JOIN UNNEST(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1) AND pos_2 = (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1))",
            read={"spark": "select posexplode_outer([])"},
        )
        self.validate_all(
            "SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z",
            write={
                "": "SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z",
                "bigquery": "SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z",
                "duckdb": "SELECT {'y': ARRAY(SELECT {'b': 1} FROM x)} FROM z",
            },
        )
        self.validate_all(
            "SELECT CAST(STRUCT(1) AS STRUCT<INT64>)",
            write={
                "bigquery": "SELECT CAST(STRUCT(1) AS STRUCT<INT64>)",
                "snowflake": "SELECT CAST(OBJECT_CONSTRUCT('_0', 1) AS OBJECT)",
            },
        )
        self.validate_all(
            "cast(x as date format 'MM/DD/YYYY')",
            write={
                "bigquery": "PARSE_DATE('%m/%d/%Y', x)",
            },
        )
        self.validate_all(
            "cast(x as time format 'YYYY.MM.DD HH:MI:SSTZH')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%Y.%m.%d %I:%M:%S%z', x)",
            },
        )
        self.validate_identity(
            "CREATE TEMP TABLE foo AS SELECT 1",
            "CREATE TEMPORARY TABLE foo AS SELECT 1",
        )
        self.validate_all(
            "REGEXP_CONTAINS('foo', '.*')",
            read={
                "bigquery": "REGEXP_CONTAINS('foo', '.*')",
                "mysql": "REGEXP_LIKE('foo', '.*')",
                "starrocks": "REGEXP('foo', '.*')",
            },
            write={
                "mysql": "REGEXP_LIKE('foo', '.*')",
                "starrocks": "REGEXP('foo', '.*')",
            },
        )
        self.validate_all(
            '"""x"""',
            write={
                "bigquery": "'x'",
                "duckdb": "'x'",
                "presto": "'x'",
                "hive": "'x'",
                "spark": "'x'",
            },
        )
        self.validate_all(
            '"""x\'"""',
            write={
                "bigquery": "'x\\''",
                "duckdb": "'x'''",
                "presto": "'x'''",
                "hive": "'x\\''",
                "spark": "'x\\''",
            },
        )
        self.validate_all(
            "r'x\\''",
            write={
                "bigquery": "'x\\''",
                "hive": "'x\\''",
            },
        )

        self.validate_all(
            "r'x\\y'",
            write={
                "bigquery": "'x\\\\y'",
                "hive": "'x\\\\y'",
            },
        )
        self.validate_all(
            "'\\\\'",
            write={
                "bigquery": "'\\\\'",
                "duckdb": "'\\'",
                "presto": "'\\'",
                "hive": "'\\\\'",
            },
        )
        self.validate_all(
            r'r"""/\*.*\*/"""',
            write={
                "bigquery": r"'/\\*.*\\*/'",
                "duckdb": r"'/\\*.*\\*/'",
                "presto": r"'/\\*.*\\*/'",
                "hive": r"'/\\*.*\\*/'",
                "spark": r"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            r'R"""/\*.*\*/"""',
            write={
                "bigquery": r"'/\\*.*\\*/'",
                "duckdb": r"'/\\*.*\\*/'",
                "presto": r"'/\\*.*\\*/'",
                "hive": r"'/\\*.*\\*/'",
                "spark": r"'/\\*.*\\*/'",
            },
        )
        self.validate_all(
            'r"""a\n"""',
            write={
                "bigquery": "'a\\n'",
                "duckdb": "'a\n'",
            },
        )
        self.validate_all(
            '"""a\n"""',
            write={
                "bigquery": "'a\\n'",
                "duckdb": "'a\n'",
            },
        )
        self.validate_all(
            "CAST(a AS INT64)",
            write={
                "bigquery": "CAST(a AS INT64)",
                "duckdb": "CAST(a AS BIGINT)",
                "presto": "CAST(a AS BIGINT)",
                "hive": "CAST(a AS BIGINT)",
                "spark": "CAST(a AS BIGINT)",
            },
        )
        self.validate_all(
            "CAST(a AS BYTES)",
            write={
                "bigquery": "CAST(a AS BYTES)",
                "duckdb": "CAST(a AS BLOB)",
                "presto": "CAST(a AS VARBINARY)",
                "hive": "CAST(a AS BINARY)",
                "spark": "CAST(a AS BINARY)",
            },
        )
        self.validate_all(
            "CAST(a AS NUMERIC)",
            write={
                "bigquery": "CAST(a AS NUMERIC)",
                "duckdb": "CAST(a AS DECIMAL)",
                "presto": "CAST(a AS DECIMAL)",
                "hive": "CAST(a AS DECIMAL)",
                "spark": "CAST(a AS DECIMAL)",
            },
        )
        self.validate_all(
            "[1, 2, 3]",
            read={
                "duckdb": "[1, 2, 3]",
                "presto": "ARRAY[1, 2, 3]",
                "hive": "ARRAY(1, 2, 3)",
                "spark": "ARRAY(1, 2, 3)",
            },
            write={
                "bigquery": "[1, 2, 3]",
                "duckdb": "[1, 2, 3]",
                "presto": "ARRAY[1, 2, 3]",
                "hive": "ARRAY(1, 2, 3)",
                "spark": "ARRAY(1, 2, 3)",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(['7', '14']) AS x",
            read={
                "spark": "SELECT * FROM UNNEST(ARRAY('7', '14')) AS (x)",
            },
            write={
                "bigquery": "SELECT * FROM UNNEST(['7', '14']) AS x",
                "presto": "SELECT * FROM UNNEST(ARRAY['7', '14']) AS _t0(x)",
                "spark": "SELECT * FROM EXPLODE(ARRAY('7', '14')) AS _t0(x)",
            },
        )
        self.validate_all(
            "SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)",
            write={"bigquery": "SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)"},
        )
        self.validate_all(
            "SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers",
            write={
                "bigquery": "SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers"
            },
        )
        self.validate_all(
            "SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)",
            write={"bigquery": "SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)"},
        )
        self.validate_all(
            "x IS unknown",
            write={
                "bigquery": "x IS NULL",
                "duckdb": "x IS NULL",
                "presto": "x IS NULL",
                "hive": "x IS NULL",
                "spark": "x IS NULL",
            },
        )
        self.validate_all(
            "x IS NOT unknown",
            write={
                "bigquery": "NOT x IS NULL",
                "duckdb": "NOT x IS NULL",
                "presto": "NOT x IS NULL",
                "hive": "NOT x IS NULL",
                "spark": "NOT x IS NULL",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP()",
            read={
                "tsql": "GETDATE()",
            },
            write={
                "tsql": "GETDATE()",
            },
        )
        self.validate_all(
            "current_datetime",
            write={
                "bigquery": "CURRENT_DATETIME()",
                "presto": "CURRENT_DATETIME()",
                "hive": "CURRENT_DATETIME()",
                "spark": "CURRENT_DATETIME()",
            },
        )
        self.validate_all(
            "current_time",
            write={
                "bigquery": "CURRENT_TIME()",
                "duckdb": "CURRENT_TIME",
                "presto": "CURRENT_TIME",
                "trino": "CURRENT_TIME",
                "hive": "CURRENT_TIME()",
                "spark": "CURRENT_TIME()",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP()",
            write={
                "bigquery": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP",
                "postgres": "CURRENT_TIMESTAMP",
                "presto": "CURRENT_TIMESTAMP",
                "hive": "CURRENT_TIMESTAMP()",
                "spark": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "DIV(x, y)",
            write={
                "bigquery": "DIV(x, y)",
                "duckdb": "x // y",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT, struct_col_b STRING>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
            write={
                "bigquery": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "duckdb": "CREATE TABLE db.example_table (col_a STRUCT(struct_col_a BIGINT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))",
                "presto": "CREATE TABLE db.example_table (col_a ROW(struct_col_a BIGINT, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))",
                "hive": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a BIGINT, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)",
                "spark": "CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: BIGINT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)",
            },
        )
        self.validate_all(
            "CREATE TABLE db.example_table (x int) PARTITION BY x cluster by x",
            write={
                "bigquery": "CREATE TABLE db.example_table (x INT64) PARTITION BY x CLUSTER BY x",
            },
        )
        self.validate_all(
            "DELETE db.example_table WHERE x = 1",
            write={
                "bigquery": "DELETE db.example_table WHERE x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE db.example_table tb WHERE tb.x = 1",
            write={
                "bigquery": "DELETE db.example_table AS tb WHERE tb.x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE db.example_table AS tb WHERE tb.x = 1",
            write={
                "bigquery": "DELETE db.example_table AS tb WHERE tb.x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.example_table WHERE x = 1",
            write={
                "bigquery": "DELETE FROM db.example_table WHERE x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.example_table tb WHERE tb.x = 1",
            write={
                "bigquery": "DELETE FROM db.example_table AS tb WHERE tb.x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.example_table AS tb WHERE tb.x = 1",
            write={
                "bigquery": "DELETE FROM db.example_table AS tb WHERE tb.x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.example_table AS tb WHERE example_table.x = 1",
            write={
                "bigquery": "DELETE FROM db.example_table AS tb WHERE example_table.x = 1",
                "presto": "DELETE FROM db.example_table WHERE x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.example_table WHERE example_table.x = 1",
            write={
                "bigquery": "DELETE FROM db.example_table WHERE example_table.x = 1",
                "presto": "DELETE FROM db.example_table WHERE example_table.x = 1",
            },
        )
        self.validate_all(
            "DELETE FROM db.t1 AS t1 WHERE NOT t1.c IN (SELECT db.t2.c FROM db.t2)",
            write={
                "bigquery": "DELETE FROM db.t1 AS t1 WHERE NOT t1.c IN (SELECT db.t2.c FROM db.t2)",
                "presto": "DELETE FROM db.t1 WHERE NOT c IN (SELECT c FROM db.t2)",
            },
        )
        self.validate_all(
            "SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])",
            write={
                "bigquery": "SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])",
                "presto": "SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY[1, 2, 3]))",
                "hive": "SELECT * FROM a WHERE b IN (SELECT EXPLODE(ARRAY(1, 2, 3)))",
                "spark": "SELECT * FROM a WHERE b IN (SELECT EXPLODE(ARRAY(1, 2, 3)))",
            },
        )
        self.validate_all(
            "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            write={
                "postgres": "CURRENT_DATE - INTERVAL '1 DAY'",
                "bigquery": "DATE_SUB(CURRENT_DATE, INTERVAL '1' DAY)",
            },
        )
        self.validate_all(
            "DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)",
            write={
                "bigquery": "DATE_ADD(CURRENT_DATE, INTERVAL '-1' DAY)",
                "duckdb": "CURRENT_DATE + INTERVAL '-1' DAY",
                "mysql": "DATE_ADD(CURRENT_DATE, INTERVAL '-1' DAY)",
                "postgres": "CURRENT_DATE + INTERVAL '-1 DAY'",
                "presto": "DATE_ADD('DAY', CAST('-1' AS BIGINT), CURRENT_DATE)",
                "hive": "DATE_ADD(CURRENT_DATE, '-1')",
                "spark": "DATE_ADD(CURRENT_DATE, '-1')",
            },
        )
        self.validate_all(
            "DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY)",
            write={
                "bigquery": "DATE_DIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE), DAY)",
                "mysql": "DATEDIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
                "starrocks": "DATE_DIFF('DAY', CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "DATE_DIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE), DAY)",
            read={
                "mysql": "DATEDIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
                "starrocks": "DATEDIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', MINUTE)",
            write={
                "bigquery": "DATE_DIFF(CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE), MINUTE)",
                "starrocks": "DATE_DIFF('MINUTE', CAST('2010-07-07' AS DATE), CAST('2008-12-25' AS DATE))",
            },
        )
        self.validate_all(
            "DATE_DIFF('2021-01-01', '2020-01-01', DAY)",
            write={
                "bigquery": "DATE_DIFF('2021-01-01', '2020-01-01', DAY)",
                "duckdb": "DATE_DIFF('DAY', CAST('2020-01-01' AS DATE), CAST('2021-01-01' AS DATE))",
            },
        )
        self.validate_all(
            "CURRENT_DATE('UTC')",
            write={
                "mysql": "CURRENT_DATE AT TIME ZONE 'UTC'",
                "postgres": "CURRENT_DATE AT TIME ZONE 'UTC'",
            },
        )
        self.validate_all(
            "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
            write={
                "bigquery": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10",
                "snowflake": "SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS FIRST LIMIT 10",
            },
        )
        self.validate_all(
            "SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb)]) AS tab",
            read={
                "bigquery": "SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb)]) as tab",
                "snowflake": "SELECT cola, colb FROM (VALUES (1, 'test')) AS tab(cola, colb)",
                "spark": "SELECT cola, colb FROM VALUES (1, 'test') AS tab(cola, colb)",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST([STRUCT(1 AS _c0)]) AS t1",
            read={
                "bigquery": "SELECT * FROM UNNEST([STRUCT(1 AS _c0)]) AS t1",
                "postgres": "SELECT * FROM (VALUES (1)) AS t1",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST([STRUCT(1 AS id)]) AS t1 CROSS JOIN UNNEST([STRUCT(1 AS id)]) AS t2",
            read={
                "bigquery": "SELECT * FROM UNNEST([STRUCT(1 AS id)]) AS t1 CROSS JOIN UNNEST([STRUCT(1 AS id)]) AS t2",
                "postgres": "SELECT * FROM (VALUES (1)) AS t1(id) CROSS JOIN (VALUES (1)) AS t2(id)",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST([1]) WITH OFFSET",
            write={"bigquery": "SELECT * FROM UNNEST([1]) WITH OFFSET AS offset"},
        )
        self.validate_all(
            "SELECT * FROM UNNEST([1]) WITH OFFSET y",
            write={"bigquery": "SELECT * FROM UNNEST([1]) WITH OFFSET AS y"},
        )
        self.validate_all(
            "GENERATE_ARRAY(1, 4)",
            read={"bigquery": "GENERATE_ARRAY(1, 4)"},
            write={"duckdb": "GENERATE_SERIES(1, 4)"},
        )
        self.validate_all(
            "TO_JSON_STRING(x)",
            read={"bigquery": "TO_JSON_STRING(x)"},
            write={
                "bigquery": "TO_JSON_STRING(x)",
                "duckdb": "CAST(TO_JSON(x) AS TEXT)",
                "presto": "JSON_FORMAT(x)",
                "spark": "TO_JSON(x)",
            },
        )
        self.validate_all(
            """SELECT
  `u`.`user_email` AS `user_email`,
  `d`.`user_id` AS `user_id`,
  `account_id` AS `account_id`
FROM `analytics_staging`.`stg_mongodb__users` AS `u`, UNNEST(`u`.`cluster_details`) AS `d`, UNNEST(`d`.`account_ids`) AS `account_id`
WHERE
  NOT `account_id` IS NULL""",
            read={
                "": """
                SELECT
                  "u"."user_email" AS "user_email",
                  "_q_0"."d"."user_id" AS "user_id",
                  "_q_1"."account_id" AS "account_id"
                FROM
                  "analytics_staging"."stg_mongodb__users" AS "u",
                  UNNEST("u"."cluster_details") AS "_q_0"("d"),
                  UNNEST("_q_0"."d"."account_ids") AS "_q_1"("account_id")
                WHERE
                  NOT "_q_1"."account_id" IS NULL
                """
            },
            pretty=True,
        )
        self.validate_all(
            "SELECT MOD(x, 10)",
            read={"postgres": "SELECT x % 10"},
            write={
                "bigquery": "SELECT MOD(x, 10)",
                "postgres": "SELECT x % 10",
            },
        )
        self.validate_all(
            "SELECT CAST(x AS DATETIME)",
            write={
                "": "SELECT CAST(x AS TIMESTAMP)",
                "bigquery": "SELECT CAST(x AS DATETIME)",
            },
        )
        self.validate_all(
            "SELECT TIME(foo, 'America/Los_Angeles')",
            write={
                "duckdb": "SELECT CAST(CAST(foo AS TIMESTAMPTZ) AT TIME ZONE 'America/Los_Angeles' AS TIME)",
                "bigquery": "SELECT TIME(foo, 'America/Los_Angeles')",
            },
        )
        self.validate_all(
            "SELECT DATETIME('2020-01-01')",
            write={
                "duckdb": "SELECT CAST('2020-01-01' AS TIMESTAMP)",
                "bigquery": "SELECT DATETIME('2020-01-01')",
            },
        )
        self.validate_all(
            "SELECT DATETIME('2020-01-01', TIME '23:59:59')",
            write={
                "duckdb": "SELECT CAST(CAST('2020-01-01' AS DATE) + CAST('23:59:59' AS TIME) AS TIMESTAMP)",
                "bigquery": "SELECT DATETIME('2020-01-01', CAST('23:59:59' AS TIME))",
            },
        )
        self.validate_all(
            "SELECT DATETIME('2020-01-01', 'America/Los_Angeles')",
            write={
                "duckdb": "SELECT CAST(CAST('2020-01-01' AS TIMESTAMPTZ) AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP)",
                "bigquery": "SELECT DATETIME('2020-01-01', 'America/Los_Angeles')",
            },
        )
        self.validate_all(
            "SELECT LENGTH(foo)",
            read={
                "bigquery": "SELECT LENGTH(foo)",
                "snowflake": "SELECT LENGTH(foo)",
            },
            write={
                "duckdb": "SELECT CASE TYPEOF(foo) WHEN 'VARCHAR' THEN LENGTH(CAST(foo AS TEXT)) WHEN 'BLOB' THEN OCTET_LENGTH(CAST(foo AS BLOB)) END",
                "snowflake": "SELECT LENGTH(foo)",
                "": "SELECT LENGTH(foo)",
            },
        )
        self.validate_all(
            "SELECT TIME_DIFF('12:00:00', '12:30:00', MINUTE)",
            write={
                "duckdb": "SELECT DATE_DIFF('MINUTE', CAST('12:30:00' AS TIME), CAST('12:00:00' AS TIME))",
                "bigquery": "SELECT TIME_DIFF('12:00:00', '12:30:00', MINUTE)",
            },
        )
        self.validate_all(
            "ARRAY_CONCAT([1, 2], [3, 4], [5, 6])",
            write={
                "bigquery": "ARRAY_CONCAT([1, 2], [3, 4], [5, 6])",
                "duckdb": "ARRAY_CONCAT([1, 2], ARRAY_CONCAT([3, 4], [5, 6]))",
                "postgres": "ARRAY_CAT(ARRAY[1, 2], ARRAY_CAT(ARRAY[3, 4], ARRAY[5, 6]))",
                "redshift": "ARRAY_CONCAT(ARRAY(1, 2), ARRAY_CONCAT(ARRAY(3, 4), ARRAY(5, 6)))",
                "snowflake": "ARRAY_CAT([1, 2], ARRAY_CAT([3, 4], [5, 6]))",
                "hive": "CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))",
                "spark2": "CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))",
                "spark": "CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))",
                "databricks": "CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))",
                "presto": "CONCAT(ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6])",
                "trino": "CONCAT(ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6])",
            },
        )
        self.validate_all(
            "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08')",
            write={
                "duckdb": "SELECT CAST(GENERATE_SERIES(CAST('2016-10-05' AS DATE), CAST('2016-10-08' AS DATE), INTERVAL 1 DAY) AS DATE[])",
                "bigquery": "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08', INTERVAL 1 DAY)",
            },
        )
        self.validate_all(
            "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08', INTERVAL '1' MONTH)",
            write={
                "duckdb": "SELECT CAST(GENERATE_SERIES(CAST('2016-10-05' AS DATE), CAST('2016-10-08' AS DATE), INTERVAL '1' MONTH) AS DATE[])",
                "bigquery": "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08', INTERVAL '1' MONTH)",
            },
        )
        self.validate_all(
            "SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00', INTERVAL '1' DAY)",
            write={
                "duckdb": "SELECT GENERATE_SERIES(CAST('2016-10-05 00:00:00' AS TIMESTAMP), CAST('2016-10-07 00:00:00' AS TIMESTAMP), INTERVAL '1' DAY)",
                "bigquery": "SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00', INTERVAL '1' DAY)",
            },
        )
        self.validate_all(
            "SELECT PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008')",
            write={
                "bigquery": "SELECT PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008')",
                "duckdb": "SELECT CAST(STRPTIME('Thursday Dec 25 2008', '%A %b %-d %Y') AS DATE)",
            },
        )
        self.validate_all(
            "SELECT PARSE_DATE('%Y%m%d', '20081225')",
            write={
                "bigquery": "SELECT PARSE_DATE('%Y%m%d', '20081225')",
                "duckdb": "SELECT CAST(STRPTIME('20081225', '%Y%m%d') AS DATE)",
                "snowflake": "SELECT DATE('20081225', 'yyyymmDD')",
            },
        )
        self.validate_all(
            "SELECT ARRAY_TO_STRING(['cake', 'pie', NULL], '--') AS text",
            write={
                "bigquery": "SELECT ARRAY_TO_STRING(['cake', 'pie', NULL], '--') AS text",
                "duckdb": "SELECT ARRAY_TO_STRING(['cake', 'pie', NULL], '--') AS text",
            },
        )
        self.validate_all(
            "SELECT ARRAY_TO_STRING(['cake', 'pie', NULL], '--', 'MISSING') AS text",
            write={
                "bigquery": "SELECT ARRAY_TO_STRING(['cake', 'pie', NULL], '--', 'MISSING') AS text",
                "duckdb": "SELECT ARRAY_TO_STRING(LIST_TRANSFORM(['cake', 'pie', NULL], x -> COALESCE(x, 'MISSING')), '--') AS text",
            },
        )
        self.validate_all(
            "STRING(a)",
            write={
                "bigquery": "STRING(a)",
                "snowflake": "CAST(a AS VARCHAR)",
                "duckdb": "CAST(a AS TEXT)",
            },
        )
        self.validate_all(
            "STRING('2008-12-25 15:30:00', 'America/New_York')",
            write={
                "bigquery": "STRING('2008-12-25 15:30:00', 'America/New_York')",
                "snowflake": "CAST(CONVERT_TIMEZONE('UTC', 'America/New_York', '2008-12-25 15:30:00') AS VARCHAR)",
                "duckdb": "CAST(CAST('2008-12-25 15:30:00' AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS TEXT)",
            },
        )

        self.validate_identity("SELECT * FROM a-b c", "SELECT * FROM a-b AS c")

        self.validate_all(
            "SAFE_DIVIDE(x, y)",
            write={
                "bigquery": "SAFE_DIVIDE(x, y)",
                "duckdb": "CASE WHEN y <> 0 THEN x / y ELSE NULL END",
                "presto": "IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)",
                "trino": "IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)",
                "hive": "IF(y <> 0, x / y, NULL)",
                "spark2": "IF(y <> 0, x / y, NULL)",
                "spark": "IF(y <> 0, x / y, NULL)",
                "databricks": "IF(y <> 0, x / y, NULL)",
                "snowflake": "IFF(y <> 0, x / y, NULL)",
                "postgres": "CASE WHEN y <> 0 THEN CAST(x AS DOUBLE PRECISION) / y ELSE NULL END",
            },
        )
        self.validate_all(
            "SAFE_DIVIDE(x + 1, 2 * y)",
            write={
                "bigquery": "SAFE_DIVIDE(x + 1, 2 * y)",
                "duckdb": "CASE WHEN (2 * y) <> 0 THEN (x + 1) / (2 * y) ELSE NULL END",
                "presto": "IF((2 * y) <> 0, CAST((x + 1) AS DOUBLE) / (2 * y), NULL)",
                "trino": "IF((2 * y) <> 0, CAST((x + 1) AS DOUBLE) / (2 * y), NULL)",
                "hive": "IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)",
                "spark2": "IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)",
                "spark": "IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)",
                "databricks": "IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)",
                "snowflake": "IFF((2 * y) <> 0, (x + 1) / (2 * y), NULL)",
                "postgres": "CASE WHEN (2 * y) <> 0 THEN CAST((x + 1) AS DOUBLE PRECISION) / (2 * y) ELSE NULL END",
            },
        )
        self.validate_all(
            """SELECT JSON_VALUE_ARRAY('{"arr": [1, "a"]}', '$.arr')""",
            write={
                "bigquery": """SELECT JSON_VALUE_ARRAY('{"arr": [1, "a"]}', '$.arr')""",
                "duckdb": """SELECT CAST('{"arr": [1, "a"]}' -> '$.arr' AS TEXT[])""",
                "snowflake": """SELECT TRANSFORM(GET_PATH(PARSE_JSON('{"arr": [1, "a"]}'), 'arr'), x -> CAST(x AS VARCHAR))""",
            },
        )
        self.validate_all(
            "SELECT INSTR('foo@example.com', '@')",
            write={
                "bigquery": "SELECT INSTR('foo@example.com', '@')",
                "duckdb": "SELECT STRPOS('foo@example.com', '@')",
                "snowflake": "SELECT CHARINDEX('@', 'foo@example.com')",
            },
        )
        self.validate_all(
            "SELECT ts + MAKE_INTERVAL(1, 2, minute => 5, day => 3)",
            write={
                "bigquery": "SELECT ts + MAKE_INTERVAL(1, 2, day => 3, minute => 5)",
                "duckdb": "SELECT ts + INTERVAL '1 year 2 month 5 minute 3 day'",
                "snowflake": "SELECT ts + INTERVAL '1 year, 2 month, 5 minute, 3 day'",
            },
        )
        self.validate_all(
            """SELECT INT64(JSON_QUERY(JSON '{"key": 2000}', '$.key'))""",
            write={
                "bigquery": """SELECT INT64(JSON_QUERY(PARSE_JSON('{"key": 2000}'), '$.key'))""",
                "duckdb": """SELECT CAST(JSON('{"key": 2000}') -> '$.key' AS BIGINT)""",
                "snowflake": """SELECT CAST(GET_PATH(PARSE_JSON('{"key": 2000}'), 'key') AS BIGINT)""",
            },
        )

        self.validate_identity(
            "CONTAINS_SUBSTR(a, b, json_scope => 'JSON_KEYS_AND_VALUES')"
        ).assert_is(exp.Anonymous)

        self.validate_all(
            """CONTAINS_SUBSTR(a, b)""",
            read={
                "": "CONTAINS(a, b)",
                "spark": "CONTAINS(a, b)",
                "databricks": "CONTAINS(a, b)",
                "snowflake": "CONTAINS(a, b)",
                "duckdb": "CONTAINS(a, b)",
                "oracle": "CONTAINS(a, b)",
            },
            write={
                "": "CONTAINS(LOWER(a), LOWER(b))",
                "spark": "CONTAINS(LOWER(a), LOWER(b))",
                "databricks": "CONTAINS(LOWER(a), LOWER(b))",
                "snowflake": "CONTAINS(LOWER(a), LOWER(b))",
                "duckdb": "CONTAINS(LOWER(a), LOWER(b))",
                "oracle": "CONTAINS(LOWER(a), LOWER(b))",
                "bigquery": "CONTAINS_SUBSTR(a, b)",
            },
        )

        self.validate_identity(
            "SELECT * FROM ML.FEATURES_AT_TIME(TABLE mydataset.feature_table, time => '2022-06-11 10:00:00+00', num_rows => 1, ignore_feature_nulls => TRUE)"
        )
        self.validate_identity("SELECT * FROM ML.FEATURES_AT_TIME((SELECT 1), num_rows => 1)")

        self.validate_identity(
            "EXPORT DATA OPTIONS (URI='gs://path*.csv.gz', FORMAT='CSV') AS SELECT * FROM all_rows"
        )
        self.validate_identity(
            "EXPORT DATA WITH CONNECTION myproject.us.myconnection OPTIONS (URI='gs://path*.csv.gz', FORMAT='CSV') AS SELECT * FROM all_rows"
        )

    def test_errors(self):
        with self.assertRaises(TokenError):
            transpile("'\\'", read="bigquery")

        # Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        with self.assertRaises(ParseError):
            transpile("SELECT * FROM UNNEST(x) AS x(y)", read="bigquery")

        with self.assertRaises(ParseError):
            transpile("DATE_ADD(x, day)", read="bigquery")

    def test_warnings(self):
        with self.assertLogs(parser_logger) as cm:
            self.validate_identity(
                "/* some comment */ DECLARE foo DATE DEFAULT DATE_SUB(current_date, INTERVAL 2 day)"
            )
            self.assertIn("contains unsupported syntax", cm.output[0])

        with self.assertLogs(helper_logger) as cm:
            self.validate_identity(
                "WITH cte(c) AS (SELECT * FROM t) SELECT * FROM cte",
                "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
            )

            self.assertIn("Can't push down CTE column names for star queries.", cm.output[0])
            self.assertIn("Named columns are not supported in table alias.", cm.output[1])

        with self.assertLogs(helper_logger) as cm:
            self.validate_identity(
                "SELECT * FROM t AS t(c1, c2)",
                "SELECT * FROM t AS t",
            )

            self.assertIn("Named columns are not supported in table alias.", cm.output[0])

        with self.assertLogs(helper_logger) as cm:
            statements = parse(
                """
            BEGIN
              DECLARE 1;
              IF from_date IS NULL THEN SET x = 1;
              END IF;
            END
            """,
                read="bigquery",
            )

            for actual, expected in zip(
                statements,
                ("BEGIN DECLARE 1", "IF from_date IS NULL THEN SET x = 1", "END IF", "END"),
            ):
                self.assertEqual(actual.sql(dialect="bigquery"), expected)

            self.assertIn("unsupported syntax", cm.output[0])

        with self.assertLogs(helper_logger) as cm:
            statements = parse(
                """
                BEGIN CALL `project_id.dataset_id.stored_procedure_id`();
                EXCEPTION WHEN ERROR THEN INSERT INTO `project_id.dataset_id.table_id` SELECT @@error.message, CURRENT_TIMESTAMP();
                END
                """,
                read="bigquery",
            )

            expected_statements = (
                "BEGIN CALL `project_id.dataset_id.stored_procedure_id`()",
                "EXCEPTION WHEN ERROR THEN INSERT INTO `project_id.dataset_id.table_id` SELECT @@error.message, CURRENT_TIMESTAMP()",
                "END",
            )

            for actual, expected in zip(statements, expected_statements):
                self.assertEqual(actual.sql(dialect="bigquery"), expected)

            self.assertIn("unsupported syntax", cm.output[0])

        with self.assertLogs(helper_logger):
            statements = parse(
                """
                BEGIN
                    DECLARE MY_VAR INT64 DEFAULT 1;
                    SET MY_VAR = (SELECT 0);

                    IF MY_VAR = 1 THEN SELECT 'TRUE';
                    ELSEIF MY_VAR = 0 THEN SELECT 'FALSE';
                    ELSE SELECT 'NULL';
                    END IF;
                END
                """,
                read="bigquery",
            )

            expected_statements = (
                "BEGIN DECLARE MY_VAR INT64 DEFAULT 1",
                "SET MY_VAR = (SELECT 0)",
                "IF MY_VAR = 1 THEN SELECT 'TRUE'",
                "ELSEIF MY_VAR = 0 THEN SELECT 'FALSE'",
                "ELSE SELECT 'NULL'",
                "END IF",
                "END",
            )

            for actual, expected in zip(statements, expected_statements):
                self.assertEqual(actual.sql(dialect="bigquery"), expected)

        with self.assertLogs(helper_logger) as cm:
            self.validate_identity(
                "SELECT * FROM t AS t(c1, c2)",
                "SELECT * FROM t AS t",
            )

            self.assertIn("Named columns are not supported in table alias.", cm.output[0])

        with self.assertLogs(helper_logger):
            self.validate_all(
                "SELECT a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]",
                write={
                    "duckdb": "SELECT a[2], b[2], c[1], d[2], e[1]",
                    "bigquery": "SELECT a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]",
                    "presto": "SELECT a[2], b[2], c[1], ELEMENT_AT(d, 2), ELEMENT_AT(e, 1)",
                },
            )
            self.validate_all(
                "a[0]",
                read={
                    "bigquery": "a[0]",
                    "duckdb": "a[1]",
                    "presto": "a[1]",
                },
            )

        with self.assertLogs(parser_logger) as cm:
            for_in_stmts = parse(
                "FOR record IN (SELECT word FROM shakespeare) DO SELECT record.word; END FOR;",
                read="bigquery",
            )
            self.assertEqual(
                [s.sql(dialect="bigquery") for s in for_in_stmts],
                ["FOR record IN (SELECT word FROM shakespeare) DO SELECT record.word", "END FOR"],
            )
            self.assertIn("'END FOR'", cm.output[0])

    def test_user_defined_functions(self):
        self.validate_identity(
            "CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) RETURNS FLOAT64 NOT DETERMINISTIC LANGUAGE js AS 'return x*y;'"
        )
        self.validate_identity("CREATE TEMPORARY FUNCTION udf(x ANY TYPE) AS (x)")
        self.validate_identity("CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) AS ((x + 4) / y)")
        self.validate_identity(
            "CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE <q STRING, r INT64> AS SELECT s, t"
        )
        self.validate_identity(
            '''CREATE TEMPORARY FUNCTION string_length_0(strings ARRAY<STRING>) RETURNS FLOAT64 LANGUAGE js AS """'use strict'; function string_length(strings) { return _.sum(_.map(strings, ((x) => x.length))); } return string_length(strings);""" OPTIONS (library=['gs://ibis-testing-libraries/lodash.min.js'])''',
            "CREATE TEMPORARY FUNCTION string_length_0(strings ARRAY<STRING>) RETURNS FLOAT64 LANGUAGE js OPTIONS (library=['gs://ibis-testing-libraries/lodash.min.js']) AS '\\'use strict\\'; function string_length(strings) { return _.sum(_.map(strings, ((x) => x.length))); } return string_length(strings);'",
        )

    def test_remove_precision_parameterized_types(self):
        self.validate_identity("CREATE TABLE test (a NUMERIC(10, 2))")
        self.validate_identity(
            "INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING(10)), CAST(14 AS STRING(10)))",
            "INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING), CAST(14 AS STRING))",
        )
        self.validate_identity(
            "SELECT CAST(1 AS NUMERIC(10, 2))",
            "SELECT CAST(1 AS NUMERIC)",
        )
        self.validate_identity(
            "SELECT CAST('1' AS STRING(10)) UNION ALL SELECT CAST('2' AS STRING(10))",
            "SELECT CAST('1' AS STRING) UNION ALL SELECT CAST('2' AS STRING)",
        )
        self.validate_identity(
            "SELECT cola FROM (SELECT CAST('1' AS STRING(10)) AS cola UNION ALL SELECT CAST('2' AS STRING(10)) AS cola)",
            "SELECT cola FROM (SELECT CAST('1' AS STRING) AS cola UNION ALL SELECT CAST('2' AS STRING) AS cola)",
        )

    def test_gap_fill(self):
        self.validate_identity(
            "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, value_columns => [('signal', 'locf')]) ORDER BY time"
        )
        self.validate_identity(
            "SELECT a, b, c, d, e FROM GAP_FILL(TABLE foo, ts_column => 'b', partitioning_columns => ['a'], value_columns => [('c', 'bar'), ('d', 'baz'), ('e', 'bla')], bucket_width => INTERVAL '1' DAY)"
        )
        self.validate_identity(
            "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, value_columns => [('signal', 'linear')], ignore_null_values => FALSE) ORDER BY time"
        )
        self.validate_identity(
            "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE) ORDER BY time"
        )
        self.validate_identity(
            "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, value_columns => [('signal', 'null')], origin => CAST('2023-11-01 09:30:01' AS DATETIME)) ORDER BY time"
        )
        self.validate_identity(
            "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, value_columns => [('signal', 'locf')]) ORDER BY time"
        )

    def test_models(self):
        self.validate_identity(
            "SELECT * FROM ML.PREDICT(MODEL mydataset.mymodel, (SELECT label, column1, column2 FROM mydataset.mytable))"
        )
        self.validate_identity(
            "SELECT label, predicted_label1, predicted_label AS predicted_label2 FROM ML.PREDICT(MODEL mydataset.mymodel2, (SELECT * EXCEPT (predicted_label), predicted_label AS predicted_label1 FROM ML.PREDICT(MODEL mydataset.mymodel1, TABLE mydataset.mytable)))"
        )
        self.validate_identity(
            "SELECT * FROM ML.PREDICT(MODEL mydataset.mymodel, (SELECT custom_label, column1, column2 FROM mydataset.mytable), STRUCT(0.55 AS threshold))"
        )
        self.validate_identity(
            "SELECT * FROM ML.PREDICT(MODEL `my_project`.my_dataset.my_model, (SELECT * FROM input_data))"
        )
        self.validate_identity(
            "SELECT * FROM ML.PREDICT(MODEL my_dataset.vision_model, (SELECT uri, ML.RESIZE_IMAGE(ML.DECODE_IMAGE(data), 480, 480, FALSE) AS input FROM my_dataset.object_table))"
        )
        self.validate_identity(
            "SELECT * FROM ML.PREDICT(MODEL my_dataset.vision_model, (SELECT uri, ML.CONVERT_COLOR_SPACE(ML.RESIZE_IMAGE(ML.DECODE_IMAGE(data), 224, 280, TRUE), 'YIQ') AS input FROM my_dataset.object_table WHERE content_type = 'image/jpeg'))"
        )
        self.validate_identity(
            "CREATE OR REPLACE MODEL foo OPTIONS (model_type='linear_reg') AS SELECT bla FROM foo WHERE cond"
        )
        self.validate_identity(
            """CREATE OR REPLACE MODEL m
TRANSFORM(
  ML.FEATURE_CROSS(STRUCT(f1, f2)) AS cross_f,
  ML.QUANTILE_BUCKETIZE(f3) OVER () AS buckets,
  label_col
)
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['label_col']
) AS
SELECT
  *
FROM t""",
            pretty=True,
        )
        self.validate_identity(
            """CREATE MODEL project_id.mydataset.mymodel
INPUT(
  f1 INT64,
  f2 FLOAT64,
  f3 STRING,
  f4 ARRAY<INT64>
)
OUTPUT(
  out1 INT64,
  out2 INT64
)
REMOTE WITH CONNECTION myproject.us.test_connection
OPTIONS (
  ENDPOINT='https://us-central1-aiplatform.googleapis.com/v1/projects/myproject/locations/us-central1/endpoints/1234'
)""",
            pretty=True,
        )

    def test_merge(self):
        self.validate_all(
            """
            MERGE dataset.Inventory T
            USING dataset.NewArrivals S ON FALSE
            WHEN NOT MATCHED BY TARGET AND product LIKE '%a%'
            THEN DELETE
            WHEN NOT MATCHED BY SOURCE AND product LIKE '%b%'
            THEN DELETE""",
            write={
                "bigquery": "MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE '%a%' THEN DELETE WHEN NOT MATCHED BY SOURCE AND product LIKE '%b%' THEN DELETE",
                "snowflake": "MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE '%a%' THEN DELETE WHEN NOT MATCHED AND product LIKE '%b%' THEN DELETE",
            },
        )

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "snowflake": "ALTER TABLE db.t1 RENAME TO db.t2",
                "bigquery": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )

    @mock.patch("sqlglot.dialects.bigquery.logger")
    def test_pushdown_cte_column_names(self, logger):
        with self.assertRaises(UnsupportedError):
            transpile(
                "WITH cte(foo) AS (SELECT * FROM tbl) SELECT foo FROM cte",
                read="spark",
                write="bigquery",
                unsupported_level=ErrorLevel.RAISE,
            )

        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte",
            read={"spark": "WITH cte(foo) AS (SELECT 1) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte",
            read={"spark": "WITH cte(foo) AS (SELECT 1 AS bar) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte",
            read={"spark": "WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo, 2) SELECT foo FROM cte",
            read={"postgres": "WITH cte(foo) AS (SELECT 1, 2) SELECT foo FROM cte"},
        )
        self.validate_all(
            "WITH cte AS (SELECT 1 AS foo UNION ALL SELECT 2) SELECT foo FROM cte",
            read={"postgres": "WITH cte(foo) AS (SELECT 1 UNION ALL SELECT 2) SELECT foo FROM cte"},
        )

    def test_json_object(self):
        self.validate_identity("SELECT JSON_OBJECT() AS json_data")
        self.validate_identity("SELECT JSON_OBJECT('foo', 10, 'bar', TRUE) AS json_data")
        self.validate_identity("SELECT JSON_OBJECT('foo', 10, 'bar', ['a', 'b']) AS json_data")
        self.validate_identity("SELECT JSON_OBJECT('a', 10, 'a', 'foo') AS json_data")
        self.validate_identity(
            "SELECT JSON_OBJECT(['a', 'b'], [10, NULL]) AS json_data",
            "SELECT JSON_OBJECT('a', 10, 'b', NULL) AS json_data",
        )
        self.validate_identity(
            """SELECT JSON_OBJECT(['a', 'b'], [JSON '10', JSON '"foo"']) AS json_data""",
            """SELECT JSON_OBJECT('a', PARSE_JSON('10'), 'b', PARSE_JSON('"foo"')) AS json_data""",
        )
        self.validate_identity(
            "SELECT JSON_OBJECT(['a', 'b'], [STRUCT(10 AS id, 'Red' AS color), STRUCT(20 AS id, 'Blue' AS color)]) AS json_data",
            "SELECT JSON_OBJECT('a', STRUCT(10 AS id, 'Red' AS color), 'b', STRUCT(20 AS id, 'Blue' AS color)) AS json_data",
        )
        self.validate_identity(
            "SELECT JSON_OBJECT(['a', 'b'], [TO_JSON(10), TO_JSON(['foo', 'bar'])]) AS json_data",
            "SELECT JSON_OBJECT('a', TO_JSON(10), 'b', TO_JSON(['foo', 'bar'])) AS json_data",
        )

        with self.assertRaises(ParseError):
            transpile("SELECT JSON_OBJECT('a', 1, 'b') AS json_data", read="bigquery")

    def test_mod(self):
        for sql in ("MOD(a, b)", "MOD('a', b)", "MOD(5, 2)", "MOD((a + 1) * 8, 5 - 1)"):
            with self.subTest(f"Testing BigQuery roundtrip of modulo operation: {sql}"):
                self.validate_identity(sql)

        self.validate_identity("SELECT MOD((SELECT 1), 2)")
        self.validate_identity(
            "MOD((a + 1), b)",
            "MOD(a + 1, b)",
        )

    def test_inline_constructor(self):
        self.validate_identity(
            """SELECT STRUCT<ARRAY<STRING>>(["2023-01-17"])""",
            """SELECT CAST(STRUCT(['2023-01-17']) AS STRUCT<ARRAY<STRING>>)""",
        )
        self.validate_identity(
            """SELECT STRUCT<STRING>((SELECT 'foo')).*""",
            """SELECT CAST(STRUCT((SELECT 'foo')) AS STRUCT<STRING>).*""",
        )

        self.validate_all(
            "SELECT ARRAY<FLOAT64>[1, 2, 3]",
            write={
                "bigquery": "SELECT ARRAY<FLOAT64>[1, 2, 3]",
                "duckdb": "SELECT CAST([1, 2, 3] AS DOUBLE[])",
            },
        )
        self.validate_all(
            "CAST(STRUCT<a INT64>(1) AS STRUCT<a INT64>)",
            write={
                "bigquery": "CAST(CAST(STRUCT(1) AS STRUCT<a INT64>) AS STRUCT<a INT64>)",
                "duckdb": "CAST(CAST(ROW(1) AS STRUCT(a BIGINT)) AS STRUCT(a BIGINT))",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])",
            write={
                "bigquery": "SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])",
                "duckdb": "SELECT * FROM (SELECT UNNEST(CAST([] AS STRUCT(x BIGINT)[]), max_depth => 2))",
            },
        )
        self.validate_all(
            "SELECT * FROM UNNEST(ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'INACTIVE'),STRUCT(4, DATETIME '2023-11-01 09:38:01', 80, 'ACTIVE')])",
            write={
                "bigquery": "SELECT * FROM UNNEST(ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[STRUCT(1, CAST('2023-11-01 09:34:01' AS DATETIME), 74, 'INACTIVE'), STRUCT(4, CAST('2023-11-01 09:38:01' AS DATETIME), 80, 'ACTIVE')])",
                "duckdb": "SELECT * FROM (SELECT UNNEST(CAST([ROW(1, CAST('2023-11-01 09:34:01' AS TIMESTAMP), 74, 'INACTIVE'), ROW(4, CAST('2023-11-01 09:38:01' AS TIMESTAMP), 80, 'ACTIVE')] AS STRUCT(device_id BIGINT, time TIMESTAMP, signal BIGINT, state TEXT)[]), max_depth => 2))",
            },
        )
        self.validate_all(
            "SELECT STRUCT<a INT64, b STRUCT<c STRING>>(1, STRUCT('c_str'))",
            write={
                "bigquery": "SELECT CAST(STRUCT(1, STRUCT('c_str')) AS STRUCT<a INT64, b STRUCT<c STRING>>)",
                "duckdb": "SELECT CAST(ROW(1, ROW('c_str')) AS STRUCT(a BIGINT, b STRUCT(c TEXT)))",
            },
        )

    def test_convert(self):
        for value, expected in [
            (datetime.datetime(2023, 1, 1), "CAST('2023-01-01 00:00:00' AS DATETIME)"),
            (datetime.datetime(2023, 1, 1, 12, 13, 14), "CAST('2023-01-01 12:13:14' AS DATETIME)"),
            (
                datetime.datetime(2023, 1, 1, 12, 13, 14, tzinfo=datetime.timezone.utc),
                "CAST('2023-01-01 12:13:14+00:00' AS TIMESTAMP)",
            ),
            (
                pytz.timezone("America/Los_Angeles").localize(
                    datetime.datetime(2023, 1, 1, 12, 13, 14)
                ),
                "CAST('2023-01-01 12:13:14-08:00' AS TIMESTAMP)",
            ),
        ]:
            with self.subTest(value):
                self.assertEqual(exp.convert(value).sql(dialect=self.dialect), expected)

    def test_unnest(self):
        self.validate_all(
            "SELECT name, laps FROM UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps), STRUCT('Makhloufi' AS name, [24.5, 25.4, 26.6, 26.1] AS laps)])",
            write={
                "bigquery": "SELECT name, laps FROM UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps), STRUCT('Makhloufi' AS name, [24.5, 25.4, 26.6, 26.1] AS laps)])",
                "duckdb": "SELECT name, laps FROM (SELECT UNNEST([{'name': 'Rudisha', 'laps': [23.4, 26.3, 26.4, 26.1]}, {'name': 'Makhloufi', 'laps': [24.5, 25.4, 26.6, 26.1]}], max_depth => 2))",
            },
        )
        self.validate_all(
            "WITH Races AS (SELECT '800M' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)])",
            write={
                "bigquery": "WITH Races AS (SELECT '800M' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)])",
                "duckdb": "WITH Races AS (SELECT '800M' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN (SELECT UNNEST([{'name': 'Rudisha', 'laps': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2))",
            },
        )
        self.validate_all(
            "SELECT participant FROM UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant",
            write={
                "bigquery": "SELECT participant FROM UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant",
                "duckdb": "SELECT participant FROM (SELECT UNNEST([{'name': 'Rudisha', 'laps': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2)) AS participant",
            },
        )
        self.validate_all(
            "WITH Races AS (SELECT '800M' AS race) SELECT race, participant FROM Races AS r CROSS JOIN UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant",
            write={
                "bigquery": "WITH Races AS (SELECT '800M' AS race) SELECT race, participant FROM Races AS r CROSS JOIN UNNEST([STRUCT('Rudisha' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant",
                "duckdb": "WITH Races AS (SELECT '800M' AS race) SELECT race, participant FROM Races AS r CROSS JOIN (SELECT UNNEST([{'name': 'Rudisha', 'laps': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2)) AS participant",
            },
        )

    def test_range_type(self):
        for type, value in (
            ("RANGE<DATE>", "'[2020-01-01, 2020-12-31)'"),
            ("RANGE<DATE>", "'[UNBOUNDED, 2020-12-31)'"),
            ("RANGE<DATETIME>", "'[2020-01-01 12:00:00, 2020-12-31 12:00:00)'"),
            ("RANGE<TIMESTAMP>", "'[2020-10-01 12:00:00+08, 2020-12-31 12:00:00+08)'"),
        ):
            with self.subTest(f"Testing BigQuery's RANGE<T> type: {type} {value}"):
                self.validate_identity(f"SELECT {type} {value}", f"SELECT CAST({value} AS {type})")

                self.assertEqual(self.parse_one(type), exp.DataType.build(type, dialect="bigquery"))

        self.validate_identity(
            "SELECT RANGE(CAST('2022-12-01' AS DATE), CAST('2022-12-31' AS DATE))"
        )
        self.validate_identity("SELECT RANGE(NULL, CAST('2022-12-31' AS DATE))")
        self.validate_identity(
            "SELECT RANGE(CAST('2022-10-01 14:53:27' AS DATETIME), CAST('2022-10-01 16:00:00' AS DATETIME))"
        )
        self.validate_identity(
            "SELECT RANGE(CAST('2022-10-01 14:53:27 America/Los_Angeles' AS TIMESTAMP), CAST('2022-10-01 16:00:00 America/Los_Angeles' AS TIMESTAMP))"
        )

    def test_null_ordering(self):
        # Aggregate functions allow "NULLS FIRST" only with ascending order and
        # "NULLS LAST" only with descending
        for sort_order, null_order in (("ASC", "NULLS LAST"), ("DESC", "NULLS FIRST")):
            self.validate_all(
                f"SELECT color, ARRAY_AGG(id ORDER BY id {sort_order}) AS ids FROM colors GROUP BY 1",
                read={
                    "": f"SELECT color, ARRAY_AGG(id ORDER BY id {sort_order} {null_order}) AS ids FROM colors GROUP BY 1"
                },
                write={
                    "bigquery": f"SELECT color, ARRAY_AGG(id ORDER BY id {sort_order}) AS ids FROM colors GROUP BY 1",
                },
            )

            self.validate_all(
                f"SELECT SUM(f1) OVER (ORDER BY f2 {sort_order}) FROM t",
                read={
                    "": f"SELECT SUM(f1) OVER (ORDER BY f2 {sort_order} {null_order}) FROM t",
                },
                write={
                    "bigquery": f"SELECT SUM(f1) OVER (ORDER BY f2 {sort_order}) FROM t",
                },
            )

    def test_json_extract(self):
        self.validate_all(
            """SELECT JSON_QUERY('{"class": {"students": []}}', '$.class')""",
            write={
                "bigquery": """SELECT JSON_QUERY('{"class": {"students": []}}', '$.class')""",
                "duckdb": """SELECT '{"class": {"students": []}}' -> '$.class'""",
                "snowflake": """SELECT GET_PATH(PARSE_JSON('{"class": {"students": []}}'), 'class')""",
            },
        )

        for func in ("JSON_EXTRACT_SCALAR", "JSON_VALUE"):
            with self.subTest(f"Testing BigQuery's {func}"):
                self.validate_all(
                    f"SELECT {func}('5')",
                    write={
                        "bigquery": f"SELECT {func}('5', '$')",
                        "duckdb": """SELECT '5' ->> '$'""",
                    },
                )

                sql = f"""SELECT {func}('{{"name": "Jakob", "age": "6"}}', '$.age')"""
                self.validate_all(
                    sql,
                    write={
                        "bigquery": sql,
                        "duckdb": """SELECT '{"name": "Jakob", "age": "6"}' ->> '$.age'""",
                        "snowflake": """SELECT JSON_EXTRACT_PATH_TEXT('{"name": "Jakob", "age": "6"}', 'age')""",
                    },
                )

                self.assertEqual(
                    self.parse_one(sql).sql("bigquery", normalize_functions="upper"), sql
                )

        # Test double quote escaping
        for func in ("JSON_VALUE", "JSON_QUERY", "JSON_QUERY_ARRAY"):
            self.validate_identity(
                f"{func}(doc, '$. a b c .d')", f"""{func}(doc, '$." a b c ".d')"""
            )

        # Test single quote & bracket escaping
        for func in ("JSON_EXTRACT", "JSON_EXTRACT_SCALAR", "JSON_EXTRACT_ARRAY"):
            self.validate_identity(
                f"{func}(doc, '$. a b c .d')", f"""{func}(doc, '$[\\' a b c \\'].d')"""
            )

    def test_json_extract_array(self):
        for func in ("JSON_QUERY_ARRAY", "JSON_EXTRACT_ARRAY"):
            with self.subTest(f"Testing BigQuery's {func}"):
                sql = f"""SELECT {func}('{{"fruits": [1, "oranges"]}}', '$.fruits')"""
                self.validate_all(
                    sql,
                    write={
                        "bigquery": sql,
                        "duckdb": """SELECT CAST('{"fruits": [1, "oranges"]}' -> '$.fruits' AS JSON[])""",
                        "snowflake": """SELECT TRANSFORM(GET_PATH(PARSE_JSON('{"fruits": [1, "oranges"]}'), 'fruits'), x -> PARSE_JSON(TO_JSON(x)))""",
                    },
                )

                self.assertEqual(
                    self.parse_one(sql).sql("bigquery", normalize_functions="upper"), sql
                )

    def test_unix_seconds(self):
        self.validate_all(
            "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
            read={
                "bigquery": "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
                "spark": "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
                "databricks": "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
            },
            write={
                "spark": "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
                "databricks": "SELECT UNIX_SECONDS('2008-12-25 15:30:00+00')",
                "duckdb": "SELECT DATE_DIFF('SECONDS', CAST('1970-01-01 00:00:00+00' AS TIMESTAMPTZ), '2008-12-25 15:30:00+00')",
                "snowflake": "SELECT TIMESTAMPDIFF(SECONDS, CAST('1970-01-01 00:00:00+00' AS TIMESTAMPTZ), '2008-12-25 15:30:00+00')",
            },
        )

        for dialect in ("bigquery", "spark", "databricks"):
            parse_one("UNIX_SECONDS(col)", dialect=dialect).assert_is(exp.UnixSeconds)

    def test_regexp_extract(self):
        self.validate_identity("REGEXP_EXTRACT(x, '(?<)')")
        self.validate_identity("REGEXP_EXTRACT(`foo`, 'bar: (.+?)', 1, 1)")
        self.validate_identity(
            r"REGEXP_EXTRACT(svc_plugin_output, r'\\\((.*)')",
            r"REGEXP_EXTRACT(svc_plugin_output, '\\\\\\((.*)')",
        )
        self.validate_identity(
            r"REGEXP_SUBSTR(value, pattern, position, occurrence)",
            r"REGEXP_EXTRACT(value, pattern, position, occurrence)",
        )

        self.validate_all(
            "SELECT REGEXP_EXTRACT(abc, 'pattern(group)') FROM table",
            write={
                "bigquery": "SELECT REGEXP_EXTRACT(abc, 'pattern(group)') FROM table",
                "duckdb": '''SELECT REGEXP_EXTRACT(abc, 'pattern(group)', 1) FROM "table"''',
            },
        )

        # The pattern does not capture a group (entire regular expression is extracted)
        self.validate_all(
            "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
            read={
                "bigquery": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "trino": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "presto": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "snowflake": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "duckdb": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "spark": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "databricks": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
            },
            write={
                "bigquery": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "trino": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "presto": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "snowflake": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "duckdb": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "spark": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "databricks": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
            },
        )

        # The pattern does capture >=1 group (the default is to extract the first instance)
        self.validate_all(
            "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]')",
            write={
                "bigquery": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]')",
                "trino": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]', 1)",
                "presto": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]', 1)",
                "snowflake": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]', 1, 1, 'c', 1)",
                "duckdb": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]', 1)",
                "spark": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]')",
                "databricks": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', '(a)[0-9]')",
            },
        )

    def test_format_temporal(self):
        self.validate_all(
            "SELECT FORMAT_DATE('%Y%m%d', '2023-12-25')",
            write={
                "bigquery": "SELECT FORMAT_DATE('%Y%m%d', '2023-12-25')",
                "duckdb": "SELECT STRFTIME(CAST('2023-12-25' AS DATE), '%Y%m%d')",
            },
        )
        self.validate_all(
            "SELECT FORMAT_DATETIME('%Y%m%d %H:%M:%S', DATETIME '2023-12-25 15:30:00')",
            write={
                "bigquery": "SELECT FORMAT_DATETIME('%Y%m%d %H:%M:%S', CAST('2023-12-25 15:30:00' AS DATETIME))",
                "duckdb": "SELECT STRFTIME(CAST('2023-12-25 15:30:00' AS TIMESTAMP), '%Y%m%d %H:%M:%S')",
            },
        )
        self.validate_all(
            "SELECT FORMAT_DATETIME('%x', '2023-12-25 15:30:00')",
            write={
                "bigquery": "SELECT FORMAT_DATETIME('%x', '2023-12-25 15:30:00')",
                "duckdb": "SELECT STRFTIME(CAST('2023-12-25 15:30:00' AS TIMESTAMP), '%x')",
            },
        )
        self.validate_all(
            """SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2050-12-25 15:30:55+00")""",
            write={
                "bigquery": "SELECT FORMAT_TIMESTAMP('%b-%d-%Y', CAST('2050-12-25 15:30:55+00' AS TIMESTAMP))",
                "duckdb": "SELECT STRFTIME(CAST(CAST('2050-12-25 15:30:55+00' AS TIMESTAMPTZ) AS TIMESTAMP), '%b-%d-%Y')",
                "snowflake": "SELECT TO_CHAR(CAST(CAST('2050-12-25 15:30:55+00' AS TIMESTAMPTZ) AS TIMESTAMP), 'mon-DD-yyyy')",
            },
        )

    def test_string_agg(self):
        self.validate_identity(
            "SELECT a, GROUP_CONCAT(b) FROM table GROUP BY a",
            "SELECT a, STRING_AGG(b, ',') FROM table GROUP BY a",
        )

        self.validate_identity("STRING_AGG(a, ' & ')")
        self.validate_identity("STRING_AGG(DISTINCT a, ' & ')")
        self.validate_identity("STRING_AGG(a, ' & ' ORDER BY LENGTH(a))")
        self.validate_identity("STRING_AGG(foo, b'|' ORDER BY bar)")

        self.validate_identity("STRING_AGG(a)", "STRING_AGG(a, ',')")
        self.validate_identity(
            "STRING_AGG(DISTINCT a ORDER BY b DESC, c DESC LIMIT 10)",
            "STRING_AGG(DISTINCT a, ',' ORDER BY b DESC, c DESC LIMIT 10)",
        )
