from sqlglot import exp, transpile, parse_one
from sqlglot.errors import ParseError
from tests.dialects.test_dialect import Validator


class TestDatabricks(Validator):
    dialect = "databricks"

    def test_databricks(self):
        null_type = exp.DataType.build("VOID", dialect="databricks")
        self.assertEqual(null_type.sql(), "NULL")
        self.assertEqual(null_type.sql("databricks"), "VOID")

        self.validate_identity("REGEXP_LIKE(x, y)")
        self.validate_identity("SELECT CAST(NULL AS VOID)")
        self.validate_identity("SELECT void FROM t")
        self.validate_identity("SELECT * FROM stream")
        self.validate_identity("SELECT t.current_time FROM t")
        self.validate_identity("ALTER TABLE labels ADD COLUMN label_score FLOAT")
        self.validate_identity("DESCRIBE HISTORY a.b")
        self.validate_identity("DESCRIBE history.tbl")
        self.validate_identity("CREATE TABLE t (a STRUCT<c: MAP<STRING, STRING>>)")
        self.validate_identity("CREATE TABLE t (c STRUCT<interval: DOUBLE COMMENT 'aaa'>)")
        self.validate_identity("CREATE TABLE my_table TBLPROPERTIES (a.b=15)")
        self.validate_identity("CREATE TABLE my_table TBLPROPERTIES ('a.b'=15)")
        self.validate_identity("SELECT CAST('11 23:4:0' AS INTERVAL DAY TO HOUR)")
        self.validate_identity("SELECT CAST('11 23:4:0' AS INTERVAL DAY TO MINUTE)")
        self.validate_identity("SELECT CAST('11 23:4:0' AS INTERVAL DAY TO SECOND)")
        self.validate_identity("SELECT CAST('23:00:00' AS INTERVAL HOUR TO MINUTE)")
        self.validate_identity("SELECT CAST('23:00:00' AS INTERVAL HOUR TO SECOND)")
        self.validate_identity("SELECT CAST('23:00:00' AS INTERVAL MINUTE TO SECOND)")
        self.validate_identity("CREATE TABLE target SHALLOW CLONE source")
        self.validate_identity("INSERT INTO a REPLACE WHERE cond VALUES (1), (2)")
        self.validate_identity("CREATE FUNCTION a.b(x INT) RETURNS INT RETURN x + 1")
        self.validate_identity("CREATE FUNCTION a AS b")
        self.validate_identity("SELECT ${x} FROM ${y} WHERE ${z} > 1")
        self.validate_identity("CREATE TABLE foo (x DATE GENERATED ALWAYS AS (CAST(y AS DATE)))")
        self.validate_identity("TRUNCATE TABLE t1 PARTITION(age = 10, name = 'test', address)")
        self.validate_identity("SELECT PARSE_JSON('{}')")
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS db.table (a TIMESTAMP, b BOOLEAN GENERATED ALWAYS AS (NOT a IS NULL)) USING DELTA"
        )
        self.validate_identity(
            "SELECT * FROM sales UNPIVOT INCLUDE NULLS (sales FOR quarter IN (q1 AS `Jan-Mar`))"
        )
        self.validate_identity(
            "SELECT * FROM sales UNPIVOT EXCLUDE NULLS (sales FOR quarter IN (q1 AS `Jan-Mar`))"
        )
        self.validate_identity(
            "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$"
        )
        self.validate_identity(
            "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $FOO$def add_one(x):\n  return x+1$FOO$"
        )
        self.validate_identity(
            "TRUNCATE TABLE t1 PARTITION(age = 10, name = 'test', city LIKE 'LA')"
        )
        self.validate_identity(
            "COPY INTO target FROM `s3://link` FILEFORMAT = AVRO VALIDATE = ALL FILES = ('file1', 'file2') FORMAT_OPTIONS ('opt1'='true', 'opt2'='test') COPY_OPTIONS ('mergeSchema'='true')"
        )
        self.validate_identity(
            "SELECT * FROM t1, t2",
            "SELECT * FROM t1 CROSS JOIN t2",
        )
        self.validate_identity(
            "SELECT TIMESTAMP '2025-04-29 18.47.18'::DATE",
            "SELECT CAST(CAST('2025-04-29 18.47.18' AS DATE) AS TIMESTAMP)",
        )
        self.validate_identity(
            "SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(foo, 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t",
            "SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t",
        )
        self.validate_identity(
            "DATE_DIFF(day, created_at, current_date())",
            "DATEDIFF(DAY, created_at, CURRENT_DATE)",
        ).args["unit"].assert_is(exp.Var)
        self.validate_identity(
            r'SELECT r"\\foo.bar\"',
            r"SELECT '\\\\foo.bar\\'",
        )
        self.validate_identity(
            "FROM_UTC_TIMESTAMP(x::TIMESTAMP, tz)",
            "FROM_UTC_TIMESTAMP(CAST(x AS TIMESTAMP), tz)",
        )

        self.validate_identity("SELECT SUBSTRING_INDEX(str, delim, count)")

        self.validate_all(
            "SELECT SUBSTRING_INDEX('a.b.c.d', '.', 2)",
            write={
                "databricks": "SELECT SUBSTRING_INDEX('a.b.c.d', '.', 2)",
                "spark": "SELECT SUBSTRING_INDEX('a.b.c.d', '.', 2)",
                "mysql": "SELECT SUBSTRING_INDEX('a.b.c.d', '.', 2)",
            },
        )

        self.validate_all(
            "SELECT TYPEOF(1)",
            read={
                "databricks": "SELECT TYPEOF(1)",
                "snowflake": "SELECT TYPEOF(1)",
                "hive": "SELECT TYPEOF(1)",
                "clickhouse": "SELECT toTypeName(1)",
            },
            write={
                "clickhouse": "SELECT toTypeName(1)",
            },
        )

        self.validate_all(
            "SELECT c1:item[1].price",
            read={
                "spark": "SELECT GET_JSON_OBJECT(c1, '$.item[1].price')",
            },
            write={
                "databricks": "SELECT c1:item[1].price",
                "spark": "SELECT GET_JSON_OBJECT(c1, '$.item[1].price')",
            },
        )

        self.validate_all(
            "SELECT GET_JSON_OBJECT(c1, '$.item[1].price')",
            write={
                "databricks": "SELECT c1:item[1].price",
                "spark": "SELECT GET_JSON_OBJECT(c1, '$.item[1].price')",
            },
        )
        self.validate_all(
            "CREATE TABLE foo (x INT GENERATED ALWAYS AS (YEAR(y)))",
            write={
                "databricks": "CREATE TABLE foo (x INT GENERATED ALWAYS AS (YEAR(y)))",
                "tsql": "CREATE TABLE foo (x AS YEAR(CAST(y AS DATE)))",
            },
        )
        self.validate_all(
            "CREATE TABLE t1 AS (SELECT c FROM t2)",
            read={
                "teradata": "CREATE TABLE t1 AS (SELECT c FROM t2) WITH DATA",
            },
        )
        self.validate_all(
            "SELECT X'1A2B'",
            read={
                "spark2": "SELECT X'1A2B'",
                "spark": "SELECT X'1A2B'",
                "databricks": "SELECT x'1A2B'",
            },
            write={
                "spark2": "SELECT X'1A2B'",
                "spark": "SELECT X'1A2B'",
                "databricks": "SELECT X'1A2B'",
            },
        )

        with self.assertRaises(ParseError):
            transpile(
                "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $foo$def add_one(x):\n  return x+1$$",
                read="databricks",
            )

        with self.assertRaises(ParseError):
            transpile(
                "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $foo bar$def add_one(x):\n  return x+1$foo bar$",
                read="databricks",
            )

        self.validate_all(
            "CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS TABLE (a INT) RETURN SELECT a",
            write={
                "databricks": "CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS TABLE (a INT) RETURN SELECT a",
                "duckdb": "CREATE OR REPLACE FUNCTION func(a, b) AS TABLE SELECT a",
            },
        )

        self.validate_all(
            "CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS BIGINT RETURN a",
            write={
                "databricks": "CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS BIGINT RETURN a",
                "duckdb": "CREATE OR REPLACE FUNCTION func(a, b) AS a",
            },
        )

        self.validate_all(
            "SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)",
            read={
                "databricks": "SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)",
                "spark": "SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)",
            },
            write={
                "spark": "SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)",
            },
        )

        for option in ("", " (foo)", " MATCH FULL", " NOT ENFORCED"):
            with self.subTest(f"Databricks foreign key REFERENCES option: {option}."):
                self.validate_identity(
                    f"CREATE TABLE t1 (foo BIGINT NOT NULL CONSTRAINT foo_c FOREIGN KEY REFERENCES t2{option})"
                )
        self.validate_identity(
            "SELECT test, LISTAGG(email, '') AS Email FROM organizations GROUP BY test",
        )

        self.validate_identity(
            "WITH t AS (VALUES ('foo_val') AS t(foo1)) SELECT foo1 FROM t",
            "WITH t AS (SELECT * FROM VALUES ('foo_val') AS t(foo1)) SELECT foo1 FROM t",
        )
        self.validate_identity("NTILE() OVER (ORDER BY 1)")

    # https://docs.databricks.com/sql/language-manual/functions/colonsign.html
    def test_json(self):
        self.validate_identity("SELECT c1:price, c1:price.foo, c1:price.bar[1]")
        self.validate_identity("SELECT TRY_CAST(c1:price AS ARRAY<VARIANT>)")
        self.validate_identity("""SELECT TRY_CAST(c1:["foo bar"]["baz qux"] AS ARRAY<VARIANT>)""")
        self.validate_identity(
            """SELECT c1:item[1].price FROM VALUES ('{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }') AS T(c1)"""
        )
        self.validate_identity(
            """SELECT c1:item[*].price FROM VALUES ('{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }') AS T(c1)"""
        )
        self.validate_identity(
            """SELECT FROM_JSON(c1:item[*].price, 'ARRAY<DOUBLE>')[0] FROM VALUES ('{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }') AS T(c1)"""
        )
        self.validate_identity(
            """SELECT INLINE(FROM_JSON(c1:item[*], 'ARRAY<STRUCT<model STRING, price DOUBLE>>')) FROM VALUES ('{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }') AS T(c1)"""
        )
        self.validate_identity(
            """SELECT c1:['price'] FROM VALUES ('{ "price": 5 }') AS T(c1)""",
            """SELECT c1:price FROM VALUES ('{ "price": 5 }') AS T(c1)""",
        )
        self.validate_identity(
            """SELECT GET_JSON_OBJECT(c1, '$.price') FROM VALUES ('{ "price": 5 }') AS T(c1)""",
            """SELECT c1:price FROM VALUES ('{ "price": 5 }') AS T(c1)""",
        )
        self.validate_identity(
            """SELECT raw:`zip code`, raw:`fb:testid`, raw:store['bicycle'], raw:store["zip code"]""",
            """SELECT raw:["zip code"], raw:["fb:testid"], raw:store.bicycle, raw:store["zip code"]""",
        )
        self.validate_all(
            "SELECT col:`fr'uit`",
            write={
                "databricks": """SELECT col:["fr'uit"]""",
                "postgres": "SELECT JSON_EXTRACT_PATH(col, 'fr''uit')",
            },
        )

    def test_datediff(self):
        self.validate_all(
            "SELECT DATEDIFF(year, 'start', 'end')",
            write={
                "tsql": "SELECT DATEDIFF(YEAR, 'start', 'end')",
                "databricks": "SELECT DATEDIFF(YEAR, 'start', 'end')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(microsecond, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(MICROSECOND, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) * 1000000 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(millisecond, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(MILLISECOND, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) * 1000 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(second, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(SECOND, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(minute, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(MINUTE, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) / 60 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(hour, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(HOUR, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) / 3600 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(day, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(DAY, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(epoch FROM CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP)) / 86400 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(week, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(WEEK, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(days FROM (CAST('end' AS TIMESTAMP) - CAST('start' AS TIMESTAMP))) / 7 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(month, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(MONTH, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(year FROM AGE(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP))) * 12 + EXTRACT(month FROM AGE(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP))) AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(quarter, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(QUARTER, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(year FROM AGE(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP))) * 4 + EXTRACT(month FROM AGE(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP))) / 3 AS BIGINT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(year, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(YEAR, 'start', 'end')",
                "postgres": "SELECT CAST(EXTRACT(year FROM AGE(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP))) AS BIGINT)",
            },
        )

    def test_add_date(self):
        self.validate_all(
            "SELECT DATEADD(year, 1, '2020-01-01')",
            write={
                "tsql": "SELECT DATEADD(YEAR, 1, '2020-01-01')",
                "databricks": "SELECT DATEADD(YEAR, 1, '2020-01-01')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF('end', 'start')",
            write={"databricks": "SELECT DATEDIFF(DAY, 'start', 'end')"},
        )
        self.validate_all(
            "SELECT DATE_ADD('2020-01-01', 1)",
            write={
                "tsql": "SELECT DATEADD(DAY, 1, '2020-01-01')",
                "databricks": "SELECT DATEADD(DAY, 1, '2020-01-01')",
            },
        )

    def test_without_as(self):
        self.validate_all(
            "CREATE TABLE x (SELECT 1)",
            write={
                "databricks": "CREATE TABLE x AS (SELECT 1)",
            },
        )

        self.validate_all(
            "WITH x (select 1) SELECT * FROM x",
            write={
                "databricks": "WITH x AS (SELECT 1) SELECT * FROM x",
            },
        )

    def test_streaming_tables(self):
        self.validate_identity(
            "CREATE STREAMING TABLE raw_data AS SELECT * FROM STREAM READ_FILES('abfss://container@storageAccount.dfs.core.windows.net/base/path')"
        )
        self.validate_identity(
            "CREATE OR REFRESH STREAMING TABLE csv_data (id INT, ts TIMESTAMP, event STRING) AS SELECT * FROM STREAM READ_FILES('s3://bucket/path', format => 'csv', schema => 'id int, ts timestamp, event string')"
        )

    def test_grant(self):
        self.validate_identity("GRANT CREATE ON SCHEMA my_schema TO `alf@melmak.et`")
        self.validate_identity("GRANT SELECT ON TABLE sample_data TO `alf@melmak.et`")
        self.validate_identity("GRANT ALL PRIVILEGES ON TABLE forecasts TO finance")
        self.validate_identity("GRANT SELECT ON TABLE t TO `fab9e00e-ca35-11ec-9d64-0242ac120002`")

    def test_revoke(self):
        self.validate_identity("REVOKE CREATE ON SCHEMA my_schema FROM `alf@melmak.et`")
        self.validate_identity("REVOKE SELECT ON TABLE sample_data FROM `alf@melmak.et`")
        self.validate_identity("REVOKE ALL PRIVILEGES ON TABLE forecasts FROM finance")
        self.validate_identity(
            "REVOKE SELECT ON TABLE t FROM `fab9e00e-ca35-11ec-9d64-0242ac120002`"
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl COMPUTE DELTA STATISTICS NOSCAN")
        self.validate_identity("ANALYZE TABLE tbl COMPUTE DELTA STATISTICS FOR ALL COLUMNS")
        self.validate_identity("ANALYZE TABLE tbl COMPUTE DELTA STATISTICS FOR COLUMNS foo, bar")
        self.validate_identity("ANALYZE TABLE ctlg.db.tbl COMPUTE DELTA STATISTICS NOSCAN")
        self.validate_identity("ANALYZE TABLES COMPUTE STATISTICS NOSCAN")
        self.validate_identity("ANALYZE TABLES FROM db COMPUTE STATISTICS")
        self.validate_identity("ANALYZE TABLES IN db COMPUTE STATISTICS")
        self.validate_identity(
            "ANALYZE TABLE ctlg.db.tbl PARTITION(foo = 'foo', bar = 'bar') COMPUTE STATISTICS NOSCAN"
        )

    def test_udf_environment_property(self):
        self.validate_identity(
            """CREATE FUNCTION a() ENVIRONMENT (dependencies = '["foo1==1", "foo2==2"]', environment_version = 'None')"""
        )

    def test_to_char_is_numeric_transpile_to_cast(self):
        # The input SQL simulates a TO_CHAR with is_numeric flag set (from dremio dialect)
        sql = "SELECT TO_CHAR(12345, '#')"
        expression = parse_one(sql, read="dremio")

        to_char_exp = expression.find(exp.ToChar)
        assert to_char_exp is not None
        assert to_char_exp.args.get("is_numeric") is True

        result = transpile(sql, read="dremio", write="databricks")[0]
        assert "CAST(12345 AS STRING)" in result
