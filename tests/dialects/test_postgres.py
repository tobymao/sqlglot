from sqlglot import ParseError, UnsupportedError, exp, transpile
from sqlglot.helper import logger as helper_logger
from tests.dialects.test_dialect import Validator


class TestPostgres(Validator):
    maxDiff = None
    dialect = "postgres"

    def test_postgres(self):
        self.validate_all(
            "x ? y",
            write={
                "": "JSONB_CONTAINS(x, y)",
                "postgres": "x ? y",
            },
        )

        self.validate_identity("SHA384(x)")
        self.validate_identity("1.x", "1. AS x")
        self.validate_identity("|/ x", "SQRT(x)")
        self.validate_identity("||/ x", "CBRT(x)")

        expr = self.parse_one("SELECT * FROM r CROSS JOIN LATERAL UNNEST(ARRAY[1]) AS s(location)")
        unnest = expr.args["joins"][0].this.this
        unnest.assert_is(exp.Unnest)

        alter_table_only = """ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE NO ACTION ON UPDATE NO ACTION"""
        expr = self.parse_one(alter_table_only)

        self.assertIsInstance(expr, exp.Alter)
        self.assertEqual(expr.sql(dialect="postgres"), alter_table_only)

        self.validate_identity("STRING_TO_ARRAY('xx~^~yy~^~zz', '~^~', 'yy')")
        self.validate_identity("SELECT x FROM t WHERE CAST($1 AS TEXT) = 'ok'")
        self.validate_identity("SELECT * FROM t TABLESAMPLE SYSTEM (50) REPEATABLE (55)")
        self.validate_identity("x @@ y")
        self.validate_identity("CAST(x AS MONEY)")
        self.validate_identity("CAST(x AS INT4RANGE)")
        self.validate_identity("CAST(x AS INT4MULTIRANGE)")
        self.validate_identity("CAST(x AS INT8RANGE)")
        self.validate_identity("CAST(x AS INT8MULTIRANGE)")
        self.validate_identity("CAST(x AS NUMRANGE)")
        self.validate_identity("CAST(x AS NUMMULTIRANGE)")
        self.validate_identity("CAST(x AS TSRANGE)")
        self.validate_identity("CAST(x AS TSMULTIRANGE)")
        self.validate_identity("CAST(x AS TSTZRANGE)")
        self.validate_identity("CAST(x AS TSTZMULTIRANGE)")
        self.validate_identity("CAST(x AS DATERANGE)")
        self.validate_identity("CAST(x AS DATEMULTIRANGE)")
        self.validate_identity("x$")
        self.validate_identity("LENGTH(x)")
        self.validate_identity("LENGTH(x, utf8)")
        self.validate_identity("CHAR_LENGTH(x)", "LENGTH(x)")
        self.validate_identity("CHARACTER_LENGTH(x)", "LENGTH(x)")
        self.validate_identity("SELECT ARRAY[1, 2, 3]")
        self.validate_identity("SELECT ARRAY(SELECT 1)")
        self.validate_identity("STRING_AGG(x, y)")
        self.validate_identity("STRING_AGG(x, ',' ORDER BY y)")
        self.validate_identity("STRING_AGG(x, ',' ORDER BY y DESC)")
        self.validate_identity("STRING_AGG(DISTINCT x, ',' ORDER BY y DESC)")
        self.validate_identity("SELECT CASE WHEN SUBSTRING('abcdefg') IN ('ab') THEN 1 ELSE 0 END")
        self.validate_identity("COMMENT ON TABLE mytable IS 'this'")
        self.validate_identity("COMMENT ON MATERIALIZED VIEW my_view IS 'this'")
        self.validate_identity("SELECT e'\\xDEADBEEF'")
        self.validate_identity("SELECT CAST(e'\\176' AS BYTEA)")
        self.validate_identity("SELECT * FROM x WHERE SUBSTRING('Thomas' FROM '...$') IN ('mas')")
        self.validate_identity("SELECT TRIM(' X' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(LEADING 'bla' FROM ' XXX ' COLLATE utf8_bin)")
        self.validate_identity("""SELECT * FROM JSON_TO_RECORDSET(z) AS y("rank" INT)""")
        self.validate_identity("x ~ 'y'")
        self.validate_identity("x ~* 'y'")
        self.validate_identity("SELECT * FROM r CROSS JOIN LATERAL UNNEST(ARRAY[1]) AS s(location)")
        self.validate_identity("CAST(1 AS DECIMAL) / CAST(2 AS DECIMAL) * -100")
        self.validate_identity("EXEC AS myfunc @id = 123", check_command_warning=True)
        self.validate_identity("SELECT CURRENT_SCHEMA")
        self.validate_identity("SELECT CURRENT_USER")
        self.validate_identity("SELECT * FROM ONLY t1")
        self.validate_identity(
            "SELECT id, name FROM xml_data AS t, XMLTABLE('/root/user' PASSING t.xml COLUMNS id INT PATH '@id', name TEXT PATH 'name/text()') AS x"
        )
        self.validate_identity(
            "SELECT id, value FROM xml_content AS t, XMLTABLE(XMLNAMESPACES('http://example.com/ns1' AS ns1, 'http://example.com/ns2' AS ns2), '/root/data' PASSING t.xml COLUMNS id INT PATH '@ns1:id', value TEXT PATH 'ns2:value/text()') AS x"
        )
        self.validate_identity(
            "SELECT * FROM t WHERE some_column >= CURRENT_DATE + INTERVAL '1 day 1 hour' AND some_another_column IS TRUE"
        )
        self.validate_identity(
            """UPDATE "x" SET "y" = CAST('0 days 60.000000 seconds' AS INTERVAL) WHERE "x"."id" IN (2, 3)"""
        )
        self.validate_identity(
            "WITH t1 AS MATERIALIZED (SELECT 1), t2 AS NOT MATERIALIZED (SELECT 2) SELECT * FROM t1, t2"
        )
        self.validate_identity(
            """LAST_VALUE("col1") OVER (ORDER BY "col2" RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND '1 month' FOLLOWING)"""
        )
        self.validate_identity(
            """ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE CASCADE"""
        )
        self.validate_identity(
            """ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE RESTRICT"""
        )
        self.validate_identity(
            "SELECT * FROM JSON_ARRAY_ELEMENTS('[1,true, [2,false]]') WITH ORDINALITY"
        )
        self.validate_identity(
            "SELECT * FROM JSON_ARRAY_ELEMENTS('[1,true, [2,false]]') WITH ORDINALITY AS kv_json"
        )
        self.validate_identity(
            "SELECT * FROM JSON_ARRAY_ELEMENTS('[1,true, [2,false]]') WITH ORDINALITY AS kv_json(a, b)"
        )
        self.validate_identity(
            "SELECT SUM(x) OVER a, SUM(y) OVER b FROM c WINDOW a AS (PARTITION BY d), b AS (PARTITION BY e)"
        )
        self.validate_identity(
            "SELECT CASE WHEN SUBSTRING('abcdefg' FROM 1) IN ('ab') THEN 1 ELSE 0 END"
        )
        self.validate_identity(
            "SELECT CASE WHEN SUBSTRING('abcdefg' FROM 1 FOR 2) IN ('ab') THEN 1 ELSE 0 END"
        )
        self.validate_identity(
            'SELECT * FROM "x" WHERE SUBSTRING("x"."foo" FROM 1 FOR 2) IN (\'mas\')'
        )
        self.validate_identity(
            "SELECT * FROM x WHERE SUBSTRING('Thomas' FROM '%#\"o_a#\"_' FOR '#') IN ('mas')"
        )
        self.validate_identity(
            "SELECT SUBSTRING('bla' + 'foo' || 'bar' FROM 3 - 1 + 5 FOR 4 + SOME_FUNC(arg1, arg2))"
        )
        self.validate_identity(
            "SELECT TO_TIMESTAMP(1284352323.5), TO_TIMESTAMP('05 Dec 2000', 'DD Mon YYYY')"
        )
        self.validate_identity(
            "SELECT * FROM foo, LATERAL (SELECT * FROM bar WHERE bar.id = foo.bar_id) AS ss"
        )
        self.validate_identity(
            "SELECT c.oid, n.nspname, c.relname "
            "FROM pg_catalog.pg_class AS c "
            "LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE c.relname OPERATOR(pg_catalog.~) '^(courses)$' COLLATE pg_catalog.default AND "
            "pg_catalog.PG_TABLE_IS_VISIBLE(c.oid) "
            "ORDER BY 2, 3"
        )
        self.validate_identity(
            "SELECT ARRAY[1, 2, 3] <@ ARRAY[1, 2]",
            "SELECT ARRAY[1, 2] @> ARRAY[1, 2, 3]",
        )
        self.validate_identity(
            "SELECT DATE_PART('isodow'::varchar(6), current_date)",
            "SELECT EXTRACT(CAST('isodow' AS VARCHAR(6)) FROM CURRENT_DATE)",
        )
        self.validate_identity(
            "END WORK AND NO CHAIN",
            "COMMIT AND NO CHAIN",
        )
        self.validate_identity(
            "END AND CHAIN",
            "COMMIT AND CHAIN",
        )
        self.validate_identity(
            """x ? 'x'""",
            "x ? 'x'",
        )
        self.validate_identity(
            "SELECT $$a$$",
            "SELECT 'a'",
        )
        self.validate_identity(
            "SELECT $$Dianne's horse$$",
            "SELECT 'Dianne''s horse'",
        )
        self.validate_identity(
            "SELECT $$The price is $9.95$$ AS msg",
            "SELECT 'The price is $9.95' AS msg",
        )
        self.validate_identity(
            "COMMENT ON TABLE mytable IS $$doc this$$", "COMMENT ON TABLE mytable IS 'doc this'"
        )
        self.validate_identity(
            "UPDATE MYTABLE T1 SET T1.COL = 13",
            "UPDATE MYTABLE AS T1 SET T1.COL = 13",
        )
        self.validate_identity(
            "x !~ 'y'",
            "NOT x ~ 'y'",
        )
        self.validate_identity(
            "x !~* 'y'",
            "NOT x ~* 'y'",
        )

        self.validate_identity(
            "x ~~ 'y'",
            "x LIKE 'y'",
        )
        self.validate_identity(
            "x ~~* 'y'",
            "x ILIKE 'y'",
        )
        self.validate_identity(
            "x !~~ 'y'",
            "NOT x LIKE 'y'",
        )
        self.validate_identity(
            "x !~~* 'y'",
            "NOT x ILIKE 'y'",
        )
        self.validate_identity(
            "'45 days'::interval day",
            "CAST('45 days' AS INTERVAL DAY)",
        )
        self.validate_identity(
            "'x' 'y' 'z'",
            "CONCAT('x', 'y', 'z')",
        )
        self.validate_identity(
            "x::cstring",
            "CAST(x AS CSTRING)",
        )
        self.validate_identity(
            "x::oid",
            "CAST(x AS OID)",
        )
        self.validate_identity(
            "x::regclass",
            "CAST(x AS REGCLASS)",
        )
        self.validate_identity(
            "x::regcollation",
            "CAST(x AS REGCOLLATION)",
        )
        self.validate_identity(
            "x::regconfig",
            "CAST(x AS REGCONFIG)",
        )
        self.validate_identity(
            "x::regdictionary",
            "CAST(x AS REGDICTIONARY)",
        )
        self.validate_identity(
            "x::regnamespace",
            "CAST(x AS REGNAMESPACE)",
        )
        self.validate_identity(
            "x::regoper",
            "CAST(x AS REGOPER)",
        )
        self.validate_identity(
            "x::regoperator",
            "CAST(x AS REGOPERATOR)",
        )
        self.validate_identity(
            "x::regproc",
            "CAST(x AS REGPROC)",
        )
        self.validate_identity(
            "x::regprocedure",
            "CAST(x AS REGPROCEDURE)",
        )
        self.validate_identity(
            "x::regrole",
            "CAST(x AS REGROLE)",
        )
        self.validate_identity(
            "x::regtype",
            "CAST(x AS REGTYPE)",
        )
        self.validate_identity(
            "123::CHARACTER VARYING",
            "CAST(123 AS VARCHAR)",
        )
        self.validate_identity(
            "TO_TIMESTAMP(123::DOUBLE PRECISION)",
            "TO_TIMESTAMP(CAST(123 AS DOUBLE PRECISION))",
        )
        self.validate_identity(
            "SELECT to_timestamp(123)::time without time zone",
            "SELECT CAST(TO_TIMESTAMP(123) AS TIME)",
        )
        self.validate_identity(
            "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS 1 PRECEDING)",
            "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        )
        self.validate_identity(
            "SELECT SUBSTRING(2022::CHAR(4) || LPAD(3::CHAR(2), 2, '0') FROM 3 FOR 4)",
            "SELECT SUBSTRING(CAST(2022 AS CHAR(4)) || LPAD(CAST(3 AS CHAR(2)), 2, '0') FROM 3 FOR 4)",
        )
        self.validate_identity(
            "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) pname ON TRUE WHERE pname IS NULL",
            "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) AS pname ON TRUE WHERE pname IS NULL",
        )
        self.validate_identity(
            "SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) v1, LATERAL VERTICES(p2.poly) v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id",
            "SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) AS v1, LATERAL VERTICES(p2.poly) AS v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id",
        )
        self.validate_identity(
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE deleted NOTNULL",
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL",
        )
        self.validate_identity(
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted ISNULL",
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL",
        )
        self.validate_identity(
            """'{"x": {"y": 1}}'::json->'x'->'y'""",
            """CAST('{"x": {"y": 1}}' AS JSON) -> 'x' -> 'y'""",
        )
        self.validate_identity(
            """'[1,2,3]'::json->>2""",
            "CAST('[1,2,3]' AS JSON) ->> 2",
        )
        self.validate_identity(
            """'{"a":1,"b":2}'::json->>'b'""",
            """CAST('{"a":1,"b":2}' AS JSON) ->> 'b'""",
        )
        self.validate_identity(
            """'{"a":[1,2,3],"b":[4,5,6]}'::json#>'{a,2}'""",
            """CAST('{"a":[1,2,3],"b":[4,5,6]}' AS JSON) #> '{a,2}'""",
        )
        self.validate_identity(
            """'{"a":[1,2,3],"b":[4,5,6]}'::json#>>'{a,2}'""",
            """CAST('{"a":[1,2,3],"b":[4,5,6]}' AS JSON) #>> '{a,2}'""",
        )
        self.validate_identity(
            "'[1,2,3]'::json->2",
            "CAST('[1,2,3]' AS JSON) -> 2",
        )
        self.validate_identity(
            """SELECT JSON_ARRAY_ELEMENTS((foo->'sections')::JSON) AS sections""",
            """SELECT JSON_ARRAY_ELEMENTS(CAST((foo -> 'sections') AS JSON)) AS sections""",
        )
        self.validate_identity(
            "MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET x.a = y.b WHEN NOT MATCHED THEN INSERT (a, b) VALUES (y.a, y.b)",
            "MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b WHEN NOT MATCHED THEN INSERT (a, b) VALUES (y.a, y.b)",
        )
        self.validate_identity(
            "SELECT * FROM t1*",
            "SELECT * FROM t1",
        )
        self.validate_identity(
            "SELECT SUBSTRING('afafa' for 1)",
            "SELECT SUBSTRING('afafa' FROM 1 FOR 1)",
        )
        self.validate_identity(
            "CAST(x AS INT8)",
            "CAST(x AS BIGINT)",
        )
        self.validate_identity(
            """
            WITH
              json_data AS (SELECT '{"field_id": [1, 2, 3]}'::JSON AS data),
              field_ids AS (SELECT 'field_id' AS field_id)

            SELECT
                JSON_ARRAY_ELEMENTS(json_data.data -> field_ids.field_id) AS element
            FROM json_data, field_ids
            """,
            """WITH json_data AS (
  SELECT
    CAST('{"field_id": [1, 2, 3]}' AS JSON) AS data
), field_ids AS (
  SELECT
    'field_id' AS field_id
)
SELECT
  JSON_ARRAY_ELEMENTS(JSON_EXTRACT_PATH(json_data.data, field_ids.field_id)) AS element
FROM json_data, field_ids""",
            pretty=True,
        )

        self.validate_all(
            "SELECT ARRAY[]::INT[] AS foo",
            write={
                "postgres": "SELECT CAST(ARRAY[] AS INT[]) AS foo",
                "duckdb": "SELECT CAST([] AS INT[]) AS foo",
            },
        )
        self.validate_all(
            "STRING_TO_ARRAY('xx~^~yy~^~zz', '~^~', 'yy')",
            read={
                "doris": "SPLIT_BY_STRING('xx~^~yy~^~zz', '~^~', 'yy')",
            },
            write={
                "doris": "SPLIT_BY_STRING('xx~^~yy~^~zz', '~^~', 'yy')",
                "postgres": "STRING_TO_ARRAY('xx~^~yy~^~zz', '~^~', 'yy')",
            },
        )
        self.validate_all(
            "SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]",
            read={
                "duckdb": "SELECT [1, 2, 3] @> [1, 2]",
            },
            write={
                "duckdb": "SELECT [1, 2, 3] @> [1, 2]",
                "mysql": UnsupportedError,
                "postgres": "SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]",
            },
        )
        self.validate_all(
            "SELECT REGEXP_REPLACE('mr .', '[^a-zA-Z]', '', 'g')",
            write={
                "duckdb": "SELECT REGEXP_REPLACE('mr .', '[^a-zA-Z]', '', 'g')",
                "postgres": "SELECT REGEXP_REPLACE('mr .', '[^a-zA-Z]', '', 'g')",
            },
        )
        self.validate_all(
            "CREATE TABLE t (c INT)",
            read={
                "mysql": "CREATE TABLE t (c INT COMMENT 'comment 1') COMMENT = 'comment 2'",
            },
        )
        self.validate_all(
            'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
            write={
                "bigquery": "SELECT * FROM `test_table` ORDER BY RAND() NULLS LAST LIMIT 5",
                "duckdb": 'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
                "postgres": 'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
                "tsql": "SELECT TOP 5 * FROM [test_table] ORDER BY RAND()",
            },
        )
        self.validate_all(
            "SELECT (data -> 'en-US') AS acat FROM my_table",
            write={
                "duckdb": """SELECT (data -> '$."en-US"') AS acat FROM my_table""",
                "postgres": "SELECT (data -> 'en-US') AS acat FROM my_table",
            },
        )
        self.validate_all(
            "SELECT (data ->> 'en-US') AS acat FROM my_table",
            write={
                "duckdb": """SELECT (data ->> '$."en-US"') AS acat FROM my_table""",
                "postgres": "SELECT (data ->> 'en-US') AS acat FROM my_table",
            },
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t",
            read={
                "clickhouse": "SELECT JSONExtractString(x, k1, k2, k3) FROM t",
                "redshift": "SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t",
            },
            write={
                "clickhouse": "SELECT JSONExtractString(x, k1, k2, k3) FROM t",
                "postgres": "SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t",
                "redshift": "SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t",
            },
        )
        self.validate_all(
            "x #> 'y'",
            read={
                "": "JSONB_EXTRACT(x, 'y')",
            },
            write={
                "": "JSONB_EXTRACT(x, 'y')",
                "postgres": "x #> 'y'",
            },
        )
        self.validate_all(
            "x #>> 'y'",
            read={
                "": "JSONB_EXTRACT_SCALAR(x, 'y')",
            },
            write={
                "": "JSONB_EXTRACT_SCALAR(x, 'y')",
                "postgres": "x #>> 'y'",
            },
        )
        self.validate_all(
            "x -> 'y' -> 0 -> 'z'",
            write={
                "": "JSON_EXTRACT(JSON_EXTRACT(JSON_EXTRACT(x, '$.y'), '$[0]'), '$.z')",
                "postgres": "x -> 'y' -> 0 -> 'z'",
            },
        )
        self.validate_all(
            """JSON_EXTRACT_PATH('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}','f4')""",
            write={
                "": """JSON_EXTRACT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4')""",
                "bigquery": """JSON_EXTRACT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4')""",
                "duckdb": """'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}' -> '$.f4'""",
                "mysql": """JSON_EXTRACT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4')""",
                "postgres": """JSON_EXTRACT_PATH('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', 'f4')""",
                "presto": """JSON_EXTRACT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4')""",
                "redshift": """JSON_EXTRACT_PATH_TEXT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', 'f4')""",
                "spark": """GET_JSON_OBJECT('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4')""",
                "sqlite": """'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}' -> '$.f4'""",
                "tsql": """ISNULL(JSON_QUERY('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4'), JSON_VALUE('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', '$.f4'))""",
            },
        )
        self.validate_all(
            """JSON_EXTRACT_PATH_TEXT('{"farm": ["a", "b", "c"]}', 'farm', '0')""",
            read={
                "duckdb": """'{"farm": ["a", "b", "c"]}' ->> '$.farm[0]'""",
                "redshift": """JSON_EXTRACT_PATH_TEXT('{"farm": ["a", "b", "c"]}', 'farm', '0')""",
            },
            write={
                "duckdb": """'{"farm": ["a", "b", "c"]}' ->> '$.farm[0]'""",
                "postgres": """JSON_EXTRACT_PATH_TEXT('{"farm": ["a", "b", "c"]}', 'farm', '0')""",
                "redshift": """JSON_EXTRACT_PATH_TEXT('{"farm": ["a", "b", "c"]}', 'farm', '0')""",
            },
        )
        self.validate_all(
            "JSON_EXTRACT_PATH(x, 'x', 'y', 'z')",
            read={
                "duckdb": "x -> '$.x.y.z'",
                "postgres": "JSON_EXTRACT_PATH(x, 'x', 'y', 'z')",
            },
            write={
                "duckdb": "x -> '$.x.y.z'",
                "redshift": "JSON_EXTRACT_PATH_TEXT(x, 'x', 'y', 'z')",
            },
        )
        self.validate_all(
            "SELECT * FROM t TABLESAMPLE SYSTEM (50)",
            write={
                "postgres": "SELECT * FROM t TABLESAMPLE SYSTEM (50)",
                "redshift": UnsupportedError,
            },
        )
        self.validate_all(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)",
            write={
                "databricks": "SELECT PERCENTILE_APPROX(amount, 0.5)",
                "postgres": "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)",
                "presto": "SELECT APPROX_PERCENTILE(amount, 0.5)",
                "spark": "SELECT PERCENTILE_APPROX(amount, 0.5)",
                "trino": "SELECT APPROX_PERCENTILE(amount, 0.5)",
            },
        )
        self.validate_all(
            "e'x'",
            write={
                "mysql": "x",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('minute', timestamp '2023-01-04 04:05:06.789')",
            write={
                "postgres": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "redshift": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "snowflake": "SELECT DATE_PART(minute, CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('month', date '20220502')",
            write={
                "postgres": "SELECT EXTRACT(month FROM CAST('20220502' AS DATE))",
                "redshift": "SELECT EXTRACT(month FROM CAST('20220502' AS DATE))",
                "snowflake": "SELECT DATE_PART(month, CAST('20220502' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT (DATE '2016-01-10', DATE '2016-02-01') OVERLAPS (DATE '2016-01-20', DATE '2016-02-10')",
            write={
                "postgres": "SELECT (CAST('2016-01-10' AS DATE), CAST('2016-02-01' AS DATE)) OVERLAPS (CAST('2016-01-20' AS DATE), CAST('2016-02-10' AS DATE))",
                "tsql": "SELECT (CAST('2016-01-10' AS DATE), CAST('2016-02-01' AS DATE)) OVERLAPS (CAST('2016-01-20' AS DATE), CAST('2016-02-10' AS DATE))",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('epoch', CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
            read={
                "": "SELECT TIME_TO_UNIX(TIMESTAMP '2023-01-04 04:05:06.789')",
            },
        )
        self.validate_all(
            "x ^ y",
            write={
                "": "POWER(x, y)",
                "postgres": "x ^ y",
            },
        )
        self.validate_all(
            "x # y",
            write={
                "": "x ^ y",
                "postgres": "x # y",
            },
        )
        self.validate_all(
            "SELECT GENERATE_SERIES(1, 5)",
            write={
                "bigquery": UnsupportedError,
                "postgres": "SELECT GENERATE_SERIES(1, 5)",
            },
        )
        self.validate_all(
            "WITH dates AS (SELECT GENERATE_SERIES('2020-01-01'::DATE, '2024-01-01'::DATE, '1 day'::INTERVAL) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC('MONTH', date) AS date FROM dates) SELECT * FROM date_table",
            write={
                "duckdb": "WITH dates AS (SELECT UNNEST(GENERATE_SERIES(CAST('2020-01-01' AS DATE), CAST('2024-01-01' AS DATE), CAST('1 day' AS INTERVAL))) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC('MONTH', date) AS date FROM dates) SELECT * FROM date_table",
                "postgres": "WITH dates AS (SELECT GENERATE_SERIES(CAST('2020-01-01' AS DATE), CAST('2024-01-01' AS DATE), CAST('1 day' AS INTERVAL)) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC('MONTH', date) AS date FROM dates) SELECT * FROM date_table",
            },
        )
        self.validate_all(
            "GENERATE_SERIES(a, b, '  2   days  ')",
            write={
                "postgres": "GENERATE_SERIES(a, b, INTERVAL '2 DAYS')",
                "presto": "UNNEST(SEQUENCE(a, b, INTERVAL '2' DAY))",
                "trino": "UNNEST(SEQUENCE(a, b, INTERVAL '2' DAY))",
            },
        )
        self.validate_all(
            "GENERATE_SERIES('2019-01-01'::TIMESTAMP, NOW(), '1day')",
            write={
                "databricks": "EXPLODE(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL '1' DAY))",
                "hive": "EXPLODE(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL '1' DAY))",
                "postgres": "GENERATE_SERIES(CAST('2019-01-01' AS TIMESTAMP), CURRENT_TIMESTAMP, INTERVAL '1 DAY')",
                "presto": "UNNEST(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL '1' DAY))",
                "spark": "EXPLODE(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL '1' DAY))",
                "spark2": "EXPLODE(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL '1' DAY))",
                "trino": "UNNEST(SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL '1' DAY))",
            },
        )
        self.validate_all(
            "SELECT * FROM GENERATE_SERIES(a, b)",
            read={
                "tsql": "SELECT * FROM GENERATE_SERIES(a, b)",
            },
            write={
                "databricks": "SELECT * FROM EXPLODE(SEQUENCE(a, b))",
                "hive": "SELECT * FROM EXPLODE(SEQUENCE(a, b))",
                "postgres": "SELECT * FROM GENERATE_SERIES(a, b)",
                "presto": "SELECT * FROM UNNEST(SEQUENCE(a, b))",
                "spark": "SELECT * FROM EXPLODE(SEQUENCE(a, b))",
                "spark2": "SELECT * FROM EXPLODE(SEQUENCE(a, b))",
                "trino": "SELECT * FROM UNNEST(SEQUENCE(a, b))",
                "tsql": "SELECT * FROM GENERATE_SERIES(a, b)",
            },
        )
        self.validate_all(
            "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)",
            write={
                "postgres": "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)",
                "presto": "SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4))",
                "trino": "SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4))",
                "tsql": "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)",
            },
        )
        self.validate_all(
            "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s",
            write={
                "postgres": "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s",
                "presto": "SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4)) AS _u(s)",
                "trino": "SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4)) AS _u(s)",
                "tsql": "SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s",
            },
        )
        self.validate_all(
            "SELECT * FROM x FETCH 1 ROW",
            write={
                "postgres": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
                "presto": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
                "hive": "SELECT * FROM x LIMIT 1",
                "spark": "SELECT * FROM x LIMIT 1",
                "sqlite": "SELECT * FROM x LIMIT 1",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
            },
        )
        self.validate_all(
            "SELECT CASE WHEN SUBSTRING('abcdefg' FROM 1 FOR 2) IN ('ab') THEN 1 ELSE 0 END",
            write={
                "hive": "SELECT CASE WHEN SUBSTRING('abcdefg', 1, 2) IN ('ab') THEN 1 ELSE 0 END",
                "spark": "SELECT CASE WHEN SUBSTRING('abcdefg', 1, 2) IN ('ab') THEN 1 ELSE 0 END",
            },
        )
        self.validate_all(
            "SELECT * FROM x WHERE SUBSTRING(col1 FROM 3 + LENGTH(col1) - 10 FOR 10) IN (col2)",
            write={
                "hive": "SELECT * FROM x WHERE SUBSTRING(col1, 3 + LENGTH(col1) - 10, 10) IN (col2)",
                "spark": "SELECT * FROM x WHERE SUBSTRING(col1, 3 + LENGTH(col1) - 10, 10) IN (col2)",
            },
        )
        self.validate_all(
            "SELECT TRIM(BOTH ' XXX ')",
            write={
                "mysql": "SELECT TRIM(' XXX ')",
                "postgres": "SELECT TRIM(' XXX ')",
                "hive": "SELECT TRIM(' XXX ')",
            },
        )
        self.validate_all(
            "TRIM(LEADING FROM ' XXX ')",
            write={
                "mysql": "LTRIM(' XXX ')",
                "postgres": "LTRIM(' XXX ')",
                "hive": "LTRIM(' XXX ')",
                "presto": "LTRIM(' XXX ')",
            },
        )
        self.validate_all(
            "TRIM(TRAILING FROM ' XXX ')",
            write={
                "mysql": "RTRIM(' XXX ')",
                "postgres": "RTRIM(' XXX ')",
                "hive": "RTRIM(' XXX ')",
                "presto": "RTRIM(' XXX ')",
            },
        )
        self.validate_all(
            "TRIM(BOTH 'as' FROM 'as string as')",
            write={
                "postgres": "TRIM(BOTH 'as' FROM 'as string as')",
                "spark": "TRIM(BOTH 'as' FROM 'as string as')",
            },
        )
        self.validate_identity(
            """SELECT TRIM(LEADING ' XXX ' COLLATE "de_DE")""",
            """SELECT LTRIM(' XXX ' COLLATE "de_DE")""",
        )
        self.validate_identity(
            """SELECT TRIM(TRAILING ' XXX ' COLLATE "de_DE")""",
            """SELECT RTRIM(' XXX ' COLLATE "de_DE")""",
        )
        self.validate_identity("LEVENSHTEIN(col1, col2)")
        self.validate_identity("LEVENSHTEIN_LESS_EQUAL(col1, col2, 1)")
        self.validate_identity("LEVENSHTEIN(col1, col2, 1, 2, 3)")
        self.validate_identity("LEVENSHTEIN_LESS_EQUAL(col1, col2, 1, 2, 3, 4)")

        self.validate_all(
            """'{"a":1,"b":2}'::json->'b'""",
            write={
                "postgres": """CAST('{"a":1,"b":2}' AS JSON) -> 'b'""",
                "redshift": """JSON_EXTRACT_PATH_TEXT('{"a":1,"b":2}', 'b')""",
            },
        )
        self.validate_all(
            """merge into x as x using (select id) as y on a = b WHEN matched then update set X."A" = y.b""",
            write={
                "postgres": """MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET "A" = y.b""",
                "trino": """MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET "A" = y.b""",
                "snowflake": """MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET X."A" = y.b""",
            },
        )
        self.validate_all(
            "merge into x as z using (select id) as y on a = b WHEN matched then update set X.a = y.b",
            write={
                "postgres": "MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
                "snowflake": "MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET X.a = y.b",
            },
        )
        self.validate_all(
            "merge into x as z using (select id) as y on a = b WHEN matched then update set Z.a = y.b",
            write={
                "postgres": "MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
                "snowflake": "MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET Z.a = y.b",
            },
        )
        self.validate_all(
            "merge into x using (select id) as y on a = b WHEN matched then update set x.a = y.b",
            write={
                "postgres": "MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
                "snowflake": "MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET x.a = y.b",
            },
        )
        self.validate_all(
            "x / y ^ z",
            write={
                "": "x / POWER(y, z)",
                "postgres": "x / y ^ z",
            },
        )
        self.validate_all(
            "CAST(x AS NAME)",
            read={
                "redshift": "CAST(x AS NAME)",
            },
            write={
                "postgres": "CAST(x AS NAME)",
                "redshift": "CAST(x AS NAME)",
            },
        )
        self.assertIsInstance(self.parse_one("id::UUID"), exp.Cast)

        self.validate_identity(
            "COPY tbl (col1, col2) FROM 'file' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)"
        )
        self.validate_identity(
            "COPY tbl (col1, col2) TO 'file' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)"
        )
        self.validate_identity(
            "COPY (SELECT * FROM t) TO 'file' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)"
        )
        self.validate_identity("cast(a as FLOAT)", "CAST(a AS DOUBLE PRECISION)")
        self.validate_identity("cast(a as FLOAT8)", "CAST(a AS DOUBLE PRECISION)")
        self.validate_identity("cast(a as FLOAT4)", "CAST(a AS REAL)")

        self.validate_all(
            "1 / DIV(4, 2)",
            read={
                "postgres": "1 / DIV(4, 2)",
            },
            write={
                "sqlite": "1 / CAST(CAST(CAST(4 AS REAL) / 2 AS INTEGER) AS REAL)",
                "duckdb": "1 / CAST(4 // 2 AS DECIMAL)",
                "bigquery": "1 / CAST(DIV(4, 2) AS NUMERIC)",
            },
        )
        self.validate_all(
            "CAST(DIV(4, 2) AS DECIMAL(5, 3))",
            read={
                "duckdb": "CAST(4 // 2 AS DECIMAL(5, 3))",
            },
            write={
                "duckdb": "CAST(CAST(4 // 2 AS DECIMAL) AS DECIMAL(5, 3))",
                "postgres": "CAST(DIV(4, 2) AS DECIMAL(5, 3))",
            },
        )

        self.validate_all(
            "SELECT TO_DATE('01/01/2000', 'MM/DD/YYYY')",
            write={
                "duckdb": "SELECT CAST(STRPTIME('01/01/2000', '%m/%d/%Y') AS DATE)",
                "postgres": "SELECT TO_DATE('01/01/2000', 'MM/DD/YYYY')",
            },
        )

        self.validate_identity(
            'SELECT js, js IS JSON AS "json?", js IS JSON VALUE AS "scalar?", js IS JSON SCALAR AS "scalar?", js IS JSON OBJECT AS "object?", js IS JSON ARRAY AS "array?" FROM t'
        )
        self.validate_identity(
            'SELECT js, js IS JSON ARRAY WITH UNIQUE KEYS AS "array w. UK?", js IS JSON ARRAY WITHOUT UNIQUE KEYS AS "array w/o UK?", js IS JSON ARRAY UNIQUE KEYS AS "array w UK 2?" FROM t'
        )
        self.validate_identity(
            "MERGE INTO target_table USING source_table AS source ON target.id = source.id WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN DO NOTHING RETURNING MERGE_ACTION(), *"
        )
        self.validate_identity(
            "SELECT 1 FROM ((VALUES (1)) AS vals(id) LEFT OUTER JOIN tbl ON vals.id = tbl.id)"
        )
        self.validate_identity("SELECT OVERLAY(a PLACING b FROM 1)")
        self.validate_identity("SELECT OVERLAY(a PLACING b FROM 1 FOR 1)")
        self.validate_identity("ARRAY[1, 2, 3] && ARRAY[1, 2]").assert_is(exp.ArrayOverlaps)

        self.validate_all(
            """SELECT JSONB_EXISTS('{"a": [1,2,3]}', 'a')""",
            write={
                "postgres": """SELECT JSONB_EXISTS('{"a": [1,2,3]}', 'a')""",
                "duckdb": """SELECT JSON_EXISTS('{"a": [1,2,3]}', '$.a')""",
            },
        )
        self.validate_all(
            "WITH t AS (SELECT ARRAY[1, 2, 3] AS col) SELECT * FROM t WHERE 1 <= ANY(col) AND 2 = ANY(col)",
            write={
                "postgres": "WITH t AS (SELECT ARRAY[1, 2, 3] AS col) SELECT * FROM t WHERE 1 <= ANY(col) AND 2 = ANY(col)",
                "hive": "WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)",
                "spark2": "WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)",
                "spark": "WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)",
                "databricks": "WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)",
            },
        )

        self.validate_identity(
            "/*+ some comment*/ SELECT b.foo, b.bar FROM baz AS b",
            "/* + some comment */ SELECT b.foo, b.bar FROM baz AS b",
        )

        self.validate_identity(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a) FILTER(WHERE CAST(b AS BOOLEAN)) AS mean_value FROM (VALUES (0, 't')) AS fake_data(a, b)"
        )

        self.validate_all(
            "SELECT JSON_OBJECT_AGG(k, v) FROM t",
            write={
                "postgres": "SELECT JSON_OBJECT_AGG(k, v) FROM t",
                "duckdb": "SELECT JSON_GROUP_OBJECT(k, v) FROM t",
            },
        )

        self.validate_all(
            "SELECT JSONB_OBJECT_AGG(k, v) FROM t",
            write={
                "postgres": "SELECT JSONB_OBJECT_AGG(k, v) FROM t",
                "duckdb": "SELECT JSON_GROUP_OBJECT(k, v) FROM t",
            },
        )

        self.validate_all(
            "SELECT DATE_BIN('30 days', timestamp_col, (SELECT MIN(TIMESTAMP) from table)) FROM table",
            write={
                "postgres": "SELECT DATE_BIN('30 days', timestamp_col, (SELECT MIN(TIMESTAMP) FROM table)) FROM table",
                "duckdb": 'SELECT TIME_BUCKET(\'30 days\', timestamp_col, (SELECT MIN(TIMESTAMP) FROM "table")) FROM "table"',
            },
        )

    def test_ddl(self):
        # Checks that user-defined types are parsed into DataType instead of Identifier
        self.parse_one("CREATE TABLE t (a udt)").this.expressions[0].args["kind"].assert_is(
            exp.DataType
        )

        # Checks that OID is parsed into a DataType (ObjectIdentifier)
        self.assertIsInstance(
            self.parse_one("CREATE TABLE p.t (c oid)").find(exp.DataType), exp.ObjectIdentifier
        )

        expr = self.parse_one("CREATE TABLE t (x INTERVAL day)")
        cdef = expr.find(exp.ColumnDef)
        cdef.args["kind"].assert_is(exp.DataType)
        self.assertEqual(expr.sql(dialect="postgres"), "CREATE TABLE t (x INTERVAL DAY)")

        self.validate_identity('ALTER INDEX "IX_Ratings_Column1" RENAME TO "IX_Ratings_Column2"')
        self.validate_identity('CREATE TABLE x (a TEXT COLLATE "de_DE")')
        self.validate_identity('CREATE TABLE x (a TEXT COLLATE pg_catalog."default")')
        self.validate_identity("CREATE TABLE t (col INT[3][5])")
        self.validate_identity("CREATE TABLE t (col INT[3])")
        self.validate_identity("CREATE INDEX IF NOT EXISTS ON t(c)")
        self.validate_identity("CREATE INDEX et_vid_idx ON et(vid) INCLUDE (fid)")
        self.validate_identity("CREATE INDEX idx_x ON x USING BTREE(x, y) WHERE (NOT y IS NULL)")
        self.validate_identity("CREATE TABLE test (elems JSONB[])")
        self.validate_identity("CREATE TABLE public.y (x TSTZRANGE NOT NULL)")
        self.validate_identity("CREATE TABLE test (foo HSTORE)")
        self.validate_identity("CREATE TABLE test (foo JSONB)")
        self.validate_identity("CREATE TABLE test (foo VARCHAR(64)[])")
        self.validate_identity("CREATE TABLE test (foo INT) PARTITION BY HASH(foo)")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING a")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING a, b")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING *")
        self.validate_identity("UPDATE tbl_name SET foo = 123 RETURNING a")
        self.validate_identity("CREATE TABLE cities_partdef PARTITION OF cities DEFAULT")
        self.validate_identity("CREATE TABLE t (c CHAR(2) UNIQUE NOT NULL) INHERITS (t1)")
        self.validate_identity("CREATE TABLE s.t (c CHAR(2) UNIQUE NOT NULL) INHERITS (s.t1, s.t2)")
        self.validate_identity("CREATE FUNCTION x(INT) RETURNS INT SET search_path = 'public'")
        self.validate_identity("TRUNCATE TABLE t1 CONTINUE IDENTITY")
        self.validate_identity("TRUNCATE TABLE t1 RESTART IDENTITY")
        self.validate_identity("TRUNCATE TABLE t1 CASCADE")
        self.validate_identity("TRUNCATE TABLE t1 RESTRICT")
        self.validate_identity("TRUNCATE TABLE t1 CONTINUE IDENTITY CASCADE")
        self.validate_identity("TRUNCATE TABLE t1 RESTART IDENTITY RESTRICT")
        self.validate_identity("ALTER TABLE t1 SET LOGGED")
        self.validate_identity("ALTER TABLE t1 SET UNLOGGED")
        self.validate_identity("ALTER TABLE t1 SET WITHOUT CLUSTER")
        self.validate_identity("ALTER TABLE t1 SET WITHOUT OIDS")
        self.validate_identity("ALTER TABLE t1 SET ACCESS METHOD method")
        self.validate_identity("ALTER TABLE t1 SET TABLESPACE tablespace")
        self.validate_identity("ALTER TABLE t1 SET (fillfactor = 5, autovacuum_enabled = TRUE)")
        self.validate_identity(
            "INSERT INTO newtable AS t(a, b, c) VALUES (1, 2, 3) ON CONFLICT(c) DO UPDATE SET a = t.a + 1 WHERE t.a < 1"
        )
        self.validate_identity(
            "ALTER TABLE tested_table ADD CONSTRAINT unique_example UNIQUE (column_name) NOT VALID"
        )
        self.validate_identity(
            "CREATE FUNCTION pymax(a INT, b INT) RETURNS INT LANGUAGE plpython3u AS $$\n  if a > b:\n    return a\n  return b\n$$",
        )
        self.validate_identity(
            "CREATE TABLE t (vid INT NOT NULL, CONSTRAINT ht_vid_nid_fid_idx EXCLUDE (INT4RANGE(vid, nid) WITH &&, INT4RANGE(fid, fid, '[]') WITH &&))"
        )
        self.validate_identity(
            "CREATE TABLE t (i INT, PRIMARY KEY (i), EXCLUDE USING gist(col varchar_pattern_ops DESC NULLS LAST WITH &&) WITH (sp1=1, sp2=2))"
        )
        self.validate_identity(
            "CREATE TABLE t (i INT, EXCLUDE USING btree(INT4RANGE(vid, nid, '[]') ASC NULLS FIRST WITH &&) INCLUDE (col1, col2))"
        )
        self.validate_identity(
            "CREATE TABLE t (i INT, EXCLUDE USING gin(col1 WITH &&, col2 WITH ||) USING INDEX TABLESPACE tablespace WHERE (id > 5))"
        )
        self.validate_identity(
            "CREATE TABLE A (LIKE B INCLUDING CONSTRAINT INCLUDING COMPRESSION EXCLUDING COMMENTS)"
        )
        self.validate_identity(
            "CREATE TABLE cust_part3 PARTITION OF customers FOR VALUES WITH (MODULUS 3, REMAINDER 2)"
        )
        self.validate_identity(
            "CREATE TABLE measurement_y2016m07 PARTITION OF measurement (unitsales DEFAULT 0) FOR VALUES FROM ('2016-07-01') TO ('2016-08-01')"
        )
        self.validate_identity(
            "CREATE TABLE measurement_ym_older PARTITION OF measurement_year_month FOR VALUES FROM (MINVALUE, MINVALUE) TO (2016, 11)"
        )
        self.validate_identity(
            "CREATE TABLE measurement_ym_y2016m11 PARTITION OF measurement_year_month FOR VALUES FROM (2016, 11) TO (2016, 12)"
        )
        self.validate_identity(
            "CREATE TABLE cities_ab PARTITION OF cities (CONSTRAINT city_id_nonzero CHECK (city_id <> 0)) FOR VALUES IN ('a', 'b')"
        )
        self.validate_identity(
            "CREATE TABLE cities_ab PARTITION OF cities (CONSTRAINT city_id_nonzero CHECK (city_id <> 0)) FOR VALUES IN ('a', 'b') PARTITION BY RANGE(population)"
        )
        self.validate_identity(
            "CREATE INDEX foo ON bar.baz USING btree(col1 varchar_pattern_ops ASC, col2)"
        )
        self.validate_identity(
            "CREATE INDEX index_issues_on_title_trigram ON public.issues USING gin(title public.gin_trgm_ops)"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT(id) DO NOTHING RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT(id) DO UPDATE SET x.id = 1 RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT(id) DO UPDATE SET x.id = excluded.id RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT ON CONSTRAINT pkey DO NOTHING RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT ON CONSTRAINT pkey DO UPDATE SET x.id = 1 RETURNING *"
        )
        self.validate_identity(
            "DELETE FROM event USING sales AS s WHERE event.eventid = s.eventid RETURNING a"
        )
        self.validate_identity(
            "WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp"
        )
        self.validate_identity(
            "CREATE TABLE test (x TIMESTAMP WITHOUT TIME ZONE[][])",
            "CREATE TABLE test (x TIMESTAMP[][])",
        )
        self.validate_identity(
            "CREATE FUNCTION add(integer, integer) RETURNS INT LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT AS 'select $1 + $2;'",
        )
        self.validate_identity(
            "CREATE FUNCTION add(integer, integer) RETURNS INT LANGUAGE SQL IMMUTABLE STRICT AS 'select $1 + $2;'"
        )
        self.validate_identity(
            "CREATE FUNCTION add(INT, INT) RETURNS INT SET search_path TO 'public' AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE FUNCTION x(INT) RETURNS INT SET foo FROM CURRENT",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE FUNCTION add(integer, integer) RETURNS integer AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE CALLED ON NULL INPUT",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE CONSTRAINT TRIGGER my_trigger AFTER INSERT OR DELETE OR UPDATE OF col_a, col_b ON public.my_table DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION do_sth()",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE UNLOGGED TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp"
        )
        self.validate_identity(
            "CREATE TABLE t (col integer ARRAY[3])",
            "CREATE TABLE t (col INT[3])",
        )
        self.validate_identity(
            "CREATE TABLE t (col integer ARRAY)",
            "CREATE TABLE t (col INT[])",
        )
        self.validate_identity(
            "CREATE FUNCTION x(INT) RETURNS INT SET search_path TO 'public'",
            "CREATE FUNCTION x(INT) RETURNS INT SET search_path = 'public'",
        )
        self.validate_identity(
            "CREATE TABLE test (x TIMESTAMP WITHOUT TIME ZONE[][])",
            "CREATE TABLE test (x TIMESTAMP[][])",
        )
        self.validate_identity(
            "CREATE OR REPLACE FUNCTION function_name (input_a character varying DEFAULT NULL::character varying)",
            "CREATE OR REPLACE FUNCTION function_name(input_a VARCHAR DEFAULT CAST(NULL AS VARCHAR))",
        )
        self.validate_identity(
            "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)",
            "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)",
        )
        self.validate_identity(
            "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)",
            "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)",
        )
        self.validate_identity(
            "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))",
            "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))",
        )
        self.validate_identity(
            "CREATE TABLE products ("
            "product_no INT UNIQUE,"
            " name TEXT,"
            " price DECIMAL CHECK (price > 0),"
            " discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),"
            " CHECK (product_no > 1),"
            " CONSTRAINT valid_discount CHECK (price > discounted_price))"
        )
        self.validate_identity(
            """
            CREATE INDEX index_ci_builds_on_commit_id_and_artifacts_expireatandidpartial
            ON public.ci_builds
            USING btree (commit_id, artifacts_expire_at, id)
            WHERE (
                ((type)::text = 'Ci::Build'::text)
                AND ((retried = false) OR (retried IS NULL))
                AND ((name)::text = ANY (ARRAY[
                    ('sast'::character varying)::text,
                    ('dependency_scanning'::character varying)::text,
                    ('sast:container'::character varying)::text,
                    ('container_scanning'::character varying)::text,
                    ('dast'::character varying)::text
                ]))
            )
            """,
            "CREATE INDEX index_ci_builds_on_commit_id_and_artifacts_expireatandidpartial ON public.ci_builds USING btree(commit_id, artifacts_expire_at, id) WHERE ((CAST((type) AS TEXT) = CAST('Ci::Build' AS TEXT)) AND ((retried = FALSE) OR (retried IS NULL)) AND (CAST((name) AS TEXT) = ANY(ARRAY[CAST((CAST('sast' AS VARCHAR)) AS TEXT), CAST((CAST('dependency_scanning' AS VARCHAR)) AS TEXT), CAST((CAST('sast:container' AS VARCHAR)) AS TEXT), CAST((CAST('container_scanning' AS VARCHAR)) AS TEXT), CAST((CAST('dast' AS VARCHAR)) AS TEXT)])))",
        )
        self.validate_identity(
            "CREATE INDEX index_ci_pipelines_on_project_idandrefandiddesc ON public.ci_pipelines USING btree(project_id, ref, id DESC)"
        )
        self.validate_identity(
            "TRUNCATE TABLE ONLY t1, t2*, ONLY t3, t4, t5* RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE ONLY t1, t2, ONLY t3, t4, t5 RESTART IDENTITY CASCADE",
        )

        self.validate_all(
            "CREATE TABLE x (a UUID, b BYTEA)",
            write={
                "duckdb": "CREATE TABLE x (a UUID, b BLOB)",
                "presto": "CREATE TABLE x (a UUID, b VARBINARY)",
                "hive": "CREATE TABLE x (a UUID, b BINARY)",
                "spark": "CREATE TABLE x (a STRING, b BINARY)",
                "tsql": "CREATE TABLE x (a UNIQUEIDENTIFIER, b VARBINARY)",
            },
        )

        self.validate_identity("CREATE TABLE tbl (col INT UNIQUE NULLS NOT DISTINCT DEFAULT 9.99)")
        self.validate_identity("CREATE TABLE tbl (col UUID UNIQUE DEFAULT GEN_RANDOM_UUID())")
        self.validate_identity("CREATE TABLE tbl (col UUID, UNIQUE NULLS NOT DISTINCT (col))")
        self.validate_identity("CREATE TABLE tbl (col_a INT GENERATED ALWAYS AS (1 + 2) STORED)")

        self.validate_identity("CREATE INDEX CONCURRENTLY ix_table_id ON tbl USING btree(id)")
        self.validate_identity(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree(id)"
        )
        self.validate_identity("DROP INDEX ix_table_id")
        self.validate_identity("DROP INDEX IF EXISTS ix_table_id")
        self.validate_identity("DROP INDEX CONCURRENTLY ix_table_id")
        self.validate_identity("DROP INDEX CONCURRENTLY IF EXISTS ix_table_id")

        self.validate_identity(
            """
        CREATE TABLE IF NOT EXISTS public.rental
        (
            inventory_id INT NOT NULL,
            CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id)
                REFERENCES public.customer (customer_id) MATCH FULL
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id)
                REFERENCES public.inventory (inventory_id) MATCH PARTIAL
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            CONSTRAINT rental_staff_id_fkey FOREIGN KEY (staff_id)
                REFERENCES public.staff (staff_id) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            INITIALLY IMMEDIATE
        )
        """,
            "CREATE TABLE IF NOT EXISTS public.rental (inventory_id INT NOT NULL, CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customer (customer_id) MATCH FULL ON UPDATE CASCADE ON DELETE RESTRICT, CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id) REFERENCES public.inventory (inventory_id) MATCH PARTIAL ON UPDATE CASCADE ON DELETE RESTRICT, CONSTRAINT rental_staff_id_fkey FOREIGN KEY (staff_id) REFERENCES public.staff (staff_id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE RESTRICT, INITIALLY IMMEDIATE)",
        )

        with self.assertRaises(ParseError):
            transpile("CREATE TABLE products (price DECIMAL CHECK price > 0)", read="postgres")
        with self.assertRaises(ParseError):
            transpile(
                "CREATE TABLE products (price DECIMAL, CHECK price > 1)",
                read="postgres",
            )

    def test_unnest(self):
        self.validate_identity(
            "SELECT * FROM UNNEST(ARRAY[1, 2], ARRAY['foo', 'bar', 'baz']) AS x(a, b)"
        )

        self.validate_all(
            "SELECT UNNEST(c) FROM t",
            write={
                "hive": "SELECT EXPLODE(c) FROM t",
                "postgres": "SELECT UNNEST(c) FROM t",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM t CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(c)))) AS _u(pos) CROSS JOIN UNNEST(c) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(c) AND _u_2.pos_2 = CARDINALITY(c))",
            },
        )
        self.validate_all(
            "SELECT UNNEST(ARRAY[1])",
            write={
                "hive": "SELECT EXPLODE(ARRAY(1))",
                "postgres": "SELECT UNNEST(ARRAY[1])",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1]) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1]))",
            },
        )

    def test_array_offset(self):
        with self.assertLogs(helper_logger) as cm:
            self.validate_all(
                "SELECT col[1]",
                write={
                    "bigquery": "SELECT col[0]",
                    "duckdb": "SELECT col[1]",
                    "hive": "SELECT col[0]",
                    "postgres": "SELECT col[1]",
                    "presto": "SELECT col[1]",
                },
            )

            self.assertEqual(
                cm.output,
                [
                    "INFO:sqlglot:Applying array index offset (-1)",
                    "INFO:sqlglot:Applying array index offset (1)",
                    "INFO:sqlglot:Applying array index offset (1)",
                    "INFO:sqlglot:Applying array index offset (1)",
                ],
            )

    def test_operator(self):
        expr = self.parse_one("1 OPERATOR(+) 2 OPERATOR(*) 3")

        expr.left.assert_is(exp.Operator)
        expr.left.left.assert_is(exp.Literal)
        expr.left.right.assert_is(exp.Literal)
        expr.right.assert_is(exp.Literal)
        self.assertEqual(expr.sql(dialect="postgres"), "1 OPERATOR(+) 2 OPERATOR(*) 3")

        self.validate_identity("SELECT operator FROM t")
        self.validate_identity("SELECT 1 OPERATOR(+) 2")
        self.validate_identity("SELECT 1 OPERATOR(+) /* foo */ 2")
        self.validate_identity("SELECT 1 OPERATOR(pg_catalog.+) 2")

    def test_bool_or(self):
        self.validate_identity(
            "SELECT a, LOGICAL_OR(b) FROM table GROUP BY a",
            "SELECT a, BOOL_OR(b) FROM table GROUP BY a",
        )

    def test_string_concat(self):
        self.validate_identity("SELECT CONCAT('abcde', 2, NULL, 22)")

        self.validate_all(
            "CONCAT(a, b)",
            write={
                "": "CONCAT(COALESCE(a, ''), COALESCE(b, ''))",
                "clickhouse": "CONCAT(COALESCE(a, ''), COALESCE(b, ''))",
                "duckdb": "CONCAT(a, b)",
                "postgres": "CONCAT(a, b)",
                "presto": "CONCAT(COALESCE(CAST(a AS VARCHAR), ''), COALESCE(CAST(b AS VARCHAR), ''))",
            },
        )
        self.validate_all(
            "a || b",
            write={
                "": "a || b",
                "clickhouse": "a || b",
                "duckdb": "a || b",
                "postgres": "a || b",
                "presto": "CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR))",
            },
        )

    def test_variance(self):
        self.validate_identity(
            "VAR_SAMP(x)",
            "VAR_SAMP(x)",
        )
        self.validate_identity(
            "VAR_POP(x)",
            "VAR_POP(x)",
        )
        self.validate_identity(
            "VARIANCE(x)",
            "VAR_SAMP(x)",
        )

        self.validate_all(
            "VAR_POP(x)",
            read={
                "": "VARIANCE_POP(x)",
            },
            write={
                "postgres": "VAR_POP(x)",
            },
        )

    def test_regexp_binary(self):
        """See https://github.com/tobymao/sqlglot/pull/2404 for details."""
        self.assertIsInstance(self.parse_one("'thomas' ~ '.*thomas.*'"), exp.Binary)
        self.assertIsInstance(self.parse_one("'thomas' ~* '.*thomas.*'"), exp.Binary)

    def test_unnest_json_array(self):
        trino_input = """
            WITH t(boxcrate) AS (
              SELECT JSON '[{"boxes": [{"name": "f1", "type": "plant", "color": "red"}]}]'
            )
            SELECT
              JSON_EXTRACT_SCALAR(boxes,'$.name')  AS name,
              JSON_EXTRACT_SCALAR(boxes,'$.type')  AS type,
              JSON_EXTRACT_SCALAR(boxes,'$.color') AS color
            FROM t
            CROSS JOIN UNNEST(CAST(boxcrate AS array(json))) AS x(tbox)
            CROSS JOIN UNNEST(CAST(json_extract(tbox, '$.boxes') AS array(json))) AS y(boxes)
        """

        expected_postgres = """WITH t(boxcrate) AS (
  SELECT
    CAST('[{"boxes": [{"name": "f1", "type": "plant", "color": "red"}]}]' AS JSON)
)
SELECT
  JSON_EXTRACT_PATH_TEXT(boxes, 'name') AS name,
  JSON_EXTRACT_PATH_TEXT(boxes, 'type') AS type,
  JSON_EXTRACT_PATH_TEXT(boxes, 'color') AS color
FROM t
CROSS JOIN JSON_ARRAY_ELEMENTS(CAST(boxcrate AS JSON)) AS x(tbox)
CROSS JOIN JSON_ARRAY_ELEMENTS(CAST(JSON_EXTRACT_PATH(tbox, 'boxes') AS JSON)) AS y(boxes)"""

        self.validate_all(expected_postgres, read={"trino": trino_input}, pretty=True)

    def test_rows_from(self):
        self.validate_identity("""SELECT * FROM ROWS FROM (FUNC1(col1, col2))""")
        self.validate_identity(
            """SELECT * FROM ROWS FROM (FUNC1(col1) AS alias1("col1" TEXT), FUNC2(col2) AS alias2("col2" INT)) WITH ORDINALITY"""
        )
        self.validate_identity(
            """SELECT * FROM table1, ROWS FROM (FUNC1(col1) AS alias1("col1" TEXT)) WITH ORDINALITY AS alias3("col3" INT, "col4" TEXT)"""
        )

    def test_array_length(self):
        self.validate_identity("SELECT ARRAY_LENGTH(ARRAY[1, 2, 3], 1)")

        self.validate_all(
            "ARRAY_LENGTH(arr, 1)",
            read={
                "bigquery": "ARRAY_LENGTH(arr)",
                "duckdb": "ARRAY_LENGTH(arr)",
                "presto": "CARDINALITY(arr)",
                "drill": "REPEATED_COUNT(arr)",
                "teradata": "CARDINALITY(arr)",
                "hive": "SIZE(arr)",
                "spark2": "SIZE(arr)",
                "spark": "SIZE(arr)",
                "databricks": "SIZE(arr)",
            },
            write={
                "duckdb": "ARRAY_LENGTH(arr, 1)",
                "presto": "CARDINALITY(arr)",
                "teradata": "CARDINALITY(arr)",
                "bigquery": "ARRAY_LENGTH(arr)",
                "drill": "REPEATED_COUNT(arr)",
                "clickhouse": "LENGTH(arr)",
                "hive": "SIZE(arr)",
                "spark2": "SIZE(arr)",
                "spark": "SIZE(arr)",
                "databricks": "SIZE(arr)",
            },
        )

        self.validate_all(
            "ARRAY_LENGTH(arr, foo)",
            write={
                "duckdb": "ARRAY_LENGTH(arr, foo)",
                "hive": UnsupportedError,
                "spark2": UnsupportedError,
                "spark": UnsupportedError,
                "databricks": UnsupportedError,
                "presto": UnsupportedError,
                "teradata": UnsupportedError,
                "bigquery": UnsupportedError,
                "drill": UnsupportedError,
                "clickhouse": UnsupportedError,
            },
        )

    def test_xmlelement(self):
        self.validate_identity("SELECT XMLELEMENT(NAME foo)")
        self.validate_identity("SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES('xyz' AS bar))")
        self.validate_identity("SELECT XMLELEMENT(NAME test, XMLATTRIBUTES(a, b)) FROM test")
        self.validate_identity(
            "SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES(CURRENT_DATE AS bar), 'cont', 'ent')"
        )
        self.validate_identity(
            """SELECT XMLELEMENT(NAME "foo$bar", XMLATTRIBUTES('xyz' AS "a&b"))"""
        )
        self.validate_identity(
            "SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES('xyz' AS bar), XMLELEMENT(NAME abc), XMLCOMMENT('test'), XMLELEMENT(NAME xyz))"
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE TBL")
        self.validate_identity("ANALYZE TBL(col1, col2)")
        self.validate_identity("ANALYZE VERBOSE SKIP_LOCKED TBL(col1, col2)")
        self.validate_identity("ANALYZE BUFFER_USAGE_LIMIT 1337 TBL")

    def test_recursive_cte(self):
        for kind in ("BREADTH", "DEPTH"):
            self.validate_identity(
                f"WITH RECURSIVE search_tree(id, link, data) AS (SELECT t.id, t.link, t.data FROM tree AS t UNION ALL SELECT t.id, t.link, t.data FROM tree AS t, search_tree AS st WHERE t.id = st.link) SEARCH {kind} FIRST BY id SET ordercol SELECT * FROM search_tree ORDER BY ordercol"
            )

        self.validate_identity(
            "WITH RECURSIVE search_graph(id, link, data, depth) AS (SELECT g.id, g.link, g.data, 1 FROM graph AS g UNION ALL SELECT g.id, g.link, g.data, sg.depth + 1 FROM graph AS g, search_graph AS sg WHERE g.id = sg.link) CYCLE id SET is_cycle USING path SELECT * FROM search_graph"
        )

    def test_json_extract(self):
        for arrow_op in ("->", "->>"):
            with self.subTest(f"Ensure {arrow_op} operator roundtrips int values as subscripts"):
                self.validate_all(
                    f"SELECT foo {arrow_op} 1",
                    write={
                        "postgres": f"SELECT foo {arrow_op} 1",
                        "duckdb": f"SELECT foo {arrow_op} '$[1]'",
                    },
                )

            with self.subTest(
                f"Ensure {arrow_op} operator roundtrips string values that represent integers as keys"
            ):
                self.validate_all(
                    f"SELECT foo {arrow_op} '12'",
                    write={
                        "postgres": f"SELECT foo {arrow_op} '12'",
                        "clickhouse": "SELECT JSONExtractString(foo, '12')",
                    },
                )
