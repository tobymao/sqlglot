from unittest import mock

from sqlglot import ParseError, exp, parse_one, transpile
from tests.dialects.test_dialect import Validator


class TestPostgres(Validator):
    maxDiff = None
    dialect = "postgres"

    def test_ddl(self):
        self.validate_identity("CREATE TABLE test (elems JSONB[])")
        self.validate_identity("CREATE TABLE public.y (x TSTZRANGE NOT NULL)")
        self.validate_identity("CREATE TABLE test (foo HSTORE)")
        self.validate_identity("CREATE TABLE test (foo JSONB)")
        self.validate_identity("CREATE TABLE test (foo VARCHAR(64)[])")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING a")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING a, b")
        self.validate_identity("INSERT INTO x VALUES (1, 'a', 2.0) RETURNING *")
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT (id) DO NOTHING RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT (id) DO UPDATE SET x.id = 1 RETURNING *"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON CONFLICT (id) DO UPDATE SET x.id = excluded.id RETURNING *"
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
        self.validate_identity("UPDATE tbl_name SET foo = 123 RETURNING a")

        self.validate_all(
            "CREATE OR REPLACE FUNCTION function_name (input_a character varying DEFAULT NULL::character varying)",
            write={
                "postgres": "CREATE OR REPLACE FUNCTION function_name(input_a VARCHAR DEFAULT CAST(NULL AS VARCHAR))",
            },
        )
        self.validate_all(
            "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)",
            write={
                "postgres": "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)"
            },
        )
        self.validate_all(
            "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)",
            write={
                "postgres": "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)"
            },
        )
        self.validate_all(
            "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))",
            write={
                "postgres": "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))"
            },
        )
        self.validate_all(
            "CREATE TABLE products ("
            "product_no INT UNIQUE,"
            " name TEXT,"
            " price DECIMAL CHECK (price > 0),"
            " discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),"
            " CHECK (product_no > 1),"
            " CONSTRAINT valid_discount CHECK (price > discounted_price))",
            write={
                "postgres": "CREATE TABLE products ("
                "product_no INT UNIQUE,"
                " name TEXT,"
                " price DECIMAL CHECK (price > 0),"
                " discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),"
                " CHECK (product_no > 1),"
                " CONSTRAINT valid_discount CHECK (price > discounted_price))"
            },
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
                "presto": "SELECT col FROM t CROSS JOIN UNNEST(c) AS _u(col)",
            },
        )
        self.validate_all(
            "SELECT UNNEST(ARRAY[1])",
            write={
                "hive": "SELECT EXPLODE(ARRAY(1))",
                "postgres": "SELECT UNNEST(ARRAY[1])",
                "presto": "SELECT col FROM UNNEST(ARRAY[1]) AS _u(col)",
            },
        )

    @mock.patch("sqlglot.helper.logger")
    def test_array_offset(self, logger):
        self.validate_all(
            "SELECT col[1]",
            write={
                "hive": "SELECT col[0]",
                "postgres": "SELECT col[1]",
                "presto": "SELECT col[1]",
            },
        )

    def test_postgres(self):
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
        self.validate_identity(
            """LAST_VALUE("col1") OVER (ORDER BY "col2" RANGE BETWEEN INTERVAL '1 day' PRECEDING AND '1 month' FOLLOWING)"""
        )
        self.validate_identity("SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]")
        self.validate_identity("SELECT ARRAY[1, 2, 3] <@ ARRAY[1, 2]")
        self.validate_identity("SELECT ARRAY[1, 2, 3] && ARRAY[1, 2]")
        self.validate_identity("$x")
        self.validate_identity("x$")
        self.validate_identity("SELECT ARRAY[1, 2, 3]")
        self.validate_identity("SELECT ARRAY(SELECT 1)")
        self.validate_identity("SELECT ARRAY_LENGTH(ARRAY[1, 2, 3], 1)")
        self.validate_identity("STRING_AGG(x, y)")
        self.validate_identity("STRING_AGG(x, ',' ORDER BY y)")
        self.validate_identity("STRING_AGG(x, ',' ORDER BY y DESC)")
        self.validate_identity("STRING_AGG(DISTINCT x, ',' ORDER BY y DESC)")
        self.validate_identity("SELECT CASE WHEN SUBSTRING('abcdefg') IN ('ab') THEN 1 ELSE 0 END")
        self.validate_identity("COMMENT ON TABLE mytable IS 'this'")
        self.validate_identity("SELECT e'\\xDEADBEEF'")
        self.validate_identity("SELECT CAST(e'\\176' AS BYTEA)")
        self.validate_identity("SELECT * FROM x WHERE SUBSTRING('Thomas' FROM '...$') IN ('mas')")
        self.validate_identity("SELECT TRIM(' X' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(LEADING 'bla' FROM ' XXX ' COLLATE utf8_bin)")
        self.validate_identity("""SELECT * FROM JSON_TO_RECORDSET(z) AS y("rank" INT)""")
        self.validate_identity("x ~ 'y'")
        self.validate_identity("x ~* 'y'")
        self.validate_identity(
            "SELECT SUM(x) OVER a, SUM(y) OVER b FROM c WINDOW a AS (PARTITION BY d), b AS (PARTITION BY e)"
        )
        self.validate_identity(
            "CREATE TABLE A (LIKE B INCLUDING CONSTRAINT INCLUDING COMPRESSION EXCLUDING COMMENTS)"
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

        self.validate_all(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)",
            write={
                "databricks": "SELECT PERCENTILE_APPROX(amount, 0.5)",
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
            "SELECT DATE_PART('isodow'::varchar(6), current_date)",
            write={
                "postgres": "SELECT EXTRACT(CAST('isodow' AS VARCHAR(6)) FROM CURRENT_DATE)",
            },
        )
        self.validate_all(
            "SELECT DATE_PART('minute', timestamp '2023-01-04 04:05:06.789')",
            write={
                "postgres": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "redshift": "SELECT EXTRACT(minute FROM CAST('2023-01-04 04:05:06.789' AS TIMESTAMP))",
                "snowflake": "SELECT DATE_PART(minute, CAST('2023-01-04 04:05:06.789' AS TIMESTAMPNTZ))",
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
            "GENERATE_SERIES(a, b, '  2   days  ')",
            write={
                "postgres": "GENERATE_SERIES(a, b, INTERVAL '2 days')",
                "presto": "SEQUENCE(a, b, INTERVAL '2' day)",
                "trino": "SEQUENCE(a, b, INTERVAL '2' day)",
            },
        )
        self.validate_all(
            "GENERATE_SERIES('2019-01-01'::TIMESTAMP, NOW(), '1day')",
            write={
                "postgres": "GENERATE_SERIES(CAST('2019-01-01' AS TIMESTAMP), CURRENT_TIMESTAMP, INTERVAL '1 day')",
                "presto": "SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL '1' day)",
                "trino": "SEQUENCE(CAST('2019-01-01' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL '1' day)",
            },
        )
        self.validate_all(
            "GENERATE_SERIES(a, b)",
            write={
                "postgres": "GENERATE_SERIES(a, b)",
                "presto": "SEQUENCE(a, b)",
                "trino": "SEQUENCE(a, b)",
                "tsql": "GENERATE_SERIES(a, b)",
            },
        )
        self.validate_all(
            "GENERATE_SERIES(a, b)",
            read={
                "postgres": "GENERATE_SERIES(a, b)",
                "presto": "SEQUENCE(a, b)",
                "trino": "SEQUENCE(a, b)",
                "tsql": "GENERATE_SERIES(a, b)",
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
            "END WORK AND NO CHAIN",
            write={"postgres": "COMMIT AND NO CHAIN"},
        )
        self.validate_all(
            "END AND CHAIN",
            write={"postgres": "COMMIT AND CHAIN"},
        )
        self.validate_all(
            "CREATE TABLE x (a UUID, b BYTEA)",
            write={
                "duckdb": "CREATE TABLE x (a UUID, b BLOB)",
                "presto": "CREATE TABLE x (a UUID, b VARBINARY)",
                "hive": "CREATE TABLE x (a UUID, b BINARY)",
                "spark": "CREATE TABLE x (a UUID, b BINARY)",
            },
        )
        self.validate_all(
            "123::CHARACTER VARYING",
            write={"postgres": "CAST(123 AS VARCHAR)"},
        )
        self.validate_all(
            "TO_TIMESTAMP(123::DOUBLE PRECISION)",
            write={"postgres": "TO_TIMESTAMP(CAST(123 AS DOUBLE PRECISION))"},
        )
        self.validate_all(
            "SELECT to_timestamp(123)::time without time zone",
            write={"postgres": "SELECT CAST(TO_TIMESTAMP(123) AS TIME)"},
        )
        self.validate_all(
            "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS 1 PRECEDING)",
            write={
                "postgres": "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
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
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
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
            "SELECT SUBSTRING(CAST(2022 AS CHAR(4)) || LPAD(CAST(3 AS CHAR(2)), 2, '0') FROM 3 FOR 4)",
            read={
                "postgres": "SELECT SUBSTRING(2022::CHAR(4) || LPAD(3::CHAR(2), 2, '0') FROM 3 FOR 4)",
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
            "SELECT * FROM foo, LATERAL (SELECT * FROM bar WHERE bar.id = foo.bar_id) AS ss",
            read={
                "postgres": "SELECT * FROM foo, LATERAL (SELECT * FROM bar WHERE bar.id = foo.bar_id) AS ss"
            },
        )
        self.validate_all(
            "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) pname ON TRUE WHERE pname IS NULL",
            write={
                "postgres": "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) AS pname ON TRUE WHERE pname IS NULL",
            },
        )
        self.validate_all(
            "SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) v1, LATERAL VERTICES(p2.poly) v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id",
            write={
                "postgres": "SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) AS v1, LATERAL VERTICES(p2.poly) AS v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id",
            },
        )
        self.validate_all(
            "SELECT * FROM r CROSS JOIN LATERAL unnest(array(1)) AS s(location)",
            write={
                "postgres": "SELECT * FROM r CROSS JOIN LATERAL UNNEST(ARRAY[1]) AS s(location)",
            },
        )
        self.validate_all(
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL",
            read={
                "postgres": "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE deleted NOTNULL"
            },
        )
        self.validate_all(
            "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL",
            read={
                "postgres": "SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted ISNULL"
            },
        )
        self.validate_all(
            "'[1,2,3]'::json->2",
            write={"postgres": "CAST('[1,2,3]' AS JSON) -> 2"},
        )
        self.validate_all(
            """'{"a":1,"b":2}'::json->'b'""",
            write={
                "postgres": """CAST('{"a":1,"b":2}' AS JSON) -> 'b'""",
                "redshift": """CAST('{"a":1,"b":2}' AS JSON)."b\"""",
            },
        )
        self.validate_all(
            """'{"x": {"y": 1}}'::json->'x'->'y'""",
            write={"postgres": """CAST('{"x": {"y": 1}}' AS JSON) -> 'x' -> 'y'"""},
        )
        self.validate_all(
            """'{"x": {"y": 1}}'::json->'x'::json->'y'""",
            write={"postgres": """CAST(CAST('{"x": {"y": 1}}' AS JSON) -> 'x' AS JSON) -> 'y'"""},
        )
        self.validate_all(
            """'[1,2,3]'::json->>2""",
            write={"postgres": "CAST('[1,2,3]' AS JSON) ->> 2"},
        )
        self.validate_all(
            """'{"a":1,"b":2}'::json->>'b'""",
            write={"postgres": """CAST('{"a":1,"b":2}' AS JSON) ->> 'b'"""},
        )
        self.validate_all(
            """'{"a":[1,2,3],"b":[4,5,6]}'::json#>'{a,2}'""",
            write={"postgres": """CAST('{"a":[1,2,3],"b":[4,5,6]}' AS JSON) #> '{a,2}'"""},
        )
        self.validate_all(
            """'{"a":[1,2,3],"b":[4,5,6]}'::json#>>'{a,2}'""",
            write={"postgres": """CAST('{"a":[1,2,3],"b":[4,5,6]}' AS JSON) #>> '{a,2}'"""},
        )
        self.validate_all(
            """SELECT JSON_ARRAY_ELEMENTS((foo->'sections')::JSON) AS sections""",
            write={
                "postgres": """SELECT JSON_ARRAY_ELEMENTS(CAST((foo -> 'sections') AS JSON)) AS sections""",
                "presto": """SELECT JSON_ARRAY_ELEMENTS(CAST((JSON_EXTRACT(foo, 'sections')) AS JSON)) AS sections""",
            },
        )
        self.validate_all(
            """x ? 'x'""",
            write={"postgres": "x ? 'x'"},
        )
        self.validate_all(
            "SELECT $$a$$",
            write={"postgres": "SELECT 'a'"},
        )
        self.validate_all(
            "SELECT $$Dianne's horse$$",
            write={"postgres": "SELECT 'Dianne''s horse'"},
        )
        self.validate_all(
            "UPDATE MYTABLE T1 SET T1.COL = 13",
            write={"postgres": "UPDATE MYTABLE AS T1 SET T1.COL = 13"},
        )
        self.validate_all(
            "x !~ 'y'",
            write={"postgres": "NOT x ~ 'y'"},
        )
        self.validate_all(
            "x !~* 'y'",
            write={"postgres": "NOT x ~* 'y'"},
        )

        self.validate_all(
            "x ~~ 'y'",
            write={"postgres": "x LIKE 'y'"},
        )
        self.validate_all(
            "x ~~* 'y'",
            write={"postgres": "x ILIKE 'y'"},
        )
        self.validate_all(
            "x !~~ 'y'",
            write={"postgres": "NOT x LIKE 'y'"},
        )
        self.validate_all(
            "x !~~* 'y'",
            write={"postgres": "NOT x ILIKE 'y'"},
        )
        self.validate_all(
            "'45 days'::interval day",
            write={"postgres": "CAST('45 days' AS INTERVAL day)"},
        )
        self.validate_all(
            "'x' 'y' 'z'",
            write={"postgres": "CONCAT('x', 'y', 'z')"},
        )
        self.validate_all(
            "x::cstring",
            write={"postgres": "CAST(x AS CSTRING)"},
        )
        self.validate_all(
            "TRIM(BOTH 'as' FROM 'as string as')",
            write={
                "postgres": "TRIM(BOTH 'as' FROM 'as string as')",
                "spark": "TRIM(BOTH 'as' FROM 'as string as')",
            },
        )
        self.validate_all(
            "merge into x as x using (select id) as y on a = b WHEN matched then update set X.a = y.b",
            write={
                "postgres": "MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
                "snowflake": "MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET X.a = y.b",
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

        self.assertIsInstance(parse_one("id::UUID", read="postgres"), exp.Cast)

    def test_bool_or(self):
        self.validate_all(
            "SELECT a, LOGICAL_OR(b) FROM table GROUP BY a",
            write={"postgres": "SELECT a, BOOL_OR(b) FROM table GROUP BY a"},
        )

    def test_string_concat(self):
        self.validate_all(
            "SELECT CONCAT('abcde', 2, NULL, 22)",
            write={
                "postgres": "SELECT CONCAT(COALESCE(CAST('abcde' AS TEXT), ''), COALESCE(CAST(2 AS TEXT), ''), COALESCE(CAST(NULL AS TEXT), ''), COALESCE(CAST(22 AS TEXT), ''))",
            },
        )
        self.validate_all(
            "CONCAT(a, b)",
            write={
                "": "CONCAT(COALESCE(CAST(a AS TEXT), ''), COALESCE(CAST(b AS TEXT), ''))",
                "duckdb": "CONCAT(COALESCE(CAST(a AS TEXT), ''), COALESCE(CAST(b AS TEXT), ''))",
                "postgres": "CONCAT(COALESCE(CAST(a AS TEXT), ''), COALESCE(CAST(b AS TEXT), ''))",
                "presto": "CONCAT(CAST(COALESCE(CAST(a AS VARCHAR), '') AS VARCHAR), CAST(COALESCE(CAST(b AS VARCHAR), '') AS VARCHAR))",
            },
        )
        self.validate_all(
            "a || b",
            write={
                "": "a || b",
                "clickhouse": "CONCAT(CAST(a AS TEXT), CAST(b AS TEXT))",
                "duckdb": "a || b",
                "postgres": "a || b",
                "presto": "CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR))",
            },
        )
