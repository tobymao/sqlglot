from sqlglot import exp, parse
from tests.dialects.test_dialect import Validator


class TestFeldera(Validator):
    dialect = "feldera"
    maxDiff = None

    def test_feldera(self):
        self.validate_identity("EXPLAIN SELECT 1")
        self.assertIsInstance(self.parse_one("EXPLAIN SELECT 1"), exp.Command)
        self.validate_identity("SHOW TABLES")
        self.assertIsInstance(self.parse_one("SHOW TABLES"), exp.Command)
        self.validate_identity("INSERT INTO foo VALUES (1)")
        self.assertIsInstance(self.parse_one("INSERT INTO foo VALUES (1)"), exp.Insert)
        self.validate_identity("create table foo (id int)", "CREATE TABLE foo (id INT)")
        self.validate_identity("-- comment\nSELECT 1", "/* comment */ SELECT 1")
        self.validate_identity("/* block */ SELECT 1")
        self.validate_identity("SELECT '10'::INT2", "SELECT CAST('10' AS SMALLINT)")
        self.validate_identity("SELECT a <=> b FROM t")
        self.validate_identity("SELECT SAFE_CAST(x AS INT) FROM t")
        self.validate_identity(
            "SELECT i, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM t QUALIFY rn = 1"
        )
        self.validate_identity(
            "SELECT * FROM l LEFT ASOF JOIN r MATCH_CONDITION (l.ts >= r.ts) ON l.id = r.id"
        )
        self.validate_identity(
            "SELECT * FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' MINUTE))",
            "SELECT * FROM TABLE(TUMBLE(t, DESCRIPTOR(ts), INTERVAL '5 MINUTE'))",
        )
        self.validate_identity(
            "SELECT * FROM TABLE(HOP(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))",
            "SELECT * FROM TABLE(HOP(t, DESCRIPTOR(ts), INTERVAL '1 MINUTE', INTERVAL '5 MINUTE'))",
        )
        self.validate_identity("SELECT EXISTS(ARRAY[1, -12, 3], x -> x > 0)")
        self.validate_identity("SELECT EXISTS(SELECT 1)")
        self.validate_identity("SELECT * EXCLUDE (col1, col2) FROM t")
        self.validate_identity("CREATE VIEW v AS WITH cte AS (SELECT 1) SELECT * FROM cte")
        self.validate_identity(
            "CREATE LOCAL VIEW v WITH ('emit_final' = 'col') AS SELECT * FROM t",
            "CREATE LOCAL VIEW v WITH (emit_final='col') AS SELECT * FROM t",
        )
        self.validate_identity(
            "CREATE TABLE kafka_sales (id INT) WITH ('connectors' = '[{\"name\": \"kafka\"}]')",
            "CREATE TABLE kafka_sales (id INT) WITH (connectors='[{\"name\": \"kafka\"}]')",
        )
        self.validate_identity("CREATE TABLE t2 (d VARCHAR DEFAULT 'a;b')")
        self.validate_identity('CREATE TABLE "MyTable" ("col" INT)')
        self.assertIsInstance(
            self.parse_one("CREATE LOCAL VIEW v WITH ('emit_final' = 'col') AS SELECT * FROM t"),
            exp.Create,
        )
        self.validate_identity("CREATE TYPE t AS (a INT, b VARCHAR)")
        self.validate_identity("CREATE TYPE t AS INT")
        self.validate_identity("CREATE TYPE IF NOT EXISTS t AS INT")
        self.assertIsInstance(self.parse_one("CREATE TYPE t AS (a INT, b VARCHAR)"), exp.Create)
        self.validate_identity("CREATE FUNCTION my_func(x INT) RETURNS INT AS x * 2")
        self.validate_identity("CREATE FUNCTION my_func(x INT) RETURNS INT")
        self.validate_identity("CREATE INDEX v_index ON v(id)")
        self.validate_identity("SET FELDERA_WARNINGS_ARE_ERRORS = ON")
        self.assertIsInstance(self.parse_one("SET FELDERA_WARNINGS_ARE_ERRORS = ON"), exp.Set)
        self.validate_identity("CREATE MATERIALIZED VIEW v AS SELECT * FROM t")
        self.validate_identity("CREATE VIEW v (a, b, c) AS SELECT x, y, z FROM t")
        self.validate_identity("DECLARE RECURSIVE VIEW v (id INT, name VARCHAR)")
        self.assertIsInstance(
            self.parse_one("DECLARE RECURSIVE VIEW v (id INT, name VARCHAR)"),
            exp.DeclareRecursiveView,
        )
        self.validate_identity("CREATE LINEAR AGGREGATE a(x INT) RETURNS INT")
        self.assertIsInstance(
            self.parse_one("CREATE LINEAR AGGREGATE a(x INT) RETURNS INT"),
            exp.Create,
        )
        self.validate_identity("REMOVE FROM t VALUES (1, 'a')")
        self.assertIsInstance(self.parse_one("REMOVE FROM t VALUES (1, 'a')"), exp.Remove)
        self.validate_identity(
            "LATENESS v.ts INTERVAL '5' SECOND",
            "LATENESS v.ts INTERVAL '5 SECOND'",
        )
        self.assertIsInstance(self.parse_one("LATENESS v.ts INTERVAL '5' SECOND"), exp.Lateness)
        self.validate_identity(
            "CREATE TABLE t (ts TIMESTAMP LATENESS INTERVAL '1' HOUR, payload INT INTERNED)",
            "CREATE TABLE t (ts TIMESTAMP LATENESS INTERVAL '1 HOUR', payload INT INTERNED)",
        )
        self.validate_identity("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR)")
        self.validate_identity("CREATE TABLE t (id INT, CONSTRAINT pk PRIMARY KEY (id))")
        self.validate_identity("CREATE TABLE t (id INT, CHECK (id > 0))")
        self.validate_identity(
            "CREATE TABLE t (empno BIGINT FOREIGN KEY REFERENCES employee(empid))",
            "CREATE TABLE t (empno BIGINT FOREIGN KEY REFERENCES employee (empid))",
        )
        self.validate_identity(
            "CREATE TABLE events (event_time TIMESTAMP WATERMARK INTERVAL '5' SECOND, payload VARCHAR)",
            "CREATE TABLE events (event_time TIMESTAMP WATERMARK INTERVAL '5 SECOND', payload VARCHAR)",
        )

        self.validate_identity("SELECT 1 MINUS SELECT 2", "SELECT 1 EXCEPT SELECT 2")
        self.validate_identity("SELECT COUNT(*) FILTER (WHERE x > 0) FROM t", "SELECT COUNT(*) FILTER(WHERE x > 0) FROM t")
        self.validate_identity(
            "SELECT a, b, COUNT(*) FROM t GROUP BY CUBE(a, b)",
            "SELECT a, b, COUNT(*) FROM t GROUP BY CUBE (a, b)",
        )
        self.validate_identity("SELECT * FROM t LIMIT 10 OFFSET 5")
        self.validate_identity("SELECT DATE '2020-01-01'", "SELECT CAST('2020-01-01' AS DATE)")
        self.validate_identity("SELECT TIME '10:00:00'", "SELECT CAST('10:00:00' AS TIME)")
        self.validate_identity(
            "SELECT TIMESTAMP '2020-01-01 10:00:00'",
            "SELECT CAST('2020-01-01 10:00:00' AS TIMESTAMP)",
        )
        self.validate_identity("SELECT INTERVAL '10' DAY", "SELECT INTERVAL '10 DAY'")
        self.validate_identity(
            "SELECT NOW() - INTERVAL '1' DAY",
            "SELECT CURRENT_TIMESTAMP - INTERVAL '1 DAY'",
        )
        self.validate_identity("SELECT ARG_MAX(name, salary) FROM emp")
        self.validate_identity("SELECT COUNTIF(x > 0) FROM t")
        self.validate_identity("SELECT SAFE_OFFSET(arr, 0) FROM t")

        self.validate_identity(
            "CREATE TABLE t (c INT) WITH ('materialized' = 'true')",
            "CREATE TABLE t (c INT) WITH (materialized='true')",
        )
        self.validate_identity(
            "CREATE TABLE t (topic VARCHAR DEFAULT CAST(CONNECTOR_METADATA()['kafka_topic'] AS VARCHAR))"
        )

    def test_multi_statement_split(self):
        expressions = parse(
            "CREATE TABLE t1 (id INT); CREATE TABLE t2 (d VARCHAR DEFAULT 'a;b');",
            read="feldera",
        )
        self.assertEqual(len(expressions), 2)
        self.assertEqual(expressions[1].sql(dialect="feldera"), "CREATE TABLE t2 (d VARCHAR DEFAULT 'a;b')")

    def test_type_aliases(self):
        self.validate_identity(
            "SELECT CAST(x AS BOOL) FROM t",
            "SELECT CAST(x AS BOOLEAN) FROM t",
        )
        self.validate_identity(
            "SELECT CAST(x AS INT2) FROM t",
            "SELECT CAST(x AS SMALLINT) FROM t",
        )
        self.validate_identity("SELECT CAST(x AS BYTEA) FROM t")
        self.validate_identity(
            "SELECT CAST(x AS VARBINARY) FROM t",
            "SELECT CAST(x AS BYTEA) FROM t",
        )
        self.validate_identity(
            "SELECT CAST(x AS NUMERIC(5,3)) FROM t",
            "SELECT CAST(x AS DECIMAL(5, 3)) FROM t",
        )
        self.validate_identity(
            "SELECT CAST(x AS DATETIME) FROM t",
            "SELECT CAST(x AS TIMESTAMP) FROM t",
        )
        self.validate_identity(
            "SELECT CAST(x AS FLOAT) FROM t",
            "SELECT CAST(x AS REAL) FROM t",
        )
        self.validate_identity("SELECT CAST(x AS CHAR(10)) FROM t")