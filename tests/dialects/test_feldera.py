from sqlglot import exp
from tests.dialects.test_dialect import Validator


class TestFeldera(Validator):
    dialect = "feldera"
    maxDiff = None

    def test_feldera(self):
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
        self.validate_identity(
            "CREATE LOCAL VIEW v WITH ('emit_final' = 'col') AS SELECT * FROM t",
            "CREATE LOCAL VIEW v WITH (emit_final='col') AS SELECT * FROM t",
        )
        self.assertIsInstance(
            self.parse_one("CREATE LOCAL VIEW v WITH ('emit_final' = 'col') AS SELECT * FROM t"),
            exp.Create,
        )
        self.validate_identity("CREATE TYPE t AS (a INT, b VARCHAR)")
        self.validate_identity("CREATE TYPE t AS INT")
        self.validate_identity("CREATE TYPE IF NOT EXISTS t AS INT")
        self.assertIsInstance(self.parse_one("CREATE TYPE t AS (a INT, b VARCHAR)"), exp.Create)
        self.validate_identity("CREATE MATERIALIZED VIEW v AS SELECT * FROM t")
        self.validate_identity("DECLARE RECURSIVE VIEW v AS SELECT * FROM t")
        self.assertIsInstance(
            self.parse_one("DECLARE RECURSIVE VIEW v AS SELECT * FROM t"),
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
        self.validate_identity(
            "CREATE TABLE events (event_time TIMESTAMP WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND, payload VARCHAR)",
            "CREATE TABLE events (event_time TIMESTAMP WATERMARK FOR event_time AS event_time - INTERVAL '5 SECOND', payload VARCHAR)",
        )

        self.validate_identity("SELECT 1 MINUS SELECT 2", "SELECT 1 EXCEPT SELECT 2")

        self.validate_identity(
            "CREATE TABLE t (c INT) WITH ('materialized' = 'true')",
            "CREATE TABLE t (c INT) WITH (materialized='true')",
        )

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
            "SELECT CAST(x AS DATETIME) FROM t",
            "SELECT CAST(x AS TIMESTAMP) FROM t",
        )