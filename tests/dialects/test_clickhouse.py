from sqlglot import exp, parse_one
from tests.dialects.test_dialect import Validator


class TestClickhouse(Validator):
    dialect = "clickhouse"

    def test_clickhouse(self):
        self.validate_identity("ATTACH DATABASE DEFAULT ENGINE = ORDINARY")
        self.validate_identity("CAST(['hello'], 'Array(Enum8(''hello'' = 1))')")
        self.validate_identity("SELECT x, COUNT() FROM y GROUP BY x WITH TOTALS")
        self.validate_identity("SELECT INTERVAL t.days day")
        self.validate_identity("SELECT match('abc', '([a-z]+)')")
        self.validate_identity("dictGet(x, 'y')")
        self.validate_identity("SELECT * FROM x FINAL")
        self.validate_identity("SELECT * FROM x AS y FINAL")
        self.validate_identity("'a' IN mapKeys(map('a', 1, 'b', 2))")
        self.validate_identity("CAST((1, 2) AS Tuple(a Int8, b Int16))")
        self.validate_identity("SELECT * FROM foo LEFT ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo LEFT ASOF JOIN bla")
        self.validate_identity("SELECT * FROM foo ASOF JOIN bla")
        self.validate_identity("SELECT * FROM foo ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo GLOBAL ANY JOIN bla")
        self.validate_identity("SELECT * FROM foo GLOBAL LEFT ANY JOIN bla")
        self.validate_identity("SELECT quantile(0.5)(a)")
        self.validate_identity("SELECT quantiles(0.5)(a) AS x FROM t")
        self.validate_identity("SELECT quantiles(0.1, 0.2, 0.3)(a)")
        self.validate_identity("SELECT quantileTiming(0.5)(RANGE(100))")
        self.validate_identity("SELECT histogram(5)(a)")
        self.validate_identity("SELECT groupUniqArray(2)(a)")
        self.validate_identity("SELECT exponentialTimeDecayedAvg(60)(a, b)")
        self.validate_identity("SELECT * FROM foo WHERE x GLOBAL IN (SELECT * FROM bar)")
        self.validate_identity("position(haystack, needle)")
        self.validate_identity("position(haystack, needle, position)")
        self.validate_identity("CAST(x AS DATETIME)")
        self.validate_identity(
            "CREATE TABLE test (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()"
        )

        self.validate_all(
            "SELECT uniq(x) FROM (SELECT any(y) AS x FROM (SELECT 1 AS y))",
            read={
                "bigquery": "SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) x FROM (SELECT 1 y))",
            },
            write={
                "bigquery": "SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) AS x FROM (SELECT 1 AS y))",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "clickhouse": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
            },
        )
        self.validate_all(
            "CAST(1 AS NULLABLE(Int64))",
            write={
                "clickhouse": "CAST(1 AS Nullable(Int64))",
            },
        )
        self.validate_all(
            "CAST(1 AS Nullable(DateTime64(6, 'UTC')))",
            write={"clickhouse": "CAST(1 AS Nullable(DateTime64(6, 'UTC')))"},
        )
        self.validate_all(
            "SELECT x #! comment",
            write={"": "SELECT x /* comment */"},
        )
        self.validate_all(
            "SELECT quantileIf(0.5)(a, true)",
            write={
                "clickhouse": "SELECT quantileIf(0.5)(a, TRUE)",
            },
        )
        self.validate_all(
            "SELECT position(needle IN haystack)",
            write={"clickhouse": "SELECT position(haystack, needle)"},
        )
        self.validate_identity(
            "SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result = 'break'"
        )
        self.validate_identity("SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_")
        self.validate_identity("SELECT * FROM x FORMAT PrettyCompact")
        self.validate_identity(
            "SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_ FORMAT PrettyCompact"
        )
        self.validate_all(
            "SELECT * FROM foo JOIN bar USING id, name",
            write={"clickhouse": "SELECT * FROM foo JOIN bar USING (id, name)"},
        )
        self.validate_all(
            "SELECT * FROM foo ANY LEFT JOIN bla ON foo.c1 = bla.c2",
            write={"clickhouse": "SELECT * FROM foo LEFT ANY JOIN bla ON foo.c1 = bla.c2"},
        )
        self.validate_all(
            "SELECT * FROM foo GLOBAL ANY LEFT JOIN bla ON foo.c1 = bla.c2",
            write={"clickhouse": "SELECT * FROM foo GLOBAL LEFT ANY JOIN bla ON foo.c1 = bla.c2"},
        )
        self.validate_all(
            """
            SELECT
                loyalty,
                count()
            FROM hits SEMI LEFT JOIN users USING (UserID)
            GROUP BY loyalty
            ORDER BY loyalty ASC
            """,
            write={
                "clickhouse": "SELECT loyalty, COUNT() FROM hits LEFT SEMI JOIN users USING (UserID)"
                + " GROUP BY loyalty ORDER BY loyalty"
            },
        )

    def test_cte(self):
        self.validate_identity("WITH 'x' AS foo SELECT foo")
        self.validate_identity("WITH SUM(bytes) AS foo SELECT foo FROM system.parts")
        self.validate_identity("WITH (SELECT foo) AS bar SELECT bar + 5")
        self.validate_identity("WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1")

    def test_ternary(self):
        self.validate_all("x ? 1 : 2", write={"clickhouse": "CASE WHEN x THEN 1 ELSE 2 END"})
        self.validate_all(
            "IF(BAR(col), sign > 0 ? FOO() : 0, 1)",
            write={
                "clickhouse": "CASE WHEN BAR(col) THEN CASE WHEN sign > 0 THEN FOO() ELSE 0 END ELSE 1 END"
            },
        )
        self.validate_all(
            "x AND FOO() > 3 + 2 ? 1 : 2",
            write={"clickhouse": "CASE WHEN x AND FOO() > 3 + 2 THEN 1 ELSE 2 END"},
        )
        self.validate_all(
            "x ? (y ? 1 : 2) : 3",
            write={"clickhouse": "CASE WHEN x THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END"},
        )
        self.validate_all(
            "x AND (foo() ? FALSE : TRUE) ? (y ? 1 : 2) : 3",
            write={
                "clickhouse": "CASE WHEN x AND (CASE WHEN foo() THEN FALSE ELSE TRUE END) THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END"
            },
        )

        ternary = parse_one("x ? (y ? 1 : 2) : 3", read="clickhouse")

        self.assertIsInstance(ternary, exp.If)
        self.assertIsInstance(ternary.this, exp.Column)
        self.assertIsInstance(ternary.args["true"], exp.Paren)
        self.assertIsInstance(ternary.args["false"], exp.Literal)

        nested_ternary = ternary.args["true"].this

        self.assertIsInstance(nested_ternary.this, exp.Column)
        self.assertIsInstance(nested_ternary.args["true"], exp.Literal)
        self.assertIsInstance(nested_ternary.args["false"], exp.Literal)

        parse_one("a and b ? 1 : 2", read="clickhouse").assert_is(exp.If).this.assert_is(exp.And)

    def test_parameterization(self):
        self.validate_all(
            "SELECT {abc: UInt32}, {b: String}, {c: DateTime},{d: Map(String, Array(UInt8))}, {e: Tuple(UInt8, String)}",
            write={
                "clickhouse": "SELECT {abc: UInt32}, {b: TEXT}, {c: DATETIME}, {d: Map(TEXT, Array(UInt8))}, {e: Tuple(UInt8, String)}",
                "": "SELECT :abc, :b, :c, :d, :e",
            },
        )
        self.validate_all(
            "SELECT * FROM {table: Identifier}",
            write={"clickhouse": "SELECT * FROM {table: Identifier}"},
        )

    def test_signed_and_unsigned_types(self):
        data_types = [
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
        ]
        for data_type in data_types:
            self.validate_all(
                f"pow(2, 32)::{data_type}",
                write={"clickhouse": f"CAST(POWER(2, 32) AS {data_type})"},
            )

    def test_ddl(self):
        self.validate_identity(
            "CREATE TABLE foo (x UInt32) TTL time_column + INTERVAL '1' MONTH DELETE WHERE column = 'value'"
        )

        self.validate_all(
            """
            CREATE TABLE example1 (
               timestamp DateTime,
               x UInt32 TTL now() + INTERVAL 1 MONTH,
               y String TTL timestamp + INTERVAL 1 DAY,
               z String
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            """,
            write={
                "clickhouse": """CREATE TABLE example1 (
  timestamp DATETIME,
  x UInt32 TTL now() + INTERVAL '1' MONTH,
  y TEXT TTL timestamp + INTERVAL '1' DAY,
  z TEXT
)
ENGINE=MergeTree
ORDER BY tuple()""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE test (id UInt64, timestamp DateTime64, data String, max_hits UInt64, sum_hits UInt64) ENGINE = MergeTree
            PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
            TTL timestamp + INTERVAL 1 DAY
            GROUP BY id, toStartOfDay(timestamp)
            SET
               max_hits = max(max_hits),
               sum_hits = sum(sum_hits)
            """,
            write={
                "clickhouse": """CREATE TABLE test (
  id UInt64,
  timestamp DateTime64,
  data TEXT,
  max_hits UInt64,
  sum_hits UInt64
)
ENGINE=MergeTree
PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
TTL
  timestamp + INTERVAL '1' DAY
GROUP BY
  id,
  toStartOfDay(timestamp)
SET
  max_hits = MAX(max_hits),
  sum_hits = SUM(sum_hits)""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE test (id String, data String) ENGINE = AggregatingMergeTree()
                ORDER BY tuple()
            SETTINGS
                max_suspicious_broken_parts=500,
                parts_to_throw_insert=100
            """,
            write={
                "clickhouse": """CREATE TABLE test (
  id TEXT,
  data TEXT
)
ENGINE=AggregatingMergeTree()
ORDER BY tuple()
SETTINGS
  max_suspicious_broken_parts = 500,
  parts_to_throw_insert = 100""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE example_table
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE,
                d + INTERVAL 1 WEEK TO VOLUME 'aaa',
                d + INTERVAL 2 WEEK TO DISK 'bbb';
            """,
            write={
                "clickhouse": """CREATE TABLE example_table (
  d DATETIME,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE,
  d + INTERVAL '1' WEEK TO VOLUME 'aaa',
  d + INTERVAL '2' WEEK TO DISK 'bbb'""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_with_where
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
            """,
            write={
                "clickhouse": """CREATE TABLE table_with_where (
  d DATETIME,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE
WHERE
  toDayOfWeek(d) = 1""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_for_recompression
            (
                d DateTime,
                key UInt64,
                value String
            ) ENGINE MergeTree()
            ORDER BY tuple()
            PARTITION BY key
            TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
            SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
            """,
            write={
                "clickhouse": """CREATE TABLE table_for_recompression (
  d DATETIME,
  key UInt64,
  value TEXT
)
ENGINE=MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL
  d + INTERVAL '1' MONTH RECOMPRESS CODEC(ZSTD(17)),
  d + INTERVAL '1' YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS
  min_rows_for_wide_part = 0,
  min_bytes_for_wide_part = 0""",
            },
            pretty=True,
        )
        self.validate_all(
            """
            CREATE TABLE table_for_aggregation
            (
                d DateTime,
                k1 Int,
                k2 Int,
                x Int,
                y Int
            )
            ENGINE = MergeTree
            ORDER BY (k1, k2)
            TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
            """,
            write={
                "clickhouse": """CREATE TABLE table_for_aggregation (
  d DATETIME,
  k1 Int32,
  k2 Int32,
  x Int32,
  y Int32
)
ENGINE=MergeTree
ORDER BY (k1, k2)
TTL
  d + INTERVAL '1' MONTH
GROUP BY
  k1,
  k2
SET
  x = MAX(x),
  y = MIN(y)""",
            },
            pretty=True,
        )
