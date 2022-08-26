from tests.dialects.test_dialect import Validator


class TestHive(Validator):
    dialect = "hive"

    def test_bits(self):
        self.validate_all(
            "x & 1",
            write={
                "duckdb": "x & 1",
                "presto": "BITWISE_AND(x, 1)",
                "hive": "x & 1",
                "spark": "x & 1",
            },
        )
        self.validate_all(
            "~x",
            write={
                "duckdb": "~x",
                "presto": "BITWISE_NOT(x)",
                "hive": "~x",
                "spark": "~x",
            },
        )
        self.validate_all(
            "x | 1",
            write={
                "duckdb": "x | 1",
                "presto": "BITWISE_OR(x, 1)",
                "hive": "x | 1",
                "spark": "x | 1",
            },
        )
        self.validate_all(
            "x << 1",
            write={
                "duckdb": "x << 1",
                "presto": "BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)",
                "hive": "x << 1",
                "spark": "SHIFTLEFT(x, 1)",
            },
        )
        self.validate_all(
            "x >> 1",
            write={
                "duckdb": "x >> 1",
                "presto": "BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
                "hive": "x >> 1",
                "spark": "SHIFTRIGHT(x, 1)",
            },
        )
        self.validate_all(
            "x & 1 > 0",
            write={
                "duckdb": "x & 1 > 0",
                "presto": "BITWISE_AND(x, 1) > 0",
                "hive": "x & 1 > 0",
                "spark": "x & 1 > 0",
            },
        )

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE test STORED AS parquet TBLPROPERTIES ('x' = '1', 'Z' = '2') AS SELECT 1",
            write={
                "presto": "CREATE TABLE test WITH (FORMAT = 'parquet', x = '1', Z = '2') AS SELECT 1",
                "hive": "CREATE TABLE test STORED AS PARQUET TBLPROPERTIES ('x' = '1', 'Z' = '2') AS SELECT 1",
                "spark": "CREATE TABLE test STORED AS PARQUET TBLPROPERTIES ('x' = '1', 'Z' = '2') AS SELECT 1",
            },
        )
        self.validate_all(
            "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            write={
                "presto": "CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY = ARRAY['y', 'z'])",
                "hive": "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
                "spark": "CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)",
            },
        )

    def test_lateral_view(self):
        self.validate_all(
            "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
            write={
                "presto": "SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)",
                "hive": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
                "spark": "SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b",
            },
        )
        self.validate_all(
            "SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a",
            write={
                "presto": "SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)",
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
                "hive": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
                "spark": "SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a",
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
            "DATEDIFF(a, b)",
            write={
                "duckdb": "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
                "presto": "DATE_DIFF('day', CAST(SUBSTR(CAST(b AS VARCHAR), 1, 10) AS DATE), CAST(SUBSTR(CAST(a AS VARCHAR), 1, 10) AS DATE))",
                "hive": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
                "spark": "DATEDIFF(TO_DATE(a), TO_DATE(b))",
            },
        )
        for unit in ("DAY", "MONTH", "YEAR"):
            self.validate_all(
                f"{unit}(x)",
                write={
                    "duckdb": f"{unit}(CAST(x AS DATE))",
                    "presto": f"{unit}(CAST(SUBSTR(CAST(x AS VARCHAR), 1, 10) AS DATE))",
                    "hive": f"{unit}(TO_DATE(x))",
                    "spark": f"{unit}(TO_DATE(x))",
                },
            )

    def test_hive(self):
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
            "ARRAY_CONTAINS(x, 1)",
            write={
                "duckdb": "ARRAY_CONTAINS(x, 1)",
                "presto": "CONTAINS(x, 1)",
                "hive": "ARRAY_CONTAINS(x, 1)",
                "spark": "ARRAY_CONTAINS(x, 1)",
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
                "duckdb": "STRPOS(SUBSTR(x, 3), 'a') + 3 - 1",
                "presto": "STRPOS(SUBSTR(x, 3), 'a') + 3 - 1",
                "hive": "LOCATE('a', x, 3)",
                "spark": "LOCATE('a', x, 3)",
            },
        )
        # pylint: disable=anomalous-backslash-in-string
        self.validate_all(
            "INITCAP('new york')",
            write={
                "duckdb": "INITCAP('new york')",
                "presto": "REGEXP_REPLACE('new york', '(\w)(\w*)', x -> UPPER(x[1]) || LOWER(x[2]))",
                "hive": "INITCAP('new york')",
                "spark": "INITCAP('new york')",
            },
        )
        self.validate_all(
            "SELECT * FROM x TABLESAMPLE(10) y",
            write={
                "presto": "SELECT * FROM x AS y TABLESAMPLE(10)",
                "hive": "SELECT * FROM x TABLESAMPLE(10) AS y",
                "spark": "SELECT * FROM x TABLESAMPLE(10) AS y",
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
            "SELECT SORT_ARRAY(x, False)",
            write={
                "duckdb": "SELECT ARRAY_REVERSE_SORT(x)",
                "presto": "SELECT ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SELECT SORT_ARRAY(x, FALSE)",
                "spark": "SELECT SORT_ARRAY(x, FALSE)",
            },
        )
