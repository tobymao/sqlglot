from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestStarrocks(Validator):
    dialect = "starrocks"

    def test_starrocks(self):
        self.validate_identity("ALTER TABLE a SWAP WITH b")

    def test_ddl(self):
        ddl_sqls = [
            "DISTRIBUTED BY HASH (col1) BUCKETS 1",
            "DISTRIBUTED BY HASH (col1)",
            "DISTRIBUTED BY RANDOM BUCKETS 1",
            "DISTRIBUTED BY RANDOM",
            "DISTRIBUTED BY HASH (col1) ORDER BY (col1)",
            "DISTRIBUTED BY HASH (col1) PROPERTIES ('replication_num'='1')",
            "PRIMARY KEY (col1) DISTRIBUTED BY HASH (col1)",
            "DUPLICATE KEY (col1, col2) DISTRIBUTED BY HASH (col1)",
            "UNIQUE KEY (col1, col2) PARTITION BY RANGE (col1) (START ('2024-01-01') END ('2024-01-31') EVERY (INTERVAL 1 DAY)) DISTRIBUTED BY HASH (col1)",
            "UNIQUE KEY (col1, col2) PARTITION BY RANGE (col1, col2) (START ('1') END ('10') EVERY (1), START ('10') END ('100') EVERY (10)) DISTRIBUTED BY HASH (col1)",
        ]

        for properties in ddl_sqls:
            with self.subTest(f"Testing create scheme: {properties}"):
                self.validate_identity(f"CREATE TABLE foo (col1 BIGINT, col2 BIGINT) {properties}")
                self.validate_identity(
                    f"CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ENGINE=OLAP {properties}"
                )

        # Test the different wider DECIMAL types
        self.validate_identity(
            "CREATE TABLE foo (col0 DECIMAL(9, 1), col1 DECIMAL32(9, 1), col2 DECIMAL64(18, 10), col3 DECIMAL128(38, 10)) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )
        self.validate_identity(
            "CREATE TABLE foo (col1 LARGEINT) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity("SELECT [1, 2, 3]")
        self.validate_identity(
            """SELECT CAST(PARSE_JSON(fieldvalue) -> '00000000-0000-0000-0000-00000000' AS VARCHAR) AS `code` FROM (SELECT '{"00000000-0000-0000-0000-00000000":"code01"}') AS t(fieldvalue)"""
        )
        self.validate_identity(
            "SELECT text FROM example_table", write_sql="SELECT `text` FROM example_table"
        )

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
        self.validate_identity(
            "SELECT DATE_DIFF('SECOND', '2010-11-30 23:59:59', '2010-11-30 20:58:59')"
        )
        self.validate_identity(
            "SELECT DATE_DIFF('MINUTE', '2010-11-30 23:59:59', '2010-11-30 20:58:59')"
        )

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP(abc, '%foo%')",
            read={
                "mysql": "SELECT REGEXP_LIKE(abc, '%foo%')",
                "starrocks": "SELECT REGEXP(abc, '%foo%')",
            },
            write={
                "mysql": "SELECT REGEXP_LIKE(abc, '%foo%')",
            },
        )

    def test_unnest(self):
        self.validate_identity(
            "SELECT student, score, t.unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS t",
            "SELECT student, score, t.unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS t(unnest)",
        )
        self.validate_all(
            "SELECT student, score, unnest FROM tests CROSS JOIN LATERAL UNNEST(scores)",
            write={
                "spark": "SELECT student, score, unnest FROM tests LATERAL VIEW EXPLODE(scores) unnest AS unnest",
                "starrocks": "SELECT student, score, unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS unnest(unnest)",
            },
        )
        self.validate_all(
            r"""SELECT * FROM UNNEST(array['John','Jane','Jim','Jamie'], array[24,25,26,27]) AS t(name, age)""",
            write={
                "postgres": "SELECT * FROM UNNEST(ARRAY['John', 'Jane', 'Jim', 'Jamie'], ARRAY[24, 25, 26, 27]) AS t(name, age)",
                "spark": "SELECT * FROM INLINE(ARRAYS_ZIP(ARRAY('John', 'Jane', 'Jim', 'Jamie'), ARRAY(24, 25, 26, 27))) AS t(name, age)",
                "starrocks": "SELECT * FROM UNNEST(['John', 'Jane', 'Jim', 'Jamie'], [24, 25, 26, 27]) AS t(name, age)",
            },
        )

        # Use UNNEST to convert into multiple columns
        # see: https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/unnest/
        self.validate_all(
            r"""SELECT id, t.type, t.scores FROM example_table, unnest(split(type, ";"), scores) AS t(type,scores)""",
            write={
                "postgres": "SELECT id, t.type, t.scores FROM example_table, UNNEST(SPLIT(type, ';'), scores) AS t(type, scores)",
                "spark": r"""SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';', '\\E')), scores)) t AS type, scores""",
                "databricks": r"""SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';', '\\E')), scores)) t AS type, scores""",
                "starrocks": r"""SELECT id, t.type, t.scores FROM example_table, UNNEST(SPLIT(type, ';'), scores) AS t(type, scores)""",
                "hive": UnsupportedError,
            },
        )

        self.validate_all(
            r"""SELECT id, t.type, t.scores FROM example_table_2 CROSS JOIN LATERAL unnest(split(type, ";"), scores) AS t(type,scores)""",
            write={
                "spark": r"""SELECT id, t.type, t.scores FROM example_table_2 LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';', '\\E')), scores)) t AS type, scores""",
                "starrocks": r"""SELECT id, t.type, t.scores FROM example_table_2 CROSS JOIN LATERAL UNNEST(SPLIT(type, ';'), scores) AS t(type, scores)""",
                "hive": UnsupportedError,
            },
        )

        lateral_explode_sqls = [
            "SELECT id, t.col FROM tbl, UNNEST(scores) AS t(col)",
            "SELECT id, t.col FROM tbl CROSS JOIN LATERAL UNNEST(scores) AS t(col)",
        ]

        for sql in lateral_explode_sqls:
            with self.subTest(f"Testing Starrocks roundtrip & transpilation of: {sql}"):
                self.validate_all(
                    sql,
                    write={
                        "starrocks": sql,
                        "spark": "SELECT id, t.col FROM tbl LATERAL VIEW EXPLODE(scores) t AS col",
                    },
                )

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE TBL(c1, c2) PROPERTIES ('prop1'=val1)")
        self.validate_identity("ANALYZE FULL TABLE TBL(c1, c2) PROPERTIES ('prop1'=val1)")
        self.validate_identity("ANALYZE SAMPLE TABLE TBL(c1, c2) PROPERTIES ('prop1'=val1)")
        self.validate_identity("ANALYZE TABLE TBL(c1, c2) WITH SYNC MODE PROPERTIES ('prop1'=val1)")
        self.validate_identity(
            "ANALYZE TABLE TBL(c1, c2) WITH ASYNC MODE PROPERTIES ('prop1'=val1)"
        )
        self.validate_identity(
            "ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 PROPERTIES ('prop1'=val1)"
        )
        self.validate_identity(
            "ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH 5 BUCKETS PROPERTIES ('prop1'=val1)"
        )
        self.validate_identity(
            "ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH SYNC MODE WITH 5 BUCKETS PROPERTIES ('prop1'=val1)"
        )
        self.validate_identity(
            "ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH ASYNC MODE WITH 5 BUCKETS PROPERTIES ('prop1'=val1)"
        )
