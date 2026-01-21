from sqlglot import exp
from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestStarrocks(Validator):
    dialect = "starrocks"

    def test_starrocks(self):
        self.assertEqual(self.validate_identity("arr[1]").expressions[0], exp.Literal.number(0))

        self.validate_identity("SELECT ARRAY_JOIN([1, 3, 5, NULL], '_', 'NULL')")
        self.validate_identity("SELECT ARRAY_JOIN([1, 3, 5, NULL], '_')")
        self.validate_identity("ALTER TABLE a SWAP WITH b")
        self.validate_identity("SELECT ARRAY_AGG(a) FROM x")
        self.validate_identity("SELECT ST_POINT(10, 20)")
        self.validate_identity("SELECT ST_DISTANCE_SPHERE(10.1, 20.2, 30.3, 40.4)")
        self.validate_identity("ARRAY_FLATTEN(arr)").assert_is(exp.Flatten)

        self.validate_all(
            "SELECT * FROM t WHERE cond",
            read={
                "": "SELECT * FROM t WHERE cond IS TRUE",
                "starrocks": "SELECT * FROM t WHERE cond",
            },
        )

        self.validate_identity("CURRENT_VERSION()")

    def test_ddl(self):
        self.validate_identity("INSERT OVERWRITE my_table SELECT * FROM other_table")
        self.validate_identity("CREATE TABLE t (c INT) COMMENT 'c'")

        ddl_sqls = [
            "PARTITION BY (col1, col2)",
            "PARTITION BY (DATE_TRUNC('DAY', col2), col1)",
            "PARTITION BY (FROM_UNIXTIME(col2))",
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
            "ORDER BY (col1, col2)",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(event_day, siteid), r2(event_day, citycode), r3(event_day))",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2))",
            "DISTRIBUTED BY HASH (col1) ROLLUP (`r1`(`col2`))",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) FROM base_index)",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) PROPERTIES ('storage_type'='column'))",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) FROM base_index PROPERTIES ('k'='v'))",
            "DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) PROPERTIES ('k1'='v1', 'k2'='v2'))",
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
        self.validate_identity(
            "CREATE VIEW foo (foo_col1) SECURITY NONE AS SELECT bar_col1 FROM bar"
        )

        # Test ROLLUP property
        self.validate_all(
            "CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ROLLUP (r1(col1, col2), r2(col1))",
            write={
                "starrocks": "CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ROLLUP (r1(col1, col2), r2(col1))",
                "spark": "CREATE TABLE foo (col1 BIGINT, col2 BIGINT)",
                "duckdb": "CREATE TABLE foo (col1 BIGINT, col2 BIGINT)",
                "postgres": "CREATE TABLE foo (col1 BIGINT, col2 BIGINT)",
            },
        )

        # expression partitioning in MV
        expr_partition = exp.PartitionedByProperty(
            this=exp.Tuple(
                expressions=[
                    exp.Anonymous(this="FROM_UNIXTIME", expressions=[exp.column("ts")]),
                    exp.column("region"),
                ]
            )
        )
        create = exp.Create(
            this=exp.to_table("t"),
            kind="VIEW",
            properties=exp.Properties(expressions=[expr_partition]),
        )
        create_sql = create.sql(dialect="starrocks")
        self.assertTrue("PARTITION BY (FROM_UNIXTIME(ts), region)" in create_sql)

        # column tuple expression partitioning (columns only) in MV
        columns_only_partition = exp.PartitionedByProperty(
            this=exp.Tuple(expressions=[exp.column("c1"), exp.column("c2")])
        )
        create = exp.Create(
            this=exp.to_table("t"),
            kind="VIEW",
            properties=exp.Properties(expressions=[columns_only_partition]),
        )
        self.assertEqual(
            columns_only_partition.sql(dialect="starrocks"),
            "PARTITION BY (c1, c2)",
        )
        create_sql = create.sql(dialect="starrocks")
        self.assertTrue("PARTITION BY (c1, c2)" in create_sql)

        # ORDER BY
        multi_column_cluster = exp.Cluster(
            expressions=[
                exp.column("c"),
                exp.column("d"),
            ]
        )
        self.assertEqual(multi_column_cluster.sql(dialect="starrocks"), "ORDER BY (c, d)")

        single_column_cluster = exp.Cluster(expressions=[exp.column("c")])
        self.assertEqual(single_column_cluster.sql(dialect="starrocks"), "ORDER BY (c)")

        # MV: Refresh trigger property
        manual_refresh = exp.RefreshTriggerProperty(kind=exp.var("MANUAL"))
        self.assertEqual(manual_refresh.sql(dialect="starrocks"), "REFRESH MANUAL")

        async_refresh = exp.RefreshTriggerProperty(
            method=exp.var("IMMEDIATE"),
            kind=exp.var("ASYNC"),
            starts=exp.Literal.string("2025-01-01 00:00:00"),
            every=exp.Literal.number(5),
            unit=exp.var("MINUTE"),
        )
        self.assertEqual(
            async_refresh.sql(dialect="starrocks"),
            "REFRESH IMMEDIATE ASYNC START ('2025-01-01 00:00:00') EVERY (INTERVAL 5 MINUTE)",
        )

        skip_unspecified_method_refresh = exp.RefreshTriggerProperty(
            method=exp.var("UNSPECIFIED"), kind=exp.var("ASYNC")
        )
        self.assertEqual(skip_unspecified_method_refresh.sql(dialect="starrocks"), "REFRESH ASYNC")

        # RENAME table without TO keyword
        self.validate_identity("ALTER TABLE t1 RENAME t2")

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

    def test_between(self):
        self.validate_all(
            "SELECT * FROM t WHERE a BETWEEN 1 AND 5",
            write={
                "starrocks": "SELECT * FROM t WHERE a BETWEEN 1 AND 5",
                "mysql": "SELECT * FROM t WHERE a BETWEEN 1 AND 5",
            },
        )
        self.validate_identity("SELECT a BETWEEN 1 AND 5 FROM t")
        self.validate_identity(
            "DELETE FROM t WHERE a BETWEEN b AND c",
            "DELETE FROM t WHERE a >= b AND a <= c",
        )
        self.validate_identity(
            "DELETE FROM t WHERE a BETWEEN 1 AND 10 AND b BETWEEN 20 AND 30 OR c BETWEEN 'x' AND 'z'",
            "DELETE FROM t WHERE a >= 1 AND a <= 10 AND b >= 20 AND b <= 30 OR c >= 'x' AND c <= 'z'",
        )

    def test_partition(self):
        # Column-based partitioning
        for cols in "col1", "col1, col2":
            with self.subTest(f"Testing PARTITION BY with {cols}"):
                self.validate_identity(
                    f"CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY ({cols})",
                    f"CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY {cols}",
                )
                self.validate_identity(
                    f"CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY {cols}"
                )

        # Expression-based partitioning
        self.validate_identity(
            "CREATE TABLE test_table (col2 DATE) PARTITION BY DATE_TRUNC('DAY', col2)"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col2 BIGINT) PARTITION BY FROM_UNIXTIME(col2, '%Y%m%d')"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col1 STRING, col2 BIGINT) PARTITION BY FROM_UNIXTIME(col2, '%Y%m%d'), col1"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col1 BIGINT, col2 DATE) PARTITION BY FROM_UNIXTIME(col2, '%Y%m%d'), DATE_TRUNC('DAY', col1)"
        )

        # LIST partitioning
        self.validate_identity(
            "CREATE TABLE test_table (col1 STRING) PARTITION BY LIST (col1) (PARTITION pLos_Angeles VALUES IN ('Los Angeles'), PARTITION pSan_Francisco VALUES IN ('San Francisco'))"
        )

        # Multi-column LIST partitioning
        self.validate_identity(
            "CREATE TABLE test_table (col1 DATE, col2 STRING) PARTITION BY LIST (col1, col2) (PARTITION p1 VALUES IN (('2022-04-01', 'LA'), ('2022-04-01', 'SF')))"
        )

        # RANGE partitioning with explicit values
        self.validate_identity(
            "CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (PARTITION p1 VALUES LESS THAN ('2020-01-31'), PARTITION p2 VALUES LESS THAN ('2020-02-29'), PARTITION p3 VALUES LESS THAN ('2020-03-31'))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col1 STRING) PARTITION BY RANGE (STR2DATE(col1, '%Y-%m-%d')) (PARTITION p1 VALUES LESS THAN ('2021-01-01'), PARTITION p2 VALUES LESS THAN ('2021-01-02'), PARTITION p3 VALUES LESS THAN ('2021-01-03'))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (PARTITION p1 VALUES LESS THAN ('2020-01-31'), PARTITION p_max VALUES LESS THAN (MAXVALUE))"
        )

        # RANGE partitioning with START/END/EVERY
        self.validate_identity(
            "CREATE TABLE test_table (col1 BIGINT) PARTITION BY RANGE (col1) (START ('1') END ('10') EVERY (1), START ('10') END ('100') EVERY (10))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (START ('2019-01-01') END ('2021-01-01') EVERY (INTERVAL 1 YEAR), START ('2021-01-01') END ('2021-05-01') EVERY (INTERVAL 1 MONTH), START ('2021-05-01') END ('2021-05-04') EVERY (INTERVAL 1 DAY))"
        )
