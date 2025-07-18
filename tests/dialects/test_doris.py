from tests.dialects.test_dialect import Validator


class TestDoris(Validator):
    dialect = "doris"

    def test_doris(self):
        self.validate_all(
            "SELECT TO_DATE('2020-02-02 00:00:00')",
            write={
                "doris": "SELECT TO_DATE('2020-02-02 00:00:00')",
                "oracle": "SELECT CAST('2020-02-02 00:00:00' AS DATE)",
            },
        )
        self.validate_all(
            "SELECT MAX_BY(a, b), MIN_BY(c, d)",
            read={
                "clickhouse": "SELECT argMax(a, b), argMin(c, d)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            read={
                "clickhouse": "SELECT arraySum(x -> x*x, [2, 3])",
            },
            write={
                "clickhouse": "SELECT arraySum(x -> x * x, [2, 3])",
                "doris": "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            },
        )
        self.validate_all(
            "MONTHS_ADD(d, n)",
            read={
                "oracle": "ADD_MONTHS(d, n)",
            },
            write={
                "doris": "MONTHS_ADD(d, n)",
                "oracle": "ADD_MONTHS(d, n)",
            },
        )
        self.validate_all(
            """SELECT JSON_EXTRACT(CAST('{"key": 1}' AS JSONB), '$.key')""",
            read={
                "postgres": """SELECT '{"key": 1}'::jsonb ->> 'key'""",
            },
            write={
                "doris": """SELECT JSON_EXTRACT(CAST('{"key": 1}' AS JSONB), '$.key')""",
                "postgres": """SELECT JSON_EXTRACT_PATH(CAST('{"key": 1}' AS JSONB), 'key')""",
            },
        )
        self.validate_all(
            "SELECT GROUP_CONCAT('aa', ',')",
            read={
                "doris": "SELECT GROUP_CONCAT('aa', ',')",
                "mysql": "SELECT GROUP_CONCAT('aa' SEPARATOR ',')",
                "postgres": "SELECT STRING_AGG('aa', ',')",
            },
        )
        self.validate_all(
            "SELECT LAG(1, 1, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LAG(1, 1, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LAG(1) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LAG(1, 2, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LAG(1, 2, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LAG(1, 2) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LEAD(1, 1, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LEAD(1, 1, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LEAD(1) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LEAD(1, 2, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LEAD(1, 2, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LEAD(1, 2) OVER (ORDER BY 1)",
            },
        )
        self.validate_identity("""JSON_TYPE('{"foo": "1" }', '$.foo')""")

    def test_identity(self):
        self.validate_identity("CREATE TABLE t (c INT) PROPERTIES ('x'='y')")
        self.validate_identity("CREATE TABLE t (c INT) COMMENT 'c'")
        self.validate_identity("COALECSE(a, b, c, d)")
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS example_tbl_unique (user_id BIGINT NOT NULL, user_name VARCHAR(50) NOT NULL, city VARCHAR(20), age SMALLINT, sex TINYINT) UNIQUE KEY (user_id, user_name) DISTRIBUTED BY HASH (user_id) BUCKETS 10 PROPERTIES ('enable_unique_key_merge_on_write'='true')"
        )
        self.validate_identity("INSERT OVERWRITE TABLE test PARTITION(p1, p2) VALUES (1, 2)")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl")
        self.validate_identity("ANALYZE DATABASE db")
        self.validate_identity("ANALYZE TABLE TBL(c1, c2)")

    def test_key(self):
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) UNIQUE KEY (c1)")
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) DUPLICATE KEY (c1)")

    def test_distributed(self):
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 INT) UNIQUE KEY (c1) DISTRIBUTED BY HASH (c1)"
        )
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) DISTRIBUTED BY RANDOM")
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 INT) DISTRIBUTED BY RANDOM BUCKETS 1"
        )

    def test_partitionbyrange(self):
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (PARTITION `p201701` VALUES LESS THAN ('2017-02-01'), PARTITION `p201702` VALUES LESS THAN ('2017-03-01'))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (PARTITION `p201701` VALUES [('2017-01-01'), ('2017-02-01')), PARTITION `other` VALUES LESS THAN (MAXVALUE))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR)"
        )
