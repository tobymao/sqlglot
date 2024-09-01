from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestStarrocks(Validator):
    dialect = "starrocks"

    def test_ddl(self):
        # Test the different wider DECIMAL types
        self.validate_identity(
            "CREATE TABLE foo (col0 DECIMAL(9, 1), col1 DECIMAL32(9, 1), col2 DECIMAL64(18, 10), col3 DECIMAL128(38, 10)) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )
        # Test the PRIMARY KEY with ENGINE property
        self.validate_identity(
            "CREATE TABLE foo2 (col1 BIGINT) ENGINE=OLAP PRIMARY KEY (col1) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )
        # Test the PRIMARY KEY without ENGINE property
        self.validate_identity(
            "CREATE TABLE foo3 (col1 BIGINT) PRIMARY KEY (col1) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )
        # Test the DISTRIBUTED BY RANDOM
        self.validate_identity(
            "CREATE TABLE foo4 (col1 BIGINT) PRIMARY KEY (col1) DISTRIBUTED BY RANDOM BUCKETS 1"
        )
        # Test the DISTRIBUTED BY RANDOM AUTO BUCKETS
        self.validate_identity(
            "CREATE TABLE foo5 (col1 BIGINT) PRIMARY KEY (col1) DISTRIBUTED BY RANDOM"
        )
        # Test the duplicate key
        self.validate_identity(
            "CREATE TABLE data_integration.foo6 (col1 BIGINT, col2 BIGINT) ENGINE=OLAP DUPLICATE KEY (col1, col2) DISTRIBUTED BY HASH (col1) BUCKETS 1"
        )
        # Test the duplicate key, distribution and order by and properties
        self.validate_all(
            """CREATE TABLE if not exists data_integration.`sample_table` (
    `tenantid` varchar(1048576) NOT NULL COMMENT "",
    `shopsite` varchar(65533) NOT NULL COMMENT "shopsite",
    `id` varchar(65533) NOT NULL COMMENT "shopsite id",
    `create_day` date NOT NULL COMMENT "",
    `price` decimal128(38, 10) NULL COMMENT "test the bigdecimal",
    `seq` int(11) NULL COMMENT "order",
    `use_status` smallint(6) NULL COMMENT "0,1",
    `created_user` bigint(20) NULL COMMENT "create user",
    `created_time` datetime NULL COMMENT "create time"
) ENGINE=OLAP
PRIMARY KEY(`tenantid`, `shopsite`, `id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`tenantid`, `shopsite`, `id`) BUCKETS 10
ORDER BY (`tenantid`, `shopsite`, `id`)
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "enable_persistent_index" = "false",
    "replicated_storage" = "false",
    "storage_medium" = "HDD",
    "compression" = "LZ4"
)""",
            write={
                "starrocks": """CREATE TABLE IF NOT EXISTS data_integration.`sample_table` (`tenantid` VARCHAR(1048576) NOT NULL COMMENT '', `shopsite` VARCHAR(65533) NOT NULL COMMENT 'shopsite', `id` VARCHAR(65533) NOT NULL COMMENT 'shopsite id', `create_day` DATE NOT NULL COMMENT '', `price` DECIMAL128(38, 10) NULL COMMENT 'test the bigdecimal', `seq` INT(11) NULL COMMENT 'order', `use_status` SMALLINT(6) NULL COMMENT '0,1', `created_user` BIGINT(20) NULL COMMENT 'create user', `created_time` DATETIME NULL COMMENT 'create time') ENGINE=OLAP PRIMARY KEY (`tenantid`, `shopsite`, `id`) COMMENT='OLAP' DISTRIBUTED BY HASH (`tenantid`, `shopsite`, `id`) BUCKETS 10 ORDER BY (`tenantid`, `shopsite`, `id`) PROPERTIES ('replication_num'='1', 'in_memory'='false', 'enable_persistent_index'='false', 'replicated_storage'='false', 'storage_medium'='HDD', 'compression'='LZ4')""",
            },
        )

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity("SELECT [1, 2, 3]")
        self.validate_identity(
            """SELECT CAST(PARSE_JSON(fieldvalue) -> '00000000-0000-0000-0000-00000000' AS VARCHAR) AS `code` FROM (SELECT '{"00000000-0000-0000-0000-00000000":"code01"}') AS t(fieldvalue)"""
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
                "spark": r"""SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';')), scores)) t AS type, scores""",
                "databricks": r"""SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';')), scores)) t AS type, scores""",
                "starrocks": r"""SELECT id, t.type, t.scores FROM example_table, UNNEST(SPLIT(type, ';'), scores) AS t(type, scores)""",
                "hive": UnsupportedError,
            },
        )

        self.validate_all(
            r"""SELECT id, t.type, t.scores FROM example_table_2 CROSS JOIN LATERAL unnest(split(type, ";"), scores) AS t(type,scores)""",
            write={
                "spark": r"""SELECT id, t.type, t.scores FROM example_table_2 LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT('\\Q', ';')), scores)) t AS type, scores""",
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
