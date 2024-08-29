from tests.dialects.test_dialect import Validator


class TestStarrocks(Validator):
    dialect = "starrocks"

    def test_ddl(self):
        self.validate_identity("CREATE TABLE foo (col VARCHAR(50))")
        self.validate_all(
            """CREATE TABLE if not exists `sample_table` (
                        `tenantid` varchar(1048576) NULL COMMENT "",
                        `create_day` date NOT NULL COMMENT "",
                        `shopsite` varchar(65533) NOT NULL COMMENT "shopsite",
                        `id` varchar(65533) NOT NULL COMMENT "shopsite id",
                        `price` decimal128(38, 10) NULL COMMENT "test the bigdecimal",
                        `seq` int(11) NULL COMMENT "order",
                        `use_status` smallint(6) NULL COMMENT "0,1",
                        `created_user` bigint(20) NULL COMMENT "create user",
                        `created_time` datetime NULL COMMENT "create time",
                    ) ENGINE=OLAP
                    DUPLICATE KEY(tenantid)
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
                "hive": """CREATE TABLE IF NOT EXISTS `sample_table` (`tenantid` STRING COMMENT '', `create_day` DATE NOT NULL COMMENT '', `shopsite` VARCHAR(65533) NOT NULL COMMENT 'shopsite', `id` VARCHAR(65533) NOT NULL COMMENT 'shopsite id', `price` DECIMAL(10, 38) COMMENT 'test the bigdecimal', `seq` INT COMMENT 'order', `use_status` SMALLINT COMMENT '0,1', `created_user` BIGINT COMMENT 'create user', `created_time` TIMESTAMP COMMENT 'create time', PRIMARY KEY (`tenantid`, `shopsite`, `id`)) COMMENT 'OLAP' CLUSTERED BY (`tenantid`, `shopsite`, `id`) SORTED BY (`tenantid`, `shopsite`, `id`) INTO 10 BUCKETS TBLPROPERTIES ('replication_num'='1', 'in_memory'='false', 'enable_persistent_index'='false', 'replicated_storage'='false', 'storage_medium'='HDD', 'compression'='LZ4')""",
                "starrocks": """CREATE TABLE IF NOT EXISTS `sample_table` (`tenantid` VARCHAR(1048576) NULL COMMENT '', `create_day` DATE NOT NULL COMMENT '', `shopsite` VARCHAR(65533) NOT NULL COMMENT 'shopsite', `id` VARCHAR(65533) NOT NULL COMMENT 'shopsite id', `price` BIGDECIMAL(38, 10) NULL COMMENT 'test the bigdecimal', `seq` INT(11) NULL COMMENT 'order', `use_status` SMALLINT(6) NULL COMMENT '0,1', `created_user` BIGINT(20) NULL COMMENT 'create user', `created_time` DATETIME NULL COMMENT 'create time', PRIMARY KEY (`tenantid`, `shopsite`, `id`)) ENGINE=OLAP DUPLICATE KEY (tenantid) COMMENT='OLAP' DISTRIBUTED BY HASH (`tenantid`, `shopsite`, `id`) BUCKETS 10 ORDER BY (`tenantid`, `shopsite`, `id`) PROPERTIES ('replication_num'='1', 'in_memory'='false', 'enable_persistent_index'='false', 'replicated_storage'='false', 'storage_medium'='HDD', 'compression'='LZ4')""",
            },
        )

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity("SELECT [1, 2, 3]")

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
