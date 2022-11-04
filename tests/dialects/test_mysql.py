from sqlglot import expressions as exp
from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
            write={
                "duckdb": "CREATE TABLE z (a INT)",
                "mysql": "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
                "spark": "CREATE TABLE z (a INT) COMMENT 'x'",
            },
        )

    def test_identity(self):
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT TRIM(LEADING 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(TRAILING 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(BOTH 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM('bla' FROM ' XXX ')")

    def test_escape(self):
        self.validate_all(
            r"'a \' b '' '",
            write={
                "mysql": r"'a '' b '' '",
                "spark": r"'a \' b \' '",
            },
        )

    def test_introducers(self):
        self.validate_all(
            "_utf8mb4 'hola'",
            read={
                "mysql": "_utf8mb4'hola'",
            },
            write={
                "mysql": "_utf8mb4 'hola'",
            },
        )
        self.validate_all(
            "N 'some text'",
            read={
                "mysql": "N'some text'",
            },
            write={
                "mysql": "N 'some text'",
            },
        )
        self.validate_all(
            "_latin1 x'4D7953514C'",
            read={
                "mysql": "_latin1 X'4D7953514C'",
            },
            write={
                "mysql": "_latin1 x'4D7953514C'",
            },
        )

    def test_hexadecimal_literal(self):
        self.validate_all(
            "SELECT 0xCC",
            write={
                "mysql": "SELECT x'CC'",
                "sqlite": "SELECT x'CC'",
                "spark": "SELECT X'CC'",
                "trino": "SELECT X'CC'",
                "bigquery": "SELECT 0xCC",
                "oracle": "SELECT 204",
            },
        )
        self.validate_all(
            "SELECT X'1A'",
            write={
                "mysql": "SELECT x'1A'",
            },
        )
        self.validate_all(
            "SELECT 0xz",
            write={
                "mysql": "SELECT `0xz`",
            },
        )

    def test_bits_literal(self):
        self.validate_all(
            "SELECT 0b1011",
            write={
                "mysql": "SELECT b'1011'",
                "postgres": "SELECT b'1011'",
                "oracle": "SELECT 11",
            },
        )
        self.validate_all(
            "SELECT B'1011'",
            write={
                "mysql": "SELECT b'1011'",
                "postgres": "SELECT b'1011'",
                "oracle": "SELECT 11",
            },
        )

    def test_string_literals(self):
        self.validate_all(
            'SELECT "2021-01-01" + INTERVAL 1 MONTH',
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL 1 MONTH",
            },
        )

    def test_convert(self):
        self.validate_all(
            "CONVERT(x USING latin1)",
            write={
                "mysql": "CAST(x AS CHAR CHARACTER SET latin1)",
            },
        )
        self.validate_all(
            "CAST(x AS CHAR CHARACTER SET latin1)",
            write={
                "mysql": "CAST(x AS CHAR CHARACTER SET latin1)",
            },
        )

    def test_hash_comments(self):
        self.validate_all(
            "SELECT 1 # arbitrary content,,, until end-of-line",
            write={
                "mysql": "SELECT 1",
            },
        )

    def test_mysql(self):
        self.validate_all(
            "GROUP_CONCAT(DISTINCT x ORDER BY y DESC)",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR ',')",
                "sqlite": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC)",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC, '')",
            },
        )
        self.validate_identity(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )
        self.validate_identity(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )
        self.validate_identity(
            "CREATE TABLE z (a INT DEFAULT NULL, PRIMARY KEY(a)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )

        self.validate_all(
            """
            CREATE TABLE `t_customer_account` (
              "id" int(11) NOT NULL AUTO_INCREMENT,
              "customer_id" int(11) DEFAULT NULL COMMENT '客户id',
              "bank" varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '行别',
              "account_no" varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '账号',
              PRIMARY KEY ("id")
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='客户账户表'
            """,
            write={
                "mysql": """CREATE TABLE `t_customer_account` (
  'id' INT(11) NOT NULL AUTO_INCREMENT,
  'customer_id' INT(11) DEFAULT NULL COMMENT '客户id',
  'bank' VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '行别',
  'account_no' VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '账号',
  PRIMARY KEY('id')
)
ENGINE=InnoDB
AUTO_INCREMENT=1
DEFAULT CHARACTER SET=utf8
COLLATE=utf8_bin
COMMENT='客户账户表'"""
            },
            pretty=True,
        )

    def test_show_simple(self):
        for key, write_key in [
            ("BINARY LOGS", "BINARY LOGS"),
            ("MASTER LOGS", "BINARY LOGS"),
            ("STORAGE ENGINES", "ENGINES"),
            ("ENGINES", "ENGINES"),
            ("EVENTS", "EVENTS"),
            ("MASTER STATUS", "MASTER STATUS"),
            ("PLUGINS", "PLUGINS"),
            ("PRIVILEGES", "PRIVILEGES"),
            ("PROFILES", "PROFILES"),
            ("REPLICAS", "REPLICAS"),
            ("SLAVE HOSTS", "REPLICAS"),
        ]:
            show = self.validate_identity(f"SHOW {key}", f"SHOW {write_key}")
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, write_key)

    def test_show_events(self):
        for key in ["BINLOG", "RELAYLOG"]:
            show = self.validate_identity(f"SHOW {key} EVENTS")
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, f"{key} EVENTS")

            show = self.validate_identity(f"SHOW {key} EVENTS IN 'log' FROM 1 LIMIT 2, 3")
            self.assertEqual(show.text("log"), "log")
            self.assertEqual(show.text("position"), "1")
            self.assertEqual(show.text("limit"), "3")
            self.assertEqual(show.text("offset"), "2")

            show = self.validate_identity(f"SHOW {key} EVENTS LIMIT 1")
            self.assertEqual(show.text("limit"), "1")
            self.assertIsNone(show.args.get("offset"))

    def test_show_like_or_where(self):
        for key, write_key in [
            ("CHARSET", "CHARACTER SET"),
            ("CHARACTER SET", "CHARACTER SET"),
            ("COLLATION", "COLLATION"),
            ("DATABASES", "DATABASES"),
            ("FUNCTION STATUS", "FUNCTION STATUS"),
            ("PROCEDURE STATUS", "PROCEDURE STATUS"),
            ("GLOBAL STATUS", "GLOBAL STATUS"),
            ("SESSION STATUS", "STATUS"),
            ("STATUS", "STATUS"),
            ("GLOBAL VARIABLES", "GLOBAL VARIABLES"),
            ("SESSION VARIABLES", "VARIABLES"),
            ("VARIABLES", "VARIABLES"),
        ]:
            expected_name = write_key.strip("GLOBAL").strip()
            template = "SHOW {}"
            show = self.validate_identity(template.format(key), template.format(write_key))
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, expected_name)

            template = "SHOW {} LIKE '%foo%'"
            show = self.validate_identity(template.format(key), template.format(write_key))
            self.assertIsInstance(show, exp.Show)
            self.assertIsInstance(show.args["like"], exp.Literal)
            self.assertEqual(show.text("like"), "%foo%")

            template = "SHOW {} WHERE Column_name LIKE '%foo%'"
            show = self.validate_identity(template.format(key), template.format(write_key))
            self.assertIsInstance(show, exp.Show)
            self.assertIsInstance(show.args["where"], exp.Where)
            self.assertEqual(show.args["where"].sql(), "WHERE Column_name LIKE '%foo%'")

    def test_show_columns(self):
        show = self.validate_identity("SHOW COLUMNS FROM tbl_name")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "COLUMNS")
        self.assertEqual(show.text("target"), "tbl_name")
        self.assertFalse(show.args["full"])

        show = self.validate_identity("SHOW FULL COLUMNS FROM tbl_name FROM db_name LIKE '%foo%'")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.text("target"), "tbl_name")
        self.assertTrue(show.args["full"])
        self.assertEqual(show.text("db"), "db_name")
        self.assertIsInstance(show.args["like"], exp.Literal)
        self.assertEqual(show.text("like"), "%foo%")

    def test_show_name(self):
        for key in [
            "CREATE DATABASE",
            "CREATE EVENT",
            "CREATE FUNCTION",
            "CREATE PROCEDURE",
            "CREATE TABLE",
            "CREATE TRIGGER",
            "CREATE VIEW",
            "FUNCTION CODE",
            "PROCEDURE CODE",
        ]:
            show = self.validate_identity(f"SHOW {key} foo")
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, key)
            self.assertEqual(show.text("target"), "foo")

    def test_show_grants(self):
        show = self.validate_identity(f"SHOW GRANTS FOR foo")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "GRANTS")
        self.assertEqual(show.text("target"), "foo")

    def test_show_engine(self):
        show = self.validate_identity("SHOW ENGINE foo STATUS")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "ENGINE")
        self.assertEqual(show.text("target"), "foo")
        self.assertFalse(show.args["mutex"])

        show = self.validate_identity("SHOW ENGINE foo MUTEX")
        self.assertEqual(show.name, "ENGINE")
        self.assertEqual(show.text("target"), "foo")
        self.assertTrue(show.args["mutex"])

    def test_show_errors(self):
        for key in ["ERRORS", "WARNINGS"]:
            show = self.validate_identity(f"SHOW {key}")
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, key)

            show = self.validate_identity(f"SHOW {key} LIMIT 2, 3")
            self.assertEqual(show.text("limit"), "3")
            self.assertEqual(show.text("offset"), "2")

    def test_show_index(self):
        show = self.validate_identity("SHOW INDEX FROM foo")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "INDEX")
        self.assertEqual(show.text("target"), "foo")

        show = self.validate_identity("SHOW INDEX FROM foo FROM bar")
        self.assertEqual(show.text("db"), "bar")

    def test_show_db_like_or_where_sql(self):
        for key in [
            "OPEN TABLES",
            "TABLE STATUS",
            "TRIGGERS",
        ]:
            show = self.validate_identity(f"SHOW {key}")
            self.assertIsInstance(show, exp.Show)
            self.assertEqual(show.name, key)

            show = self.validate_identity(f"SHOW {key} FROM db_name")
            self.assertEqual(show.name, key)
            self.assertEqual(show.text("db"), "db_name")

            show = self.validate_identity(f"SHOW {key} LIKE '%foo%'")
            self.assertEqual(show.name, key)
            self.assertIsInstance(show.args["like"], exp.Literal)
            self.assertEqual(show.text("like"), "%foo%")

            show = self.validate_identity(f"SHOW {key} WHERE Column_name LIKE '%foo%'")
            self.assertEqual(show.name, key)
            self.assertIsInstance(show.args["where"], exp.Where)
            self.assertEqual(show.args["where"].sql(), "WHERE Column_name LIKE '%foo%'")

    def test_show_processlist(self):
        show = self.validate_identity("SHOW PROCESSLIST")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "PROCESSLIST")
        self.assertFalse(show.args["full"])

        show = self.validate_identity("SHOW FULL PROCESSLIST")
        self.assertEqual(show.name, "PROCESSLIST")
        self.assertTrue(show.args["full"])

    def test_show_profile(self):
        show = self.validate_identity("SHOW PROFILE")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "PROFILE")

        show = self.validate_identity("SHOW PROFILE BLOCK IO")
        self.assertEqual(show.args["types"], ["BLOCK IO"])

        show = self.validate_identity(
            "SHOW PROFILE BLOCK IO, PAGE FAULTS FOR QUERY 1 OFFSET 2 LIMIT 3"
        )
        self.assertEqual(show.args["types"], ["BLOCK IO", "PAGE FAULTS"])
        self.assertEqual(show.text("query"), "1")
        self.assertEqual(show.text("offset"), "2")
        self.assertEqual(show.text("limit"), "3")

    def test_show_replica_status(self):
        show = self.validate_identity("SHOW REPLICA STATUS")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "REPLICA STATUS")

        show = self.validate_identity("SHOW SLAVE STATUS", "SHOW REPLICA STATUS")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "REPLICA STATUS")

        show = self.validate_identity("SHOW REPLICA STATUS FOR CHANNEL channel_name")
        self.assertEqual(show.text("channel"), "channel_name")

    def test_show_tables(self):
        show = self.validate_identity("SHOW TABLES")
        self.assertIsInstance(show, exp.Show)
        self.assertEqual(show.name, "TABLES")

        show = self.validate_identity("SHOW FULL TABLES FROM db_name LIKE '%foo%'")
        self.assertTrue(show.args["full"])
        self.assertEqual(show.text("db"), "db_name")
        self.assertIsInstance(show.args["like"], exp.Literal)
        self.assertEqual(show.text("like"), "%foo%")
