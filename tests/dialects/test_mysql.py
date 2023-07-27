from sqlglot import expressions as exp
from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_ddl(self):
        self.validate_identity("CREATE TABLE foo (id BIGINT)")
        self.validate_identity("UPDATE items SET items.price = 0 WHERE items.id >= 5 LIMIT 10")
        self.validate_identity("DELETE FROM t WHERE a <= 10 LIMIT 10")
        self.validate_identity("CREATE TABLE foo (a BIGINT, INDEX USING BTREE (b))")
        self.validate_identity("CREATE TABLE foo (a BIGINT, FULLTEXT INDEX (b))")
        self.validate_identity("CREATE TABLE foo (a BIGINT, SPATIAL INDEX (b))")
        self.validate_identity(
            "CREATE TABLE foo (a BIGINT, INDEX b USING HASH (c) COMMENT 'd' VISIBLE ENGINE_ATTRIBUTE = 'e' WITH PARSER foo)"
        )
        self.validate_identity(
            "DELETE t1 FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL"
        )
        self.validate_identity(
            "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id = t2.id AND t2.id = t3.id"
        )
        self.validate_identity(
            "DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id = t2.id AND t2.id = t3.id"
        )
        self.validate_identity(
            "INSERT IGNORE INTO subscribers (email) VALUES ('john.doe@gmail.com'), ('jane.smith@ibm.com')"
        )
        self.validate_identity(
            "INSERT INTO t1 (a, b, c) VALUES (1, 2, 3), (4, 5, 6) ON DUPLICATE KEY UPDATE c = VALUES(a) + VALUES(b)"
        )
        self.validate_identity(
            "INSERT INTO t1 (a, b) SELECT c, d FROM t2 UNION SELECT e, f FROM t3 ON DUPLICATE KEY UPDATE b = b + c"
        )
        self.validate_identity(
            "INSERT INTO t1 (a, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = c + 1"
        )
        self.validate_identity(
            "INSERT INTO x VALUES (1, 'a', 2.0) ON DUPLICATE KEY UPDATE x.id = 1"
        )

        self.validate_all(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
            write={
                "duckdb": "CREATE TABLE z (a INT)",
                "mysql": "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'",
                "spark": "CREATE TABLE z (a INT) COMMENT 'x'",
                "sqlite": "CREATE TABLE z (a INTEGER)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC",
            write={
                "mysql": "CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()) DEFAULT CHARACTER SET=utf8 ROW_FORMAT=DYNAMIC",
            },
        )
        self.validate_all(
            "CREATE TABLE x (id int not null auto_increment, primary key (id))",
            write={
                "sqlite": "CREATE TABLE x (id INTEGER NOT NULL AUTOINCREMENT PRIMARY KEY)",
            },
        )
        self.validate_all(
            "CREATE TABLE x (id int not null auto_increment)",
            write={
                "sqlite": "CREATE TABLE x (id INTEGER NOT NULL)",
            },
        )
        self.validate_all(
            "CREATE TABLE `foo` (`id` char(36) NOT NULL DEFAULT (uuid()), PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`))",
            write={
                "mysql": "CREATE TABLE `foo` (`id` CHAR(36) NOT NULL DEFAULT (UUID()), PRIMARY KEY (`id`), UNIQUE `id` (`id`))",
            },
        )
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE KEY d (b), KEY e (b))",
            write={
                "mysql": "CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE d (b), INDEX e (b))",
            },
        )

    def test_identity(self):
        self.validate_identity("SELECT 1 XOR 0")
        self.validate_identity("SELECT 1 && 0", "SELECT 1 AND 0")
        self.validate_identity("SELECT /*+ BKA(t1) NO_BKA(t2) */ * FROM t1 INNER JOIN t2")
        self.validate_identity("SELECT /*+ MERGE(dt) */ * FROM (SELECT * FROM t1) AS dt")
        self.validate_identity("SELECT /*+ INDEX(t, i) */ c1 FROM t WHERE c2 = 'value'")
        self.validate_identity("SELECT @a MEMBER OF(@c), @b MEMBER OF(@c)")
        self.validate_identity("SELECT JSON_ARRAY(4, 5) MEMBER OF('[[3,4],[4,5]]')")
        self.validate_identity("SELECT CAST('[4,5]' AS JSON) MEMBER OF('[[3,4],[4,5]]')")
        self.validate_identity("""SELECT 'ab' MEMBER OF('[23, "abc", 17, "ab", 10]')""")
        self.validate_identity("""SELECT * FROM foo WHERE 'ab' MEMBER OF(content)""")
        self.validate_identity("CAST(x AS ENUM('a', 'b'))")
        self.validate_identity("CAST(x AS SET('a', 'b'))")
        self.validate_identity("SELECT CURRENT_TIMESTAMP(6)")
        self.validate_identity("x ->> '$.name'")
        self.validate_identity("SELECT CAST(`a`.`b` AS CHAR) FROM foo")
        self.validate_identity("SELECT TRIM(LEADING 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(TRAILING 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM(BOTH 'bla' FROM ' XXX ')")
        self.validate_identity("SELECT TRIM('bla' FROM ' XXX ')")
        self.validate_identity("@@GLOBAL.max_connections")
        self.validate_identity("CREATE TABLE A LIKE B")
        self.validate_identity("SELECT * FROM t1, t2 FOR SHARE OF t1, t2 SKIP LOCKED")
        self.validate_identity(
            """SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, '$.value'))"""
        )
        self.validate_identity(
            "SELECT * FROM t1, t2, t3 FOR SHARE OF t1 NOWAIT FOR UPDATE OF t2, t3 SKIP LOCKED"
        )
        self.validate_identity(
            """SELECT * FROM foo WHERE 3 MEMBER OF(info->'$.value')""",
            """SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, '$.value'))""",
        )

        # Index hints
        self.validate_identity(
            "SELECT * FROM table1 USE INDEX (col1_index, col2_index) WHERE col1 = 1 AND col2 = 2 AND col3 = 3"
        )
        self.validate_identity(
            "SELECT * FROM table1 IGNORE INDEX (col3_index) WHERE col1 = 1 AND col2 = 2 AND col3 = 3"
        )
        self.validate_identity(
            "SELECT * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2) ORDER BY a"
        )
        self.validate_identity("SELECT * FROM t1 USE INDEX (i1) USE INDEX (i1, i1)")
        self.validate_identity("SELECT * FROM t1 USE INDEX FOR JOIN (i1) FORCE INDEX FOR JOIN (i2)")
        self.validate_identity(
            "SELECT * FROM t1 USE INDEX () IGNORE INDEX (i2) USE INDEX (i1) USE INDEX (i2)"
        )

        # SET Commands
        self.validate_identity("SET @var_name = expr")
        self.validate_identity("SET @name = 43")
        self.validate_identity("SET @total_tax = (SELECT SUM(tax) FROM taxable_transactions)")
        self.validate_identity("SET GLOBAL max_connections = 1000")
        self.validate_identity("SET @@GLOBAL.max_connections = 1000")
        self.validate_identity("SET SESSION sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET LOCAL sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET @@SESSION.sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET @@LOCAL.sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET @@sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET sql_mode = 'TRADITIONAL'")
        self.validate_identity("SET PERSIST max_connections = 1000")
        self.validate_identity("SET @@PERSIST.max_connections = 1000")
        self.validate_identity("SET PERSIST_ONLY back_log = 100")
        self.validate_identity("SET @@PERSIST_ONLY.back_log = 100")
        self.validate_identity("SET @@SESSION.max_join_size = DEFAULT")
        self.validate_identity("SET @@SESSION.max_join_size = @@GLOBAL.max_join_size")
        self.validate_identity("SET @x = 1, SESSION sql_mode = ''")
        self.validate_identity("SET GLOBAL max_connections = 1000, sort_buffer_size = 1000000")
        self.validate_identity("SET @@GLOBAL.sort_buffer_size = 50000, sort_buffer_size = 1000000")
        self.validate_identity("SET CHARACTER SET 'utf8'")
        self.validate_identity("SET CHARACTER SET utf8")
        self.validate_identity("SET CHARACTER SET DEFAULT")
        self.validate_identity("SET NAMES 'utf8'")
        self.validate_identity("SET NAMES DEFAULT")
        self.validate_identity("SET NAMES 'utf8' COLLATE 'utf8_unicode_ci'")
        self.validate_identity("SET NAMES utf8 COLLATE utf8_unicode_ci")
        self.validate_identity("SET autocommit = ON")
        self.validate_identity("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        self.validate_identity("SET TRANSACTION READ ONLY")
        self.validate_identity("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE")
        self.validate_identity("SELECT SCHEMA()")
        self.validate_identity("SELECT DATABASE()")
        self.validate_identity(
            "SET GLOBAL sort_buffer_size = 1000000, SESSION sort_buffer_size = 1000000"
        )
        self.validate_identity(
            "SET @@GLOBAL.sort_buffer_size = 1000000, @@LOCAL.sort_buffer_size = 1000000"
        )

    def test_types(self):
        self.validate_all(
            "CAST(x AS MEDIUMTEXT) + CAST(y AS LONGTEXT)",
            read={
                "mysql": "CAST(x AS MEDIUMTEXT) + CAST(y AS LONGTEXT)",
            },
            write={
                "spark": "CAST(x AS TEXT) + CAST(y AS TEXT)",
            },
        )
        self.validate_all(
            "CAST(x AS MEDIUMBLOB) + CAST(y AS LONGBLOB)",
            read={
                "mysql": "CAST(x AS MEDIUMBLOB) + CAST(y AS LONGBLOB)",
            },
            write={
                "spark": "CAST(x AS BLOB) + CAST(y AS BLOB)",
            },
        )

    def test_canonical_functions(self):
        self.validate_identity("SELECT LEFT('str', 2)", "SELECT LEFT('str', 2)")
        self.validate_identity("SELECT INSTR('str', 'substr')", "SELECT LOCATE('substr', 'str')")
        self.validate_identity("SELECT UCASE('foo')", "SELECT UPPER('foo')")
        self.validate_identity("SELECT LCASE('foo')", "SELECT LOWER('foo')")
        self.validate_identity(
            "SELECT DAY_OF_MONTH('2023-01-01')", "SELECT DAYOFMONTH('2023-01-01')"
        )
        self.validate_identity("SELECT DAY_OF_WEEK('2023-01-01')", "SELECT DAYOFWEEK('2023-01-01')")
        self.validate_identity("SELECT DAY_OF_YEAR('2023-01-01')", "SELECT DAYOFYEAR('2023-01-01')")
        self.validate_identity(
            "SELECT WEEK_OF_YEAR('2023-01-01')", "SELECT WEEKOFYEAR('2023-01-01')"
        )

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
            "N'some text'",
            read={
                "mysql": "n'some text'",
            },
            write={
                "mysql": "N'some text'",
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
        write_CC = {
            "bigquery": "SELECT 0xCC",
            "clickhouse": "SELECT 0xCC",
            "databricks": "SELECT 204",
            "drill": "SELECT 204",
            "duckdb": "SELECT 204",
            "hive": "SELECT 204",
            "mysql": "SELECT x'CC'",
            "oracle": "SELECT 204",
            "postgres": "SELECT x'CC'",
            "presto": "SELECT 204",
            "redshift": "SELECT 204",
            "snowflake": "SELECT x'CC'",
            "spark": "SELECT X'CC'",
            "sqlite": "SELECT x'CC'",
            "starrocks": "SELECT x'CC'",
            "tableau": "SELECT 204",
            "teradata": "SELECT 204",
            "trino": "SELECT X'CC'",
            "tsql": "SELECT 0xCC",
        }
        write_CC_with_leading_zeros = {
            "bigquery": "SELECT 0x0000CC",
            "clickhouse": "SELECT 0x0000CC",
            "databricks": "SELECT 204",
            "drill": "SELECT 204",
            "duckdb": "SELECT 204",
            "hive": "SELECT 204",
            "mysql": "SELECT x'0000CC'",
            "oracle": "SELECT 204",
            "postgres": "SELECT x'0000CC'",
            "presto": "SELECT 204",
            "redshift": "SELECT 204",
            "snowflake": "SELECT x'0000CC'",
            "spark": "SELECT X'0000CC'",
            "sqlite": "SELECT x'0000CC'",
            "starrocks": "SELECT x'0000CC'",
            "tableau": "SELECT 204",
            "teradata": "SELECT 204",
            "trino": "SELECT X'0000CC'",
            "tsql": "SELECT 0x0000CC",
        }

        self.validate_all("SELECT X'1A'", write={"mysql": "SELECT x'1A'"})
        self.validate_all("SELECT 0xz", write={"mysql": "SELECT `0xz`"})
        self.validate_all("SELECT 0xCC", write=write_CC)
        self.validate_all("SELECT 0xCC ", write=write_CC)
        self.validate_all("SELECT x'CC'", write=write_CC)
        self.validate_all("SELECT 0x0000CC", write=write_CC_with_leading_zeros)
        self.validate_all("SELECT x'0000CC'", write=write_CC_with_leading_zeros)

    def test_bits_literal(self):
        write_1011 = {
            "bigquery": "SELECT 11",
            "clickhouse": "SELECT 0b1011",
            "databricks": "SELECT 11",
            "drill": "SELECT 11",
            "hive": "SELECT 11",
            "mysql": "SELECT b'1011'",
            "oracle": "SELECT 11",
            "postgres": "SELECT b'1011'",
            "presto": "SELECT 11",
            "redshift": "SELECT 11",
            "snowflake": "SELECT 11",
            "spark": "SELECT 11",
            "sqlite": "SELECT 11",
            "mysql": "SELECT b'1011'",
            "tableau": "SELECT 11",
            "teradata": "SELECT 11",
            "trino": "SELECT 11",
            "tsql": "SELECT 11",
        }

        self.validate_all("SELECT 0b1011", write=write_1011)
        self.validate_all("SELECT b'1011'", write=write_1011)

    def test_string_literals(self):
        self.validate_all(
            'SELECT "2021-01-01" + INTERVAL 1 MONTH',
            write={
                "mysql": "SELECT '2021-01-01' + INTERVAL '1' MONTH",
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

    def test_match_against(self):
        self.validate_all(
            "MATCH(col1, col2, col3) AGAINST('abc')",
            read={
                "": "MATCH(col1, col2, col3) AGAINST('abc')",
                "mysql": "MATCH(col1, col2, col3) AGAINST('abc')",
            },
            write={
                "": "MATCH(col1, col2, col3) AGAINST('abc')",
                "mysql": "MATCH(col1, col2, col3) AGAINST('abc')",
            },
        )
        self.validate_all(
            "MATCH(col1, col2) AGAINST('abc' IN NATURAL LANGUAGE MODE)",
            write={"mysql": "MATCH(col1, col2) AGAINST('abc' IN NATURAL LANGUAGE MODE)"},
        )
        self.validate_all(
            "MATCH(col1, col2) AGAINST('abc' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
            write={
                "mysql": "MATCH(col1, col2) AGAINST('abc' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)"
            },
        )
        self.validate_all(
            "MATCH(col1, col2) AGAINST('abc' IN BOOLEAN MODE)",
            write={"mysql": "MATCH(col1, col2) AGAINST('abc' IN BOOLEAN MODE)"},
        )
        self.validate_all(
            "MATCH(col1, col2) AGAINST('abc' WITH QUERY EXPANSION)",
            write={"mysql": "MATCH(col1, col2) AGAINST('abc' WITH QUERY EXPANSION)"},
        )
        self.validate_all(
            "MATCH(a.b) AGAINST('abc')",
            write={"mysql": "MATCH(a.b) AGAINST('abc')"},
        )

    def test_date_format(self):
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%Y')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%Y')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMPNTZ), 'yyyy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%m')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%m')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMPNTZ), 'mm')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%d')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%d')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMPNTZ), 'DD')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%Y-%m-%d')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%Y-%m-%d')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMPNTZ), 'yyyy-mm-DD')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15 22:23:34', '%H')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15 22:23:34', '%H')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15 22:23:34' AS TIMESTAMPNTZ), 'hh24')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%w')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%w')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMPNTZ), 'dy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')",
            write={
                "mysql": "SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')",
                "snowflake": "SELECT TO_CHAR(CAST('2009-10-04 22:23:00' AS TIMESTAMPNTZ), 'DY mmmm yyyy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s')",
            write={
                "mysql": "SELECT DATE_FORMAT('2007-10-04 22:23:00', '%T')",
                "snowflake": "SELECT TO_CHAR(CAST('2007-10-04 22:23:00' AS TIMESTAMPNTZ), 'hh24:mi:ss')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('1900-10-04 22:23:00', '%d %y %a %d %m %b')",
            write={
                "mysql": "SELECT DATE_FORMAT('1900-10-04 22:23:00', '%d %y %W %d %m %b')",
                "snowflake": "SELECT TO_CHAR(CAST('1900-10-04 22:23:00' AS TIMESTAMPNTZ), 'DD yy DY DD mm mon')",
            },
        )

    def test_mysql_time(self):
        self.validate_identity("FROM_UNIXTIME(a, b)")
        self.validate_identity("FROM_UNIXTIME(a, b, c)")
        self.validate_identity("TIME_STR_TO_UNIX(x)", "UNIX_TIMESTAMP(x)")

    def test_mysql(self):
        self.validate_all(
            "CAST(x AS TEXT)",
            write={
                "mysql": "CAST(x AS CHAR)",
                "presto": "CAST(x AS VARCHAR)",
                "starrocks": "CAST(x AS STRING)",
            },
        )
        self.validate_all("CAST(x AS SIGNED)", write={"mysql": "CAST(x AS SIGNED)"})
        self.validate_all("CAST(x AS SIGNED INTEGER)", write={"mysql": "CAST(x AS SIGNED)"})
        self.validate_all("CAST(x AS UNSIGNED)", write={"mysql": "CAST(x AS UNSIGNED)"})
        self.validate_all("CAST(x AS UNSIGNED INTEGER)", write={"mysql": "CAST(x AS UNSIGNED)"})
        self.validate_all("TIME_STR_TO_TIME(x)", write={"mysql": "CAST(x AS TIMESTAMP)"})
        self.validate_all(
            """SELECT 17 MEMBER OF('[23, "abc", 17, "ab", 10]')""",
            write={
                "": """SELECT JSON_ARRAY_CONTAINS(17, '[23, "abc", 17, "ab", 10]')""",
                "mysql": """SELECT 17 MEMBER OF('[23, "abc", 17, "ab", 10]')""",
            },
        )
        self.validate_all(
            "SELECT DATE_ADD('2023-06-23 12:00:00', INTERVAL 2 * 2 MONTH) FROM foo",
            write={
                "mysql": "SELECT DATE_ADD('2023-06-23 12:00:00', INTERVAL (2 * 2) MONTH) FROM foo",
            },
        )
        self.validate_all(
            "SELECT * FROM t LOCK IN SHARE MODE", write={"mysql": "SELECT * FROM t FOR SHARE"}
        )
        self.validate_all(
            "SELECT DATE(DATE_SUB(`dt`, INTERVAL DAYOFMONTH(`dt`) - 1 DAY)) AS __timestamp FROM tableT",
            write={
                "mysql": "SELECT DATE(DATE_SUB(`dt`, INTERVAL (DAYOFMONTH(`dt`) - 1) DAY)) AS __timestamp FROM tableT",
            },
        )
        self.validate_all(
            "SELECT a FROM tbl FOR UPDATE",
            write={
                "": "SELECT a FROM tbl",
                "mysql": "SELECT a FROM tbl FOR UPDATE",
                "oracle": "SELECT a FROM tbl FOR UPDATE",
                "postgres": "SELECT a FROM tbl FOR UPDATE",
                "redshift": "SELECT a FROM tbl",
                "tsql": "SELECT a FROM tbl FOR UPDATE",
            },
        )
        self.validate_all(
            "SELECT a FROM tbl FOR SHARE",
            write={
                "": "SELECT a FROM tbl",
                "mysql": "SELECT a FROM tbl FOR SHARE",
                "oracle": "SELECT a FROM tbl FOR SHARE",
                "postgres": "SELECT a FROM tbl FOR SHARE",
                "tsql": "SELECT a FROM tbl FOR SHARE",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT x ORDER BY y DESC)",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR ',')",
                "sqlite": "GROUP_CONCAT(DISTINCT x)",
                "tsql": "STRING_AGG(x, ',') WITHIN GROUP (ORDER BY y DESC)",
                "postgres": "STRING_AGG(DISTINCT x, ',' ORDER BY y DESC NULLS LAST)",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(x ORDER BY y SEPARATOR z)",
            write={
                "mysql": "GROUP_CONCAT(x ORDER BY y SEPARATOR z)",
                "sqlite": "GROUP_CONCAT(x, z)",
                "tsql": "STRING_AGG(x, z) WITHIN GROUP (ORDER BY y)",
                "postgres": "STRING_AGG(x, z ORDER BY y NULLS FIRST)",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(DISTINCT x, '')",
                "tsql": "STRING_AGG(x, '') WITHIN GROUP (ORDER BY y DESC)",
                "postgres": "STRING_AGG(DISTINCT x, '' ORDER BY y DESC NULLS LAST)",
            },
        )
        self.validate_identity(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )
        self.validate_identity(
            "CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )
        self.validate_identity(
            "CREATE TABLE z (a INT DEFAULT NULL, PRIMARY KEY (a)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='x'"
        )

        self.validate_all(
            """
            CREATE TABLE `t_customer_account` (
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `customer_id` int(11) DEFAULT NULL COMMENT '客户id',
              `bank` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '行别',
              `account_no` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '账号',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='客户账户表'
            """,
            write={
                "mysql": """CREATE TABLE `t_customer_account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `customer_id` INT(11) DEFAULT NULL COMMENT '客户id',
  `bank` VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '行别',
  `account_no` VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '账号',
  PRIMARY KEY (`id`)
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

        self.validate_all(
            "SHOW INDEX FROM bar.foo", write={"mysql": "SHOW INDEX FROM foo FROM bar"}
        )

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
        self.assertEqual(show.args["types"][0].name, "BLOCK IO")

        show = self.validate_identity(
            "SHOW PROFILE BLOCK IO, PAGE FAULTS FOR QUERY 1 OFFSET 2 LIMIT 3"
        )
        self.assertEqual(show.args["types"][0].name, "BLOCK IO")
        self.assertEqual(show.args["types"][1].name, "PAGE FAULTS")
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

    def test_set_variable(self):
        cmd = self.parse_one("SET SESSION x = 1")
        item = cmd.expressions[0]
        self.assertEqual(item.text("kind"), "SESSION")
        self.assertIsInstance(item.this, exp.EQ)
        self.assertEqual(item.this.left.name, "x")
        self.assertEqual(item.this.right.name, "1")

        cmd = self.parse_one("SET @@GLOBAL.x = @@GLOBAL.y")
        item = cmd.expressions[0]
        self.assertEqual(item.text("kind"), "")
        self.assertIsInstance(item.this, exp.EQ)
        self.assertIsInstance(item.this.left, exp.SessionParameter)
        self.assertIsInstance(item.this.right, exp.SessionParameter)

        cmd = self.parse_one("SET NAMES 'charset_name' COLLATE 'collation_name'")
        item = cmd.expressions[0]
        self.assertEqual(item.text("kind"), "NAMES")
        self.assertEqual(item.name, "charset_name")
        self.assertEqual(item.text("collate"), "collation_name")

        cmd = self.parse_one("SET CHARSET DEFAULT")
        item = cmd.expressions[0]
        self.assertEqual(item.text("kind"), "CHARACTER SET")
        self.assertEqual(item.this.name, "DEFAULT")

        cmd = self.parse_one("SET x = 1, y = 2")
        self.assertEqual(len(cmd.expressions), 2)
