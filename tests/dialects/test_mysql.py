import unittest
import sys

from sqlglot import UnsupportedError, expressions as exp
from sqlglot.dialects.mysql import MySQL
from tests.dialects.test_dialect import Validator


class TestMySQL(Validator):
    dialect = "mysql"

    def test_ddl(self):
        for t in ("BIGINT", "INT", "MEDIUMINT", "SMALLINT", "TINYINT"):
            self.validate_identity(f"CREATE TABLE t (id {t} UNSIGNED)")
            self.validate_identity(f"CREATE TABLE t (id {t}(10) UNSIGNED)")

        self.validate_identity("CREATE TABLE bar (abacate DOUBLE(10, 2) UNSIGNED)")
        self.validate_identity("CREATE TABLE t (id DECIMAL(20, 4) UNSIGNED)")
        self.validate_identity("CREATE TABLE foo (a BIGINT, UNIQUE (b) USING BTREE)")
        self.validate_identity("CREATE TABLE foo (id BIGINT)")
        self.validate_identity("CREATE TABLE 00f (1d BIGINT)")
        self.validate_identity("CREATE TABLE temp (id SERIAL PRIMARY KEY)")
        self.validate_identity("UPDATE items SET items.price = 0 WHERE items.id >= 5 LIMIT 10")
        self.validate_identity("DELETE FROM t WHERE a <= 10 LIMIT 10")
        self.validate_identity("CREATE TABLE foo (a BIGINT, INDEX USING BTREE (b))")
        self.validate_identity("CREATE TABLE foo (a BIGINT, FULLTEXT INDEX (b))")
        self.validate_identity("CREATE TABLE foo (a BIGINT, SPATIAL INDEX (b))")
        self.validate_identity("ALTER TABLE t1 ADD COLUMN x INT, ALGORITHM=INPLACE, LOCK=EXCLUSIVE")
        self.validate_identity("ALTER TABLE t ADD INDEX `i` (`c`)")
        self.validate_identity("ALTER TABLE t ADD UNIQUE `i` (`c`)")
        self.validate_identity("ALTER TABLE test_table MODIFY COLUMN test_column LONGTEXT")
        self.validate_identity("ALTER VIEW v AS SELECT a, b, c, d FROM foo")
        self.validate_identity("ALTER VIEW v AS SELECT * FROM foo WHERE c > 100")
        self.validate_identity(
            "ALTER ALGORITHM = MERGE VIEW v AS SELECT * FROM foo", check_command_warning=True
        )
        self.validate_identity(
            "ALTER DEFINER = 'admin'@'localhost' VIEW v AS SELECT * FROM foo",
            check_command_warning=True,
        )
        self.validate_identity(
            "ALTER SQL SECURITY = DEFINER VIEW v AS SELECT * FROM foo", check_command_warning=True
        )
        self.validate_identity(
            "INSERT INTO things (a, b) VALUES (1, 2) AS new_data ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), a = new_data.a, b = new_data.b"
        )
        self.validate_identity(
            "CREATE TABLE `oauth_consumer` (`key` VARCHAR(32) NOT NULL, UNIQUE `OAUTH_CONSUMER_KEY` (`key`))"
        )
        self.validate_identity(
            "CREATE TABLE `x` (`username` VARCHAR(200), PRIMARY KEY (`username`(16)))"
        )
        self.validate_identity(
            "UPDATE items SET items.price = 0 WHERE items.id >= 5 ORDER BY items.id LIMIT 10"
        )
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
        self.validate_identity(
            "CREATE OR REPLACE VIEW my_view AS SELECT column1 AS `boo`, column2 AS `foo` FROM my_table WHERE column3 = 'some_value' UNION SELECT q.* FROM fruits_table, JSON_TABLE(Fruits, '$[*]' COLUMNS(id VARCHAR(255) PATH '$.$id', value VARCHAR(255) PATH '$.value')) AS q",
        )
        self.validate_identity(
            "CREATE TABLE test_table (id INT AUTO_INCREMENT, PRIMARY KEY (id) USING BTREE)"
        )
        self.validate_identity(
            "CREATE TABLE test_table (id INT AUTO_INCREMENT, PRIMARY KEY (id) USING HASH)"
        )
        self.validate_identity(
            "/*left*/ EXPLAIN SELECT /*hint*/ col FROM t1 /*right*/",
            "/* left */ DESCRIBE /* hint */ SELECT col FROM t1 /* right */",
        )
        self.validate_identity(
            "CREATE TABLE t (name VARCHAR)",
            "CREATE TABLE t (name TEXT)",
        )
        self.validate_identity(
            "ALTER TABLE t ADD KEY `i` (`c`)",
            "ALTER TABLE t ADD INDEX `i` (`c`)",
        )
        self.validate_identity(
            "CREATE TABLE `foo` (`id` char(36) NOT NULL DEFAULT (uuid()), PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`))",
            "CREATE TABLE `foo` (`id` CHAR(36) NOT NULL DEFAULT (UUID()), PRIMARY KEY (`id`), UNIQUE `id` (`id`))",
        )
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE KEY d (b), KEY e (b))",
            "CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE d (b), INDEX e (b))",
        )
        self.validate_identity(
            "CREATE TABLE test (ts TIMESTAMP, ts_tz TIMESTAMPTZ, ts_ltz TIMESTAMPLTZ)",
            "CREATE TABLE test (ts DATETIME, ts_tz TIMESTAMP, ts_ltz TIMESTAMP)",
        )
        self.validate_identity(
            "ALTER TABLE test_table ALTER COLUMN test_column SET DATA TYPE LONGTEXT",
            "ALTER TABLE test_table MODIFY COLUMN test_column LONGTEXT",
        )
        self.validate_identity(
            "CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC",
            "CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()) DEFAULT CHARACTER SET=utf8 ROW_FORMAT=DYNAMIC",
        )
        self.validate_identity(
            "CREATE TABLE `foo` (a VARCHAR(10), KEY idx_a (a DESC))",
            "CREATE TABLE `foo` (a VARCHAR(10), INDEX idx_a (a DESC))",
        )

        self.validate_all(
            "insert into t(i) values (default)",
            write={
                "duckdb": "INSERT INTO t (i) VALUES (DEFAULT)",
                "mysql": "INSERT INTO t (i) VALUES (DEFAULT)",
            },
        )
        self.validate_all(
            "CREATE TABLE t (id INT UNSIGNED)",
            write={
                "duckdb": "CREATE TABLE t (id UINTEGER)",
                "mysql": "CREATE TABLE t (id INT UNSIGNED)",
            },
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
            "CREATE TABLE x (id int not null auto_increment, primary key (id))",
            write={
                "mysql": "CREATE TABLE x (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))",
                "sqlite": "CREATE TABLE x (id INTEGER NOT NULL AUTOINCREMENT PRIMARY KEY)",
            },
        )
        self.validate_identity("ALTER TABLE t ALTER INDEX i INVISIBLE")
        self.validate_identity("ALTER TABLE t ALTER INDEX i VISIBLE")
        self.validate_identity("ALTER TABLE t ALTER COLUMN c SET INVISIBLE")
        self.validate_identity("ALTER TABLE t ALTER COLUMN c SET VISIBLE")

    def test_identity(self):
        self.validate_identity("SELECT HIGH_PRIORITY STRAIGHT_JOIN SQL_CALC_FOUND_ROWS * FROM t")
        self.validate_identity("SELECT CAST(COALESCE(`id`, 'NULL') AS CHAR CHARACTER SET binary)")
        self.validate_identity("SELECT e.* FROM e STRAIGHT_JOIN p ON e.x = p.y")
        self.validate_identity("ALTER TABLE test_table ALTER COLUMN test_column SET DEFAULT 1")
        self.validate_identity("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00.0000')")
        self.validate_identity("SELECT @var1 := 1, @var2")
        self.validate_identity("UNLOCK TABLES")
        self.validate_identity("LOCK TABLES `app_fields` WRITE", check_command_warning=True)
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
        self.validate_identity("SELECT a || b", "SELECT a OR b")
        self.validate_identity(
            "SELECT * FROM x ORDER BY BINARY a", "SELECT * FROM x ORDER BY CAST(a AS BINARY)"
        )
        self.validate_identity(
            """SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, '$.value'))"""
        )
        self.validate_identity(
            "SELECT * FROM t1, t2, t3 FOR SHARE OF t1 NOWAIT FOR UPDATE OF t2, t3 SKIP LOCKED"
        )
        self.validate_identity(
            "REPLACE INTO table SELECT id FROM table2 WHERE cnt > 100", check_command_warning=True
        )
        self.validate_identity(
            "CAST(x AS VARCHAR)",
            "CAST(x AS CHAR)",
        )
        self.validate_identity(
            """SELECT * FROM foo WHERE 3 MEMBER OF(info->'$.value')""",
            """SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, '$.value'))""",
        )
        self.validate_identity(
            "SELECT 1 AS row",
            "SELECT 1 AS `row`",
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
        self.validate_identity("DATABASE()", "SCHEMA()")
        self.validate_identity(
            "SET GLOBAL sort_buffer_size = 1000000, SESSION sort_buffer_size = 1000000"
        )
        self.validate_identity(
            "SET @@GLOBAL.sort_buffer_size = 1000000, @@LOCAL.sort_buffer_size = 1000000"
        )
        self.validate_identity("INTERVAL '1' YEAR")
        self.validate_identity("DATE_ADD(x, INTERVAL '1' YEAR)")
        self.validate_identity("CHAR(0)")
        self.validate_identity("CHAR(77, 121, 83, 81, '76')")
        self.validate_identity("CHAR(77, 77.3, '77.3' USING utf8mb4)")
        self.validate_identity("SELECT * FROM t1 PARTITION(p0)")
        self.validate_identity("SELECT @var1 := 1, @var2")
        self.validate_identity("SELECT @var1, @var2 := @var1")
        self.validate_identity("SELECT @var1 := COUNT(*) FROM t1")

    def test_types(self):
        for char_type in MySQL.Generator.CHAR_CAST_MAPPING:
            with self.subTest(f"MySQL cast into {char_type}"):
                self.validate_identity(f"CAST(x AS {char_type.value})", "CAST(x AS CHAR)")

        for signed_type in MySQL.Generator.SIGNED_CAST_MAPPING:
            with self.subTest(f"MySQL cast into {signed_type}"):
                self.validate_identity(f"CAST(x AS {signed_type.value})", "CAST(x AS SIGNED)")

        self.validate_identity("CAST(x AS ENUM('a', 'b'))")
        self.validate_identity("CAST(x AS SET('a', 'b'))")
        self.validate_identity(
            "CAST(x AS MEDIUMINT) + CAST(y AS YEAR(4))",
            "CAST(x AS SIGNED) + CAST(y AS YEAR(4))",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMP)",
            "CAST(x AS DATETIME)",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPTZ)",
            "TIMESTAMP(x)",
        )
        self.validate_identity(
            "CAST(x AS TIMESTAMPLTZ)",
            "TIMESTAMP(x)",
        )

        self.validate_all(
            "CAST(x AS MEDIUMTEXT) + CAST(y AS LONGTEXT) + CAST(z AS TINYTEXT)",
            write={
                "mysql": "CAST(x AS CHAR) + CAST(y AS CHAR) + CAST(z AS CHAR)",
                "spark": "CAST(x AS TEXT) + CAST(y AS TEXT) + CAST(z AS TEXT)",
            },
        )
        self.validate_all(
            "CAST(x AS MEDIUMBLOB) + CAST(y AS LONGBLOB) + CAST(z AS TINYBLOB)",
            write={
                "mysql": "CAST(x AS CHAR) + CAST(y AS CHAR) + CAST(z AS CHAR)",
                "spark": "CAST(x AS BLOB) + CAST(y AS BLOB) + CAST(z AS BLOB)",
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
        self.validate_all(
            "CHAR(10)",
            write={
                "mysql": "CHAR(10)",
                "presto": "CHR(10)",
                "sqlite": "CHAR(10)",
                "tsql": "CHAR(10)",
            },
        )
        self.validate_identity("CREATE TABLE t (foo VARBINARY(5))")
        self.validate_all(
            "CREATE TABLE t (foo BLOB)",
            write={
                "mysql": "CREATE TABLE t (foo BLOB)",
                "oracle": "CREATE TABLE t (foo BLOB)",
                "postgres": "CREATE TABLE t (foo BYTEA)",
                "tsql": "CREATE TABLE t (foo VARBINARY)",
                "sqlite": "CREATE TABLE t (foo BLOB)",
                "duckdb": "CREATE TABLE t (foo VARBINARY)",
                "hive": "CREATE TABLE t (foo BINARY)",
                "bigquery": "CREATE TABLE t (foo BYTES)",
                "redshift": "CREATE TABLE t (foo VARBYTE)",
                "clickhouse": "CREATE TABLE t (foo Nullable(String))",
            },
        )

    def test_escape(self):
        self.validate_identity("""'"abc"'""")
        self.validate_identity(
            r"'\'a'",
            "'''a'",
        )
        self.validate_identity(
            '''"'abc'"''',
            "'''abc'''",
        )
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
            "bigquery": "SELECT FROM_HEX('CC')",
            "clickhouse": UnsupportedError,
            "databricks": "SELECT X'CC'",
            "drill": "SELECT 204",
            "duckdb": "SELECT FROM_HEX('CC')",
            "hive": "SELECT 204",
            "mysql": "SELECT x'CC'",
            "oracle": "SELECT 204",
            "postgres": "SELECT x'CC'",
            "presto": "SELECT x'CC'",
            "redshift": "SELECT 204",
            "snowflake": "SELECT x'CC'",
            "spark": "SELECT X'CC'",
            "sqlite": "SELECT x'CC'",
            "starrocks": "SELECT x'CC'",
            "tableau": "SELECT 204",
            "teradata": "SELECT X'CC'",
            "trino": "SELECT x'CC'",
            "tsql": "SELECT 0xCC",
        }
        write_CC_with_leading_zeros = {
            "bigquery": "SELECT FROM_HEX('0000CC')",
            "clickhouse": UnsupportedError,
            "databricks": "SELECT X'0000CC'",
            "drill": "SELECT 204",
            "duckdb": "SELECT FROM_HEX('0000CC')",
            "hive": "SELECT 204",
            "mysql": "SELECT x'0000CC'",
            "oracle": "SELECT 204",
            "postgres": "SELECT x'0000CC'",
            "presto": "SELECT x'0000CC'",
            "redshift": "SELECT 204",
            "snowflake": "SELECT x'0000CC'",
            "spark": "SELECT X'0000CC'",
            "sqlite": "SELECT x'0000CC'",
            "starrocks": "SELECT x'0000CC'",
            "tableau": "SELECT 204",
            "teradata": "SELECT X'0000CC'",
            "trino": "SELECT x'0000CC'",
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
                "postgres": "(col1 @@ 'abc' OR col2 @@ 'abc' OR col3 @@ 'abc')",  # not quite correct because it's not ts_query
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
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMP), 'yyyy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%m')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%m')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMP), 'mm')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%d')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%d')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMP), 'DD')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%Y-%m-%d')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%Y-%m-%d')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMP), 'yyyy-mm-DD')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15 22:23:34', '%H')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15 22:23:34', '%H')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15 22:23:34' AS TIMESTAMP), 'hh24')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2017-06-15', '%w')",
            write={
                "mysql": "SELECT DATE_FORMAT('2017-06-15', '%w')",
                "snowflake": "SELECT TO_CHAR(CAST('2017-06-15' AS TIMESTAMP), 'dy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2024-08-22 14:53:12', '%a')",
            write={
                "mysql": "SELECT DATE_FORMAT('2024-08-22 14:53:12', '%a')",
                "snowflake": "SELECT TO_CHAR(CAST('2024-08-22 14:53:12' AS TIMESTAMP), 'DY')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2009-10-04 22:23:00', '%a %M %Y')",
            write={
                "mysql": "SELECT DATE_FORMAT('2009-10-04 22:23:00', '%a %M %Y')",
                "snowflake": "SELECT TO_CHAR(CAST('2009-10-04 22:23:00' AS TIMESTAMP), 'DY mmmm yyyy')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s')",
            write={
                "mysql": "SELECT DATE_FORMAT('2007-10-04 22:23:00', '%T')",
                "snowflake": "SELECT TO_CHAR(CAST('2007-10-04 22:23:00' AS TIMESTAMP), 'hh24:mi:ss')",
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT('1900-10-04 22:23:00', '%d %y %a %d %m %b')",
            write={
                "mysql": "SELECT DATE_FORMAT('1900-10-04 22:23:00', '%d %y %a %d %m %b')",
                "snowflake": "SELECT TO_CHAR(CAST('1900-10-04 22:23:00' AS TIMESTAMP), 'DD yy DY DD mm mon')",
            },
        )

    def test_mysql_time(self):
        self.validate_identity("TIME_STR_TO_UNIX(x)", "UNIX_TIMESTAMP(x)")
        self.validate_identity("SELECT FROM_UNIXTIME(1711366265, '%Y %D %M')")
        self.validate_all(
            "SELECT TO_DAYS(x)",
            write={
                "mysql": "SELECT (DATEDIFF(x, '0000-01-01') + 1)",
                "presto": "SELECT (DATE_DIFF('DAY', CAST(CAST('0000-01-01' AS TIMESTAMP) AS DATE), CAST(CAST(x AS TIMESTAMP) AS DATE)) + 1)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(x, y)",
            read={
                "presto": "SELECT DATE_DIFF('DAY', y, x)",
                "redshift": "SELECT DATEDIFF(DAY, y, x)",
            },
            write={
                "mysql": "SELECT DATEDIFF(x, y)",
                "presto": "SELECT DATE_DIFF('DAY', y, x)",
                "redshift": "SELECT DATEDIFF(DAY, y, x)",
            },
        )
        self.validate_all(
            "DAYOFYEAR(x)",
            write={
                "mysql": "DAYOFYEAR(x)",
                "": "DAY_OF_YEAR(CAST(x AS DATE))",
            },
        )
        self.validate_all(
            "DAYOFMONTH(x)",
            write={"mysql": "DAYOFMONTH(x)", "": "DAY_OF_MONTH(CAST(x AS DATE))"},
        )
        self.validate_all(
            "DAYOFWEEK(x)",
            write={"mysql": "DAYOFWEEK(x)", "": "DAY_OF_WEEK(CAST(x AS DATE))"},
        )
        self.validate_all(
            "WEEKOFYEAR(x)",
            write={"mysql": "WEEKOFYEAR(x)", "": "WEEK_OF_YEAR(CAST(x AS DATE))"},
        )
        self.validate_all(
            "DAY(x)",
            write={"mysql": "DAY(x)", "": "DAY(CAST(x AS DATE))"},
        )
        self.validate_all(
            "WEEK(x)",
            write={"mysql": "WEEK(x)", "": "WEEK(CAST(x AS DATE))"},
        )
        self.validate_all(
            "YEAR(x)",
            write={"mysql": "YEAR(x)", "": "YEAR(CAST(x AS DATE))"},
        )
        self.validate_all(
            "DATE(x)",
            read={"": "TS_OR_DS_TO_DATE(x)"},
        )
        self.validate_all(
            "STR_TO_DATE(x, '%M')",
            read={"": "TS_OR_DS_TO_DATE(x, '%B')"},
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%d')",
            write={"presto": "CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)"},
        )
        self.validate_all(
            "STR_TO_DATE(x, '%Y-%m-%dT%T')", write={"presto": "DATE_PARSE(x, '%Y-%m-%dT%T')"}
        )
        self.validate_all(
            "SELECT FROM_UNIXTIME(col)",
            read={
                "postgres": "SELECT TO_TIMESTAMP(col)",
            },
            write={
                "mysql": "SELECT FROM_UNIXTIME(col)",
                "postgres": "SELECT TO_TIMESTAMP(col)",
                "redshift": "SELECT (TIMESTAMP 'epoch' + col * INTERVAL '1 SECOND')",
            },
        )

        # No timezone, make sure DATETIME captures the correct precision
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.123456+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.123456+00:00' AS DATETIME(6))",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.123+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.123+00:00' AS DATETIME(3))",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15+00:00' AS DATETIME)",
        )

        # With timezone, make sure the TIMESTAMP constructor is used
        # also TIMESTAMP doesnt have the subsecond precision truncation issue that DATETIME does so we dont need to TIMESTAMP(6)
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15-08:00', 'America/Los_Angeles')",
            write_sql="SELECT TIMESTAMP('2023-01-01 13:14:15-08:00')",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15-08:00', 'America/Los_Angeles')",
            write_sql="SELECT TIMESTAMP('2023-01-01 13:14:15-08:00')",
        )

    @unittest.skipUnless(
        sys.version_info >= (3, 11),
        "Python 3.11 relaxed datetime.fromisoformat() parsing with regards to microseconds",
    )
    def test_mysql_time_python311(self):
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.12345+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.12345+00:00' AS DATETIME(6))",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.1234+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.1234+00:00' AS DATETIME(6))",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.12+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.12+00:00' AS DATETIME(3))",
        )
        self.validate_identity(
            "SELECT TIME_STR_TO_TIME('2023-01-01 13:14:15.1+00:00')",
            write_sql="SELECT CAST('2023-01-01 13:14:15.1+00:00' AS DATETIME(3))",
        )

    def test_mysql(self):
        for func in ("CHAR_LENGTH", "CHARACTER_LENGTH"):
            with self.subTest(f"Testing MySQL's {func}"):
                self.validate_all(
                    f"SELECT {func}('foo')",
                    write={
                        "duckdb": "SELECT LENGTH('foo')",
                        "mysql": "SELECT CHAR_LENGTH('foo')",
                        "postgres": "SELECT LENGTH('foo')",
                    },
                )

        self.validate_all(
            "CURDATE()",
            write={
                "mysql": "CURRENT_DATE",
                "postgres": "CURRENT_DATE",
            },
        )
        self.validate_all(
            "SELECT CONCAT('11', '22')",
            read={
                "postgres": "SELECT '11' || '22'",
            },
            write={
                "mysql": "SELECT CONCAT('11', '22')",
                "postgres": "SELECT CONCAT('11', '22')",
            },
        )
        self.validate_all(
            "SELECT department, GROUP_CONCAT(name) AS employee_names FROM data GROUP BY department",
            read={
                "postgres": "SELECT department, array_agg(name) AS employee_names FROM data GROUP BY department",
            },
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP(CAST('2024-04-29 12:00:00' AS DATETIME))",
            read={
                "mysql": "SELECT UNIX_TIMESTAMP(CAST('2024-04-29 12:00:00' AS DATETIME))",
                "postgres": "SELECT EXTRACT(epoch FROM TIMESTAMP '2024-04-29 12:00:00')",
            },
        )
        self.validate_all(
            "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]')",
            read={
                "sqlite": "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]')",
            },
            write={
                "mysql": "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]')",
                "sqlite": "SELECT '[10, 20, [30, 40]]' -> '$[1]'",
            },
        )
        self.validate_all(
            "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]', '$[0]')",
            read={
                "sqlite": "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]', '$[0]')",
            },
            write={
                "mysql": "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]', '$[0]')",
                "sqlite": "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]', '$[0]')",
            },
        )
        self.validate_all(
            "SELECT * FROM x LEFT JOIN y ON x.id = y.id UNION ALL SELECT * FROM x RIGHT JOIN y ON x.id = y.id WHERE NOT EXISTS(SELECT 1 FROM x WHERE x.id = y.id) ORDER BY 1 LIMIT 0",
            read={
                "postgres": "SELECT * FROM x FULL JOIN y ON x.id = y.id ORDER BY 1 LIMIT 0",
            },
        )
        self.validate_all(
            # MySQL doesn't support FULL OUTER joins
            "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.x = t2.x UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.x = t2.x WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x)",
            read={
                "postgres": "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.x = t2.x",
            },
        )
        self.validate_all(
            "SELECT * FROM t1 LEFT OUTER JOIN t2 USING (x) UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 USING (x) WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x)",
            read={
                "postgres": "SELECT * FROM t1 FULL OUTER JOIN t2 USING (x) ",
            },
        )
        self.validate_all(
            "SELECT * FROM t1 LEFT OUTER JOIN t2 USING (x, y) UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 USING (x, y) WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x AND t1.y = t2.y)",
            read={
                "postgres": "SELECT * FROM t1 FULL OUTER JOIN t2 USING (x, y) ",
            },
        )
        self.validate_all(
            "a XOR b",
            read={
                "mysql": "a XOR b",
                "snowflake": "BOOLXOR(a, b)",
            },
            write={
                "duckdb": "(a AND (NOT b)) OR ((NOT a) AND b)",
                "mysql": "a XOR b",
                "postgres": "(a AND (NOT b)) OR ((NOT a) AND b)",
                "snowflake": "BOOLXOR(a, b)",
                "trino": "(a AND (NOT b)) OR ((NOT a) AND b)",
            },
        )

        self.validate_all(
            "SELECT * FROM test LIMIT 0 + 1, 0 + 1",
            write={
                "mysql": "SELECT * FROM test LIMIT 1 OFFSET 1",
                "postgres": "SELECT * FROM test LIMIT 0 + 1 OFFSET 0 + 1",
                "presto": "SELECT * FROM test OFFSET 1 LIMIT 1",
                "snowflake": "SELECT * FROM test LIMIT 1 OFFSET 1",
                "trino": "SELECT * FROM test OFFSET 1 LIMIT 1",
                "bigquery": "SELECT * FROM test LIMIT 1 OFFSET 1",
            },
        )
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
        self.validate_all("TIME_STR_TO_TIME(x)", write={"mysql": "CAST(x AS DATETIME)"})
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
        self.validate_identity("SELECT name FROM temp WHERE name = ? FOR UPDATE")
        self.validate_all(
            "SELECT a FROM tbl FOR UPDATE",
            write={
                "": "SELECT a FROM tbl",
                "mysql": "SELECT a FROM tbl FOR UPDATE",
                "oracle": "SELECT a FROM tbl FOR UPDATE",
                "postgres": "SELECT a FROM tbl FOR UPDATE",
                "redshift": "SELECT a FROM tbl",
                "tsql": "SELECT a FROM tbl",
            },
        )
        self.validate_all(
            "SELECT a FROM tbl FOR SHARE",
            write={
                "": "SELECT a FROM tbl",
                "mysql": "SELECT a FROM tbl FOR SHARE",
                "oracle": "SELECT a FROM tbl FOR SHARE",
                "postgres": "SELECT a FROM tbl FOR SHARE",
                "tsql": "SELECT a FROM tbl",
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
        self.validate_all(
            "GROUP_CONCAT(a, b, c SEPARATOR ',')",
            write={
                "mysql": "GROUP_CONCAT(CONCAT(a, b, c) SEPARATOR ',')",
                "sqlite": "GROUP_CONCAT(a || b || c, ',')",
                "tsql": "STRING_AGG(CONCAT(a, b, c), ',')",
                "postgres": "STRING_AGG(CONCAT(a, b, c), ',')",
                "presto": "ARRAY_JOIN(ARRAY_AGG(CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR), CAST(c AS VARCHAR))), ',')",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(a, b, c SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(CONCAT(a, b, c) SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(a || b || c, '')",
                "tsql": "STRING_AGG(CONCAT(a, b, c), '')",
                "postgres": "STRING_AGG(CONCAT(a, b, c), '')",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT a, b, c SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT CONCAT(a, b, c) SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(DISTINCT a || b || c, '')",
                "tsql": "STRING_AGG(CONCAT(a, b, c), '')",
                "postgres": "STRING_AGG(DISTINCT CONCAT(a, b, c), '')",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(a, b, c ORDER BY d SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(CONCAT(a, b, c) ORDER BY d SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(a || b || c, '')",
                "tsql": "STRING_AGG(CONCAT(a, b, c), '') WITHIN GROUP (ORDER BY d)",
                "postgres": "STRING_AGG(CONCAT(a, b, c), '' ORDER BY d NULLS FIRST)",
            },
        )
        self.validate_all(
            "GROUP_CONCAT(DISTINCT a, b, c ORDER BY d SEPARATOR '')",
            write={
                "mysql": "GROUP_CONCAT(DISTINCT CONCAT(a, b, c) ORDER BY d SEPARATOR '')",
                "sqlite": "GROUP_CONCAT(DISTINCT a || b || c, '')",
                "tsql": "STRING_AGG(CONCAT(a, b, c), '') WITHIN GROUP (ORDER BY d)",
                "postgres": "STRING_AGG(DISTINCT CONCAT(a, b, c), '' ORDER BY d NULLS FIRST)",
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
            ("SCHEMAS", "DATABASES"),
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
        show = self.validate_identity("SHOW GRANTS FOR foo")
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

    def test_json_object(self):
        self.validate_identity("SELECT JSON_OBJECT('id', 87, 'name', 'carrot')")

    def test_is_null(self):
        self.validate_all(
            "SELECT ISNULL(x)", write={"": "SELECT (x IS NULL)", "mysql": "SELECT (x IS NULL)"}
        )

    def test_monthname(self):
        self.validate_all(
            "MONTHNAME(x)",
            write={
                "": "TIME_TO_STR(CAST(x AS DATE), '%B')",
                "mysql": "DATE_FORMAT(x, '%M')",
            },
        )

    def test_safe_div(self):
        self.validate_all(
            "a / b",
            write={
                "bigquery": "a / NULLIF(b, 0)",
                "clickhouse": "a / b",
                "databricks": "a / NULLIF(b, 0)",
                "duckdb": "a / b",
                "hive": "a / b",
                "mysql": "a / b",
                "oracle": "a / NULLIF(b, 0)",
                "snowflake": "a / NULLIF(b, 0)",
                "spark": "a / b",
                "starrocks": "a / b",
                "drill": "CAST(a AS DOUBLE) / NULLIF(b, 0)",
                "postgres": "CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)",
                "presto": "CAST(a AS DOUBLE) / NULLIF(b, 0)",
                "redshift": "CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)",
                "sqlite": "CAST(a AS REAL) / b",
                "teradata": "CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)",
                "trino": "CAST(a AS DOUBLE) / NULLIF(b, 0)",
                "tsql": "CAST(a AS FLOAT) / NULLIF(b, 0)",
            },
        )

    def test_timestamp_trunc(self):
        hive_dialects = ("spark", "databricks")
        for dialect in ("postgres", "snowflake", "duckdb", *hive_dialects):
            for unit in (
                "SECOND",
                "DAY",
                "MONTH",
                "YEAR",
            ):
                with self.subTest(f"MySQL -> {dialect} Timestamp Trunc with unit {unit}: "):
                    cast = (
                        "TIMESTAMP('2001-02-16 20:38:40')"
                        if dialect in hive_dialects
                        else "CAST('2001-02-16 20:38:40' AS DATETIME)"
                    )
                    self.validate_all(
                        f"DATE_ADD('0000-01-01 00:00:00', INTERVAL (TIMESTAMPDIFF({unit}, '0000-01-01 00:00:00', {cast})) {unit})",
                        read={
                            dialect: f"DATE_TRUNC({unit}, TIMESTAMP '2001-02-16 20:38:40')",
                        },
                        write={
                            "mysql": f"DATE_ADD('0000-01-01 00:00:00', INTERVAL (TIMESTAMPDIFF({unit}, '0000-01-01 00:00:00', {cast})) {unit})",
                        },
                    )

    def test_at_time_zone(self):
        with self.assertLogs() as cm:
            # Check AT TIME ZONE doesnt discard the column name and also raises a warning
            self.validate_identity(
                "SELECT foo AT TIME ZONE 'UTC'",
                write_sql="SELECT foo",
            )
            assert "AT TIME ZONE is not supported" in cm.output[0]

    def test_json_value(self):
        json_doc = """'{"item": "shoes", "price": "49.95"}'"""
        self.validate_identity(f"""SELECT JSON_VALUE({json_doc}, '$.price')""")
        self.validate_identity(
            f"""SELECT JSON_VALUE({json_doc}, '$.price' RETURNING DECIMAL(4, 2))"""
        )

        for on_option in ("NULL", "ERROR", "DEFAULT 1"):
            self.validate_identity(
                f"""SELECT JSON_VALUE({json_doc}, '$.price' RETURNING DECIMAL(4, 2) {on_option} ON EMPTY {on_option} ON ERROR) AS price"""
            )

    def test_grant(self):
        grant_cmds = [
            "GRANT 'role1', 'role2' TO 'user1'@'localhost', 'user2'@'localhost'",
            "GRANT SELECT ON world.* TO 'role3'",
            "GRANT SELECT ON db2.invoice TO 'jeffrey'@'localhost'",
            "GRANT INSERT ON `d%`.* TO u",
            "GRANT ALL ON test.* TO ''@'localhost'",
            "GRANT SELECT (col1), INSERT (col1, col2) ON mydb.mytbl TO 'someuser'@'somehost'",
            "GRANT SELECT, INSERT, UPDATE ON *.* TO u2",
        ]

        for sql in grant_cmds:
            with self.subTest(f"Testing MySQL's GRANT command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

    def test_explain(self):
        self.validate_identity(
            "EXPLAIN ANALYZE SELECT * FROM t", "DESCRIBE ANALYZE SELECT * FROM t"
        )

        expression = self.parse_one("EXPLAIN ANALYZE SELECT * FROM t")
        self.assertIsInstance(expression, exp.Describe)
        self.assertEqual(expression.text("style"), "ANALYZE")

        for format in ("JSON", "TRADITIONAL", "TREE"):
            self.validate_identity(f"DESCRIBE FORMAT={format} UPDATE test SET test_col = 'abc'")

    def test_number_format(self):
        self.validate_all(
            "SELECT FORMAT(12332.123456, 4)",
            write={
                "duckdb": "SELECT FORMAT('{:,.4f}', 12332.123456)",
                "mysql": "SELECT FORMAT(12332.123456, 4)",
            },
        )
        self.validate_all(
            "SELECT FORMAT(12332.1, 4)",
            write={
                "duckdb": "SELECT FORMAT('{:,.4f}', 12332.1)",
                "mysql": "SELECT FORMAT(12332.1, 4)",
            },
        )
        self.validate_all(
            "SELECT FORMAT(12332.2, 0)",
            write={
                "duckdb": "SELECT FORMAT('{:,.0f}', 12332.2)",
                "mysql": "SELECT FORMAT(12332.2, 0)",
            },
        )
        self.validate_all(
            "SELECT FORMAT(12332.2, 2, 'de_DE')",
            write={
                "duckdb": UnsupportedError,
                "mysql": "SELECT FORMAT(12332.2, 2, 'de_DE')",
            },
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE LOCAL TABLE tbl")
        self.validate_identity("ANALYZE NO_WRITE_TO_BINLOG TABLE tbl")
        self.validate_identity("ANALYZE tbl UPDATE HISTOGRAM ON col1")
        self.validate_identity("ANALYZE tbl UPDATE HISTOGRAM ON col1 USING DATA 'json_data'")
        self.validate_identity("ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS")
        self.validate_identity("ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS AUTO UPDATE")
        self.validate_identity("ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS MANUAL UPDATE")
        self.validate_identity("ANALYZE tbl DROP HISTOGRAM ON col1")
