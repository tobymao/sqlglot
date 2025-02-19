from tests.dialects.test_dialect import Validator

from sqlglot.helper import logger as helper_logger


class TestSQLite(Validator):
    dialect = "sqlite"

    def test_sqlite(self):
        self.validate_identity("UNHEX(a, b)")
        self.validate_identity("SELECT DATE()")
        self.validate_identity("SELECT DATE('now', 'start of month', '+1 month', '-1 day')")
        self.validate_identity("SELECT DATETIME(1092941466, 'unixepoch')")
        self.validate_identity("SELECT DATETIME(1092941466, 'auto')")
        self.validate_identity("SELECT DATETIME(1092941466, 'unixepoch', 'localtime')")
        self.validate_identity("SELECT UNIXEPOCH()")
        self.validate_identity("SELECT JULIANDAY('now') - JULIANDAY('1776-07-04')")
        self.validate_identity("SELECT UNIXEPOCH() - UNIXEPOCH('2004-01-01 02:34:56')")
        self.validate_identity("SELECT DATE('now', 'start of year', '+9 months', 'weekday 2')")
        self.validate_identity("SELECT (JULIANDAY('now') - 2440587.5) * 86400.0")
        self.validate_identity("SELECT UNIXEPOCH('now', 'subsec')")
        self.validate_identity("SELECT TIMEDIFF('now', '1809-02-12')")
        self.validate_identity(
            "SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[2]', '$[0]', '$[1]')",
        )
        self.validate_identity(
            """SELECT item AS "item", some AS "some" FROM data WHERE (item = 'value_1' COLLATE NOCASE) AND (some = 't' COLLATE NOCASE) ORDER BY item ASC LIMIT 1 OFFSET 0"""
        )
        self.validate_identity("SELECT * FROM GENERATE_SERIES(1, 5)")
        self.validate_identity("SELECT INSTR(haystack, needle)")

        self.validate_all("SELECT LIKE(y, x)", write={"sqlite": "SELECT x LIKE y"})
        self.validate_all("SELECT GLOB('*y*', 'xyz')", write={"sqlite": "SELECT 'xyz' GLOB '*y*'"})
        self.validate_all(
            "SELECT LIKE('%y%', 'xyz', '')", write={"sqlite": "SELECT 'xyz' LIKE '%y%' ESCAPE ''"}
        )
        self.validate_all(
            "CURRENT_DATE",
            read={
                "": "CURRENT_DATE",
                "snowflake": "CURRENT_DATE()",
            },
        )
        self.validate_all(
            "CURRENT_TIME",
            read={
                "": "CURRENT_TIME",
                "snowflake": "CURRENT_TIME()",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP",
            read={
                "": "CURRENT_TIMESTAMP",
                "snowflake": "CURRENT_TIMESTAMP()",
            },
        )
        self.validate_all(
            "SELECT DATE('2020-01-01 16:03:05')",
            read={
                "snowflake": "SELECT CAST('2020-01-01 16:03:05' AS DATE)",
            },
        )
        self.validate_all(
            "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
            write={
                "sqlite": 'SELECT CAST("a"."b" AS INTEGER) FROM foo',
                "spark": "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            },
        )
        self.validate_all(
            "EDITDIST3(col1, col2)",
            read={
                "sqlite": "EDITDIST3(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
            },
            write={
                "sqlite": "EDITDIST3(col1, col2)",
                "spark": "LEVENSHTEIN(col1, col2)",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
                "sqlite": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            },
        )
        self.validate_all("x", read={"snowflake": "LEAST(x)"})
        self.validate_all("MIN(x)", read={"snowflake": "MIN(x)"}, write={"snowflake": "MIN(x)"})
        self.validate_all(
            "MIN(x, y, z)",
            read={"snowflake": "LEAST(x, y, z)"},
            write={"snowflake": "LEAST(x, y, z)"},
        )
        self.validate_all(
            "UNICODE(x)",
            write={
                "": "UNICODE(x)",
                "mysql": "ORD(CONVERT(x USING utf32))",
                "oracle": "ASCII(UNISTR(x))",
                "postgres": "ASCII(x)",
                "redshift": "ASCII(x)",
                "spark": "ASCII(x)",
            },
        )
        self.validate_identity(
            "SELECT * FROM station WHERE city IS NOT ''",
            "SELECT * FROM station WHERE NOT city IS ''",
        )
        self.validate_identity("SELECT JSON_OBJECT('col1', 1, 'col2', '1')")

    def test_strftime(self):
        self.validate_identity("SELECT STRFTIME('%Y/%m/%d', 'now')")
        self.validate_identity("SELECT STRFTIME('%Y-%m-%d', '2016-10-16', 'start of month')")
        self.validate_identity(
            "SELECT STRFTIME('%s')",
            "SELECT STRFTIME('%s', CURRENT_TIMESTAMP)",
        )

        self.validate_all(
            "SELECT STRFTIME('%Y-%m-%d', '2020-01-01 12:05:03')",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2020-01-01 12:05:03' AS TIMESTAMP), '%Y-%m-%d')",
                "sqlite": "SELECT STRFTIME('%Y-%m-%d', '2020-01-01 12:05:03')",
            },
        )
        self.validate_all(
            "SELECT STRFTIME('%Y-%m-%d', CURRENT_TIMESTAMP)",
            write={
                "duckdb": "SELECT STRFTIME(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), '%Y-%m-%d')",
                "sqlite": "SELECT STRFTIME('%Y-%m-%d', CURRENT_TIMESTAMP)",
            },
        )

    def test_datediff(self):
        self.validate_all(
            "DATEDIFF(a, b, 'day')",
            write={"sqlite": "CAST((JULIANDAY(a) - JULIANDAY(b)) AS INTEGER)"},
        )
        self.validate_all(
            "DATEDIFF(a, b, 'hour')",
            write={"sqlite": "CAST((JULIANDAY(a) - JULIANDAY(b)) * 24.0 AS INTEGER)"},
        )
        self.validate_all(
            "DATEDIFF(a, b, 'year')",
            write={"sqlite": "CAST((JULIANDAY(a) - JULIANDAY(b)) / 365.0 AS INTEGER)"},
        )

    def test_hexadecimal_literal(self):
        self.validate_all(
            "SELECT 0XCC",
            write={
                "sqlite": "SELECT x'CC'",
                "mysql": "SELECT x'CC'",
            },
        )

    def test_window_null_treatment(self):
        self.validate_all(
            "SELECT FIRST_VALUE(Name) OVER (PARTITION BY AlbumId ORDER BY Bytes DESC) AS LargestTrack FROM tracks",
            write={
                "sqlite": "SELECT FIRST_VALUE(Name) OVER (PARTITION BY AlbumId ORDER BY Bytes DESC) AS LargestTrack FROM tracks"
            },
        )

    def test_longvarchar_dtype(self):
        self.validate_all(
            "CREATE TABLE foo (bar LONGVARCHAR)",
            write={"sqlite": "CREATE TABLE foo (bar TEXT)"},
        )

    def test_warnings(self):
        with self.assertLogs(helper_logger) as cm:
            self.validate_identity(
                "SELECT * FROM t AS t(c1, c2)",
                "SELECT * FROM t AS t",
            )

            self.assertIn("Named columns are not supported in table alias.", cm.output[0])

    def test_ddl(self):
        for conflict_action in ("ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"):
            with self.subTest(f"ON CONFLICT {conflict_action}"):
                self.validate_identity("CREATE TABLE a (b, c, UNIQUE (b, c) ON CONFLICT IGNORE)")

        self.validate_identity("INSERT OR ABORT INTO foo (x, y) VALUES (1, 2)")
        self.validate_identity("INSERT OR FAIL INTO foo (x, y) VALUES (1, 2)")
        self.validate_identity("INSERT OR IGNORE INTO foo (x, y) VALUES (1, 2)")
        self.validate_identity("INSERT OR REPLACE INTO foo (x, y) VALUES (1, 2)")
        self.validate_identity("INSERT OR ROLLBACK INTO foo (x, y) VALUES (1, 2)")
        self.validate_identity("CREATE TABLE foo (id INTEGER PRIMARY KEY ASC)")
        self.validate_identity("CREATE TEMPORARY TABLE foo (id INTEGER)")

        self.validate_all(
            """
            CREATE TABLE "Track"
            (
                CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
                FOREIGN KEY ("AlbumId") REFERENCES "Album" (
                    "AlbumId"
                ) ON DELETE NO ACTION ON UPDATE NO ACTION,
                FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
                FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
            )
            """,
            write={
                "sqlite": """CREATE TABLE "Track" (
  CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
  FOREIGN KEY ("AlbumId") REFERENCES "Album" (
    "AlbumId"
  ) ON DELETE NO ACTION ON UPDATE NO ACTION,
  FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
)""",
            },
            pretty=True,
        )
        self.validate_all(
            "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
            read={
                "mysql": "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
                "postgres": "CREATE TABLE z (a INT GENERATED BY DEFAULT AS IDENTITY NOT NULL UNIQUE PRIMARY KEY)",
            },
            write={
                "sqlite": "CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)",
                "mysql": "CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)",
                "postgres": "CREATE TABLE z (a INT GENERATED BY DEFAULT AS IDENTITY NOT NULL UNIQUE PRIMARY KEY)",
            },
        )
        self.validate_all(
            """CREATE TABLE "x" ("Name" NVARCHAR(200) NOT NULL)""",
            write={
                "sqlite": """CREATE TABLE "x" ("Name" TEXT(200) NOT NULL)""",
                "mysql": "CREATE TABLE `x` (`Name` VARCHAR(200) NOT NULL)",
            },
        )

        self.validate_identity(
            "CREATE TABLE store (store_id INTEGER PRIMARY KEY AUTOINCREMENT, mgr_id INTEGER NOT NULL UNIQUE REFERENCES staff ON UPDATE CASCADE)"
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE tbl")
        self.validate_identity("ANALYZE schma.tbl")
