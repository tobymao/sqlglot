from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_singlestore(self):
        ast = parse_one(
            "SELECT id AS my_id FROM data WHERE my_id = 1 GROUP BY my_id HAVING my_id = 1",
            dialect=self.dialect,
        )
        ast = qualify(ast, dialect=self.dialect, schema={"data": {"id": "INT", "my_id": "INT"}})
        self.assertEqual(
            "SELECT `data`.`id` AS `my_id` FROM `data` AS `data` WHERE `data`.`my_id` = 1 GROUP BY `data`.`my_id` HAVING `data`.`id` = 1",
            ast.sql(dialect=self.dialect),
        )

        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM `users` ORDER BY ALL")

    def test_byte_strings(self):
        self.validate_identity("SELECT e'text'")
        self.validate_identity("SELECT E'text'", "SELECT e'text'")

    def test_restricted_keywords(self):
        self.validate_identity("SELECT * FROM abs", "SELECT * FROM `abs`")
        self.validate_identity("SELECT * FROM ABS", "SELECT * FROM `ABS`")
        self.validate_identity(
            "SELECT * FROM security_lists_intersect", "SELECT * FROM `security_lists_intersect`"
        )
        self.validate_identity("SELECT * FROM vacuum", "SELECT * FROM `vacuum`")

    def test_time_formatting(self):
        self.validate_identity("SELECT STR_TO_DATE('March 3rd, 2015', '%M %D, %Y')")
        self.validate_identity("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %h:%i:%s')")
        self.validate_identity(
            "SELECT TO_DATE('03/01/2019', 'MM/DD/YYYY') AS `result`",
        )
        self.validate_identity(
            "SELECT TO_TIMESTAMP('The date and time are 01/01/2018 2:30:15.123456', 'The date and time are MM/DD/YYYY HH12:MI:SS.FF6') AS `result`",
        )
        self.validate_identity(
            "SELECT TO_CHAR('2018-03-01', 'MM/DD')",
        )
        self.validate_identity(
            "SELECT TIME_FORMAT('12:05:47', '%s, %i, %h')",
            "SELECT DATE_FORMAT('12:05:47' :> TIME(6), '%s, %i, %h')",
        )

    def test_cast(self):
        self.validate_all(
            "SELECT 1 :> INT",
            read={
                "": "SELECT CAST(1 AS INT)",
            },
            write={
                "singlestore": "SELECT 1 :> INT",
                "": "SELECT CAST(1 AS INT)",
            },
        )
        self.validate_all(
            "SELECT 1 !:> INT",
            read={
                "": "SELECT TRY_CAST(1 AS INT)",
            },
            write={
                "singlestore": "SELECT 1 !:> INT",
                "": "SELECT TRY_CAST(1 AS INT)",
            },
        )
        self.validate_identity("SELECT '{\"a\" : 1}' :> JSON")
        self.validate_identity("SELECT NOW() !:> TIMESTAMP(6)")
        self.validate_identity("SELECT x :> GEOGRAPHYPOINT")

    def test_unix_functions(self):
        self.validate_identity("SELECT FROM_UNIXTIME(1234567890)")
        self.validate_identity("SELECT FROM_UNIXTIME(1234567890, '%M %D, %Y')")
        self.validate_identity("SELECT UNIX_TIMESTAMP()")
        self.validate_identity("SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30') AS funday")

        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"duckdb": "SELECT EPOCH('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"duckdb": "SELECT TIME_STR_TO_UNIX('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"": "SELECT TIME_STR_TO_UNIX('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"": "SELECT UNIX_SECONDS('2009-02-13 23:31:30')"},
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME(1234567890, '%Y-%m-%d %T')",
            read={"hive": "SELECT FROM_UNIXTIME(1234567890)"},
        )
        self.validate_all(
            "SELECT FROM_UNIXTIME(1234567890) :> TEXT",
            read={"": "SELECT UNIX_TO_TIME_STR(1234567890)"},
        )
