from sqlglot import exp
from sqlglot.dialects.exasol import Exasol
from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    dialect = "exasol"

    # def test_databricks_transpilation(self):
    #     run = [2]
    #     if 0 in run:
    #         self.validate_all(
    #             "SELECT CAST(CAST('2025-04-29 18:47:18' AS DATE) AS TIMESTAMP)",
    #             read={
    #                 "databricks": "SELECT CAST(CAST('2025-04-29 18.47.18' AS DATE) AS TIMESTAMP)",
    #                 # "exasol": "SELECT CAST(CAST('2025-04-29 18:47:18' AS DATE) AS TIMESTAMP)",
    #             },
    #             # write={
    #             #     "exasol": "SELECT CAST(CAST('2025-04-29 18:47:18' AS DATE) AS TIMESTAMP)",
    #             #     "databricks": "SELECT CAST(CAST('2025-04-29 18.47.18' AS DATE) AS TIMESTAMP)",
    #             # },
    #         )
    #     if 1 in run:
    #         self.validate_all(
    #             "SELECT TO_CHAR(CAST(CONVERT_TZ(CAST(foo AS TIMESTAMP),'UTC','America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t",
    #             read={
    #                 "databricks": "SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t",
    #             },
    #             # write={
    #             # },
    #         )
    #     if 2 in run:
    #         self.validate_all(
    #             "SELECT DAYS_BETWEEN(created_at, CURRENT_DATE)",
    #             read={
    #                 "databricks": "SELECT DATEDIFF(DAY, created_at, CURRENT_DATE)",
    #             },
    #             # write={
    #             # },
    #         )

    def test_integration_identity(self):
        ######### STRING FUNCTIONS ###########
        # self.validate_identity("SELECT ASCII('X')")
        # self.validate_identity("SELECT BIT_LENGTH('aou') BIT_LENGTH")
        # currently CHARACTER_LENGTH allows more than 1 arg (3)
        # self.validate_identity(
        #     "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
        #     "SELECT LENGTH('aeiouäöü') C_LENGTH",
        # )
        self.validate_identity("SELECT CHAR(88) CHR", "SELECT CHR(88) CHR")
        self.validate_identity("SELECT COLOGNE_PHONETIC('schmitt'), COLOGNE_PHONETIC('Schmidt')")
        self.validate_identity("SELECT CONCAT('abc', 'def', 'g') CONCAT")

        # dumps = [
        #     "SELECT DUMP('123abc') DUMP",
        #     "SELECT DUMP('üäö45', 16) DUMP",
        #     "SELECT DUMP('üäö45', 16, 1) DUMP",
        #     "SELECT DUMP('üäö45', 16, 1, 3) DUMP",
        # ]
        # for d in dumps:
        #     self.validate_identity(d)

        self.validate_identity("SELECT EDIT_DISTANCE('schmitt', 'Schmidt')")
        self.validate_identity("SELECT INITCAP('ExAsOl is great')")
        self.validate_identity("SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')")

        instrs = [
            (
                "SELECT LOCATE('cab','abcabcabc') LOCATE1",
                "SELECT INSTR('abcabcabc', 'cab') LOCATE1"
            ),
            (
                "SELECT LOCATE('user','user1,user2,user3,user4,user5', -1) LOCATE2",
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1) LOCATE2"
            ),
            (
                "SELECT POSITION('cab' IN 'abcabcabc') POS",
                "SELECT INSTR('abcabcabc', 'cab') POS"
            ),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user') INSTR",
                None
            ),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1) INSTR",
                None
            ),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1, 2) INSTR3",
                None
            ),
        ]
        for sql, write_sql in instrs:
            self.validate_identity(sql, write_sql)

        # self.validate_identity("SELECT LOCATE('cab', 'abcabcabc', -1) LOC")
        # self.validate_identity("SELECT POSITION('cab' IN 'abcabcabc') POS")
        self.validate_identity("SELECT LCASE('AbCdEf') LCASE", "SELECT LOWER('AbCdEf') LCASE")
        self.validate_identity("SELECT LOWER('AbCdEf') LCASE")
        self.validate_identity("SELECT LEFT('abcdef', 3) LEFT_SUBSTR")
        self.validate_identity("SELECT LENGTH('abc') LENGTH")
        self.validate_identity(
            "SELECT LTRIM('ab cdef', ' ab') LTRIM",
            "SELECT TRIM(LEADING ' ab' FROM 'ab cdef') LTRIM",
        )
        self.validate_identity(
            "SELECT RTRIM('ab cdef', ' efd') RTRIM",
            "SELECT TRIM(TRAILING ' efd' FROM 'ab cdef') RTRIM",
        )
        self.validate_identity(
            "SELECT MID('abcdef', 2, 3) S1, MID('abcdef', -3) S2, MID('abcdef', 7) S3, MID('abcdef', -7) S4"
        )
        # self.validate_identity("SELECT OCTET_LENGTH('abcd') OCT_LENGTH")
        self.validate_identity(
            r"SELECT REGEXP_REPLACE('From: my_mail@yahoo.com', '(?i)^From: ([a-z0-9._%+-]+)@([a-z0-9.-]+\.[a-z]{2,4}$)', 'Name: \1 - Domain: \2') REGEXP_REPLACE"
        )
        self.validate_identity(
            r"SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') EMAIL"
        )
        self.validate_identity("SELECT REPEAT('abc', 3)")
        self.validate_identity(
            "SELECT REPLACE('Apple juice is great', 'Apple', 'Orange') REPLACE_1"
        )
        # self.validate_identity("SELECT REVERSE('abcde') REVERSE")
        self.validate_identity("SELECT RIGHT('abcdef', 3) RIGHT_SUBSTR")
        self.validate_identity("SELECT RPAD('abc', 5, 'X')")
        self.validate_identity("SELECT LPAD('abc', 5, 'X')")

        self.validate_identity(
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', 4, 2) S2, SUBSTR('abcdef', -3) S3, SUBSTR('abcdef', 7) S4",
        )
        self.validate_identity("SELECT UPPER('AbCdEf') UPPER")
        self.validate_identity("SELECT UCASE('bCdEf') UCASE", "SELECT UPPER('bCdEf') UCASE")

    def test_integration_all(self):
        self.validate_all(
            "SELECT LENGTH('aeiouäöü') C_LENGTH",
            read={
                "exasol": "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
                "databricks": "SELECT char_length('aeiouäöü') C_LENGTH",
                "clickhouse": "SELECT length('aeiouäöü') C_LENGTH",
            },
            write={
                "exasol": "SELECT LENGTH('aeiouäöü') C_LENGTH",
                "databricks": "SELECT LENGTH('aeiouäöü') AS C_LENGTH",
                "clickhouse": "SELECT CHAR_LENGTH('aeiouäöü') AS C_LENGTH",
            },
        )

        self.validate_all(
            "SELECT CHR(99) CHR",
            read={
                "exasol": "SELECT CHAR(99) CHR",
                "databricks": "SELECT char(99) AS CHR",
                "mysql": "SELECT CHAR(99) AS CHR",
            },
            write={
                "exasol": "SELECT CHR(99) CHR",
                "mysql": "SELECT CHAR(99) AS CHR",
                "databricks": "SELECT CHR(99) AS CHR",
            },
        )

        self.validate_all(
            "SELECT CONCAT('abc', 'def', 'g') CONCAT",
            read={
                "exasol": "SELECT CONCAT('abc', 'def', 'g') CONCAT",
                "databricks": "SELECT CONCAT('abc', 'def', 'g') AS CONCAT",
                "clickhouse": "SELECT CONCAT('abc', 'def', 'g') AS CONCAT",
            },
            write={
                "exasol": "SELECT CONCAT('abc', 'def', 'g') CONCAT",
                "databricks": "SELECT CONCAT('abc', 'def', 'g') AS CONCAT",
                "clickhouse": "SELECT CONCAT('abc', 'def', 'g') AS CONCAT",
            },
        )

        self.validate_all(
            "SELECT EDIT_DISTANCE('schmitt', 'Schmidt')",
            read={
                "exasol": "SELECT EDIT_DISTANCE('schmitt', 'Schmidt')",
                "databricks": "SELECT levenshtein('schmitt', 'Schmidt')",
                "clickhouse": "SELECT editDistance('schmitt', 'Schmidt')",
            },
            write={
                "exasol": "SELECT EDIT_DISTANCE('schmitt', 'Schmidt')",
                "databricks": "SELECT LEVENSHTEIN('schmitt', 'Schmidt')",
                "clickhouse": "SELECT editDistance('schmitt', 'Schmidt')",
            },
        )

        self.validate_all(
            "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
            read={
                "exasol": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
                "mysql": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
                "snowflake": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
            },
            write={
                "exasol": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
                "mysql": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
                "snowflake": "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')",
            },
        )

        self.validate_all(
            "SELECT LOWER('AbCdEf') LCASE",
            read={
                "exasol": "SELECT LCASE('AbCdEf') LCASE",
                "databricks": "SELECT LCASE('AbCdEf') AS LCASE",
                "clickhouse": "SELECT lower('AbCdEf') AS LCASE",
            },
            write={
                "exasol": "SELECT LOWER('AbCdEf') LCASE",
                "databricks": "SELECT LOWER('AbCdEf') AS LCASE",
                "clickhouse": "SELECT LOWER('AbCdEf') AS LCASE",
            },
        )

        self.validate_all(
            "SELECT UPPER('AbCdEf') UPPER",
            read={
                "exasol": "SELECT UCASE('AbCdEf') UPPER",
                "databricks": "SELECT UCASE('AbCdEf') AS UPPER",
                "clickhouse": "SELECT upper('AbCdEf') AS UPPER",
            },
            write={
                "exasol": "SELECT UPPER('AbCdEf') UPPER",
                "databricks": "SELECT UPPER('AbCdEf') AS UPPER",
                "clickhouse": "SELECT UPPER('AbCdEf') AS UPPER",
            },
        )

        self.validate_all(
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', 4, 2) S2, SUBSTR('abcdef', -3) S3, SUBSTR('abcdef', 7) S4",
            read={
                "exasol": "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
                "databricks": "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
                "clickhouse": "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
            },
            write={
                "exasol": "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', 4, 2) S2, SUBSTR('abcdef', -3) S3, SUBSTR('abcdef', 7) S4",
                "databricks": "SELECT SUBSTRING('abcdef', 2, 3) AS S1, SUBSTRING('abcdef', 4, 2) AS S2, SUBSTRING('abcdef', -3) AS S3, SUBSTRING('abcdef', 7) AS S4",
                "clickhouse": "SELECT SUBSTRING('abcdef', 2, 3) AS S1, SUBSTRING('abcdef', 4, 2) AS S2, SUBSTRING('abcdef', -3) AS S3, SUBSTRING('abcdef', 7) AS S4",
            },
        )

        self.validate_all(
            "SELECT LOCATE('cab', 'abcabcabc', -1) LOC",
            write={
                "exasol": "SELECT INSTR('abcabcabc', 'cab', -1) LOC",
                "duckdb": "SELECT CASE WHEN STRPOS(SUBSTRING('abcabcabc', -1), 'cab') = 0 THEN 0 ELSE STRPOS(SUBSTRING('abcabcabc', -1), 'cab') + -1 - 1 END AS LOC",
                "presto": "SELECT IF(STRPOS(SUBSTRING('abcabcabc', -1), 'cab') = 0, 0, STRPOS(SUBSTRING('abcabcabc', -1), 'cab') + -1 - 1) AS LOC",
                "hive": "SELECT LOCATE('cab', 'abcabcabc', -1) AS LOC",
                "spark": "SELECT LOCATE('cab', 'abcabcabc', -1) AS LOC",
            },
        )

        self.validate_all(
            "SELECT INITCAP('ExAsOl is great')",
            write={
                "exasol": "SELECT INITCAP('ExAsOl is great')",
                "duckdb": "SELECT INITCAP('ExAsOl is great')",
                "presto": r"SELECT REGEXP_REPLACE('ExAsOl is great', '(\w)(\w*)', x -> UPPER(x[1]) || LOWER(x[2]))",
                "hive": "SELECT INITCAP('ExAsOl is great')",
                "spark": "SELECT INITCAP('ExAsOl is great')",
            },
        )
