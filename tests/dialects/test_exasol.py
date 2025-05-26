from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    dialect = "exasol"

    def test_integration_cross(self):
        pass

    def test_integration_identity(self):
        ########## STRING FUNCTIONS ###########
   
        self.validate_identity("SELECT BIT_LENGTH('aou') BIT_LENGTH")
        self.validate_identity(
            "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
            "SELECT LENGTH('aeiouäöü') C_LENGTH",
        )
        self.validate_identity("SELECT CHAR(88) CHR", "SELECT CHR(88) CHR")
        self.validate_identity(
            "SELECT COLOGNE_PHONETIC('schmitt'), COLOGNE_PHONETIC('Schmidt')"
        )
        self.validate_identity("SELECT CONCAT('abc', 'def', 'g') CONCAT")

        self.validate_identity("SELECT EDIT_DISTANCE('schmitt', 'Schmidt')")
        self.validate_identity("SELECT INITCAP('ExAsOl is great')")
        self.validate_identity(
            "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')"
        )

        instrs = [
            (
                "SELECT INSTR('abcabcabc', 'cab') INSTR1",
                "SELECT POSITION('cab' IN 'abcabcabc') INSTR1",
            ),
            (
                "SELECT INSTR('abcabcabc', 'cab', 1, 1) INSTR1",
                "SELECT INSTR('abcabcabc', 'cab') INSTR1",
            ),
            (
                "SELECT LOCATE('cab', 'abcabcabc', 1) INSTR1",
                "SELECT LOCATE('cab', 'abcabcabc') INSTR1",
            ),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1) INSTR2",
                "SELECT LOCATE('user', 'user1,user2,user3,user4,user5', -1) INSTR2",
            ),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1, 2) INSTR3",
                None,
            ),
        ]
        for sql, write_sql in instrs:
            self.validate_identity(sql, write_sql)

        self.validate_identity("SELECT LOCATE('cab', 'abcabcabc', -1) LOC")
        self.validate_identity("SELECT POSITION('cab' IN 'abcabcabc') POS")
        self.validate_identity(
            "SELECT LCASE('AbCdEf') LCASE", "SELECT LOWER('AbCdEf') LCASE"
        )
        self.validate_identity("SELECT LOWER('AbCdEf') LCASE")
        self.validate_identity("SELECT LEFT('abcdef', 3) LEFT_SUBSTR")
        self.validate_identity("SELECT LENGTH('abc') LENGTH")
        self.validate_identity("SELECT LTRIM('ab cdef', ' ab') LTRIM")
        self.validate_identity("SELECT RTRIM('ab cdef', ' efd') RTRIM")
        self.validate_identity(
            "SELECT MID('abcdef', 2, 3) S1, MID('abcdef', -3) S2, MID('abcdef', 7) S3, MID('abcdef', -7) S4"
        )
     
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

        self.validate_identity("SELECT RIGHT('abcdef', 3) RIGHT_SUBSTR")
        self.validate_identity("SELECT RPAD('abc', 5, 'X')")
        self.validate_identity("SELECT LPAD('abc', 5, 'X')")

        self.validate_identity(
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', 4, 2) S2, SUBSTR('abcdef', -3) S3, SUBSTR('abcdef', 7) S4",
        )
        self.validate_identity("SELECT UPPER('AbCdEf') UPPER")
        self.validate_identity(
            "SELECT UCASE('bCdEf') UCASE", "SELECT UPPER('bCdEf') UCASE"
        )

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
            "SELECT CHR(88) CHR",
            read={
                "exasol": "SELECT CHAR(88) CHR",
                "databricks": "SELECT char(88) AS CHR",
                "mysql": "SELECT CHAR(88) AS CHR",
            },
            write={
                "exasol": "SELECT CHR(88) CHR",
                "mysql": "SELECT CHAR(88) AS CHR",
                "databricks": "SELECT CHR(88) AS CHR",
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
                "exasol": "SELECT LOCATE('cab', 'abcabcabc', -1) LOC",
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


