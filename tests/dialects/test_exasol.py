from tests.dialects.test_dialect import Validator


class TestExasol(Validator):
    dialect = "exasol"

    def test_integration_identity(self):
        self.validate_identity(
            "SELECT MID('abcdef', 2, 3) S1, MID('abcdef', -3) S2, MID('abcdef', 7) S3, MID('abcdef', -7) S4",
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', -3) S2, SUBSTR('abcdef', 7) S3, SUBSTR('abcdef', -7) S4",
        )
        self.validate_identity(
            "SELECT SUBSTRING('abcdef' FROM 4 FOR 2) S",
            "SELECT SUBSTR('abcdef', 4, 2) S",
        )
        self.validate_identity("SELECT EDIT_DISTANCE('schmitt', 'Schmidt')")

        tochars = [
            (
                "SELECT TO_CHAR(DATE '1999-12-31') TO_CHAR",
                "SELECT TO_CHAR(CAST('1999-12-31' AS DATE), 'YYYY-MM-DD HH:MI:SS') TO_CHAR",
            ),
            (
                "SELECT TO_CHAR(TIMESTAMP '1999-12-31 23:59:00','HH24:MI:SS DD-MM-YYYY') TO_CHAR",
                "SELECT TO_CHAR(CAST('1999-12-31 23:59:00' AS TIMESTAMP), 'HH:MI:SS DD-MM-YYYY') TO_CHAR",
            ),
            # TODO: nls_param disappears (https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_char%20(datetime).htm)
            # (
            #     "SELECT TO_CHAR(DATE '2013-12-16', 'DD. MON YYYY', 'NLS_DATE_LANGUAGE=DEU') TO_CHAR",
            #     None,
            # ),
            ("SELECT TO_CHAR(12345.6789) TO_CHAR", None),
            ("SELECT TO_CHAR(12345.67890, '9999999.999999999') TO_CHAR", None),
            ("SELECT TO_CHAR(-12345.67890, '000G000G000D000000MI') TO_CHAR", None),
        ]

        for sql, write_sql in tochars:
            self.validate_identity(sql, write_sql)

        instrs = [
            (
                "SELECT LOCATE('cab','abcabcabc') LOCATE1",
                "SELECT INSTR('abcabcabc', 'cab') LOCATE1",
            ),
            (
                "SELECT LOCATE('user','user1,user2,user3,user4,user5', -1) LOCATE2",
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1) LOCATE2",
            ),
            (
                "SELECT POSITION('cab' IN 'abcabcabc') POS",
                "SELECT INSTR('abcabcabc', 'cab') POS",
            ),
            ("SELECT INSTR('user1,user2,user3,user4,user5', 'user') INSTR", None),
            ("SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1) INSTR", None),
            (
                "SELECT INSTR('user1,user2,user3,user4,user5', 'user', -1, 2) INSTR3",
                None,
            ),
        ]
        for sql, write_sql in instrs:
            self.validate_identity(sql, write_sql)

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
