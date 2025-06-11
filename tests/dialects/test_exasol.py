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
        # self.validate_identity(
        #     "SELECT TO_CHAR(DATE '2013-12-16', 'DD. MON YYYY', 'NLS_DATE_LANGUAGE=DEU') TO_CHAR"
        # )
        # , SUBSTRING('abcdef FROM -3) S3
        ######### STRING FUNCTIONS ###########

        # print("yay")

        # def test_tokenizer_hashtype_variants(self):
        #     sql = "HASHTYPE(256 BIT), HASHTYPE(64 BYTE)"
        #     tokenizer = Exasol.Tokenizer()
        #     tokens = tokenizer.tokenize(sql)
        #     token_data = [(token.token_type, token.text) for token in tokens]
        #     expected = [
        #         (ExasolTokenType.HASHTYPE, "HASHTYPE"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "256"),
        #         (TokenType.BIT, "BIT"),
        #         (TokenType.R_PAREN, ")"),
        #         (TokenType.COMMA, ","),
        #         (ExasolTokenType.HASHTYPE, "HASHTYPE"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "64"),
        #         (ExasolTokenType.BYTE, "BYTE"),
        #         (TokenType.R_PAREN, ")"),
        #     ]
        #     self.assertEqual(token_data, expected)

        # def test_tokenizer_timestamp_with_local_time_zone(self):
        #     sql = "TIMESTAMP(3) WITH LOCAL TIME ZONE"
        #     tokenizer = Tokenizer()
        #     tokens = tokenizer.tokenize(sql)
        #     token_data = [(token.token_type, token.text) for token in tokens]
        #     expected = [
        #         (TokenType.TIMESTAMP, "TIMESTAMP"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "3"),
        #         (TokenType.R_PAREN, ")"),
        #         (ExasolTokenType.WITH_LOCAL_TIME_ZONE, "WITH LOCAL TIME ZONE"),
        #     ]
        #     self.assertEqual(token_data, expected)

        # def test_tokenizer_interval_year_to_month(self):
        #     sql = "INTERVAL YEAR(2) TO MONTH"
        #     tokenizer = Tokenizer()
        #     tokens = tokenizer.tokenize(sql)
        #     token_data = [(token.token_type, token.text) for token in tokens]

        #     expected = [
        #         (TokenType.INTERVAL, "INTERVAL"),
        #         (TokenType.YEAR, "YEAR"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "2"),
        #         (TokenType.R_PAREN, ")"),
        #         (ExasolTokenType.TO, "TO"),
        #         (ExasolTokenType.MONTH, "MONTH"),
        #     ]

        #     self.assertEqual(token_data, expected)

        # def test_tokenizer_interval_day_to_second(self):
        #     sql = "INTERVAL DAY(3) TO SECOND(2)"
        #     tokenizer = Tokenizer()
        #     tokens = tokenizer.tokenize(sql)
        #     token_data = [(token.token_type, token.text) for token in tokens]

        #     expected = [
        #         (TokenType.INTERVAL, "INTERVAL"),
        #         (ExasolTokenType.DAY, "DAY"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "3"),
        #         (TokenType.R_PAREN, ")"),
        #         (ExasolTokenType.TO, "TO"),
        #         (ExasolTokenType.SECOND, "SECOND"),
        #         (TokenType.L_PAREN, "("),
        #         (TokenType.NUMBER, "2"),
        #         (TokenType.R_PAREN, ")"),
        #     ]

        #     self.assertEqual(token_data, expected)

        # --- Generator tests (newly added) ---

    # def test_convert_tz(self):
    #         self.validate_identity(
    #             "CONVERT_TZ(TIMESTAMP '2012-05-10 12:00:00', 'UTC', 'Europe/Berlin')",
    #             exp.ConvertTZ(
    #                 this=exp.Literal.string("TIMESTAMP '2012-05-10 12:00:00'"),
    #                 from_tz=exp.Literal.string("UTC"),
    #                 to_tz=exp.Literal.string("Europe/Berlin")
    #             )
    #         )

    #         self.validate_identity(
    #             "CONVERT_TZ(TIMESTAMP '2012-03-25 02:30:00', 'Europe/Berlin', 'UTC', 'INVALID REJECT AMBIGUOUS REJECT')",
    #             exp.ConvertTZ(
    #                 this=exp.Literal.string("TIMESTAMP '2012-03-25 02:30:00'"),
    #                 from_tz=exp.Literal.string("Europe/Berlin"),
    #                 to_tz=exp.Literal.string("UTC"),
    #                 options=exp.Literal.string("INVALID REJECT AMBIGUOUS REJECT")
    #             )
    #         )

    def test_generator_transforms(self):
        generator = Exasol.Generator()
        generator.dialect.NULL_ORDERING = None
        self.maxDiff = None
        tests = [
            (exp.Abs(this=exp.Column(this="x")), "ABS(x)"),
            (exp.Anonymous(this="ACOS", expressions=[exp.Column(this="x")]), "ACOS(x)"),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Date(this=exp.Literal.string("2000-02-28")),
                        expression=exp.Literal.number(1),
                        unit=exp.to_identifier("DAY"),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_DAYS(DATE '2000-02-28', 1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2001-02-28 12:00:00")),
                        expression=exp.Literal.number(1),
                        unit=exp.to_identifier("DAY"),
                    ),
                    alias=exp.to_identifier("AD2"),
                ),
                "ADD_DAYS(TIMESTAMP '2001-02-28 12:00:00', 1) AD2",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2001-02-28 12:00:00")),
                        expression=exp.Literal.number(-1),
                        unit=exp.to_identifier("HOUR"),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_HOURS(TIMESTAMP '2001-02-28 12:00:00', -1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2001-02-28 12:00:00")),
                        expression=exp.Literal.number(-1),
                        unit=exp.to_identifier("MINUTE"),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_MINUTES(TIMESTAMP '2001-02-28 12:00:00', -1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.AddMonths(
                        this=exp.Timestamp(this=exp.Literal.string("2001-02-28 12:00:00")),
                        expression=exp.Literal.number(2),
                    ),
                    alias=exp.to_identifier("AM1"),
                ),
                "ADD_MONTHS(TIMESTAMP '2001-02-28 12:00:00', 2) AM1",
            ),
            (
                exp.Alias(
                    this=exp.AddMonths(
                        this=exp.Date(this=exp.Literal.string("2001-02-28")),
                        expression=exp.Literal.number(2),
                    ),
                    alias=exp.to_identifier("AM2"),
                ),
                "ADD_MONTHS(DATE '2001-02-28', 2) AM2",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2000-01-01 00:00:00")),
                        expression=exp.Literal.number(1.234),
                        unit=exp.to_identifier("SECOND"),
                    ),
                    alias=exp.to_identifier("AS2"),
                ),
                "ADD_SECONDS(TIMESTAMP '2000-01-01 00:00:00', 1.234) AS2",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2000-01-01 00:00:00")),
                        expression=exp.Literal.number(-1),
                        unit=exp.to_identifier("WEEK"),
                    ),
                    alias=exp.to_identifier("AW1"),
                ),
                "ADD_WEEKS(TIMESTAMP '2000-01-01 00:00:00', -1) AW1",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Timestamp(this=exp.Literal.string("2000-01-01 00:00:00")),
                        expression=exp.Literal.number(1),
                        unit=exp.to_identifier("YEAR"),
                    ),
                    alias=exp.to_identifier("AY1"),
                ),
                "ADD_YEARS(TIMESTAMP '2000-01-01 00:00:00', 1) AY1",
            ),
            (
                exp.Alias(
                    this=exp.Any(
                        this=exp.LT(
                            this=exp.Column(this="age"),
                            expression=exp.Literal.number(30),
                        ),
                        over=exp.Window(
                            partition_by=[exp.Column(this="department")],
                            order=exp.Order(
                                expressions=[
                                    exp.Ordered(
                                        this=exp.Column(this="age", desc=False, nulls_first=None)
                                    )
                                ]
                            ),
                        ),
                    ),
                    alias=exp.to_identifier("ANY_"),
                ),
                "ANY(age < 30) OVER (PARTITION BY department ORDER BY age) ANY_",
            ),
            (
                exp.ApproxDistinct(this=exp.Column(this="x")),
                "APPROXIMATE_COUNT_DISTINCT(x)",
            ),
            (
                exp.Unicode(
                    this=exp.Column(this="x"),
                    sql_name="ASCII",
                ),
                "ASCII(x)",
            ),
            (exp.Anonymous(this="ASIN", expressions=[exp.Column(this="x")]), "ASIN(x)"),
            (exp.Anonymous(this="ATAN", expressions=[exp.Column(this="x")]), "ATAN(x)"),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="ATAN2",
                                expressions=[
                                    exp.Literal.number(1),
                                    exp.Literal.number(1),
                                ],
                            ),
                            alias=exp.to_identifier("ATAN2"),
                        )
                    ]
                ),
                "SELECT ATAN2(1, 1) ATAN2",
            ),
            (exp.Avg(this=exp.Column(this="x")), "AVG(x)"),
            (
                exp.Alias(
                    this=exp.Avg(this=exp.Column(this="starting_salary")),
                    alias=exp.to_identifier("AVG"),
                ),
                "AVG(starting_salary) AVG",
            ),
            (
                exp.Alias(
                    this=exp.Avg(
                        this=exp.Column(this="starting_salary"),
                        over=exp.Window(
                            partition_by=[exp.Column(this="department")],
                            order=exp.Order(
                                expressions=[exp.Ordered(this=exp.Column(this="hire_date"))]
                            ),
                        ),
                    ),
                    alias=exp.to_identifier("AVG"),
                ),
                "AVG(starting_salary) OVER (PARTITION BY department ORDER BY hire_date) AVG",
            ),
            (
                exp.BitwiseAnd(this=exp.Column(this="a"), expression=exp.Column(this="b")),
                "BIT_AND(a, b)",
            ),
            # (
            #     exp.BitCheck(
            #         this=exp.Column(this="a"), expression=exp.Column(this="b")
            #     ),
            #     "BIT_CHECK(a, b)",
            # ),
            # (
            #     exp.Alias(
            #         this=exp.BitLength(this=exp.Literal.string("äöü")),
            #         alias=exp.to_identifier("BIT_LENGTH"),
            #     ),
            #     "BIT_LENGTH('äöü') BIT_LENGTH",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.BitLRotate(
            #                 this=exp.Column(this="1024"),
            #                 expression=exp.Column(this="63"),
            #             )
            #         ]
            #     ),
            #     "SELECT BIT_LROTATE(1024, 63)",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.BitwiseLeftShift(
                            this=exp.Column(this="1"), expression=exp.Column(this="10")
                        )
                    ]
                ),
                "SELECT BIT_LSHIFT(1, 10)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.BitwiseRightShift(
                            this=exp.Column(this="1"), expression=exp.Column(this="10")
                        )
                    ]
                ),
                "SELECT BIT_RSHIFT(1, 10)",
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.BitSet(
            #                 this=exp.Column(this="1"), expression=exp.Column(this="10")
            #             )
            #         ]
            #     ),
            #     "SELECT BIT_SET(1, 10)",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.BitToNum(
            #                 expressions=[
            #                     exp.Literal.number(1),
            #                     exp.Literal.number(1),
            #                     exp.Literal.number(0),
            #                     exp.Literal.number(0),
            #                 ]
            #             )
            #         ]
            #     ),
            #     "SELECT BIT_TO_NUM(1, 1, 0, 0)",
            # ),
            (
                exp.BitwiseOr(this=exp.Column(this="a"), expression=exp.Column(this="b")),
                "BIT_OR(a, b)",
            ),
            (exp.BitwiseNot(this=exp.Column(this="a")), "BIT_NOT(a)"),
            (
                exp.BitwiseXor(this=exp.Column(this="a"), expression=exp.Column(this="b")),
                "BIT_XOR(a, b)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="name"),
                        exp.Alias(
                            this=exp.Case(
                                this=exp.Column(this="grade"),
                                ifs=[
                                    exp.When(
                                        this=exp.Literal.number(1),
                                        true=exp.Literal.string("VERY GOOD"),
                                    ),
                                    exp.When(
                                        this=exp.Literal.number(2),
                                        true=exp.Literal.string("GOOD"),
                                    ),
                                    exp.When(
                                        this=exp.Literal.number(3),
                                        true=exp.Literal.string("SATISFACTORY"),
                                    ),
                                    exp.When(
                                        this=exp.Literal.number(4),
                                        true=exp.Literal.string("FAIR"),
                                    ),
                                    exp.When(
                                        this=exp.Literal.number(5),
                                        true=exp.Literal.string("UNSATISFACTORY"),
                                    ),
                                    exp.When(
                                        this=exp.Literal.number(6),
                                        true=exp.Literal.string("POOR"),
                                    ),
                                ],
                                default=exp.Literal.string("INVALID"),
                            ),
                            alias=exp.to_identifier("GRADE"),
                        ),
                    ]
                ),
                "SELECT name, CASE grade WHEN 1 THEN 'VERY GOOD' WHEN 2 THEN 'GOOD' WHEN 3 THEN 'SATISFACTORY' WHEN 4 THEN 'FAIR' WHEN 5 THEN 'UNSATISFACTORY' WHEN 6 THEN 'POOR' ELSE 'INVALID' END GRADE",
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.Length(
            #                     this=exp.Literal.string("aeiouäöü")
            #                 ),
            #                 alias=exp.to_identifier("C_LENGTH"),
            #             )
            #         ]
            #     ),
            #     "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="COLOGNE_PHONETIC",
                            expressions=[
                                exp.Literal.string("schmitt"),
                            ],
                        ),
                        exp.Anonymous(
                            this="COLOGNE_PHONETIC",
                            expressions=[
                                exp.Literal.string("Schmidt"),
                            ],
                        ),
                    ]
                ),
                "SELECT COLOGNE_PHONETIC('schmitt'), COLOGNE_PHONETIC('Schmidt')",
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Anonymous(this="CONNECT_BY_ISCYCLE"),
            #             exp.Alias(
            #                 this=exp.SysConnectByPath(
            #                     this=exp.Column(this="last_name"),
            #                     expressions=[exp.Literal.string("/")],
            #                 ),
            #                 alias=exp.to_identifier("PATH"),
            #             ),
            #         ],
            #     )
            #     .from_("employees")
            #     .where("last_name='clark'"),
            #     "SELECT CONNECT_BY_ISCYCLE, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.ConnectByIsLeaf(),
            #             exp.Alias(
            #                 this=exp.SysConnectByPath(
            #                     this=exp.Column(this="last_name"),
            #                     expressions=[exp.Literal.string("/")],
            #                 ),
            #                 alias=exp.to_identifier("PATH"),
            #             ),
            #         ],
            #     )
            #     .from_("employees")
            #     .where("last_name='clark'"),
            #     "SELECT CONNECT_BY_ISLEAF, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Anonymous(this="CONNECT_BY_ISCYCLE"),
            #             exp.Alias(
            #                 this=exp.SysConnectByPath(
            #                     this=exp.Column(this="last_name"),
            #                     expressions=[exp.Literal.string("/")],
            #                 ),
            #                 alias=exp.to_identifier("PATH"),
            #             ),
            #         ],
            #     )
            #     .from_("employees")
            #     .where("last_name='clark'"),
            #     "SELECT CONNECT_BY_ISCYCLE, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.ConnectByIsLeaf(),
            #             exp.Alias(
            #                 this=exp.SysConnectByPath(
            #                     this=exp.Column(this="last_name"),
            #                     expressions=[exp.Literal.string("/")],
            #                 ),
            #                 alias=exp.to_identifier("PATH"),
            #             ),
            #         ],
            #     )
            #     .from_("employees")
            #     .where("last_name='clark'"),
            #     "SELECT CONNECT_BY_ISLEAF, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            # ),
            (
                exp.Convert(this=exp.Var(this="CHAR(15)"), expression=exp.Literal.string("ABC")),
                "CONVERT(CHAR(15), 'ABC')",
            ),
            (
                exp.Convert(
                    this=exp.Var(this="DATE"),
                    expression=exp.Literal.string("2006-01-01"),
                ),
                "CONVERT(DATE, '2006-01-01')",
            ),
            # (
            #     exp.ConvertTZ(
            #         this=exp.Timestamp(this=exp.Literal.string("2012-05-10 12:00:00")),
            #         from_tz=exp.Literal.string("UTC"),
            #         to_tz=exp.Literal.string("Europe/Berlin"),
            #     ),
            #     "CONVERT_TZ(TIMESTAMP '2012-05-10 12:00:00', 'UTC', 'Europe/Berlin')",
            # ),
            # (
            #     exp.ConvertTZ(
            #         this=exp.Timestamp(this=exp.Literal.string("2012-03-25 02:30:00")),
            #         from_tz=exp.Literal.string("Europe/Berlin"),
            #         to_tz=exp.Literal.string("UTC"),
            #         options=exp.Literal.string("INVALID REJECT AMBIGUOUS REJECT"),
            #     ),
            #     "CONVERT_TZ(TIMESTAMP '2012-03-25 02:30:00', 'Europe/Berlin', 'UTC', 'INVALID REJECT AMBIGUOUS REJECT')",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Column(this="department"),
            #             exp.Alias(
            #                 this=exp.Corr(
            #                     this=exp.Column(this="age"),
            #                     expression=exp.Column(this="current_salary"),
            #                 ),
            #                 alias=exp.to_identifier("CORR"),
            #             ),
            #         ],
            #     )
            #     .from_("employee_table")
            #     .group_by("department"),
            #     "SELECT department, CORR(age, current_salary) CORR FROM employee_table GROUP BY department",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="id"),
                        exp.Column(this="department"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Count(this=exp.Star()),
                                partition_by=[exp.Column(this="department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.Column(this="age"))]
                                ),
                            ),
                            alias=exp.to_identifier("COUNT"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.Column(this="department")),
                            exp.Ordered(this=exp.Column(this="age")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT id, department, COUNT(*) OVER (PARTITION BY department ORDER BY age) COUNT FROM "employee_table" ORDER BY department, age',
            ),
            (
                exp.CovarPop(this=exp.Column(this="a"), expression=exp.Column(this="b")),
                "COVAR_POP(a, b)",
            ),
            (
                exp.CovarSamp(this=exp.Column(this="a"), expression=exp.Column(this="b")),
                "COVAR_SAMP(a, b)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("current_salary"),
                        exp.to_identifier("age"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.CovarSamp(
                                    this=exp.to_identifier("age"),
                                    expression=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("age"))]
                                ),
                            ),
                            alias=exp.to_identifier("COVAR_SAMP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("age")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "current_salary", "age", COVAR_SAMP("age", "current_salary") OVER (PARTITION BY "department" ORDER BY "age") COVAR_SAMP FROM "employee_table" ORDER BY "department", "age"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="COS",
                            expressions=[
                                exp.Div(
                                    this=exp.Anonymous(this="PI"),
                                    expression=exp.Literal.number(3),
                                )
                            ],
                        )
                    ]
                ),
                "SELECT COS(PI() / 3)",
            ),
            (
                exp.Select(
                    expressions=[exp.Anonymous(this="COSH", expressions=[exp.Literal.number(1)])]
                ),
                "SELECT COSH(1)",
            ),
            (
                exp.Select(
                    expressions=[exp.Anonymous(this="COT", expressions=[exp.Literal.number(1)])]
                ),
                "SELECT COT(1)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(this="CUME_DIST"),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("current_salary"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("CUME_DIST"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "current_salary", CUME_DIST() OVER (PARTITION BY "department" ORDER BY "current_salary") CUME_DIST FROM "employee_table" ORDER BY "department", "current_salary"',
            ),
            # (exp.Select(expressions=[exp.CurrentCluster()]), "SELECT CURRENT_CLUSTER"),
            # (exp.Select(expressions=[exp.CurrentCluster()]), "SELECT CURRENT_CLUSTER"),
            (exp.CurrentDate(), "CURRENT_DATE"),
            (exp.CurrentSchema(), "CURRENT_SCHEMA"),
            # (exp.Select(expressions=[exp.CurrentSession()]), "SELECT CURRENT_SESSION"),
            # (
            #     exp.Select(expressions=[exp.CurrentStatement()]),
            #     "SELECT CURRENT_STATEMENT",
            # ),
            # (exp.Select(expressions=[exp.CurrentSession()]), "SELECT CURRENT_SESSION"),
            # (
            #     exp.Select(expressions=[exp.CurrentStatement()]),
            #     "SELECT CURRENT_STATEMENT",
            # ),
            (exp.Select(expressions=[exp.CurrentUser()]), "SELECT CURRENT_USER"),
            (exp.CurrentTimestamp(), "CURRENT_TIMESTAMP"),
            (exp.CurrentTimestamp(this=exp.Literal.number(6)), "CURRENT_TIMESTAMP(6)"),
            (
                exp.DateTrunc(
                    unit=exp.Literal.string("minute"),
                    this=exp.Timestamp(this=exp.Literal.string("2006-12-31 23:59:59")),
                ),
                "DATE_TRUNC('minute', TIMESTAMP '2006-12-31 23:59:59')",
            ),
            (
                exp.DateTrunc(
                    unit=exp.Literal.string("month"),
                    this=exp.Date(this=exp.Literal.string("2006-12-31")),
                ),
                "DATE_TRUNC('month', DATE '2006-12-31')",
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.DaysBetween(
            #                     this=exp.Date(this=exp.Literal.string("1999-12-31")),
            #                     expression=exp.Date(this=exp.Literal.string("2000-01-01")),
            #                 ),
            #                 alias=exp.to_identifier("DB1"),
            #             ),
            #             exp.Alias(
            #                 this=exp.DaysBetween(
            #                     this=exp.Timestamp(this=exp.Literal.string("2000-01-01 12:00:00")),
            #                     expression=exp.Timestamp(
            #                         this=exp.Literal.string("1999-12-31 00:00:00")
            #                     ),
            #                 ),
            #                 alias=exp.to_identifier("DB2"),
            #             ),
            #         ]
            #     ),
            #     "SELECT DAYS_BETWEEN(DATE '1999-12-31', DATE '2000-01-01') DB1, DAYS_BETWEEN(TIMESTAMP '2000-01-01 12:00:00', TIMESTAMP '1999-12-31 00:00:00') DB2",
            # ),
            # (exp.Select(expressions=[exp.DBTimezone()]), "SELECT DBTIMEZONE"),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.DaysBetween(
            #                     this=exp.Date(this=exp.Literal.string("1999-12-31")),
            #                     expression=exp.Date(this=exp.Literal.string("2000-01-01")),
            #                 ),
            #                 alias=exp.to_identifier("DB1"),
            #             ),
            #             exp.Alias(
            #                 this=exp.DaysBetween(
            #                     this=exp.Timestamp(this=exp.Literal.string("2000-01-01 12:00:00")),
            #                     expression=exp.Timestamp(
            #                         this=exp.Literal.string("1999-12-31 00:00:00")
            #                     ),
            #                 ),
            #                 alias=exp.to_identifier("DB2"),
            #             ),
            #         ]
            #     ),
            #     "SELECT DAYS_BETWEEN(DATE '1999-12-31', DATE '2000-01-01') DB1, DAYS_BETWEEN(TIMESTAMP '2000-01-01 12:00:00', TIMESTAMP '1999-12-31 00:00:00') DB2",
            # ),
            # (exp.Select(expressions=[exp.DBTimezone()]), "SELECT DBTIMEZONE"),
            (
                exp.Select(
                    expressions=[
                        exp.Decode(
                            expressions=[
                                exp.Literal.string("abc"),
                                exp.Literal.string("xyz"),
                                exp.Literal.number(1),
                                exp.Literal.string("abc"),
                                exp.Literal.number(2),
                                exp.Literal.number(3),
                            ]
                        )
                    ]
                ),
                "SELECT DECODE('abc', 'xyz', 1, 'abc', 2, 3)",
            ),
            # (
            #     exp.Select(expressions=[exp.Degrees(this=exp.Anonymous(this="PI"))]),
            #     "SELECT DEGREES(PI())",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Column(this="id"),
            #             exp.Column(this="department"),
            #             exp.Column(this="current_salary"),
            #             exp.Alias(
            #                 this=exp.Window(
            #                     this=exp.DenseRank(),
            #                     partition_by=[exp.Column(this="department")],
            #                     order=exp.Order(
            #                         expressions=[
            #                             exp.Ordered(this=exp.Column(this="current_salary"))
            #                         ]
            #                     ),
            #                 ),
            #                 alias=exp.to_identifier("DENSE_RANK"),
            #             ),
            #         ],
            #         order=exp.Order(
            #             expressions=[
            #                 exp.Ordered(this=exp.Column(this="department")),
            #                 exp.Ordered(this=exp.Column(this="current_salary")),
            #             ]
            #         ),
            #     ).from_("employee_table"),
            #     "SELECT id, department, current_salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY current_salary) DENSE_RANK FROM employee_table ORDER BY department, current_salary",
            # ),
            # (
            #     exp.Select(expressions=[exp.Degrees(this=exp.Anonymous(this="PI"))]),
            #     "SELECT DEGREES(PI())",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Column(this="id"),
            #             exp.Column(this="department"),
            #             exp.Column(this="current_salary"),
            #             exp.Alias(
            #                 this=exp.Window(
            #                     this=exp.DenseRank(),
            #                     partition_by=[exp.Column(this="department")],
            #                     order=exp.Order(
            #                         expressions=[
            #                             exp.Ordered(this=exp.Column(this="current_salary"))
            #                         ]
            #                     ),
            #                 ),
            #                 alias=exp.to_identifier("DENSE_RANK"),
            #             ),
            #         ],
            #         order=exp.Order(
            #             expressions=[
            #                 exp.Ordered(this=exp.Column(this="department")),
            #                 exp.Ordered(this=exp.Column(this="current_salary")),
            #             ]
            #         ),
            #     ).from_("employee_table"),
            #     "SELECT id, department, current_salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY current_salary) DENSE_RANK FROM employee_table ORDER BY department, current_salary",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.Func(
            #                     this="DUMP",
            #                     args={
            #                         "expressions": [
            #                             exp.Literal.string("üäö45"),
            #                             exp.Literal.number(1010),
            #                             exp.Literal.number(1),
            #                             exp.Literal.number(3),
            #                         ]
            #                     },
            #                 ),
            #                 alias=exp.to_identifier("D"),
            #             )
            #         ]
            #     ),
            #     "SELECT DUMP('üäö45', 1010, 1, 3) D",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.Dump(
            #                     this=exp.Literal.string("üäö45"),
            #                     format=exp.Literal.number(1010),
            #                     start_position=exp.Literal.number(1),
            #                     length=exp.Literal.number(3),
            #                 ),
            #                 alias=exp.to_identifier("D"),
            #             )
            #         ]
            #     ),
            #     "SELECT DUMP('üäö45', 1010, 1, 3) D",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Column(this="department"),
            #             exp.Alias(
            #                 this=exp.Every(this=exp.Column(this="age >= 30")),
            #                 alias=exp.to_identifier("EVERY"),
            #             ),
            #         ]
            #     )
            #     .from_("employee_table")
            #     .group_by("department"),
            #     "SELECT department, EVERY(age >= 30) EVERY FROM employee_table GROUP BY department",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Column(this="department"),
            #             exp.Alias(
            #                 this=exp.Every(this=exp.Column(this="age >= 30")),
            #                 alias=exp.to_identifier("EVERY"),
            #             ),
            #         ]
            #     )
            #     .from_("employee_table")
            #     .group_by("department"),
            #     "SELECT department, EVERY(age >= 30) EVERY FROM employee_table GROUP BY department",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Extract(
                                this=exp.Timestamp(
                                    this=exp.Literal.string("2000-10-01 12:22:59.123")
                                ),
                                expression=exp.Var(this="SECOND"),
                            ),
                            alias=exp.to_identifier("EXS"),
                        ),
                        exp.Alias(
                            this=exp.Extract(
                                this=exp.Date(this=exp.Literal.string("2000-10-01")),
                                expression=exp.Var(this="MONTH"),
                            ),
                            alias=exp.to_identifier("EXM"),
                        ),
                        exp.Alias(
                            this=exp.Extract(
                                this=exp.Literal(
                                    this="1 23:59:30.123",
                                    is_string=True,
                                    prefix="INTERVAL DAY TO SECOND",
                                ),
                                expression=exp.Var(this="HOUR"),
                            ),
                            alias=exp.to_identifier("EXH"),
                        ),
                    ]
                ),
                "SELECT EXTRACT(SECOND FROM TIMESTAMP '2000-10-01 12:22:59.123') EXS, "
                "EXTRACT(MONTH FROM DATE '2000-10-01') EXM, "
                "EXTRACT(HOUR FROM INTERVAL DAY TO SECOND '1 23:59:30.123') EXH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.FirstValue(
                                this=exp.Column(this="x"),
                                over=exp.Window(
                                    partition_by=[exp.Column(this="department")],
                                    order=exp.Order(
                                        expressions=[exp.Ordered(this=exp.Column(this="x"))]
                                    ),
                                ),
                            ),
                            alias=exp.to_identifier("fv"),
                        )
                    ]
                ),
                "SELECT FIRST_VALUE(x) OVER (PARTITION BY department ORDER BY x) fv",
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.FromPosixTime(this=exp.Column(this="x")),
            #                 alias=exp.to_identifier("FPT1"),
            #             )
            #         ]
            #     ),
            #     "SELECT FROM_POSIX_TIME(x) FPT1",
            # ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.FromPosixTime(this=exp.Column(this="x")),
            #                 alias=exp.to_identifier("FPT1"),
            #             )
            #         ]
            #     ),
            #     "SELECT FROM_POSIX_TIME(x) FPT1",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Greatest(
                                expressions=[
                                    exp.Column(this="x"),
                                    exp.Column(this="y"),
                                    exp.Column(this="z"),
                                ]
                            ),
                            alias=exp.to_identifier("GREATEST"),
                        )
                    ]
                ),
                "SELECT GREATEST(x, y, z) GREATEST",
            ),
            (
                exp.Alias(
                    this=exp.GroupConcat(
                        this=exp.Column(this="id"),
                        order=[exp.Ordered(this=exp.Column(this="hire_date"))],
                        separator=exp.Literal.string(", "),
                    ),
                    alias=exp.to_identifier("GROUP_CONCAT_RESULT"),
                ),
                "GROUP_CONCAT(id ORDER BY hire_date SEPARATOR ', ') GROUP_CONCAT_RESULT",
            ),
            (
                exp.Select(expressions=[exp.MD5(expressions=[exp.Column(this="abc")])]),
                "SELECT HASH_MD5(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.MD5(expressions=[exp.Column(this="c1"), exp.Column(this="c2")])
                    ]
                ),
                "SELECT HASH_MD5(c1, c2)",
            ),
            (
                exp.Select(expressions=[exp.SHA(expressions=[exp.Column(this="abc")])]),
                "SELECT HASH_SHA1(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.SHA(expressions=[exp.Column(this="c1"), exp.Column(this="c2")])
                    ]
                ),
                "SELECT HASH_SHA1(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="name"),
                        exp.Column(this="age"),
                        exp.Alias(
                            this=exp.If(
                                this=exp.LT(
                                    this=exp.Column(this="age"),
                                    expression=exp.Literal.number(18),
                                ),
                                true=exp.Literal.string("underaged"),
                                false=exp.Literal.string("adult"),
                            ),
                            alias=exp.to_identifier("LEGALITY"),
                        ),
                    ],
                ).from_("persons"),
                "SELECT name, age, IF age < 18 THEN 'underaged' ELSE 'adult' ENDIF LEGALITY FROM \"persons\"",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Initcap(this=exp.Literal.string("ExAsOl is great")),
                            alias=exp.to_identifier("INITCAP"),
                        )
                    ]
                ),
                "SELECT INITCAP('ExAsOl is great') INITCAP",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Stuff(
                            this=exp.Literal.string("abc"),
                            start=exp.Literal.number(2),
                            length=exp.Literal.number(2),
                            expression=exp.Literal.string("xxx"),
                        )
                    ]
                ),
                "SELECT INSERT('abc', 2, 2, 'xxx')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.StrPosition(
                                this=exp.Literal.string("abcabcabc"),
                                substr=exp.Literal.string("cab"),
                                position=exp.Literal.number(1),
                                occurrence=exp.Literal.number(1),
                            ),
                            alias="INSTR1",
                        ),
                        exp.Alias(
                            this=exp.StrPosition(
                                this=exp.Literal.string("user1,user2,user3,user4,user5"),
                                substr=exp.Literal.string("user"),
                                position=exp.Literal.number(-1),
                                occurrence=exp.Literal.number(2),
                            ),
                            alias="INSTR2",
                        ),
                    ]
                ),
                "SELECT INSTR('abcabcabc', 'cab') INSTR1, "
                "INSTR('user1,user2,user3,user4,user5', 'user', -1, 2) INSTR2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Identifier(this="c1"),
                        exp.Alias(
                            this=exp.Anonymous(this="IPROC"),
                            alias=exp.to_identifier("IPROC"),
                        ),
                    ],
                    order=exp.Order(expressions=[exp.Ordered(this=exp.Identifier(this="c1"))]),
                ).from_("t"),
                'SELECT "c1", IPROC() IPROC FROM "t" ORDER BY "c1"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Is(
                                this=exp.Literal.string("xyz"),
                                type=exp.to_identifier("BOOLEAN"),
                            ),
                            alias=exp.to_identifier("IS_BOOLEAN"),
                        ),
                        exp.Alias(
                            this=exp.Is(
                                this=exp.Literal.string("+12.34"),
                                type=exp.to_identifier("NUMBER"),
                            ),
                            alias=exp.to_identifier("IS_NUMBER"),
                        ),
                        exp.Alias(
                            this=exp.Is(
                                this=exp.Literal.string("12.13.2011"),
                                expression=exp.Literal.string("DD.MM.YYYY"),
                                type=exp.to_identifier("DATE"),
                            ),
                            alias=exp.to_identifier("IS_DATE"),
                        ),
                    ]
                ),
                "SELECT IS_BOOLEAN('xyz') IS_BOOLEAN, IS_NUMBER('+12.34') IS_NUMBER, IS_DATE('12.13.2011', 'DD.MM.YYYY') IS_DATE",
            ),
            # (exp.FirstValue(this=exp.Column(this="x")), "FIRST_VALUE(x)"),
            (
                exp.Alias(
                    this=exp.JSONBExtract(
                        this=exp.Column(this="json"),
                        expressions=[
                            exp.Literal.string("$.firstname"),
                            exp.Literal.string("$.surname"),
                            exp.Literal.string("$.age"),
                            exp.Literal.string("$.error()"),
                        ],
                        emits=[
                            ("forename", "VARCHAR(100)"),
                            ("surname", "VARCHAR(100)"),
                            ("age", "INT"),
                            ("error_column", "VARCHAR(2000000)"),
                        ],
                    ),
                    alias=exp.to_identifier("extracted_json"),
                ),
                "JSON_EXTRACT(json, '$.firstname', '$.surname', '$.age', '$.error()') "
                "EMITS(forename VARCHAR(100), surname VARCHAR(100), age INT, error_column VARCHAR(2000000)) extracted_json",
            ),
            (
                exp.JSONValue(
                    this=exp.Column(this="json"),
                    path=exp.Literal.string("$.name"),
                    null_on_empty=True,
                    default=exp.Literal.string("invalid name"),
                    on_error=True,  # or make this a separate flag if needed
                ),
                "JSON_VALUE(json, '$.name' NULL ON EMPTY DEFAULT 'invalid name' ON ERROR)",
            ),
            (
                exp.JSONValue(
                    this=exp.Column(this="json"),
                    path=exp.Literal.string("$.name"),
                    null_on_empty=True,
                    default=exp.Literal.string("invalid name"),
                    on_error=True,  # or make this a separate flag if needed
                ),
                "JSON_VALUE(json, '$.name' NULL ON EMPTY DEFAULT 'invalid name' ON ERROR)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Lag(
                                    this=exp.to_identifier("id"),
                                    expression=exp.Literal.number(1),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("LAG"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", LAG("id", 1) OVER (PARTITION BY "department" ORDER BY "hire_date") LAG FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.LastValue(this=exp.Column(this="id")),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("LAST_"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", LAST_VALUE(id) OVER (PARTITION BY "department" ORDER BY "hire_date") LAST_ FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            # (
            #     exp.Select(
            #         expressions=[
            #             exp.Alias(
            #                 this=exp.LCase(this=exp.Literal.string("AbCdEf")),
            #                 alias=exp.to_identifier("LCASE"),
            #             )
            #         ]
            #     ),
            #     "SELECT LCASE('AbCdEf') LCASE",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Lead(
                                    this=exp.to_identifier("id"),
                                    expression=exp.Literal.number(1),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("LEAD"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", LEAD("id", 1) OVER (PARTITION BY "department" ORDER BY "hire_date") LEAD FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Least(
                                expressions=[
                                    exp.Literal.number(3),
                                    exp.Literal.number(1),
                                    exp.Literal.number(5),
                                ]
                            ),
                            alias=exp.to_identifier("LEAST"),
                        )
                    ]
                ),
                "SELECT LEAST(3, 1, 5) LEAST",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Left(
                                this=exp.Literal.string("abcdef"),
                                expression=exp.Literal.number(3),
                            ),
                            alias=exp.to_identifier("LEFT_SUBSTR"),
                        )
                    ]
                ),
                "SELECT LEFT('abcdef', 3) LEFT_SUBSTR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Length(this=exp.Literal.string("abc")),
                            alias=exp.to_identifier("LENGTH"),
                        )
                    ]
                ),
                "SELECT LENGTH('abc') LENGTH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Ln(this=exp.Literal.number(10)),
                            alias=exp.to_identifier("LN"),
                        )
                    ]
                ),
                "SELECT LN(10) LN",
            ),
            (
                exp.Select(expressions=[exp.Lower(this=exp.Literal.string("HELLO"))]),
                "SELECT LOWER('HELLO')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Pad(
                            this=exp.Literal.string("abc"),
                            expression=exp.Literal.number(5),
                            fill_pattern=exp.Literal.string("X"),
                            is_left=True,
                        ),
                    ]
                ),
                "SELECT LPAD('abc', 5, 'X')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Trim(
                            this=exp.Literal.string("  abc"),
                            expression=exp.Literal.string(" "),
                            position="LEADING",
                        )
                    ]
                ),
                "SELECT TRIM(LEADING ' ' FROM '  abc')",
            ),
            (
                exp.Mod(this=exp.Column(this="15"), expression=exp.Column(this="6")),
                "MOD(15, 6)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="MINUTES_BETWEEN",
                                expressions=[
                                    exp.Timestamp(this=exp.Literal.string("2012-03-25 02:30:00")),
                                    exp.Timestamp(this=exp.Literal.string("2000-01-01 12:00:02")),
                                ],
                            ),
                            alias="MINUTES",
                        )
                    ]
                ),
                "SELECT MINUTES_BETWEEN(TIMESTAMP '2012-03-25 02:30:00', TIMESTAMP '2000-01-01 12:00:02') MINUTES",
            ),
            (
                exp.Select(
                    expressions=[exp.Alias(this=exp.Anonymous(this="NOW"), alias="NOW_TIME")]
                ),
                "SELECT NOW() NOW_TIME",
            ),
            (
                exp.Select(
                    expressions=[exp.Alias(this=exp.Anonymous(this="NPROC"), alias="PROC_COUNT")]
                ),
                "SELECT NPROC() PROC_COUNT",
            ),
            (
                exp.Alias(
                    this=exp.NthValue(
                        this=exp.Column(this="id"),
                        expression=exp.Literal.number(3),
                        from_last=True,
                        respect_nulls=True,
                        over=exp.Window(
                            partition_by=[exp.Column(this="department")],
                            order=exp.Order(
                                expressions=[exp.Ordered(this=exp.Column(this="hire_date"))]
                            ),
                        ),
                    ),
                    alias=exp.to_identifier("NTH_VAL"),
                ),
                "NTH_VALUE(id, 3) FROM LAST RESPECT NULLS OVER (PARTITION BY department ORDER BY hire_date) NTH_VAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(
                                    this="NTILE", expressions=[exp.Literal.number(4)]
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("NTILE"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", NTILE(4) OVER (PARTITION BY "department" ORDER BY "hire_date") NTILE FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NULLIF",
                                expressions=[
                                    exp.Column(this="1"),
                                    exp.Column(this="2"),
                                ],
                            ),
                            alias="NULLIF1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NULLIF",
                                expressions=[
                                    exp.Column(this="1"),
                                    exp.Column(this="1"),
                                ],
                            ),
                            alias="NULLIF2",
                        ),
                    ]
                ),
                "SELECT NULLIF(1, 2) NULLIF1, NULLIF(1, 1) NULLIF2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NULLIFZERO",
                                expressions=[
                                    exp.Column(this="0"),
                                ],
                            ),
                            alias="NIZ1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NULLIFZERO",
                                expressions=[
                                    exp.Column(this="1"),
                                ],
                            ),
                            alias="NIZ2",
                        ),
                    ]
                ),
                "SELECT NULLIFZERO(0) NIZ1, NULLIFZERO(1) NIZ2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NUMTODSINTERVAL",
                                expressions=[
                                    exp.Column(this="3.2"),
                                    exp.Literal.string("HOUR"),
                                ],
                            ),
                            alias="NUMTODSINTERVAL",
                        )
                    ]
                ),
                "SELECT NUMTODSINTERVAL(3.2, 'HOUR') NUMTODSINTERVAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NUMTOYMINTERVAL",
                                expressions=[
                                    exp.Column(this="3.5"),
                                    exp.Literal.string("YEAR"),
                                ],
                            ),
                            alias="NUMTOYMINTERVAL",
                        )
                    ]
                ),
                "SELECT NUMTOYMINTERVAL(3.5, 'YEAR') NUMTOYMINTERVAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NVL",
                                expressions=[
                                    exp.Column(this="NULL"),
                                    exp.Literal.string("abc"),
                                ],
                            ),
                            alias="NVL_1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NVL",
                                expressions=[
                                    exp.Literal.string("xyz"),
                                    exp.Literal.string("abc"),
                                ],
                            ),
                            alias="NVL_2",
                        ),
                    ]
                ),
                "SELECT NVL(NULL, 'abc') NVL_1, NVL('xyz', 'abc') NVL_2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NVL2",
                                expressions=[
                                    exp.Column(this="NULL"),
                                    exp.Literal.number(2),
                                    exp.Literal.number(3),
                                ],
                            ),
                            alias="NVL_1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="NVL2",
                                expressions=[
                                    exp.Literal.number(1),
                                    exp.Literal.number(2),
                                    exp.Literal.number(3),
                                ],
                            ),
                            alias="NVL_2",
                        ),
                    ]
                ),
                "SELECT NVL2(NULL, 2, 3) NVL_1, NVL2(1, 2, 3) NVL_2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="OCTET_LENGTH",
                                expressions=[exp.Literal.string("äöü")],
                            ),
                            alias="OCT_LENGTH",
                        )
                    ]
                ),
                "SELECT OCTET_LENGTH('äöü') OCT_LENGTH",
            ),
            (
                exp.Select(expressions=[exp.Alias(this=exp.Anonymous(this="PI"), alias="PI_VAL")]),
                "SELECT PI() PI_VAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(this="PERCENT_RANK"),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("current_salary"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("PERCENT_RANK"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "current_salary", PERCENT_RANK() OVER (PARTITION BY "department" ORDER BY "current_salary") PERCENT_RANK FROM "employee_table" ORDER BY "department", "current_salary"',
            ),
            (
                exp.Alias(
                    this=exp.PercentileDisc(
                        this=exp.Literal.number(0.7),
                        order=exp.Order(
                            expressions=[exp.Ordered(this=exp.Column(this="current_salary"))]
                        ),
                        over=exp.Window(partition_by=[exp.Column(this="department")]),
                    ),
                    alias=exp.to_identifier("PERCENTILE_DISC"),
                ),
                "PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY current_salary) OVER (PARTITION BY department) PERCENTILE_DISC",
            ),
            (
                exp.Alias(
                    this=exp.PercentileCont(
                        this=exp.Literal.number(0.7),
                        order=exp.Order(
                            expressions=[exp.Ordered(this=exp.Column(this="current_salary"))]
                        ),
                        over=exp.Window(partition_by=[exp.Column(this="department")]),
                    ),
                    alias=exp.to_identifier("PERCENTILE_CONT"),
                ),
                "PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY current_salary) OVER (PARTITION BY department) PERCENTILE_CONT",
            ),
            (
                exp.Alias(
                    this=exp.StrPosition(
                        this=exp.Literal.string("abcabcabc"),
                        substr=exp.Literal.string("cab"),
                    ),
                    alias=exp.to_identifier("POS"),
                ),
                "POSITION('cab' IN 'abcabcabc') POS",
            ),
            (
                exp.Alias(
                    this=exp.Pow(this=exp.Literal.number(2), expression=exp.Literal.number(10)),
                    alias=exp.to_identifier("POWER"),
                ),
                "POWER(2, 10) POWER",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="POSIX_TIME",
                                expressions=[exp.Literal.string("1970-01-01 00:00:01")],
                            ),
                            alias="PT1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="POSIX_TIME",
                                expressions=[exp.Literal.string("2009-02-13 23:31:30")],
                            ),
                            alias="PT2",
                        ),
                    ]
                ),
                "SELECT POSIX_TIME('1970-01-01 00:00:01') PT1, POSIX_TIME('2009-02-13 23:31:30') PT2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Rand(),
                            alias="RANDOM_1",
                        ),
                        exp.Alias(
                            this=exp.Rand(
                                expressions=[
                                    exp.Literal.number(5),
                                    exp.Literal.number(20),
                                ],
                            ),
                            alias="RANDOM_2",
                        ),
                    ]
                ),
                "SELECT RANDOM() RANDOM_1, RANDOM(5, 20) RANDOM_2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(this="RANK"),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.Column(this="current_salary"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("RANK"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "current_salary", RANK() OVER (PARTITION BY "department" ORDER BY current_salary) RANK FROM "employee_table" ORDER BY "department", "current_salary"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(
                                    this="RATIO_TO_REPORT",
                                    expressions=[exp.Column(this="current_salary")],
                                ),
                                partition_by=[exp.to_identifier("department")],
                            ),
                            alias=exp.to_identifier("RATIO_TO_REPORT"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "current_salary", RATIO_TO_REPORT(current_salary) OVER (PARTITION BY "department") RATIO_TO_REPORT FROM "employee_table" ORDER BY "department", "current_salary"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="REGEXP_INSTR",
                                expressions=[
                                    exp.Literal.string("Phone: +497003927877678"),
                                    exp.Literal.string(r"\+?\d+"),
                                ],
                            ),
                            alias="REGEXP_INSTR1",
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="REGEXP_INSTR",
                                expressions=[
                                    exp.Literal.string(
                                        "From: my_mail@yahoo.com - To: SERVICE@EXASOL.COM"
                                    ),
                                    exp.Literal.string(
                                        r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}"
                                    ),
                                    exp.Literal.number(1),
                                    exp.Literal.number(2),
                                ],
                            ),
                            alias="REGEXP_INSTR2",
                        ),
                    ]
                ),
                r"SELECT REGEXP_INSTR('Phone: +497003927877678', '\+?\d+') REGEXP_INSTR1, REGEXP_INSTR('From: my_mail@yahoo.com - To: SERVICE@EXASOL.COM', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}', 1, 2) REGEXP_INSTR2",
            ),
            (
                exp.Alias(
                    this=exp.RegexpExtract(
                        this=exp.Literal.string("My mail address is my_mail@yahoo.com"),
                        expression=exp.Literal.string(
                            "(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}"
                        ),
                    ),
                    alias=exp.to_identifier("EMAIL"),
                ),
                "REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}') EMAIL",
            ),
            (
                exp.Alias(
                    this=exp.RegexpReplace(
                        this=exp.Literal.string("From: my_mail@yahoo.com"),
                        expression=exp.Literal.string(
                            "(?i)^From: ([a-z0-9._%+-]+)@([a-z0-9.-]+\\.[a-z]{2,4}$)"
                        ),
                        replacement=exp.Literal.string("Name: \\1 - Domain: \\2"),
                    ),
                    alias=exp.to_identifier("REGEXP_REPLACE"),
                ),
                "REGEXP_REPLACE('From: my_mail@yahoo.com', '(?i)^From: ([a-z0-9._%+-]+)@([a-z0-9.-]+\\.[a-z]{2,4}$)', 'Name: \\1 - Domain: \\2') REGEXP_REPLACE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(
                                    this="REGR_SLOPE",
                                    expressions=[
                                        exp.to_identifier("starting_salary"),
                                        exp.to_identifier("current_salary"),
                                    ],
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("REGR_SLOPE"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", REGR_SLOPE("starting_salary", "current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") REGR_SLOPE FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Repeat(this=exp.Literal.string("abc"), times=exp.Literal.number(3))
                    ]
                ),
                "SELECT REPEAT('abc', 3)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="REPLACE",
                                expressions=[
                                    exp.Literal.string("Apple juice is great"),
                                    exp.Literal.string("Apple"),
                                    exp.Literal.string("Orange"),
                                ],
                            ),
                            alias=exp.to_identifier("REPLACE_1"),
                        )
                    ]
                ),
                "SELECT REPLACE('Apple juice is great', 'Apple', 'Orange') REPLACE_1",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Right(
                                this=exp.Literal.string("abcdef"),
                                expression=exp.Literal.number(3),
                            ),
                            alias="RIGHT_SUBSTR",
                        )
                    ]
                ),
                "SELECT RIGHT('abcdef', 3) RIGHT_SUBSTR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Right(
                                this=exp.Literal.string("abcdef"),
                                expression=exp.Literal.number(3),
                            ),
                            alias="RIGHT_SUBSTR",
                        )
                    ]
                ),
                "SELECT RIGHT('abcdef', 3) RIGHT_SUBSTR",
            ),
            (
                exp.Select(expressions=[exp.Round(this=exp.Literal.number(42.678))]),
                "SELECT ROUND(42.678)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Round(
                            this=exp.Literal.number(42.678),
                            decimals=exp.Literal.number(2),
                        )
                    ]
                ),
                "SELECT ROUND(42.678, 2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Round(
                            this=exp.Date(this=exp.Literal.string("2023-01-01")),
                            decimals=exp.Literal.string("YYYY"),
                        )
                    ]
                ),
                "SELECT ROUND(DATE '2023-01-01', 'YYYY')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Round(
                            this=exp.Date(this=exp.Literal.string("2023-01-01")),
                            decimals=exp.Literal.string("Q"),
                        )
                    ]
                ),
                "SELECT ROUND(DATE '2023-01-01', 'Q')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Round(
                            this=exp.Timestamp(this=exp.Literal.string("2023-01-01 10:00:00")),
                            decimals=exp.Literal.string("HH"),
                        )
                    ]
                ),
                "SELECT ROUND(TIMESTAMP '2023-01-01 10:00:00', 'HH')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="SALES_ID"),
                        exp.Anonymous(this="ROWNUM"),
                    ],
                )
                .from_(exp.Table(this="SALES"))
                .where(
                    exp.LT(
                        this=exp.Anonymous(this="ROWNUM"),
                        expression=exp.Literal.number(10),
                    )
                ),
                "SELECT SALES_ID, ROWNUM FROM SALES WHERE ROWNUM < 10",
            ),
            # (
            #     exp.select(
            #         "SALES_ID",
            #         exp.Anonymous(this="ROWNUM"),
            #     )
            #     .from_("SALES")
            #     .where(
            #         exp.LT(
            #             this=exp.Anonymous(this="ROWNUM"),
            #             expression=exp.Literal.number(10),
            #         )
            #     ),
            #     "SELECT SALES_ID, ROWNUM FROM SALES WHERE ROWNUM < 10",
            # ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(
                                    this="ROW_NUMBER",
                                    # expressions=[
                                    #     exp.Column(this="starting_salary"),
                                    #     exp.Column(this="current_salary"),
                                    # ],
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("ROW_NUMBER"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", ROW_NUMBER() OVER (PARTITION BY "department" ORDER BY "hire_date") ROW_NUMBER FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.select(
                    exp.Anonymous(this="ROWID"),
                    "i",
                ).from_("t"),
                'SELECT ROWID, "i" FROM "t"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Pad(
                            this=exp.Literal.string("abc"),
                            expression=exp.Literal.number(5),
                            fill_pattern=exp.Literal.string("X"),
                            is_left=False,
                        ),
                    ]
                ),
                "SELECT RPAD('abc', 5, 'X')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="RTRIM",
                            expressions=[
                                exp.Literal.string("abcdef"),
                                exp.Literal.string("afe"),
                            ],
                        )
                    ]
                ),
                "SELECT RTRIM('abcdef', 'afe')",
            ),
            (
                exp.Create(
                    this=exp.to_table("scope_view"),
                    kind="VIEW",
                    expression=exp.Select(expressions=[exp.Anonymous(this="SCOPE_USER")]),
                ),
                'CREATE VIEW "scope_view" AS SELECT SCOPE_USER',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="SECOND",
                            expressions=[
                                exp.Timestamp(this=exp.Literal.string("2010-10-20 11:59:40.123")),
                                exp.Literal.number(2),
                            ],
                        )
                    ]
                ),
                "SELECT SECOND(TIMESTAMP '2010-10-20 11:59:40.123', 2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="SECONDS_BETWEEN",
                                expressions=[
                                    exp.Timestamp(
                                        this=exp.Literal.string("2000-01-01 12:01:02.345")
                                    ),
                                    exp.Timestamp(this=exp.Literal.string("2000-01-01 12:00:00")),
                                ],
                            ),
                            alias=exp.to_identifier("SB"),
                        )
                    ]
                ),
                "SELECT SECONDS_BETWEEN(TIMESTAMP '2000-01-01 12:01:02.345', TIMESTAMP '2000-01-01 12:00:00') SB",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.SessionParameter(
                                this=exp.Identifier(this="current_session"),
                                kind=exp.Literal.string("NLS_FIRST_DAY_OF_WEEK"),
                            ),
                            alias="SESSION_VALUE",
                        )
                    ]
                ),
                "SELECT SESSION_PARAMETER(\"current_session\", 'NLS_FIRST_DAY_OF_WEEK') SESSION_VALUE",
            ),
            (
                exp.select(
                    exp.Anonymous(this="SESSIONTIMEZONE"),
                ),
                "SELECT SESSIONTIMEZONE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Sign(this=exp.Literal.number(-123)),
                    ]
                ),
                "SELECT SIGN(-123)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="SIN",
                            expressions=[
                                exp.Div(
                                    this=exp.Anonymous(this="PI"),
                                    expression=exp.Literal.number(3),
                                )
                            ],
                        )
                    ]
                ),
                "SELECT SIN(PI() / 3)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="SINH",
                                expressions=[
                                    exp.Literal.number(0),
                                ],
                            ),
                            alias="SINH",
                        )
                    ]
                ),
                "SELECT SINH(0) SINH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="SOUNDEX",
                            expressions=[
                                exp.Literal.string("smythe"),
                            ],
                        ),
                    ]
                ),
                "SELECT SOUNDEX('smythe')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="SPACE",
                            expressions=[
                                exp.Literal.number(5),
                            ],
                        ),
                    ]
                ),
                "SELECT SPACE(5)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Sqrt(
                            this=exp.Literal.number(2),
                        ),
                    ]
                ),
                "SELECT SQRT(2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Stddev(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("STDDEV"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", STDDEV("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") STDDEV FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.StddevPop(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("STDDEV_POP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", STDDEV_POP("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") STDDEV_POP FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.StddevSamp(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("STDDEV_SAMP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", STDDEV_SAMP("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") STDDEV_SAMP FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Substring(
                                this=exp.Literal.string("abcdef"),
                                start=exp.Literal.number(4),
                                length=exp.Literal.number(2),
                            ),
                            alias="S2",
                        )
                    ]
                ),
                "SELECT SUBSTR('abcdef', 4, 2) S2",
                # "SELECT SUBSTRING('abcdef' FROM 4 FOR 2) S2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Sum(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("SUM_SALARY"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", SUM("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") SUM_SALARY FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(expressions=[exp.Alias(this=exp.Anonymous(this="SYS_GUID"))]),
                "SELECT SYS_GUID()",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(this="SYSTIMESTAMP"),
                        exp.Anonymous(
                            this="SYSTIMESTAMP",
                            expressions=[exp.Literal.number(6)],
                        ),
                    ]
                ),
                "SELECT SYSTIMESTAMP, SYSTIMESTAMP(6)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="TAN",
                            expressions=[
                                exp.Div(
                                    this=exp.Anonymous(this="PI"),
                                    expression=exp.Literal.number(4),
                                )
                            ],
                        )
                    ]
                ),
                "SELECT TAN(PI() / 4)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(this="TANH", expressions=[exp.Literal.number(0)]),
                            alias="TANH",
                        )
                    ]
                ),
                "SELECT TANH(0) TANH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ToChar(
                                expressions=[
                                    exp.Date(this=exp.Literal.string("1999-12-31")),
                                ],
                            ),
                            alias="TO_CHAR",
                        )
                    ]
                ),
                "SELECT TO_CHAR(DATE '1999-12-31') TO_CHAR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ToChar(
                                expressions=[
                                    exp.Timestamp(this=exp.Literal.string("1999-12-31 23:59:00")),
                                    exp.Literal.string("HH24:MI:SS DD-MM-YYYY"),
                                ],
                            ),
                            alias="TO_CHAR",
                        )
                    ]
                ),
                "SELECT TO_CHAR(TIMESTAMP '1999-12-31 23:59:00', 'HH24:MI:SS DD-MM-YYYY') TO_CHAR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ToChar(
                                expressions=[
                                    exp.Date(this=exp.Literal.string("2013-12-16")),
                                    exp.Literal.string("DD. MON YYYY"),
                                    exp.Literal.string("NLS_DATE_LANGUAGE=DEU"),
                                ],
                            ),
                            alias="TO_CHAR",
                        )
                    ]
                ),
                "SELECT TO_CHAR(DATE '2013-12-16', 'DD. MON YYYY', 'NLS_DATE_LANGUAGE=DEU') TO_CHAR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ToChar(
                                expressions=[
                                    exp.Literal(this="-12345.67890", is_string=False),
                                    exp.Literal.string("000G000G000D000000MI"),
                                ],
                            ),
                            alias="TO_CHAR",
                        )
                    ]
                ),
                "SELECT TO_CHAR(-12345.67890, '000G000G000D000000MI') TO_CHAR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.StrToDate(
                                this=exp.Literal.string("1999-12-31"),
                            ),
                            alias="TO_DATE",
                        )
                    ]
                ),
                "SELECT TO_DATE('1999-12-31') TO_DATE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.StrToDate(
                                this=exp.Literal.string("31-12-1999"),
                            ),
                            alias=exp.to_identifier("TO_DATE"),
                        )
                    ]
                ),
                "SELECT TO_DATE('31-12-1999') TO_DATE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TO_DSINTERVAL",
                                expressions=[exp.Literal.string("3 10:59:59.123")],
                            ),
                            alias="TO_DSINTERVAL",
                        )
                    ]
                ),
                "SELECT TO_DSINTERVAL('3 10:59:59.123') TO_DSINTERVAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ToNumber(
                                expressions=[
                                    exp.Literal.string("+123"),
                                ],
                            ),
                            alias="TO_NUMBER1",
                        ),
                        exp.Alias(
                            this=exp.ToNumber(
                                expressions=[
                                    exp.Literal.string("-123.45"),
                                    exp.Literal.string("99999.999"),
                                ],
                            ),
                            alias="TO_NUMBER2",
                        ),
                    ]
                ),
                "SELECT TO_NUMBER('+123') TO_NUMBER1, TO_NUMBER('-123.45', '99999.999') TO_NUMBER2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.TsOrDsToTimestamp(
                                expressions=[exp.Literal.string("1999-12-31 23:59:00")],
                            ),
                            alias="TO_TIMESTAMP",
                        )
                    ]
                ),
                "SELECT TO_TIMESTAMP('1999-12-31 23:59:00') TO_TIMESTAMP",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.TsOrDsToTimestamp(
                                expressions=[
                                    exp.Literal.string("23:59:00 31-12-1999"),
                                    exp.Literal.string("HH24:MI:SS DD-MM-YYYY"),
                                ],
                            ),
                            alias="TO_TIMESTAMP",
                        )
                    ]
                ),
                "SELECT TO_TIMESTAMP('23:59:00 31-12-1999', 'HH24:MI:SS DD-MM-YYYY') TO_TIMESTAMP",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TO_YMINTERVAL",
                                expressions=[exp.Literal.string("3-11")],
                            ),
                            alias="TO_YMINTERVAL",
                        )
                    ]
                ),
                "SELECT TO_YMINTERVAL('3-11') TO_YMINTERVAL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TRANSLATE",
                                expressions=[
                                    exp.Literal.string("abcd"),
                                    exp.Literal.string("abc"),
                                    exp.Literal.string("xy"),
                                ],
                            ),
                            alias="TRANSLATE",
                        )
                    ]
                ),
                "SELECT TRANSLATE('abcd', 'abc', 'xy') TRANSLATE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Trim(
                                this=exp.Literal.string("abcdef"),
                                expression=exp.Literal.string("acf"),
                                position=False,
                            ),
                            alias="TRIM",
                        )
                    ]
                ),
                "SELECT TRIM('acf' FROM 'abcdef') TRIM",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Trim(
                            this=exp.Literal.string("1234567891"),
                            expression=exp.Literal.string("1"),
                            position="LEADING",
                        )
                    ]
                ),
                "SELECT TRIM(LEADING '1' FROM '1234567891')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Trim(
                            this=exp.Literal.string("1234567891"),
                            expression=exp.Literal.string("1"),
                            position="TRAILING",
                        )
                    ]
                ),
                "SELECT TRIM(TRAILING '1' FROM '1234567891')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Trim(
                            this=exp.Literal.string("1234567891"),
                            expression=exp.Literal.string("1"),
                            position="BOTH",
                        )
                    ]
                ),
                "SELECT TRIM(BOTH '1' FROM '1234567891')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TRUNC",
                                expressions=[
                                    exp.Date(this=exp.Literal.string("2006-12-31")),
                                    exp.Literal.string("MM"),
                                ],
                            ),
                            alias=exp.to_identifier("TRUNC"),
                        )
                    ]
                ),
                "SELECT TRUNC(DATE '2006-12-31', 'MM') TRUNC",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TRUNC",
                                expressions=[
                                    exp.Timestamp(this=exp.Literal.string("2006-12-31 23:59:59")),
                                    exp.Literal.string("MI"),
                                ],
                            ),
                            alias=exp.to_identifier("TRUNC"),
                        )
                    ]
                ),
                "SELECT TRUNC(TIMESTAMP '2006-12-31 23:59:59', 'MI') TRUNC",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="TRUNC",
                                expressions=[
                                    exp.Literal(
                                        this="123.456",
                                        is_string=False,
                                    ),
                                    exp.Literal.number(2),
                                ],
                            ),
                            alias=exp.to_identifier("TRUNC"),
                        )
                    ]
                ),
                "SELECT TRUNC(123.456, 2) TRUNC",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="TYPEOF",
                            expressions=[
                                exp.Mul(
                                    this=exp.Literal.number(1),
                                    expression=exp.Literal.number(0.1),
                                )
                            ],
                        ),
                        exp.Anonymous(
                            this="TYPEOF",
                            expressions=[
                                exp.Mul(
                                    this=exp.Literal.number(0.1),
                                    expression=exp.Literal.number(1),
                                )
                            ],
                        ),
                    ]
                ),
                "SELECT TYPEOF(1 * 0.1), TYPEOF(0.1 * 1)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="UCASE",
                                expressions=[
                                    exp.Literal.string("AbCdEf"),
                                ],
                            ),
                            alias="UCASE",
                        )
                    ]
                ),
                "SELECT UCASE('AbCdEf') UCASE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Unicode(this=exp.Literal.string("ä")),
                            alias="UNICODE",
                        )
                    ]
                ),
                "SELECT UNICODE('ä') UNICODE",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="UNICODECHR",
                                expressions=[
                                    exp.Literal.number(252),
                                ],
                            ),
                            alias="UNICODECHR",
                        )
                    ]
                ),
                "SELECT UNICODECHR(252) UNICODECHR",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Upper(
                                this=exp.Literal.string("AbCdEf"),
                            ),
                            alias="UPPER",
                        )
                    ]
                ),
                "SELECT UPPER('AbCdEf') UPPER",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(this="IPROC"),
                        exp.Identifier(this="c1"),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="VALUE2PROC",
                                expressions=[
                                    exp.to_identifier("c1"),
                                ],
                            ),
                            alias="V2P_1",
                        ),
                        exp.Identifier(this="c2"),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="VALUE2PROC",
                                expressions=[
                                    exp.to_identifier("c2"),
                                ],
                            ),
                            alias="V2P_2",
                        ),
                    ]
                ).from_("t"),
                'SELECT IPROC(), "c1", VALUE2PROC("c1") V2P_1, "c2", VALUE2PROC("c2") V2P_2 FROM "t"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.VariancePop(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("VAR_POP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", VAR_POP("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") VAR_POP FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Variance(
                                    this=exp.to_identifier("current_salary"),
                                    sql_name="VAR_SAMP",
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("VAR_SAMP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", VAR_SAMP("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") VAR_SAMP FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.to_identifier("id"),
                        exp.to_identifier("department"),
                        exp.to_identifier("hire_date"),
                        exp.to_identifier("current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Variance(
                                    this=exp.to_identifier("current_salary"),
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[exp.Ordered(this=exp.to_identifier("hire_date"))]
                                ),
                            ),
                            alias=exp.to_identifier("VARIANCE"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                'SELECT "id", "department", "hire_date", "current_salary", VARIANCE("current_salary") OVER (PARTITION BY "department" ORDER BY "hire_date") VARIANCE FROM "employee_table" ORDER BY "department", "hire_date"',
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Week(this=exp.Date(this=exp.Literal.string("2012-01-05"))),
                            alias=exp.to_identifier("WEEK"),
                        )
                    ]
                ),
                "SELECT WEEK(DATE '2012-01-05') WEEK",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("-0.1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E1"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E2"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0.49"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E3"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0.5"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E4"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0.9"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E5"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E6"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("1.1"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E7"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("1"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E8"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0.5"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E9"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0.1"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E10"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E11"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="WIDTH_BUCKET",
                                expressions=[
                                    exp.Null(),
                                    exp.Literal.number("0"),
                                    exp.Literal.number("1"),
                                    exp.Literal.number("2"),
                                ],
                            ),
                            alias=exp.to_identifier("E12"),
                        ),
                    ]
                ),
                "SELECT "
                "WIDTH_BUCKET(-0.1, 0, 1, 2) E1, "
                "WIDTH_BUCKET(0, 0, 1, 2) E2, "
                "WIDTH_BUCKET(0.49, 0, 1, 2) E3, "
                "WIDTH_BUCKET(0.5, 0, 1, 2) E4, "
                "WIDTH_BUCKET(0.9, 0, 1, 2) E5, "
                "WIDTH_BUCKET(1, 0, 1, 2) E6, "
                "WIDTH_BUCKET(1.1, 1, 0, 2) E7, "
                "WIDTH_BUCKET(1, 1, 0, 2) E8, "
                "WIDTH_BUCKET(0.5, 1, 0, 2) E9, "
                "WIDTH_BUCKET(0.1, 1, 0, 2) E10, "
                "WIDTH_BUCKET(0, 1, 0, 2) E11, "
                "WIDTH_BUCKET(NULL, 0, 1, 2) E12",
            ),
            (
                exp.Select(
                    expressions=[exp.Year(this=exp.Date(this=exp.Literal.string("2010-10-20")))]
                ),
                "SELECT YEAR(DATE '2010-10-20')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="YEARS_BETWEEN",
                                expressions=[
                                    exp.Date(this=exp.Literal.string("2001-01-01")),
                                    exp.Date(this=exp.Literal.string("2000-06-15")),
                                ],
                            ),
                            alias=exp.to_identifier("YB1"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="YEARS_BETWEEN",
                                expressions=[
                                    exp.Timestamp(this=exp.Literal.string("2001-01-01 12:00:00")),
                                    exp.Timestamp(this=exp.Literal.string("2000-01-01 00:00:00")),
                                ],
                            ),
                            alias=exp.to_identifier("YB2"),
                        ),
                    ]
                ),
                "SELECT YEARS_BETWEEN(DATE '2001-01-01', DATE '2000-06-15') YB1, "
                "YEARS_BETWEEN(TIMESTAMP '2001-01-01 12:00:00', TIMESTAMP '2000-01-01 00:00:00') YB2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="ZEROIFNULL",
                                expressions=[exp.Null()],
                            ),
                            alias=exp.to_identifier("ZIN1"),
                        ),
                        exp.Alias(
                            this=exp.Anonymous(
                                this="ZEROIFNULL",
                                expressions=[exp.Literal.number("1")],
                            ),
                            alias=exp.to_identifier("ZIN2"),
                        ),
                    ]
                ),
                "SELECT ZEROIFNULL(NULL) ZIN1, ZEROIFNULL(1) ZIN2",
            ),
        ]

        for expression, expected_sql in tests:
            with self.subTest(sql=expected_sql):
                # print(generator.sql(expression))
                # print(expected_sql)
                self.assertEqual(generator.sql(expression), expected_sql)
                self.assertEqual(generator.sql(expression), expected_sql)
