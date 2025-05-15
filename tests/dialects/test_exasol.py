from datetime import date, datetime, timezone
from sqlglot import exp, parse_one
from sqlglot.dialects.exasol import Exasol, ExasolTokenType
from sqlglot.expressions import convert
from sqlglot.optimizer import traverse_scope
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.tokens import TokenType
from tests.dialects.test_dialect import Validator
from sqlglot.errors import ErrorLevel


class TestExasol(Validator):
    dialect = "exasol"

    def test_integration_cross(self):
        pass

    def test_integration_identity(self):
        ########## STRING FUNCTIONS ###########
        self.validate_identity("SELECT ASCII('X')")
        self.validate_identity("SELECT BIT_LENGTH('aou') BIT_LENGTH")
        # currently CHARACTER_LENGTH allows more than 1 arg (3)
        self.validate_identity(
            "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
            "SELECT LENGTH('aeiouäöü') C_LENGTH",
        )
        self.validate_identity("SELECT CHAR(88) CHR", "SELECT CHR(88) CHR")
        self.validate_identity(
            "SELECT COLOGNE_PHONETIC('schmitt'), COLOGNE_PHONETIC('Schmidt')"
        )
        self.validate_identity("SELECT CONCAT('abc', 'def', 'g') CONCAT")

        dumps = [
            "SELECT DUMP('123abc') DUMP",
            "SELECT DUMP('üäö45', 16) DUMP",
            "SELECT DUMP('üäö45', 16, 1) DUMP",
            "SELECT DUMP('üäö45', 16, 1, 3) DUMP",
        ]
        for d in dumps:
            self.validate_identity(d)

        self.validate_identity("SELECT EDIT_DISTANCE('schmitt', 'Schmidt')")
        # INITCAP currently allows more than 1 argument (2)
        self.validate_identity("SELECT INITCAP('ExAsOl is great', 'tre T') INITCAP")
        self.validate_identity(
            "SELECT INSERT('abc', 2, 2, 'xxx'), INSERT('abcdef', 3, 2, 'CD')"
        )

        instrs = [
            (
                "SELECT INSTR('abcabcabc', 'cab') INSTR1",
                "SELECT POSITION('cab' IN 'abcabcabc') INSTR1",
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
        self.validate_identity("SELECT REVERSE('abcde') REVERSE")
        self.validate_identity("SELECT RIGHT('abcdef', 3) RIGHT_SUBSTR")
        self.validate_identity("SELECT RPAD('abc', 5, 'X')")
        self.validate_identity("SELECT LPAD('abc', 5, 'X')")
        self.validate_identity(
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTRING('abcdef' FROM 4 FOR 2) S2, SUBSTRING('abcdef' FROM -3) S3, SUBSTR('abcdef', 7) S4",
            "SELECT SUBSTR('abcdef', 2, 3) S1, SUBSTR('abcdef', 4, 2) S2, SUBSTR('abcdef', -3) S3, SUBSTR('abcdef', 7) S4",
        )
        self.validate_identity("SELECT TO_CHAR(DATE '2013-12-16', 'DD. MON YYYY', 'NLS_DATE_LANGUAGE=DEU') TO_CHAR")
        # , SUBSTRING('abcdef FROM -3) S3
        ########## STRING FUNCTIONS ###########

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
    #     self.validate_identity(
    #         "CONVERT_TZ(TIMESTAMP '2012-05-10 12:00:00', 'UTC', 'Europe/Berlin')",
    #         exp.ConvertTZ(
    #             this=exp.Literal.string("TIMESTAMP '2012-05-10 12:00:00'"),
    #             from_tz=exp.Literal.string("UTC"),
    #             to_tz=exp.Literal.string("Europe/Berlin")
    #         )
    #     )

    #     self.validate_identity(
    #         "CONVERT_TZ(TIMESTAMP '2012-03-25 02:30:00', 'Europe/Berlin', 'UTC', 'INVALID REJECT AMBIGUOUS REJECT')",
    #         exp.ConvertTZ(
    #             this=exp.Literal.string("TIMESTAMP '2012-03-25 02:30:00'"),
    #             from_tz=exp.Literal.string("Europe/Berlin"),
    #             to_tz=exp.Literal.string("UTC"),
    #             options=exp.Literal.string("INVALID REJECT AMBIGUOUS REJECT")
    #         )
    #     )
    def test_generator_transforms(self):
        generator = Exasol.Generator()
        generator.dialect.NULL_ORDERING = None
        tests = [
            (exp.Abs(this=exp.Column(this="x")), "ABS(x)"),
            (exp.Acos(this=exp.Column(this="x")), "ACOS(x)"),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Anonymous(this="DATE '2000-02-28'"),
                        expression=exp.Literal.number(1),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_DAYS(DATE '2000-02-28', 1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.DateAdd(
                        this=exp.Anonymous(this="TIMESTAMP '2001-02-28 12:00:00'"),
                        expression=exp.Literal.number(1),
                    ),
                    alias=exp.to_identifier("AD2"),
                ),
                "ADD_DAYS(TIMESTAMP '2001-02-28 12:00:00', 1) AD2",
            ),
            (
                exp.Alias(
                    this=exp.AddHours(
                        this=exp.Literal(
                            this="2001-02-28 12:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(-1),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_HOURS(TIMESTAMP '2001-02-28 12:00:00', -1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.AddMinutes(
                        this=exp.Literal(
                            this="2001-02-28 12:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(-1),
                    ),
                    alias=exp.to_identifier("AD1"),
                ),
                "ADD_MINUTES(TIMESTAMP '2001-02-28 12:00:00', -1) AD1",
            ),
            (
                exp.Alias(
                    this=exp.AddMonths(
                        this=exp.Literal(
                            this="2001-02-28 12:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(2),
                    ),
                    alias=exp.to_identifier("AM1"),
                ),
                "ADD_MONTHS(TIMESTAMP '2001-02-28 12:00:00', 2) AM1",
            ),
            (
                exp.Alias(
                    this=exp.AddMonths(
                        this=exp.Literal(
                            this="2001-02-28", prefix="DATE", is_string=True
                        ),
                        expression=exp.Literal.number(2),
                    ),
                    alias=exp.to_identifier("AM2"),
                ),
                "ADD_MONTHS(DATE '2001-02-28', 2) AM2",
            ),
            (
                exp.Alias(
                    this=exp.AddSeconds(
                        this=exp.Literal(
                            this="2000-01-01 00:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(1.234),
                    ),
                    alias=exp.to_identifier("AS2"),
                ),
                "ADD_SECONDS(TIMESTAMP '2000-01-01 00:00:00', 1.234) AS2",
            ),
            (
                exp.Alias(
                    this=exp.AddWeeks(
                        this=exp.Literal(
                            this="2000-01-01 00:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(-1),
                    ),
                    alias=exp.to_identifier("AW1"),
                ),
                "ADD_WEEKS(TIMESTAMP '2000-01-01 00:00:00', -1) AW1",
            ),
            (
                exp.Alias(
                    this=exp.AddYears(
                        this=exp.Literal(
                            this="2000-01-01 00:00:00",
                            prefix="TIMESTAMP",
                            is_string=True,
                        ),
                        expression=exp.Literal.number(1),
                    ),
                    alias=exp.to_identifier("AY1"),
                ),
                "ADD_YEARS(TIMESTAMP '2000-01-01 00:00:00', 1) AY1",
            ),
            (
                exp.Alias(
                    this=exp.AnyValue(
                        this=exp.LT(
                            this=exp.Column(this="age"),
                            expression=exp.Literal.number(30),
                        ),
                        over=exp.Window(
                            partition_by=[exp.Column(this="department")],
                            order=exp.Order(
                                expressions=[
                                    exp.Ordered(
                                        this=exp.Column(
                                            this="age", desc=False, nulls_first=None
                                        )
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
            (exp.Ascii(this=exp.Column(this="x")), "ASCII(x)"),
            (exp.Asin(this=exp.Column(this="x")), "ASIN(x)"),
            (exp.Atan(this=exp.Column(this="x")), "ATAN(x)"),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Atan2(
                                this=exp.Literal.number(1),
                                expression=exp.Literal.number(1),
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
                                expressions=[
                                    exp.Ordered(this=exp.Column(this="hire_date"))
                                ]
                            ),
                        ),
                    ),
                    alias=exp.to_identifier("AVG"),
                ),
                "AVG(starting_salary) OVER (PARTITION BY department ORDER BY hire_date) AVG",
            ),
            (
                exp.BitwiseAnd(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
                "BIT_AND(a, b)",
            ),
            (
                exp.BitCheck(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
                "BIT_CHECK(a, b)",
            ),
            (
                exp.Alias(
                    this=exp.BitLength(this=exp.Literal.string("äöü")),
                    alias=exp.to_identifier("BIT_LENGTH"),
                ),
                "BIT_LENGTH('äöü') BIT_LENGTH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.BitLRotate(
                            this=exp.Column(this="1024"),
                            expression=exp.Column(this="63"),
                        )
                    ]
                ),
                "SELECT BIT_LROTATE(1024, 63)",
            ),
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
            (
                exp.Select(
                    expressions=[
                        exp.BitSet(
                            this=exp.Column(this="1"), expression=exp.Column(this="10")
                        )
                    ]
                ),
                "SELECT BIT_SET(1, 10)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.BitToNum(
                            expressions=[
                                exp.Literal.number(1),
                                exp.Literal.number(1),
                                exp.Literal.number(0),
                                exp.Literal.number(0),
                            ]
                        )
                    ]
                ),
                "SELECT BIT_TO_NUM(1, 1, 0, 0)",
            ),
            (
                exp.BitwiseOr(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
                "BIT_OR(a, b)",
            ),
            (exp.BitwiseNot(this=exp.Column(this="a")), "BIT_NOT(a)"),
            (
                exp.BitwiseXor(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
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
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.CharacterLength(
                                this=exp.Literal.string("aeiouäöü")
                            ),
                            alias=exp.to_identifier("C_LENGTH"),
                        )
                    ]
                ),
                "SELECT CHARACTER_LENGTH('aeiouäöü') C_LENGTH",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.ColognePhonetic(this=exp.Literal.string("schmitt")),
                        exp.ColognePhonetic(this=exp.Literal.string("Schmidt")),
                    ]
                ),
                "SELECT COLOGNE_PHONETIC('schmitt'), COLOGNE_PHONETIC('Schmidt')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.ConnectByIsCycle(),
                        exp.Alias(
                            this=exp.SysConnectByPath(
                                this=exp.Column(this="last_name"),
                                expressions=[exp.Literal.string("/")],
                            ),
                            alias=exp.to_identifier("PATH"),
                        ),
                    ],
                )
                .from_("employees")
                .where("last_name='clark'"),
                "SELECT CONNECT_BY_ISCYCLE, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.ConnectByIsLeaf(),
                        exp.Alias(
                            this=exp.SysConnectByPath(
                                this=exp.Column(this="last_name"),
                                expressions=[exp.Literal.string("/")],
                            ),
                            alias=exp.to_identifier("PATH"),
                        ),
                    ],
                )
                .from_("employees")
                .where("last_name='clark'"),
                "SELECT CONNECT_BY_ISLEAF, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees WHERE last_name = 'clark'",
            ),
            (
                exp.Convert(
                    this=exp.Var(this="CHAR(15)"), expression=exp.Literal.string("ABC")
                ),
                "CONVERT(CHAR(15), 'ABC')",
            ),
            (
                exp.Convert(
                    this=exp.Var(this="DATE"),
                    expression=exp.Literal.string("2006-01-01"),
                ),
                "CONVERT(DATE, '2006-01-01')",
            ),
            (
                exp.ConvertTZ(
                    this=exp.Var(this="TIMESTAMP '2012-05-10 12:00:00'"),
                    from_tz=exp.Literal.string("UTC"),
                    to_tz=exp.Literal.string("Europe/Berlin"),
                ),
                "CONVERT_TZ(TIMESTAMP '2012-05-10 12:00:00', 'UTC', 'Europe/Berlin')",
            ),
            (
                exp.ConvertTZ(
                    this=exp.Var(this="TIMESTAMP '2012-03-25 02:30:00'"),
                    from_tz=exp.Literal.string("Europe/Berlin"),
                    to_tz=exp.Literal.string("UTC"),
                    options=exp.Literal.string("INVALID REJECT AMBIGUOUS REJECT"),
                ),
                "CONVERT_TZ(TIMESTAMP '2012-03-25 02:30:00', 'Europe/Berlin', 'UTC', 'INVALID REJECT AMBIGUOUS REJECT')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="department"),
                        exp.Alias(
                            this=exp.Corr(
                                this=exp.Column(this="age"),
                                expression=exp.Column(this="current_salary"),
                            ),
                            alias=exp.to_identifier("CORR"),
                        ),
                    ],
                )
                .from_("employee_table")
                .group_by("department"),
                "SELECT department, CORR(age, current_salary) CORR FROM employee_table GROUP BY department",
            ),
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
                                    expressions=[
                                        exp.Ordered(this=exp.Column(this="age"))
                                    ]
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
                "SELECT id, department, COUNT(*) OVER (PARTITION BY department ORDER BY age) COUNT FROM employee_table ORDER BY department, age",
            ),
            (
                exp.CovarPop(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
                "COVAR_POP(a, b)",
            ),
            (
                exp.CovarSamp(
                    this=exp.Column(this="a"), expression=exp.Column(this="b")
                ),
                "COVAR_SAMP(a, b)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="id"),
                        exp.Column(this="department"),
                        exp.Column(this="age"),
                        exp.Column(this="current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.CovarSamp(
                                    this=exp.Column(this="age"),
                                    expression=exp.Column(this="current_salary"),
                                ),
                                partition_by=[exp.Column(this="department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.Column(this="age"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("COVAR_SAMP"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.Column(this="department")),
                            exp.Ordered(this=exp.Column(this="age")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, age, current_salary, COVAR_SAMP(age, current_salary) OVER (PARTITION BY department ORDER BY age) COVAR_SAMP FROM employee_table ORDER BY department, age",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Cos(
                            this=exp.Div(
                                this=exp.Anonymous(this="PI"),
                                expression=exp.Literal.number(3),
                            )
                        )
                    ]
                ),
                "SELECT COS(PI() / 3)",
            ),
            (
                exp.Select(expressions=[exp.CosH(this=exp.Column(this="1"))]),
                "SELECT COSH(1)",
            ),
            (
                exp.Select(expressions=[exp.Cot(this=exp.Column(this="1"))]),
                "SELECT COT(1)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="id"),
                        exp.Column(this="department"),
                        exp.Column(this="current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.Anonymous(this="CUME_DIST"),
                                partition_by=[exp.Column(this="department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(
                                            this=exp.Column(this="current_salary")
                                        )
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("CUME_DIST"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.Column(this="department")),
                            exp.Ordered(this=exp.Column(this="current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, current_salary, CUME_DIST() OVER (PARTITION BY department ORDER BY current_salary) CUME_DIST FROM employee_table ORDER BY department, current_salary",
            ),
            (exp.Select(expressions=[exp.CurrentCluster()]), "SELECT CURRENT_CLUSTER"),
            (exp.CurrentDate(), "CURDATE()"),
            (exp.CurrentSchema(), "CURRENT_SCHEMA"),
            (exp.Select(expressions=[exp.CurrentSession()]), "SELECT CURRENT_SESSION"),
            (
                exp.Select(expressions=[exp.CurrentStatement()]),
                "SELECT CURRENT_STATEMENT",
            ),
            (exp.Select(expressions=[exp.CurrentUser()]), "SELECT CURRENT_USER"),
            (exp.CurrentTimestamp(), "CURRENT_TIMESTAMP"),
            (exp.CurrentTimestamp(this=exp.Literal.number(6)), "CURRENT_TIMESTAMP(6)"),
            (
                exp.DateTrunc(
                    this=exp.Literal.string("minute"),
                    expression=exp.Literal(
                        this="2006-12-31 23:59:59", is_string=True, prefix="TIMESTAMP"
                    ),
                ),
                "DATE_TRUNC('minute', TIMESTAMP '2006-12-31 23:59:59')",
            ),
            (
                exp.DateTrunc(
                    this=exp.Literal.string("month"),
                    expression=exp.Literal(
                        this="2006-12-31", is_string=True, prefix="DATE"
                    ),
                ),
                "DATE_TRUNC('month', DATE '2006-12-31')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.DaysBetween(
                                this=exp.Date(this=exp.Literal.string("1999-12-31")),
                                expression=exp.Date(
                                    this=exp.Literal.string("2000-01-01")
                                ),
                            ),
                            alias=exp.to_identifier("DB1"),
                        ),
                        exp.Alias(
                            this=exp.DaysBetween(
                                this=exp.Timestamp(
                                    this=exp.Literal.string("2000-01-01 12:00:00")
                                ),
                                expression=exp.Timestamp(
                                    this=exp.Literal.string("1999-12-31 00:00:00")
                                ),
                            ),
                            alias=exp.to_identifier("DB2"),
                        ),
                    ]
                ),
                "SELECT DAYS_BETWEEN(DATE '1999-12-31', DATE '2000-01-01') DB1, DAYS_BETWEEN(TIMESTAMP '2000-01-01 12:00:00', TIMESTAMP '1999-12-31 00:00:00') DB2",
            ),
            (exp.Select(expressions=[exp.DBTimezone()]), "SELECT DBTIMEZONE"),
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
            (
                exp.Select(expressions=[exp.Degrees(this=exp.Anonymous(this="PI"))]),
                "SELECT DEGREES(PI())",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="id"),
                        exp.Column(this="department"),
                        exp.Column(this="current_salary"),
                        exp.Alias(
                            this=exp.Window(
                                this=exp.DenseRank(),
                                partition_by=[exp.Column(this="department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(
                                            this=exp.Column(this="current_salary")
                                        )
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("DENSE_RANK"),
                        ),
                    ],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.Column(this="department")),
                            exp.Ordered(this=exp.Column(this="current_salary")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, current_salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY current_salary) DENSE_RANK FROM employee_table ORDER BY department, current_salary",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Dump(
                                this=exp.Literal.string("üäö45"),
                                format=exp.Literal.number(1010),
                                start_position=exp.Literal.number(1),
                                length=exp.Literal.number(3),
                            ),
                            alias=exp.to_identifier("D"),
                        )
                    ]
                ),
                "SELECT DUMP('üäö45', 1010, 1, 3) D",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Dump(
                                this=exp.Literal.string("üäö45"),
                                format=exp.Literal.number(1010),
                                start_position=exp.Literal.number(1),
                                length=exp.Literal.number(3),
                            ),
                            alias=exp.to_identifier("D"),
                        )
                    ]
                ),
                "SELECT DUMP('üäö45', 1010, 1, 3) D",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Column(this="department"),
                        exp.Alias(
                            this=exp.Every(this=exp.Column(this="age >= 30")),
                            alias=exp.to_identifier("EVERY"),
                        ),
                    ]
                )
                .from_("employee_table")
                .group_by("department"),
                "SELECT department, EVERY(age >= 30) EVERY FROM employee_table GROUP BY department",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Extract(
                                this=exp.Literal(
                                    this="2000-10-01 12:22:59.123",
                                    is_string=True,
                                    prefix="TIMESTAMP",
                                ),
                                expression=exp.Var(this="SECOND"),
                            ),
                            alias=exp.to_identifier("EXS"),
                        ),
                        exp.Alias(
                            this=exp.Extract(
                                this=exp.Literal(
                                    this="2000-10-01", is_string=True, prefix="DATE"
                                ),
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
                                        expressions=[
                                            exp.Ordered(this=exp.Column(this="x"))
                                        ]
                                    ),
                                ),
                            ),
                            alias=exp.to_identifier("fv"),
                        )
                    ]
                ),
                "SELECT FIRST_VALUE(x) OVER (PARTITION BY department ORDER BY x) fv",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.FromPosixTime(this=exp.Column(this="x")),
                            alias=exp.to_identifier("FPT1"),
                        )
                    ]
                ),
                "SELECT FROM_POSIX_TIME(x) FPT1",
            ),
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
                exp.Select(
                    expressions=[exp.Grouping(expressions=[exp.Column(this="region")])]
                ),
                "SELECT GROUPING(region)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Grouping(
                            expressions=[exp.Column(this="y"), exp.Column(this="m")]
                        )
                    ]
                ),
                "SELECT GROUPING(y, m)",
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
                        exp.MD5(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
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
                        exp.SHA(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASH_SHA1(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[exp.HashSha256(expressions=[exp.Column(this="abc")])]
                ),
                "SELECT HASH_SHA256(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashSha256(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASH_SHA256(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[exp.HashSha512(expressions=[exp.Column(this="abc")])]
                ),
                "SELECT HASH_SHA512(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashSha512(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASH_SHA512(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[exp.HashTiger(expressions=[exp.Column(this="abc")])]
                ),
                "SELECT HASH_TIGER(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTiger(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASH_TIGER(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[exp.HashTypeMd5(expressions=[exp.Column(this="abc")])]
                ),
                "SELECT HASHTYPE_MD5(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeMd5(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASHTYPE_MD5(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[exp.HashTypeSha1(expressions=[exp.Column(this="abc")])]
                ),
                "SELECT HASHTYPE_SHA1(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeSha1(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASHTYPE_SHA1(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeSha256(expressions=[exp.Column(this="abc")])
                    ]
                ),
                "SELECT HASHTYPE_SHA256(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeSha256(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASHTYPE_SHA256(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeSha512(expressions=[exp.Column(this="abc")])
                    ]
                ),
                "SELECT HASHTYPE_SHA512(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeSha512(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASHTYPE_SHA512(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeTiger(expressions=[exp.Column(this="abc")])
                    ]
                ),
                "SELECT HASHTYPE_TIGER(abc)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.HashTypeTiger(
                            expressions=[exp.Column(this="c1"), exp.Column(this="c2")]
                        )
                    ]
                ),
                "SELECT HASHTYPE_TIGER(c1, c2)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Hour(
                            this=exp.Literal(
                                this="2001-02-28 12:00:00",
                                prefix="TIMESTAMP",
                                is_string=True,
                            ),
                        ),
                    ]
                ),
                "SELECT HOUR(TIMESTAMP '2001-02-28 12:00:00')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.HoursBetween(
                                this=exp.Literal(
                                    this="2000-01-01 12:00:00",
                                    prefix="TIMESTAMP",
                                    is_string=True,
                                ),
                                expression=exp.Literal(
                                    this="2000-01-01 11:01:05",
                                    prefix="TIMESTAMP",
                                    is_string=True,
                                ),
                            ),
                            alias=exp.to_identifier("HB"),
                        )
                    ]
                ),
                "SELECT HOURS_BETWEEN(TIMESTAMP '2000-01-01 12:00:00', TIMESTAMP '2000-01-01 11:01:05') HB",
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
                "SELECT name, age, IF age < 18 THEN 'underaged' ELSE 'adult' ENDIF LEGALITY FROM persons",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.InitCap(
                                this=exp.Literal.string("ExAsOl is great")
                            ),
                            alias=exp.to_identifier("INITCAP"),
                        )
                    ]
                ),
                "SELECT INITCAP('ExAsOl is great') INITCAP",
            ),
            (
                exp.Insert(
                    this=exp.Literal.string("abc"),
                    start=exp.Literal.number(2),
                    length=exp.Literal.number(2),
                    expression=exp.Literal.string("xxx"),
                ),
                "INSERT('abc', 2, 2, 'xxx')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Instr(
                                this=exp.Literal.string("abcabcabc"),
                                substr=exp.Literal.string("cab"),
                            ),
                            alias="INSTR1",
                        ),
                        exp.Alias(
                            this=exp.Instr(
                                this=exp.Literal.string(
                                    "user1,user2,user3,user4,user5"
                                ),
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
                    order=exp.Order(
                        expressions=[exp.Ordered(this=exp.Identifier(this="c1"))]
                    ),
                ).from_("t"),
                "SELECT c1, IPROC() IPROC FROM t ORDER BY c1",
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
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("LAG"),
                        ),
                    ],
                    # from_=exp.from_("employee_table"),
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, hire_date, LAG(id, 1) OVER (PARTITION BY department ORDER BY hire_date) LAG FROM employee_table ORDER BY department, hire_date",
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
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("LAST_"),
                        ),
                    ],
                    # from_=exp.from_("employee_table"),
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, hire_date, LAST_VALUE(id) OVER (PARTITION BY department ORDER BY hire_date) LAST_ FROM employee_table ORDER BY department, hire_date",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.LCase(this=exp.Literal.string("AbCdEf")),
                            alias=exp.to_identifier("LCASE"),
                        )
                    ]
                ),
                "SELECT LCASE('AbCdEf') LCASE",
            ),
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
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("LEAD"),
                        ),
                    ],
                    # from_=exp.from_("employee_table"),
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, hire_date, LEAD(id, 1) OVER (PARTITION BY department ORDER BY hire_date) LEAD FROM employee_table ORDER BY department, hire_date",
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
                exp.Command(
                    expressions=[
                        exp.Select(
                            expressions=[
                                exp.column("last_name"),
                                exp.Level(),
                                exp.Alias(
                                    this=exp.SysConnectByPath(
                                        this=exp.column("last_name"),
                                        expressions=[exp.Literal.string("/")],
                                    ),
                                    alias=exp.to_identifier("PATH"),
                                ),
                            ]
                        ).from_("employees"),
                        exp.ConnectBy(
                            this=exp.EQ(
                                this=exp.column("employee_id"),
                                expression=exp.column("manager_id"),
                            ),
                            prior=True,
                        ),
                        exp.StartWith(
                            this=exp.EQ(
                                this=exp.column("last_name"),
                                expression=exp.Literal.string("Clark"),
                            )
                        ),
                    ]
                ),
                "SELECT last_name, LEVEL, SYS_CONNECT_BY_PATH(last_name, '/') PATH FROM employees CONNECT BY PRIOR employee_id = manager_id START WITH last_name = 'Clark'",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.ListAgg(
                                this=exp.Column(this="name"),
                                separator=exp.Literal.string(", "),
                                order=exp.Column(this="name"),
                            ),
                            alias=exp.to_identifier("names_list"),
                        )
                    ]
                ),
                "SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) names_list",
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
                exp.Select(
                    expressions=[
                        exp.LocalTimestamp(),
                        exp.LocalTimestamp(precision=exp.Literal.number(6)),
                    ]
                ),
                "SELECT LOCALTIMESTAMP, LOCALTIMESTAMP(6)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Locate(
                                this=exp.Literal.string("cab"),
                                expression=exp.Literal.string("abcabcabc"),
                            ),
                            alias=exp.to_identifier("LOCATE1"),
                        ),
                        exp.Alias(
                            this=exp.Locate(
                                this=exp.Literal.string("user"),
                                expression=exp.Literal.string(
                                    "user1,user2,user3,user4,user5"
                                ),
                                start=exp.Literal.number(-1),
                            ),
                            alias=exp.to_identifier("LOCATE2"),
                        ),
                    ]
                ),
                "SELECT LOCATE('cab', 'abcabcabc') LOCATE1, LOCATE('user', 'user1,user2,user3,user4,user5', -1) LOCATE2",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Log(
                                this=exp.Literal.number(1000),
                                base=exp.Literal.number(10),
                            ),
                            alias="LOG_RESULT",
                        ),
                        exp.Alias(
                            this=exp.Log2(this=exp.Literal.number(256)),
                            alias="LOG2_RESULT",
                        ),
                        exp.Alias(
                            this=exp.Log10(this=exp.Literal.number(10000)),
                            alias="LOG10_RESULT",
                        ),
                    ]
                ),
                "SELECT LOG(1000, 10) LOG_RESULT, LOG2(256) LOG2_RESULT, LOG10(10000) LOG10_RESULT",
            ),
            (
                exp.Select(expressions=[exp.Lower(this=exp.Literal.string("HELLO"))]),
                "SELECT LOWER('HELLO')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.LPad(
                            this=exp.Literal.string("abc"),
                            length=exp.Literal.number(5),
                            pad=exp.Literal.string("X"),
                        )
                    ]
                ),
                "SELECT LPAD('abc', 5, 'X')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.LTrim(
                            this=exp.Literal.string("  abc"),
                            trim_chars=exp.Literal.string(" "),
                        )
                    ]
                ),
                "SELECT LTRIM('  abc', ' ')",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.LTrim(
                                this=exp.Literal.string("  sql  "),
                            ),
                            alias="TRIMMED",
                        )
                    ]
                ),
                "SELECT LTRIM('  sql  ') TRIMMED",
            ),
            (
                exp.Mod(this=exp.Column(this="15"), expression=exp.Column(this="6")),
                "MOD(15, 6)",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Mid(
                                this=exp.Literal.string("abcdef"),
                                start=exp.Literal.number(2),
                            ),
                            alias="MID_COL",
                        )
                    ]
                ),
                "SELECT MID('abcdef', 2) MID_COL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Mid(
                                this=exp.Literal.string("abcdef"),
                                start=exp.Literal.number(2),
                                length=exp.Literal.number(3),
                            ),
                            alias="MID_COL",
                        )
                    ]
                ),
                "SELECT MID('abcdef', 2, 3) MID_COL",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.MinScale(this=exp.Literal.number("123.00000")),
                            alias="S1",
                        ),
                        exp.Alias(
                            this=exp.MinScale(this=exp.Literal.number("9.99999999999")),
                            alias="S2",
                        ),
                        exp.Alias(
                            this=exp.MinScale(this=exp.Literal.number("-0.0045600")),
                            alias="S3",
                        ),
                    ]
                ),
                "SELECT MIN_SCALE(123.00000) S1, MIN_SCALE(9.99999999999) S2, MIN_SCALE(-0.0045600) S3",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="MINUTES_BETWEEN",
                                expressions=[
                                    exp.Literal.string(
                                        "TIMESTAMP '2012-03-25 02:30:00'"
                                    ),
                                    exp.Literal.string(
                                        "TIMESTAMP '2000-01-01 12:00:02'"
                                    ),
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
                    expressions=[
                        exp.Alias(this=exp.Anonymous(this="NOW"), alias="NOW_TIME")
                    ]
                ),
                "SELECT NOW() NOW_TIME",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(this=exp.Anonymous(this="NPROC"), alias="PROC_COUNT")
                    ]
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
                                expressions=[
                                    exp.Ordered(this=exp.Column(this="hire_date"))
                                ]
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
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
                                ),
                            ),
                            alias=exp.to_identifier("NTILE"),
                        ),
                    ],
                    # from_=exp.from_("employee_table"),
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.to_identifier("department")),
                            exp.Ordered(this=exp.to_identifier("hire_date")),
                        ]
                    ),
                ).from_("employee_table"),
                "SELECT id, department, hire_date, NTILE(4) OVER (PARTITION BY department ORDER BY hire_date) NTILE FROM employee_table ORDER BY department, hire_date",
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
                exp.Select(
                    expressions=[
                        exp.Alias(this=exp.Anonymous(this="PI"), alias="PI_VAL")
                    ]
                ),
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
                                        exp.Ordered(
                                            this=exp.Column(this="current_salary")
                                        )
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
                "SELECT id, department, current_salary, PERCENT_RANK() OVER (PARTITION BY department ORDER BY current_salary) PERCENT_RANK FROM employee_table ORDER BY department, current_salary",
            ),
            (
                exp.Alias(
                    this=exp.PercentileDisc(
                        this=exp.Literal.number(0.7),
                        order=exp.Order(
                            expressions=[
                                exp.Ordered(this=exp.Column(this="current_salary"))
                            ]
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
                            expressions=[
                                exp.Ordered(this=exp.Column(this="current_salary"))
                            ]
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
                        this=exp.Literal.string("cab"),
                        expression=exp.Literal.string("abcabcabc"),
                    ),
                    alias=exp.to_identifier("POS"),
                ),
                "POSITION('cab' IN 'abcabcabc') POS",
            ),
            (
                exp.Alias(
                    this=exp.Pow(
                        this=exp.Literal.number(2), expression=exp.Literal.number(10)
                    ),
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
                                        exp.Ordered(
                                            this=exp.Column(this="current_salary")
                                        )
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
                "SELECT id, department, current_salary, RANK() OVER (PARTITION BY department ORDER BY current_salary) RANK FROM employee_table ORDER BY department, current_salary",
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
                "SELECT id, department, current_salary, RATIO_TO_REPORT(current_salary) OVER (PARTITION BY department) RATIO_TO_REPORT FROM employee_table ORDER BY department, current_salary",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Alias(
                            this=exp.Anonymous(
                                this="REGEXP_INSTR",
                                expressions=[
                                    exp.Literal.string("Phone: +497003927877678"),
                                    exp.Literal.string("\+?\d+"),
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
                                        "(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}"
                                    ),
                                    exp.Literal.number(1),
                                    exp.Literal.number(2),
                                ],
                            ),
                            alias="REGEXP_INSTR2",
                        ),
                    ]
                ),
                "SELECT REGEXP_INSTR('Phone: +497003927877678', '\+?\d+') REGEXP_INSTR1, REGEXP_INSTR('From: my_mail@yahoo.com - To: SERVICE@EXASOL.COM', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}', 1, 2) REGEXP_INSTR2",
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
                                        exp.Column(this="starting_salary"),
                                        exp.Column(this="current_salary"),
                                    ],
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
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
                "SELECT id, department, hire_date, REGR_SLOPE(starting_salary, current_salary) OVER (PARTITION BY department ORDER BY hire_date) REGR_SLOPE FROM employee_table ORDER BY department, hire_date",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Repeat(
                            this=exp.Literal.string("abc"), times=exp.Literal.number(3)
                        )
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
            # Datetime valid units
            (
                exp.Select(
                    expressions=[
                        exp.Round(
                            this=exp.Literal.string("DATE '2023-01-01'"),
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
                            this=exp.Literal.string("DATE '2023-01-01'"),
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
                            this=exp.Literal.string("TIMESTAMP '2023-01-01 10:00:00'"),
                            decimals=exp.Literal.string("HH"),
                        )
                    ]
                ),
                "SELECT ROUND(TIMESTAMP '2023-01-01 10:00:00', 'HH')",
            ),
            (
                exp.select(
                    "SALES_ID",
                    exp.Anonymous(this="ROWNUM"),
                )
                .from_("SALES")
                .where(
                    exp.LT(
                        this=exp.Anonymous(this="ROWNUM"),
                        expression=exp.Literal.number(10),
                    )
                ),
                "SELECT SALES_ID, ROWNUM FROM SALES WHERE ROWNUM < 10",
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
                                    this="ROW_NUMBER",
                                    # expressions=[
                                    #     exp.Column(this="starting_salary"),
                                    #     exp.Column(this="current_salary"),
                                    # ],
                                ),
                                partition_by=[exp.to_identifier("department")],
                                order=exp.Order(
                                    expressions=[
                                        exp.Ordered(this=exp.to_identifier("hire_date"))
                                    ]
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
                "SELECT id, department, hire_date, ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date) ROW_NUMBER FROM employee_table ORDER BY department, hire_date",
            ),
            (
                exp.select(
                    exp.Anonymous(this="ROWID"),
                    "i",
                ).from_("t"),
                "SELECT ROWID, i FROM t",
            ),
            (
                exp.Select(
                    expressions=[
                        exp.Anonymous(
                            this="RPAD",
                            expressions=[
                                exp.Literal.string("abc"),
                                exp.Literal.number(5),
                                exp.Literal.string("X"),
                            ],
                        )
                    ]
                ),
                "SELECT RPAD('abc', 5, 'X')",
            ),
        ]

        for expression, expected_sql in tests:
            with self.subTest(sql=expected_sql):
                print(generator.sql(expression))
                print(expected_sql)
                self.assertEqual(generator.sql(expression), expected_sql)
