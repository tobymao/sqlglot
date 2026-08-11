from sqlglot import exp, parse_one
from tests.dialects.test_dialect import Validator


class TestTrino(Validator):
    dialect = "trino"

    def test_trino(self):
        self.validate_identity("REFRESH MATERIALIZED VIEW mynamespace.test_view")
        self.validate_identity("JSON_QUERY(m.properties, 'lax $.area' OMIT QUOTES NULL ON ERROR)")
        self.validate_identity("JSON_EXTRACT(content, json_path)")
        self.validate_identity("JSON_QUERY(content, 'lax $.HY.*')")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH ARRAY WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH UNCONDITIONAL WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITHOUT CONDITIONAL WRAPPER)")
        self.validate_identity("JSON_QUERY(description, 'strict $.comment' KEEP QUOTES)")
        self.validate_identity(
            "JSON_QUERY(description, 'strict $.comment' OMIT QUOTES ON SCALAR STRING)"
        )
        self.validate_identity(
            "JSON_QUERY(content, 'strict $.HY.*' WITH UNCONDITIONAL WRAPPER KEEP QUOTES)"
        )
        self.validate_identity(
            "SELECT TIMESTAMP '2012-10-31 01:00 -2'",
            "SELECT CAST('2012-10-31 01:00 -2' AS TIMESTAMP WITH TIME ZONE)",
        )
        self.validate_identity(
            "SELECT TIMESTAMP '2012-10-31 01:00 +2'",
            "SELECT CAST('2012-10-31 01:00 +2' AS TIMESTAMP WITH TIME ZONE)",
        )

        self.validate_all(
            "SELECT FROM_ISO8601_TIMESTAMP_NANOS('2020-05-11T11:15:05.000000000')",
            write={
                "duckdb": "SELECT CAST('2020-05-11T11:15:05.000000000' AS TIMESTAMPTZ)",
                "trino": "SELECT FROM_ISO8601_TIMESTAMP_NANOS('2020-05-11T11:15:05.000000000')",
                "snowflake": "SELECT CAST('2020-05-11T11:15:05.000000000' AS TIMESTAMPTZ)",
                "spark": "SELECT CAST('2020-05-11T11:15:05.000000000' AS TIMESTAMP)",
                "databricks": "SELECT CAST('2020-05-11T11:15:05.000000000' AS TIMESTAMP)",
                "bigquery": "SELECT CAST('2020-05-11T11:15:05.000000000' AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMP '2012-10-31 01:00:00 +02:00'",
            write={
                "duckdb": "SELECT CAST('2012-10-31 01:00:00 +02:00' AS TIMESTAMPTZ)",
                "trino": "SELECT CAST('2012-10-31 01:00:00 +02:00' AS TIMESTAMP WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "SELECT FORMAT('%s', 123)",
            write={
                "duckdb": "SELECT FORMAT('{}', 123)",
                "snowflake": "SELECT TO_CHAR(123)",
                "trino": "SELECT FORMAT('%s', 123)",
            },
        )

        self.validate_identity(
            "SELECT * FROM tbl MATCH_RECOGNIZE (PARTITION BY id ORDER BY col MEASURES FIRST(col, 2) AS col1, LAST(col, 2) AS col2 PATTERN (B* A) DEFINE A AS col = 1)"
        )

        self.validate_identity("SELECT VERSION()")

    def test_listagg(self):
        self.validate_identity(
            "SELECT LISTAGG(DISTINCT col, ',') WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE '...' WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE '...' WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col) WITHIN GROUP (ORDER BY col DESC) FROM tbl",
            "SELECT LISTAGG(col, ',') WITHIN GROUP (ORDER BY col DESC) FROM tbl",
        )

    def test_trim(self):
        self.validate_identity("SELECT TRIM('!' FROM '!foo!')")
        self.validate_identity("SELECT TRIM(BOTH '$' FROM '$var$')")
        self.validate_identity("SELECT TRIM(TRAILING 'ER' FROM UPPER('worker'))")
        self.validate_identity(
            "SELECT TRIM(LEADING FROM '  abcd')",
            "SELECT LTRIM('  abcd')",
        )
        self.validate_identity(
            "SELECT TRIM('!foo!', '!')",
            "SELECT TRIM('!' FROM '!foo!')",
        )

    def test_ddl(self):
        self.validate_identity("ALTER TABLE users RENAME TO people")
        self.validate_identity("ALTER TABLE IF EXISTS users RENAME TO people")
        self.validate_identity("ALTER TABLE users ADD COLUMN zip VARCHAR")
        self.validate_identity("ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip VARCHAR")
        self.validate_identity("ALTER TABLE users DROP COLUMN zip")
        self.validate_identity("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip")
        self.validate_identity("ALTER TABLE users RENAME COLUMN id TO user_id")
        self.validate_identity("ALTER TABLE IF EXISTS users RENAME COLUMN IF EXISTS id TO user_id")
        self.validate_identity("ALTER TABLE users ALTER COLUMN id SET DATA TYPE BIGINT")
        self.validate_identity("ALTER TABLE users ALTER COLUMN id DROP NOT NULL")
        self.validate_identity(
            "ALTER TABLE people SET AUTHORIZATION alice", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET AUTHORIZATION ROLE PUBLIC", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES x = 'y'", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES foo = 123, 'foo bar' = 456",
            check_command_warning=True,
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES x = DEFAULT", check_command_warning=True
        )
        self.validate_identity("ALTER VIEW people RENAME TO users")
        self.validate_identity(
            "ALTER VIEW people SET AUTHORIZATION alice", check_command_warning=True
        )
        self.validate_identity("CREATE SCHEMA foo WITH (LOCATION='s3://bucket/foo')")
        self.validate_identity(
            "CREATE TABLE foo.bar WITH (LOCATION='s3://bucket/foo/bar') AS SELECT 1"
        )

        # Hive connector syntax (partitioned_by)
        self.validate_identity(
            "CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONED_BY=ARRAY['a', 'b'])"
        )
        self.validate_identity(
            'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONED_BY=ARRAY[\'a\', \'b\'])',
            identify=True,
        )

        # Iceberg connector syntax (partitioning, can contain Iceberg transform expressions)
        self.validate_identity(
            "CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONING=ARRAY['a', 'bucket(4, b)', 'month(c)'])",
        )
        self.validate_identity(
            'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONING=ARRAY[\'a\', \'bucket(4, b)\', \'month(c)\'])',
            identify=True,
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE tbl")
        self.validate_identity("ANALYZE tbl WITH (prop1=val1, prop2=val2)")

    def test_json_value(self):
        self.validate_identity(
            "JSON_VALUE(jl.extra_attributes, 'lax $.amount_source' RETURNING VARCHAR)"
        )

        json_doc = """'{"item": "shoes", "price": "49.95"}'"""
        self.validate_identity(f"""SELECT JSON_VALUE({json_doc}, 'strict $.price')""")
        self.validate_identity(
            f"""SELECT JSON_VALUE({json_doc}, 'lax $.price' RETURNING DECIMAL(4, 2))"""
        )

        for on_option in ("NULL", "ERROR", "DEFAULT 1"):
            self.validate_identity(
                f"""SELECT JSON_VALUE({json_doc}, 'lax $.price' RETURNING DECIMAL(4, 2) {on_option} ON EMPTY {on_option} ON ERROR) AS price"""
            )

    def test_array_first(self):
        self.validate_identity("SELECT ARRAY_FIRST(ARRAY['a', 'b']) FROM tbl")
        self.validate_identity("SELECT ARRAY_FIRST(ARRAY['a', 'b'], x -> x = 'b') FROM tbl")

    def test_inline_udf(self):
        self.validate_identity(
            "WITH FUNCTION f(num INTEGER) RETURNS INTEGER RETURN num SELECT F(1)"
        )
        self.validate_identity(
            "WITH FUNCTION hello(name VARCHAR) RETURNS VARCHAR RETURN FORMAT('Hello %s!', name), "
            "FUNCTION bye() RETURNS VARCHAR RETURN 'Bye!' "
            "SELECT HELLO('Finn') || BYE()"
        )
        self.validate_identity(
            "WITH FUNCTION doubled(x INTEGER) RETURNS INTEGER RETURN x * 2 "
            "WITH t AS (SELECT 3 AS v) SELECT DOUBLED(v) FROM t"
        )
        self.validate_identity(
            "WITH FUNCTION f(x INTEGER) RETURNS INTEGER RETURN x "
            "WITH RECURSIVE t(n) AS (SELECT 1 AS n) SELECT F(n) FROM t"
        )
        self.validate_identity(
            """WITH FUNCTION f(num int)
    RETURNS int
    RETURN num
SELECT f(1)""",
            "WITH FUNCTION f(num INTEGER) RETURNS INTEGER RETURN num SELECT F(1)",
        )
        self.validate_identity("WITH function AS (SELECT 1 AS x) SELECT x FROM function")
        self.validate_identity("WITH function(x) AS (SELECT 1) SELECT x FROM function")

        self.validate_identity("WITH FUNCTION f() RETURNS INTEGER LANGUAGE SQL RETURN 1 SELECT F()")
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER DETERMINISTIC RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER NOT DETERMINISTIC RETURN 1 SELECT F()"
        )
        self.assertIsInstance(
            self.validate_identity("SELECT NOT deterministic FROM t").selects[0], exp.Not
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER CALLED ON NULL INPUT RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER RETURNS NULL ON NULL INPUT RETURN 1 SELECT F()"
        )
        self.validate_identity("WITH FUNCTION f() RETURNS INTEGER COMMENT 'hi' RETURN 1 SELECT F()")

        # SECURITY and WITH (...) are part of Trino's documented function-specification
        # grammar, but real Trino rejects both for LANGUAGE SQL inline functions
        # specifically ("Security mode not supported for inline functions", "Function
        # language 'SQL' does not support properties"). These assert round-trip
        # correctness for that shared grammar, not that this exact combination executes
        # on an inline SQL UDF.
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER SECURITY DEFINER RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER SECURITY INVOKER RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER WITH (weight=42) RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER LANGUAGE SQL WITH (weight=42, cost='low') "
            "RETURN 1 SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION custom_sqrt(a INTEGER) RETURNS DOUBLE COMMENT 'Custom sqrt function' "
            "RETURNS NULL ON NULL INPUT NOT DETERMINISTIC LANGUAGE SQL SECURITY DEFINER "
            "WITH (weight=42, cost='low') RETURN a SELECT CUSTOM_SQRT(4)"
        )

    def test_inline_udf_begin_end(self):
        # https://trino.io/docs/current/udf/sql/begin.html
        self.validate_identity(
            "WITH FUNCTION meaning_of_life() RETURNS INTEGER "
            "BEGIN DECLARE a INTEGER DEFAULT 6; DECLARE b INTEGER DEFAULT 7; RETURN a * b; END "
            "SELECT MEANING_OF_LIFE()"
        )

        # https://trino.io/docs/current/udf/sql/set.html
        self.validate_identity(
            "WITH FUNCTION one() RETURNS INTEGER "
            "BEGIN DECLARE counter INTEGER DEFAULT 1; SET counter = 0; "
            "SET counter = counter + 2; SET counter = counter / counter; RETURN counter; END "
            "SELECT ONE()"
        )

        # https://trino.io/docs/current/udf/sql/declare.html - multiple identifiers can
        # share one DECLARE and type
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER "
            "BEGIN DECLARE first_name, last_name, middle_name VARCHAR(25); RETURN 1; END "
            "SELECT F()"
        )

        # BEGIN can nest; confirmed against a real Trino instance (returns 2)
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER "
            "BEGIN DECLARE x INTEGER DEFAULT 1; BEGIN SET x = x + 1; END; RETURN x; END "
            "SELECT F()"
        )

        # DECLARE isn't reserved in Trino, so it must still work as a plain identifier
        self.validate_identity("SELECT declare FROM (VALUES (1), (2)) AS t(declare)")
        self.validate_identity("WITH FUNCTION declare() RETURNS INTEGER RETURN 1 SELECT DECLARE()")

        # A Block built without begin=True (i.e. not by _parse_routine_block) must
        # not get a synthesized BEGIN
        self.assertEqual(
            exp.Block(
                expressions=[parse_one("SELECT 1"), parse_one("SELECT 2"), exp.EndStatement()]
            ).sql(dialect="trino"),
            "SELECT 1; SELECT 2; END",
        )

    def test_inline_udf_if(self):
        # https://trino.io/docs/current/udf/sql/if.html - verbatim from the docs, but
        # real Trino rejects this exact body with "Function must end in a RETURN
        # statement": its function-body check requires a literal trailing RETURN and
        # doesn't credit an IF/ELSEIF/ELSE that already returns on every branch. This
        # asserts round-trip grammar only, confirmed against a real Trino instance.
        self.validate_identity(
            "WITH FUNCTION simple_if(a BIGINT) RETURNS VARCHAR "
            "BEGIN IF a = 0 THEN RETURN 'zero'; ELSEIF a = 1 THEN RETURN 'one'; "
            "ELSE RETURN 'more than one or negative'; END IF; END "
            "SELECT SIMPLE_IF(3)"
        )

        # IF with no ELSE/ELSEIF at all; confirmed against a real Trino instance
        self.validate_identity(
            "WITH FUNCTION f(a INTEGER) RETURNS VARCHAR "
            "BEGIN IF a = 0 THEN RETURN 'zero'; END IF; RETURN 'other'; END "
            "SELECT F(1)"
        )

        # An ELSEIF chain with no final ELSE; confirmed against a real Trino instance
        self.validate_identity(
            "WITH FUNCTION f(a INTEGER) RETURNS VARCHAR "
            "BEGIN IF a = 0 THEN RETURN 'zero'; ELSEIF a = 1 THEN RETURN 'one'; END IF; "
            "RETURN 'other'; END "
            "SELECT F(1)"
        )

        # IF can nest; the trailing RETURN is required by the same real-Trino
        # completeness check noted above, confirmed to actually run and return 'a zero'
        self.validate_identity(
            "WITH FUNCTION f(a INTEGER, b INTEGER) RETURNS VARCHAR "
            "BEGIN IF a = 0 THEN IF b = 0 THEN RETURN 'both zero'; ELSE RETURN 'a zero'; "
            "END IF; ELSE RETURN 'a nonzero'; END IF; RETURN 'unreachable'; END "
            "SELECT F(1, 2)"
        )

        # Combines with DECLARE/SET, and a CASE expression nested inside a RETURN
        # doesn't get confused with the surrounding IF's own ELSE/END IF
        self.validate_identity(
            "WITH FUNCTION f(a INTEGER) RETURNS INTEGER "
            "BEGIN DECLARE result INTEGER DEFAULT 0; "
            "IF a > 0 THEN RETURN CASE WHEN a > 10 THEN 1 ELSE 2 END; "
            "ELSE SET result = -1; END IF; RETURN result; END "
            "SELECT F(1)"
        )

        # Trino's own function-body analysis rejects a NOT DETERMINISTIC declaration
        # on a body it can tell is trivially deterministic (same class of issue as the
        # SECURITY/WITH (...) note above), so this asserts round-trip grammar only.
        self.validate_identity(
            "WITH FUNCTION f() RETURNS INTEGER LANGUAGE SQL NOT DETERMINISTIC "
            "BEGIN DECLARE x INTEGER DEFAULT 1; RETURN x; END "
            "SELECT F()"
        )
        self.validate_identity(
            "WITH FUNCTION doubled(x INTEGER) RETURNS INTEGER BEGIN RETURN x * 2; END "
            "WITH t AS (SELECT 3 AS v) SELECT DOUBLED(v) FROM t"
        )

    def test_inline_udf_case(self):
        # https://trino.io/docs/current/udf/sql/case.html - verbatim from the docs
        # (operand form). The docs label the operand form "Searched case" with a
        # synopsis ending in a bare END, but that contradicts this very example,
        # which uses the operand form and ends in END CASE; confirmed against a
        # real Trino instance that only END CASE is accepted for either form.
        self.validate_identity(
            "WITH FUNCTION simple_case(a BIGINT) RETURNS VARCHAR "
            "BEGIN CASE a WHEN 0 THEN RETURN 'zero'; WHEN 1 THEN RETURN 'one'; "
            "ELSE RETURN 'more than one or negative'; END CASE; RETURN NULL; END "
            "SELECT SIMPLE_CASE(0)"
        )

        # No-operand form ("Simple case" per the docs' own, inverted labeling);
        # confirmed against a real Trino instance
        self.validate_identity(
            "WITH FUNCTION searched_case(a BIGINT) RETURNS VARCHAR "
            "BEGIN CASE WHEN a = 0 THEN RETURN 'zero'; WHEN a = 1 THEN RETURN 'one'; "
            "ELSE RETURN 'other'; END CASE; RETURN NULL; END "
            "SELECT SEARCHED_CASE(0)"
        )

        # No ELSE at all, and only a single WHEN; confirmed against a real Trino
        # instance that this falls through to the following RETURN rather than
        # erroring
        self.validate_identity(
            "WITH FUNCTION no_else(a BIGINT) RETURNS VARCHAR "
            "BEGIN CASE a WHEN 0 THEN RETURN 'zero'; END CASE; RETURN 'fallthrough'; END "
            "SELECT NO_ELSE(0)"
        )

        # No-operand form, no ELSE; confirmed against a real Trino instance
        self.validate_identity(
            "WITH FUNCTION no_operand_no_else(a INTEGER) RETURNS VARCHAR "
            "BEGIN CASE WHEN a = 0 THEN RETURN 'zero'; END CASE; RETURN 'other'; END "
            "SELECT NO_OPERAND_NO_ELSE(1)"
        )

        # CASE can nest, in both the operand and no-operand forms; confirmed to
        # actually run and return 'a0b0'/'a0bN'/'aN' against a real Trino instance
        self.validate_identity(
            "WITH FUNCTION nested_case(a BIGINT, b BIGINT) RETURNS VARCHAR "
            "BEGIN DECLARE result VARCHAR; "
            "CASE WHEN a = 0 THEN CASE b WHEN 0 THEN SET result = 'a0b0'; "
            "ELSE SET result = 'a0bN'; END CASE; ELSE SET result = 'aN'; END CASE; "
            "RETURN result; END "
            "SELECT NESTED_CASE(0, 0)"
        )

        # Combines with IF, and a CASE expression nested inside a SET doesn't get
        # confused with the surrounding CASE statement's own ELSE/END CASE
        self.validate_identity(
            "WITH FUNCTION mix(a INTEGER) RETURNS INTEGER "
            "BEGIN DECLARE result INTEGER DEFAULT 0; "
            "CASE WHEN a > 0 THEN IF a > 10 THEN SET result = 1; ELSE SET result = 2; END IF; "
            "ELSE SET result = CASE WHEN a < -10 THEN -1 ELSE -2 END; END CASE; "
            "RETURN result; END "
            "SELECT MIX(1)"
        )

        # CASE as the literal last statement, with nothing after it before the
        # enclosing END; real Trino rejects this body with "Function must end in
        # a RETURN statement" - the same function-body completeness check noted
        # on the IF phase, which requires a literal trailing RETURN and doesn't
        # credit a CASE that already returns on every branch. This asserts
        # round-trip grammar only, confirmed against a real Trino instance.
        self.validate_identity(
            "WITH FUNCTION last_stmt(a INTEGER) RETURNS VARCHAR "
            "BEGIN CASE a WHEN 0 THEN RETURN 'zero'; ELSE RETURN 'other'; END CASE; END "
            "SELECT LAST_STMT(1)"
        )

    def test_inline_udf_while(self):
        # https://trino.io/docs/current/udf/sql/while.html - verbatim from the docs
        # (wrapped in a function since the docs show it as a body fragment);
        # confirmed against a real Trino instance (returns 625 = 5^4)
        # (renamed from the docs' own function name to avoid colliding with the
        # builtin two-argument POWER function)
        self.validate_identity(
            "WITH FUNCTION npow(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE r BIGINT DEFAULT 1; DECLARE p BIGINT DEFAULT n; "
            "WHILE p > 1 DO SET r = r * n; SET p = p - 1; END WHILE; "
            "RETURN r; END "
            "SELECT NPOW(5)"
        )

        # The optional `label :` prefix names the block for ITERATE/LEAVE (not
        # yet supported); confirmed to still return 625 against a real Trino
        # instance, and that a repeated label after END WHILE is rejected
        self.validate_identity(
            "WITH FUNCTION npow_labeled(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE r BIGINT DEFAULT 1; DECLARE p BIGINT DEFAULT n; "
            "abc: WHILE p > 1 DO SET r = r * n; SET p = p - 1; END WHILE; "
            "RETURN r; END "
            "SELECT NPOW_LABELED(5)"
        )

        # WHILE as the literal last statement, with nothing after it before the
        # enclosing END; real Trino rejects this body with "Function must end in
        # a RETURN statement", the same function-body completeness check noted
        # on the IF and CASE phases. This asserts round-trip grammar only,
        # confirmed against a real Trino instance.
        self.validate_identity(
            "WITH FUNCTION last_stmt(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE r BIGINT DEFAULT 1; WHILE r < n DO SET r = r + 1; END WHILE; END "
            "SELECT LAST_STMT(5)"
        )

        # WHILE can nest, and combines with IF and CASE; confirmed against a real
        # Trino instance to actually run and return 4 for count_evens(3)
        self.validate_identity(
            "WITH FUNCTION count_evens(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE total BIGINT DEFAULT 0; DECLARE i BIGINT DEFAULT 0; "
            "WHILE i < n DO SET total = total + 1; IF i = 0 THEN SET total = total + 1; END IF; "
            "SET i = i + 1; END WHILE; "
            "RETURN total; END "
            "SELECT COUNT_EVENS(3)"
        )
        self.validate_identity(
            "WITH FUNCTION while_case(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE i BIGINT DEFAULT 0; "
            "WHILE i < n DO CASE WHEN i = 0 THEN SET i = 1; ELSE SET i = i + 1; END CASE; END WHILE; "
            "RETURN i; END "
            "SELECT WHILE_CASE(3)"
        )

    def test_inline_udf_loop_repeat(self):
        # https://trino.io/docs/current/udf/sql/loop.html - verbatim from the docs
        # (wrapped as a function invocation); confirmed against a real Trino
        # instance to return 10, 20, 30 for step values of 10, 20, 30
        self.validate_identity(
            "WITH FUNCTION to_one_hundred(start_value BIGINT, step BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE count BIGINT DEFAULT 0; DECLARE current BIGINT DEFAULT 0; "
            "SET current = start_value; "
            "abc: LOOP IF current >= 100 THEN LEAVE abc; END IF; "
            "SET count = count + 1; SET current = current + step; END LOOP; "
            "RETURN count; END "
            "SELECT TO_ONE_HUNDRED(0, 10)"
        )

        # https://trino.io/docs/current/udf/sql/repeat.html - verbatim from the
        # docs; unlike WHILE/LOOP the body always runs at least once, confirmed
        # against a real Trino instance to return 10, 10, 11, 12, 13 for inputs
        # 5, 9, 10, 11, 12
        self.validate_identity(
            "WITH FUNCTION test_repeat(a BIGINT) RETURNS BIGINT "
            "BEGIN REPEAT SET a = a + 1; UNTIL a >= 10 END REPEAT; RETURN a; END "
            "SELECT TEST_REPEAT(5)"
        )

        # https://trino.io/docs/current/udf/sql/iterate.html - verbatim from the
        # docs (renamed from the docs' own function name to avoid colliding with
        # the builtin COUNT aggregate); confirmed against a real Trino instance
        # to return 7
        self.validate_identity(
            "WITH FUNCTION iter_count() RETURNS BIGINT "
            "BEGIN DECLARE a BIGINT DEFAULT 0; DECLARE b BIGINT DEFAULT 0; "
            "top: REPEAT SET a = a + 1; IF a <= 3 THEN ITERATE top; END IF; SET b = b + 1; "
            "UNTIL a >= 10 END REPEAT; "
            "RETURN b; END "
            "SELECT ITER_COUNT()"
        )

        # LOOP can nest inside WHILE, and LEAVE targets the innermost matching
        # label; confirmed against a real Trino instance to return 6 (2 outer
        # iterations x 3 inner increments each)
        self.validate_identity(
            "WITH FUNCTION nested_test(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE total BIGINT DEFAULT 0; DECLARE i BIGINT DEFAULT 0; DECLARE j BIGINT DEFAULT 0; "
            "outer_loop: WHILE i < n DO SET j = 0; "
            "inner_loop: LOOP IF j >= 3 THEN LEAVE inner_loop; END IF; "
            "SET total = total + 1; SET j = j + 1; END LOOP; "
            "SET i = i + 1; END WHILE; "
            "RETURN total; END "
            "SELECT NESTED_TEST(2)"
        )

        # LOOP/REPEAT as the literal last statement, with nothing after before
        # the enclosing END; real Trino rejects this body with "Function must
        # end in a RETURN statement", the same function-body completeness check
        # noted on the IF/CASE/WHILE phases. This asserts round-trip grammar
        # only, confirmed against a real Trino instance.
        self.validate_identity(
            "WITH FUNCTION last_stmt(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE i BIGINT DEFAULT 0; "
            "top: LOOP IF i >= n THEN LEAVE top; END IF; SET i = i + 1; END LOOP; END "
            "SELECT LAST_STMT(5)"
        )

        # `ITERATE`/`LEAVE` are themselves valid label names, confirmed against
        # a real Trino instance to still return 5 for both; the label lookahead
        # has to be checked before ITERATE/LEAVE are matched as keywords, or
        # this misparses as a bare (invalid) ITERATE/LEAVE statement
        self.validate_identity(
            "WITH FUNCTION label_test(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE i BIGINT DEFAULT 0; "
            "iterate: LOOP IF i >= n THEN LEAVE iterate; END IF; SET i = i + 1; END LOOP; "
            "RETURN i; END "
            "SELECT LABEL_TEST(5)"
        )
        self.validate_identity(
            "WITH FUNCTION label_test2(n BIGINT) RETURNS BIGINT "
            "BEGIN DECLARE i BIGINT DEFAULT 0; "
            "leave: LOOP IF i >= n THEN LEAVE leave; END IF; SET i = i + 1; END LOOP; "
            "RETURN i; END "
            "SELECT LABEL_TEST2(5)"
        )
