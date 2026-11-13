from sqlglot import ErrorLevel, ParseError, UnsupportedError, exp, parse, parse_one
from tests.dialects.test_dialect import Validator


class TestTeradata(Validator):
    dialect = "teradata"

    def test_characters(self):
        self.validate_identity("characters(x)", "LENGTH(x)").assert_is(exp.Length)
        for value in ("x", "'abc'", "''", "'a  '", "'\u00e9\u4e2d'", "NULL"):
            self.validate_all(
                f"CHARACTERS({value})",
                write={
                    "teradata": f"LENGTH({value})",
                    "databricks": f"LENGTH({value})",
                    "postgres": f"LENGTH({value})",
                },
            )

    def test_curdate(self):
        for sql in ("CURDATE()", "curdate()"):
            self.validate_identity(sql, "CURRENT_DATE").assert_is(exp.CurrentDate)
            self.validate_all(
                f"SELECT {sql}",
                write={
                    "teradata": "SELECT CURRENT_DATE",
                    "databricks": "SELECT CURRENT_DATE",
                    "postgres": "SELECT CURRENT_DATE",
                },
            )
        self.validate_identity('SELECT curdate, "curdate", t.curdate FROM t')
        self.validate_identity("CREATE TABLE t (curdate DATE)")

    def test_curtime(self):
        for sql in ("CURTIME()", "curtime()"):
            self.validate_identity(sql, "CURRENT_TIME").assert_is(exp.CurrentTime)
            self.validate_identity(
                f"SELECT {sql} AS request_time", "SELECT CURRENT_TIME AS request_time"
            )
        self.validate_identity('SELECT curtime, "curtime", t.curtime FROM t')
        self.validate_identity("CREATE TABLE t (curtime TIME)")
        self.validate_identity("SELECT t.CURTIME() FROM t")
        self.assertIsInstance(parse_one("CURTIME()"), exp.Anonymous)

    def test_current_time(self):
        self.validate_identity("current_time", "CURRENT_TIME").assert_is(exp.CurrentTime)
        for precision in (0, 3, 6):
            self.validate_identity(f"CURRENT_TIME({precision})").assert_is(exp.CurrentTime)
        self.validate_identity("SELECT CURRENT_TIME AS request_time")
        self.validate_identity("CURRENT_TIME AT TIME ZONE 'America Pacific'")
        self.validate_identity('SELECT t.current_time, "CURRENT_TIME" FROM t')

    def test_dayofweek(self):
        self.validate_identity("dayofweek(d)", "DAYOFWEEK(d)").assert_is(exp.DayOfWeek)
        for value in (
            "d",
            "CURRENT_DATE",
            "CAST('2026-11-15' AS DATE)",
            "CAST('2026-11-15 12:00:00' AS TIMESTAMP)",
            "NULL",
        ):
            expression = self.validate_identity(f"DAYOFWEEK({value})").assert_is(exp.DayOfWeek)
            self.assertEqual(expression, self.parse_one(expression.sql(self.dialect)))
        self.validate_all(
            "DAYOFWEEK(d)",
            read={"": "DAY_OF_WEEK(d)", "hive": "DAYOFWEEK(d)"},
            write={"": "DAY_OF_WEEK(d)", "teradata": "DAYOFWEEK(d)", "hive": "DAYOFWEEK(d)"},
        )
        self.validate_identity('SELECT dayofweek, "dayofweek", t.dayofweek FROM t')
        self.validate_identity("SELECT t.DAYOFWEEK(d) FROM t")

    def test_execute(self):
        for sql, expected in (
            ("EXEC example_macro", "EXECUTE example_macro"),
            ("EXEC example_macro()", "EXECUTE example_macro"),
            ("EXECUTE example_macro()", "EXECUTE example_macro"),
            ("EXECUTE example_macro", "EXECUTE example_macro"),
            ("EXEC example_db.example_macro(1)", "EXECUTE example_db.example_macro(1)"),
            ("EXECUTE example_macro(1, 'value')", "EXECUTE example_macro(1, 'value')"),
            ("EXECUTE example_macro(NULL, -2)", "EXECUTE example_macro(NULL, -2)"),
            (
                "EXECUTE example_macro(arg_a=1, arg_b='value')",
                "EXECUTE example_macro(arg_a = 1, arg_b = 'value')",
            ),
            (
                "EXECUTE example_macro(DATE '2024-01-02', COALESCE(NULL, 3), 'a;b')",
                "EXECUTE example_macro(CAST('2024-01-02' AS DATE), COALESCE(NULL, 3), 'a;b')",
            ),
            (
                'EXECUTE "example db"."example macro"(1)',
                'EXECUTE "example db"."example macro"(1)',
            ),
        ):
            with self.subTest(sql=sql):
                expression = self.validate_identity(sql, expected).assert_is(exp.Execute)
                self.assertEqual(expression, self.parse_one(expected, error_level=ErrorLevel.RAISE))
                self.assertEqual(expression, exp.Expr.load(expression.dump()))
                self.assertIsNone(expression.find(exp.Anonymous))

        expression = self.parse_one("EXEC example_db.example_macro(1, arg_b = 'value')")
        self.assertEqual(expression.name, "example_macro")
        self.assertEqual(expression.this.db, "example_db")
        self.assertEqual(len(expression.expressions), 2)
        self.assertIsInstance(expression.expressions[1], exp.EQ)
        self.assertEqual(expression.expressions[1].this.name, "arg_b")

        expressions = parse(
            "EXEC example_macro('a;b'); SELECT 1; EXECUTE example_db.example_macro(2)",
            read=self.dialect,
            error_level=ErrorLevel.RAISE,
        )
        self.assertEqual(
            [type(expression) for expression in expressions],
            [exp.Execute, exp.Select, exp.Execute],
        )
        self.assertEqual(expressions[0].expressions, [exp.Literal.string("a;b")])

    def test_execute_command_fallback(self):
        for sql in (
            "EXECUTE example_macro(, 2)",
            "EXECUTE example_macro(1,)",
            "EXECUTE example_macro(,)",
            "EXEC example_macro(1,, 3)",
            "EXECUTE IMMEDIATE 'SELECT 1'",
            "EXECUTE example_statement USING value_a",
        ):
            with self.subTest(sql=sql):
                self.validate_identity(sql, check_command_warning=True).assert_is(exp.Command)

    def test_execute_return_status(self):
        expression = exp.Execute(
            this=exp.to_table("example_macro"),
            return_status=exp.Parameter(this=exp.Var(this="status")),
        )
        with self.assertRaises(UnsupportedError):
            expression.sql("teradata", unsupported_level=ErrorLevel.RAISE)

    def test_execute_parse_errors(self):
        for sql in (
            "EXECUTE",
            "EXECUTE example_macro(1",
            "EXECUTE example_macro(arg_a =)",
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError):
                    self.parse_one(sql, error_level=ErrorLevel.RAISE)

    def test_teradata(self):
        self.validate_all(
            "RANDOM(l, u)",
            write={
                "": "(u - l) * RAND() + l",
                "teradata": "RANDOM(l, u)",
            },
        )
        self.validate_identity("TO_NUMBER(expr, fmt, nlsparam)")
        self.validate_identity("SELECT TOP 10 * FROM tbl")
        self.validate_identity("SELECT * FROM tbl SAMPLE 5")
        self.validate_identity(
            "SELECT * FROM tbl SAMPLE 0.33, .25, .1",
            "SELECT * FROM tbl SAMPLE 0.33, 0.25, 0.1",
        )
        self.validate_all(
            "SELECT * FROM t SAMPLE 100",
            write={
                "teradata": "SELECT * FROM t SAMPLE 100",
                "snowflake": "SELECT * FROM t TABLESAMPLE (100 ROWS)",
            },
        )
        self.validate_all(
            "SELECT * FROM t SAMPLE 0.25",
            read={
                "snowflake": "SELECT * FROM t TABLESAMPLE (25)",
            },
            write={
                "teradata": "SELECT * FROM t SAMPLE 0.25",
                "snowflake": "SELECT * FROM t TABLESAMPLE (25)",
            },
        )
        self.validate_all(
            "SELECT * FROM t SAMPLE 0.6, 0.25",
            write={
                "teradata": "SELECT * FROM t SAMPLE 0.6, 0.25",
                "snowflake": "SELECT * FROM t",
            },
        )
        with self.assertRaises(UnsupportedError):
            self.parse_one("SELECT * FROM t SAMPLE 10, 20").sql(
                "snowflake", unsupported_level=ErrorLevel.RAISE
            )

        self.validate_all(
            "DATABASE tduser",
            read={
                "databricks": "USE tduser",
            },
            write={
                "databricks": "USE tduser",
                "teradata": "DATABASE tduser",
            },
        )

        self.validate_identity("SELECT 0x1d", "SELECT X'1d'")
        self.validate_identity("SELECT X'1D'", "SELECT X'1D'")
        self.validate_identity("SELECT x'1d'", "SELECT X'1d'")

        self.validate_identity(
            "RENAME TABLE emp TO employee", check_command_warning=True
        ).assert_is(exp.Command)

    def test_translate(self):
        self.validate_identity("TRANSLATE(x USING LATIN_TO_UNICODE)")
        self.validate_identity("TRANSLATE(x USING LATIN_TO_UNICODE WITH ERROR)")

    def test_locking(self):
        self.validate_identity("LOCKING ROW FOR ACCESS SELECT * FROM table1")
        self.validate_identity("LOCKING TABLE table1 FOR ACCESS SELECT col1, col2 FROM table1")
        self.validate_identity("LOCKING ROW FOR SHARE SELECT * FROM table1")
        self.validate_identity("LOCKING DATABASE db1 FOR READ SELECT * FROM table1")
        self.validate_identity("LOCKING ROW FOR EXCLUSIVE SELECT * FROM table1")
        self.validate_identity("LOCKING VIEW view1 FOR ACCESS SELECT * FROM view1")

        # Test with more complex SELECT statements
        self.validate_identity(
            "LOCKING ROW FOR ACCESS SELECT col1, col2 FROM table1 WHERE col1 > 10"
        )
        self.validate_identity(
            "LOCKING TABLE table1 FOR ACCESS SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
        )

        self.validate_all(
            "LOCKING ROW FOR ACCESS SELECT * FROM t",
            write={
                "teradata": "LOCKING ROW FOR ACCESS SELECT * FROM t",
                "duckdb": "SELECT * FROM t",
                "snowflake": "SELECT * FROM t",
            },
        )

        # Test that it still works in CREATE VIEW context (regression test)
        self.validate_identity(
            "CREATE VIEW view_b AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b"
        )

    def test_update(self):
        self.validate_all(
            "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
            write={
                "teradata": "UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = '' WHERE A.col1 = B.col1",
                "mysql": "UPDATE A JOIN `schema`.tableA AS A ON TRUE JOIN (SELECT col1 FROM `schema`.tableA GROUP BY col1) AS B ON TRUE SET A.col2 = '' WHERE A.col1 = B.col1",
            },
        )

    def test_statistics(self):
        self.validate_identity("COLLECT STATISTICS ON tbl INDEX(col)", check_command_warning=True)
        self.validate_identity("COLLECT STATS ON tbl COLUMNS(col)", check_command_warning=True)
        self.validate_identity("COLLECT STATS COLUMNS(col) ON tbl", check_command_warning=True)
        self.validate_identity("HELP STATISTICS personel.employee", check_command_warning=True)
        self.validate_identity(
            "HELP STATISTICS personnel.employee FROM my_qcd", check_command_warning=True
        )

    def test_create(self):
        self.validate_identity(
            "REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
            "CREATE OR REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
        )
        self.validate_identity(
            "REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
            "CREATE OR REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b",
        )
        self.validate_identity("CREATE TABLE x (y INT) PRIMARY INDEX (y) PARTITION BY y INDEX (y)")
        self.validate_identity("CREATE TABLE x (y INT) PARTITION BY y INDEX (y)")
        self.validate_identity(
            "CREATE MULTISET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT PRESERVE ROWS"
        )
        self.validate_identity(
            "CREATE SET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT DELETE ROWS"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PRIMARY INDEX (y) PARTITION BY RANGE_N(b BETWEEN 'a', 'b' AND 'c' EACH '1')"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN 0, 1 AND 2 EACH 1)"
        )
        self.validate_identity(
            "CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN *, 1 AND * EACH b) INDEX (a)"
        )
        self.validate_identity(
            "CREATE TABLE a, NO FALLBACK PROTECTION, NO LOG, NO JOURNAL, CHECKSUM=ON, NO MERGEBLOCKRATIO, BLOCKCOMPRESSION=ALWAYS (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, NO FALLBACK PROTECTION, NO LOG, NO JOURNAL, CHECKSUM=ON, NO MERGEBLOCKRATIO, BLOCKCOMPRESSION=ALWAYS (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, WITH JOURNAL TABLE=x.y.z, CHECKSUM=OFF, MERGEBLOCKRATIO=1, DATABLOCKSIZE=10 KBYTES (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, BEFORE JOURNAL, AFTER JOURNAL, FREESPACE=1, DEFAULT DATABLOCKSIZE, BLOCKCOMPRESSION=DEFAULT (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, DUAL JOURNAL, DUAL AFTER JOURNAL, MERGEBLOCKRATIO=1 PERCENT, DATABLOCKSIZE=10 KILOBYTES (a INT)"
        )
        self.validate_identity(
            "CREATE TABLE a, DUAL BEFORE JOURNAL, LOCAL AFTER JOURNAL, MAXIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=AUTOTEMP(c1 INT) (a INT)"
        )
        self.validate_identity(
            "CREATE VOLATILE MULTISET TABLE a, NOT LOCAL AFTER JOURNAL, FREESPACE=1 PERCENT, DATABLOCKSIZE=10 BYTES, WITH NO CONCURRENT ISOLATED LOADING FOR ALL (a INT)"
        )
        self.validate_identity(
            "CREATE VOLATILE SET TABLE example1 AS (SELECT col1, col2, col3 FROM table1) WITH DATA PRIMARY INDEX (col1) ON COMMIT PRESERVE ROWS"
        )
        self.validate_identity(
            "CREATE SET GLOBAL TEMPORARY TABLE a, NO BEFORE JOURNAL, NO AFTER JOURNAL, MINIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=NEVER (a INT)"
        )
        self.validate_all(
            """
            CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL,
            CHECKSUM = DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT)
            PRIMARY INDEX (a),
            INDEX(x, y)
            """,
            write={
                "teradata": "CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM=DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT) PRIMARY INDEX (a) INDEX (x, y)",
            },
        )
        self.validate_all(
            "REPLACE VIEW a AS (SELECT b FROM c)",
            write={"teradata": "CREATE OR REPLACE VIEW a AS (SELECT b FROM c)"},
        )
        self.validate_all(
            "CREATE VOLATILE TABLE a",
            write={
                "teradata": "CREATE VOLATILE TABLE a",
                "bigquery": "CREATE TABLE a",
                "clickhouse": "CREATE TABLE a",
                "databricks": "CREATE TABLE a",
                "drill": "CREATE TABLE a",
                "duckdb": "CREATE TABLE a",
                "hive": "CREATE TABLE a",
                "mysql": "CREATE TABLE a",
                "oracle": "CREATE TABLE a",
                "postgres": "CREATE TABLE a",
                "presto": "CREATE TABLE a",
                "redshift": "CREATE TABLE a",
                "snowflake": "CREATE TABLE a",
                "spark": "CREATE TABLE a",
                "sqlite": "CREATE TABLE a",
                "starrocks": "CREATE TABLE a",
                "tableau": "CREATE TABLE a",
                "trino": "CREATE TABLE a",
                "tsql": "CREATE TABLE a",
            },
        )
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD', measurement INT COMPRESS)"
        )
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD', measurement INT COMPRESS (1, 2, 3))"
        )
        self.validate_identity(
            "CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT 'YYYY-MM-DD' COMPRESS (CAST('9999-09-09' AS DATE)), measurement INT)"
        )

    def test_insert(self):
        self.validate_all(
            "INS INTO x SELECT * FROM y", write={"teradata": "INSERT INTO x SELECT * FROM y"}
        )

    def test_mod(self):
        self.validate_all("a MOD b", write={"teradata": "a MOD b", "mysql": "a % b"})
        self.validate_identity("SELECT MOD(8, 5)", "SELECT 8 MOD 5")
        self.validate_identity("SELECT 2 ** MOD(8, 5)", "SELECT 2 ** (8 MOD 5)")

    def test_power(self):
        self.validate_all("a ** b", write={"teradata": "a ** b", "mysql": "POWER(a, b)"})

    def test_abbrev(self):
        self.validate_identity("a LT b", "a < b")
        self.validate_identity("a LE b", "a <= b")
        self.validate_identity("a GT b", "a > b")
        self.validate_identity("a GE b", "a >= b")
        self.validate_identity("a ^= b", "a <> b")
        self.validate_identity("a NE b", "a <> b")
        self.validate_identity("a NOT= b", "a <> b")
        self.validate_identity("a EQ b", "a = b")
        self.validate_identity("SEL a FROM b", "SELECT a FROM b")
        self.validate_identity(
            "SELECT col1, col2 FROM dbc.table1 WHERE col1 EQ 'value1' MINUS SELECT col1, col2 FROM dbc.table2",
            "SELECT col1, col2 FROM dbc.table1 WHERE col1 = 'value1' EXCEPT SELECT col1, col2 FROM dbc.table2",
        )
        self.validate_identity("UPD a SET b = 1", "UPDATE a SET b = 1")
        self.validate_identity("DEL FROM a", "DELETE FROM a")

    def test_datatype(self):
        self.validate_all(
            "CREATE TABLE z (a ST_GEOMETRY(1))",
            write={
                "teradata": "CREATE TABLE z (a ST_GEOMETRY(1))",
                "redshift": "CREATE TABLE z (a GEOMETRY(1))",
            },
        )

        self.validate_identity("CREATE TABLE z (a SYSUDTLIB.INT)")

    def test_byteint(self):
        from sqlglot.optimizer.annotate_types import annotate_types

        self.validate_all(
            "CAST(x AS BYTEINT)",
            read={
                "databricks": "CAST(x AS TINYINT)",
            },
            write={
                "teradata": "CAST(x AS BYTEINT)",
                "databricks": "CAST(x AS TINYINT)",
                "postgres": "CAST(x AS SMALLINT)",
                "tsql": "CAST(x AS SMALLINT)",
            },
        )
        expression = self.validate_identity("CAST(127 AS BYTEINT)").assert_is(exp.Cast)
        self.assertTrue(expression.to.is_type(exp.DType.TINYINT))
        self.assertTrue(annotate_types(expression, dialect=self.dialect).is_type(exp.DType.TINYINT))
        self.validate_all(
            "CAST(-128 AS BYTEINT)",
            write={
                "teradata": "CAST(-128 AS BYTEINT)",
                "tsql": "CAST(-128 AS SMALLINT)",
            },
        )
        self.validate_identity("CAST(x AS byteint)", "CAST(x AS BYTEINT)")
        table = self.validate_identity("CREATE TABLE z (a BYTEINT, b SMALLINT)")
        self.assertEqual(
            [datatype.this for datatype in table.find_all(exp.DataType)],
            [exp.DType.TINYINT, exp.DType.SMALLINT],
        )
        self.validate_identity("CAST(x AS SMALLINT)")

    def test_cast(self):
        self.validate_all(
            "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
            read={
                "bigquery": "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
            },
            write={
                "teradata": "CAST('1992-01' AS DATE FORMAT 'YYYY-DD')",
                "bigquery": "PARSE_DATE('%Y-%d', '1992-01')",
                "databricks": "TO_DATE('1992-01', 'yyyy-d')",
                "mysql": "STR_TO_DATE('1992-01', '%Y-%d')",
                "spark": "TO_DATE('1992-01', 'yyyy-d')",
                "": "STR_TO_DATE('1992-01', '%Y-%d')",
            },
        )
        self.validate_identity("CAST('1992-01' AS FORMAT 'YYYY-DD')")

        self.validate_all(
            "TRYCAST('-2.5' AS DECIMAL(5, 2))",
            read={
                "snowflake": "TRY_CAST('-2.5' AS DECIMAL(5, 2))",
            },
            write={
                "snowflake": "TRY_CAST('-2.5' AS DECIMAL(5, 2))",
                "teradata": "TRYCAST('-2.5' AS DECIMAL(5, 2))",
            },
        )

    def test_format_override(self):
        # Teradata column format overrides use the `(FORMAT <format_string>)` syntax.
        # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
        self.validate_identity("SELECT ('a' || 'b') (FORMAT '...')")
        self.validate_identity("SELECT Col1 (FORMAT '+9999') FROM Test1")
        self.validate_identity("SELECT date_col (FORMAT 'YYYY-MM-DD') FROM t")
        self.validate_identity(
            "SELECT CAST(Col1 AS INTEGER) FROM Test1",
            "SELECT CAST(Col1 AS INT) FROM Test1",
        )

    def test_title(self):
        from sqlglot.lineage import lineage
        from sqlglot.optimizer.annotate_types import annotate_types

        self.validate_identity("SELECT COALESCE(TITLE(x), 'Untitled') FROM t")
        self.validate_identity("SELECT UPPER(TITLE(x (TITLE 'Heading'))) FROM t")

        for sql, expected in (
            ("SELECT x (TITLE 'Display heading') FROM t", None),
            ("SELECT t.x (TITLE '') AS result FROM t", None),
            ("SELECT COALESCE(x, 1) (TITLE 'Heading') FROM t", None),
            ("SELECT (x + 1) (TITLE 'Heading') FROM t", None),
            (
                "SELECT x (TITLE 'Heading', FORMAT '999') FROM t",
                "SELECT x (FORMAT '999') (TITLE 'Heading') FROM t",
            ),
        ):
            with self.subTest(sql=sql):
                expression = self.validate_identity(sql, expected)
                self.assertEqual(expression, self.parse_one(expected or sql))
                self.assertEqual(expression, exp.Expr.load(expression.dump()))
                self.assertIsNone(expression.find(exp.Anonymous))
        title = self.parse_one("SELECT x (TITLE 'Not an alias') FROM t").expressions[0]
        self.assertEqual(title.output_name, "x")
        formatted = self.parse_one("SELECT x (FORMAT '999', TITLE 'Heading') FROM t").expressions[0]
        self.assertEqual(formatted.output_name, "x")
        self.assertEqual([column.name for column in title.find_all(exp.Column)], ["x"])
        self.assertTrue(
            annotate_types(self.parse_one("SELECT 1 (FORMAT '999', TITLE 'Heading')"))
            .expressions[0]
            .is_type(exp.DType.INT)
        )
        root = lineage(
            "result",
            "SELECT x (TITLE 'Heading') AS result FROM t",
            schema={"t": {"x": "INT"}},
            dialect="teradata",
        )
        self.assertEqual(root.downstream[0].name, "t.x")
        self.validate_identity('SELECT example_udf("TITLE")', 'SELECT EXAMPLE_UDF("TITLE")')
        for sql in (
            "SELECT x (TITLE)",
            "SELECT x (TITLE heading)",
            "SELECT x (TITLE :heading)",
            "SELECT x (TITLE 'a', TITLE 'b')",
            "SELECT x (TITLE 'a',)",
        ):
            with self.subTest(sql=sql), self.assertRaises(ParseError):
                self.parse_one(sql, error_level=ErrorLevel.RAISE)
        with self.assertRaises(UnsupportedError):
            title.sql("postgres", unsupported_level=ErrorLevel.RAISE)

    def test_named(self):
        from sqlglot.lineage import lineage
        from sqlglot.optimizer.annotate_types import annotate_types

        for sql, expected in (
            ("SELECT x (NAMED result) FROM t", "SELECT x AS result FROM t"),
            ('SELECT x (NAMED "result name") FROM t', 'SELECT x AS "result name" FROM t'),
            ("SELECT x (NAMED 'result name') FROM t", 'SELECT x AS "result name" FROM t'),
            ("SELECT (x + 1) (NAMED result) FROM t", "SELECT (x + 1) AS result FROM t"),
            (
                "SELECT x (NAMED result, TITLE 'Heading', FORMAT '999') FROM t",
                "SELECT x (FORMAT '999') (TITLE 'Heading') AS result FROM t",
            ),
            ("SELECT x FROM t WHERE x (NAMED result) > 1", None),
            ("SELECT x (NAMED inner_result) + 1 AS result FROM t", None),
            ("SELECT COALESCE(x (NAMED inner_result), 0) AS result FROM t", None),
        ):
            with self.subTest(sql=sql):
                expression = self.validate_identity(sql, expected)
                self.assertEqual(expression, self.parse_one(expected or sql))
                self.assertEqual(expression, exp.Expr.load(expression.dump()))
                self.assertIsNone(expression.find(exp.Anonymous))
        self.assertEqual(
            self.parse_one("SELECT x (NAMED result) FROM t").expressions[0].output_name, "result"
        )
        self.validate_all(
            "SELECT x (NAMED result) FROM t",
            write={
                "teradata": "SELECT x AS result FROM t",
                "postgres": "SELECT x AS result FROM t",
            },
        )
        self.assertTrue(
            annotate_types(self.parse_one("SELECT 1 (NAMED result)"))
            .expressions[0]
            .is_type(exp.DType.INT)
        )
        root = lineage(
            "result",
            "SELECT x (NAMED result) FROM t",
            schema={"t": {"x": "INT"}},
            dialect="teradata",
        )
        self.assertEqual(root.downstream[0].name, "t.x")
        self.validate_identity('SELECT example_udf("NAMED")', 'SELECT EXAMPLE_UDF("NAMED")')
        self.validate_identity("SELECT example_udf(NAMED(x))", "SELECT EXAMPLE_UDF(NAMED(x))")
        for sql in (
            "SELECT x (NAMED)",
            "SELECT x (NAMED 123)",
            "SELECT x (NAMED :alias)",
            "SELECT x (NAMED a, NAMED b)",
            "SELECT x (NAMED result,)",
            "SELECT x (NAMED result, 'TITLE' 'Heading')",
        ):
            with self.subTest(sql=sql), self.assertRaises(ParseError):
                self.parse_one(sql, error_level=ErrorLevel.RAISE)
        for sql in (
            "SELECT x FROM t WHERE x (NAMED result) > 1",
            "SELECT x (NAMED inner_result) + 1 AS result FROM t",
            "SELECT COALESCE(x (NAMED inner_result), 0) AS result FROM t",
        ):
            with self.subTest(sql=sql):
                expression = self.parse_one(sql)
                self.assertIsNotNone(expression.find(exp.TeradataNamed))
                with self.assertRaises(UnsupportedError):
                    expression.sql("postgres", unsupported_level=ErrorLevel.RAISE)

    def test_named_as_precedence(self):
        for sql in (
            "SELECT x (NAMED result) AS a FROM t",
            "SELECT x (NAMED result) a FROM t",
            'SELECT x (NAMED "result name") AS a FROM t',
        ):
            with self.subTest(sql=sql):
                expression = self.validate_identity(sql, "SELECT x AS a FROM t")
                self.assertIs(type(expression.expressions[0]), exp.Alias)
                self.assertIsNone(expression.find(exp.TeradataNamed))
                self.assertEqual(
                    expression.sql("postgres", unsupported_level=ErrorLevel.RAISE),
                    "SELECT x AS a FROM t",
                )
        self.validate_identity(
            "SELECT x (NAMED result) (TITLE 'Heading') (FORMAT '999') AS a FROM t",
            "SELECT x (TITLE 'Heading') (FORMAT '999') AS a FROM t",
        )
        self.validate_identity(
            "SELECT x (FORMAT '999') (NAMED result) (TITLE 'Heading') AS a FROM t",
            "SELECT x (FORMAT '999') (TITLE 'Heading') AS a FROM t",
        )
        self.validate_identity(
            "SELECT x (NAMED result /* note */) AS a FROM t",
            "SELECT x /* note */ AS a FROM t",
        )

    def test_named_attribute_order(self):
        from itertools import permutations

        from sqlglot.lineage import lineage
        from sqlglot.optimizer.annotate_types import annotate_types
        from sqlglot.optimizer.qualify import qualify

        schema = {"t": {"x": "INT"}}
        orders = (
            *permutations(("NAMED result", "TITLE 'Heading'")),
            *permutations(("NAMED result", "FORMAT '999'")),
            *permutations(("NAMED result", "TITLE 'Heading'", "FORMAT '999'")),
        )
        for attributes in orders:
            for combined in (False, True):
                phrases = (
                    f"({', '.join(attributes)})"
                    if combined
                    else " ".join(f"({attribute})" for attribute in attributes)
                )
                sql = f"SELECT x {phrases} FROM t WHERE result > 1"
                with self.subTest(sql=sql):
                    expression = self.parse_one(sql)
                    self.assertIs(type(expression.expressions[0]), exp.Alias)
                    self.assertIsNone(expression.find(exp.TeradataNamed))
                    self.assertEqual(expression.expressions[0].output_name, "result")
                    self.assertEqual(expression, exp.Expr.load(expression.dump()))
                    self.assertEqual(expression, self.parse_one(expression.sql("teradata")))
                    qualified = qualify(expression.copy(), dialect="teradata", schema=schema)
                    column = qualified.args["where"].find(exp.Column)
                    self.assertEqual((column.table, column.name), ("t", "x"))
                    self.assertTrue(
                        annotate_types(qualified, schema=schema, dialect="teradata")
                        .expressions[0]
                        .is_type(exp.DType.INT)
                    )
                    root = lineage("result", expression, schema=schema, dialect="teradata")
                    self.assertEqual(root.downstream[0].name, "t.x")

    def test_time(self):
        self.validate_identity("CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP WITH TIME ZONE)")

        self.validate_all(
            "CURRENT_TIMESTAMP",
            read={
                "teradata": "CURRENT_TIMESTAMP",
                "snowflake": "CURRENT_TIMESTAMP()",
            },
        )

        self.validate_all(
            "SELECT '2023-01-01' + INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' + INTERVAL '5' YEAR",
                "snowflake": "SELECT DATEADD(YEAR, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' - INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' - INTERVAL '5' YEAR",
                "snowflake": "SELECT DATEADD(YEAR, -5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' - INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' - INTERVAL '5' YEAR",
                "sqlite": "SELECT DATE_SUB('2023-01-01', 5, YEAR)",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + INTERVAL '5' YEAR",
            read={
                "teradata": "SELECT '2023-01-01' + INTERVAL '5' YEAR",
                "sqlite": "SELECT DATE_SUB('2023-01-01', -5, YEAR)",
            },
        )
        self.validate_all(
            "SELECT (90 * INTERVAL '1' DAY)",
            read={
                "teradata": "SELECT (90 * INTERVAL '1' DAY)",
                "snowflake": "SELECT INTERVAL '1' QUARTER",
            },
        )
        self.validate_all(
            "SELECT (7 * INTERVAL '1' DAY)",
            read={
                "teradata": "SELECT (7 * INTERVAL '1' DAY)",
                "snowflake": "SELECT INTERVAL '1' WEEK",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + (90 * INTERVAL '5' DAY)",
            read={
                "teradata": "SELECT '2023-01-01' + (90 * INTERVAL '5' DAY)",
                "snowflake": "SELECT DATEADD(QUARTER, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "SELECT '2023-01-01' + (7 * INTERVAL '5' DAY)",
            read={
                "teradata": "SELECT '2023-01-01' + (7 * INTERVAL '5' DAY)",
                "snowflake": "SELECT DATEADD(WEEK, 5, '2023-01-01')",
            },
        )
        self.validate_all(
            "CAST(TO_CHAR(x, 'Q') AS INT)",
            read={
                "teradata": "CAST(TO_CHAR(x, 'Q') AS INT)",
                "snowflake": "DATE_PART(QUARTER, x)",
                "bigquery": "EXTRACT(QUARTER FROM x)",
            },
        )
        self.validate_all(
            "EXTRACT(MONTH FROM x)",
            read={
                "teradata": "EXTRACT(MONTH FROM x)",
                "snowflake": "DATE_PART(MONTH, x)",
                "bigquery": "EXTRACT(MONTH FROM x)",
            },
        )
        self.validate_all(
            "CAST(TO_CHAR(x, 'Q') AS INT)",
            read={
                "snowflake": "quarter(x)",
                "teradata": "CAST(TO_CHAR(x, 'Q') AS INT)",
            },
        )

    def test_query_band(self):
        self.validate_identity("SET QUERY_BAND = 'app=myapp;' FOR SESSION")
        self.validate_identity("SET QUERY_BAND = 'app=myapp;user=john;' FOR TRANSACTION")
        self.validate_identity("SET QUERY_BAND = 'priority=high;' UPDATE FOR SESSION")
        self.validate_identity("SET QUERY_BAND = 'workload=batch;' UPDATE FOR TRANSACTION")
        self.validate_identity("SET QUERY_BAND = 'org=Finance;report=Fin123;' FOR SESSION")
        self.validate_identity("SET QUERY_BAND = NONE FOR SESSION")
        self.validate_identity("SET QUERY_BAND = NONE FOR SESSION VOLATILE")
        self.validate_identity("SET QUERY_BAND = 'priority=high;' UPDATE FOR SESSION VOLATILE")
        self.validate_identity(
            "SET QUERY_BAND = 'NONE' FOR SESSION"
        )  # quoted NONE should remain quoted
        self.validate_identity("SET QUERY_BAND = '' FOR SESSION")

    def test_window_aggregate_default_frame(self):
        self.validate_all(
            "SELECT SUM(x) OVER (PARTITION BY p ORDER BY d) FROM t",
            write={
                "teradata": "SELECT SUM(x) OVER (PARTITION BY p ORDER BY d) FROM t",
                "snowflake": "SELECT SUM(x) OVER (PARTITION BY p ORDER BY d NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
                "duckdb": "SELECT SUM(x) OVER (PARTITION BY p ORDER BY d NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
            },
        )
        self.validate_all(
            "SELECT COUNT(*) OVER (ORDER BY d) FROM t",
            write={
                "teradata": "SELECT COUNT(*) OVER (ORDER BY d) FROM t",
                "snowflake": "SELECT COUNT(*) OVER (ORDER BY d NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
            },
        )
        self.validate_all(
            "SELECT SUM(x) OVER (PARTITION BY p) FROM t",
            write={
                "teradata": "SELECT SUM(x) OVER (PARTITION BY p) FROM t",
                "snowflake": "SELECT SUM(x) OVER (PARTITION BY p) FROM t",
            },
        )
        self.validate_all(
            "SELECT RANK() OVER (ORDER BY d), ROW_NUMBER() OVER (ORDER BY d), LAG(x) OVER (ORDER BY d), FIRST_VALUE(x) OVER (ORDER BY d) FROM t",
            write={
                "snowflake": "SELECT RANK() OVER (ORDER BY d NULLS FIRST), ROW_NUMBER() OVER (ORDER BY d NULLS FIRST), LAG(x) OVER (ORDER BY d NULLS FIRST), FIRST_VALUE(x) OVER (ORDER BY d NULLS FIRST) FROM t",
            },
        )
        self.validate_identity(
            "SELECT SUM(x) OVER (ORDER BY d ROWS UNBOUNDED PRECEDING) FROM t",
            "SELECT SUM(x) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        )
        self.validate_identity(
            "SELECT AVG(x) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
            "SELECT AVG(x) OVER (ORDER BY d) FROM t",
        )

    def test_trunc(self):
        # Temporal vs. numeric disambiguation (and Unknown fallback)
        self.parse_one("TRUNC(CAST(x AS DATE), 'MONTH')").assert_is(exp.DateTrunc)
        self.parse_one("TRUNC(CAST(x AS TIMESTAMP), 'HH')").assert_is(exp.DateTrunc)
        self.parse_one("TRUNC(CURRENT_DATE, 'Q')").assert_is(exp.DateTrunc)
        self.parse_one("TRUNC(col, 'MM')").assert_is(exp.DateTrunc)
        self.validate_identity("TRUNC(3.14159, 2)").assert_is(exp.Trunc)
        self.validate_identity("TRUNC(3.14159)").assert_is(exp.Trunc)
        self.validate_identity("TRUNC(price, 0)").assert_is(exp.Trunc)
        self.validate_identity("TRUNC(foo, bar)").assert_is(exp.Anonymous)

        # Temporal TRUNC without an explicit format defaults to 'DD' (canonical DAY)
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE))",
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'DD')",
                "oracle": "TRUNC(CAST('2024-05-15' AS DATE), 'DD')",
                "duckdb": "DATE_TRUNC('DAY', CAST('2024-05-15' AS DATE))",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), DAY)",
                "tsql": "DATETRUNC(DAY, CAST('2024-05-15' AS DATE))",
            },
        )

        # Teradata format abbreviations normalize into canonical DATE_TRUNC units and translate back
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'MM')",
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'MONTH')",
                "duckdb": "DATE_TRUNC('MONTH', CAST('2024-05-15' AS DATE))",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), MONTH)",
                "snowflake": "DATE_TRUNC('MONTH', CAST('2024-05-15' AS DATE))",
                "postgres": "DATE_TRUNC('MONTH', CAST('2024-05-15' AS DATE))",
                "tsql": "DATETRUNC(MONTH, CAST('2024-05-15' AS DATE))",
            },
        )
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'Q')",
            read={
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), QUARTER)",
                "duckdb": "DATE_TRUNC('QUARTER', CAST('2024-05-15' AS DATE))",
                "snowflake": "DATE_TRUNC('QUARTER', CAST('2024-05-15' AS DATE))",
            },
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'Q')",
                "oracle": "TRUNC(CAST('2024-05-15' AS DATE), 'Q')",
                "duckdb": "DATE_TRUNC('QUARTER', CAST('2024-05-15' AS DATE))",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), QUARTER)",
                "tsql": "DATETRUNC(QUARTER, CAST('2024-05-15' AS DATE))",
            },
        )
        # Teradata 'DAY'/'DY'/'D' mean start of week, whereas 'DD' means daily truncation
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'DD')",
            read={
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), DAY)",
                "duckdb": "DATE_TRUNC('DAY', CAST('2024-05-15' AS DATE))",
                "snowflake": "DATE_TRUNC('DAY', CAST('2024-05-15' AS DATE))",
            },
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'DD')",
                "duckdb": "DATE_TRUNC('DAY', CAST('2024-05-15' AS DATE))",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), DAY)",
            },
        )
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'DAY')",
            read={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'D')",
                "oracle": "TRUNC(CAST('2024-05-15' AS DATE), 'DAY')",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), WEEK)",
            },
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'DAY')",
                "oracle": "TRUNC(CAST('2024-05-15' AS DATE), 'DAY')",
                "duckdb": "CAST(DATE_TRUNC('WEEK', CAST('2024-05-15' AS DATE) + INTERVAL '1' DAY) + INTERVAL '-1' DAY AS DATE)",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), WEEK)",
                "tsql": "DATETRUNC(WEEK, CAST('2024-05-15' AS DATE))",
                "snowflake": UnsupportedError,
            },
        )
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'IW')",
            read={
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), WEEK(MONDAY))",
                "duckdb": "DATE_TRUNC('WEEK', CAST('2024-05-15' AS DATE))",
                "snowflake": "DATE_TRUNC('WEEK', CAST('2024-05-15' AS DATE))",
            },
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'IW')",
                "duckdb": "DATE_TRUNC('WEEK', CAST('2024-05-15' AS DATE))",
                "bigquery": "DATE_TRUNC(CAST('2024-05-15' AS DATE), WEEK(MONDAY))",
                "tsql": "DATETRUNC(ISO_WEEK, CAST('2024-05-15' AS DATE))",
            },
        )
        self.validate_all(
            "TRUNC(CAST(x AS TIMESTAMP), 'HH')",
            write={
                "teradata": "TRUNC(CAST(x AS TIMESTAMP), 'HH')",
                "duckdb": "DATE_TRUNC('HOUR', CAST(x AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "TRUNC(CAST(x AS DATE), 'HH')",
            read={
                "snowflake": "DATE_TRUNC('HOUR', CAST(x AS DATE))",
            },
        )
        self.validate_identity("TRUNC(CAST(x AS TIMESTAMP), 'MI')")
        self.validate_identity("TRUNC(x, 'YYYY')", "TRUNC(x, 'YEAR')")
        self.validate_identity("TRUNC(x, 'IYYY')")
        self.validate_identity("TRUNC(x, 'CC')")
        self.validate_all(
            "TRUNC(CAST('2024-05-15' AS DATE), 'CC')",
            write={
                "teradata": "TRUNC(CAST('2024-05-15' AS DATE), 'CC')",
                "postgres": "DATE_TRUNC('CENTURY', CAST('2024-05-15' AS DATE))",
                "duckdb": UnsupportedError,
            },
        )
        # Preserve native calendar boundaries and reject unsupported translations.
        for unit in ("W", "WW"):
            self.validate_all(
                f"TRUNC(CAST('2024-05-15' AS DATE), '{unit}')",
                write={
                    "teradata": f"TRUNC(CAST('2024-05-15' AS DATE), '{unit}')",
                    "oracle": f"TRUNC(CAST('2024-05-15' AS DATE), '{unit}')",
                    "exasol": f"TRUNC(CAST('2024-05-15' AS DATE), '{unit}')",
                    "duckdb": UnsupportedError,
                    "bigquery": UnsupportedError,
                    "postgres": UnsupportedError,
                    "snowflake": UnsupportedError,
                    "tsql": UnsupportedError,
                },
            )

        # Unsupported DateTrunc units/args warn/raise in Teradata
        with self.assertRaises(UnsupportedError):
            parse_one("DATE_TRUNC(d, SECOND)", read="bigquery").sql(
                "teradata", unsupported_level=ErrorLevel.RAISE
            )
        with self.assertRaises(UnsupportedError):
            parse_one("DATE_TRUNC(d, MONTH, 'UTC')", read="bigquery").sql(
                "teradata", unsupported_level=ErrorLevel.RAISE
            )

        # Numeric TRUNC cross-dialect transpilation
        self.validate_all(
            "TRUNC(3.14159, 2)",
            read={
                "mysql": "TRUNCATE(3.14159, 2)",
                "oracle": "TRUNC(3.14159, 2)",
                "postgres": "TRUNC(3.14159, 2)",
            },
            write={
                "teradata": "TRUNC(3.14159, 2)",
                "mysql": "TRUNCATE(3.14159, 2)",
                "tsql": "ROUND(3.14159, 2, 1)",
                "spark": "CAST(3.14159 AS BIGINT)",
            },
        )
