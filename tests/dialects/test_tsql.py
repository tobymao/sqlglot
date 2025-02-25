from sqlglot import exp, parse, parse_one
from tests.dialects.test_dialect import Validator
from sqlglot.errors import ParseError, UnsupportedError
from sqlglot.optimizer.annotate_types import annotate_types


class TestTSQL(Validator):
    dialect = "tsql"

    def test_tsql(self):
        self.validate_identity(
            "with x as (select 1) select * from x union select * from x order by 1 limit 0",
            "WITH x AS (SELECT 1 AS [1]) SELECT TOP 0 * FROM (SELECT * FROM x UNION SELECT * FROM x) AS _l_0 ORDER BY 1",
        )

        # https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms187879(v=sql.105)?redirectedfrom=MSDN
        # tsql allows .. which means use the default schema
        self.validate_identity("SELECT * FROM a..b")

        self.validate_identity("CREATE view a.b.c", "CREATE VIEW b.c")
        self.validate_identity("DROP view a.b.c", "DROP VIEW b.c")
        self.validate_identity("ROUND(x, 1, 0)")
        self.validate_identity("EXEC MyProc @id=7, @name='Lochristi'", check_command_warning=True)
        self.validate_identity("SELECT TRIM('     test    ') AS Result")
        self.validate_identity("SELECT TRIM('.,! ' FROM '     #     test    .') AS Result")
        self.validate_identity("SELECT * FROM t TABLESAMPLE (10 PERCENT)")
        self.validate_identity("SELECT * FROM t TABLESAMPLE (20 ROWS)")
        self.validate_identity("SELECT * FROM t TABLESAMPLE (10 PERCENT) REPEATABLE (123)")
        self.validate_identity("SELECT CONCAT(column1, column2)")
        self.validate_identity("SELECT TestSpecialChar.Test# FROM TestSpecialChar")
        self.validate_identity("SELECT TestSpecialChar.Test@ FROM TestSpecialChar")
        self.validate_identity("SELECT TestSpecialChar.Test$ FROM TestSpecialChar")
        self.validate_identity("SELECT TestSpecialChar.Test_ FROM TestSpecialChar")
        self.validate_identity("SELECT TOP (2 + 1) 1")
        self.validate_identity("SELECT * FROM t WHERE NOT c", "SELECT * FROM t WHERE NOT c <> 0")
        self.validate_identity("1 AND true", "1 <> 0 AND (1 = 1)")
        self.validate_identity("CAST(x AS int) OR y", "CAST(x AS INTEGER) <> 0 OR y <> 0")
        self.validate_identity("TRUNCATE TABLE t1 WITH (PARTITIONS(1, 2 TO 5, 10 TO 20, 84))")
        self.validate_identity(
            "SELECT TOP 10 s.RECORDID, n.c.value('(/*:FORM_ROOT/*:SOME_TAG)[1]', 'float') AS SOME_TAG_VALUE FROM source_table.dbo.source_data AS s(nolock) CROSS APPLY FormContent.nodes('/*:FORM_ROOT') AS N(C)"
        )
        self.validate_identity(
            "CREATE CLUSTERED INDEX [IX_OfficeTagDetail_TagDetailID] ON [dbo].[OfficeTagDetail]([TagDetailID] ASC)"
        )
        self.validate_identity(
            "CREATE INDEX [x] ON [y]([z] ASC) WITH (allow_page_locks=on) ON X([y])"
        )
        self.validate_identity(
            "CREATE INDEX [x] ON [y]([z] ASC) WITH (allow_page_locks=on) ON PRIMARY"
        )
        self.validate_identity(
            "COPY INTO test_1 FROM 'path' WITH (FORMAT_NAME = test, FILE_TYPE = 'CSV', CREDENTIAL = (IDENTITY='Shared Access Signature', SECRET='token'), FIELDTERMINATOR = ';', ROWTERMINATOR = '0X0A', ENCODING = 'UTF8', DATEFORMAT = 'ymd', MAXERRORS = 10, ERRORFILE = 'errorsfolder', IDENTITY_INSERT = 'ON')"
        )
        self.validate_identity(
            'SELECT 1 AS "[x]"',
            "SELECT 1 AS [[x]]]",
        )
        self.assertEqual(
            annotate_types(self.validate_identity("SELECT 1 WHERE EXISTS(SELECT 1)")).sql("tsql"),
            "SELECT 1 WHERE EXISTS(SELECT 1)",
        )

        self.validate_all(
            "WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * INTO TEMP_NESTED_WITH FROM (SELECT * FROM C) AS temp",
            read={
                "snowflake": "CREATE TABLE TEMP_NESTED_WITH AS WITH C AS (WITH A AS (SELECT 2 AS value) SELECT * FROM A) SELECT * FROM C",
                "tsql": "WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * INTO TEMP_NESTED_WITH FROM (SELECT * FROM C) AS temp",
            },
            write={
                "snowflake": "CREATE TABLE TEMP_NESTED_WITH AS WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * FROM (SELECT * FROM C) AS temp",
            },
        )
        self.validate_all(
            "SELECT IIF(cond <> 0, 'True', 'False')",
            read={
                "spark": "SELECT IF(cond, 'True', 'False')",
                "sqlite": "SELECT IIF(cond, 'True', 'False')",
                "tsql": "SELECT IIF(cond <> 0, 'True', 'False')",
            },
        )
        self.validate_all(
            "SELECT TRIM(BOTH 'a' FROM a)",
            read={
                "mysql": "SELECT TRIM(BOTH 'a' FROM a)",
            },
            write={
                "mysql": "SELECT TRIM(BOTH 'a' FROM a)",
                "tsql": "SELECT TRIM(BOTH 'a' FROM a)",
            },
        )
        self.validate_all(
            "SELECT TIMEFROMPARTS(23, 59, 59, 0, 0)",
            read={
                "duckdb": "SELECT MAKE_TIME(23, 59, 59)",
                "mysql": "SELECT MAKETIME(23, 59, 59)",
                "postgres": "SELECT MAKE_TIME(23, 59, 59)",
                "snowflake": "SELECT TIME_FROM_PARTS(23, 59, 59)",
            },
            write={
                "tsql": "SELECT TIMEFROMPARTS(23, 59, 59, 0, 0)",
            },
        )
        self.validate_all(
            "SELECT DATETIMEFROMPARTS(2013, 4, 5, 12, 00, 00, 0)",
            read={
                # The nanoseconds are ignored since T-SQL doesn't support that precision
                "snowflake": "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)"
            },
            write={
                "duckdb": "SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00 + (0 / 1000.0))",
                "snowflake": "SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 0 * 1000000)",
                "tsql": "SELECT DATETIMEFROMPARTS(2013, 4, 5, 12, 00, 00, 0)",
            },
        )
        self.validate_all(
            "SELECT TOP 1 * FROM (SELECT x FROM t1 UNION ALL SELECT x FROM t2) AS _l_0",
            read={
                "": "SELECT x FROM t1 UNION ALL SELECT x FROM t2 LIMIT 1",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp",
            read={
                "duckdb": "CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT c FROM t",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp",
            write={
                "duckdb": "CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp",
                "postgres": "WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp",
                "oracle": "WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) temp",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT * INTO TEMP UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp",
            write={
                "duckdb": "CREATE TEMPORARY TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp",
                "postgres": "WITH t(c) AS (SELECT 1) SELECT * INTO TEMPORARY foo FROM (SELECT c AS c FROM t) AS temp",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp",
            write={
                "duckdb": "CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp",
            write={
                "duckdb": "CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp",
            },
        )
        self.validate_all(
            "WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y",
            read={
                "duckdb": "WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) SELECT 1 AS c UNION (SELECT c FROM t)",
            read={
                "duckdb": "SELECT 1 AS c UNION (WITH t(c) AS (SELECT 1) SELECT c FROM t)",
            },
        )
        self.validate_all(
            "WITH t(c) AS (SELECT 1) MERGE INTO x AS z USING (SELECT c AS c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
            read={
                "postgres": "MERGE INTO x AS z USING (WITH t(c) AS (SELECT 1) SELECT c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
            },
        )
        self.validate_all(
            "WITH t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT * FROM (SELECT SUM(n) AS s4 FROM t) AS subq",
            read={
                "duckdb": "SELECT * FROM (WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT SUM(n) AS s4 FROM t) AS subq",
            },
        )
        self.validate_all(
            "CREATE TABLE #mytemptable (a INTEGER)",
            read={
                "duckdb": "CREATE TEMPORARY TABLE mytemptable (a INT)",
            },
            write={
                "tsql": "CREATE TABLE #mytemptable (a INTEGER)",
                "snowflake": "CREATE TEMPORARY TABLE mytemptable (a INT)",
                "duckdb": "CREATE TEMPORARY TABLE mytemptable (a INT)",
                "oracle": "CREATE GLOBAL TEMPORARY TABLE mytemptable (a INT)",
                "hive": "CREATE TEMPORARY TABLE mytemptable (a INT)",
                "spark2": "CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET",
                "spark": "CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET",
                "databricks": "CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET",
            },
        )
        self.validate_all(
            "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))",
            write={
                "spark": "CREATE TEMPORARY TABLE mytemp (a INT, b CHAR(2), c TIMESTAMP, d FLOAT) USING PARQUET",
                "tsql": "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))",
            },
        )
        self.validate_all(
            """CREATE TABLE [dbo].[mytable](
                [email] [varchar](255) NOT NULL,
                CONSTRAINT [UN_t_mytable] UNIQUE NONCLUSTERED
                (
                    [email] ASC
                )
                )""",
            write={
                "hive": "CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)",
                "spark2": "CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)",
                "spark": "CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)",
                "databricks": "CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)",
            },
        )

        self.validate_all(
            "CREATE TABLE x ( A INTEGER NOT NULL, B INTEGER NULL )",
            write={
                "tsql": "CREATE TABLE x (A INTEGER NOT NULL, B INTEGER NULL)",
                "hive": "CREATE TABLE x (A INT NOT NULL, B INT)",
            },
        )

        self.validate_identity(
            'CREATE TABLE x (CONSTRAINT "pk_mytable" UNIQUE NONCLUSTERED (a DESC)) ON b (c)',
            "CREATE TABLE x (CONSTRAINT [pk_mytable] UNIQUE NONCLUSTERED (a DESC)) ON b (c)",
        )

        self.validate_all(
            """CREATE TABLE x ([zip_cd] VARCHAR(5) NULL NOT FOR REPLICATION, [zip_cd_mkey] VARCHAR(5) NOT NULL, CONSTRAINT [pk_mytable] PRIMARY KEY CLUSTERED ([zip_cd_mkey] ASC) WITH (PAD_INDEX=ON, STATISTICS_NORECOMPUTE=OFF) ON [INDEX]) ON [SECONDARY]""",
            write={
                "tsql": "CREATE TABLE x ([zip_cd] VARCHAR(5) NULL NOT FOR REPLICATION, [zip_cd_mkey] VARCHAR(5) NOT NULL, CONSTRAINT [pk_mytable] PRIMARY KEY CLUSTERED ([zip_cd_mkey] ASC) WITH (PAD_INDEX=ON, STATISTICS_NORECOMPUTE=OFF) ON [INDEX]) ON [SECONDARY]",
                "spark2": "CREATE TABLE x (`zip_cd` VARCHAR(5), `zip_cd_mkey` VARCHAR(5) NOT NULL, CONSTRAINT `pk_mytable` PRIMARY KEY (`zip_cd_mkey`))",
            },
        )

        self.validate_identity("CREATE TABLE x (A INTEGER NOT NULL, B INTEGER NULL)")

        self.validate_all(
            "CREATE TABLE x ( A INTEGER NOT NULL, B INTEGER NULL )",
            write={
                "hive": "CREATE TABLE x (A INT NOT NULL, B INT)",
            },
        )

        self.validate_identity(
            "CREATE TABLE tbl (a AS (x + 1) PERSISTED, b AS (y + 2), c AS (y / 3) PERSISTED NOT NULL)"
        )

        self.validate_identity(
            "CREATE TABLE [db].[tbl]([a] [int])",
            "CREATE TABLE [db].[tbl] ([a] INTEGER)",
        )

        self.validate_identity("SELECT a = 1", "SELECT 1 AS a").selects[0].assert_is(
            exp.Alias
        ).args["alias"].assert_is(exp.Identifier)

        self.validate_all(
            "IF OBJECT_ID('tempdb.dbo.#TempTableName', 'U') IS NOT NULL DROP TABLE #TempTableName",
            write={
                "tsql": "DROP TABLE IF EXISTS #TempTableName",
                "spark": "DROP TABLE IF EXISTS TempTableName",
            },
        )

        self.validate_identity(
            "MERGE INTO mytable WITH (HOLDLOCK) AS T USING mytable_merge AS S "
            "ON (T.user_id = S.user_id) WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (S.c1, S.c2)"
        )
        self.validate_identity("UPDATE STATISTICS x", check_command_warning=True)
        self.validate_identity("UPDATE x SET y = 1 OUTPUT x.a, x.b INTO @y FROM y")
        self.validate_identity("UPDATE x SET y = 1 OUTPUT x.a, x.b FROM y")
        self.validate_identity("INSERT INTO x (y) OUTPUT x.a, x.b INTO l SELECT * FROM z")
        self.validate_identity("INSERT INTO x (y) OUTPUT x.a, x.b SELECT * FROM z")
        self.validate_identity("DELETE x OUTPUT x.a FROM z")
        self.validate_identity("SELECT * FROM t WITH (TABLOCK, INDEX(myindex))")
        self.validate_identity("SELECT * FROM t WITH (NOWAIT)")
        self.validate_identity("SELECT CASE WHEN a > 1 THEN b END")
        self.validate_identity("SELECT * FROM taxi ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY")
        self.validate_identity("END")
        self.validate_identity("@x")
        self.validate_identity("#x")
        self.validate_identity("PRINT @TestVariable", check_command_warning=True)
        self.validate_identity("SELECT Employee_ID, Department_ID FROM @MyTableVar")
        self.validate_identity("INSERT INTO @TestTable VALUES (1, 'Value1', 12, 20)")
        self.validate_identity("SELECT * FROM #foo")
        self.validate_identity("SELECT * FROM ##foo")
        self.validate_identity("SELECT a = 1", "SELECT 1 AS a")
        self.validate_identity(
            "DECLARE @TestVariable AS VARCHAR(100) = 'Save Our Planet'",
        )
        self.validate_identity(
            "SELECT a = 1 UNION ALL SELECT a = b", "SELECT 1 AS a UNION ALL SELECT b AS a"
        )
        self.validate_identity(
            "SELECT x FROM @MyTableVar AS m JOIN Employee ON m.EmployeeID = Employee.EmployeeID"
        )
        self.validate_identity(
            "SELECT DISTINCT DepartmentName, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY BaseRate) OVER (PARTITION BY DepartmentName) AS MedianCont FROM dbo.DimEmployee"
        )
        self.validate_identity(
            'SELECT "x"."y" FROM foo',
            "SELECT [x].[y] FROM foo",
        )

        self.validate_all(
            "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 2 ROWS",
            read={
                "postgres": "SELECT * FROM t OFFSET 2",
            },
            write={
                "postgres": "SELECT * FROM t ORDER BY (SELECT NULL) NULLS FIRST OFFSET 2",
                "tsql": "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 2 ROWS",
            },
        )
        self.validate_all(
            "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
            read={
                "duckdb": "SELECT * FROM t LIMIT 10 OFFSET 5",
                "sqlite": "SELECT * FROM t LIMIT 5, 10",
                "tsql": "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
            },
            write={
                "duckdb": "SELECT * FROM t ORDER BY (SELECT NULL) NULLS FIRST LIMIT 10 OFFSET 5",
                "sqlite": "SELECT * FROM t ORDER BY (SELECT NULL) LIMIT 10 OFFSET 5",
            },
        )
        self.validate_all(
            "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
            write={
                "tsql": "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
                "spark": "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            },
        )
        self.validate_all(
            "CONVERT(INT, CONVERT(NUMERIC, '444.75'))",
            write={
                "mysql": "CAST(CAST('444.75' AS DECIMAL) AS SIGNED)",
                "tsql": "CONVERT(INTEGER, CONVERT(NUMERIC, '444.75'))",
            },
        )
        self.validate_all(
            "STRING_AGG(x, y) WITHIN GROUP (ORDER BY z DESC)",
            write={
                "tsql": "STRING_AGG(x, y) WITHIN GROUP (ORDER BY z DESC)",
                "mysql": "GROUP_CONCAT(x ORDER BY z DESC SEPARATOR y)",
                "sqlite": "GROUP_CONCAT(x, y)",
                "postgres": "STRING_AGG(x, y ORDER BY z DESC NULLS LAST)",
            },
        )
        self.validate_all(
            "STRING_AGG(x, '|') WITHIN GROUP (ORDER BY z ASC)",
            write={
                "tsql": "STRING_AGG(x, '|') WITHIN GROUP (ORDER BY z ASC)",
                "mysql": "GROUP_CONCAT(x ORDER BY z ASC SEPARATOR '|')",
                "sqlite": "GROUP_CONCAT(x, '|')",
                "postgres": "STRING_AGG(x, '|' ORDER BY z ASC NULLS FIRST)",
            },
        )
        self.validate_all(
            "STRING_AGG(x, '|')",
            write={
                "tsql": "STRING_AGG(x, '|')",
                "mysql": "GROUP_CONCAT(x SEPARATOR '|')",
                "sqlite": "GROUP_CONCAT(x, '|')",
                "postgres": "STRING_AGG(x, '|')",
            },
        )
        self.validate_all(
            "HASHBYTES('SHA1', x)",
            read={
                "snowflake": "SHA1(x)",
                "spark": "SHA(x)",
            },
            write={
                "snowflake": "SHA1(x)",
                "spark": "SHA(x)",
                "tsql": "HASHBYTES('SHA1', x)",
            },
        )
        self.validate_all(
            "HASHBYTES('SHA2_256', x)",
            read={
                "spark": "SHA2(x, 256)",
            },
            write={
                "tsql": "HASHBYTES('SHA2_256', x)",
                "spark": "SHA2(x, 256)",
            },
        )
        self.validate_all(
            "HASHBYTES('SHA2_512', x)",
            read={
                "spark": "SHA2(x, 512)",
            },
            write={
                "tsql": "HASHBYTES('SHA2_512', x)",
                "spark": "SHA2(x, 512)",
            },
        )
        self.validate_all(
            "HASHBYTES('MD5', 'x')",
            read={
                "spark": "MD5('x')",
            },
            write={
                "tsql": "HASHBYTES('MD5', 'x')",
                "spark": "MD5('x')",
            },
        )
        self.validate_identity("HASHBYTES('MD2', 'x')")
        self.validate_identity("LOG(n)")
        self.validate_identity("LOG(n, b)")

        self.validate_all(
            "STDEV(x)",
            read={
                "": "STDDEV(x)",
            },
            write={
                "": "STDDEV(x)",
                "tsql": "STDEV(x)",
            },
        )

        # Check that TRUE and FALSE dont get expanded to (1=1) or (1=0) when used in a VALUES expression
        self.validate_identity(
            "SELECT val FROM (VALUES ((TRUE), (FALSE), (NULL))) AS t(val)",
            write_sql="SELECT val FROM (VALUES ((1), (0), (NULL))) AS t(val)",
        )
        self.validate_identity("'a' + 'b'")
        self.validate_identity(
            "'a' || 'b'",
            "'a' + 'b'",
        )

        self.validate_identity(
            "CREATE TABLE db.t1 (a INTEGER, b VARCHAR(50), CONSTRAINT c PRIMARY KEY (a DESC))",
        )
        self.validate_identity(
            "CREATE TABLE db.t1 (a INTEGER, b INTEGER, CONSTRAINT c PRIMARY KEY (a DESC, b))"
        )

        self.validate_all(
            "SCHEMA_NAME(id)",
            write={
                "sqlite": "'main'",
                "mysql": "SCHEMA()",
                "postgres": "CURRENT_SCHEMA",
                "tsql": "SCHEMA_NAME(id)",
            },
        )

        with self.assertRaises(ParseError):
            parse_one("SELECT begin", read="tsql")

        self.validate_identity("CREATE PROCEDURE test(@v1 INTEGER = 1, @v2 CHAR(1) = 'c')")
        self.validate_identity("DECLARE @v1 AS INTEGER = 1, @v2 AS CHAR(1) = 'c'")

        for output in ("OUT", "OUTPUT", "READ_ONLY"):
            self.validate_identity(
                f"CREATE PROCEDURE test(@v1 INTEGER = 1 {output}, @v2 CHAR(1) {output})"
            )

        self.validate_identity(
            "CREATE PROCEDURE test(@v1 AS INTEGER = 1, @v2 AS CHAR(1) = 'c')",
            "CREATE PROCEDURE test(@v1 INTEGER = 1, @v2 CHAR(1) = 'c')",
        )

    def test_option(self):
        possible_options = [
            "HASH GROUP",
            "ORDER GROUP",
            "CONCAT UNION",
            "HASH UNION",
            "MERGE UNION",
            "LOOP JOIN",
            "MERGE JOIN",
            "HASH JOIN",
            "DISABLE_OPTIMIZED_PLAN_FORCING",
            "EXPAND VIEWS",
            "FAST 15",
            "FORCE ORDER",
            "FORCE EXTERNALPUSHDOWN",
            "DISABLE EXTERNALPUSHDOWN",
            "FORCE SCALEOUTEXECUTION",
            "DISABLE SCALEOUTEXECUTION",
            "IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX",
            "KEEP PLAN",
            "KEEPFIXED PLAN",
            "MAX_GRANT_PERCENT = 5",
            "MIN_GRANT_PERCENT = 10",
            "MAXDOP 13",
            "MAXRECURSION 8",
            "NO_PERFORMANCE_SPOOL",
            "OPTIMIZE FOR UNKNOWN",
            "PARAMETERIZATION SIMPLE",
            "PARAMETERIZATION FORCED",
            "QUERYTRACEON 99",
            "RECOMPILE",
            "ROBUST PLAN",
            "USE PLAN N'<xml_plan>'",
            "LABEL = 'MyLabel'",
        ]

        possible_statements = [
            # These should be un-commented once support for the OPTION clause is added for DELETE, MERGE and UPDATE
            # "DELETE FROM Table1",
            # "MERGE INTO Locations AS T USING locations_stage AS S ON T.LocationID = S.LocationID WHEN MATCHED THEN UPDATE SET LocationName = S.LocationName",
            # "UPDATE Customers SET ContactName = 'Alfred Schmidt', City = 'Frankfurt' WHERE CustomerID = 1",
            "SELECT * FROM Table1",
            "SELECT * FROM Table1 WHERE id = 2",
        ]

        for statement in possible_statements:
            for option in possible_options:
                query = f"{statement} OPTION({option})"
                result = self.validate_identity(query)
                options = result.args.get("options")
                self.assertIsInstance(options, list, f"When parsing query {query}")
                is_query_options = map(lambda o: isinstance(o, exp.QueryOption), options)
                self.assertTrue(all(is_query_options), f"When parsing query {query}")

            self.validate_identity(
                f"{statement} OPTION(RECOMPILE, USE PLAN N'<xml_plan>', MAX_GRANT_PERCENT = 5)"
            )

        raising_queries = [
            # Missing parentheses
            "SELECT * FROM Table1 OPTION HASH GROUP",
            # Must be followed by 'PLAN"
            "SELECT * FROM Table1 OPTION(KEEPFIXED)",
            # Missing commas
            "SELECT * FROM Table1 OPTION(HASH GROUP HASH GROUP)",
        ]
        for query in raising_queries:
            with self.assertRaises(ParseError, msg=f"When running '{query}'"):
                self.parse_one(query)

    def test_types(self):
        self.validate_identity("CAST(x AS XML)")
        self.validate_identity("CAST(x AS UNIQUEIDENTIFIER)")
        self.validate_identity("CAST(x AS MONEY)")
        self.validate_identity("CAST(x AS SMALLMONEY)")
        self.validate_identity("CAST(x AS IMAGE)")
        self.validate_identity("CAST(x AS SQL_VARIANT)")
        self.validate_identity("CAST(x AS BIT)")

        self.validate_all(
            "CAST(x AS DATETIME2(6))",
            write={
                "hive": "CAST(x AS TIMESTAMP)",
            },
        )
        self.validate_all(
            "CAST(x AS ROWVERSION)",
            read={
                "tsql": "CAST(x AS TIMESTAMP)",
            },
            write={
                "tsql": "CAST(x AS ROWVERSION)",
                "hive": "CAST(x AS BINARY)",
            },
        )

        for temporal_type in ("SMALLDATETIME", "DATETIME", "DATETIME2"):
            self.validate_all(
                f"CAST(x AS {temporal_type})",
                read={
                    "": f"CAST(x AS {temporal_type})",
                },
                write={
                    "mysql": "CAST(x AS DATETIME)",
                    "duckdb": "CAST(x AS TIMESTAMP)",
                    "tsql": f"CAST(x AS {temporal_type})",
                },
            )

    def test_types_ints(self):
        self.validate_all(
            "CAST(X AS INT)",
            write={
                "hive": "CAST(X AS INT)",
                "spark2": "CAST(X AS INT)",
                "spark": "CAST(X AS INT)",
                "tsql": "CAST(X AS INTEGER)",
            },
        )

        self.validate_all(
            "CAST(X AS BIGINT)",
            write={
                "hive": "CAST(X AS BIGINT)",
                "spark2": "CAST(X AS BIGINT)",
                "spark": "CAST(X AS BIGINT)",
                "tsql": "CAST(X AS BIGINT)",
            },
        )

        self.validate_all(
            "CAST(X AS SMALLINT)",
            write={
                "hive": "CAST(X AS SMALLINT)",
                "spark2": "CAST(X AS SMALLINT)",
                "spark": "CAST(X AS SMALLINT)",
                "tsql": "CAST(X AS SMALLINT)",
            },
        )

        self.validate_all(
            "CAST(X AS TINYINT)",
            read={
                "duckdb": "CAST(X AS UTINYINT)",
            },
            write={
                "duckdb": "CAST(X AS UTINYINT)",
                "hive": "CAST(X AS SMALLINT)",
                "spark2": "CAST(X AS SMALLINT)",
                "spark": "CAST(X AS SMALLINT)",
                "tsql": "CAST(X AS TINYINT)",
            },
        )

    def test_types_decimals(self):
        self.validate_all(
            "CAST(x as FLOAT)",
            write={
                "spark": "CAST(x AS FLOAT)",
                "tsql": "CAST(x AS FLOAT)",
            },
        )

        self.validate_all(
            "CAST(x as FLOAT(32))",
            write={"tsql": "CAST(x AS FLOAT(32))", "hive": "CAST(x AS FLOAT)"},
        )

        self.validate_all(
            "CAST(x as FLOAT(64))",
            write={"tsql": "CAST(x AS FLOAT(64))", "spark": "CAST(x AS DOUBLE)"},
        )

        self.validate_all(
            "CAST(x as FLOAT(6))", write={"tsql": "CAST(x AS FLOAT(6))", "hive": "CAST(x AS FLOAT)"}
        )

        self.validate_all(
            "CAST(x as FLOAT(36))",
            write={"tsql": "CAST(x AS FLOAT(36))", "hive": "CAST(x AS DOUBLE)"},
        )

        self.validate_all(
            "CAST(x as FLOAT(99))",
            write={"tsql": "CAST(x AS FLOAT(99))", "hive": "CAST(x AS DOUBLE)"},
        )

        self.validate_all(
            "CAST(x as DOUBLE)",
            write={
                "spark": "CAST(x AS DOUBLE)",
                "tsql": "CAST(x AS FLOAT)",
            },
        )

        self.validate_all(
            "CAST(x as DECIMAL(15, 4))",
            write={
                "spark": "CAST(x AS DECIMAL(15, 4))",
                "tsql": "CAST(x AS NUMERIC(15, 4))",
            },
        )

        self.validate_all(
            "CAST(x as NUMERIC(13,3))",
            write={
                "spark": "CAST(x AS DECIMAL(13, 3))",
                "tsql": "CAST(x AS NUMERIC(13, 3))",
            },
        )

        self.validate_all(
            "CAST(x as MONEY)",
            write={
                "spark": "CAST(x AS DECIMAL(15, 4))",
                "tsql": "CAST(x AS MONEY)",
            },
        )

        self.validate_all(
            "CAST(x as SMALLMONEY)",
            write={
                "spark": "CAST(x AS DECIMAL(6, 4))",
                "tsql": "CAST(x AS SMALLMONEY)",
            },
        )

        self.validate_all(
            "CAST(x as REAL)",
            write={
                "spark": "CAST(x AS FLOAT)",
                "tsql": "CAST(x AS FLOAT)",
            },
        )

    def test_types_string(self):
        self.validate_all(
            "CAST(x as CHAR(1))",
            write={
                "spark": "CAST(x AS CHAR(1))",
                "tsql": "CAST(x AS CHAR(1))",
            },
        )

        self.validate_all(
            "CAST(x as VARCHAR(2))",
            write={
                "spark": "CAST(x AS VARCHAR(2))",
                "tsql": "CAST(x AS VARCHAR(2))",
            },
        )

        self.validate_all(
            "CAST(x as NCHAR(1))",
            write={
                "spark": "CAST(x AS CHAR(1))",
                "tsql": "CAST(x AS NCHAR(1))",
            },
        )

        self.validate_all(
            "CAST(x as NVARCHAR(2))",
            write={
                "spark": "CAST(x AS VARCHAR(2))",
                "tsql": "CAST(x AS NVARCHAR(2))",
            },
        )

        self.validate_all(
            "CAST(x as UNIQUEIDENTIFIER)",
            write={
                "spark": "CAST(x AS STRING)",
                "tsql": "CAST(x AS UNIQUEIDENTIFIER)",
            },
        )

    def test_types_date(self):
        self.validate_all(
            "CAST(x as DATE)",
            write={
                "spark": "CAST(x AS DATE)",
                "tsql": "CAST(x AS DATE)",
            },
        )

        self.validate_all(
            "CAST(x as DATE)",
            write={
                "spark": "CAST(x AS DATE)",
                "tsql": "CAST(x AS DATE)",
            },
        )

        self.validate_all(
            "CAST(x as TIME(4))",
            write={
                "spark": "CAST(x AS TIMESTAMP)",
                "tsql": "CAST(x AS TIME(4))",
            },
        )

        self.validate_all(
            "CAST(x as DATETIME2)",
            write={
                "spark": "CAST(x AS TIMESTAMP)",
                "tsql": "CAST(x AS DATETIME2)",
            },
        )

        self.validate_all(
            "CAST(x as DATETIMEOFFSET)",
            write={
                "spark": "CAST(x AS TIMESTAMP)",
                "tsql": "CAST(x AS DATETIMEOFFSET)",
            },
        )

        self.validate_all(
            "CREATE TABLE t (col1 DATETIME2(2))",
            read={
                "snowflake": "CREATE TABLE t (col1 TIMESTAMP_NTZ(2))",
            },
            write={
                "tsql": "CREATE TABLE t (col1 DATETIME2(2))",
            },
        )

    def test_types_bin(self):
        self.validate_all(
            "CAST(x as BIT)",
            write={
                "spark": "CAST(x AS BOOLEAN)",
                "tsql": "CAST(x AS BIT)",
            },
        )

        self.validate_all(
            "CAST(x as VARBINARY)",
            write={
                "spark": "CAST(x AS BINARY)",
                "tsql": "CAST(x AS VARBINARY)",
            },
        )

        self.validate_all(
            "CAST(x AS BOOLEAN)",
            write={"tsql": "CAST(x AS BIT)"},
        )

        self.validate_all("a = TRUE", write={"tsql": "a = 1"})

        self.validate_all("a != FALSE", write={"tsql": "a <> 0"})

        self.validate_all("a IS TRUE", write={"tsql": "a = 1"})

        self.validate_all("a IS NOT FALSE", write={"tsql": "NOT a = 0"})

        self.validate_all(
            "CASE WHEN a IN (TRUE) THEN 'y' ELSE 'n' END",
            write={"tsql": "CASE WHEN a IN (1) THEN 'y' ELSE 'n' END"},
        )

        self.validate_all(
            "CASE WHEN a NOT IN (FALSE) THEN 'y' ELSE 'n' END",
            write={"tsql": "CASE WHEN NOT a IN (0) THEN 'y' ELSE 'n' END"},
        )

        self.validate_all("SELECT TRUE, FALSE", write={"tsql": "SELECT 1, 0"})

        self.validate_all("SELECT TRUE AS a, FALSE AS b", write={"tsql": "SELECT 1 AS a, 0 AS b"})

        self.validate_all(
            "SELECT 1 FROM a WHERE TRUE", write={"tsql": "SELECT 1 FROM a WHERE (1 = 1)"}
        )

        self.validate_all(
            "CASE WHEN TRUE THEN 'y' WHEN FALSE THEN 'n' ELSE NULL END",
            write={"tsql": "CASE WHEN (1 = 1) THEN 'y' WHEN (1 = 0) THEN 'n' ELSE NULL END"},
        )

    def test_ddl(self):
        for view_attr in ("ENCRYPTION", "SCHEMABINDING", "VIEW_METADATA"):
            self.validate_identity(f"CREATE VIEW a.b WITH {view_attr} AS SELECT * FROM x")

        self.validate_identity("ALTER TABLE dbo.DocExe DROP CONSTRAINT FK_Column_B").assert_is(
            exp.Alter
        ).args["actions"][0].assert_is(exp.Drop)

        for clustered_keyword in ("CLUSTERED", "NONCLUSTERED"):
            self.validate_identity(
                'CREATE TABLE "dbo"."benchmark" ('
                '"name" CHAR(7) NOT NULL, '
                '"internal_id" VARCHAR(10) NOT NULL, '
                f'UNIQUE {clustered_keyword} ("internal_id" ASC))',
                "CREATE TABLE [dbo].[benchmark] ("
                "[name] CHAR(7) NOT NULL, "
                "[internal_id] VARCHAR(10) NOT NULL, "
                f"UNIQUE {clustered_keyword} ([internal_id] ASC))",
            )

        self.validate_identity("CREATE SCHEMA testSchema")
        self.validate_identity("CREATE VIEW t AS WITH cte AS (SELECT 1 AS c) SELECT c FROM cte")
        self.validate_identity(
            "ALTER TABLE tbl SET SYSTEM_VERSIONING=ON(HISTORY_TABLE=db.tbl, DATA_CONSISTENCY_CHECK=OFF, HISTORY_RETENTION_PERIOD=5 DAYS)"
        )
        self.validate_identity(
            "ALTER TABLE tbl SET SYSTEM_VERSIONING=ON(HISTORY_TABLE=db.tbl, HISTORY_RETENTION_PERIOD=INFINITE)"
        )
        self.validate_identity("ALTER TABLE tbl SET SYSTEM_VERSIONING=OFF")
        self.validate_identity("ALTER TABLE tbl SET FILESTREAM_ON = 'test'")
        self.validate_identity(
            "ALTER TABLE tbl SET DATA_DELETION=ON(FILTER_COLUMN=col, RETENTION_PERIOD=5 MONTHS)"
        )
        self.validate_identity("ALTER TABLE tbl SET DATA_DELETION=ON")
        self.validate_identity("ALTER TABLE tbl SET DATA_DELETION=OFF")

        self.validate_identity("ALTER VIEW v AS SELECT a, b, c, d FROM foo")
        self.validate_identity("ALTER VIEW v AS SELECT * FROM foo WHERE c > 100")
        self.validate_identity(
            "ALTER VIEW v WITH SCHEMABINDING AS SELECT * FROM foo WHERE c > 100",
            check_command_warning=True,
        )
        self.validate_identity(
            "ALTER VIEW v WITH ENCRYPTION AS SELECT * FROM foo WHERE c > 100",
            check_command_warning=True,
        )
        self.validate_identity(
            "ALTER VIEW v WITH VIEW_METADATA AS SELECT * FROM foo WHERE c > 100",
            check_command_warning=True,
        )
        self.validate_identity(
            "CREATE PROCEDURE foo AS BEGIN DELETE FROM bla WHERE foo < CURRENT_TIMESTAMP - 7 END",
            "CREATE PROCEDURE foo AS BEGIN DELETE FROM bla WHERE foo < GETDATE() - 7 END",
        )

        self.validate_all(
            "CREATE TABLE [#temptest] (name INTEGER)",
            read={
                "duckdb": "CREATE TEMPORARY TABLE 'temptest' (name INTEGER)",
                "tsql": "CREATE TABLE [#temptest] (name INTEGER)",
            },
        )
        self.validate_all(
            "CREATE TABLE tbl (id INTEGER IDENTITY PRIMARY KEY)",
            read={
                "mysql": "CREATE TABLE tbl (id INT AUTO_INCREMENT PRIMARY KEY)",
                "tsql": "CREATE TABLE tbl (id INTEGER IDENTITY PRIMARY KEY)",
            },
        )
        self.validate_all(
            "CREATE TABLE tbl (id INTEGER NOT NULL IDENTITY(10, 1) PRIMARY KEY)",
            read={
                "postgres": "CREATE TABLE tbl (id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10) PRIMARY KEY)",
                "tsql": "CREATE TABLE tbl (id INTEGER NOT NULL IDENTITY(10, 1) PRIMARY KEY)",
            },
            write={
                "databricks": "CREATE TABLE tbl (id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)",
                "postgres": "CREATE TABLE tbl (id INT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)",
            },
        )
        self.validate_all(
            "CREATE TABLE x (a UNIQUEIDENTIFIER, b VARBINARY)",
            write={
                "duckdb": "CREATE TABLE x (a UUID, b BLOB)",
                "presto": "CREATE TABLE x (a UUID, b VARBINARY)",
                "spark": "CREATE TABLE x (a STRING, b BINARY)",
                "postgres": "CREATE TABLE x (a UUID, b BYTEA)",
            },
        )
        self.validate_all(
            "SELECT * INTO foo.bar.baz FROM (SELECT * FROM a.b.c) AS temp",
            read={
                "": "CREATE TABLE foo.bar.baz AS SELECT * FROM a.b.c",
                "duckdb": "CREATE TABLE foo.bar.baz AS (SELECT * FROM a.b.c)",
            },
        )
        self.validate_all(
            "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('db.tbl') AND name = 'idx') EXEC('CREATE INDEX idx ON db.tbl')",
            read={
                "": "CREATE INDEX IF NOT EXISTS idx ON db.tbl",
            },
        )

        self.validate_all(
            "IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'foo') EXEC('CREATE SCHEMA foo')",
            read={
                "": "CREATE SCHEMA IF NOT EXISTS foo",
            },
        )
        self.validate_all(
            "IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'baz' AND table_schema = 'bar' AND table_catalog = 'foo') EXEC('CREATE TABLE foo.bar.baz (a INTEGER)')",
            read={
                "": "CREATE TABLE IF NOT EXISTS foo.bar.baz (a INTEGER)",
            },
        )
        self.validate_all(
            "IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'baz' AND table_schema = 'bar' AND table_catalog = 'foo') EXEC('SELECT * INTO foo.bar.baz FROM (SELECT ''2020'' AS z FROM a.b.c) AS temp')",
            read={
                "": "CREATE TABLE IF NOT EXISTS foo.bar.baz AS SELECT '2020' AS z FROM a.b.c",
            },
        )
        self.validate_all(
            "IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'baz' AND table_schema = 'bar' AND table_catalog = 'foo') EXEC('WITH cte1 AS (SELECT 1 AS col_a), cte2 AS (SELECT 1 AS col_b) SELECT * INTO foo.bar.baz FROM (SELECT col_a FROM cte1 UNION ALL SELECT col_b FROM cte2) AS temp')",
            read={
                "": "CREATE TABLE IF NOT EXISTS foo.bar.baz AS WITH cte1 AS (SELECT 1 AS col_a), cte2 AS (SELECT 1 AS col_b) SELECT col_a FROM cte1 UNION ALL SELECT col_b FROM cte2"
            },
        )
        self.validate_all(
            "CREATE OR ALTER VIEW a.b AS SELECT 1",
            read={
                "": "CREATE OR REPLACE VIEW a.b AS SELECT 1",
            },
            write={
                "tsql": "CREATE OR ALTER VIEW a.b AS SELECT 1",
            },
        )
        self.validate_all(
            "ALTER TABLE a ADD b INTEGER, c INTEGER",
            read={
                "": "ALTER TABLE a ADD COLUMN b INT, ADD COLUMN c INT",
            },
            write={
                "": "ALTER TABLE a ADD COLUMN b INT, ADD COLUMN c INT",
                "tsql": "ALTER TABLE a ADD b INTEGER, c INTEGER",
            },
        )
        self.validate_all(
            "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))",
            write={
                "spark": "CREATE TEMPORARY TABLE mytemp (a INT, b CHAR(2), c TIMESTAMP, d FLOAT) USING PARQUET",
                "tsql": "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))",
            },
        )

        for colstore in ("NONCLUSTERED COLUMNSTORE", "CLUSTERED COLUMNSTORE"):
            self.validate_identity(f"CREATE {colstore} INDEX index_name ON foo.bar")

        self.validate_identity(
            "CREATE COLUMNSTORE INDEX index_name ON foo.bar",
            "CREATE NONCLUSTERED COLUMNSTORE INDEX index_name ON foo.bar",
        )

    def test_insert_cte(self):
        self.validate_all(
            "INSERT INTO foo.bar WITH cte AS (SELECT 1 AS one) SELECT * FROM cte",
            write={"tsql": "WITH cte AS (SELECT 1 AS one) INSERT INTO foo.bar SELECT * FROM cte"},
        )

    def test_transaction(self):
        self.validate_identity("BEGIN TRANSACTION")
        self.validate_all("BEGIN TRAN", write={"tsql": "BEGIN TRANSACTION"})
        self.validate_identity("BEGIN TRANSACTION transaction_name")
        self.validate_identity("BEGIN TRANSACTION @tran_name_variable")
        self.validate_identity("BEGIN TRANSACTION transaction_name WITH MARK 'description'")

    def test_commit(self):
        self.validate_all("COMMIT", write={"tsql": "COMMIT TRANSACTION"})
        self.validate_all("COMMIT TRAN", write={"tsql": "COMMIT TRANSACTION"})
        self.validate_identity("COMMIT TRANSACTION")
        self.validate_identity("COMMIT TRANSACTION transaction_name")
        self.validate_identity("COMMIT TRANSACTION @tran_name_variable")

        self.validate_identity(
            "COMMIT TRANSACTION @tran_name_variable WITH (DELAYED_DURABILITY = ON)"
        )
        self.validate_identity(
            "COMMIT TRANSACTION transaction_name WITH (DELAYED_DURABILITY = OFF)"
        )

    def test_rollback(self):
        self.validate_all("ROLLBACK", write={"tsql": "ROLLBACK TRANSACTION"})
        self.validate_all("ROLLBACK TRAN", write={"tsql": "ROLLBACK TRANSACTION"})
        self.validate_identity("ROLLBACK TRANSACTION")
        self.validate_identity("ROLLBACK TRANSACTION transaction_name")
        self.validate_identity("ROLLBACK TRANSACTION @tran_name_variable")

    def test_udf(self):
        self.validate_identity(
            "DECLARE @DWH_DateCreated AS DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104)",
        )
        self.validate_identity(
            "CREATE PROCEDURE foo @a INTEGER, @b INTEGER AS SELECT @a = SUM(bla) FROM baz AS bar"
        )
        self.validate_identity(
            "CREATE PROC foo @ID INTEGER, @AGE INTEGER AS SELECT DB_NAME(@ID) AS ThatDB"
        )
        self.validate_identity("CREATE PROC foo AS SELECT BAR() AS baz")
        self.validate_identity("CREATE PROCEDURE foo AS SELECT BAR() AS baz")

        self.validate_identity("CREATE PROCEDURE foo WITH ENCRYPTION AS SELECT 1")
        self.validate_identity("CREATE PROCEDURE foo WITH RECOMPILE AS SELECT 1")
        self.validate_identity("CREATE PROCEDURE foo WITH SCHEMABINDING AS SELECT 1")
        self.validate_identity("CREATE PROCEDURE foo WITH NATIVE_COMPILATION AS SELECT 1")
        self.validate_identity("CREATE PROCEDURE foo WITH EXECUTE AS OWNER AS SELECT 1")
        self.validate_identity("CREATE PROCEDURE foo WITH EXECUTE AS 'username' AS SELECT 1")
        self.validate_identity(
            "CREATE PROCEDURE foo WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION AS SELECT 1"
        )

        self.validate_identity("CREATE FUNCTION foo(@bar INTEGER) RETURNS TABLE AS RETURN SELECT 1")
        self.validate_identity("CREATE FUNCTION dbo.ISOweek(@DATE DATETIME2) RETURNS INTEGER")

        # The following two cases don't necessarily correspond to valid TSQL, but they are used to verify
        # that the syntax RETURNS @return_variable TABLE <table_type_definition> ... is parsed correctly.
        #
        # See also "Transact-SQL Multi-Statement Table-Valued Function Syntax"
        # https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver16
        self.validate_identity(
            "CREATE FUNCTION foo(@bar INTEGER) RETURNS @foo TABLE (x INTEGER, y NUMERIC) AS RETURN SELECT 1"
        )
        self.validate_identity(
            "CREATE FUNCTION foo() RETURNS @contacts TABLE (first_name VARCHAR(50), phone VARCHAR(25)) AS SELECT @fname, @phone"
        )

        self.validate_all(
            """
            CREATE FUNCTION udfProductInYear (
                @model_year INT
            )
            RETURNS TABLE
            AS
            RETURN
                SELECT
                    product_name,
                    model_year,
                    list_price
                FROM
                    production.products
                WHERE
                    model_year = @model_year
            """,
            write={
                "tsql": """CREATE FUNCTION udfProductInYear(
    @model_year INTEGER
)
RETURNS TABLE AS
RETURN SELECT
  product_name,
  model_year,
  list_price
FROM production.products
WHERE
  model_year = @model_year""",
            },
            pretty=True,
        )

    def test_procedure_keywords(self):
        self.validate_identity("BEGIN")
        self.validate_identity("END")
        self.validate_identity("SET XACT_ABORT ON")

    def test_fullproc(self):
        sql = """
            CREATE procedure [TRANSF].[SP_Merge_Sales_Real]
                @Loadid INTEGER
               ,@NumberOfRows INTEGER
            WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION
            AS
            BEGIN
                SET XACT_ABORT ON;

                DECLARE @DWH_DateCreated AS DATETIME = CONVERT(DATETIME, getdate(), 104);
                DECLARE @DWH_DateModified DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104);
                DECLARE @DWH_IdUserCreated INTEGER = SUSER_ID (CURRENT_USER());
                DECLARE @DWH_IdUserModified INTEGER = SUSER_ID (SYSTEM_USER);

                DECLARE @SalesAmountBefore float;
                SELECT @SalesAmountBefore=SUM(SalesAmount) FROM TRANSF.[Pre_Merge_Sales_Real] S;
            END
        """

        expected_sqls = [
            "CREATE PROCEDURE [TRANSF].[SP_Merge_Sales_Real] @Loadid INTEGER, @NumberOfRows INTEGER WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION AS BEGIN SET XACT_ABORT ON",
            "DECLARE @DWH_DateCreated AS DATETIME = CONVERT(DATETIME, GETDATE(), 104)",
            "DECLARE @DWH_DateModified AS DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104)",
            "DECLARE @DWH_IdUserCreated AS INTEGER = SUSER_ID(CURRENT_USER())",
            "DECLARE @DWH_IdUserModified AS INTEGER = SUSER_ID(CURRENT_USER())",
            "DECLARE @SalesAmountBefore AS FLOAT",
            "SELECT @SalesAmountBefore = SUM(SalesAmount) FROM TRANSF.[Pre_Merge_Sales_Real] AS S",
            "END",
        ]

        for expr, expected_sql in zip(parse(sql, read="tsql"), expected_sqls):
            self.assertEqual(expr.sql(dialect="tsql"), expected_sql)

        sql = """
            CREATE PROC [dbo].[transform_proc] AS

            DECLARE @CurrentDate VARCHAR(20);
            SET @CurrentDate = CONVERT(VARCHAR(20), GETDATE(), 120);

            CREATE TABLE [target_schema].[target_table]
            (a INTEGER)
            WITH (DISTRIBUTION = REPLICATE, HEAP);
        """

        expected_sqls = [
            "CREATE PROC [dbo].[transform_proc] AS DECLARE @CurrentDate AS VARCHAR(20)",
            "SET @CurrentDate = CONVERT(VARCHAR(20), GETDATE(), 120)",
            "CREATE TABLE [target_schema].[target_table] (a INTEGER) WITH (DISTRIBUTION=REPLICATE, HEAP)",
        ]

        for expr, expected_sql in zip(parse(sql, read="tsql"), expected_sqls):
            self.assertEqual(expr.sql(dialect="tsql"), expected_sql)

    def test_charindex(self):
        self.validate_identity(
            "SELECT CAST(SUBSTRING('ABCD~1234', CHARINDEX('~', 'ABCD~1234') + 1, LEN('ABCD~1234')) AS BIGINT)"
        )

        self.validate_all(
            "CHARINDEX(x, y, 9)",
            read={
                "spark": "LOCATE(x, y, 9)",
            },
            write={
                "spark": "LOCATE(x, y, 9)",
                "tsql": "CHARINDEX(x, y, 9)",
            },
        )
        self.validate_all(
            "CHARINDEX(x, y)",
            read={
                "spark": "LOCATE(x, y)",
            },
            write={
                "spark": "LOCATE(x, y)",
                "tsql": "CHARINDEX(x, y)",
            },
        )
        self.validate_all(
            "CHARINDEX('sub', 'testsubstring', 3)",
            read={
                "spark": "LOCATE('sub', 'testsubstring', 3)",
            },
            write={
                "spark": "LOCATE('sub', 'testsubstring', 3)",
                "tsql": "CHARINDEX('sub', 'testsubstring', 3)",
            },
        )
        self.validate_all(
            "CHARINDEX('sub', 'testsubstring')",
            read={
                "spark": "LOCATE('sub', 'testsubstring')",
            },
            write={
                "spark": "LOCATE('sub', 'testsubstring')",
                "tsql": "CHARINDEX('sub', 'testsubstring')",
            },
        )

    def test_len(self):
        self.validate_all(
            "LEN(x)", read={"": "LENGTH(x)"}, write={"spark": "LENGTH(CAST(x AS STRING))"}
        )
        self.validate_all(
            "RIGHT(x, 1)",
            read={"": "RIGHT(CAST(x AS STRING), 1)"},
            write={"spark": "RIGHT(CAST(x AS STRING), 1)"},
        )
        self.validate_all(
            "LEFT(x, 1)",
            read={"": "LEFT(CAST(x AS STRING), 1)"},
            write={"spark": "LEFT(CAST(x AS STRING), 1)"},
        )
        self.validate_all("LEN(1)", write={"tsql": "LEN(1)", "spark": "LENGTH(CAST(1 AS STRING))"})
        self.validate_all("LEN('x')", write={"tsql": "LEN('x')", "spark": "LENGTH('x')"})

    def test_replicate(self):
        self.validate_all(
            "REPLICATE('x', 2)",
            write={
                "spark": "REPEAT('x', 2)",
                "tsql": "REPLICATE('x', 2)",
            },
        )

    def test_isnull(self):
        self.validate_all("ISNULL(x, y)", write={"spark": "COALESCE(x, y)"})

    def test_json(self):
        self.validate_identity(
            """JSON_QUERY(REPLACE(REPLACE(x , '''', '"'), '""', '"'))""",
            """ISNULL(JSON_QUERY(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'), JSON_VALUE(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'))""",
        )

        self.validate_all(
            "JSON_QUERY(r.JSON, '$.Attr_INT')",
            write={
                "spark": "GET_JSON_OBJECT(r.JSON, '$.Attr_INT')",
                "tsql": "ISNULL(JSON_QUERY(r.JSON, '$.Attr_INT'), JSON_VALUE(r.JSON, '$.Attr_INT'))",
            },
        )
        self.validate_all(
            "JSON_VALUE(r.JSON, '$.Attr_INT')",
            write={
                "spark": "GET_JSON_OBJECT(r.JSON, '$.Attr_INT')",
                "tsql": "ISNULL(JSON_QUERY(r.JSON, '$.Attr_INT'), JSON_VALUE(r.JSON, '$.Attr_INT'))",
            },
        )

    def test_datefromparts(self):
        self.validate_all(
            "SELECT DATEFROMPARTS('2020', 10, 01)",
            write={
                "spark": "SELECT MAKE_DATE('2020', 10, 01)",
                "tsql": "SELECT DATEFROMPARTS('2020', 10, 01)",
            },
        )

    def test_datename(self):
        self.validate_all(
            "SELECT DATENAME(mm, '1970-01-01')",
            write={
                "spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'MMMM')",
                "tsql": "SELECT FORMAT(CAST('1970-01-01' AS DATETIME2), 'MMMM')",
            },
        )
        self.validate_all(
            "SELECT DATENAME(dw, '1970-01-01')",
            write={
                "spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'EEEE')",
                "tsql": "SELECT FORMAT(CAST('1970-01-01' AS DATETIME2), 'dddd')",
            },
        )

    def test_datepart(self):
        self.validate_identity(
            "DATEPART(QUARTER, x)",
            "DATEPART(QUARTER, CAST(x AS DATETIME2))",
        )
        self.validate_identity(
            "DATEPART(YEAR, x)",
            "FORMAT(CAST(x AS DATETIME2), 'yyyy')",
        )
        self.validate_identity(
            "DATEPART(HOUR, date_and_time)",
            "DATEPART(HOUR, CAST(date_and_time AS DATETIME2))",
        )
        self.validate_identity(
            "DATEPART(WEEKDAY, date_and_time)",
            "DATEPART(DW, CAST(date_and_time AS DATETIME2))",
        )
        self.validate_identity(
            "DATEPART(DW, date_and_time)",
            "DATEPART(DW, CAST(date_and_time AS DATETIME2))",
        )

        self.validate_all(
            "SELECT DATEPART(month,'1970-01-01')",
            write={
                "postgres": "SELECT TO_CHAR(CAST('1970-01-01' AS TIMESTAMP), 'MM')",
                "spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'MM')",
                "tsql": "SELECT FORMAT(CAST('1970-01-01' AS DATETIME2), 'MM')",
            },
        )
        self.validate_all(
            "SELECT DATEPART(YEAR, CAST('2017-01-01' AS DATE))",
            read={
                "postgres": "SELECT DATE_PART('YEAR', '2017-01-01'::DATE)",
            },
            write={
                "postgres": "SELECT TO_CHAR(CAST(CAST('2017-01-01' AS DATE) AS TIMESTAMP), 'YYYY')",
                "spark": "SELECT DATE_FORMAT(CAST(CAST('2017-01-01' AS DATE) AS TIMESTAMP), 'yyyy')",
                "tsql": "SELECT FORMAT(CAST(CAST('2017-01-01' AS DATE) AS DATETIME2), 'yyyy')",
            },
        )
        self.validate_all(
            "SELECT DATEPART(month, CAST('2017-03-01' AS DATE))",
            read={
                "postgres": "SELECT DATE_PART('month', '2017-03-01'::DATE)",
            },
            write={
                "postgres": "SELECT TO_CHAR(CAST(CAST('2017-03-01' AS DATE) AS TIMESTAMP), 'MM')",
                "spark": "SELECT DATE_FORMAT(CAST(CAST('2017-03-01' AS DATE) AS TIMESTAMP), 'MM')",
                "tsql": "SELECT FORMAT(CAST(CAST('2017-03-01' AS DATE) AS DATETIME2), 'MM')",
            },
        )
        self.validate_all(
            "SELECT DATEPART(day, CAST('2017-01-02' AS DATE))",
            read={
                "postgres": "SELECT DATE_PART('day', '2017-01-02'::DATE)",
            },
            write={
                "postgres": "SELECT TO_CHAR(CAST(CAST('2017-01-02' AS DATE) AS TIMESTAMP), 'DD')",
                "spark": "SELECT DATE_FORMAT(CAST(CAST('2017-01-02' AS DATE) AS TIMESTAMP), 'dd')",
                "tsql": "SELECT FORMAT(CAST(CAST('2017-01-02' AS DATE) AS DATETIME2), 'dd')",
            },
        )

        for fmt in ("WEEK", "WW", "WK"):
            self.validate_identity(
                f"SELECT DATEPART({fmt}, '2024-11-21')",
                "SELECT DATEPART(WK, CAST('2024-11-21' AS DATETIME2))",
            )

    def test_convert(self):
        self.validate_all(
            "CONVERT(NVARCHAR(200), x)",
            write={
                "spark": "CAST(x AS VARCHAR(200))",
                "tsql": "CONVERT(NVARCHAR(200), x)",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR, x)",
            write={
                "spark": "CAST(x AS VARCHAR(30))",
                "tsql": "CONVERT(NVARCHAR, x)",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(MAX), x)",
            write={
                "spark": "CAST(x AS STRING)",
                "tsql": "CONVERT(NVARCHAR(MAX), x)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(200), x)",
            write={
                "spark": "CAST(x AS VARCHAR(200))",
                "tsql": "CONVERT(VARCHAR(200), x)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR, x)",
            write={
                "spark": "CAST(x AS VARCHAR(30))",
                "tsql": "CONVERT(VARCHAR, x)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(MAX), x)",
            write={
                "spark": "CAST(x AS STRING)",
                "tsql": "CONVERT(VARCHAR(MAX), x)",
            },
        )
        self.validate_all(
            "CONVERT(CHAR(40), x)",
            write={
                "spark": "CAST(x AS CHAR(40))",
                "tsql": "CONVERT(CHAR(40), x)",
            },
        )
        self.validate_all(
            "CONVERT(CHAR, x)",
            write={
                "spark": "CAST(x AS CHAR(30))",
                "tsql": "CONVERT(CHAR, x)",
            },
        )
        self.validate_all(
            "CONVERT(NCHAR(40), x)",
            write={
                "spark": "CAST(x AS CHAR(40))",
                "tsql": "CONVERT(NCHAR(40), x)",
            },
        )
        self.validate_all(
            "CONVERT(NCHAR, x)",
            write={
                "spark": "CAST(x AS CHAR(30))",
                "tsql": "CONVERT(NCHAR, x)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR, x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
                "tsql": "CONVERT(VARCHAR, x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(40), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(40))",
                "tsql": "CONVERT(VARCHAR(40), x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(MAX), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS STRING)",
                "tsql": "CONVERT(VARCHAR(MAX), x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR, x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
                "tsql": "CONVERT(NVARCHAR, x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(40), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(40))",
                "tsql": "CONVERT(NVARCHAR(40), x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(MAX), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS STRING)",
                "tsql": "CONVERT(NVARCHAR(MAX), x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(DATE, x, 121)",
            write={
                "spark": "TO_DATE(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
                "tsql": "CONVERT(DATE, x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(DATETIME, x, 121)",
            write={
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
                "tsql": "CONVERT(DATETIME, x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(DATETIME2, x, 121)",
            write={
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
                "tsql": "CONVERT(DATETIME2, x, 121)",
            },
        )
        self.validate_all(
            "CONVERT(INT, x)",
            write={
                "spark": "CAST(x AS INT)",
                "tsql": "CONVERT(INTEGER, x)",
            },
        )
        self.validate_all(
            "CONVERT(INT, x, 121)",
            write={
                "spark": "CAST(x AS INT)",
                "tsql": "CONVERT(INTEGER, x, 121)",
            },
        )
        self.validate_all(
            "TRY_CONVERT(NVARCHAR, x, 121)",
            write={
                "spark": "TRY_CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
                "tsql": "TRY_CONVERT(NVARCHAR, x, 121)",
            },
        )
        self.validate_all(
            "TRY_CONVERT(INT, x)",
            write={
                "spark": "TRY_CAST(x AS INT)",
                "tsql": "TRY_CONVERT(INTEGER, x)",
            },
        )
        self.validate_all(
            "TRY_CAST(x AS INT)",
            write={
                "spark": "TRY_CAST(x AS INT)",
                "tsql": "TRY_CAST(x AS INTEGER)",
            },
        )
        self.validate_all(
            "SELECT CONVERT(VARCHAR(10), testdb.dbo.test.x, 120) y FROM testdb.dbo.test",
            write={
                "mysql": "SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, '%Y-%m-%d %T') AS CHAR(10)) AS y FROM testdb.dbo.test",
                "spark": "SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, 'yyyy-MM-dd HH:mm:ss') AS VARCHAR(10)) AS y FROM testdb.dbo.test",
                "tsql": "SELECT CONVERT(VARCHAR(10), testdb.dbo.test.x, 120) AS y FROM testdb.dbo.test",
            },
        )
        self.validate_all(
            "SELECT CONVERT(VARCHAR(10), y.x) z FROM testdb.dbo.test y",
            write={
                "mysql": "SELECT CAST(y.x AS CHAR(10)) AS z FROM testdb.dbo.test AS y",
                "spark": "SELECT CAST(y.x AS VARCHAR(10)) AS z FROM testdb.dbo.test AS y",
                "tsql": "SELECT CONVERT(VARCHAR(10), y.x) AS z FROM testdb.dbo.test AS y",
            },
        )
        self.validate_all(
            "SELECT CAST((SELECT x FROM y) AS VARCHAR) AS test",
            write={
                "spark": "SELECT CAST((SELECT x FROM y) AS STRING) AS test",
                "tsql": "SELECT CAST((SELECT x FROM y) AS VARCHAR) AS test",
            },
        )

    def test_add_date(self):
        self.validate_identity("SELECT DATEADD(YEAR, 1, '2017/08/25')")

        self.validate_all(
            "DATEADD(year, 50, '2006-07-31')",
            write={"bigquery": "DATE_ADD('2006-07-31', INTERVAL 50 YEAR)"},
        )
        self.validate_all(
            "SELECT DATEADD(year, 1, '2017/08/25')",
            write={"spark": "SELECT ADD_MONTHS('2017/08/25', 12)"},
        )
        self.validate_all(
            "SELECT DATEADD(qq, 1, '2017/08/25')",
            write={"spark": "SELECT ADD_MONTHS('2017/08/25', 3)"},
        )
        self.validate_all(
            "SELECT DATEADD(wk, 1, '2017/08/25')",
            write={
                "spark": "SELECT DATE_ADD('2017/08/25', 7)",
                "databricks": "SELECT DATEADD(WEEK, 1, '2017/08/25')",
            },
        )

    def test_date_diff(self):
        self.validate_identity("SELECT DATEDIFF(HOUR, 1.5, '2021-01-01')")

        self.validate_all(
            "SELECT DATEDIFF(quarter, 0, '2021-01-01')",
            write={
                "tsql": "SELECT DATEDIFF(QUARTER, CAST('1900-01-01' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))",
                "spark": "SELECT DATEDIFF(QUARTER, CAST('1900-01-01' AS TIMESTAMP), CAST('2021-01-01' AS TIMESTAMP))",
                "duckdb": "SELECT DATE_DIFF('QUARTER', CAST('1900-01-01' AS TIMESTAMP), CAST('2021-01-01' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(day, 1, '2021-01-01')",
            write={
                "tsql": "SELECT DATEDIFF(DAY, CAST('1900-01-02' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))",
                "spark": "SELECT DATEDIFF(DAY, CAST('1900-01-02' AS TIMESTAMP), CAST('2021-01-01' AS TIMESTAMP))",
                "duckdb": "SELECT DATE_DIFF('DAY', CAST('1900-01-02' AS TIMESTAMP), CAST('2021-01-01' AS TIMESTAMP))",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(year, '2020-01-01', '2021-01-01')",
            write={
                "tsql": "SELECT DATEDIFF(YEAR, CAST('2020-01-01' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))",
                "spark": "SELECT DATEDIFF(YEAR, CAST('2020-01-01' AS TIMESTAMP), CAST('2021-01-01' AS TIMESTAMP))",
                "spark2": "SELECT CAST(MONTHS_BETWEEN(CAST('2021-01-01' AS TIMESTAMP), CAST('2020-01-01' AS TIMESTAMP)) / 12 AS INT)",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(mm, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(MONTH, CAST('start' AS TIMESTAMP), CAST('end' AS TIMESTAMP))",
                "spark2": "SELECT CAST(MONTHS_BETWEEN(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP)) AS INT)",
                "tsql": "SELECT DATEDIFF(MONTH, CAST('start' AS DATETIME2), CAST('end' AS DATETIME2))",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(quarter, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(QUARTER, CAST('start' AS TIMESTAMP), CAST('end' AS TIMESTAMP))",
                "spark": "SELECT DATEDIFF(QUARTER, CAST('start' AS TIMESTAMP), CAST('end' AS TIMESTAMP))",
                "spark2": "SELECT CAST(MONTHS_BETWEEN(CAST('end' AS TIMESTAMP), CAST('start' AS TIMESTAMP)) / 3 AS INT)",
                "tsql": "SELECT DATEDIFF(QUARTER, CAST('start' AS DATETIME2), CAST('end' AS DATETIME2))",
            },
        )

        # Check superfluous casts arent added. ref: https://github.com/TobikoData/sqlmesh/issues/2672
        self.validate_all(
            "SELECT DATEDIFF(DAY, CAST(a AS DATETIME2), CAST(b AS DATETIME2)) AS x FROM foo",
            write={
                "tsql": "SELECT DATEDIFF(DAY, CAST(a AS DATETIME2), CAST(b AS DATETIME2)) AS x FROM foo",
                "clickhouse": "SELECT DATE_DIFF(DAY, CAST(CAST(a AS Nullable(DateTime)) AS DateTime64(6)), CAST(CAST(b AS Nullable(DateTime)) AS DateTime64(6))) AS x FROM foo",
            },
        )

        self.validate_identity(
            "SELECT DATEADD(DAY, DATEDIFF(DAY, -3, GETDATE()), '08:00:00')",
            "SELECT DATEADD(DAY, DATEDIFF(DAY, CAST('1899-12-29' AS DATETIME2), CAST(GETDATE() AS DATETIME2)), '08:00:00')",
        )

    def test_lateral_subquery(self):
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y FROM x CROSS APPLY (SELECT v, y FROM t) t(v, y)",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y FROM x INNER JOIN LATERAL (SELECT v, y FROM t) AS t(v, y)",
                "tsql": "SELECT x.a, x.b, t.v, t.y FROM x CROSS APPLY (SELECT v, y FROM t) AS t(v, y)",
            },
        )
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y)",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y)",
                "tsql": "SELECT x.a, x.b, t.v, t.y FROM x OUTER APPLY (SELECT v, y FROM t) AS t(v, y)",
            },
        )
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y) OUTER APPLY (SELECT v, y FROM t) s(v, y) LEFT JOIN z ON z.id = s.id",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) LEFT JOIN LATERAL (SELECT v, y FROM t) AS s(v, y) LEFT JOIN z ON z.id = s.id",
                "tsql": "SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x OUTER APPLY (SELECT v, y FROM t) AS t(v, y) OUTER APPLY (SELECT v, y FROM t) AS s(v, y) LEFT JOIN z ON z.id = s.id",
            },
        )

    def test_lateral_table_valued_function(self):
        self.validate_all(
            "SELECT t.x, y.z FROM x CROSS APPLY tvfTest(t.x) y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x INNER JOIN LATERAL TVFTEST(t.x) AS y(z)",
                "tsql": "SELECT t.x, y.z FROM x CROSS APPLY TVFTEST(t.x) AS y(z)",
            },
        )
        self.validate_all(
            "SELECT t.x, y.z FROM x OUTER APPLY tvfTest(t.x)y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x LEFT JOIN LATERAL TVFTEST(t.x) AS y(z)",
                "tsql": "SELECT t.x, y.z FROM x OUTER APPLY TVFTEST(t.x) AS y(z)",
            },
        )
        self.validate_all(
            "SELECT t.x, y.z FROM x OUTER APPLY a.b.tvfTest(t.x)y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x LEFT JOIN LATERAL a.b.tvfTest(t.x) AS y(z)",
                "tsql": "SELECT t.x, y.z FROM x OUTER APPLY a.b.tvfTest(t.x) AS y(z)",
            },
        )

    def test_top(self):
        self.validate_all(
            "SELECT DISTINCT TOP 3 * FROM A",
            read={
                "spark": "SELECT DISTINCT * FROM A LIMIT 3",
            },
            write={
                "spark": "SELECT DISTINCT * FROM A LIMIT 3",
                "teradata": "SELECT DISTINCT TOP 3 * FROM A",
                "tsql": "SELECT DISTINCT TOP 3 * FROM A",
            },
        )
        self.validate_all(
            "SELECT TOP (3) * FROM A",
            write={
                "spark": "SELECT * FROM A LIMIT 3",
            },
        )
        self.validate_identity("SELECT TOP 10 PERCENT")
        self.validate_identity("SELECT TOP 10 PERCENT WITH TIES")

    def test_format(self):
        self.validate_identity("SELECT FORMAT(foo, 'dddd', 'de-CH')")
        self.validate_identity("SELECT FORMAT(EndOfDayRate, 'N', 'en-us')")
        self.validate_identity("SELECT FORMAT('01-01-1991', 'd.mm.yyyy')")
        self.validate_identity("SELECT FORMAT(12345, '###.###.###')")
        self.validate_identity("SELECT FORMAT(1234567, 'f')")

        self.validate_all(
            "SELECT FORMAT(1000000.01,'###,###.###')",
            write={
                "spark": "SELECT FORMAT_NUMBER(1000000.01, '###,###.###')",
                "tsql": "SELECT FORMAT(1000000.01, '###,###.###')",
            },
        )
        self.validate_all(
            "SELECT FORMAT(1234567, 'f')",
            write={
                "spark": "SELECT FORMAT_NUMBER(1234567, 'f')",
                "tsql": "SELECT FORMAT(1234567, 'f')",
            },
        )
        self.validate_all(
            "SELECT FORMAT('01-01-1991', 'dd.mm.yyyy')",
            write={
                "spark": "SELECT DATE_FORMAT('01-01-1991', 'dd.mm.yyyy')",
                "tsql": "SELECT FORMAT('01-01-1991', 'dd.mm.yyyy')",
            },
        )
        self.validate_all(
            "SELECT FORMAT(date_col, 'dd.mm.yyyy')",
            write={
                "spark": "SELECT DATE_FORMAT(date_col, 'dd.mm.yyyy')",
                "tsql": "SELECT FORMAT(date_col, 'dd.mm.yyyy')",
            },
        )
        self.validate_all(
            "SELECT FORMAT(date_col, 'm')",
            write={
                "spark": "SELECT DATE_FORMAT(date_col, 'MMMM d')",
                "tsql": "SELECT FORMAT(date_col, 'MMMM d')",
            },
        )
        self.validate_all(
            "SELECT FORMAT(num_col, 'c')",
            write={
                "spark": "SELECT FORMAT_NUMBER(num_col, 'c')",
                "tsql": "SELECT FORMAT(num_col, 'c')",
            },
        )

    def test_string(self):
        self.validate_all(
            "SELECT N'test'",
            write={"spark": "SELECT 'test'"},
        )
        self.validate_all(
            "SELECT n'test'",
            write={"spark": "SELECT 'test'"},
        )
        self.validate_all(
            "SELECT '''test'''",
            write={"spark": r"SELECT '\'test\''"},
        )

    def test_eomonth(self):
        self.validate_all(
            "EOMONTH(GETDATE())",
            read={
                "spark": "LAST_DAY(CURRENT_TIMESTAMP())",
            },
            write={
                "bigquery": "LAST_DAY(CAST(CURRENT_TIMESTAMP() AS DATE))",
                "clickhouse": "LAST_DAY(CAST(CURRENT_TIMESTAMP() AS Nullable(DATE)))",
                "duckdb": "LAST_DAY(CAST(CURRENT_TIMESTAMP AS DATE))",
                "mysql": "LAST_DAY(DATE(CURRENT_TIMESTAMP()))",
                "postgres": "CAST(DATE_TRUNC('MONTH', CAST(CURRENT_TIMESTAMP AS DATE)) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)",
                "presto": "LAST_DAY_OF_MONTH(CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS DATE))",
                "redshift": "LAST_DAY(CAST(GETDATE() AS DATE))",
                "snowflake": "LAST_DAY(TO_DATE(CURRENT_TIMESTAMP()))",
                "spark": "LAST_DAY(TO_DATE(CURRENT_TIMESTAMP()))",
                "tsql": "EOMONTH(CAST(GETDATE() AS DATE))",
            },
        )
        self.validate_all(
            "EOMONTH(GETDATE(), -1)",
            write={
                "bigquery": "LAST_DAY(DATE_ADD(CAST(CURRENT_TIMESTAMP() AS DATE), INTERVAL -1 MONTH))",
                "clickhouse": "LAST_DAY(DATE_ADD(MONTH, -1, CAST(CURRENT_TIMESTAMP() AS Nullable(DATE))))",
                "duckdb": "LAST_DAY(CAST(CURRENT_TIMESTAMP AS DATE) + INTERVAL (-1) MONTH)",
                "mysql": "LAST_DAY(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 MONTH))",
                "postgres": "CAST(DATE_TRUNC('MONTH', CAST(CURRENT_TIMESTAMP AS DATE) + INTERVAL '-1 MONTH') + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)",
                "presto": "LAST_DAY_OF_MONTH(DATE_ADD('MONTH', -1, CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS DATE)))",
                "redshift": "LAST_DAY(DATEADD(MONTH, -1, CAST(GETDATE() AS DATE)))",
                "snowflake": "LAST_DAY(DATEADD(MONTH, -1, TO_DATE(CURRENT_TIMESTAMP())))",
                "spark": "LAST_DAY(ADD_MONTHS(TO_DATE(CURRENT_TIMESTAMP()), -1))",
                "tsql": "EOMONTH(DATEADD(MONTH, -1, CAST(GETDATE() AS DATE)))",
            },
        )

    def test_identifier_prefixes(self):
        self.assertTrue(
            self.validate_identity("#x")
            .assert_is(exp.Column)
            .this.assert_is(exp.Identifier)
            .args.get("temporary")
        )
        self.assertTrue(
            self.validate_identity("##x")
            .assert_is(exp.Column)
            .this.assert_is(exp.Identifier)
            .args.get("global")
        )

        self.validate_identity("@x").assert_is(exp.Parameter).this.assert_is(exp.Var)
        self.validate_identity("SELECT * FROM @x").args["from"].this.assert_is(
            exp.Table
        ).this.assert_is(exp.Parameter).this.assert_is(exp.Var)

        self.validate_all(
            "SELECT @x",
            write={
                "databricks": "SELECT ${x}",
                "hive": "SELECT ${x}",
                "spark": "SELECT ${x}",
                "tsql": "SELECT @x",
            },
        )
        self.validate_all(
            "SELECT * FROM #mytemptable",
            write={
                "duckdb": "SELECT * FROM mytemptable",
                "spark": "SELECT * FROM mytemptable",
                "tsql": "SELECT * FROM #mytemptable",
            },
        )
        self.validate_all(
            "SELECT * FROM ##mytemptable",
            write={
                "duckdb": "SELECT * FROM mytemptable",
                "spark": "SELECT * FROM mytemptable",
                "tsql": "SELECT * FROM ##mytemptable",
            },
        )

    def test_temporal_table(self):
        self.validate_identity(
            """CREATE TABLE test ("data" CHAR(7), "valid_from" DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, "valid_to" DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ("valid_from", "valid_to")) WITH(SYSTEM_VERSIONING=ON)""",
            "CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON)",
        )
        self.validate_identity(
            """CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=ON))"""
        )
        self.validate_identity(
            """CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=ON))"""
        )
        self.validate_identity(
            """CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=OFF))"""
        )
        self.validate_identity(
            """CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history]))"""
        )
        self.validate_identity(
            """CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history]))"""
        )

    def test_system_time(self):
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF 'foo'")
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF 'foo' AS alias")
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME FROM c TO d")
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME BETWEEN c AND d")
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME CONTAINED IN (c, d)")
        self.validate_identity("SELECT [x] FROM [a].[b] FOR SYSTEM_TIME ALL AS alias")

    def test_current_user(self):
        self.validate_all(
            "SUSER_NAME()",
            write={"spark": "CURRENT_USER()"},
        )
        self.validate_all(
            "SUSER_SNAME()",
            write={"spark": "CURRENT_USER()"},
        )
        self.validate_all(
            "SYSTEM_USER()",
            write={"spark": "CURRENT_USER()"},
        )
        self.validate_all(
            "SYSTEM_USER",
            write={"spark": "CURRENT_USER()"},
        )

    def test_hints(self):
        self.validate_all(
            "SELECT x FROM a INNER HASH JOIN b ON b.id = a.id",
            write={"spark": "SELECT x FROM a INNER JOIN b ON b.id = a.id"},
        )
        self.validate_all(
            "SELECT x FROM a INNER LOOP JOIN b ON b.id = a.id",
            write={"spark": "SELECT x FROM a INNER JOIN b ON b.id = a.id"},
        )
        self.validate_all(
            "SELECT x FROM a INNER REMOTE JOIN b ON b.id = a.id",
            write={"spark": "SELECT x FROM a INNER JOIN b ON b.id = a.id"},
        )
        self.validate_all(
            "SELECT x FROM a INNER MERGE JOIN b ON b.id = a.id",
            write={"spark": "SELECT x FROM a INNER JOIN b ON b.id = a.id"},
        )
        self.validate_all(
            "SELECT x FROM a WITH (NOLOCK)",
            write={
                "spark": "SELECT x FROM a",
                "tsql": "SELECT x FROM a WITH (NOLOCK)",
                "": "SELECT x FROM a WITH (NOLOCK)",
            },
        )
        self.validate_identity("SELECT x FROM a INNER LOOP JOIN b ON b.id = a.id")

    def test_openjson(self):
        self.validate_identity("SELECT * FROM OPENJSON(@json)")

        self.validate_all(
            """SELECT [key], value FROM OPENJSON(@json,'$.path.to."sub-object"')""",
            write={
                "tsql": """SELECT [key], value FROM OPENJSON(@json, '$.path.to."sub-object"')""",
            },
        )
        self.validate_all(
            "SELECT * FROM OPENJSON(@array) WITH (month VARCHAR(3), temp int, month_id tinyint '$.sql:identity()') as months",
            write={
                "tsql": "SELECT * FROM OPENJSON(@array) WITH (month VARCHAR(3), temp INTEGER, month_id TINYINT '$.sql:identity()') AS months",
            },
        )
        self.validate_all(
            """
            SELECT *
            FROM OPENJSON ( @json )
            WITH (
                          Number   VARCHAR(200)   '$.Order.Number',
                          Date     DATETIME       '$.Order.Date',
                          Customer VARCHAR(200)   '$.AccountNumber',
                          Quantity INT            '$.Item.Quantity',
                          [Order]  NVARCHAR(MAX)  AS JSON
             )
            """,
            write={
                "tsql": """SELECT
  *
FROM OPENJSON(@json) WITH (
    Number VARCHAR(200) '$.Order.Number',
    Date DATETIME '$.Order.Date',
    Customer VARCHAR(200) '$.AccountNumber',
    Quantity INTEGER '$.Item.Quantity',
    [Order] NVARCHAR(MAX) AS JSON
)"""
            },
            pretty=True,
        )

    def test_set(self):
        self.validate_all(
            "SET KEY VALUE",
            write={
                "tsql": "SET KEY VALUE",
                "duckdb": "SET KEY = VALUE",
                "spark": "SET KEY = VALUE",
            },
        )
        self.validate_all(
            "SET @count = (SELECT COUNT(1) FROM x)",
            write={
                "databricks": "SET count = (SELECT COUNT(1) FROM x)",
                "tsql": "SET @count = (SELECT COUNT(1) FROM x)",
                "spark": "SET count = (SELECT COUNT(1) FROM x)",
            },
        )

    def test_qualify_derived_table_outputs(self):
        self.validate_identity(
            "WITH t AS (SELECT 1) SELECT * FROM t",
            "WITH t AS (SELECT 1 AS [1]) SELECT * FROM t",
        )
        self.validate_identity(
            'WITH t AS (SELECT "c") SELECT * FROM t',
            "WITH t AS (SELECT [c] AS [c]) SELECT * FROM t",
        )
        self.validate_identity(
            "SELECT * FROM (SELECT 1) AS subq",
            "SELECT * FROM (SELECT 1 AS [1]) AS subq",
        )
        self.validate_identity(
            'SELECT * FROM (SELECT "c") AS subq',
            "SELECT * FROM (SELECT [c] AS [c]) AS subq",
        )

        self.validate_all(
            "WITH t1(c) AS (SELECT 1), t2 AS (SELECT CAST(c AS INTEGER) AS c FROM t1) SELECT * FROM t2",
            read={
                "duckdb": "WITH t1(c) AS (SELECT 1), t2 AS (SELECT CAST(c AS INTEGER) FROM t1) SELECT * FROM t2",
            },
        )

    def test_declare(self):
        # supported cases
        self.validate_identity("DECLARE @X INT", "DECLARE @X AS INTEGER")
        self.validate_identity("DECLARE @X INT = 1", "DECLARE @X AS INTEGER = 1")
        self.validate_identity(
            "DECLARE @X INT, @Y VARCHAR(10)", "DECLARE @X AS INTEGER, @Y AS VARCHAR(10)"
        )
        self.validate_identity(
            "declare @X int = (select col from table where id = 1)",
            "DECLARE @X AS INTEGER = (SELECT col FROM table WHERE id = 1)",
        )
        self.validate_identity(
            "declare @X TABLE (Id INT NOT NULL, Name VARCHAR(100) NOT NULL)",
            "DECLARE @X AS TABLE (Id INTEGER NOT NULL, Name VARCHAR(100) NOT NULL)",
        )
        self.validate_identity(
            "declare @X TABLE (Id INT NOT NULL, constraint PK_Id primary key (Id))",
            "DECLARE @X AS TABLE (Id INTEGER NOT NULL, CONSTRAINT PK_Id PRIMARY KEY (Id))",
        )
        self.validate_identity(
            "declare @X UserDefinedTableType",
            "DECLARE @X AS UserDefinedTableType",
        )
        self.validate_identity(
            "DECLARE @MyTableVar TABLE (EmpID INT NOT NULL, PRIMARY KEY CLUSTERED (EmpID), UNIQUE NONCLUSTERED (EmpID), INDEX CustomNonClusteredIndex NONCLUSTERED (EmpID))",
            check_command_warning=True,
        )
        self.validate_identity(
            "DECLARE vendor_cursor CURSOR FOR SELECT VendorID, Name FROM Purchasing.Vendor WHERE PreferredVendorStatus = 1 ORDER BY VendorID",
            check_command_warning=True,
        )

    def test_scope_resolution_op(self):
        # we still want to support :: casting shorthand for tsql
        self.validate_identity("x::int", "CAST(x AS INTEGER)")
        self.validate_identity("x::varchar", "CAST(x AS VARCHAR)")
        self.validate_identity("x::varchar(MAX)", "CAST(x AS VARCHAR(MAX))")

        for lhs, rhs in (
            ("", "FOO(a, b)"),
            ("bar", "baZ(1, 2)"),
            ("LOGIN", "EricKurjan"),
            ("GEOGRAPHY", "Point(latitude, longitude, 4326)"),
            (
                "GEOGRAPHY",
                "STGeomFromText('POLYGON((-122.358 47.653 , -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))', 4326)",
            ),
        ):
            with self.subTest(f"Scope resolution, LHS: {lhs}, RHS: {rhs}"):
                expr = self.validate_identity(f"{lhs}::{rhs}")
                base_sql = expr.sql()
                self.assertEqual(base_sql, f"SCOPE_RESOLUTION({lhs + ', ' if lhs else ''}{rhs})")
                self.assertEqual(parse_one(base_sql).sql("tsql"), f"{lhs}::{rhs}")

    def test_count(self):
        count = annotate_types(self.validate_identity("SELECT COUNT(1) FROM x"))
        self.assertEqual(count.expressions[0].type.this, exp.DataType.Type.INT)

        count_big = annotate_types(self.validate_identity("SELECT COUNT_BIG(1) FROM x"))
        self.assertEqual(count_big.expressions[0].type.this, exp.DataType.Type.BIGINT)

        self.validate_all(
            "SELECT COUNT_BIG(1) FROM x",
            read={
                "duckdb": "SELECT COUNT(1) FROM x",
                "spark": "SELECT COUNT(1) FROM x",
            },
            write={
                "duckdb": "SELECT COUNT(1) FROM x",
                "spark": "SELECT COUNT(1) FROM x",
                "tsql": "SELECT COUNT_BIG(1) FROM x",
            },
        )
        self.validate_all(
            "SELECT COUNT(1) FROM x",
            write={
                "duckdb": "SELECT COUNT(1) FROM x",
                "spark": "SELECT COUNT(1) FROM x",
                "tsql": "SELECT COUNT(1) FROM x",
            },
        )

    def test_grant(self):
        self.validate_identity("GRANT EXECUTE ON TestProc TO User2")
        self.validate_identity("GRANT EXECUTE ON TestProc TO TesterRole WITH GRANT OPTION")
        self.validate_identity(
            "GRANT EXECUTE ON TestProc TO User2 AS TesterRole", check_command_warning=True
        )

    def test_parsename(self):
        for i in range(4):
            with self.subTest("Testing PARSENAME <-> SPLIT_PART"):
                self.validate_all(
                    f"SELECT PARSENAME('1.2.3', {i})",
                    read={
                        "spark": f"SELECT SPLIT_PART('1.2.3', '.', {4 - i})",
                        "databricks": f"SELECT SPLIT_PART('1.2.3', '.', {4 - i})",
                    },
                    write={
                        "spark": f"SELECT SPLIT_PART('1.2.3', '.', {4 - i})",
                        "databricks": f"SELECT SPLIT_PART('1.2.3', '.', {4 - i})",
                        "tsql": f"SELECT PARSENAME('1.2.3', {i})",
                    },
                )

        # Test non-dot delimiter
        self.validate_all(
            "SELECT SPLIT_PART('1,2,3', ',', 1)",
            write={
                "spark": "SELECT SPLIT_PART('1,2,3', ',', 1)",
                "databricks": "SELECT SPLIT_PART('1,2,3', ',', 1)",
                "tsql": UnsupportedError,
            },
        )

        # Test column-type parameters
        self.validate_all(
            "WITH t AS (SELECT 'a.b.c' AS value, 1 AS idx) SELECT SPLIT_PART(value, '.', idx) FROM t",
            write={
                "spark": "WITH t AS (SELECT 'a.b.c' AS value, 1 AS idx) SELECT SPLIT_PART(value, '.', idx) FROM t",
                "databricks": "WITH t AS (SELECT 'a.b.c' AS value, 1 AS idx) SELECT SPLIT_PART(value, '.', idx) FROM t",
                "tsql": UnsupportedError,
            },
        )

    def test_next_value_for(self):
        self.validate_identity(
            "SELECT NEXT VALUE FOR db.schema.sequence_name OVER (ORDER BY foo), col"
        )
        self.validate_all(
            "SELECT NEXT VALUE FOR db.schema.sequence_name",
            read={
                "oracle": "SELECT NEXT VALUE FOR db.schema.sequence_name",
                "tsql": "SELECT NEXT VALUE FOR db.schema.sequence_name",
            },
            write={
                "oracle": "SELECT NEXT VALUE FOR db.schema.sequence_name",
            },
        )

    # string literals in the DATETRUNC are casted as DATETIME2
    def test_datetrunc(self):
        self.validate_all(
            "SELECT DATETRUNC(month, 'foo')",
            write={
                "duckdb": "SELECT DATE_TRUNC('MONTH', CAST('foo' AS TIMESTAMP))",
                "tsql": "SELECT DATETRUNC(MONTH, CAST('foo' AS DATETIME2))",
            },
        )
        self.validate_all(
            "SELECT DATETRUNC(month, foo)",
            write={
                "duckdb": "SELECT DATE_TRUNC('MONTH', foo)",
                "tsql": "SELECT DATETRUNC(MONTH, foo)",
            },
        )
        self.validate_all(
            "SELECT DATETRUNC(year, CAST('foo1' AS date))",
            write={
                "duckdb": "SELECT DATE_TRUNC('YEAR', CAST('foo1' AS DATE))",
                "tsql": "SELECT DATETRUNC(YEAR, CAST('foo1' AS DATE))",
            },
        )
