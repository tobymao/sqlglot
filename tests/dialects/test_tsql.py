from sqlglot import exp, parse, parse_one
from tests.dialects.test_dialect import Validator


class TestTSQL(Validator):
    dialect = "tsql"

    def test_tsql(self):
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
        self.validate_identity("DECLARE @TestVariable AS VARCHAR(100)='Save Our Planet'")
        self.validate_identity("PRINT @TestVariable")
        self.validate_identity("SELECT Employee_ID, Department_ID FROM @MyTableVar")
        self.validate_identity("INSERT INTO @TestTable VALUES (1, 'Value1', 12, 20)")
        self.validate_identity('SELECT "x"."y" FROM foo')
        self.validate_identity("SELECT * FROM #foo")
        self.validate_identity("SELECT * FROM ##foo")
        self.validate_identity(
            "SELECT x FROM @MyTableVar AS m JOIN Employee ON m.EmployeeID = Employee.EmployeeID"
        )
        self.validate_identity(
            "SELECT DISTINCT DepartmentName, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY BaseRate) OVER (PARTITION BY DepartmentName) AS MedianCont FROM dbo.DimEmployee"
        )

        self.validate_all(
            "SELECT DATEPART(year, CAST('2017-01-01' AS DATE))",
            read={"postgres": "SELECT DATE_PART('year', '2017-01-01'::DATE)"},
        )
        self.validate_all(
            "SELECT DATEPART(month, CAST('2017-03-01' AS DATE))",
            read={"postgres": "SELECT DATE_PART('month', '2017-03-01'::DATE)"},
        )
        self.validate_all(
            "SELECT DATEPART(day, CAST('2017-01-02' AS DATE))",
            read={"postgres": "SELECT DATE_PART('day', '2017-01-02'::DATE)"},
        )
        self.validate_all(
            "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
            write={
                "tsql": 'SELECT CAST("a"."b" AS SMALLINT) FROM foo',
                "spark": "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            },
        )
        self.validate_all(
            "CONVERT(INT, CONVERT(NUMERIC, '444.75'))",
            write={
                "mysql": "CAST(CAST('444.75' AS DECIMAL) AS SIGNED)",
                "tsql": "CAST(CAST('444.75' AS NUMERIC) AS INTEGER)",
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
                "tsql": "STRING_AGG(x, '|') WITHIN GROUP (ORDER BY z)",
                "mysql": "GROUP_CONCAT(x ORDER BY z SEPARATOR '|')",
                "sqlite": "GROUP_CONCAT(x, '|')",
                "postgres": "STRING_AGG(x, '|' ORDER BY z NULLS FIRST)",
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
            "SELECT CAST([a].[b] AS SMALLINT) FROM foo",
            write={
                "tsql": 'SELECT CAST("a"."b" AS SMALLINT) FROM foo',
                "spark": "SELECT CAST(`a`.`b` AS SMALLINT) FROM foo",
            },
        )
        self.validate_all(
            "HASHBYTES('SHA1', x)",
            read={
                "spark": "SHA(x)",
            },
            write={
                "tsql": "HASHBYTES('SHA1', x)",
                "spark": "SHA(x)",
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

    def test_types(self):
        self.validate_identity("CAST(x AS XML)")
        self.validate_identity("CAST(x AS UNIQUEIDENTIFIER)")
        self.validate_identity("CAST(x AS MONEY)")
        self.validate_identity("CAST(x AS SMALLMONEY)")
        self.validate_identity("CAST(x AS ROWVERSION)")
        self.validate_identity("CAST(x AS IMAGE)")
        self.validate_identity("CAST(x AS SQL_VARIANT)")
        self.validate_identity("CAST(x AS BIT)")
        self.validate_all(
            "CAST(x AS DATETIME2)",
            read={
                "": "CAST(x AS DATETIME)",
            },
            write={
                "mysql": "CAST(x AS DATETIME)",
                "tsql": "CAST(x AS DATETIME2)",
            },
        )
        self.validate_all(
            "CAST(x AS DATETIME2(6))",
            write={
                "hive": "CAST(x AS TIMESTAMP)",
            },
        )

    def test__types_ints(self):
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
            write={
                "hive": "CAST(X AS TINYINT)",
                "spark2": "CAST(X AS TINYINT)",
                "spark": "CAST(X AS TINYINT)",
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
                "tsql": "CAST(x AS DOUBLE)",
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
                "tsql": "CAST(x AS CHAR(1))",
            },
        )

        self.validate_all(
            "CAST(x as NVARCHAR(2))",
            write={
                "spark": "CAST(x AS VARCHAR(2))",
                "tsql": "CAST(x AS VARCHAR(2))",
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
                "tsql": "CAST(x AS TIMESTAMP(4))",
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
                "tsql": "CAST(x AS TIMESTAMPTZ)",
            },
        )

        self.validate_all(
            "CAST(x as SMALLDATETIME)",
            write={
                "spark": "CAST(x AS TIMESTAMP)",
                "tsql": "CAST(x AS DATETIME2)",
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
            "CAST(x as UNIQUEIDENTIFIER)",
            write={
                "spark": "CAST(x AS STRING)",
                "tsql": "CAST(x AS UNIQUEIDENTIFIER)",
            },
        )

        self.validate_all(
            "CAST(x as VARBINARY)",
            write={
                "spark": "CAST(x AS BINARY)",
                "tsql": "CAST(x AS VARBINARY)",
            },
        )

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))",
            write={
                "tsql": "CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIMESTAMP(4), d FLOAT(24))"
            },
        )

    def test_transaction(self):
        # BEGIN { TRAN | TRANSACTION }
        #    [ { transaction_name | @tran_name_variable }
        #    [ WITH MARK [ 'description' ] ]
        #    ]
        # [ ; ]
        self.validate_identity("BEGIN TRANSACTION")
        self.validate_all("BEGIN TRAN", write={"tsql": "BEGIN TRANSACTION"})
        self.validate_identity("BEGIN TRANSACTION transaction_name")
        self.validate_identity("BEGIN TRANSACTION @tran_name_variable")
        self.validate_identity("BEGIN TRANSACTION transaction_name WITH MARK 'description'")

    def test_commit(self):
        # COMMIT [ { TRAN | TRANSACTION }  [ transaction_name | @tran_name_variable ] ] [ WITH ( DELAYED_DURABILITY = { OFF | ON } ) ] [ ; ]

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
        # Applies to SQL Server and Azure SQL Database
        # ROLLBACK { TRAN | TRANSACTION }
        #     [ transaction_name | @tran_name_variable
        #     | savepoint_name | @savepoint_variable ]
        # [ ; ]
        self.validate_all("ROLLBACK", write={"tsql": "ROLLBACK TRANSACTION"})
        self.validate_all("ROLLBACK TRAN", write={"tsql": "ROLLBACK TRANSACTION"})
        self.validate_identity("ROLLBACK TRANSACTION")
        self.validate_identity("ROLLBACK TRANSACTION transaction_name")
        self.validate_identity("ROLLBACK TRANSACTION @tran_name_variable")

    def test_udf(self):
        self.validate_identity(
            "DECLARE @DWH_DateCreated DATETIME = CONVERT(DATETIME, getdate(), 104)"
        )
        self.validate_identity(
            "CREATE PROCEDURE foo @a INTEGER, @b INTEGER AS SELECT @a = SUM(bla) FROM baz AS bar"
        )
        self.validate_identity(
            "CREATE PROC foo @ID INTEGER, @AGE INTEGER AS SELECT DB_NAME(@ID) AS ThatDB"
        )
        self.validate_identity("CREATE PROC foo AS SELECT BAR() AS baz")
        self.validate_identity("CREATE PROCEDURE foo AS SELECT BAR() AS baz")
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
            AS
            BEGIN
                SET XACT_ABORT ON;

                DECLARE @DWH_DateCreated DATETIME = CONVERT(DATETIME, getdate(), 104);
                DECLARE @DWH_DateModified DATETIME = CONVERT(DATETIME, getdate(), 104);
                DECLARE @DWH_IdUserCreated INTEGER = SUSER_ID (SYSTEM_USER);
                DECLARE @DWH_IdUserModified INTEGER = SUSER_ID (SYSTEM_USER);

                DECLARE @SalesAmountBefore float;
                SELECT @SalesAmountBefore=SUM(SalesAmount) FROM TRANSF.[Pre_Merge_Sales_Real] S;
            END
        """

        expected_sqls = [
            'CREATE PROCEDURE "TRANSF"."SP_Merge_Sales_Real" @Loadid INTEGER, @NumberOfRows INTEGER AS BEGIN SET XACT_ABORT ON',
            "DECLARE @DWH_DateCreated DATETIME = CONVERT(DATETIME, getdate(), 104)",
            "DECLARE @DWH_DateModified DATETIME = CONVERT(DATETIME, getdate(), 104)",
            "DECLARE @DWH_IdUserCreated INTEGER = SUSER_ID (SYSTEM_USER)",
            "DECLARE @DWH_IdUserModified INTEGER = SUSER_ID (SYSTEM_USER)",
            "DECLARE @SalesAmountBefore float",
            'SELECT @SalesAmountBefore = SUM(SalesAmount) FROM TRANSF."Pre_Merge_Sales_Real" AS S',
            "END",
        ]

        for expr, expected_sql in zip(parse(sql, read="tsql"), expected_sqls):
            self.assertEqual(expr.sql(dialect="tsql"), expected_sql)

    def test_charindex(self):
        self.validate_all(
            "CHARINDEX(x, y, 9)",
            write={
                "spark": "LOCATE(x, y, 9)",
            },
        )
        self.validate_all(
            "CHARINDEX(x, y)",
            write={
                "spark": "LOCATE(x, y)",
            },
        )
        self.validate_all(
            "CHARINDEX('sub', 'testsubstring', 3)",
            write={
                "spark": "LOCATE('sub', 'testsubstring', 3)",
            },
        )
        self.validate_all(
            "CHARINDEX('sub', 'testsubstring')",
            write={
                "spark": "LOCATE('sub', 'testsubstring')",
            },
        )

    def test_len(self):
        self.validate_all("LEN(x)", write={"spark": "LENGTH(x)"})

    def test_replicate(self):
        self.validate_all("REPLICATE('x', 2)", write={"spark": "REPEAT('x', 2)"})

    def test_isnull(self):
        self.validate_all("ISNULL(x, y)", write={"spark": "COALESCE(x, y)"})

    def test_jsonvalue(self):
        self.validate_all(
            "JSON_VALUE(r.JSON, '$.Attr_INT')",
            write={"spark": "GET_JSON_OBJECT(r.JSON, '$.Attr_INT')"},
        )

    def test_datefromparts(self):
        self.validate_all(
            "SELECT DATEFROMPARTS('2020', 10, 01)",
            write={"spark": "SELECT MAKE_DATE('2020', 10, 01)"},
        )

    def test_datename(self):
        self.validate_all(
            "SELECT DATENAME(mm,'1970-01-01')",
            write={"spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'MMMM')"},
        )
        self.validate_all(
            "SELECT DATENAME(dw,'1970-01-01')",
            write={"spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'EEEE')"},
        )

    def test_datepart(self):
        self.validate_all(
            "SELECT DATEPART(month,'1970-01-01')",
            write={"spark": "SELECT DATE_FORMAT(CAST('1970-01-01' AS TIMESTAMP), 'MM')"},
        )
        self.validate_identity("DATEPART(YEAR, x)", "FORMAT(CAST(x AS DATETIME2), 'yyyy')")

    def test_convert_date_format(self):
        self.validate_all(
            "CONVERT(NVARCHAR(200), x)",
            write={
                "spark": "CAST(x AS VARCHAR(200))",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR, x)",
            write={
                "spark": "CAST(x AS VARCHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(MAX), x)",
            write={
                "spark": "CAST(x AS STRING)",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(200), x)",
            write={
                "spark": "CAST(x AS VARCHAR(200))",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR, x)",
            write={
                "spark": "CAST(x AS VARCHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(MAX), x)",
            write={
                "spark": "CAST(x AS STRING)",
            },
        )
        self.validate_all(
            "CONVERT(CHAR(40), x)",
            write={
                "spark": "CAST(x AS CHAR(40))",
            },
        )
        self.validate_all(
            "CONVERT(CHAR, x)",
            write={
                "spark": "CAST(x AS CHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(NCHAR(40), x)",
            write={
                "spark": "CAST(x AS CHAR(40))",
            },
        )
        self.validate_all(
            "CONVERT(NCHAR, x)",
            write={
                "spark": "CAST(x AS CHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR, x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(40), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(40))",
            },
        )
        self.validate_all(
            "CONVERT(VARCHAR(MAX), x, 121)",
            write={
                "spark": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR, x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(40), x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(40))",
            },
        )
        self.validate_all(
            "CONVERT(NVARCHAR(MAX), x, 121)",
            write={
                "spark": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
            },
        )
        self.validate_all(
            "CONVERT(DATE, x, 121)",
            write={
                "spark": "TO_DATE(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
            },
        )
        self.validate_all(
            "CONVERT(DATETIME, x, 121)",
            write={
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
            },
        )
        self.validate_all(
            "CONVERT(DATETIME2, x, 121)",
            write={
                "spark": "TO_TIMESTAMP(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
            },
        )
        self.validate_all(
            "CONVERT(INT, x)",
            write={
                "spark": "CAST(x AS INT)",
            },
        )
        self.validate_all(
            "CONVERT(INT, x, 121)",
            write={
                "spark": "CAST(x AS INT)",
            },
        )
        self.validate_all(
            "TRY_CONVERT(NVARCHAR, x, 121)",
            write={
                "spark": "CAST(DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS VARCHAR(30))",
            },
        )
        self.validate_all(
            "TRY_CONVERT(INT, x)",
            write={
                "spark": "CAST(x AS INT)",
            },
        )
        self.validate_all(
            "TRY_CAST(x AS INT)",
            write={
                "spark": "CAST(x AS INT)",
            },
        )
        self.validate_all(
            "CAST(x AS INT)",
            write={
                "spark": "CAST(x AS INT)",
            },
        )
        self.validate_all(
            "SELECT CONVERT(VARCHAR(10), testdb.dbo.test.x, 120) y FROM testdb.dbo.test",
            write={
                "mysql": "SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, '%Y-%m-%d %T') AS CHAR(10)) AS y FROM testdb.dbo.test",
                "spark": "SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, 'yyyy-MM-dd HH:mm:ss') AS VARCHAR(10)) AS y FROM testdb.dbo.test",
            },
        )
        self.validate_all(
            "SELECT CONVERT(VARCHAR(10), y.x) z FROM testdb.dbo.test y",
            write={
                "mysql": "SELECT CAST(y.x AS CHAR(10)) AS z FROM testdb.dbo.test AS y",
                "spark": "SELECT CAST(y.x AS VARCHAR(10)) AS z FROM testdb.dbo.test AS y",
            },
        )
        self.validate_all(
            "SELECT CAST((SELECT x FROM y) AS VARCHAR) AS test",
            write={
                "spark": "SELECT CAST((SELECT x FROM y) AS STRING) AS test",
            },
        )

    def test_add_date(self):
        self.validate_identity("SELECT DATEADD(year, 1, '2017/08/25')")

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
                "databricks": "SELECT DATEADD(week, 1, '2017/08/25')",
            },
        )

    def test_date_diff(self):
        self.validate_identity("SELECT DATEDIFF(year, '2020/01/01', '2021/01/01')")

        self.validate_all(
            "SELECT DATEDIFF(year, '2020/01/01', '2021/01/01')",
            write={
                "tsql": "SELECT DATEDIFF(year, '2020/01/01', '2021/01/01')",
                "spark": "SELECT DATEDIFF(year, '2020/01/01', '2021/01/01')",
                "spark2": "SELECT MONTHS_BETWEEN('2021/01/01', '2020/01/01') / 12",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(mm, 'start','end')",
            write={
                "databricks": "SELECT DATEDIFF(month, 'start', 'end')",
                "spark2": "SELECT MONTHS_BETWEEN('end', 'start')",
                "tsql": "SELECT DATEDIFF(month, 'start', 'end')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(quarter, 'start', 'end')",
            write={
                "databricks": "SELECT DATEDIFF(quarter, 'start', 'end')",
                "spark": "SELECT DATEDIFF(quarter, 'start', 'end')",
                "spark2": "SELECT MONTHS_BETWEEN('end', 'start') / 3",
                "tsql": "SELECT DATEDIFF(quarter, 'start', 'end')",
            },
        )

    def test_iif(self):
        self.validate_identity("SELECT IIF(cond, 'True', 'False')")
        self.validate_all(
            "SELECT IIF(cond, 'True', 'False');",
            write={
                "spark": "SELECT IF(cond, 'True', 'False')",
            },
        )

    def test_lateral_subquery(self):
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y FROM x CROSS APPLY (SELECT v, y FROM t) t(v, y)",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y FROM x, LATERAL (SELECT v, y FROM t) AS t(v, y)",
            },
        )
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y)",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y)",
            },
        )
        self.validate_all(
            "SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y) OUTER APPLY (SELECT v, y FROM t) s(v, y) LEFT JOIN z ON z.id = s.id",
            write={
                "spark": "SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) LEFT JOIN LATERAL (SELECT v, y FROM t) AS s(v, y) LEFT JOIN z ON z.id = s.id",
            },
        )

    def test_lateral_table_valued_function(self):
        self.validate_all(
            "SELECT t.x, y.z FROM x CROSS APPLY tvfTest(t.x)y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x, LATERAL TVFTEST(t.x) AS y(z)",
            },
        )
        self.validate_all(
            "SELECT t.x, y.z FROM x OUTER APPLY tvfTest(t.x)y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x LEFT JOIN LATERAL TVFTEST(t.x) AS y(z)",
            },
        )
        self.validate_all(
            "SELECT t.x, y.z FROM x OUTER APPLY a.b.tvfTest(t.x)y(z)",
            write={
                "spark": "SELECT t.x, y.z FROM x LEFT JOIN LATERAL a.b.TVFTEST(t.x) AS y(z)",
            },
        )

    def test_top(self):
        self.validate_all(
            "SELECT TOP 3 * FROM A",
            write={
                "spark": "SELECT * FROM A LIMIT 3",
            },
        )
        self.validate_all(
            "SELECT TOP (3) * FROM A",
            write={
                "spark": "SELECT * FROM A LIMIT 3",
            },
        )

    def test_format(self):
        self.validate_identity("SELECT FORMAT('01-01-1991', 'd.mm.yyyy')")
        self.validate_identity("SELECT FORMAT(12345, '###.###.###')")
        self.validate_identity("SELECT FORMAT(1234567, 'f')")
        self.validate_all(
            "SELECT FORMAT(1000000.01,'###,###.###')",
            write={"spark": "SELECT FORMAT_NUMBER(1000000.01, '###,###.###')"},
        )
        self.validate_all(
            "SELECT FORMAT(1234567, 'f')",
            write={"spark": "SELECT FORMAT_NUMBER(1234567, 'f')"},
        )
        self.validate_all(
            "SELECT FORMAT('01-01-1991', 'dd.mm.yyyy')",
            write={"spark": "SELECT DATE_FORMAT('01-01-1991', 'dd.mm.yyyy')"},
        )
        self.validate_all(
            "SELECT FORMAT(date_col, 'dd.mm.yyyy')",
            write={"spark": "SELECT DATE_FORMAT(date_col, 'dd.mm.yyyy')"},
        )
        self.validate_all(
            "SELECT FORMAT(date_col, 'm')",
            write={"spark": "SELECT DATE_FORMAT(date_col, 'MMMM d')"},
        )
        self.validate_all(
            "SELECT FORMAT(num_col, 'c')",
            write={"spark": "SELECT FORMAT_NUMBER(num_col, 'c')"},
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
            write={"spark": "LAST_DAY(CURRENT_TIMESTAMP())"},
        )
        self.validate_all(
            "EOMONTH(GETDATE(), -1)",
            write={"spark": "LAST_DAY(ADD_MONTHS(CURRENT_TIMESTAMP(), -1))"},
        )

    def test_identifier_prefixes(self):
        expr = parse_one("#x", read="tsql")
        self.assertIsInstance(expr, exp.Column)
        self.assertIsInstance(expr.this, exp.Identifier)
        self.assertEqual(expr.sql("tsql"), "#x")

        expr = parse_one("@x", read="tsql")
        self.assertIsInstance(expr, exp.Parameter)
        self.assertIsInstance(expr.this, exp.Var)
        self.assertEqual(expr.sql("tsql"), "@x")

        table = parse_one("select * from @x", read="tsql").args["from"].this
        self.assertIsInstance(table, exp.Table)
        self.assertIsInstance(table.this, exp.Parameter)
        self.assertIsInstance(table.this.this, exp.Var)

    def test_system_time(self):
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF 'foo'",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME AS OF 'foo'""",
            },
        )
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF 'foo' AS alias",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME AS OF 'foo' AS alias""",
            },
        )
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME FROM c TO d",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME FROM c TO d""",
            },
        )
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME BETWEEN c AND d",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME BETWEEN c AND d""",
            },
        )
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME CONTAINED IN (c, d)",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME CONTAINED IN (c, d)""",
            },
        )
        self.validate_all(
            "SELECT [x] FROM [a].[b] FOR SYSTEM_TIME ALL AS alias",
            write={
                "tsql": """SELECT "x" FROM "a"."b" FOR SYSTEM_TIME ALL AS alias""",
            },
        )

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
                "tsql": """SELECT "key", value FROM OPENJSON(@json, '$.path.to."sub-object"')""",
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
    Date DATETIME2 '$.Order.Date',
    Customer VARCHAR(200) '$.AccountNumber',
    Quantity INTEGER '$.Item.Quantity',
    "Order" TEXT AS JSON
)"""
            },
            pretty=True,
        )
