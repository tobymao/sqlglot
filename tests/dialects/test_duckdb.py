from sqlglot import ErrorLevel, UnsupportedError, exp, parse_one, transpile
from sqlglot.helper import logger as helper_logger
from sqlglot.optimizer.annotate_types import annotate_types
from tests.dialects.test_dialect import Validator


class TestDuckDB(Validator):
    dialect = "duckdb"

    def test_duckdb(self):
        query = "WITH _data AS (SELECT [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}] AS col) SELECT t.col['b'] FROM _data, UNNEST(_data.col) AS t(col) WHERE t.col['a'] = 1"
        expr = annotate_types(self.validate_identity(query))
        self.assertEqual(
            expr.sql(dialect="bigquery"),
            "WITH _data AS (SELECT [STRUCT(1 AS a, 2 AS b), STRUCT(2 AS a, 3 AS b)] AS col) SELECT col.b FROM _data, UNNEST(_data.col) AS col WHERE col.a = 1",
        )

        self.validate_all(
            'STRUCT_PACK("a b" := 1)',
            write={
                "duckdb": "{'a b': 1}",
                "spark": "STRUCT(1 AS `a b`)",
                "snowflake": "OBJECT_CONSTRUCT('a b', 1)",
            },
        )
        self.validate_all(
            "ARRAY_TO_STRING(arr, delim)",
            read={
                "bigquery": "ARRAY_TO_STRING(arr, delim)",
                "postgres": "ARRAY_TO_STRING(arr, delim)",
                "presto": "ARRAY_JOIN(arr, delim)",
                "snowflake": "ARRAY_TO_STRING(arr, delim)",
                "spark": "ARRAY_JOIN(arr, delim)",
            },
            write={
                "bigquery": "ARRAY_TO_STRING(arr, delim)",
                "duckdb": "ARRAY_TO_STRING(arr, delim)",
                "postgres": "ARRAY_TO_STRING(arr, delim)",
                "presto": "ARRAY_JOIN(arr, delim)",
                "snowflake": "ARRAY_TO_STRING(arr, delim)",
                "spark": "ARRAY_JOIN(arr, delim)",
                "tsql": "STRING_AGG(arr, delim)",
            },
        )
        self.validate_all(
            "SELECT SUM(X) OVER (ORDER BY x)",
            write={
                "bigquery": "SELECT SUM(X) OVER (ORDER BY x NULLS LAST)",
                "duckdb": "SELECT SUM(X) OVER (ORDER BY x)",
                "mysql": "SELECT SUM(X) OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x)",
            },
        )
        self.validate_all(
            "SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            write={
                "bigquery": "SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
                "duckdb": "SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
                "mysql": "SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            },
        )
        self.validate_all(
            "SELECT * FROM x ORDER BY 1 NULLS LAST",
            write={
                "duckdb": "SELECT * FROM x ORDER BY 1",
                "mysql": "SELECT * FROM x ORDER BY 1",
            },
        )

        self.validate_all(
            "CREATE TEMPORARY FUNCTION f1(a, b) AS (a + b)",
            read={"bigquery": "CREATE TEMP FUNCTION f1(a INT64, b INT64) AS (a + b)"},
        )
        self.validate_identity("SELECT 1 WHERE x > $1")
        self.validate_identity("SELECT 1 WHERE x > $name")
        self.validate_identity("""SELECT '{"x": 1}' -> c FROM t""")

        self.assertEqual(
            parse_one("select * from t limit (select 5)").sql(dialect="duckdb"),
            exp.select("*").from_("t").limit(exp.select("5").subquery()).sql(dialect="duckdb"),
        )
        self.assertEqual(
            parse_one("select * from t offset (select 5)").sql(dialect="duckdb"),
            exp.select("*").from_("t").offset(exp.select("5").subquery()).sql(dialect="duckdb"),
        )

        self.validate_all(
            "{'a': 1, 'b': '2'}", write={"presto": "CAST(ROW(1, '2') AS ROW(a INTEGER, b VARCHAR))"}
        )
        self.validate_all(
            "struct_pack(a := 1, b := 2)",
            write={"presto": "CAST(ROW(1, 2) AS ROW(a INTEGER, b INTEGER))"},
        )

        self.validate_all(
            "struct_pack(a := 1, b := x)",
            write={
                "duckdb": "{'a': 1, 'b': x}",
                "presto": UnsupportedError,
            },
        )

        for join_type in ("SEMI", "ANTI"):
            exists = "EXISTS" if join_type == "SEMI" else "NOT EXISTS"

            self.validate_all(
                f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                write={
                    "bigquery": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "clickhouse": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "databricks": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "doris": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "drill": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "duckdb": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "hive": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "mysql": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "oracle": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "postgres": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "presto": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "redshift": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "snowflake": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "spark": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "sqlite": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "starrocks": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "teradata": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "trino": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                    "tsql": f"SELECT * FROM t1 WHERE {exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)",
                },
            )
            self.validate_all(
                f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                read={
                    "duckdb": f"SELECT * FROM t1 {join_type} JOIN t2 ON t1.x = t2.x",
                    "spark": f"SELECT * FROM t1 LEFT {join_type} JOIN t2 ON t1.x = t2.x",
                },
            )

        self.validate_identity("""SELECT '{"duck": [1, 2, 3]}' -> '$.duck[#-1]'""")
        self.validate_all(
            """SELECT JSON_EXTRACT('{"duck": [1, 2, 3]}', '/duck/0')""",
            write={
                "": """SELECT JSON_EXTRACT('{"duck": [1, 2, 3]}', '/duck/0')""",
                "duckdb": """SELECT '{"duck": [1, 2, 3]}' -> '/duck/0'""",
            },
        )
        self.validate_all(
            """SELECT JSON('{"fruit":"banana"}') -> 'fruit'""",
            write={
                "duckdb": """SELECT JSON('{"fruit":"banana"}') -> '$.fruit'""",
                "snowflake": """SELECT GET_PATH(PARSE_JSON('{"fruit":"banana"}'), 'fruit')""",
            },
        )
        self.validate_all(
            """SELECT JSON('{"fruit": {"foo": "banana"}}') -> 'fruit' -> 'foo'""",
            write={
                "duckdb": """SELECT JSON('{"fruit": {"foo": "banana"}}') -> '$.fruit' -> '$.foo'""",
                "snowflake": """SELECT GET_PATH(GET_PATH(PARSE_JSON('{"fruit": {"foo": "banana"}}'), 'fruit'), 'foo')""",
            },
        )
        self.validate_all(
            "SELECT {'bla': column1, 'foo': column2, 'bar': column3} AS data FROM source_table",
            read={
                "bigquery": "SELECT STRUCT(column1 AS bla, column2 AS foo, column3 AS bar) AS data FROM source_table",
                "duckdb": "SELECT {'bla': column1, 'foo': column2, 'bar': column3} AS data FROM source_table",
            },
            write={
                "bigquery": "SELECT STRUCT(column1 AS bla, column2 AS foo, column3 AS bar) AS data FROM source_table",
            },
        )
        self.validate_all(
            "WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(x) FILTER (WHERE x > 1) FROM cte",
            write={
                "duckdb": "WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(x) FILTER(WHERE x > 1) FROM cte",
                "snowflake": "WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(IFF(x > 1, x, NULL)) FROM cte",
            },
        )
        self.validate_all(
            "SELECT AVG(x) FILTER (WHERE TRUE) FROM t",
            write={
                "duckdb": "SELECT AVG(x) FILTER(WHERE TRUE) FROM t",
                "snowflake": "SELECT AVG(IFF(TRUE, x, NULL)) FROM t",
            },
        )
        self.validate_all(
            "SELECT UNNEST(ARRAY[1, 2, 3]), UNNEST(ARRAY[4, 5]), UNNEST(ARRAY[6])",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_3, col_2, NULL) AS col_2, IF(pos = pos_4, col_3, NULL) AS col_3 FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([1, 2, 3]), ARRAY_LENGTH([4, 5]), ARRAY_LENGTH([6])) - 1)) AS pos CROSS JOIN UNNEST([1, 2, 3]) AS col WITH OFFSET AS pos_2 CROSS JOIN UNNEST([4, 5]) AS col_2 WITH OFFSET AS pos_3 CROSS JOIN UNNEST([6]) AS col_3 WITH OFFSET AS pos_4 WHERE ((pos = pos_2 OR (pos > (ARRAY_LENGTH([1, 2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([1, 2, 3]) - 1))) AND (pos = pos_3 OR (pos > (ARRAY_LENGTH([4, 5]) - 1) AND pos_3 = (ARRAY_LENGTH([4, 5]) - 1)))) AND (pos = pos_4 OR (pos > (ARRAY_LENGTH([6]) - 1) AND pos_4 = (ARRAY_LENGTH([6]) - 1)))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col, IF(_u.pos = _u_3.pos_3, _u_3.col_2) AS col_2, IF(_u.pos = _u_4.pos_4, _u_4.col_3) AS col_3 FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1, 2, 3]), CARDINALITY(ARRAY[4, 5]), CARDINALITY(ARRAY[6])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS _u_2(col, pos_2) CROSS JOIN UNNEST(ARRAY[4, 5]) WITH ORDINALITY AS _u_3(col_2, pos_3) CROSS JOIN UNNEST(ARRAY[6]) WITH ORDINALITY AS _u_4(col_3, pos_4) WHERE ((_u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1, 2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1, 2, 3]))) AND (_u.pos = _u_3.pos_3 OR (_u.pos > CARDINALITY(ARRAY[4, 5]) AND _u_3.pos_3 = CARDINALITY(ARRAY[4, 5])))) AND (_u.pos = _u_4.pos_4 OR (_u.pos > CARDINALITY(ARRAY[6]) AND _u_4.pos_4 = CARDINALITY(ARRAY[6])))",
            },
        )

        self.validate_all(
            "SELECT UNNEST(ARRAY[1, 2, 3]), UNNEST(ARRAY[4, 5]), UNNEST(ARRAY[6]) FROM x",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_3, col_2, NULL) AS col_2, IF(pos = pos_4, col_3, NULL) AS col_3 FROM x CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([1, 2, 3]), ARRAY_LENGTH([4, 5]), ARRAY_LENGTH([6])) - 1)) AS pos CROSS JOIN UNNEST([1, 2, 3]) AS col WITH OFFSET AS pos_2 CROSS JOIN UNNEST([4, 5]) AS col_2 WITH OFFSET AS pos_3 CROSS JOIN UNNEST([6]) AS col_3 WITH OFFSET AS pos_4 WHERE ((pos = pos_2 OR (pos > (ARRAY_LENGTH([1, 2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([1, 2, 3]) - 1))) AND (pos = pos_3 OR (pos > (ARRAY_LENGTH([4, 5]) - 1) AND pos_3 = (ARRAY_LENGTH([4, 5]) - 1)))) AND (pos = pos_4 OR (pos > (ARRAY_LENGTH([6]) - 1) AND pos_4 = (ARRAY_LENGTH([6]) - 1)))",
                "presto": "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col, IF(_u.pos = _u_3.pos_3, _u_3.col_2) AS col_2, IF(_u.pos = _u_4.pos_4, _u_4.col_3) AS col_3 FROM x CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1, 2, 3]), CARDINALITY(ARRAY[4, 5]), CARDINALITY(ARRAY[6])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS _u_2(col, pos_2) CROSS JOIN UNNEST(ARRAY[4, 5]) WITH ORDINALITY AS _u_3(col_2, pos_3) CROSS JOIN UNNEST(ARRAY[6]) WITH ORDINALITY AS _u_4(col_3, pos_4) WHERE ((_u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1, 2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1, 2, 3]))) AND (_u.pos = _u_3.pos_3 OR (_u.pos > CARDINALITY(ARRAY[4, 5]) AND _u_3.pos_3 = CARDINALITY(ARRAY[4, 5])))) AND (_u.pos = _u_4.pos_4 OR (_u.pos > CARDINALITY(ARRAY[6]) AND _u_4.pos_4 = CARDINALITY(ARRAY[6])))",
            },
        )
        self.validate_all(
            "SELECT UNNEST(x) + 1",
            write={
                "bigquery": "SELECT IF(pos = pos_2, col, NULL) + 1 AS col FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1)) AS pos CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(x) - 1) AND pos_2 = (ARRAY_LENGTH(x) - 1))",
            },
        )
        self.validate_all(
            "SELECT UNNEST(x) + 1 AS y",
            write={
                "bigquery": "SELECT IF(pos = pos_2, y, NULL) + 1 AS y FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1)) AS pos CROSS JOIN UNNEST(x) AS y WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(x) - 1) AND pos_2 = (ARRAY_LENGTH(x) - 1))",
            },
        )

        self.validate_identity("INSERT INTO x BY NAME SELECT 1 AS y")
        self.validate_identity("SELECT 1 AS x UNION ALL BY NAME SELECT 2 AS x")
        self.validate_identity("SELECT SUM(x) FILTER (x = 1)", "SELECT SUM(x) FILTER(WHERE x = 1)")

        # https://github.com/duckdb/duckdb/releases/tag/v0.8.0
        self.assertEqual(
            parse_one("a / b", read="duckdb").assert_is(exp.Div).sql(dialect="duckdb"), "a / b"
        )
        self.assertEqual(
            parse_one("a // b", read="duckdb").assert_is(exp.IntDiv).sql(dialect="duckdb"), "a // b"
        )

        self.validate_identity("SELECT df1.*, df2.* FROM df1 POSITIONAL JOIN df2")
        self.validate_identity("MAKE_TIMESTAMP(1992, 9, 20, 13, 34, 27.123456)")
        self.validate_identity("MAKE_TIMESTAMP(1667810584123456)")
        self.validate_identity("SELECT EPOCH_MS(10) AS t")
        self.validate_identity("SELECT MAKE_TIMESTAMP(10) AS t")
        self.validate_identity("SELECT TO_TIMESTAMP(10) AS t")
        self.validate_identity("SELECT UNNEST(column, recursive := TRUE) FROM table")
        self.validate_identity("VAR_POP(a)")
        self.validate_identity("SELECT * FROM foo ASOF LEFT JOIN bar ON a = b")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Year USING FIRST(Population)")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population) GROUP BY Country")
        self.validate_identity("PIVOT Cities ON Country, Name USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Country || '_' || Name USING SUM(Population)")
        self.validate_identity("PIVOT Cities ON Year USING SUM(Population) GROUP BY Country, Name")
        self.validate_identity("SELECT {'a': 1} AS x")
        self.validate_identity("SELECT {'a': {'b': {'c': 1}}, 'd': {'e': 2}} AS x")
        self.validate_identity("SELECT {'x': 1, 'y': 2, 'z': 3}")
        self.validate_identity("SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345}")
        self.validate_identity("SELECT ROW(x, x + 1, y) FROM (SELECT 1 AS x, 'a' AS y)")
        self.validate_identity("SELECT (x, x + 1, y) FROM (SELECT 1 AS x, 'a' AS y)")
        self.validate_identity("SELECT a.x FROM (SELECT {'x': 1, 'y': 2, 'z': 3} AS a)")
        self.validate_identity("FROM  x SELECT x UNION SELECT 1", "SELECT x FROM x UNION SELECT 1")
        self.validate_identity("FROM (FROM tbl)", "SELECT * FROM (SELECT * FROM tbl)")
        self.validate_identity("FROM tbl", "SELECT * FROM tbl")
        self.validate_identity("x -> '$.family'")
        self.validate_identity("CREATE TABLE color (name ENUM('RED', 'GREEN', 'BLUE'))")
        self.validate_identity(
            "SELECT * FROM x LEFT JOIN UNNEST(y)", "SELECT * FROM x LEFT JOIN UNNEST(y) ON TRUE"
        )
        self.validate_identity(
            """SELECT '{"foo": [1, 2, 3]}' -> 'foo' -> 0""",
            """SELECT '{"foo": [1, 2, 3]}' -> '$.foo' -> '$[0]'""",
        )
        self.validate_identity(
            "JSON_EXTRACT(x, '$.family')",
            "x -> '$.family'",
        )
        self.validate_identity(
            "JSON_EXTRACT_PATH(x, '$.family')",
            "x -> '$.family'",
        )
        self.validate_identity(
            "JSON_EXTRACT_STRING(x, '$.family')",
            "x ->> '$.family'",
        )
        self.validate_identity(
            "JSON_EXTRACT_PATH_TEXT(x, '$.family')",
            "x ->> '$.family'",
        )
        self.validate_identity(
            "ATTACH DATABASE ':memory:' AS new_database", check_command_warning=True
        )
        self.validate_identity(
            "SELECT {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'}"
        )
        self.validate_identity(
            "SELECT a['x space'] FROM (SELECT {'x space': 1, 'y': 2, 'z': 3} AS a)"
        )
        self.validate_identity(
            "PIVOT Cities ON Year IN (2000, 2010) USING SUM(Population) GROUP BY Country"
        )
        self.validate_identity(
            "PIVOT Cities ON Year USING SUM(Population) AS total, MAX(Population) AS max GROUP BY Country"
        )
        self.validate_identity(
            "WITH pivot_alias AS (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) SELECT * FROM pivot_alias"
        )
        self.validate_identity(
            "SELECT * FROM (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) AS pivot_alias"
        )

        self.validate_all("0b1010", write={"": "0 AS b1010"})
        self.validate_all("0x1010", write={"": "0 AS x1010"})
        self.validate_all("x ~ y", write={"duckdb": "REGEXP_MATCHES(x, y)"})
        self.validate_all("SELECT * FROM 'x.y'", write={"duckdb": 'SELECT * FROM "x.y"'})
        self.validate_all(
            "SELECT STRFTIME(CAST('2020-01-01' AS TIMESTAMP), CONCAT('%Y', '%m'))",
            write={
                "duckdb": "SELECT STRFTIME(CAST('2020-01-01' AS TIMESTAMP), CONCAT('%Y', '%m'))",
                "tsql": "SELECT FORMAT(CAST('2020-01-01' AS DATETIME2), CONCAT('yyyy', 'MM'))",
            },
        )
        self.validate_all(
            "SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2'))",
            read={
                "duckdb": "SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2'))",
                "snowflake": "SELECT * FROM produce PIVOT(SUM(produce.sales) FOR produce.quarter IN ('Q1', 'Q2'))",
            },
        )
        self.validate_all(
            "SELECT UNNEST([1, 2, 3])",
            write={
                "duckdb": "SELECT UNNEST([1, 2, 3])",
                "snowflake": "SELECT IFF(_u.pos = _u_2.pos_2, _u_2.col, NULL) AS col FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (GREATEST(ARRAY_SIZE([1, 2, 3])) - 1) + 1))) AS _u(seq, key, path, index, pos, this) CROSS JOIN TABLE(FLATTEN(INPUT => [1, 2, 3])) AS _u_2(seq, key, path, pos_2, col, this) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > (ARRAY_SIZE([1, 2, 3]) - 1) AND _u_2.pos_2 = (ARRAY_SIZE([1, 2, 3]) - 1))",
            },
        )
        self.validate_all(
            "VAR_POP(x)",
            read={
                "": "VARIANCE_POP(x)",
            },
            write={
                "": "VARIANCE_POP(x)",
            },
        )
        self.validate_all(
            "DATE_DIFF('DAY', CAST(b AS DATE), CAST(a AS DATE))",
            read={
                "duckdb": "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
                "hive": "DATEDIFF(a, b)",
                "spark": "DATEDIFF(a, b)",
                "spark2": "DATEDIFF(a, b)",
            },
        )
        self.validate_all(
            "XOR(a, b)",
            read={
                "": "a ^ b",
                "bigquery": "a ^ b",
                "presto": "BITWISE_XOR(a, b)",
                "postgres": "a # b",
            },
            write={
                "": "a ^ b",
                "bigquery": "a ^ b",
                "duckdb": "XOR(a, b)",
                "presto": "BITWISE_XOR(a, b)",
                "postgres": "a # b",
            },
        )
        self.validate_all(
            "PIVOT_WIDER Cities ON Year USING SUM(Population)",
            write={"duckdb": "PIVOT Cities ON Year USING SUM(Population)"},
        )
        self.validate_all(
            "WITH t AS (SELECT 1) FROM t", write={"duckdb": "WITH t AS (SELECT 1) SELECT * FROM t"}
        )
        self.validate_all(
            "WITH t AS (SELECT 1) SELECT * FROM (FROM t)",
            write={"duckdb": "WITH t AS (SELECT 1) SELECT * FROM (SELECT * FROM t)"},
        )
        self.validate_all(
            """SELECT DATEDIFF('day', t1."A", t1."B") FROM "table" AS t1""",
            write={
                "duckdb": """SELECT DATE_DIFF('DAY', t1."A", t1."B") FROM "table" AS t1""",
                "trino": """SELECT DATE_DIFF('DAY', t1."A", t1."B") FROM "table" AS t1""",
            },
        )
        self.validate_all(
            "SELECT DATE_DIFF('day', DATE '2020-01-01', DATE '2020-01-05')",
            write={
                "duckdb": "SELECT DATE_DIFF('DAY', CAST('2020-01-01' AS DATE), CAST('2020-01-05' AS DATE))",
                "trino": "SELECT DATE_DIFF('DAY', CAST('2020-01-01' AS DATE), CAST('2020-01-05' AS DATE))",
            },
        )
        self.validate_all(
            "WITH 'x' AS (SELECT 1) SELECT * FROM x",
            write={"duckdb": 'WITH "x" AS (SELECT 1) SELECT * FROM x'},
        )
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS table (cola INT, colb STRING) USING ICEBERG PARTITIONED BY (colb)",
            write={
                "duckdb": "CREATE TABLE IF NOT EXISTS table (cola INT, colb TEXT)",
            },
        )
        self.validate_all(
            "CREATE TABLE IF NOT EXISTS table (cola INT COMMENT 'cola', colb STRING) USING ICEBERG PARTITIONED BY (colb)",
            write={
                "duckdb": "CREATE TABLE IF NOT EXISTS table (cola INT, colb TEXT)",
            },
        )
        self.validate_all(
            "[0, 1, 2]",
            read={
                "spark": "ARRAY(0, 1, 2)",
            },
            write={
                "bigquery": "[0, 1, 2]",
                "duckdb": "[0, 1, 2]",
                "presto": "ARRAY[0, 1, 2]",
                "spark": "ARRAY(0, 1, 2)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_LENGTH([0], 1) AS x",
            write={"duckdb": "SELECT ARRAY_LENGTH([0], 1) AS x"},
        )
        self.validate_identity("REGEXP_REPLACE(this, pattern, replacement, modifiers)")
        self.validate_all(
            "REGEXP_MATCHES(x, y)",
            write={
                "duckdb": "REGEXP_MATCHES(x, y)",
                "presto": "REGEXP_LIKE(x, y)",
                "hive": "x RLIKE y",
                "spark": "x RLIKE y",
            },
        )
        self.validate_all(
            "STR_SPLIT(x, 'a')",
            write={
                "duckdb": "STR_SPLIT(x, 'a')",
                "presto": "SPLIT(x, 'a')",
                "hive": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
                "spark": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
            },
        )
        self.validate_all(
            "STRING_TO_ARRAY(x, 'a')",
            write={
                "duckdb": "STR_SPLIT(x, 'a')",
                "presto": "SPLIT(x, 'a')",
                "hive": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
                "spark": "SPLIT(x, CONCAT('\\\\Q', 'a'))",
            },
        )
        self.validate_all(
            "STR_SPLIT_REGEX(x, 'a')",
            write={
                "duckdb": "STR_SPLIT_REGEX(x, 'a')",
                "presto": "REGEXP_SPLIT(x, 'a')",
                "hive": "SPLIT(x, 'a')",
                "spark": "SPLIT(x, 'a')",
            },
        )
        self.validate_all(
            "STRUCT_EXTRACT(x, 'abc')",
            write={
                "duckdb": "STRUCT_EXTRACT(x, 'abc')",
                "presto": "x.abc",
                "hive": "x.abc",
                "postgres": "x.abc",
                "redshift": "x.abc",
                "spark": "x.abc",
            },
        )
        self.validate_all(
            "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
            write={
                "duckdb": "STRUCT_EXTRACT(STRUCT_EXTRACT(x, 'y'), 'abc')",
                "presto": "x.y.abc",
                "hive": "x.y.abc",
                "spark": "x.y.abc",
            },
        )
        self.validate_all(
            "QUANTILE(x, 0.5)",
            write={
                "duckdb": "QUANTILE(x, 0.5)",
                "presto": "APPROX_PERCENTILE(x, 0.5)",
                "hive": "PERCENTILE(x, 0.5)",
                "spark": "PERCENTILE(x, 0.5)",
            },
        )
        self.validate_all(
            "UNNEST(x)",
            read={
                "spark": "EXPLODE(x)",
            },
            write={
                "duckdb": "UNNEST(x)",
                "spark": "EXPLODE(x)",
            },
        )
        self.validate_all(
            "1d",
            write={
                "duckdb": "1 AS d",
                "spark": "1 AS d",
            },
        )
        self.validate_all(
            "POWER(TRY_CAST(2 AS SMALLINT), 3)",
            read={
                "hive": "POW(2S, 3)",
                "spark": "POW(2S, 3)",
            },
        )
        self.validate_all(
            "LIST_SUM([1, 2])",
            read={
                "spark": "ARRAY_SUM(ARRAY(1, 2))",
            },
        )
        self.validate_all(
            "IF((y) <> 0, (x) / (y), NULL)",
            read={
                "bigquery": "SAFE_DIVIDE(x, y)",
            },
        )
        self.validate_all(
            "STRUCT_PACK(x := 1, y := '2')",
            write={
                "bigquery": "STRUCT(1 AS x, '2' AS y)",
                "duckdb": "{'x': 1, 'y': '2'}",
                "spark": "STRUCT(1 AS x, '2' AS y)",
            },
        )
        self.validate_all(
            "STRUCT_PACK(key1 := 'value1', key2 := 42)",
            write={
                "bigquery": "STRUCT('value1' AS key1, 42 AS key2)",
                "duckdb": "{'key1': 'value1', 'key2': 42}",
                "spark": "STRUCT('value1' AS key1, 42 AS key2)",
            },
        )
        self.validate_all(
            "ARRAY_SORT(x)",
            write={
                "duckdb": "ARRAY_SORT(x)",
                "presto": "ARRAY_SORT(x)",
                "hive": "SORT_ARRAY(x)",
                "spark": "SORT_ARRAY(x)",
            },
        )
        self.validate_all(
            "ARRAY_REVERSE_SORT(x)",
            write={
                "duckdb": "ARRAY_REVERSE_SORT(x)",
                "presto": "ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SORT_ARRAY(x, FALSE)",
                "spark": "SORT_ARRAY(x, FALSE)",
            },
        )
        self.validate_all(
            "LIST_REVERSE_SORT(x)",
            write={
                "duckdb": "ARRAY_REVERSE_SORT(x)",
                "presto": "ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)",
                "hive": "SORT_ARRAY(x, FALSE)",
                "spark": "SORT_ARRAY(x, FALSE)",
            },
        )
        self.validate_all(
            "LIST_SORT(x)",
            write={
                "duckdb": "ARRAY_SORT(x)",
                "presto": "ARRAY_SORT(x)",
                "hive": "SORT_ARRAY(x)",
                "spark": "SORT_ARRAY(x)",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST",
                "duckdb": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname",
            },
        )
        self.validate_all(
            "MONTH('2021-03-01')",
            write={
                "duckdb": "MONTH('2021-03-01')",
                "presto": "MONTH('2021-03-01')",
                "hive": "MONTH('2021-03-01')",
                "spark": "MONTH('2021-03-01')",
            },
        )
        self.validate_all(
            "ARRAY_CONCAT([1, 2], [3, 4])",
            read={
                "bigquery": "ARRAY_CONCAT([1, 2], [3, 4])",
                "postgres": "ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4])",
                "snowflake": "ARRAY_CAT([1, 2], [3, 4])",
            },
            write={
                "bigquery": "ARRAY_CONCAT([1, 2], [3, 4])",
                "duckdb": "ARRAY_CONCAT([1, 2], [3, 4])",
                "hive": "CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
                "postgres": "ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4])",
                "presto": "CONCAT(ARRAY[1, 2], ARRAY[3, 4])",
                "snowflake": "ARRAY_CAT([1, 2], [3, 4])",
                "spark": "CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
            },
        )
        self.validate_all(
            "SELECT CAST(CAST(x AS DATE) AS DATE) + INTERVAL 1 DAY",
            read={
                "hive": "SELECT DATE_ADD(TO_DATE(x), 1)",
            },
        )
        self.validate_all(
            "SELECT CAST('2018-01-01 00:00:00' AS DATE) + INTERVAL 3 DAY",
            read={
                "hive": "SELECT DATE_ADD('2018-01-01 00:00:00', 3)",
            },
            write={
                "duckdb": "SELECT CAST('2018-01-01 00:00:00' AS DATE) + INTERVAL '3' DAY",
                "hive": "SELECT CAST('2018-01-01 00:00:00' AS DATE) + INTERVAL '3' DAY",
            },
        )
        self.validate_all(
            "SELECT CAST('2020-05-06' AS DATE) - INTERVAL 5 DAY",
            read={"bigquery": "SELECT DATE_SUB(CAST('2020-05-06' AS DATE), INTERVAL 5 DAY)"},
        )
        self.validate_all(
            "SELECT CAST('2020-05-06' AS DATE) + INTERVAL 5 DAY",
            read={"bigquery": "SELECT DATE_ADD(CAST('2020-05-06' AS DATE), INTERVAL 5 DAY)"},
        )
        self.validate_identity("SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY y DESC) FROM t")
        self.validate_identity("SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY y DESC) FROM t")
        self.validate_all(
            "SELECT QUANTILE_CONT(x, q) FROM t",
            write={
                "duckdb": "SELECT QUANTILE_CONT(x, q) FROM t",
                "postgres": "SELECT PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) FROM t",
                "snowflake": "SELECT PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) FROM t",
            },
        )
        self.validate_all(
            "SELECT QUANTILE_DISC(x, q) FROM t",
            write={
                "duckdb": "SELECT QUANTILE_DISC(x, q) FROM t",
                "postgres": "SELECT PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x) FROM t",
                "snowflake": "SELECT PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x) FROM t",
            },
        )
        self.validate_all(
            "SELECT MEDIAN(x) FROM t",
            write={
                "duckdb": "SELECT QUANTILE_CONT(x, 0.5) FROM t",
                "postgres": "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM t",
                "snowflake": "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM t",
            },
        )

        with self.assertRaises(UnsupportedError):
            transpile(
                "SELECT REGEXP_EXTRACT(a, 'pattern', 1) from table",
                read="bigquery",
                write="duckdb",
                unsupported_level=ErrorLevel.IMMEDIATE,
            )

        self.validate_identity("SELECT ISNAN(x)")

        self.validate_all(
            "SELECT COUNT_IF(x)",
            write={
                "duckdb": "SELECT COUNT_IF(x)",
                "bigquery": "SELECT COUNTIF(x)",
            },
        )

        self.validate_identity("SELECT * FROM RANGE(1, 5, 10)")
        self.validate_identity("SELECT * FROM GENERATE_SERIES(2, 13, 4)")

        self.validate_all(
            "WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM RANGE(1, 5) t(i)) SELECT * FROM t",
            write={
                "duckdb": "WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM RANGE(1, 5) AS t(i)) SELECT * FROM t",
                "sqlite": "WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM (SELECT value AS i FROM GENERATE_SERIES(1, 5)) AS t) SELECT * FROM t",
            },
        )

        self.validate_identity(
            """SELECT i FROM RANGE(5) AS _(i) ORDER BY i ASC""",
            """SELECT i FROM RANGE(0, 5) AS _(i) ORDER BY i ASC""",
        )

        self.validate_identity(
            """SELECT i FROM GENERATE_SERIES(12) AS _(i) ORDER BY i ASC""",
            """SELECT i FROM GENERATE_SERIES(0, 12) AS _(i) ORDER BY i ASC""",
        )

    def test_array_index(self):
        with self.assertLogs(helper_logger) as cm:
            self.validate_all(
                "SELECT some_arr[1] AS first FROM blah",
                read={
                    "bigquery": "SELECT some_arr[0] AS first FROM blah",
                },
                write={
                    "bigquery": "SELECT some_arr[0] AS first FROM blah",
                    "duckdb": "SELECT some_arr[1] AS first FROM blah",
                    "presto": "SELECT some_arr[1] AS first FROM blah",
                },
            )
            self.validate_identity(
                "[x.STRING_SPLIT(' ')[1] FOR x IN ['1', '2', 3] IF x.CONTAINS('1')]"
            )

            self.assertEqual(
                cm.output,
                [
                    "WARNING:sqlglot:Applying array index offset (-1)",
                    "WARNING:sqlglot:Applying array index offset (1)",
                    "WARNING:sqlglot:Applying array index offset (1)",
                    "WARNING:sqlglot:Applying array index offset (1)",
                    "WARNING:sqlglot:Applying array index offset (-1)",
                    "WARNING:sqlglot:Applying array index offset (1)",
                ],
            )

    def test_time(self):
        self.validate_identity("SELECT CURRENT_DATE")
        self.validate_identity("SELECT CURRENT_TIMESTAMP")

        self.validate_all(
            "SELECT MAKE_DATE(2016, 12, 25)", read={"bigquery": "SELECT DATE(2016, 12, 25)"}
        )
        self.validate_all(
            "SELECT CAST(CAST('2016-12-25 23:59:59' AS DATETIME) AS DATE)",
            read={"bigquery": "SELECT DATE(DATETIME '2016-12-25 23:59:59')"},
        )
        self.validate_all(
            "SELECT STRPTIME(STRFTIME(CAST(CAST('2016-12-25' AS TIMESTAMPTZ) AS DATE), '%d/%m/%Y') || ' ' || 'America/Los_Angeles', '%d/%m/%Y %Z')",
            read={
                "bigquery": "SELECT DATE(TIMESTAMP '2016-12-25', 'America/Los_Angeles')",
            },
        )
        self.validate_all(
            "SELECT CAST(CAST(STRPTIME('05/06/2020', '%m/%d/%Y') AS DATE) AS DATE)",
            read={"bigquery": "SELECT DATE(PARSE_DATE('%m/%d/%Y', '05/06/2020'))"},
        )
        self.validate_all(
            "SELECT CAST('2020-01-01' AS DATE) + INTERVAL (-1) DAY",
            read={"mysql": "SELECT DATE '2020-01-01' + INTERVAL -1 DAY"},
        )
        self.validate_all(
            "SELECT INTERVAL '1 quarter'",
            write={"duckdb": "SELECT (90 * INTERVAL '1' DAY)"},
        )
        self.validate_all(
            "SELECT ((DATE_TRUNC('DAY', CAST(CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP) + INTERVAL (0 - MOD((DAYOFWEEK(CAST(CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)) DAY) + (7 * INTERVAL (-5) DAY))) AS t1",
            read={
                "presto": "SELECT ((DATE_ADD('week', -5, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AS t1",
            },
        )
        self.validate_all(
            "EPOCH(x)",
            read={
                "presto": "TO_UNIXTIME(x)",
            },
            write={
                "bigquery": "TIME_TO_UNIX(x)",
                "duckdb": "EPOCH(x)",
                "presto": "TO_UNIXTIME(x)",
                "spark": "UNIX_TIMESTAMP(x)",
            },
        )
        self.validate_all(
            "EPOCH_MS(x)",
            write={
                "bigquery": "TIMESTAMP_MILLIS(x)",
                "duckdb": "EPOCH_MS(x)",
                "presto": "FROM_UNIXTIME(CAST(x AS DOUBLE) / POW(10, 3))",
                "spark": "TIMESTAMP_MILLIS(x)",
            },
        )
        self.validate_all(
            "STRFTIME(x, '%y-%-m-%S')",
            write={
                "bigquery": "FORMAT_DATE('%y-%-m-%S', x)",
                "duckdb": "STRFTIME(x, '%y-%-m-%S')",
                "postgres": "TO_CHAR(x, 'YY-FMMM-SS')",
                "presto": "DATE_FORMAT(x, '%y-%c-%s')",
                "spark": "DATE_FORMAT(x, 'yy-M-ss')",
            },
        )
        self.validate_all(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            write={
                "bigquery": "FORMAT_DATE('%Y-%m-%d %H:%M:%S', x)",
                "duckdb": "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
                "presto": "DATE_FORMAT(x, '%Y-%m-%d %T')",
                "hive": "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%y-%-m')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%y-%-m', x)",
                "duckdb": "STRPTIME(x, '%y-%-m')",
                "presto": "DATE_PARSE(x, '%y-%c')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'yy-M')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'yy-M')",
            },
        )
        self.validate_all(
            "TO_TIMESTAMP(x)",
            write={
                "bigquery": "TIMESTAMP_SECONDS(x)",
                "duckdb": "TO_TIMESTAMP(x)",
                "presto": "FROM_UNIXTIME(x)",
                "hive": "FROM_UNIXTIME(x)",
            },
        )
        self.validate_all(
            "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
            write={
                "bigquery": "PARSE_TIMESTAMP('%-m/%-d/%y %-I:%M %p', x)",
                "duckdb": "STRPTIME(x, '%-m/%-d/%y %-I:%M %p')",
                "presto": "DATE_PARSE(x, '%c/%e/%y %l:%i %p')",
                "hive": "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, 'M/d/yy h:mm a')) AS TIMESTAMP)",
                "spark": "TO_TIMESTAMP(x, 'M/d/yy h:mm a')",
            },
        )
        self.validate_all(
            "CAST(start AS TIMESTAMPTZ) AT TIME ZONE 'America/New_York'",
            read={
                "snowflake": "CONVERT_TIMEZONE('America/New_York', CAST(start AS TIMESTAMPTZ))",
            },
            write={
                "bigquery": "TIMESTAMP(DATETIME(CAST(start AS TIMESTAMP), 'America/New_York'))",
                "duckdb": "CAST(start AS TIMESTAMPTZ) AT TIME ZONE 'America/New_York'",
                "snowflake": "CONVERT_TIMEZONE('America/New_York', CAST(start AS TIMESTAMPTZ))",
            },
        )

    def test_sample(self):
        self.validate_identity(
            "SELECT * FROM tbl USING SAMPLE 5",
            "SELECT * FROM tbl USING SAMPLE (5 ROWS)",
        )
        self.validate_identity(
            "SELECT * FROM tbl USING SAMPLE 10%",
            "SELECT * FROM tbl USING SAMPLE (10 PERCENT)",
        )
        self.validate_identity(
            "SELECT * FROM tbl USING SAMPLE 10 PERCENT (bernoulli)",
            "SELECT * FROM tbl USING SAMPLE BERNOULLI (10 PERCENT)",
        )
        self.validate_identity(
            "SELECT * FROM tbl USING SAMPLE reservoir(50 ROWS) REPEATABLE (100)",
            "SELECT * FROM tbl USING SAMPLE RESERVOIR (50 ROWS) REPEATABLE (100)",
        )
        self.validate_identity(
            "SELECT * FROM tbl USING SAMPLE 10% (system, 377)",
            "SELECT * FROM tbl USING SAMPLE SYSTEM (10 PERCENT) REPEATABLE (377)",
        )
        self.validate_identity(
            "SELECT * FROM tbl TABLESAMPLE RESERVOIR(20%), tbl2 WHERE tbl.i=tbl2.i",
            "SELECT * FROM tbl TABLESAMPLE RESERVOIR (20 PERCENT), tbl2 WHERE tbl.i = tbl2.i",
        )
        self.validate_identity(
            "SELECT * FROM tbl, tbl2 WHERE tbl.i=tbl2.i USING SAMPLE RESERVOIR(20%)",
            "SELECT * FROM tbl, tbl2 WHERE tbl.i = tbl2.i USING SAMPLE RESERVOIR (20 PERCENT)",
        )

        self.validate_all(
            "SELECT * FROM example TABLESAMPLE (3 ROWS) REPEATABLE (82)",
            read={
                "duckdb": "SELECT * FROM example TABLESAMPLE (3) REPEATABLE (82)",
                "snowflake": "SELECT * FROM example SAMPLE (3 ROWS) SEED (82)",
            },
            write={
                "duckdb": "SELECT * FROM example TABLESAMPLE (3 ROWS) REPEATABLE (82)",
                "snowflake": "SELECT * FROM example TABLESAMPLE (3 ROWS) SEED (82)",
            },
        )

    def test_array(self):
        self.validate_identity("ARRAY(SELECT id FROM t)")
        self.validate_identity("ARRAY((SELECT id FROM t))")

    def test_cast(self):
        self.validate_identity("CAST(x AS REAL)")
        self.validate_identity("CAST(x AS UINTEGER)")
        self.validate_identity("CAST(x AS UBIGINT)")
        self.validate_identity("CAST(x AS USMALLINT)")
        self.validate_identity("CAST(x AS UTINYINT)")
        self.validate_identity("CAST(x AS TEXT)")
        self.validate_identity("CAST(x AS INT128)")
        self.validate_identity("CAST(x AS DOUBLE)")
        self.validate_identity("CAST(x AS DECIMAL(15, 4))")
        self.validate_identity("CAST(x AS STRUCT(number BIGINT))")
        self.validate_identity(
            "CAST(ROW(1, ROW(1)) AS STRUCT(number BIGINT, row STRUCT(number BIGINT)))"
        )

        self.validate_identity("CAST(x AS INT64)", "CAST(x AS BIGINT)")
        self.validate_identity("CAST(x AS INT32)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS INT16)", "CAST(x AS SMALLINT)")
        self.validate_identity("CAST(x AS NUMERIC(1, 2))", "CAST(x AS DECIMAL(1, 2))")
        self.validate_identity("CAST(x AS HUGEINT)", "CAST(x AS INT128)")
        self.validate_identity("CAST(x AS CHAR)", "CAST(x AS TEXT)")
        self.validate_identity("CAST(x AS BPCHAR)", "CAST(x AS TEXT)")
        self.validate_identity("CAST(x AS STRING)", "CAST(x AS TEXT)")
        self.validate_identity("CAST(x AS INT1)", "CAST(x AS TINYINT)")
        self.validate_identity("CAST(x AS FLOAT4)", "CAST(x AS REAL)")
        self.validate_identity("CAST(x AS FLOAT)", "CAST(x AS REAL)")
        self.validate_identity("CAST(x AS INT4)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS INTEGER)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS SIGNED)", "CAST(x AS INT)")
        self.validate_identity("CAST(x AS BLOB)", "CAST(x AS BLOB)")
        self.validate_identity("CAST(x AS BYTEA)", "CAST(x AS BLOB)")
        self.validate_identity("CAST(x AS BINARY)", "CAST(x AS BLOB)")
        self.validate_identity("CAST(x AS VARBINARY)", "CAST(x AS BLOB)")
        self.validate_identity("CAST(x AS LOGICAL)", "CAST(x AS BOOLEAN)")
        self.validate_all(
            "CAST(x AS NUMERIC)",
            write={
                "duckdb": "CAST(x AS DECIMAL(18, 3))",
                "postgres": "CAST(x AS DECIMAL(18, 3))",
            },
        )
        self.validate_all(
            "CAST(x AS DECIMAL)",
            write={
                "duckdb": "CAST(x AS DECIMAL(18, 3))",
                "postgres": "CAST(x AS DECIMAL(18, 3))",
            },
        )
        self.validate_all(
            "CAST(x AS BIT)",
            read={
                "duckdb": "CAST(x AS BITSTRING)",
            },
            write={
                "duckdb": "CAST(x AS BIT)",
                "tsql": "CAST(x AS BIT)",
            },
        )
        self.validate_all(
            "123::CHARACTER VARYING",
            write={
                "duckdb": "CAST(123 AS TEXT)",
            },
        )
        self.validate_all(
            "cast([[1]] as int[][])",
            write={
                "duckdb": "CAST([[1]] AS INT[][])",
                "spark": "CAST(ARRAY(ARRAY(1)) AS ARRAY<ARRAY<INT>>)",
            },
        )
        self.validate_all(
            "CAST(x AS DATE) + INTERVAL (7 * -1) DAY", read={"spark": "DATE_SUB(x, 7)"}
        )
        self.validate_all(
            "TRY_CAST(1 AS DOUBLE)",
            read={
                "hive": "1d",
                "spark": "1d",
            },
        )
        self.validate_all(
            "CAST(x AS DATE)",
            write={
                "duckdb": "CAST(x AS DATE)",
                "": "CAST(x AS DATE)",
            },
        )
        self.validate_all(
            "COL::BIGINT[]",
            write={
                "duckdb": "CAST(COL AS BIGINT[])",
                "presto": "CAST(COL AS ARRAY(BIGINT))",
                "hive": "CAST(COL AS ARRAY<BIGINT>)",
                "spark": "CAST(COL AS ARRAY<BIGINT>)",
                "postgres": "CAST(COL AS BIGINT[])",
                "snowflake": "CAST(COL AS ARRAY)",
            },
        )
        self.validate_all(
            "CAST([STRUCT_PACK(a := 1)] AS STRUCT(a BIGINT)[])",
            write={
                "duckdb": "CAST([{'a': 1}] AS STRUCT(a BIGINT)[])",
            },
        )
        self.validate_all(
            "CAST([[STRUCT_PACK(a := 1)]] AS STRUCT(a BIGINT)[][])",
            write={
                "duckdb": "CAST([[{'a': 1}]] AS STRUCT(a BIGINT)[][])",
            },
        )

    def test_bool_or(self):
        self.validate_all(
            "SELECT a, LOGICAL_OR(b) FROM table GROUP BY a",
            write={"duckdb": "SELECT a, BOOL_OR(b) FROM table GROUP BY a"},
        )

    def test_encode_decode(self):
        self.validate_all(
            "ENCODE(x)",
            read={
                "spark": "ENCODE(x, 'utf-8')",
                "presto": "TO_UTF8(x)",
            },
            write={
                "duckdb": "ENCODE(x)",
                "spark": "ENCODE(x, 'utf-8')",
                "presto": "TO_UTF8(x)",
            },
        )
        self.validate_all(
            "DECODE(x)",
            read={
                "spark": "DECODE(x, 'utf-8')",
                "presto": "FROM_UTF8(x)",
            },
            write={
                "duckdb": "DECODE(x)",
                "spark": "DECODE(x, 'utf-8')",
                "presto": "FROM_UTF8(x)",
            },
        )
        self.validate_all(
            "DECODE(x)",
            read={
                "presto": "FROM_UTF8(x, y)",
            },
        )

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "snowflake": "ALTER TABLE db.t1 RENAME TO db.t2",
                "duckdb": "ALTER TABLE db.t1 RENAME TO t2",
            },
        )

    def test_timestamps_with_units(self):
        self.validate_all(
            "SELECT w::TIMESTAMP_S, x::TIMESTAMP_MS, y::TIMESTAMP_US, z::TIMESTAMP_NS",
            write={
                "duckdb": "SELECT CAST(w AS TIMESTAMP_S), CAST(x AS TIMESTAMP_MS), CAST(y AS TIMESTAMP), CAST(z AS TIMESTAMP_NS)",
            },
        )

    def test_isnan(self):
        self.validate_all(
            "ISNAN(x)",
            read={"bigquery": "IS_NAN(x)"},
            write={"bigquery": "IS_NAN(x)", "duckdb": "ISNAN(x)"},
        )

    def test_isinf(self):
        self.validate_all(
            "ISINF(x)",
            read={"bigquery": "IS_INF(x)"},
            write={"bigquery": "IS_INF(x)", "duckdb": "ISINF(x)"},
        )

    def test_parameter_token(self):
        self.validate_all(
            "SELECT $foo",
            read={"bigquery": "SELECT @foo"},
            write={"bigquery": "SELECT @foo", "duckdb": "SELECT $foo"},
        )
