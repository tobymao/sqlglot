from tests.dialects.test_dialect import Validator

from sqlglot import ErrorLevel


class TestDuckDB(Validator):
    def test_time(self):
        self.validate_all(
            "STR_TO_TIME('2020-01-01', '%Y-%m-%d')",
            read={
                "duckdb": "STRPTIME('2020-01-01', '%Y-%m-%d')",
            },
            write={
                "duckdb": "STRPTIME('2020-01-01', '%Y-%m-%d')",
            },
        )

    def test_duckdb(self):
        self.validate("EPOCH(x)", "EPOCH(x)", read="duckdb")
        self.validate("EPOCH(x)", "TO_UNIXTIME(x)", read="duckdb", write="presto")
        self.validate(
            "EPOCH_MS(x)", "FROM_UNIXTIME(x / 1000)", read="duckdb", write="presto"
        )
        self.validate(
            "STRFTIME(x, '%y-%-m-%S')",
            "DATE_FORMAT(x, '%y-%c-%S')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STRPTIME(x, '%y-%-m')",
            "DATE_PARSE(x, '%y-%c')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "LIST_VALUE(0, 1, 2)", "ARRAY[0, 1, 2]", read="duckdb", write="presto"
        )
        self.validate("Array(1, 2)", "LIST_VALUE(1, 2)", write="duckdb")

        self.validate("REGEXP_MATCHES(x, y)", "REGEXP_MATCHES(x, y)", read="duckdb")
        self.validate("REGEXP_MATCHES(x, y)", "x RLIKE y", read="duckdb", write="hive")
        self.validate(
            "REGEXP_MATCHES('abc', 'abc')",
            "REGEXP_LIKE('abc', 'abc')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "REGEXP_MATCHES('abc', '(b|c).*')",
            "REGEXP_LIKE('abc', '(b|c).*')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "REGEXP_MATCHES(x, 'abc')",
            "REGEXP_LIKE(x, 'abc')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STR_SPLIT(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STRING_SPLIT(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STRING_TO_ARRAY(x, 'a')", "SPLIT(x, 'a')", read="duckdb", write="presto"
        )
        self.validate(
            "STR_SPLIT_REGEX(x, 'a')",
            "REGEXP_SPLIT(x, 'a')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "STRING_SPLIT_REGEX(x, 'a')",
            "REGEXP_SPLIT(x, 'a')",
            read="duckdb",
            write="presto",
        )

        self.validate(
            "STRUCT_EXTRACT(x, 'abc')", "STRUCT_EXTRACT(x, 'abc')", read="duckdb"
        )

        self.validate(
            "QUANTILE(x, 0.5)",
            "APPROX_PERCENTILE(x, 0.5)",
            read="duckdb",
            write="presto",
            unsupported_level=ErrorLevel.IGNORE,
        )
        self.validate(
            "QUANTILE(x, 0.5)", "PERCENTILE(x, 0.5)", read="duckdb", write="spark"
        )
        self.validate(
            "PERCENTILE(x, 0.5)", "QUANTILE(x, 0.5)", read="hive", write="duckdb"
        )

        self.validate("MONTH(x)", "MONTH(x)", write="duckdb", identity=False)
        self.validate("YEAR(x)", "YEAR(x)", write="duckdb", identity=False)
        self.validate("DAY(x)", "DAY(x)", write="duckdb", identity=False)

        self.validate(
            "DATEDIFF(a, b)",
            "DATE_DIFF('day', CAST(b AS DATE), CAST(a AS DATE))",
            read="hive",
            write="duckdb",
        )
        self.validate(
            "STR_TO_UNIX('2020-01-01', '%Y-%M-%d')",
            "EPOCH(STRPTIME('2020-01-01', '%Y-%M-%d'))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_DATE('2020-01-01')",
            "CAST('2020-01-01' AS DATE)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_TIME('2020-01-01')",
            "CAST('2020-01-01' AS TIMESTAMP)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_STR_TO_UNIX('2020-01-01')",
            "EPOCH(CAST('2020-01-01' AS TIMESTAMP))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_STR(x, '%Y-%m-%d')",
            "STRFTIME(x, '%Y-%m-%d')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_TIME_STR(x)",
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TIME_TO_UNIX(x)",
            "EPOCH(x)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "TS_OR_DS_TO_DATE_STR(x)",
            "SUBSTRING(CAST(x AS TEXT), 1, 10)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_STR(x, y)",
            "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), y)",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME(x)",
            "TO_TIMESTAMP(CAST(x AS BIGINT))",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "UNIX_TO_TIME_STR(x)",
            "STRFTIME(TO_TIMESTAMP(CAST(x AS BIGINT)), '%Y-%m-%d %H:%M:%S')",
            identity=False,
            write="duckdb",
        )
        self.validate(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            "DATE_FORMAT(x, 'yyyy-MM-dd HH:mm:ss')",
            read="duckdb",
            write="hive",
        )
        self.validate(
            "STRFTIME(x, '%Y-%m-%d %H:%M:%S')",
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%S')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "TO_TIMESTAMP(x)",
            "DATE_PARSE(x, '%Y-%m-%d %H:%i:%s')",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "TS_OR_DS_TO_DATE(x)",
            "CAST(x AS DATE)",
            write="duckdb",
            identity=False,
        )
        self.validate(
            "CAST(x AS DATE)",
            "CAST(x AS DATE)",
            read="duckdb",
            identity=False,
        )

        self.validate(
            "UNNEST(x)",
            "EXPLODE(x)",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "EXPLODE(x)",
            "UNNEST(x)",
            read="spark",
            write="duckdb",
        )

        self.validate("1d", "1 AS d", read="duckdb")
        self.validate("1d", "CAST(1 AS DOUBLE)", read="spark", write="duckdb")
        self.validate(
            "POW(2S, 3)", "POW(CAST(2 AS SMALLINT), 3)", read="spark", write="duckdb"
        )

        self.validate(
            "DATE_TO_DATE_STR(x)",
            "STRFTIME(x, '%Y-%m-%d')",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DATE_TO_DATE_STR(x)",
            "DATE_FORMAT(x, 'yyyy-MM-dd')",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(STRFTIME(x, '%Y%m%d') AS INT)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DATE_TO_DI(x)",
            "CAST(DATE_FORMAT(x, 'yyyyMMdd') AS INT)",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "CAST(STRPTIME(CAST(x AS STRING), '%Y%m%d') AS DATE)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "DI_TO_DATE(x)",
            "TO_DATE(CAST(x AS STRING), 'yyyyMMdd')",
            read="duckdb",
            write="spark",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS STRING), '-', ''), 1, 8) AS INT)",
            read="duckdb",
            write="duckdb",
        )
        self.validate(
            "TS_OR_DI_TO_DI(x)",
            "CAST(SUBSTR(REPLACE(CAST(x AS VARCHAR), '-', ''), 1, 8) AS INT)",
            read="duckdb",
            write="presto",
        )
        self.validate(
            "ARRAY_SUM(ARRAY(1, 2))",
            "LIST_SUM(LIST_VALUE(1, 2))",
            read="spark",
            write="duckdb",
        )

        self.validate(
            "SAFE_DIVIDE(x, y)",
            "IF(y <> 0, x / y, NULL)",
            read="bigquery",
            write="duckdb",
        )

        self.validate(
            "STRUCT_PACK(x := 1, y := '2')",
            "STRUCT_PACK(x := 1, y := '2')",
            read="duckdb",
        )
        self.validate(
            "STRUCT_PACK(x := 1, y := '2')",
            "STRUCT(x = 1, y = '2')",
            read="duckdb",
            write="spark",
        )

        self.validate(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname",
            read="duckdb",
            write="duckdb",
        )
