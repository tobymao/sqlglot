from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_singlestore(self):
        ast = parse_one(
            "SELECT id AS my_id FROM data WHERE my_id = 1 GROUP BY my_id HAVING my_id = 1",
            dialect=self.dialect,
        )
        ast = qualify(ast, dialect=self.dialect, schema={"data": {"id": "INT", "my_id": "INT"}})
        self.assertEqual(
            "SELECT `data`.`id` AS `my_id` FROM `data` AS `data` WHERE `data`.`my_id` = 1 GROUP BY `data`.`my_id` HAVING `data`.`id` = 1",
            ast.sql(dialect=self.dialect),
        )

        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM `users` ORDER BY ALL")

    def test_byte_strings(self):
        self.validate_identity("SELECT e'text'")
        self.validate_identity("SELECT E'text'", "SELECT e'text'")

    def test_restricted_keywords(self):
        self.validate_identity("SELECT * FROM abs", "SELECT * FROM `abs`")
        self.validate_identity("SELECT * FROM ABS", "SELECT * FROM `ABS`")
        self.validate_identity(
            "SELECT * FROM security_lists_intersect", "SELECT * FROM `security_lists_intersect`"
        )
        self.validate_identity("SELECT * FROM vacuum", "SELECT * FROM `vacuum`")

    def test_time_formatting(self):
        self.validate_identity("SELECT STR_TO_DATE('March 3rd, 2015', '%M %D, %Y')")
        self.validate_identity("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %h:%i:%s')")
        self.validate_identity(
            "SELECT TO_DATE('03/01/2019', 'MM/DD/YYYY') AS `result`",
        )
        self.validate_identity(
            "SELECT TO_TIMESTAMP('The date and time are 01/01/2018 2:30:15.123456', 'The date and time are MM/DD/YYYY HH12:MI:SS.FF6') AS `result`",
        )
        self.validate_identity(
            "SELECT TO_CHAR('2018-03-01', 'MM/DD')",
        )
        self.validate_identity(
            "SELECT TIME_FORMAT('12:05:47', '%s, %i, %h')",
            "SELECT DATE_FORMAT('12:05:47' :> TIME(6), '%s, %i, %h')",
        )
        self.validate_identity("SELECT DATE('2019-01-01 05:06')")
        self.validate_all(
            "SELECT DATE('2019-01-01 05:06')",
            read={
                "": "SELECT TS_OR_DS_TO_DATE('2019-01-01 05:06')",
                "singlestore": "SELECT DATE('2019-01-01 05:06')",
            },
        )

    def test_cast(self):
        self.validate_all(
            "SELECT 1 :> INT",
            read={
                "": "SELECT CAST(1 AS INT)",
            },
            write={
                "singlestore": "SELECT 1 :> INT",
                "": "SELECT CAST(1 AS INT)",
            },
        )
        self.validate_all(
            "SELECT 1 !:> INT",
            read={
                "": "SELECT TRY_CAST(1 AS INT)",
            },
            write={
                "singlestore": "SELECT 1 !:> INT",
                "": "SELECT TRY_CAST(1 AS INT)",
            },
        )
        self.validate_identity("SELECT '{\"a\" : 1}' :> JSON")
        self.validate_identity("SELECT NOW() !:> TIMESTAMP(6)")
        self.validate_identity("SELECT x :> GEOGRAPHYPOINT")

    def test_unix_functions(self):
        self.validate_identity("SELECT FROM_UNIXTIME(1234567890)")
        self.validate_identity("SELECT FROM_UNIXTIME(1234567890, '%M %D, %Y')")
        self.validate_identity("SELECT UNIX_TIMESTAMP()")
        self.validate_identity("SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30') AS funday")

        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"duckdb": "SELECT EPOCH('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"duckdb": "SELECT TIME_STR_TO_UNIX('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"": "SELECT TIME_STR_TO_UNIX('2009-02-13 23:31:30')"},
        )
        self.validate_all(
            "SELECT UNIX_TIMESTAMP('2009-02-13 23:31:30')",
            read={"": "SELECT UNIX_SECONDS('2009-02-13 23:31:30')"},
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME(1234567890, '%Y-%m-%d %T')",
            read={"hive": "SELECT FROM_UNIXTIME(1234567890)"},
        )
        self.validate_all(
            "SELECT FROM_UNIXTIME(1234567890) :> TEXT",
            read={"": "SELECT UNIX_TO_TIME_STR(1234567890)"},
        )

    def test_json_extract(self):
        self.validate_identity("SELECT a::b FROM t", "SELECT JSON_EXTRACT_JSON(a, 'b') FROM t")
        self.validate_identity("SELECT a::b FROM t", "SELECT JSON_EXTRACT_JSON(a, 'b') FROM t")
        self.validate_identity("SELECT a::$b FROM t", "SELECT JSON_EXTRACT_STRING(a, 'b') FROM t")
        self.validate_identity("SELECT a::%b FROM t", "SELECT JSON_EXTRACT_DOUBLE(a, 'b') FROM t")
        self.validate_identity(
            "SELECT a::`b`::`2` FROM t",
            "SELECT JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(a, 'b'), '2') FROM t",
        )
        self.validate_identity("SELECT a::2 FROM t", "SELECT JSON_EXTRACT_JSON(a, '2') FROM t")

        self.validate_all(
            "SELECT JSON_EXTRACT_JSON(a, 'b') FROM t",
            read={
                "mysql": "SELECT JSON_EXTRACT(a, '$.b') FROM t",
                "singlestore": "SELECT JSON_EXTRACT_JSON(a, 'b') FROM t",
            },
            write={"mysql": "SELECT JSON_EXTRACT(a, '$.b') FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_STRING(a, 'b') FROM t",
            write={"": "SELECT JSON_EXTRACT_SCALAR(a, '$.b', STRING) FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_DOUBLE(a, 'b') FROM t",
            write={"": "SELECT JSON_EXTRACT_SCALAR(a, '$.b', DOUBLE) FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_BIGINT(a, 'b') FROM t",
            write={"": "SELECT JSON_EXTRACT_SCALAR(a, '$.b', BIGINT) FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_BIGINT(a, 'b') FROM t",
            write={"": "SELECT JSON_EXTRACT_SCALAR(a, '$.b', BIGINT) FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_JSON(a, 'b', '2') FROM t",
            read={
                "mysql": "SELECT JSON_EXTRACT(a, '$.b[2]') FROM t",
                "singlestore": "SELECT JSON_EXTRACT_JSON(a, 'b', '2') FROM t",
            },
            write={"mysql": "SELECT JSON_EXTRACT(a, '$.b[2]') FROM t"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT_STRING(a, 'b', 2) FROM t",
            write={"": "SELECT JSON_EXTRACT_SCALAR(a, '$.b[2]', STRING) FROM t"},
        )

        self.validate_all(
            "SELECT BSON_EXTRACT_BSON(a, 'b') FROM t",
            read={
                "mysql": "SELECT JSONB_EXTRACT(a, 'b') FROM t",
                "singlestore": "SELECT BSON_EXTRACT_BSON(a, 'b') FROM t",
            },
            write={"mysql": "SELECT JSONB_EXTRACT(a, '$.b') FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_STRING(a, 'b') FROM t",
            write={"": "SELECT JSONB_EXTRACT_SCALAR(a, '$.b', STRING) FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_DOUBLE(a, 'b') FROM t",
            write={"": "SELECT JSONB_EXTRACT_SCALAR(a, '$.b', DOUBLE) FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_BIGINT(a, 'b') FROM t",
            write={"": "SELECT JSONB_EXTRACT_SCALAR(a, '$.b', BIGINT) FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_BIGINT(a, 'b') FROM t",
            write={"": "SELECT JSONB_EXTRACT_SCALAR(a, '$.b', BIGINT) FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_BSON(a, 'b', 2) FROM t",
            write={"": "SELECT JSONB_EXTRACT(a, '$.b[2]') FROM t"},
        )
        self.validate_all(
            "SELECT BSON_EXTRACT_STRING(a, 'b', 2) FROM t",
            write={"": "SELECT JSONB_EXTRACT_SCALAR(a, '$.b[2]', STRING) FROM t"},
        )
        self.validate_all(
            'SELECT JSON_EXTRACT_STRING(\'{"item": "shoes", "price": "49.95"}\', \'price\') :> DECIMAL(4, 2)',
            read={
                "mysql": 'SELECT JSON_VALUE(\'{"item": "shoes", "price": "49.95"}\', \'$.price\' RETURNING DECIMAL(4, 2))'
            },
        )

    def test_json(self):
        self.validate_identity("SELECT JSON_ARRAY_CONTAINS_STRING('[\"a\", \"b\"]', 'b')")
        self.validate_identity("SELECT JSON_ARRAY_CONTAINS_DOUBLE('[1, 2]', 1)")
        self.validate_identity('SELECT JSON_ARRAY_CONTAINS_JSON(\'["{"a": 1}"]\', \'{"a":   1}\')')
        self.validate_all(
            "SELECT JSON_ARRAY_CONTAINS_JSON('[\"a\"]', TO_JSON('a'))",
            read={
                "mysql": "SELECT 'a' MEMBER OF ('[\"a\"]')",
                "singlestore": "SELECT JSON_ARRAY_CONTAINS_JSON('[\"a\"]', TO_JSON('a'))",
            },
        )
        self.validate_all(
            'SELECT JSON_PRETTY(\'["G","alpha","20",10]\')',
            read={
                "singlestore": 'SELECT JSON_PRETTY(\'["G","alpha","20",10]\')',
                "": 'SELECT JSON_FORMAT(\'["G","alpha","20",10]\')',
            },
        )

    def test_date_parts_functions(self):
        self.validate_identity(
            "SELECT DAYNAME('2014-04-18')", "SELECT DATE_FORMAT('2014-04-18', '%W')"
        )
        self.validate_identity(
            "SELECT HOUR('2009-02-13 23:31:30')",
            "SELECT DATE_FORMAT('2009-02-13 23:31:30' :> TIME(6), '%k') :> INT",
        )
        self.validate_identity(
            "SELECT MICROSECOND('2009-02-13 23:31:30.123456')",
            "SELECT DATE_FORMAT('2009-02-13 23:31:30.123456' :> TIME(6), '%f') :> INT",
        )
        self.validate_identity(
            "SELECT SECOND('2009-02-13 23:31:30.123456')",
            "SELECT DATE_FORMAT('2009-02-13 23:31:30.123456' :> TIME(6), '%s') :> INT",
        )
        self.validate_identity(
            "SELECT MONTHNAME('2014-04-18')", "SELECT DATE_FORMAT('2014-04-18', '%M')"
        )
        self.validate_identity(
            "SELECT WEEKDAY('2014-04-18')", "SELECT (DAYOFWEEK('2014-04-18') + 5) % 7"
        )
        self.validate_identity(
            "SELECT MINUTE('2009-02-13 23:31:30.123456')",
            "SELECT DATE_FORMAT('2009-02-13 23:31:30.123456' :> TIME(6), '%i') :> INT",
        )
        self.validate_all(
            "SELECT ((DAYOFWEEK('2014-04-18') % 7) + 1)",
            read={
                "singlestore": "SELECT ((DAYOFWEEK('2014-04-18') % 7) + 1)",
                "": "SELECT DAYOFWEEK_ISO('2014-04-18')",
            },
        )
        self.validate_all(
            "SELECT DAY('2014-04-18')",
            read={
                "singlestore": "SELECT DAY('2014-04-18')",
                "": "SELECT DAY_OF_MONTH('2014-04-18')",
            },
        )

    def test_math_functions(self):
        self.validate_all(
            "SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets",
            read={
                "singlestore": "SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets",
                "": "SELECT HLL(asset_id) AS approx_distinct_asset_id FROM acd_assets",
            },
        )
        self.validate_identity(
            "SELECT APPROX_COUNT_DISTINCT(asset_id1, asset_id2) AS approx_distinct_asset_id FROM acd_assets"
        )
        self.validate_all(
            "SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets",
            read={
                "singlestore": "SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets",
                "": "SELECT APPROX_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets",
            },
        )
        self.validate_all(
            "SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM `users`",
            read={
                "singlestore": "SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM `users`",
                "": "SELECT COUNT_IF(age > 18) FROM users",
            },
        )
        self.validate_all(
            "SELECT MAX(ABS(age > 18)) FROM `users`",
            read={
                "singlestore": "SELECT MAX(ABS(age > 18)) FROM `users`",
                "": "SELECT LOGICAL_OR(age > 18) FROM users",
            },
        )
        self.validate_all(
            "SELECT MIN(ABS(age > 18)) FROM `users`",
            read={
                "singlestore": "SELECT MIN(ABS(age > 18)) FROM `users`",
                "": "SELECT LOGICAL_AND(age > 18) FROM users",
            },
        )
        self.validate_identity(
            "SELECT `class`, student_id, test1, APPROX_PERCENTILE(test1, 0.3) OVER (PARTITION BY `class`) AS percentile FROM test_scores"
        )
        self.validate_identity(
            "SELECT `class`, student_id, test1, APPROX_PERCENTILE(test1, 0.3, 0.4) OVER (PARTITION BY `class`) AS percentile FROM test_scores"
        )
        self.validate_all(
            "SELECT APPROX_PERCENTILE(test1, 0.3) FROM test_scores",
            read={
                "singlestore": "SELECT APPROX_PERCENTILE(test1, 0.3) FROM test_scores",
                # accuracy parameter is not supported in SingleStore, so it is ignored
                "": "SELECT APPROX_QUANTILE(test1, 0.3, 0.4) FROM test_scores",
            },
        )
        self.validate_all(
            "SELECT VAR_SAMP(yearly_total) FROM player_scores",
            read={
                "singlestore": "SELECT VAR_SAMP(yearly_total) FROM player_scores",
                "": "SELECT VARIANCE(yearly_total) FROM player_scores",
            },
            write={
                "": "SELECT VARIANCE(yearly_total) FROM player_scores",
            },
        )
        self.validate_all(
            "SELECT VAR_POP(yearly_total) FROM player_scores",
            read={
                "singlestore": "SELECT VARIANCE(yearly_total) FROM player_scores",
                "": "SELECT VARIANCE_POP(yearly_total) FROM player_scores",
            },
            write={
                "": "SELECT VARIANCE_POP(yearly_total) FROM player_scores",
            },
        )

    def test_logical(self):
        self.validate_all(
            "SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)",
            read={
                "mysql": "SELECT TRUE XOR FALSE",
                "singlestore": "SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)",
            },
        )

    def test_string_functions(self):
        self.validate_all(
            "SELECT 'a' RLIKE 'b'",
            read={
                "bigquery": "SELECT REGEXP_CONTAINS('a', 'b')",
                "singlestore": "SELECT 'a' RLIKE 'b'",
            },
        )
        self.validate_identity("SELECT 'a' REGEXP 'b'", "SELECT 'a' RLIKE 'b'")
        self.validate_all(
            "SELECT LPAD('', LENGTH('a') * 3, 'a')",
            read={
                "": "SELECT REPEAT('a', 3)",
                "singlestore": "SELECT LPAD('', LENGTH('a') * 3, 'a')",
            },
        )
        self.validate_all(
            "SELECT REGEXP_SUBSTR('adog', 'O', 1, 1, 'c')",
            read={
                # group parameter is not supported in SingleStore, so it is ignored
                "": "SELECT REGEXP_EXTRACT('adog', 'O', 1, 1, 'c', 'gr1')",
                "singlestore": "SELECT REGEXP_SUBSTR('adog', 'O', 1, 1, 'c')",
            },
        )
        self.validate_all(
            "SELECT ('a' RLIKE '^[\x00-\x7f]*$')",
            read={"singlestore": "SELECT ('a' RLIKE '^[\x00-\x7f]*$')", "": "SELECT IS_ASCII('a')"},
        )
        self.validate_all(
            "SELECT UNHEX(MD5('data'))",
            read={
                "singlestore": "SELECT UNHEX(MD5('data'))",
                "": "SELECT MD5_DIGEST('data')",
            },
        )
        self.validate_all(
            "SELECT CHAR(101)", read={"": "SELECT CHR(101)", "singlestore": "SELECT CHAR(101)"}
        )
        self.validate_all(
            "SELECT INSTR('ohai', 'i')",
            read={
                "": "SELECT CONTAINS('ohai', 'i')",
                "singlestore": "SELECT INSTR('ohai', 'i')",
            },
        )
        self.validate_all(
            "SELECT REGEXP_MATCH('adog', 'O', 'c')",
            read={
                # group, position, occurrence parameters are not supported in SingleStore, so they are ignored
                "": "SELECT REGEXP_EXTRACT_ALL('adog', 'O', 1, 1, 'c', 'gr1')",
                "singlestore": "SELECT REGEXP_MATCH('adog', 'O', 'c')",
            },
        )
        self.validate_all(
            "SELECT REGEXP_SUBSTR('adog', 'O', 1, 1, 'c')",
            read={
                # group parameter is not supported in SingleStore, so it is ignored
                "": "SELECT REGEXP_EXTRACT('adog', 'O', 1, 1, 'c', 'gr1')",
                "singlestore": "SELECT REGEXP_SUBSTR('adog', 'O', 1, 1, 'c')",
            },
        )
        self.validate_all(
            "SELECT REGEXP_INSTR('abcd', CONCAT('^', 'ab'))",
            read={
                "": "SELECT STARTS_WITH('abcd', 'ab')",
                "singlestore": "SELECT REGEXP_INSTR('abcd', CONCAT('^', 'ab'))",
            },
        )
        self.validate_all(
            "SELECT CONV('f', 16, 10)",
            read={
                "redshift": "SELECT STRTOL('f',16)",
                "singlestore": "SELECT CONV('f', 16, 10)",
            },
        )

    def test_reduce_functions(self):
        self.validate_all(
            "SELECT REDUCE(0, JSON_TO_ARRAY('[1,2,3,4]'), REDUCE_ACC() + REDUCE_VALUE()) AS `Result`",
            read={
                # finish argument is not supported in SingleStore, so it is ignored
                "": "SELECT REDUCE(JSON_TO_ARRAY('[1,2,3,4]'), 0, REDUCE_ACC() + REDUCE_VALUE(), REDUCE_ACC() + REDUCE_VALUE()) AS Result",
                "singlestore": "SELECT REDUCE(0, JSON_TO_ARRAY('[1,2,3,4]'), REDUCE_ACC() + REDUCE_VALUE()) AS `Result`",
            },
        )

    def test_time_functions(self):
        self.validate_all(
            "SELECT TIME_BUCKET('1d', '2019-03-14 06:04:12', '2019-03-13 03:00:00')",
            read={
                # unit and zone parameters are not supported in SingleStore, so they are ignored
                "": "SELECT DATE_BIN('1d', '2019-03-14 06:04:12', DAY, 'UTC', '2019-03-13 03:00:00')",
                "singlestore": "SELECT TIME_BUCKET('1d', '2019-03-14 06:04:12', '2019-03-13 03:00:00')",
            },
        )
        self.validate_all(
            "SELECT CONVERT_TZ(NOW() :> DATETIME, 'GMT', 'UTC')",
            read={
                "spark2": "SELECT TO_UTC_TIMESTAMP(NOW(), 'GMT')",
                "singlestore": "SELECT CONVERT_TZ(NOW() :> DATETIME, 'GMT', 'UTC')",
            },
        )
        self.validate_all(
            "SELECT STR_TO_DATE(20190314, '%Y%m%d')",
            read={
                "": "SELECT DI_TO_DATE(20190314)",
                "singlestore": "SELECT STR_TO_DATE(20190314, '%Y%m%d')",
            },
        )
        self.validate_all(
            "SELECT (DATE_FORMAT('2019-03-14 06:04:12', '%Y%m%d') :> INT)",
            read={
                "singlestore": "SELECT (DATE_FORMAT('2019-03-14 06:04:12', '%Y%m%d') :> INT)",
                "": "SELECT DATE_TO_DI('2019-03-14 06:04:12')",
            },
        )
        self.validate_all(
            "SELECT (DATE_FORMAT('2019-03-14 06:04:12', '%Y%m%d') :> INT)",
            read={
                "singlestore": "SELECT (DATE_FORMAT('2019-03-14 06:04:12', '%Y%m%d') :> INT)",
                "": "SELECT TS_OR_DI_TO_DI('2019-03-14 06:04:12')",
            },
        )
