from sqlglot import parse_one, exp
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

    def test_national_strings(self):
        self.validate_all(
            "SELECT 'text'", read={"": "SELECT N'text'", "singlestore": "SELECT 'text'"}
        )

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
        self.validate_all(
            "SELECT age :> TEXT FROM `users`",
            read={
                "": "SELECT CAST(age, 'TEXT') FROM users",
                "singlestore": "SELECT age :> TEXT FROM `users`",
            },
        )

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
        self.validate_all(
            "SELECT JSON_AGG(name ORDER BY id ASC NULLS LAST, name DESC NULLS FIRST) FROM t",
            read={
                "singlestore": "SELECT JSON_AGG(name ORDER BY id ASC NULLS LAST, name DESC NULLS FIRST) FROM t",
                "oracle": "SELECT JSON_ARRAYAGG(name ORDER BY id ASC, name DESC) FROM t",
            },
        )
        self.validate_identity("SELECT JSON_AGG(name) FROM t")
        self.validate_identity("SELECT JSON_AGG(t.*) FROM t")
        self.validate_all(
            "SELECT JSON_BUILD_ARRAY(id, name) FROM t",
            read={
                "singlestore": "SELECT JSON_BUILD_ARRAY(id, name) FROM t",
                "oracle": "SELECT JSON_ARRAY(id, name) FROM t",
            },
        )
        self.validate_identity("JSON_BUILD_ARRAY(id, name)").assert_is(exp.JSONArray)
        self.validate_all(
            "SELECT BSON_MATCH_ANY_EXISTS('{\"x\":true}', 'x')",
            read={
                "singlestore": "SELECT BSON_MATCH_ANY_EXISTS('{\"x\":true}', 'x')",
                "": "SELECT JSONB_EXISTS('{\"x\":true}', 'x')",
            },
        )
        self.validate_all(
            "SELECT JSON_MATCH_ANY_EXISTS('{\"a\":1}', 'a')",
            read={
                "singlestore": "SELECT JSON_MATCH_ANY_EXISTS('{\"a\":1}', 'a')",
                "oracle": "SELECT JSON_EXISTS('{\"a\":1}', '$.a')",
            },
        )
        self.validate_all(
            "SELECT JSON_BUILD_OBJECT('name', name) FROM t",
            read={
                "singlestore": "SELECT JSON_BUILD_OBJECT('name', name) FROM t",
                "": "SELECT JSON_OBJECT('name', name) FROM t",
            },
        )
        self.validate_identity("JSON_BUILD_OBJECT('name', name)").assert_is(exp.JSONObject)

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
        self.validate_all(
            "SELECT POWER(id, 1 / 3) FROM orders",
            read={
                "": "SELECT CBRT(id) FROM orders",
                "singlestore": "SELECT POWER(id, 1 / 3) FROM orders",
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
        self.validate_all(
            "SELECT LOWER('ABC') RLIKE LOWER('a.*')",
            read={
                "postgres": "SELECT 'ABC' ~* 'a.*'",
                "singlestore": "SELECT LOWER('ABC') RLIKE LOWER('a.*')",
            },
        )
        self.validate_all(
            "SELECT CONCAT(SUBSTRING('abcdef', 1, 2 - 1), 'xyz', SUBSTRING('abcdef', 2 + 3))",
            read={
                "singlestore": "SELECT CONCAT(SUBSTRING('abcdef', 1, 2 - 1), 'xyz', SUBSTRING('abcdef', 2 + 3))",
                "": "SELECT STUFF('abcdef', 2, 3, 'xyz')",
            },
        )
        self.validate_all(
            "SELECT SHA(email) FROM t",
            read={
                "singlestore": "SELECT SHA(email) FROM t",
                "": "SELECT STANDARD_HASH(email) FROM t",
            },
        )
        self.validate_all(
            "SELECT SHA(email) FROM t",
            read={
                "singlestore": "SELECT SHA(email) FROM t",
                "": "SELECT STANDARD_HASH(email, 'sha') FROM t",
            },
        )
        self.validate_all(
            "SELECT MD5(email) FROM t",
            read={
                "singlestore": "SELECT MD5(email) FROM t",
                "": "SELECT STANDARD_HASH(email, 'MD5') FROM t",
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
            "SELECT '2019-03-14 06:04:12' :> DATE",
            read={
                "": "SELECT TIME_STR_TO_DATE('2019-03-14 06:04:12')",
                "singlestore": "SELECT '2019-03-14 06:04:12' :> DATE",
            },
        )
        self.validate_all(
            "SELECT CONVERT_TZ(NOW() :> TIMESTAMP, 'GMT', 'UTC')",
            read={
                "spark2": "SELECT TO_UTC_TIMESTAMP(NOW(), 'GMT')",
                "singlestore": "SELECT CONVERT_TZ(NOW() :> TIMESTAMP, 'GMT', 'UTC')",
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
        self.validate_all(
            "SELECT '2019-03-14 06:04:12' :> TIME",
            read={
                # zone parameter is not supported in SingleStore, so it is ignored
                "bigquery": "SELECT TIME('2019-03-14 06:04:12', 'GMT')",
                "singlestore": "SELECT '2019-03-14 06:04:12' :> TIME",
            },
        )
        self.validate_all(
            "SELECT DATE_ADD(NOW(), INTERVAL '1' MONTH)",
            read={
                "bigquery": "SELECT DATETIME_ADD(NOW(), INTERVAL 1 MONTH)",
                "singlestore": "SELECT DATE_ADD(NOW(), INTERVAL '1' MONTH)",
            },
        )
        self.validate_all(
            "SELECT DATE_TRUNC('MINUTE', '2016-08-08 12:05:31')",
            read={
                "bigquery": "SELECT DATETIME_TRUNC('2016-08-08 12:05:31', MINUTE)",
                "singlestore": "SELECT DATE_TRUNC('MINUTE', '2016-08-08 12:05:31')",
            },
        )
        self.validate_all(
            "SELECT DATE_SUB('2010-04-02', INTERVAL '1' WEEK)",
            read={
                "bigquery": "SELECT DATETIME_SUB('2010-04-02', INTERVAL '1' WEEK)",
                "singlestore": "SELECT DATE_SUB('2010-04-02', INTERVAL '1' WEEK)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMPDIFF(QUARTER, '2009-02-13', '2013-09-01')",
            read={
                "singlestore": "SELECT TIMESTAMPDIFF(QUARTER, '2009-02-13', '2013-09-01')",
                "": "SELECT DATETIME_DIFF('2013-09-01', '2009-02-13', QUARTER)",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMPDIFF(QUARTER, '2009-02-13', '2013-09-01')",
            read={
                "singlestore": "SELECT TIMESTAMPDIFF(QUARTER, '2009-02-13', '2013-09-01')",
                "bigquery": "SELECT DATE_DIFF('2013-09-01', '2009-02-13', QUARTER)",
                "duckdb": "SELECT DATE_DIFF('QUARTER', '2009-02-13', '2013-09-01')",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF(DATE('2013-09-01'), DATE('2009-02-13'))",
            read={
                "hive": "SELECT DATEDIFF('2013-09-01', '2009-02-13')",
                "singlestore": "SELECT DATEDIFF(DATE('2013-09-01'), DATE('2009-02-13'))",
            },
        )
        self.validate_all(
            "SELECT DATE_TRUNC('MINUTE', '2016-08-08 12:05:31')",
            read={
                "": "SELECT TIMESTAMP_TRUNC('2016-08-08 12:05:31', MINUTE)",
                "singlestore": "SELECT DATE_TRUNC('MINUTE', '2016-08-08 12:05:31')",
            },
        )
        self.validate_all(
            "SELECT TIMESTAMPDIFF(WEEK, '2009-01-01', '2009-12-31') AS numweeks",
            read={
                "redshift": "SELECT datediff(week,'2009-01-01','2009-12-31') AS numweeks",
                "singlestore": "SELECT TIMESTAMPDIFF(WEEK, '2009-01-01', '2009-12-31') AS numweeks",
            },
        )
        self.validate_all(
            "SELECT DATEDIFF('2009-12-31', '2009-01-01') AS numweeks",
            read={
                "": "SELECT TS_OR_DS_DIFF('2009-12-31', '2009-01-01') AS numweeks",
                "singlestore": "SELECT DATEDIFF('2009-12-31', '2009-01-01') AS numweeks",
            },
        )
        self.validate_all(
            "SELECT CURRENT_DATE()",
            read={
                "": "SELECT CURRENT_DATE()",
                "singlestore": "SELECT CURRENT_DATE",
            },
        )
        self.validate_all(
            "SELECT UTC_DATE()",
            read={
                "": "SELECT CURRENT_DATE('UTC')",
                "singlestore": "SELECT UTC_DATE",
            },
            write={"": "SELECT CURRENT_DATE('UTC')"},
        )
        self.validate_all(
            "SELECT CURRENT_TIME()",
            read={
                "": "SELECT CURRENT_TIME()",
                "singlestore": "SELECT CURRENT_TIME",
            },
        )
        self.validate_identity("SELECT CURRENT_TIME(6)")
        self.validate_all(
            "SELECT UTC_TIME()",
            read={
                "": "SELECT CURRENT_TIME('UTC')",
                "singlestore": "SELECT UTC_TIME",
            },
            write={"": "SELECT CURRENT_TIME('UTC')"},
        )
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP()",
            read={
                "": "SELECT CURRENT_TIMESTAMP()",
                "singlestore": "SELECT CURRENT_TIMESTAMP",
            },
        )
        self.validate_identity("SELECT CURRENT_TIMESTAMP(6)")
        self.validate_all(
            "SELECT UTC_TIMESTAMP()",
            read={
                "": "SELECT CURRENT_TIMESTAMP('UTC')",
                "singlestore": "SELECT UTC_TIMESTAMP",
            },
            write={"": "SELECT CURRENT_TIMESTAMP('UTC')"},
        )
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP(6) :> DATETIME(6)",
            read={
                "bigquery": "SELECT CURRENT_DATETIME()",
                "singlestore": "SELECT CURRENT_TIMESTAMP(6) :> DATETIME(6)",
            },
        )
        self.validate_identity("SELECT UTC_TIMESTAMP(6)")
        self.validate_identity("SELECT UTC_TIME(6)")

    def test_types(self):
        self.validate_all(
            "CREATE TABLE testTypes (a DECIMAL(10, 20))",
            read={
                "singlestore": "CREATE TABLE testTypes (a DECIMAL(10, 20))",
                "bigquery": "CREATE TABLE testTypes (a BIGDECIMAL(10, 20))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a BOOLEAN)",
            read={
                "singlestore": "CREATE TABLE testTypes (a BOOLEAN)",
                "tsql": "CREATE TABLE testTypes (a BIT)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DATE)",
            read={
                "singlestore": "CREATE TABLE testTypes (a DATE)",
                "clickhouse": "CREATE TABLE testTypes (a DATE32)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DATETIME)",
            read={
                "singlestore": "CREATE TABLE testTypes (a DATETIME)",
                "clickhouse": "CREATE TABLE testTypes (a DATETIME64)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DECIMAL(9, 3))",
            read={
                "singlestore": "CREATE TABLE testTypes (a DECIMAL(9, 3))",
                "clickhouse": "CREATE TABLE testTypes (a DECIMAL32(3))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DECIMAL(18, 3))",
            read={
                "singlestore": "CREATE TABLE testTypes (a DECIMAL(18, 3))",
                "clickhouse": "CREATE TABLE testTypes (a DECIMAL64(3))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DECIMAL(38, 3))",
            read={
                "singlestore": "CREATE TABLE testTypes (a DECIMAL(38, 3))",
                "clickhouse": "CREATE TABLE testTypes (a DECIMAL128(3))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a DECIMAL(65, 3))",
            read={
                "singlestore": "CREATE TABLE testTypes (a DECIMAL(65, 3))",
                "clickhouse": "CREATE TABLE testTypes (a DECIMAL256(3))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a ENUM('a'))",
            read={
                "singlestore": "CREATE TABLE testTypes (a ENUM('a'))",
                "clickhouse": "CREATE TABLE testTypes (a ENUM8('a'))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a ENUM('a'))",
            read={
                "singlestore": "CREATE TABLE testTypes (a ENUM('a'))",
                "clickhouse": "CREATE TABLE testTypes (a ENUM16('a'))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a TEXT(2))",
            read={
                "singlestore": "CREATE TABLE testTypes (a TEXT(2))",
                "clickhouse": "CREATE TABLE testTypes (a FIXEDSTRING(2))",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHY)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHY)",
                "snowflake": "CREATE TABLE testTypes (a GEOMETRY)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHYPOINT)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHYPOINT)",
                "clickhouse": "CREATE TABLE testTypes (a POINT)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHY)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHY)",
                "clickhouse": "CREATE TABLE testTypes (a RING)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHY)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHY)",
                "clickhouse": "CREATE TABLE testTypes (a LINESTRING)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHY)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHY)",
                "clickhouse": "CREATE TABLE testTypes (a POLYGON)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a GEOGRAPHY)",
            read={
                "singlestore": "CREATE TABLE testTypes (a GEOGRAPHY)",
                "clickhouse": "CREATE TABLE testTypes (a MULTIPOLYGON)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a BSON)",
            read={
                "singlestore": "CREATE TABLE testTypes (a BSON)",
                "postgres": "CREATE TABLE testTypes (a JSONB)",
            },
        )
        self.validate_identity("CREATE TABLE testTypes (a TIMESTAMP(6))")
        self.validate_all(
            "CREATE TABLE testTypes (a TIMESTAMP)",
            read={
                "singlestore": "CREATE TABLE testTypes (a TIMESTAMP)",
                "duckdb": "CREATE TABLE testTypes (a TIMESTAMP_S)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a TIMESTAMP(6))",
            read={
                "singlestore": "CREATE TABLE testTypes (a TIMESTAMP(6))",
                "duckdb": "CREATE TABLE testTypes (a TIMESTAMP_MS)",
            },
        )
        self.validate_all(
            "CREATE TABLE testTypes (a BLOB)",
            read={
                "singlestore": "CREATE TABLE testTypes (a BLOB)",
                "": "CREATE TABLE testTypes (a VARBINARY)",
            },
        )

    def test_column_with_tablename(self):
        self.validate_identity("SELECT `t0`.`name` FROM `t0`")

    def test_unicodestring_sql(self):
        self.validate_all(
            "SELECT 'data'",
            read={"presto": "SELECT U&'d\\0061t\\0061'", "singlestore": "SELECT 'data'"},
        )

    def test_collate_sql(self):
        self.validate_all(
            "SELECT name :> LONGTEXT COLLATE 'utf8mb4_bin' FROM `users`",
            read={
                "": "SELECT name COLLATE 'utf8mb4_bin' FROM users",
            },
        )
        self.validate_identity(
            "SELECT name :> LONGTEXT COLLATE 'utf8mb4_bin' FROM `users`",
            "SELECT name :> LONGTEXT :> LONGTEXT COLLATE 'utf8mb4_bin' FROM `users`",
        )

    def test_match_against(self):
        self.validate_identity(
            "SELECT MATCH(name) AGAINST('search term') FROM products"
        ).expressions[0].assert_is(exp.MatchAgainst)
        self.validate_identity(
            "SELECT MATCH(name, name) AGAINST('book') FROM products"
        ).expressions[0].assert_is(exp.MatchAgainst)
        self.validate_identity(
            "SELECT MATCH(TABLE products2) AGAINST('search term') FROM products2"
        ).expressions[0].assert_is(exp.MatchAgainst)

    def test_show(self):
        self.validate_identity("SHOW AGGREGATES FROM db1")
        self.validate_identity("SHOW AGGREGATES LIKE 'multiply%'")
        self.validate_identity("SHOW CDC EXTRACTOR POOL")
        self.validate_identity("SHOW CREATE AGGREGATE avg_udaf")
        self.validate_identity("SHOW CREATE PIPELINE mypipeline")
        self.validate_identity("SHOW CREATE PROJECTION lineitem_sort_shipdate FOR TABLE lineitem")
        self.validate_identity("SHOW DATABASE STATUS")
        self.validate_identity("SHOW DISTRIBUTED_PLANCACHE STATUS")
        self.validate_identity("SHOW FULLTEXT SERVICE STATUS")
        self.validate_identity("SHOW FULLTEXT SERVICE METRICS LOCAL")
        self.validate_identity("SHOW FULLTEXT SERVICE METRICS FOR NODE 1")
        self.validate_identity("SHOW FUNCTIONS FROM db LIKE 'a'")
        self.validate_identity("SHOW GROUPS")
        self.validate_identity("SHOW GROUPS FOR ROLE 'role_name_0'")
        self.validate_identity("SHOW GROUPS FOR USER 'root'")
        self.validate_identity("SHOW INDEXES FROM mytbl", "SHOW INDEX FROM mytbl")
        self.validate_identity("SHOW KEYS FROM mytbl", "SHOW INDEX FROM mytbl")
        self.validate_identity("SHOW LINKS ON Orderdb")
        self.validate_identity("SHOW LOAD ERRORS")
        self.validate_identity("SHOW LOAD WARNINGS")
        self.validate_identity("SHOW PARTITIONS ON memsql_demo")
        self.validate_identity("SHOW PIPELINES")
        self.validate_identity("SHOW PLAN JSON 25")
        self.validate_identity("SHOW PLAN 25")
        self.validate_identity("SHOW PLANCACHE")
        self.validate_identity("SHOW PROCEDURES FROM dbExample")
        self.validate_identity("SHOW PROCEDURES LIKE '%sp%'")
        self.validate_identity("SHOW PROJECTIONS ON TABLE t")
        self.validate_identity("SHOW PROJECTIONS")
        self.validate_identity("SHOW REPLICATION STATUS")
        self.validate_identity("SHOW REPRODUCTION")
        self.validate_identity("SHOW REPRODUCTION INTO OUTFILE 'a'")
        self.validate_identity("SHOW RESOURCE POOLS")
        self.validate_identity("SHOW ROLES LIKE 'xyz'")
        self.validate_identity("SHOW ROLES FOR GROUP 'group_0'")
        self.validate_identity("SHOW ROLES FOR USER 'root'")
        self.validate_identity("SHOW STATUS")
        self.validate_identity("SHOW USERS")
        self.validate_identity("SHOW USERS FOR GROUP 'group_name'")
        self.validate_identity("SHOW USERS FOR ROLE 'role_name'")

    def test_truncate(self):
        self.validate_all(
            "TRUNCATE t1; TRUNCATE t2",
            read={
                "": "TRUNCATE TABLE t1, t2",
            },
        )

    def test_vector(self):
        self.validate_all(
            "CREATE TABLE t (a VECTOR(10, I32))",
            read={
                "snowflake": "CREATE TABLE t (a VECTOR(INT, 10))",
                "singlestore": "CREATE TABLE t (a VECTOR(10, I32))",
            },
            write={
                "snowflake": "CREATE TABLE t (a VECTOR(INT, 10))",
            },
        )
        self.validate_all(
            "CREATE TABLE t (a VECTOR(10))",
            read={
                "snowflake": "CREATE TABLE t (a VECTOR(10))",
                "singlestore": "CREATE TABLE t (a VECTOR(10))",
            },
            write={
                "snowflake": "CREATE TABLE t (a VECTOR(10))",
            },
        )

    def test_alter(self):
        self.validate_identity("ALTER TABLE t CHANGE middle_initial middle_name")
        self.validate_identity("ALTER TABLE t MODIFY COLUMN name TEXT COLLATE 'binary'")

    def test_constraints(self):
        self.validate_all(
            "CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED AUTO NOT NULL)",
            read={
                "": "CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED NOT NULL)",
                "singlestore": "CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) AUTO NOT NULL)",
            },
        )
        self.validate_identity(
            "CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED BIGINT NOT NULL)"
        )
