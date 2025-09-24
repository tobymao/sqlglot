--------------------------------------
-- Dialect
--------------------------------------
ABS(1);
INT;

ABS(1.5);
DOUBLE;

GREATEST(1, 2, 3);
INT;

GREATEST(1, 2.5, 3);
DOUBLE;

LEAST(1, 2, 3);
INT;

LEAST(1, 2.5, 3);
DOUBLE;

CURRENT_TIME();
TIME;

TIME_ADD(CAST('09:05:03' AS TIME), INTERVAL 2 HOUR);
TIME;

TIME_SUB(CAST('09:05:03' AS TIME), INTERVAL 2 HOUR);
TIME;

SORT_ARRAY(ARRAY(tbl.str_col));
ARRAY<STRING>;

SORT_ARRAY(ARRAY(tbl.double_col));
ARRAY<DOUBLE>;

SORT_ARRAY(ARRAY(tbl.bigint_col));
ARRAY<BIGINT>;

tbl.bigint || tbl.str_col;
VARCHAR;

tbl.str_col || tbl.bigint;
VARCHAR;

ARRAY_REVERSE(['a', 'b']);
ARRAY<VARCHAR>;

ARRAY_REVERSE([1, 1.5]);
ARRAY<DOUBLE>;

ARRAY_SLICE([1, 1.5], 1, 2);
ARRAY<DOUBLE>;

FROM_BASE32(tbl.str_col);
BINARY;

FROM_BASE64(tbl.str_col);
BINARY;

ANY_VALUE(tbl.str_col);
STRING;

ANY_VALUE(tbl.array_col);
ARRAY<STRING>;

CHR(65);
VARCHAR;

COUNTIF(tbl.bigint_col > 1);
BIGINT;

LAST_VALUE(tbl.bigint_col) OVER (ORDER BY tbl.bigint_col);
BIGINT;

TO_BASE32(tbl.bytes_col);
VARCHAR;

TO_BASE64(tbl.bytes_col);
VARCHAR;

UNIX_DATE(tbl.date_col);
BIGINT;

UNIX_SECONDS(tbl.timestamp_col);
BIGINT;

STARTS_WITH(tbl.str_col, prefix);
BOOLEAN;

ENDS_WITH(tbl.str_col, suffix);
BOOLEAN;

ASCII('A');
INT;

UNICODE('bcd');
INT;

LAST_DAY(tbl.timestamp_col);
DATE;

JUSTIFY_DAYS(INTERVAL '1' DAY);
INTERVAL;

JUSTIFY_HOURS(INTERVAL '1' HOUR);
INTERVAL;

JUSTIFY_INTERVAL(INTERVAL '1' HOUR);
INTERVAL;

UNIX_MICROS(CAST('2008-12-25 15:30:00+00' AS TIMESTAMP));
BIGINT;

UNIX_MILLIS(CAST('2008-12-25 15:30:00+00' AS TIMESTAMP));
BIGINT;

--------------------------------------
-- Spark2 / Spark3 / Databricks
--------------------------------------

# dialect: spark2, spark, databricks
SUBSTRING(tbl.str_col, 0, 0);
STRING;

# dialect: spark2, spark, databricks
SUBSTRING(tbl.bin_col, 0, 0);
BINARY;

# dialect: spark2, spark, databricks
CONCAT(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: spark2, spark, databricks
CONCAT(tbl.bin_col, tbl.str_col);
STRING;

# dialect: spark2, spark, databricks
CONCAT(tbl.str_col, tbl.bin_col);
STRING;

# dialect: spark2, spark, databricks
CONCAT(tbl.str_col, tbl.str_col);
STRING;

# dialect: spark2, spark, databricks
CONCAT(tbl.str_col, unknown);
STRING;

# dialect: spark2, spark, databricks
CONCAT(tbl.bin_col, unknown);
UNKNOWN;

# dialect: spark2, spark, databricks
CONCAT(unknown, unknown);
UNKNOWN;

# dialect: spark2, spark, databricks
LPAD(tbl.bin_col, 1, tbl.bin_col);
BINARY;

# dialect: spark2, spark, databricks
RPAD(tbl.bin_col, 1, tbl.bin_col);
BINARY;

# dialect: spark2, spark, databricks
LPAD(tbl.bin_col, 1, tbl.str_col);
STRING;

# dialect: spark2, spark, databricks
RPAD(tbl.bin_col, 1, tbl.str_col);
STRING;

# dialect: spark2, spark, databricks
LPAD(tbl.str_col, 1, tbl.bin_col);
STRING;

# dialect: spark2, spark, databricks
RPAD(tbl.str_col, 1, tbl.bin_col);
STRING;

# dialect: spark2, spark, databricks
LPAD(tbl.str_col, 1, tbl.str_col);
STRING;

# dialect: spark2, spark, databricks
RPAD(tbl.str_col, 1, tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
IF(cond, tbl.double_col, tbl.bigint_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
IF(cond, tbl.bigint_col, tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark
IF(cond, tbl.double_col, tbl.str_col);
STRING;

# dialect: hive, spark2, spark
IF(cond, tbl.str_col, tbl.double_col);
STRING;

# dialect: databricks
IF(cond, tbl.str_col, tbl.double_col);
DOUBLE;

# dialect: databricks
IF(cond, tbl.double_col, tbl.str_col);
DOUBLE;

# dialect: hive, spark2, spark
IF(cond, tbl.date_col, tbl.str_col);
STRING;

# dialect: hive, spark2, spark
IF(cond, tbl.str_col, tbl.date_col);
STRING;

# dialect: databricks
IF(cond, tbl.date_col, tbl.str_col);
DATE;

# dialect: databricks
IF(cond, tbl.str_col, tbl.date_col);
DATE;

# dialect: hive, spark2, spark, databricks
IF(cond, tbl.date_col, tbl.timestamp_col);
TIMESTAMP;

# dialect: hive, spark2, spark, databricks
IF(cond, tbl.timestamp_col, tbl.date_col);
TIMESTAMP;

# dialect: hive, spark2, spark, databricks
IF(cond, NULL, tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
IF(cond, tbl.str_col, NULL);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.str_col, tbl.date_col, tbl.bigint_col);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.date_col, tbl.str_col, tbl.bigint_col);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.date_col, tbl.bigint_col, tbl.str_col);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.str_col, tbl.date_col, tbl.bigint_col);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.date_col, tbl.str_col, tbl.bigint_col);
STRING;

# dialect: hive, spark2, spark
COALESCE(tbl.date_col, NULL, tbl.bigint_col, tbl.str_col);
STRING;

# dialect: databricks
COALESCE(tbl.str_col, tbl.bigint_col);
BIGINT;

# dialect: databricks
COALESCE(tbl.bigint_col, tbl.str_col);
BIGINT;

# dialect: databricks
COALESCE(tbl.str_col, NULL, tbl.bigint_col);
BIGINT;

# dialect: databricks
COALESCE(tbl.bigint_col, NULL, tbl.str_col);
BIGINT;

# dialect: databricks
COALESCE(tbl.bool_col, tbl.str_col);
BOOLEAN;

# dialect: hive, spark2, spark
COALESCE(tbl.interval_col, tbl.str_col);
STRING;

# dialect: databricks
COALESCE(tbl.interval_col, tbl.str_col);
INTERVAL;

# dialect: databricks
COALESCE(tbl.bin_col, tbl.str_col);
BINARY;

--------------------------------------
-- BigQuery
--------------------------------------

# dialect: bigquery
SIGN(1);
INT;

# dialect: bigquery
SIGN(1.5);
DOUBLE;

# dialect: bigquery
CEIL(1);
DOUBLE;

# dialect: bigquery
CEIL(5.5);
DOUBLE;

# dialect: bigquery
CEIL(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
FLOOR(1);
DOUBLE;

# dialect: bigquery
FLOOR(5.5);
DOUBLE;

# dialect: bigquery
FLOOR(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
SQRT(1);
DOUBLE;

# dialect: bigquery
SQRT(5.5);
DOUBLE;

# dialect: bigquery
SQRT(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
LN(1);
DOUBLE;

# dialect: bigquery
LN(5.5);
DOUBLE;

# dialect: bigquery
LN(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
LOG(1);
DOUBLE;

# dialect: bigquery
LOG(5.5);
DOUBLE;

# dialect: bigquery
LOG(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
ROUND(1);
DOUBLE;

# dialect: bigquery
ROUND(5.5);
DOUBLE;

# dialect: bigquery
ROUND(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
EXP(1);
DOUBLE;

# dialect: bigquery
EXP(5.5);
DOUBLE;

# dialect: bigquery
EXP(tbl.bignum_col);
BIGDECIMAL;

# dialect: bigquery
CONCAT(tbl.str_col, tbl.str_col);
STRING;

# dialect: bigquery
CONCAT(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: bigquery
CONCAT(0, tbl.str_col);
STRING;

# dialect: bigquery
CONCAT(tbl.str_col, 0);
STRING;

# dialect: bigquery
LEFT(tbl.str_col, 1);
STRING;

# dialect: bigquery
LEFT(tbl.bin_col, 1);
BINARY;

# dialect: bigquery
RIGHT(tbl.str_col, 1);
STRING;

# dialect: bigquery
RIGHT(tbl.bin_col, 1);
BINARY;

# dialect: bigquery
LOWER(tbl.str_col);
STRING;

# dialect: bigquery
LOWER(tbl.bin_col);
BINARY;

# dialect: bigquery
UPPER(tbl.str_col);
STRING;

# dialect: bigquery
UPPER(tbl.bin_col);
BINARY;

# dialect: bigquery
LPAD(tbl.str_col, 1, tbl.str_col);
STRING;

# dialect: bigquery
LPAD(tbl.bin_col, 1, tbl.bin_col);
BINARY;

# dialect: bigquery
RPAD(tbl.str_col, 1, tbl.str_col);
STRING;

# dialect: bigquery
RPAD(tbl.bin_col, 1, tbl.bin_col);
BINARY;

# dialect: bigquery
LTRIM(tbl.str_col);
STRING;

# dialect: bigquery
LTRIM(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: bigquery
RTRIM(tbl.str_col);
STRING;

# dialect: bigquery
RTRIM(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: bigquery
TRIM(tbl.str_col);
STRING;

# dialect: bigquery
TRIM(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: bigquery
REGEXP_EXTRACT(tbl.str_col, pattern);
STRING;

# dialect: bigquery
REGEXP_EXTRACT(tbl.bin_col, pattern);
BINARY;

# dialect: bigquery
REGEXP_REPLACE(tbl.str_col, pattern, replacement);
STRING;

# dialect: bigquery
REGEXP_REPLACE(tbl.bin_col, pattern, replacement);
BINARY;

# dialect: bigquery
REPEAT(tbl.str_col, 1);
STRING;

# dialect: bigquery
REPEAT(tbl.bin_col, 1);
BINARY;

# dialect: bigquery
SUBSTRING(tbl.str_col, 1);
STRING;

# dialect: bigquery
SUBSTRING(tbl.bin_col, 1);
BINARY;

# dialect: bigquery
SPLIT(tbl.str_col, delim);
ARRAY<STRING>;

# dialect: bigquery
SPLIT(tbl.bin_col, delim);
ARRAY<BINARY>;

# dialect: bigquery
STRING(json_expr);
STRING;

# dialect: bigquery
STRING(timestamp_expr, timezone);
STRING;

# dialect: bigquery
ARRAY_CONCAT(['a'], ['b']);
ARRAY<STRING>;

# dialect: bigquery
ARRAY_CONCAT_AGG(tbl.array_col);
ARRAY<STRING>;

# dialect: bigquery
ARRAY_TO_STRING(['a'], ['b'], ',');
STRING;

# dialect: bigquery
ARRAY_FIRST(['a', 'b']);
STRING;

# dialect: bigquery
ARRAY_LAST(['a', 'b']);
STRING;

# dialect: bigquery
ARRAY_FIRST([1, 1.5]);
DOUBLE;

# dialect: bigquery
ARRAY_LAST([1, 1.5]);
DOUBLE;

# dialect: bigquery
GENERATE_ARRAY(1, 5, 0.3);
ARRAY<DOUBLE>;

# dialect: bigquery
GENERATE_ARRAY(1, 5);
ARRAY<BIGINT>;

# dialect: bigquery
GENERATE_ARRAY(1, 2.5);
ARRAY<DOUBLE>;

# dialect: bigquery
INT64(JSON '999');
BIGINT;

# dialect: bigquery
LOGICAL_AND(tbl.bool_col);
BOOLEAN;

# dialect: bigquery
LOGICAL_OR(tbl.bool_col);
BOOLEAN;

# dialect: bigquery
MAKE_INTERVAL(1, 6, 15);
INTERVAL;

# dialect: bigquery
SHA1(tbl.str_col);
BINARY;

# dialect: bigquery
SHA256(tbl.str_col);
BINARY;

# dialect: bigquery
SHA512(tbl.str_col);
BINARY;

# dialect: bigquery
CORR(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: bigquery
COVAR_POP(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: bigquery
COVAR_SAMP(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: bigquery
DATETIME(2025, 1, 1, 12, 0, 0);
DATETIME;

# dialect: bigquery
LAG(tbl.bigint_col, 1 , 2.5) OVER (ORDER BY tbl.bigint_col);
DOUBLE;

# dialect: bigquery
LAG(tbl.bigint_col, 1 , 2) OVER (ORDER BY tbl.bigint_col);
BIGINT;

# dialect: bigquery
ASCII('A');
BIGINT;

# dialect: bigquery
UNICODE('bcd');
BIGINT;

# dialect: bigquery
BIT_AND(tbl.bin_col);
BIGINT;

# dialect: bigquery
BIT_OR(tbl.bin_col);
BIGINT;

# dialect: bigquery
BIT_XOR(tbl.bin_col);
BIGINT;

# dialect: bigquery
BIT_COUNT(tbl.bin_col);
BIGINT;

# dialect: bigquery
JSON_ARRAY(10);
JSON;

# dialect: bigquery
JSON_ARRAY(10, [1, 2]);
JSON;

# dialect: bigquery
JSON_VALUE(JSON '{"foo": "1" }', '$.foo');
STRING;

# dialect: bigquery
JSON_EXTRACT_SCALAR(JSON '["a","b"]');
STRING;

# dialect: bigquery
JSON_VALUE_ARRAY(JSON '["a","b"]');
ARRAY<STRING>;

# dialect: bigquery
JSON_EXTRACT_STRING_ARRAY(JSON '["a","b"]');
ARRAY<STRING>;

# dialect: bigquery
JSON_TYPE(JSON '1');
STRING;

# dialect: bigquery
GENERATE_TIMESTAMP_ARRAY('2016-10-05', '2016-10-07', INTERVAL '1' DAY);
ARRAY<TIMESTAMP>;

# dialect: bigquery
TIME(15, 30, 00);
TIME;

# dialect: bigquery
TIME(TIMESTAMP "2008-12-25 15:30:00");
TIME;

# dialect: bigquery
TIME(DATETIME "2008-12-25 15:30:00");
TIME;

# dialect: bigquery
TIME_TRUNC(TIME "15:30:00", HOUR);
TIME;

# dialect: bigquery
DATE_FROM_UNIX_DATE(1);
DATE;

# dialect: bigquery
DATE_TRUNC(DATE '2008-12-25', MONTH);
DATE;

# dialect: bigquery
DATE_TRUNC(TIMESTAMP '2008-12-25', MONTH);
TIMESTAMP;

# dialect: bigquery
DATE_TRUNC(DATETIME '2008-12-25', MONTH);
DATETIME;

# dialect: bigquery
TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC");
TIMESTAMP;

# dialect: bigquery
TIMESTAMP_TRUNC(DATETIME "2008-12-25 15:30:00", DAY);
DATETIME;

# dialect: bigquery
PARSE_DATETIME('%a %b %e %I:%M:%S %Y', 'Thu Dec 25 07:30:00 2008');
DATETIME;

# dialect: bigquery
FORMAT_TIME("%R", TIME "15:30:00");
STRING;

# dialect: bigquery
PARSE_TIME("%I:%M:%S", "07:30:00");
TIME;

# dialect: bigquery
BYTE_LENGTH("foo");
BIGINT;

# dialect: bigquery
CODE_POINTS_TO_STRING([65, 255, 513, 1024]);
STRING;

# dialect: bigquery
REVERSE("abc");
STRING;

# dialect: bigquery
REVERSE(tbl.bin_col);
BINARY;

# dialect: bigquery
REVERSE(b'1a3');
BINARY;

# dialect: bigquery
REGEXP_EXTRACT_ALL('Try `func(x)` or `func(y)`', '`(.+?)`');
ARRAY<STRING>;

# dialect: bigquery
REGEXP_EXTRACT_ALL(b'\x48\x65\x6C\x6C\x6F', b'(\x6C+)');
ARRAY<BINARY>;

# dialect: bigquery
REPLACE ('cherry', 'pie', 'cobbler');
STRING;

# dialect: bigquery
REPLACE(b'\x48\x65\x6C\x6C\x6F', b'\x6C\x6C', b'\x59\x59');
BINARY;

# dialect: bigquery
TRANSLATE('AaBbCc', 'abc', '1');
STRING;

# dialect: bigquery
TRANSLATE(b'AaBbCc', b'abc', b'123');
BINARY;

# dialect: bigquery
SOUNDEX('foo');
STRING;

# dialect: bigquery
MD5('foo');
BINARY;

# dialect: bigquery
MAX_BY(tbl.str_col, tbl.bigint_col);
STRING;

# dialect: bigquery
MAX_BY(tbl.bigint_col, tbl.str_col);
BIGINT;

# dialect: bigquery
MIN_BY(tbl.str_col, tbl.bigint_col);
STRING;

# dialect: bigquery
MIN_BY(tbl.bigint_col, tbl.str_col);
BIGINT;

# dialect: bigquery
GROUPING(tbl.str_col);
BIGINT;

# dialect: bigquery
GROUPING(tbl.bigint_col);
BIGINT;

# dialect: bigquery
FARM_FINGERPRINT('foo');
BIGINT;

# dialect: bigquery
FARM_FINGERPRINT(b'foo');
BIGINT;

# dialect: bigquery
APPROX_TOP_COUNT(tbl.str_col, 2);
ARRAY<STRUCT<STRING, BIGINT>>;

# dialect: bigquery
APPROX_TOP_COUNT(tbl.bigint_col, 2);
ARRAY<STRUCT<BIGINT, BIGINT>>;

# dialect: bigquery
APPROX_TOP_SUM(tbl.str_col, 1.5, 2);
ARRAY<STRUCT<STRING, BIGINT>>;

# dialect: bigquery
APPROX_TOP_SUM(tbl.bigint_col, 1.5, 2);
ARRAY<STRUCT<BIGINT, BIGINT>>;

# dialect: bigquery
APPROX_QUANTILES(tbl.bigint_col, 2);
ARRAY<BIGINT>;

# dialect: bigquery
APPROX_QUANTILES(tbl.str_col, 2);
ARRAY<STRING>;

# dialect: bigquery
APPROX_QUANTILES(DISTINCT tbl.bigint_col, 2);
ARRAY<BIGINT>;

# dialect: bigquery
APPROX_QUANTILES(DISTINCT tbl.str_col, 2);
ARRAY<STRING>;

# dialect: bigquery
SAFE_CONVERT_BYTES_TO_STRING(b'\xc2');
STRING;

# dialect: bigquery
FROM_HEX('foo');
BINARY;

# dialect: bigquery
TO_HEX(b'foo');
STRING;

# dialect: bigquery
TO_CODE_POINTS('foo');
ARRAY<BIGINT>;

# dialect: bigquery
TO_CODE_POINTS(b'\x66\x6f\x6f');
ARRAY<BIGINT>;

# dialect: bigquery
CODE_POINTS_TO_BYTES([65, 98]);
BINARY;

# dialect: bigquery
PARSE_BIGNUMERIC('1.2');
BIGDECIMAL;

# dialect: bigquery
PARSE_NUMERIC('1.2');
DECIMAL;

# dialect: bigquery
BOOL(PARSE_JSON('true'));
BOOLEAN;

# dialect: bigquery
FLOAT64(PARSE_JSON('9.8'));
FLOAT64;

# dialect: bigquery
FLOAT64(PARSE_JSON('9.8'), wide_number_mode => 'round');
FLOAT64;

# dialect: bigquery
CONTAINS_SUBSTR('aa', 'a');
BOOLEAN;

# dialect: bigquery
CONTAINS_SUBSTR(PARSE_JSON('{"lunch":"soup"}'), 'lunch', json_scope => 'JSON_VALUES');
BOOLEAN;

# dialect: bigquery
NORMALIZE('\u00ea');
STRING;

# dialect: bigquery
NORMALIZE('\u00ea', NFKC);
STRING;

# dialect: bigquery
NORMALIZE_AND_CASEFOLD('\u00ea', NFKC);
STRING;

# dialect: bigquery
NORMALIZE_AND_CASEFOLD('\u00ea', NFKC);
STRING;

# dialect: bigquery
OCTET_LENGTH("foo");
BIGINT;

# dialect: bigquery
REGEXP_INSTR('ab@cd-ef', '@[^-]*');
BIGINT;

# dialect: bigquery
REGEXP_INSTR('a@cd-ef', '@[^-]*', 1, 1, 0);
BIGINT;

# dialect: bigquery
ROW_NUMBER() OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
FIRST_VALUE(tbl.bigint_col) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
FIRST_VALUE(tbl.str_col) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
FIRST_VALUE(tbl.bigint_col RESPECT NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
FIRST_VALUE(tbl.bigint_col IGNORE NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
FIRST_VALUE(tbl.str_col RESPECT NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
FIRST_VALUE(tbl.str_col IGNORE NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
NTH_VALUE(tbl.bigint_col, 2) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
NTH_VALUE(tbl.str_col, 2) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
NTH_VALUE(tbl.bigint_col, 2 RESPECT NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
NTH_VALUE(tbl.str_col, 2 RESPECT NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
NTH_VALUE(tbl.bigint_col, 2 IGNORE NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
NTH_VALUE(tbl.str_col, 2 IGNORE NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
PERCENTILE_DISC(tbl.bigint_col, 0.5) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
PERCENTILE_DISC(tbl.str_col, 0.5) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
PERCENTILE_DISC(tbl.bigint_col, 0.5 RESPECT NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
PERCENTILE_DISC(tbl.str_col, 0.5 RESPECT NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
PERCENTILE_DISC(tbl.bigint_col, 0.5 IGNORE NULLS) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
PERCENTILE_DISC(tbl.str_col, 0.5 IGNORE NULLS) OVER (ORDER BY 1);
STRING;

# dialect: bigquery
LEAD(tbl.bigint_col);
BIGINT;

# dialect: bigquery
LEAD(tbl.str_col);
STRING;

# dialect: bigquery
LEAD(tbl.bigint_col, 2);
BIGINT;

# dialect: bigquery
LEAD(tbl.str_col, 2);
STRING;

# dialect: bigquery
FORMAT('%f %E %f %f', 1.1, 2.2, 3.4, 4.4);
STRING;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS NUMERIC), CAST(1 AS NUMERIC)) OVER (ORDER BY 1);
NUMERIC;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS NUMERIC), CAST(1 AS BIGNUMERIC)) OVER (ORDER BY 1);
BIGNUMERIC;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS NUMERIC), CAST(1 AS FLOAT64)) OVER (ORDER BY 1);
FLOAT64;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS BIGNUMERIC), CAST(1 AS NUMERIC)) OVER (ORDER BY 1);
BIGNUMERIC;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS BIGNUMERIC), CAST(1 AS BIGNUMERIC)) OVER (ORDER BY 1);
BIGNUMERIC;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS BIGNUMERIC), CAST(1 AS FLOAT64)) OVER (ORDER BY 1);
FLOAT64;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS FLOAT64), CAST(1 AS NUMERIC)) OVER (ORDER BY 1);
FLOAT64;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS FLOAT64), CAST(1 AS BIGNUMERIC)) OVER (ORDER BY 1);
FLOAT64;

# dialect: bigquery
PERCENTILE_CONT(CAST(1 AS FLOAT64), CAST(1 AS FLOAT64)) OVER (ORDER BY 1);
FLOAT64;

# dialect: bigquery
CUME_DIST() OVER (ORDER BY 1);
DOUBLE;

# dialect: bigquery
DENSE_RANK() OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
NTILE(1) OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
RANK() OVER (ORDER BY 1);
BIGINT;

# dialect: bigquery
PERCENT_RANK() OVER (ORDER BY 1);
DOUBLE;

# dialect: bigquery
JSON_OBJECT('foo', 10, 'bar', TRUE);
JSON;

# dialect: bigquery
JSON_QUERY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits');
STRING;

# dialect: bigquery
JSON_QUERY(JSON_OBJECT('fruits', ['apples', 'oranges', 'grapes']), '$.fruits');
JSON;

# dialect: bigquery
JSON_EXTRACT('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits');
STRING;

# dialect: bigquery
JSON_EXTRACT(JSON_OBJECT('fruits', ['apples', 'oranges', 'grapes']), '$.fruits');
JSON;

# dialect: bigquery
JSON_QUERY_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits');
ARRAY<STRING>;

# dialect: bigquery
JSON_QUERY_ARRAY(JSON_OBJECT('fruits', ['apples', 'oranges', 'grapes']), '$.fruits');
ARRAY<JSON>;

# dialect: bigquery
JSON_EXTRACT_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits');
ARRAY<STRING>;

# dialect: bigquery
JSON_EXTRACT_ARRAY(JSON_OBJECT('fruits', ['apples', 'oranges', 'grapes']), '$.fruits');
ARRAY<JSON>;

# dialect: bigquery
JSON_ARRAY_APPEND(PARSE_JSON('["a", "b", "c"]'), '$', 1);
JSON;

# dialect: bigquery
JSON_ARRAY_APPEND(PARSE_JSON('["a", "b", "c"]'), '$', [1, 2], append_each_element => FALSE);
JSON;

# dialect: bigquery
JSON_ARRAY_INSERT(PARSE_JSON('["a", ["b", "c"], "d"]'), '$[1]', 1);
JSON;

# dialect: bigquery
JSON_ARRAY_INSERT(PARSE_JSON('["a", "b", "c"]'), '$[1]', [1, 2], insert_each_element => FALSE);
JSON;

# dialect: bigquery
JSON_ARRAY_INSERT(PARSE_JSON('["a", ["b", "c"], "d"]'), '$[1]', 1);
JSON;

# dialect: bigquery
JSON_ARRAY_INSERT(PARSE_JSON('["a", "b", "c"]'), '$[1]', [1, 2], insert_each_element => FALSE);
JSON;

# dialect: bigquery
JSON_KEYS(PARSE_JSON('{"a": {"b":1}}'));
ARRAY<STRING>;

# dialect: bigquery
JSON_KEYS(PARSE_JSON('{"a": {"b":1}}'), 1);
ARRAY<STRING>;

# dialect: bigquery
JSON_KEYS(PARSE_JSON('{"a": {"b":1}}'), 1, node => 'lax');
ARRAY<STRING>;

# dialect: bigquery
JSON_REMOVE(PARSE_JSON('["a", ["b", "c"], "d"]'), '$[1]', '$[1]');
JSON;

# dialect: bigquery
JSON_SET(PARSE_JSON('{"a": 1}'), '$', PARSE_JSON('{"b": 2, "c": 3}'));
JSON;

# dialect: bigquery
JSON_SET(PARSE_JSON('{"a": 1}'), '$.b', 999, create_if_missing => FALSE);
JSON;

# dialect: bigquery
JSON_STRIP_NULLS(PARSE_JSON('[1, null, 2, null, [null]]'));
JSON;

# dialect: bigquery
JSON_STRIP_NULLS(PARSE_JSON('[1, null, 2, null]'), include_arrays => FALSE);
JSON;

# dialect: bigquery
JSON_STRIP_NULLS(PARSE_JSON('{"a": {"b": {"c": null}}, "d": [null], "e": [], "f": 1}'), include_arrays => FALSE, remove_empty => TRUE);
JSON;

# dialect: bigquery
LAX_BOOL(PARSE_JSON('true'));
BOOLEAN;

# dialect: bigquery
LAX_FLOAT64(PARSE_JSON('9.8'));
DOUBLE;

# dialect: bigquery
LAX_INT64(PARSE_JSON('10'));
BIGINT;

# dialect: bigquery
LAX_STRING(PARSE_JSON('"str"'));
STRING;

# dialect: bigquery
TO_JSON_STRING(STRUCT(1 AS id, [10, 20] AS cords));
STRING;

# dialect: bigquery
TO_JSON(STRUCT(1 AS id, [10, 20] AS cords));
JSON;

# dialect: bigquery
ABS(CAST(-1 AS INT64));
INT64;

# dialect: bigquery
ABS(CAST(-1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
ABS(CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
ABS(CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
IS_INF(1);
BOOLEAN;

# dialect: bigquery
IS_NAN(1);
BOOLEAN;

# dialect: bigquery
CBRT(27);
DOUBLE;

# dialect: bigquery
RAND();
DOUBLE;

# dialect: bigquery
ACOS(0.5);
DOUBLE;

# dialect: bigquery
ACOSH(0.5);
DOUBLE;

# dialect: bigquery
ASIN(1);
DOUBLE;

# dialect: bigquery
ASINH(1);
DOUBLE;

# dialect: bigquery
ATAN(0.5);
DOUBLE;

# dialect: bigquery
ATANH(0.5);
DOUBLE;

# dialect: bigquery
ATAN2(0.5, 0.3);
DOUBLE;

# dialect: bigquery
COT(1);
DOUBLE;

# dialect: bigquery
COTH(1);
DOUBLE;

# dialect: bigquery
CSC(1);
DOUBLE;

# dialect: bigquery
CSCH(1);
DOUBLE;

# dialect: bigquery
SEC(1);
DOUBLE;

# dialect: bigquery
SECH(1);
DOUBLE;

# dialect: bigquery
SIN(1);
DOUBLE;

# dialect: bigquery
SINH(1);
DOUBLE;

# dialect: bigquery
COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]);
DOUBLE;

#dialect: bigquery
EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]);
DOUBLE;

# dialect: bigquery
RANGE_BUCKET(20, [0, 10, 20, 30, 40]);
BIGINT;

# dialect: bigquery
SAFE_ADD(CAST(1 AS INT64), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS INT64), CAST(1 AS INT64));
INT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS INT64), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS INT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS NUMERIC), CAST(1 AS INT64));
NUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS NUMERIC), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS NUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS NUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS BIGNUMERIC), CAST(1 AS INT64));
BIGNUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS BIGNUMERIC), CAST(1 AS NUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS BIGNUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_ADD(CAST(1 AS BIGNUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS FLOAT64), CAST(1 AS INT64));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS FLOAT64), CAST(1 AS NUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS FLOAT64), CAST(1 AS BIGNUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_ADD(CAST(1 AS FLOAT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS INT64), CAST(1 AS INT64));
INT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS INT64), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS INT64), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS INT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS NUMERIC), CAST(1 AS INT64));
NUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS NUMERIC), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS NUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS NUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS BIGNUMERIC), CAST(1 AS INT64));
BIGNUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS BIGNUMERIC), CAST(1 AS NUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS BIGNUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS BIGNUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS FLOAT64), CAST(1 AS INT64));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS FLOAT64), CAST(1 AS NUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS FLOAT64), CAST(1 AS BIGNUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_MULTIPLY(CAST(1 AS FLOAT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS INT64), CAST(1 AS INT64));
INT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS INT64), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS INT64), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS INT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS NUMERIC), CAST(1 AS INT64));
NUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS NUMERIC), CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS NUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS NUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS BIGNUMERIC), CAST(1 AS INT64));
BIGNUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS BIGNUMERIC), CAST(1 AS NUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS BIGNUMERIC), CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS BIGNUMERIC), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS FLOAT64), CAST(1 AS INT64));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS FLOAT64), CAST(1 AS NUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS FLOAT64), CAST(1 AS BIGNUMERIC));
FLOAT64;

# dialect: bigquery
SAFE_SUBTRACT(CAST(1 AS FLOAT64), CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_NEGATE(CAST(1 AS FLOAT64));
FLOAT64;

# dialect: bigquery
SAFE_NEGATE(CAST(1 AS NUMERIC));
NUMERIC;

# dialect: bigquery
SAFE_NEGATE(CAST(1 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
STRING_AGG(tbl.str_col);
STRING;

# dialect: bigquery
STRING_AGG(tbl.bin_col);
BINARY;

# dialect: bigquery
DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY);
DATETIME;

# dialect: bigquery
DATETIME_TRUNC(TIMESTAMP "2008-12-25 15:30:00", DAY);
TIMESTAMP;

# dialect: bigquery
GENERATE_UUID();
STRING;

# dialect: bigquery
STRUCT(tbl.str_col);
STRUCT<str_col STRING>;

--------------------------------------
-- Snowflake
--------------------------------------

# dialect: snowflake
AI_AGG('foo', 'bar');
VARCHAR;

# dialect: snowflake
AI_AGG(null, 'bar');
VARCHAR;

# dialect: snowflake
AI_SUMMARIZE_AGG('foo');
VARCHAR;

# dialect: snowflake
AI_SUMMARIZE_AGG(null);
VARCHAR;

# dialect: snowflake
AI_CLASSIFY('text', ['travel', 'cooking']);
VARCHAR;

# dialect: snowflake
AI_CLASSIFY('text', ['travel', 'cooking'], {'output_mode': 'multi'});
VARCHAR;

# dialect: snowflake
ASCII('A');
INT;

# dialect: snowflake
ASCII('');
INT;

# dialect: snowflake
ASCII(NULL);
INT;

# dialect: snowflake
BASE64_DECODE_BINARY('SGVsbG8=');
BINARY;

# dialect: snowflake
BASE64_DECODE_STRING('SGVsbG8gV29ybGQ=');
VARCHAR;

# dialect: snowflake
BASE64_DECODE_STRING('SGVsbG8gV29ybGQ=', 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/');
VARCHAR;

# dialect: snowflake
BASE64_ENCODE('Hello World', 76);
VARCHAR;

# dialect: snowflake
BASE64_ENCODE('Hello World', 76, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/');
VARCHAR;

# dialect: snowflake
BIT_LENGTH('abc');
INT;

# dialect: snowflake
BIT_LENGTH(tbl.str_col);
INT;

# dialect: snowflake
BIT_LENGTH(tbl.bin_col);
INT;

# dialect: snowflake
CHARINDEX('world', 'hello world');
INT;

# dialect: snowflake
CHARINDEX('world', 'hello world', 1);
INT;

# dialect: snowflake
CHAR(65);
VARCHAR;

# dialect: snowflake
CHR(8364);
VARCHAR;

# dialect: snowflake
COLLATE('hello', 'utf8');
VARCHAR;

# dialect: snowflake
COMPRESS('Hello World', 'SNAPPY');
BINARY;

# dialect: snowflake
COMPRESS('Hello World', 'zlib(1)');
BINARY;

# dialect: snowflake
DECOMPRESS_BINARY('compressed_data', 'SNAPPY');
BINARY;

# dialect: snowflake
DECOMPRESS_STRING('compressed_data', 'ZSTD');
VARCHAR;

# dialect: snowflake
LPAD('Hello', 10, '*');
VARCHAR;

# dialect: snowflake
LPAD(tbl.str_col, 10);
VARCHAR;

# dialect: snowflake
LPAD(tbl.bin_col, 10, 0x20);
BINARY;

# dialect: snowflake
COLLATION('hello');
VARCHAR;

# dialect: snowflake
CONCAT('Hello', 'World!');
VARCHAR;

# dialect: snowflake
CONCAT(tbl.str_col, tbl.str_col, tbl.str_col);
VARCHAR;

# dialect: snowflake
CONCAT_WS(':', 'one');
VARCHAR;

# dialect: snowflake
CONCAT_WS(',', 'one', 'two', 'three');
VARCHAR;

# dialect: snowflake
CONCAT_WS(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: snowflake
CONTAINS('hello world', 'world');
BOOLEAN;

# dialect: snowflake
CONTAINS(tbl.str_col, 'test');
BOOLEAN;

# dialect: snowflake
CONTAINS(tbl.bin_col, tbl.bin_col);
BOOLEAN;

# dialect: snowflake
CONTAINS(tbl.bin_col, NULL);
BOOLEAN;

# dialect: snowflake
EDITDISTANCE('hello', 'world');
INT;

# dialect: snowflake
EDITDISTANCE(tbl.str_col, 'test');
INT;

# dialect: snowflake
EDITDISTANCE('hello', 'world', 3);
INT;

# dialect: snowflake
ENDSWITH('hello world', 'world');
BOOLEAN;

# dialect: snowflake
ENDSWITH(tbl.str_col, 'test');
BOOLEAN;

# dialect: snowflake
ENDSWITH(tbl.bin_col, tbl.bin_col);
BOOLEAN;

# dialect: snowflake
ENDSWITH(tbl.bin_col, NULL);
BOOLEAN;

# dialect: snowflake
HEX_DECODE_BINARY('48656C6C6F');
BINARY;

# dialect: snowflake
HEX_DECODE_STRING('48656C6C6F');
VARCHAR;

# dialect: snowflake
HEX_ENCODE('Hello World');
VARCHAR;

# dialect: snowflake
HEX_ENCODE('Hello World', 'upper');
VARCHAR;

# dialect: snowflake
HEX_ENCODE('Hello World', 'lower');
VARCHAR;

# dialect: snowflake
INITCAP('hello world');
VARCHAR;

# dialect: snowflake
INITCAP('hello world', ' ');
VARCHAR;

# dialect: snowflake
INITCAP(tbl.str_col);
VARCHAR;

# dialect: snowflake
JAROWINKLER_SIMILARITY('hello', 'world');
INT;

# dialect: snowflake
INSERT('abc', 1, 2, 'Z');
VARCHAR;

# dialect: snowflake
INSERT(tbl.bin_col, 1, 2, tbl.bin_col);
BINARY;

# dialect: snowflake
LEAST(x::DECIMAL(18, 2));
DECIMAL(18, 2);

# dialect: snowflake
LEFT('hello world', 5);
VARCHAR;

# dialect: snowflake
LEFT(tbl.str_col, 3);
STRING;

# dialect: snowflake
LEFT(tbl.bin_col, 3);
BINARY;

# dialect: snowflake
LEFT(tbl.bin_col, NULL);
BINARY;

# dialect: snowflake
LEN(tbl.str_col);
INT;

# dialect: snowflake
LEN(tbl.bin_col);
INT;

# dialect: snowflake
LENGTH(tbl.str_col);
INT;

# dialect: snowflake
LENGTH(tbl.bin_col);
INT;

# dialect: snowflake
LOWER(tbl.str_col);
VARCHAR;

# dialect: snowflake
LTRIM('  hello world  ');
VARCHAR;

# dialect: snowflake
LTRIM(tbl.str_col);
VARCHAR;

# dialect: snowflake
LTRIM(NULL);
VARCHAR;

# dialect: snowflake
REGEXP_LIKE('foo', 'bar');
BOOLEAN;

# dialect: snowflake
REGEXP_LIKE(NULL, 'bar');
BOOLEAN;

# dialect: snowflake
REGEXP_LIKE('foo', 'bar', 'baz');
BOOLEAN;

# dialect: snowflake
REGEXP_LIKE('foo', NULL, 'baz');
BOOLEAN;

# dialect: snowflake
REGEXP_REPLACE('hello world', 'world', 'universe');
VARCHAR;

# dialect: snowflake
REGEXP_REPLACE('hello world', 'world', NULL);
VARCHAR;

# dialect: snowflake
REGEXP_REPLACE('hello world', 'world', 'universe', 1, 1, 'i');
VARCHAR;

# dialect: snowflake
REGEXP_SUBSTR('hello world', 'world');
VARCHAR;

# dialect: snowflake
REGEXP_SUBSTR(NULL, 'world');
VARCHAR;

# dialect: snowflake
REGEXP_SUBSTR('hello world', NULL);
VARCHAR;

# dialect: snowflake
REGEXP_SUBSTR('hello world', 'world', 1);
VARCHAR;

# dialect: snowflake
REGEXP_SUBSTR('hello world', 'world', 1, 1, 'e', NULL);
VARCHAR;

# dialect: snowflake
REPEAT('hello', 3);
VARCHAR;

# dialect: snowflake
REPEAT(tbl.str_col, 2);
VARCHAR;

# dialect: snowflake
REPEAT('hello', NULL);
VARCHAR;

# dialect: snowflake
REPLACE(tbl.str_col, 'old', 'new');
VARCHAR;

# dialect: snowflake
REPLACE('hello', 'old', NULL);
VARCHAR;

# dialect: snowflake
REVERSE('Hello, world!');
VARCHAR;

# dialect: snowflake
REVERSE(tbl.str_col);
VARCHAR;

# dialect: snowflake
REVERSE(tbl.bin_col);
BINARY;

# dialect: snowflake
REVERSE(NULL);
VARCHAR;

# dialect: snowflake
RIGHT('hello world', 5);
VARCHAR;

# dialect: snowflake
RIGHT(tbl.str_col, 3);
STRING;

# dialect: snowflake
RIGHT(tbl.bin_col, 3);
BINARY;

# dialect: snowflake
RIGHT(tbl.str_col, NULL);
STRING;

# dialect: snowflake
RLIKE('foo', 'bar');
BOOLEAN;

# dialect: snowflake
RLIKE(NULL, 'bar');
BOOLEAN;

# dialect: snowflake
RLIKE('foo', 'bar', NULL);
BOOLEAN;

# dialect: snowflake
RTRIM('  hello world  ');
VARCHAR;

# dialect: snowflake
RTRIM(tbl.str_col);
VARCHAR;

# dialect: snowflake
RTRIM(NULL);
VARCHAR;

# dialect: snowflake
SHA1('foo');
VARCHAR;

# dialect: snowflake
SHA1(null);
VARCHAR;

# dialect: snowflake
SHA1_BINARY('foo');
BINARY;

# dialect: snowflake
SHA1_BINARY(null);
BINARY;

# dialect: snowflake
SHA1_HEX('foo');
VARCHAR;

# dialect: snowflake
SHA1_HEX(null);
VARCHAR;

# dialect: snowflake
SHA2('foo');
VARCHAR;

# dialect: snowflake
SHA2(null);
VARCHAR;

# dialect: snowflake
SHA2('foo', 256);
VARCHAR;

# dialect: snowflake
SHA2('foo', null);
VARCHAR;

# dialect: snowflake
SHA2_BINARY('foo');
BINARY;

# dialect: snowflake
SHA2_BINARY(null);
BINARY;

# dialect: snowflake
SHA2_BINARY('foo', 256);
BINARY;

# dialect: snowflake
SHA2_BINARY('foo', null);
BINARY;

# dialect: snowflake
SHA2_HEX('foo');
VARCHAR;

# dialect: snowflake
SHA2_HEX(null);
VARCHAR;

# dialect: snowflake
SHA2_HEX('foo', 256);
VARCHAR;

# dialect: snowflake
SHA2_HEX('foo', null);
VARCHAR;

# dialect: snowflake
SPACE(5);
VARCHAR;

# dialect: snowflake
SPACE(tbl.int_col);
VARCHAR;

# dialect: snowflake
SPACE(NULL);
VARCHAR;

# dialect: snowflake
SPLIT('hello world', ' ');
ARRAY;

# dialect: snowflake
SPLIT(tbl.str_col, ',');
ARRAY;

# dialect: snowflake
SPLIT(NULL, ',');
ARRAY;

# dialect: snowflake
STARTSWITH('hello world', 'hello');
BOOLEAN;

# dialect: snowflake
STARTSWITH(tbl.str_col, 'test');
BOOLEAN;

# dialect: snowflake
STARTSWITH(tbl.bin_col, tbl.bin_col);
BOOLEAN;

# dialect: snowflake
STARTSWITH(tbl.bin_col, NULL);
BOOLEAN;

# dialect: snowflake
SUBSTR('hello world', 1, 5);
VARCHAR;

# dialect: snowflake
SUBSTR(tbl.str_col, 1, 3);
STRING;

# dialect: snowflake
SUBSTR(tbl.bin_col, 1, 3);
BINARY;

# dialect: snowflake
SUBSTR(tbl.str_col, NULL);
STRING;

# dialect: snowflake
TRIM('hello world');
VARCHAR;

# dialect: snowflake
TRIM('hello world', 'hello');
VARCHAR;

# dialect: snowflake
TRIM(tbl.str_col);
VARCHAR;

# dialect: snowflake
TRIM(tbl.str_col, tbl.str_col);
VARCHAR;

# dialect: snowflake
TRIM(NULL);
VARCHAR;

# dialect: snowflake
UPPER('Hello, world!');
VARCHAR;

# dialect: snowflake
UPPER(tbl.str_col);
VARCHAR;

# dialect: snowflake
UUID_STRING();
VARCHAR;

# dialect: snowflake
UUID_STRING('foo', 'bar');
VARCHAR;

# dialect: snowflake
UUID_STRING(null, null);
VARCHAR;

# dialect: snowflake
MD5(tbl.str_col);
VARCHAR;

# dialect: snowflake
MD5_HEX(tbl.str_col);
VARCHAR;

# dialect: snowflake
MD5_BINARY(tbl.str_col);
BINARY;

# dialect: snowflake
MD5_NUMBER_LOWER64(tbl.str_col);
BIGINT;

# dialect: snowflake
MD5_NUMBER_UPPER64(tbl.str_col);
BIGINT;

# dialect: snowflake
'Hello' NOT ILIKE 'h%';
BOOLEAN;

# dialect: snowflake
'Hello' ILIKE 'h_llo';
BOOLEAN;

# dialect: snowflake
tbl.str_col NOT ILIKE '%x%';
BOOLEAN;

# dialect: snowflake
'Hello' NOT LIKE 'H%';
BOOLEAN;

# dialect: snowflake
'Hello' LIKE 'H_llo';
BOOLEAN;

# dialect: snowflake
tbl.str_col NOT LIKE '%e%';
BOOLEAN;

# dialect: snowflake
tbl.str_col LIKE ALL ('H%', '%o');
BOOLEAN;

# dialect: snowflake
tbl.str_col LIKE ANY ('H%', '%o');
BOOLEAN;

# dialect: snowflake
tbl.str_col ILIKE ANY ('h%', '%x');
BOOLEAN;

# dialect: snowflake
LIKE(tbl.str_col, 'pattern');
BOOLEAN;

# dialect: snowflake
ILIKE(tbl.str_col, 'pattern');
BOOLEAN;

--------------------------------------
-- T-SQL
--------------------------------------

# dialect: tsql
SYSDATETIMEOFFSET();
TIMESTAMPTZ;
