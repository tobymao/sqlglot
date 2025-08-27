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
JSON_VALUE_ARRAY(JSON '["a","b"]');
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
FROM_HEX('foo');
BINARY;

--------------------------------------
-- Snowflake
--------------------------------------

# dialect: snowflake
LEAST(x::DECIMAL(18, 2));
DECIMAL(18, 2);

--------------------------------------
-- T-SQL
--------------------------------------

# dialect: tsql
SYSDATETIMEOFFSET();
TIMESTAMPTZ;
