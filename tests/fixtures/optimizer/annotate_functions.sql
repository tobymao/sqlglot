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

LOCALTIME();
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

ANY_VALUE(tbl.bool_col);
BOOLEAN;

ANY_VALUE(tbl.bigint_col);
BIGINT;

ANY_VALUE(tbl.date_col);
DATE;

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

# dialect: snowflake
NEXT_DAY(tbl.date_col, 'MONDAY');
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

KURTOSIS(tbl.double_col);
DOUBLE;

KURTOSIS(tbl.int_col);
DOUBLE;

LENGTH(tbl.str_col);
INT;

LENGTH(tbl.bin_col);
INT;

DAYNAME(tbl.date_col);
VARCHAR;

CBRT(tbl.int_col);
DOUBLE;

CBRT(tbl.double_col);
DOUBLE;

ISINF(tbl.float_col);
BOOLEAN;

ISNAN(tbl.float_col);
BOOLEAN;

CURRENT_CATALOG();
VARCHAR;

CURRENT_USER();
VARCHAR;

# dialect: snowflake
TO_BINARY('test');
BINARY;

# dialect: snowflake
TO_BINARY('test', 'HEX');
BINARY;

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

# dialect: spark, databricks
LOCALTIMESTAMP();
TIMESTAMPNTZ;

# dialect: hive, spark2, spark, databricks
ENCODE(tbl.str_col, tbl.str_col);
BINARY;

# dialect: hive, spark2, spark, databricks
ENCODE(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: spark, databricks
CURRENT_TIMEZONE();
STRING;

# dialect: hive, spark2, spark, databricks
UNIX_TIMESTAMP();
BIGINT;

# dialect: hive, spark2, spark, databricks
ACOS(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
ACOS(tbl.double_col);
DOUBLE;

# dialect: spark2, spark, databricks
ATAN2(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: spark2, spark, databricks
ATAN2(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: spark2, spark, databricks
ATAN2(tbl.double_col, tbl.int_col);
DOUBLE;

# dialect: spark, databricks
ACOSH(tbl.double_col);
DOUBLE;

# dialect: spark, databricks
ACOSH(tbl.int_col);
DOUBLE;

# dialect: spark2, spark, databricks
COT(tbl.int_col);
DOUBLE;

# dialect: spark2, spark, databricks
COT(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
COSH(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
COSH(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
SINH(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
SINH(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
TANH(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
TANH(tbl.int_col);
DOUBLE;

# dialect: spark, databricks
TO_BINARY(tbl.str_col, tbl.str_col);
BINARY;

# dialect: spark, databricks
TO_BINARY(tbl.int_col, tbl.str_col);
BINARY;

# dialect: spark, databricks
TO_BINARY(tbl.double_col, tbl.str_col);
BINARY;

# dialect: hive, spark2, spark, databricks
SHA(tbl.str_col);
VARCHAR;

# dialect: hive, spark2, spark, databricks
SHA1(tbl.str_col);
VARCHAR;

# dialect: hive, spark2, spark, databricks
SHA2(tbl.str_col, tbl.int_col);
VARCHAR;

# dialect: hive, spark2, spark, databricks
SPACE(tbl.int_col);
VARCHAR;

# dialect: spark2, spark, databricks
RANDN();
DOUBLE;

# dialect: spark2, spark, databricks
BIT_LENGTH(tbl.str_col);
INT;

# dialect: hive, spark2, spark, databricks
ASIN(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
ASIN(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
SIN(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
SIN(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
COS(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
COS(tbl.double_col);
DOUBLE;

# dialect: spark, databricks
ASINH(tbl.int_col);
DOUBLE;

# dialect: spark, databricks
ASINH(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
ATAN(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
ATAN(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
TAN(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
TAN(tbl.double_col);
DOUBLE;

# dialect: spark, databricks
ATANH(tbl.double_col);
DOUBLE;

# dialect: spark, databricks
ATANH(tbl.int_col);
DOUBLE;

# dialect: spark, databricks
SEC(tbl.int_col);
DOUBLE;

# dialect: spark, databricks
SEC(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
CORR(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
CORR(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
CBRT(tbl.double_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
CBRT(tbl.int_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
CURRENT_CATALOG();
STRING;

# dialect: hive, spark2, spark, databricks
CURRENT_DATABASE();
STRING;

# dialect: spark, databricks
DATE_FROM_UNIX_DATE(tbl.int_col);
DATE;

# dialect: hive, spark2, spark, databricks
MONTHS_BETWEEN(tbl.timestamp_col, tbl.timestamp_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
MONTHS_BETWEEN(tbl.timestamp_col, tbl.timestamp_col, tbl.bool_col);
DOUBLE;

# dialect: hive, spark2, spark, databricks
MONTH(tbl.date_col);
INT;

# dialect: spark, databricks
MONTHNAME(tbl.date_col);
STRING;

# dialect: hive, spark, databricks
CURRENT_SCHEMA();
STRING;

# dialect: hive, spark2, spark, databricks
CURRENT_USER();
STRING;

# dialect: hive, spark2, spark, databricks
UNHEX(tbl.str_col);
BINARY;

# dialect: hive, spark2, spark, databricks
HEX(tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
HEX(tbl.int_col);
STRING;

# dialect: hive, spark2, spark, databricks
SOUNDEX(tbl.str_col);
STRING;

# dialect: spark, databricks
SESSION_USER();
STRING;

# dialect: hive, spark2, spark, databricks
FACTORIAL(tbl.int_col);
BIGINT;

# dialect: spark, databricks
ARRAY_SIZE(tbl.array_col);
INT;

# dialect: hive, spark2, spark, databricks
QUARTER(tbl.date_col);
INT;

# dialect: hive, spark2, spark, databricks
SECOND(tbl.timestamp_col);
INT;

# dialect: hive, spark2, spark, databricks
MD5(tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
HOUR(tbl.timestamp_col);
INT;

# dialect: spark, databricks
BITMAP_COUNT(tbl.bin_col);
BIGINT;

# dialect: spark, databricks
RANDSTR(tbl.int_col);
STRING;

# dialect: spark, databricks
RANDSTR(tbl.int_col, tbl.int_col);
STRING;

# dialect: spark, databricks
COLLATION(tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
REPEAT(tbl.str_col, tbl.int_col);
STRING;

# dialect: spark2, spark, databricks
FORMAT_STRING(tbl.str_col, tbl.int_col, tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
REPLACE(tbl.str_col, tbl.str_col, tbl.str_col);
STRING;

# dialect: spark, databricks
OVERLAY(tbl.str_col PLACING tbl.str_col FROM tbl.int_col);
STRING;

# dialect: spark, databricks
OVERLAY(tbl.bin_col PLACING tbl.bin_col FROM tbl.int_col FOR tbl.int_col);
BINARY;

# dialect: hive, spark2, spark, databricks
REVERSE(tbl.str_col);
STRING;

# dialect: hive, spark2, spark, databricks
REVERSE(tbl.array_col);
ARRAY<STRING>;

# dialect: spark2, spark, databricks
RIGHT(tbl.str_col, tbl.int_col);
STRING;

# dialect: hive, spark2, spark, databricks
TRANSLATE(tbl.str_col, tbl.str_col, tbl.str_col);
STRING; 

# dialect: hive, spark2, spark, databricks
SPLIT(tbl.str_col, tbl.str_col, tbl.int_col);
ARRAY<STRING>;

# dialect: hive, spark2, spark, databricks
SPLIT(tbl.str_col, tbl.str_col);
ARRAY<STRING>;

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
AVG(1);
FLOAT64;

# dialect: bigquery
AVG(5.5);
FLOAT64;

# dialect: bigquery
AVG(tbl.bignum_col);
BIGNUMERIC;

# dialect: bigquery
SAFE_DIVIDE(tbl.int_col, tbl.int_col);
FLOAT64;

# dialect: bigquery
SAFE_DIVIDE(tbl.int_col, tbl.bignum_col);
BIGNUMERIC;

# dialect: bigquery
SAFE_DIVIDE(tbl.int_col, tbl.double_col);
FLOAT64;

# dialect: bigquery
SAFE_DIVIDE(tbl.bignum_col, tbl.int_col);
BIGNUMERIC;

# dialect: bigquery
SAFE_DIVIDE(tbl.bignum_col, tbl.bignum_col);
BIGNUMERIC;

# dialect: bigquery
SAFE_DIVIDE(tbl.bignum_col, tbl.double_col);
FLOAT64;

# dialect: bigquery
SAFE_DIVIDE(tbl.double_col, tbl.int_col);
FLOAT64;

# dialect: bigquery
SAFE_DIVIDE(tbl.double_col, tbl.bignum_col);
FLOAT64;

# dialect: bigquery
SAFE_DIVIDE(tbl.double_col, tbl.double_col);
FLOAT64;

# dialect: bigquery
SAFE.TIMESTAMP(tbl.str_col);
TIMESTAMPTZ;

# dialect: bigquery
TIMESTAMP(tbl.str_col);
TIMESTAMPTZ;

# dialect: bigquery
SAFE.PARSE_DATE('%Y-%m-%d', '2024-01-15');
DATE;

# dialect: bigquery
PARSE_DATE('%Y-%m-%d', '2024-01-15');
DATE;

# dialect: bigquery
SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00');
DATETIME;

# dialect: bigquery
SAFE.PARSE_TIME('%H:%M:%S', '10:30:00');
TIME;

# dialect: bigquery
SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00');
TIMESTAMPTZ;

# dialect: bigquery
PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00');
TIMESTAMPTZ;

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
TO_HEX(MD5('foo'));
STRING;

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
NET.HOST('http://example.com');
STRING;

# dialect: bigquery
NET.REG_DOMAIN('http://example.com');
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
STRING_AGG(DISTINCT tbl.str_col);
STRING;

# dialect: bigquery
STRING_AGG(tbl.str_col ORDER BY tbl.str_col);
STRING;

# dialect: bigquery
STRING_AGG(DISTINCT tbl.str_col, ',' ORDER BY tbl.str_col);
STRING;

# dialect: bigquery
STRING_AGG(DISTINCT tbl.bin_col ORDER BY tbl.bin_col);
BINARY;

# dialect: bigquery
STRING_AGG(tbl.str_col, ',' LIMIT 10);
STRING;

# dialect: bigquery
STRING_AGG(tbl.str_col, ',' ORDER BY tbl.str_col LIMIT 10);
STRING;

# dialect: bigquery
STRING_AGG(DISTINCT tbl.str_col, ',' ORDER BY tbl.str_col LIMIT 10);
STRING;

# dialect: bigquery
STRING_AGG(DISTINCT tbl.bin_col ORDER BY tbl.bin_col LIMIT 10);
BINARY;

# dialect: bigquery
ARRAY_AGG(tbl.int_col LIMIT 10);
ARRAY<INT>;

# dialect: bigquery
ARRAY_AGG(DISTINCT tbl.str_col ORDER BY tbl.str_col LIMIT 10);
ARRAY<STRING>;

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

# dialect: bigquery
LENGTH(tbl.str_col);
BIGINT;

# dialect: bigquery
LENGTH(tbl.bin_col);
BIGINT;

# dialect: bigquery
IF(TRUE, '2010-01-01', DATE '2020-02-02');
DATE;

# dialect: bigquery
IF(TRUE, DATETIME '2010-01-01 00:00:00', '2020-02-02 00:00:00');
DATETIME;

# dialect: bigquery
IF(TRUE, '00:00:00', TIME '00:01:00');
TIME;

# dialect: bigquery
IF(TRUE, 1, CAST(2.5 AS BIGNUMERIC));
BIGNUMERIC;

# dialect: bigquery
IF(TRUE, 1.5, 2.5);
FLOAT64;

# dialect: bigquery
IF(TRUE, '2010-01-01 00:00:00', TIMESTAMP '2020-02-02 00:00:00');
TIMESTAMP;

# dialect: bigquery
COALESCE('2010-01-01', DATE '2020-02-02');
DATE;

# dialect: bigquery
COALESCE(DATETIME '2010-01-01 00:00:00', '2020-02-02 00:00:00');
DATETIME;

# dialect: bigquery
IFNULL('00:00:00', TIME '00:01:00');
TIME;

# dialect: bigquery
IFNULL(TIMESTAMP '2010-01-01 00:00:00', '2020-02-02 00:00:00');
TIMESTAMP;

# dialect: bigquery
ANY_VALUE(c2::STRING HAVING MIN c1::INT64);
STRING;

# dialect: bigquery
ANY_VALUE(c2::STRING HAVING MAX c1::INT64);
STRING;

# dialect: bigquery
r'a';
STRING;

# dialect: bigquery
DATE_ADD(DATE '2008-12-25', INTERVAL 5 DAY);
DATE;

# dialect: bigquery
DATE_ADD(DATE '2008-12-25', INTERVAL 2 WEEK);
DATE;

# dialect: bigquery
DATE_ADD(DATE '2008-12-25', INTERVAL 3 MONTH);
DATE;

# dialect: bigquery
DATE_ADD(DATE '2008-12-25', INTERVAL 1 QUARTER);
DATE;

# dialect: bigquery
DATE_ADD(DATE '2008-12-25', INTERVAL 2 YEAR);
DATE;

# dialect: bigquery
DATE_ADD(TIMESTAMP '2008-12-25 15:30:00', INTERVAL 5 DAY);
TIMESTAMP;

# dialect: bigquery
DATE_ADD(TIMESTAMP '2008-12-25 15:30:00', INTERVAL 2 HOUR);
TIMESTAMP;

# dialect: bigquery
DATE_ADD(TIMESTAMP '2008-12-25 15:30:00', INTERVAL 30 MINUTE);
TIMESTAMP;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 5 DAY);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 2 WEEK);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 3 MONTH);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 1 QUARTER);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 2 YEAR);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 2 HOUR);
DATETIME;

# dialect: bigquery
DATE_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 30 MINUTE);
DATETIME;

--------------------------------------
-- Snowflake
--------------------------------------

# dialect: snowflake
ABS(tbl.bigint_col);
BIGINT;

# dialect: snowflake
ABS(tbl.double_col);
DOUBLE;

# dialect: snowflake
ADD_MONTHS(tbl.date_col, 2);
DATE;

# dialect: snowflake
ADD_MONTHS(tbl.timestamp_col, -1);
TIMESTAMP;

# dialect: snowflake
ARRAY_CONSTRUCT();
ARRAY;

# dialect: snowflake
ARRAY_CONSTRUCT_COMPACT();
ARRAY;

# dialect: snowflake
ARRAY_CONSTRUCT_COMPACT(1, null, 2);
ARRAY;

# dialect: snowflake
ARRAY_COMPACT([1, null, 2]);
ARRAY;

# dialect: snowflake
ARRAY_APPEND([1, 2, 3], 4);
ARRAY;

# dialect: snowflake
ARRAY_CAT([1, 2], [3, 4]);
ARRAY;

# dialect: snowflake
ARRAY_PREPEND([2, 3, 4], 1);
ARRAY;

# dialect: snowflake
ARRAY_REMOVE([1, 2, 3], 2);
ARRAY;

# dialect: snowflake
ARRAYS_ZIP([1, 2], [3, 4]);
ARRAY;

# dialect: snowflake
ASIN(tbl.double_col);
DOUBLE;

# dialect: snowflake
ASINH(tbl.double_col);
DOUBLE;

# dialect: snowflake
ATAN(tbl.double_col);
DOUBLE;

# dialect: snowflake
ATAN2(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
ATANH(tbl.double_col);
DOUBLE;

# dialect: snowflake
CBRT(tbl.double_col);
DOUBLE;

# dialect: snowflake
CBRT(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
CBRT(tbl.int_col);
DOUBLE;

# dialect: snowflake
COVAR_POP(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
COVAR_SAMP(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
COVAR_POP(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
COVAR_SAMP(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

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
BASE64_DECODE_STRING('SGVsbG8gV29ybGQ=', '+/=');
VARCHAR;

# dialect: snowflake
BASE64_ENCODE(tbl.bin_col);
VARCHAR;

# dialect: snowflake
BASE64_ENCODE('Hello World');
VARCHAR;

# dialect: snowflake
BASE64_ENCODE('Hello World', 76);
VARCHAR;

# dialect: snowflake
BASE64_ENCODE('Hello World', 76, '+/=');
VARCHAR;

# dialect: snowflake
BIT_LENGTH('abc');
INT;

# dialect: snowflake
BITMAP_BIT_POSITION(tbl.int_col);
BIGINT;

# dialect: snowflake
BITMAP_BUCKET_NUMBER(tbl.int_col);
BIGINT;

# dialect: snowflake
BITMAP_CONSTRUCT_AGG(tbl.int_col);
BINARY;

# dialect: snowflake
BITMAP_COUNT(BITMAP_CONSTRUCT_AGG(tbl.int_col));
BIGINT;

# dialect: snowflake
BIT_LENGTH(tbl.str_col);
INT;

# dialect: snowflake
BIT_LENGTH(tbl.bin_col);
INT;

# dialect: snowflake
BITNOT(5);
INT;

# dialect: snowflake
BITNOT(tbl.bin_col);
BINARY;

# dialect: snowflake
BIT_NOT(5);
INT;

# dialect: snowflake
BITAND(2, 4);
INT;

# dialect: snowflake
BITAND(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: snowflake
BIT_AND(2, 4);
INT;

# dialect: snowflake
BITOR(2, 4);
INT;

# dialect: snowflake
BITOR(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: snowflake
BITSHIFTLEFT(2, 1);
INT;

# dialect: snowflake
BITSHIFTLEFT(tbl.bin_col, 4);
BINARY;

# dialect: snowflake
BITSHIFTRIGHT(24, 1);
INT;

# dialect: snowflake
BITSHIFTRIGHT(tbl.bin_col, 4);
BINARY;

# dialect: snowflake
BITXOR(5, 3);
INT;

# dialect: snowflake
BITXOR(tbl.bin_col, tbl.bin_col);
BINARY;

# dialect: snowflake
BITANDAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITAND_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_AND_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_ANDAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITORAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITOR_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_OR_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_ORAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITXORAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITXOR_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_XOR_AGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BIT_XORAGG(tbl.int_col);
NUMBER(38, 0);

# dialect: snowflake
BITMAP_OR_AGG(tbl.bin_col);
BINARY;

# dialect: snowflake
BOOLXOR_AGG(tbl.bool_col);
BOOLEAN;

# dialect: snowflake
BOOLNOT(tbl.int_col);
BOOLEAN;

# dialect: snowflake
BOOLNOT(NULL);
BOOLEAN;

# dialect: snowflake
BOOLAND(1, -2);
BOOLEAN;

# dialect: snowflake
BOOLOR(1, 0);
BOOLEAN;

# dialect: snowflake
BOOLXOR(2, 0);
BOOLEAN;

# dialect: snowflake
BOOLAND_AGG(tbl.bool_col);
BOOLEAN;

# dialect: snowflake
BOOLOR_AGG(tbl.bool_col);
BOOLEAN;

# dialect: snowflake
TO_BOOLEAN('true');
BOOLEAN;

# dialect: snowflake
TO_BOOLEAN(1);
BOOLEAN;

# dialect: snowflake
TO_BOOLEAN(tbl.varchar_col);
BOOLEAN;

# dialect: snowflake
ARRAY_AGG(tbl.bin_col);
ARRAY;

# dialect: snowflake
ARRAY_AGG(tbl.bool_col);
ARRAY;

# dialect: snowflake
ARRAY_AGG(tbl.date_col);
ARRAY;

# dialect: snowflake
ARRAY_AGG(tbl.double_col);
ARRAY;

# dialect: snowflake
ARRAY_AGG(tbl.str_col);
ARRAY;

# dialect: snowflake
ARRAY_UNIQUE_AGG(tbl.bin_col);
ARRAY;

# dialect: snowflake
ARRAY_UNIQUE_AGG(tbl.bool_col);
ARRAY;

# dialect: snowflake
ARRAY_UNIQUE_AGG(tbl.date_col);
ARRAY;

# dialect: snowflake
ARRAY_UNIQUE_AGG(tbl.double_col);
ARRAY;

# dialect: snowflake
ARRAY_UNIQUE_AGG(tbl.str_col);
ARRAY;

# dialect: snowflake
ARRAY_UNION_AGG(tbl.array_col);
ARRAY;

# dialect: snowflake
CHARINDEX('world', 'hello world');
INT;

# dialect: snowflake
CHARINDEX('world', 'hello world', 1);
INT;

# dialect: snowflake
CASE WHEN score >= 90 THEN 100 WHEN score >= 80 THEN 220 END;
INT;

# dialect: snowflake
CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END;
VARCHAR;

# dialect: snowflake
CASE WHEN score >= 90 THEN TRUE WHEN score >= 80 THEN FALSE ELSE NULL END;
BOOLEAN;

# dialect: snowflake
CEIL(3.14);
DOUBLE;

# dialect: snowflake
CEIL(3.14::FLOAT, 1);
FLOAT;

# dialect: snowflake
CEIL(3.14, 1);
DOUBLE;

# dialect: snowflake
CEIL(10::NUMERIC);
NUMBER;

# dialect: snowflake
CHAR(65);
VARCHAR;

# dialect: snowflake
CHR(8364);
VARCHAR;

# dialect: snowflake
CHECK_JSON('{"key": "value", "array": [1, 2, 3]}');
VARCHAR;

# dialect: snowflake
CHECK_XML('<root><key attribute="attr">value</key></root>');
VARCHAR;

# dialect: snowflake
CHECK_XML('<root><key attribute="attr">value</key></root>', TRUE);
VARCHAR;

# dialect: snowflake
COLLATE('hello', 'utf8');
VARCHAR;

# dialect: snowflake
COSH(1.5);
DOUBLE;

# dialect: snowflake
COALESCE(42, 0, 100);
INT;

# dialect: snowflake
COALESCE(1.5, 2.7);
DOUBLE;

# dialect: snowflake
COALESCE(1::BIGINT, 2::BIGINT);
BIGINT;

# dialect: snowflake
COALESCE('hello', 'world');
VARCHAR;

# dialect: snowflake
COALESCE(CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE));
DATE;

# dialect: snowflake
CAST(1.5 AS DECFLOAT);
DECFLOAT;

# dialect: snowflake
CAST(1 AS VARCHAR);
VARCHAR;

# dialect: snowflake
CAST('123' AS INT);
INT;

# dialect: snowflake
COALESCE(TRUE, FALSE);
BOOLEAN;

# dialect: snowflake
COUNT(*);
BIGINT;

# dialect: snowflake
COUNT(DISTINCT tbl.str_col);
BIGINT;

# dialect: snowflake
COMPRESS('Hello World', 'SNAPPY');
BINARY;

# dialect: snowflake
COMPRESS('Hello World', 'zlib(1)');
BINARY;

# dialect: snowflake
DATE_PART('year', tbl.date_col);
INT;

# dialect: snowflake
DATE_PART('month', tbl.timestamp_col);
INT;

# dialect: snowflake
DATE_PART('day', tbl.date_col);
INT;

# dialect: snowflake
DATEADD(HOUR, 3, TO_TIME('05:00:00'));
TIME;

# dialect: snowflake
DATEADD(YEAR, 1, TO_TIMESTAMP('2022-05-08 14:30:00'));
TIMESTAMP;

# dialect: snowflake
DATEADD(MONTH, 1, '2023-01-31'::DATE);
DATE;

# dialect: snowflake
DATEADD(HOUR, 2, '2022-04-05'::DATE);
TIMESTAMPNTZ;

# dialect: snowflake
DEGREES(PI()/3);
DOUBLE;

# dialect: snowflake
DEGREES(1);
DOUBLE;

# dialect: snowflake
DATE_FROM_PARTS(1977, 8, 7);
DATE;

# dialect: snowflake
DECOMPRESS_BINARY('compressed_data', 'SNAPPY');
BINARY;

# dialect: snowflake
DECOMPRESS_STRING('compressed_data', 'ZSTD');
VARCHAR;

# dialect: snowflake
DIV0(10, 0);
DOUBLE;

# dialect: snowflake
DIV0(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
DIV0NULL(10, 0);
DOUBLE;

# dialect: snowflake
DIV0NULL(tbl.double_col, tbl.double_col);
DOUBLE;

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
RPAD('Hello', 10, '*');
VARCHAR;

# dialect: snowflake
RPAD(tbl.str_col, 10);
VARCHAR;

# dialect: snowflake
RPAD(tbl.bin_col, 10, 0x20);
BINARY;

# dialect: snowflake
COLLATION('hello');
VARCHAR;

# dialect: snowflake
COT(tbl.double_col);
DOUBLE;

# dialect: snowflake
COS(tbl.double_col);
DOUBLE;

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
CONVERT_TIMEZONE('America/New_York', '2024-08-06 09:10:00.000');
TIMESTAMPTZ;

# dialect: snowflake
CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-08-06 09:10:00.000');
TIMESTAMPNTZ;

# dialect: snowflake
CURRENT_ACCOUNT();
VARCHAR;

# dialect: snowflake
CURRENT_ACCOUNT_NAME();
VARCHAR;

# dialect: snowflake
CURRENT_AVAILABLE_ROLES();
VARCHAR;

# dialect: snowflake
CURRENT_CLIENT();
VARCHAR;

# dialect: snowflake
CURRENT_IP_ADDRESS();
VARCHAR;

# dialect: snowflake
CURRENT_DATABASE();
VARCHAR;

# dialect: snowflake
CURRENT_SCHEMAS();
VARCHAR;

# dialect: snowflake
CURRENT_SECONDARY_ROLES();
VARCHAR;

# dialect: snowflake
CURRENT_SESSION();
VARCHAR;

# dialect: snowflake
CURRENT_STATEMENT();
VARCHAR;

# dialect: snowflake
CURRENT_VERSION();
VARCHAR;

# dialect: snowflake
CURRENT_TRANSACTION();
VARCHAR;

# dialect: snowflake
CURRENT_WAREHOUSE();
VARCHAR;

# dialect: snowflake
CURRENT_ORGANIZATION_USER();
VARCHAR;

# dialect: snowflake
CURRENT_REGION();
VARCHAR;

# dialect: snowflake
CURRENT_ROLE();
VARCHAR;

# dialect: snowflake
CURRENT_ROLE_TYPE();
VARCHAR;

# dialect: snowflake
CURRENT_ORGANIZATION_NAME();
VARCHAR;

# dialect: snowflake
DATEDIFF('year', tbl.date_col, tbl.date_col);
INT;

# dialect: snowflake
DATEDIFF('month', tbl.timestamp_col, tbl.timestamp_col);
INT;

# dialect: snowflake
TIMESTAMPDIFF('year', tbl.date_col, tbl.date_col);
INT;

# dialect: snowflake
TIMESTAMPDIFF('month', tbl.timestamp_col, tbl.timestamp_col);
INT;

# dialect: snowflake
TIMEDIFF('year', tbl.date_col, tbl.date_col);
INT;

# dialect: snowflake
TIMEDIFF('month', tbl.timestamp_col, tbl.timestamp_col);
INT;

# dialect: snowflake
DATE_TRUNC('year', TO_DATE('2024-05-09'));
DATE;

# dialect: snowflake
DATE_TRUNC('minute', TO_TIME('08:50:48'));
TIME;

# dialect: snowflake
DATE_TRUNC('minute', TO_TIMESTAMP('2024-05-09 08:50:57.891'));
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45);
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123);
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_FROM_PARTS(CAST('2024-05-09' AS DATE), CAST('14:30:45' AS TIME));
TIMESTAMP;

# dialect: snowflake
TIMESTAMPFROMPARTS(2024, 5, 9, 14, 30, 45);
TIMESTAMP;

# dialect: snowflake
TIMESTAMPFROMPARTS(CAST('2024-05-09' AS DATE), CAST('14:30:45' AS TIME));
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_LTZ_FROM_PARTS(2024, 5, 9, 14, 30, 45);
TIMESTAMPLTZ;

# dialect: snowflake
TIMESTAMP_LTZ_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123);
TIMESTAMPLTZ;

# dialect: snowflake
TIMESTAMP_NTZ_FROM_PARTS(2024, 5, 9, 14, 30, 45);
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_NTZ_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123);
TIMESTAMP;

# dialect: snowflake
TIMESTAMP_TZ_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123, 'UTC');
TIMESTAMPTZ;

# dialect: snowflake
TIMESTAMP_TZ_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123);
TIMESTAMPTZ;

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
EQUAL_NULL(1, 2);
BOOLEAN;

# dialect: snowflake
EXTRACT(YEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(QUARTER FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(MONTH FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(WEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(WEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(DAY FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(DAYOFMONTH FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(DAYOFWEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(DAYOFWEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(DAYOFYEAR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(YEAROFWEEK FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(YEAROFWEEKISO FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(HOUR FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(MINUTE FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(SECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
INT;

# dialect: snowflake
EXTRACT(NANOSECOND FROM CAST('2026-01-06 11:45:00.123456789' AS TIMESTAMP_NTZ));
BIGINT;

# dialect: snowflake
EXTRACT(EPOCH_SECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
BIGINT;

# dialect: snowflake
EXTRACT(EPOCH_MILLISECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
BIGINT;

# dialect: snowflake
EXTRACT(EPOCH_MICROSECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
BIGINT;

# dialect: snowflake
EXTRACT(EPOCH_NANOSECOND FROM CAST('2026-01-06 11:45:00' AS TIMESTAMP_NTZ));
BIGINT;

# dialect: snowflake
EXTRACT(YEAR FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(QUARTER FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(MONTH FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(WEEK FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(WEEKISO FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(DAY FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(DAYOFMONTH FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(DAYOFWEEK FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(DAYOFWEEKISO FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(DAYOFYEAR FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(YEAROFWEEK FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(YEAROFWEEKISO FROM CAST('2026-01-06' AS DATE));
INT;

# dialect: snowflake
EXTRACT(HOUR FROM CAST('11:45:00.123456789' AS TIME));
INT;

# dialect: snowflake
EXTRACT(MINUTE FROM CAST('11:45:00.123456789' AS TIME));
INT;

# dialect: snowflake
EXTRACT(SECOND FROM CAST('11:45:00.123456789' AS TIME));
INT;

# dialect: snowflake
YEAR(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
YEAR(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
YEAROFWEEK(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
YEAROFWEEK(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
YEAROFWEEKISO(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
YEAROFWEEKISO(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
DAY(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
DAY(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
DAYOFMONTH(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
DAYOFMONTH(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
DAYOFWEEK(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
DAYOFWEEK(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
DAYOFWEEKISO(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
DAYOFWEEKISO(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
DAYOFYEAR(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
DAYOFYEAR(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
WEEK(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
WEEK(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
WEEKOFYEAR(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
WEEKOFYEAR(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
WEEKISO(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
WEEKISO(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
MONTH(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
MONTH(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
QUARTER(CAST('2024-05-09' AS DATE));
TINYINT;

# dialect: snowflake
QUARTER(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
TINYINT;

# dialect: snowflake
EXP(1);
DOUBLE;

# dialect: snowflake
EXP(5.5);
DOUBLE;

# dialect: snowflake
FACTORIAL(5);
BIGINT;

# dialect: snowflake
FLOOR(42);
INT;

# dialect: snowflake
FLOOR(135.135, 1);
DOUBLE;

# dialect: snowflake
FLOOR(tbl.bigint_col, -1);
BIGINT;

# dialect: snowflake
GETBIT(11, 3);
INT;

# dialect: snowflake
GROUPING(tbl.str_col);
INT;

# dialect: snowflake
GROUPING(tbl.bigint_col);
INT;

# dialect: snowflake
GROUPING_ID(tbl.str_col);
BIGINT;

# dialect: snowflake
GROUPING_ID(tbl.bigint_col, tbl.str_col);
BIGINT;

# dialect: snowflake
GREATEST(tbl.bigint_col, tbl.bigint_col);
BIGINT;

# dialect: snowflake
GREATEST(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
GREATEST(tbl.str_col, tbl.str_col);
VARCHAR;

# dialect: snowflake
GREATEST(tbl.double_col, tbl.bigint_col);
DOUBLE;

# dialect: snowflake
GREATEST(tbl.bigint_col, tbl.double_col);
DOUBLE;

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
GREATEST_IGNORE_NULLS(1, 2, 3);
INT;

# dialect: snowflake
GREATEST_IGNORE_NULLS(1, 2.5, 3);
DOUBLE;

# dialect: snowflake
GREATEST_IGNORE_NULLS('a', 'b', 'c');
VARCHAR;

# dialect: snowflake
GREATEST_IGNORE_NULLS(CAST('2023-01-01' AS DATE), CAST('2023-01-02' AS DATE));
DATE;

# dialect: snowflake
HASH_AGG(tbl.str_col);
DECIMAL(19, 0);

# dialect: snowflake
LEAST_IGNORE_NULLS(1, 2, 3);
INT;

# dialect: snowflake
LEAST_IGNORE_NULLS(1, 2.5, 3);
DOUBLE;

# dialect: snowflake
LEAST_IGNORE_NULLS('a', 'b', 'c');
VARCHAR;

# dialect: snowflake
LEAST_IGNORE_NULLS(CAST('2023-01-01' AS DATE), CAST('2023-01-02' AS DATE));
DATE;

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
HOUR(CAST('08:50:57' AS TIME));
INT;

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
IFF(TRUE, 42, 0);
INT;

# dialect: snowflake
IFF(TRUE, 42, NULL);
INT;

# dialect: snowflake
IFF(col1 > 0, 'yes', 'no');
VARCHAR;

# dialect: snowflake
IFF(FALSE, 1.5, 2.7);
DOUBLE;

# dialect: snowflake
IFF(TRUE, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE));
DATE;

# dialect: snowflake
IFNULL('hello', 'world');
VARCHAR;

# dialect: snowflake
IFNULL(1, 2);
INT;

# dialect: snowflake
IFNULL(1.5, 2.7);
DOUBLE;

# dialect: snowflake
IFNULL(5::BIGINT, 10::BIGINT);
BIGINT;

# dialect: snowflake
IFNULL(CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE));
DATE;

# dialect: snowflake
IFNULL(5::BIGINT, 2.71::FLOAT);
FLOAT;

# dialect: snowflake
IS_NULL_VALUE(payload:field);
BOOLEAN;

# dialect: snowflake
1 IN (1, 2, 3);
BOOLEAN;

# dialect: snowflake
1 NOT IN (1, 2, 3);
BOOLEAN;

# dialect: snowflake
JAROWINKLER_SIMILARITY('hello', 'world');
INT;

# dialect: duckdb
JARO_WINKLER_SIMILARITY('hello', 'world');
DOUBLE;

# dialect: snowflake
INSERT('abc', 1, 2, 'Z');
VARCHAR;

# dialect: snowflake
INSERT(tbl.bin_col, 1, 2, tbl.bin_col);
BINARY;

# dialect: snowflake
KURTOSIS(tbl.double_col);
DOUBLE;

# dialect: snowflake
KURTOSIS(tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
KURTOSIS(tbl.float_col);
DOUBLE;

# dialect: snowflake
KURTOSIS(tbl.float_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
KURTOSIS(tbl.int_col);
NUMBER(38, 12);

# dialect: snowflake
KURTOSIS(tbl.int_col) OVER (PARTITION BY 1);
NUMBER(38, 12);

# dialect: snowflake
KURTOSIS(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
KURTOSIS(tbl.decfloat_col) OVER (PARTITION BY 1);
DECFLOAT;

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
LAST_DAY(CAST('2024-05-09' AS DATE));
DATE;

# dialect: snowflake
LAST_DAY(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
DATE;

# dialect: snowflake
LAST_DAY(CAST('2024-02-15' AS DATE), MONTH);
DATE;

# dialect: snowflake
LEN(tbl.str_col);
INT;

# dialect: snowflake
LEN(tbl.bin_col);
INT;

# dialect: snowflake
LOCALTIMESTAMP;
TIMESTAMPLTZ;

# dialect: snowflake
LOCALTIMESTAMP();
TIMESTAMPLTZ;

# dialect: snowflake
LOCALTIMESTAMP(3);
TIMESTAMPLTZ;

# dialect: snowflake
OCTET_LENGTH(tbl.str_col);
INT;

# dialect: snowflake
OCTET_LENGTH(tbl.bin_col);
INT;

# dialect: snowflake
PARSE_URL('https://example.com/path');
OBJECT;

# dialect: snowflake
PARSE_URL(tbl.str_col, 0);
OBJECT;

# dialect: snowflake
POSITION('abc' IN 'abcdef');
INT;

# dialect: snowflake
POSITION('abc', 'abcdef');
INT;

# dialect: snowflake
POSITION('abc', 'abcdef', 1);
INT;

# dialect: snowflake
PREVIOUS_DAY(CAST('2024-05-09' AS DATE), 'MONDAY');
DATE;

# dialect: snowflake
PREVIOUS_DAY(CAST('2024-05-09 08:50:57' AS TIMESTAMP), 'MONDAY');
DATE;

# dialect: snowflake
DECODE(x, 1, 100, 2, 200, 0);
INT;

# dialect: snowflake
DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Neither');
VARCHAR;

# dialect: snowflake
DECODE(100, 100, 1, 90, 2, 5.5);
DOUBLE;

# dialect: snowflake
DECODE(x, 1, 100, NULL);
INT;

# dialect: snowflake
PI();
DOUBLE;

# dialect: snowflake
POW(tbl.double_col, 2);
DOUBLE;

# dialect: snowflake
RANDOM();
BIGINT;

# dialect: snowflake
RANDOM(123);
BIGINT;

# dialect: snowflake
RANDSTR(123, 456);
VARCHAR;

# dialect: snowflake
RANDSTR(123, RANDOM());
VARCHAR;

# dialect: snowflake
RADIANS(tbl.double_col);
DOUBLE;

# dialect: snowflake
LOWER(tbl.str_col);
VARCHAR;

# dialect: snowflake
LN(tbl.double_col);
DOUBLE;

# dialect: snowflake
LOG(tbl.double_col);
DOUBLE;

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
MAP_CAT(CAST(col AS MAP(VARCHAR, VARCHAR)), CAST(col AS MAP(VARCHAR, VARCHAR)));
MAP;

# dialect: snowflake
MAP_CONTAINS_KEY('k1', CAST(col AS MAP(VARCHAR, VARCHAR)));
BOOLEAN;

# dialect: snowflake
MAP_DELETE(CAST(col AS MAP(VARCHAR, VARCHAR)), 'b');
MAP;

# dialect: snowflake
MAP_INSERT(CAST(col AS MAP(VARCHAR, VARCHAR)), 'b', '2');
MAP;

# dialect: snowflake
MAP_KEYS(CAST(col AS MAP(VARCHAR, VARCHAR)));
ARRAY;

# dialect: snowflake
MAP_PICK(CAST(col AS MAP(VARCHAR, VARCHAR)), 'a', 'c');
MAP;

# dialect: snowflake
MAP_SIZE(CAST(col AS MAP(VARCHAR, VARCHAR)));
INT;

# dialect: snowflake
MINUTE(CAST('08:50:57' AS TIME));
INT;

# dialect: snowflake
MEDIAN(2.71::FLOAT);
FLOAT;

# dialect: snowflake
MEDIAN(tbl.bigint_col) OVER (PARTITION BY 1);
DECIMAL(38, 3);

# dialect: snowflake
MEDIAN(CAST(100 AS DECIMAL(10,2)));
DECIMAL(13, 5);

# dialect: snowflake
MONTHNAME(CAST('2024-05-09' AS DATE));
VARCHAR;

# dialect: snowflake
MONTHNAME(CAST('2024-05-09 08:50:57' AS TIMESTAMP));
VARCHAR;

# dialect: snowflake
NORMAL(0, 1, RANDOM());
DOUBLE;

# dialect: snowflake
NVL2(col1, col2, col3);
UNKNOWN;

# dialect: snowflake
NVL('hello', 'world');
VARCHAR;

# dialect: snowflake
NVL(tbl.int_col, 42);
INT;

# dialect: snowflake
NVL(tbl.date_col, CAST('2024-01-01' AS DATE));
DATE;

# dialect: snowflake
NVL(1, 3.14);
DOUBLE;

# dialect: snowflake
NVL(5::BIGINT, 2.71::FLOAT);
FLOAT;

# dialect: snowflake
NULLIF(1, 2);
INT;

# dialect: snowflake
NULLIF(1.5, 2.7);
DOUBLE;

# dialect: snowflake
NULLIF(5::BIGINT, 10::BIGINT);
BIGINT;

# dialect: snowflake
NULLIF(CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE));
DATE;

# dialect: snowflake
NULLIF(1::INT, 2::BIGINT);
BIGINT;

# dialect: snowflake
NULLIF(1::INT, 2.5::DOUBLE);
DOUBLE;

# dialect: snowflake
NULLIFZERO(5);
INT;

# dialect: snowflake
NULLIFZERO(5::BIGINT);
BIGINT;

# dialect: snowflake
NULLIFZERO(5.5);
DOUBLE;

# dialect: snowflake
NULLIFZERO(5.5::FLOAT);
FLOAT;

# dialect: snowflake
MOD(tbl.bigint_col, 3);
BIGINT;

# dialect: snowflake
MOD(tbl.double_col, 2.5);
DOUBLE;

# dialect: snowflake
MOD(42, 7);
INT;

# dialect: snowflake
MONTHS_BETWEEN(tbl.date_col, CAST('2019-01-01' AS DATE));
DOUBLE;

# dialect: snowflake
MONTHS_BETWEEN(tbl.timestamp_col, CAST('2019-02-15 01:00:00' AS TIMESTAMP));
DOUBLE;

# dialect: snowflake
REGR_AVGX(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_AVGX(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_AVGX(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_AVGY(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_AVGY(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_AVGY(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_COUNT(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_COUNT(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_COUNT(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_COUNT(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_INTERCEPT(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_INTERCEPT(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_INTERCEPT(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_INTERCEPT(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_R2(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_R2(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_R2(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_R2(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_SXX(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_SXX(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_SXX(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_SXX(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_SXY(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_SXY(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_SXY(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_SXY(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_SYY(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_SYY(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_SYY(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_SYY(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_SLOPE(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: snowflake
REGR_SLOPE(tbl.double_col, tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
REGR_SLOPE(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_SLOPE(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_VALX(NULL, 2.0);
DOUBLE;

# dialect: snowflake
REGR_VALX(NULL, NULL);
DOUBLE;

# dialect: snowflake
REGR_VALX(2.0, NULL);
DOUBLE;

# dialect: snowflake
REGR_VALX(1.0, 2.0);
DOUBLE;

# dialect: snowflake
REGR_VALX(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_VALX(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
REGR_VALY(1.0, 2.0);
DOUBLE;

# dialect: snowflake
REGR_VALY(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: snowflake
REGR_VALY(tbl.decfloat_col, tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
'foo' REGEXP 'bar';
BOOLEAN;

# dialect: snowflake
'foo' NOT REGEXP 'bar';
BOOLEAN;

# dialect: snowflake
'text123' REGEXP '^[a-z]+[0-9]+$';
BOOLEAN;

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
REGEXP_COUNT('hello world', 'l');
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_COUNT('hello world', 'l', 1);
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_COUNT('hello world', 'l', 1, 'i');
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_EXTRACT_ALL('hello world', 'world');
ARRAY;

# dialect: snowflake
REGEXP_EXTRACT_ALL('hello world', 'world', 1);
ARRAY;

# dialect: snowflake
REGEXP_EXTRACT_ALL('hello world', 'world', 1, 1);
ARRAY;

# dialect: snowflake
REGEXP_EXTRACT_ALL('hello world', 'world', 1, 1, 'i');
ARRAY;

# dialect: snowflake
REGEXP_EXTRACT_ALL('hello world', 'world', 1, 1, 'i', 0);
ARRAY;

# dialect: snowflake
REGEXP_INSTR('hello world', 'world');
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_INSTR('hello world', 'world', 1, 1, 0);
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_INSTR('hello world', 'world', 1, 1, 0, 'i');
DECIMAL(38, 0);

# dialect: snowflake
REGEXP_INSTR('hello world', 'world', 1, 1, 0, 'i', 1);
DECIMAL(38, 0);

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
REGEXP_SUBSTR_ALL('hello world', 'world');
ARRAY;

# dialect: snowflake
REGEXP_SUBSTR_ALL('hello world', 'world', 1);
ARRAY;

# dialect: snowflake
REGEXP_SUBSTR_ALL('hello world', 'world', 1, 1);
ARRAY;

# dialect: snowflake
REGEXP_SUBSTR_ALL('hello world', 'world', 1, 1, 'i');
ARRAY;

# dialect: snowflake
REGEXP_SUBSTR_ALL('hello world', 'world', 1, 1, 'i', 0);
ARRAY;

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
ROUND(42);
INT;

# dialect: snowflake
ROUND(tbl.bigint_col, -1);
BIGINT;

# dialect: snowflake
ROUND(tbl.double_col, 0, 'HALF_TO_EVEN');
DOUBLE;

# dialect: snowflake
ROUND(CAST(3.14 AS FLOAT), 1);
FLOAT;

# dialect: snowflake
ROUND(CAST(1.5 AS DECFLOAT), 0);
DECFLOAT;

# dialect: snowflake
FLOOR(CAST(3.7 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
FLOOR(CAST(3.7 AS FLOAT));
FLOAT;

# dialect: snowflake
FLOOR(tbl.double_col);
DOUBLE;

# dialect: snowflake
CEIL(CAST(3.2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
CEIL(CAST(3.2 AS FLOAT));
FLOAT;

# dialect: snowflake
CEIL(tbl.double_col);
DOUBLE;

# dialect: snowflake
SQRT(CAST(16 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
SQRT(CAST(16 AS DOUBLE));
DOUBLE;

# dialect: snowflake
EXP(CAST(2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
EXP(CAST(2 AS DOUBLE));
DOUBLE;

# dialect: snowflake
LN(CAST(10 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
LN(CAST(10 AS DOUBLE));
DOUBLE;

# dialect: snowflake
LOG(CAST(100 AS DECFLOAT), 10);
DECFLOAT;

# dialect: snowflake
LOG(CAST(100 AS DOUBLE), 10);
DOUBLE;

# dialect: snowflake
POW(CAST(2 AS DECFLOAT), 3);
DECFLOAT;

# dialect: snowflake
POW(CAST(2 AS DOUBLE), 3);
DOUBLE;

# dialect: snowflake
SIN(CAST(1.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
SIN(CAST(1.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
COS(CAST(1.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
COS(CAST(1.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
TAN(CAST(1.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
TAN(CAST(1.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
COT(CAST(1.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
COT(CAST(1.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
ASIN(CAST(0.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
ASIN(CAST(0.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
ACOS(CAST(0.5 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
ACOS(CAST(0.5 AS DOUBLE));
DOUBLE;

# dialect: snowflake
ATAN(CAST(1 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
ATAN(CAST(1 AS DOUBLE));
DOUBLE;

# dialect: snowflake
ATAN2(CAST(1 AS DECFLOAT), 1);
DECFLOAT;

# dialect: snowflake
ATAN2(CAST(1 AS DOUBLE), 1);
DOUBLE;

# dialect: snowflake
DEGREES(CAST(3.14159 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
DEGREES(CAST(3.14159 AS DOUBLE));
DOUBLE;

# dialect: snowflake
RADIANS(CAST(180 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
RADIANS(CAST(180 AS DOUBLE));
DOUBLE;

# dialect: snowflake
TANH(CAST(1 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
TANH(CAST(1 AS DOUBLE));
DOUBLE;

# dialect: snowflake
TO_DECFLOAT('123.456');
DECFLOAT;

# dialect: snowflake
TO_DECFLOAT('123.456', '999.999');
DECFLOAT;

# dialect: snowflake
TRY_TO_DECFLOAT('123.456');
DECFLOAT;

# dialect: snowflake
TRY_TO_DECFLOAT('invalid');
DECFLOAT;

# dialect: snowflake
TRY_TO_BINARY('48656C6C6F');
BINARY;

# dialect: snowflake
TRY_TO_BINARY('48656C6C6F', 'HEX');
BINARY;

# dialect: snowflake
TRY_TO_BOOLEAN('true');
BOOLEAN;

# dialect: snowflake
TO_DATE('2024-01-31');
DATE;

# dialect: snowflake
TO_DATE('2024-01-31', 'AUTO');
DATE;

# dialect: snowflake
TRY_TO_DATE('2024-01-31');
DATE;

# dialect: snowflake
TRY_TO_DATE('2024-01-31', 'AUTO');
DATE;

# dialect: snowflake
TO_DECIMAL('123.45');
DECIMAL(38, 0);

# dialect: snowflake
TO_DECIMAL('123.45', '999.99');
DECIMAL(38, 0);

# dialect: snowflake
TO_DECIMAL('123.45', '999.99', 10, 2);
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_DECIMAL('123.45');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_DECIMAL('123.45', '999.99');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_DECIMAL('123.45', '999.99', 10, 2);
DECIMAL(38, 0);

# dialect: snowflake
TO_DOUBLE('123.456');
DOUBLE;

# dialect: snowflake
TO_DOUBLE('123.456', '999.99');
DOUBLE;

# dialect: snowflake
TRY_TO_DOUBLE('123.456');
DOUBLE;

# dialect: snowflake
TRY_TO_DOUBLE('123.456', '999.99');
DOUBLE;

# dialect: snowflake
TO_FILE(tbl.obj_col);
FILE;

# dialect: snowflake
TO_FILE('file.csv');
FILE;

# dialect: snowflake
TO_FILE('file.csv', '/relativepath/');
FILE;

# dialect: snowflake
TRY_TO_FILE(tbl.obj_col);
FILE;

# dialect: snowflake
TRY_TO_FILE('file.csv');
FILE;

# dialect: snowflake
TRY_TO_FILE('file.csv', '/relativepath/');
FILE;

# dialect: snowflake
TO_NUMBER('123.45');
DECIMAL(38, 0);

# dialect: snowflake
TO_NUMBER('123.45', '999.99');
DECIMAL(38, 0);

# dialect: snowflake
TO_NUMBER('123.45', '999.99', 10, 2);
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMBER('123.45');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMBER('123.45', '999.99');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMBER('123.45', '999.99', 10, 2);
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMERIC('123.45');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMERIC('123.45', '999.99');
DECIMAL(38, 0);

# dialect: snowflake
TRY_TO_NUMERIC('123.45', '999.99', 10, 2);
DECIMAL(38, 0);

# dialect: snowflake
TO_TIME('12:30:00');
TIME;

# dialect: snowflake
TO_TIME('12:30:00', 'AUTO');
TIME;

# dialect: snowflake
TRY_TO_TIME('12:30:00');
TIME;

# dialect: snowflake
TRY_TO_TIME('12:30:00', 'AUTO');
TIME;

# dialect: snowflake
TO_TIME('093000', 'HH24MISS');
TIME;

# dialect: snowflake
TRY_TO_TIME('093000', 'HH24MISS');
TIME;

# dialect: snowflake
TO_TIMESTAMP('2024-01-15 12:30:00');
TIMESTAMP;

# dialect: snowflake
TO_TIMESTAMP('2024-01-15 12:30:00', 'AUTO');
TIMESTAMP;

# dialect: snowflake
TO_TIMESTAMP_LTZ('2024-01-15 12:30:00');
TIMESTAMPLTZ;

# dialect: snowflake
TO_TIMESTAMP_LTZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPLTZ;

# dialect: snowflake
TO_TIMESTAMP_NTZ('2024-01-15 12:30:00');
TIMESTAMPNTZ;

# dialect: snowflake
TO_TIMESTAMP_NTZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPNTZ;

# dialect: snowflake
TO_TIMESTAMP_TZ('2024-01-15 12:30:00');
TIMESTAMPTZ;

# dialect: snowflake
TO_TIMESTAMP_TZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP('2024-01-15 12:30:00');
TIMESTAMP;

# dialect: snowflake
TRY_TO_TIMESTAMP('2024-01-15 12:30:00', 'AUTO');
TIMESTAMP;

# dialect: snowflake
TRY_TO_TIMESTAMP_LTZ('2024-01-15 12:30:00');
TIMESTAMPLTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP_LTZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPLTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP_NTZ('2024-01-15 12:30:00');
TIMESTAMPNTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP_NTZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPNTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP_TZ('2024-01-15 12:30:00');
TIMESTAMPTZ;

# dialect: snowflake
TRY_TO_TIMESTAMP_TZ('2024-01-15 12:30:00', 'AUTO');
TIMESTAMPTZ;

# dialect: snowflake
ABS(CAST(-123.456 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
ABS(CAST(-123.456 AS FLOAT));
FLOAT;

# dialect: snowflake
MOD(CAST(10 AS DECFLOAT), 3);
DECFLOAT;

# dialect: snowflake
MOD(CAST(10 AS FLOAT), 3);
FLOAT;

# dialect: snowflake
GREATEST(CAST(1 AS FLOAT), CAST(2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
GREATEST(CAST(2 AS DECFLOAT), CAST(2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
GREATEST(CAST(1 AS FLOAT), CAST(2 AS FLOAT));
FLOAT;

# dialect: snowflake
LEAST(CAST(1 AS FLOAT), CAST(2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
LEAST(CAST(1 AS DECFLOAT), CAST(2 AS DECFLOAT));
DECFLOAT;

# dialect: snowflake
LEAST(CAST(1 AS FLOAT), CAST(2 AS FLOAT));
FLOAT;

# dialect: snowflake
SECOND(CAST('08:50:57' AS TIME));
INT;

# dialect: snowflake
SQUARE(tbl.double_col);
DOUBLE;

# dialect: snowflake
TANH(tbl.double_col);
DOUBLE;

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
RTRIMMED_LENGTH(' ABCD ');
INT;

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
SIN(tbl.double_col);
DOUBLE;

# dialect: snowflake
SINH(1);
DOUBLE;

# dialect: snowflake
SINH(1.5);
DOUBLE;

# dialect: snowflake
SIGN(tbl.double_col);
INT;

# dialect: snowflake
SKEW(tbl.double_col);
DOUBLE;

# dialect: snowflake
SOUNDEX(tbl.str_col);
VARCHAR;

# dialect: snowflake
SOUNDEX_P123('test');
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
SQRT(tbl.double_col);
DOUBLE;

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
SPLIT_PART('11.22.33', '.', 1);
VARCHAR;

# dialect: snowflake
STRTOK('hello world');
VARCHAR;

# dialect: snowflake
STRTOK('hello world', ' ');
VARCHAR;

# dialect: snowflake
STRTOK('a.b.c', '.', 1);
VARCHAR;

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
SEARCH(line, 'king');
BOOLEAN;

# dialect: snowflake
SEARCH((play, line), 'dream');
BOOLEAN;

# dialect: snowflake
SEARCH(line, 'king', ANALYZER => 'UNICODE_ANALYZER');
BOOLEAN;

# dialect: snowflake
SEARCH(line, 'king', SEARCH_MODE => 'OR');
BOOLEAN;

# dialect: snowflake
SEARCH(line, 'king', ANALYZER => 'UNICODE_ANALYZER', SEARCH_MODE => 'AND');
BOOLEAN;

# dialect: snowflake
SEARCH_IP(col, '192.168.0.0');
BOOLEAN;

# dialect: snowflake
STDDEV(tbl.double_col);
DOUBLE;

# dialect: snowflake
STDDEV(tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
STDDEV_POP(tbl.double_col);
DOUBLE;

# dialect: snowflake
STDDEV_POP(tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
STDDEV_SAMP(tbl.double_col);
DOUBLE;

# dialect: snowflake
STDDEV_SAMP(tbl.double_col) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
STRTOK_TO_ARRAY('a,b,c', ',');
ARRAY;

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
TAN(tbl.double_col);
DOUBLE;

# dialect: snowflake
TIMEADD(hour, 1, CAST('14:30:45' AS TIME));
TIME;

# dialect: snowflake
TIMEADD(minute, 30, CAST('2024-05-09 14:30:45' AS TIMESTAMP));
TIMESTAMP;

# dialect: snowflake
TIMEADD(day, 1, CAST('2024-05-09' AS DATE));
DATE;

# dialect: snowflake
TIMEADD(hour, 1, CAST('2024-05-09' AS DATE));
TIMESTAMPNTZ;

# dialect: snowflake
TIME_FROM_PARTS(14, 30, 45);
TIME;

# dialect: snowflake
TIME_FROM_PARTS(14, 30, 45, 123);
TIME;

# dialect: snowflake
TIMEFROMPARTS(14, 30, 45);
TIME;

# dialect: snowflake
TIMEFROMPARTS(14, 30, 45, 123);
TIME;

# dialect: snowflake
TIME_SLICE(tbl.timestamp_col, 15, 'minute');
TIMESTAMP;

# dialect: snowflake
TIME_SLICE(tbl.date_col, 1, 'day', 'start');
DATE;

# dialect: snowflake
TIMESTAMPADD(DAY, 5, CAST('2008-12-25' AS DATE));
DATE;

# dialect: snowflake
TIMESTAMPADD(HOUR, 3, TO_TIME('05:00:00'));
TIME;

# dialect: snowflake
TIMESTAMPADD(YEAR, 1, TO_TIMESTAMP('2022-05-08 14:30:00'));
TIMESTAMP;

# dialect: snowflake
TRANSLATE('hello world', 'elo', 'XYZ');
VARCHAR;

# dialect: snowflake
UNICODE('€');
INT;

# dialect: snowflake
WIDTH_BUCKET(tbl.double_col, 0, 100, 10);
INT;

# dialect: snowflake
ZEROIFNULL(5);
INT;

# dialect: snowflake
ZEROIFNULL(5::BIGINT);
BIGINT;

# dialect: snowflake
ZEROIFNULL(5.5);
DOUBLE;

# dialect: snowflake
ZEROIFNULL(5.5::FLOAT);
FLOAT;

# dialect: snowflake
ZEROIFNULL(5.12::DECIMAL(10,2));
DECIMAL(10, 2);

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
TRY_BASE64_DECODE_BINARY('SGVsbG8=');
BINARY;

# dialect: snowflake
TRY_BASE64_DECODE_BINARY('SGVsbG8=', 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/');
BINARY;

# dialect: snowflake
TRY_BASE64_DECODE_STRING('SGVsbG8gV29ybGQ=');
VARCHAR;

# dialect: snowflake
TRY_BASE64_DECODE_STRING('SGVsbG8gV29ybGQ=', 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/');
VARCHAR;

# dialect: snowflake
TRY_HEX_DECODE_BINARY('48656C6C6F');
BINARY;

# dialect: snowflake
TRY_HEX_DECODE_STRING('48656C6C6F');
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

# dialect: snowflake
OBJECT_AGG(tbl.str_col, tbl.variant_col);
OBJECT;

# dialect: snowflake
PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY tbl.int_col);
INT;

# dialect: snowflake
PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY tbl.double_col);
DOUBLE;

# dialect: snowflake
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY tbl.int_col);
INT;

# dialect: snowflake
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tbl.double_col);
DOUBLE;

# dialect: snowflake
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tbl.bigint_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
PARSE_IP('192.168.1.1', 'INET');
OBJECT;

# dialect: snowflake
MAX(tbl.bigint_col);
BIGINT;

# dialect: snowflake
MAX(tbl.int_col);
INT;

# dialect: snowflake
MAX(tbl.double_col);
DOUBLE;

# dialect: snowflake
MAX(tbl.str_col);
VARCHAR;

# dialect: snowflake
MAX(tbl.date_col);
DATE;

# dialect: snowflake
MAX(tbl.timestamp_col);
TIMESTAMP;

# dialect: snowflake
MAX_BY('foo', tbl.bigint_col);
VARCHAR;

# dialect: snowflake
MAX_BY('foo', tbl.bigint_col, 3);
ARRAY;

# dialect: snowflake
MIN_BY('foo', tbl.bigint_col);
VARCHAR;

# dialect: snowflake
MIN_BY('foo', tbl.bigint_col, 3);
ARRAY;

# dialect: snowflake
APPROX_PERCENTILE(tbl.bigint_col, 0.5);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE(tbl.double_col, 0.5);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE(tbl.int_col, 0.9);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE(tbl.bigint_col, 0.5) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE(tbl.double_col, 0.5) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE(tbl.int_col, 0.9) OVER (PARTITION BY 1);
DOUBLE;

# dialect: snowflake
APPROX_PERCENTILE_COMBINE(tbl.state_col);
OBJECT;

# dialect: snowflake
APPROX_PERCENTILE_ACCUMULATE(tbl.bigint_col);
OBJECT;

# dialect: snowflake
APPROX_PERCENTILE_ACCUMULATE(tbl.double_col);
OBJECT;

# dialect: snowflake
APPROX_PERCENTILE_ACCUMULATE(tbl.int_col);
OBJECT;

# dialect: snowflake
APPROX_PERCENTILE_ESTIMATE(tbl.state_col, 0.5);
DOUBLE;

# dialect: snowflake
APPROX_TOP_K_ACCUMULATE(tbl.str_col, 10);
OBJECT;

# dialect: snowflake
APPROX_TOP_K_COMBINE(tbl.state_col, 10);
OBJECT;

# dialect: snowflake
APPROX_TOP_K_COMBINE(tbl.state_col);
OBJECT;

# dialect: snowflake
APPROX_TOP_K_ESTIMATE(tbl.state_col, 4);
ARRAY;

# dialect: snowflake
APPROX_TOP_K_ESTIMATE(tbl.state_col);
ARRAY;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.str_col);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.bigint_col);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.double_col);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(*);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(DISTINCT tbl.str_col);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.str_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.bigint_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
APPROX_COUNT_DISTINCT(tbl.double_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
APPROX_TOP_K(tbl.bigint_col);
ARRAY;

# dialect: snowflake
APPROX_TOP_K(tbl.str_col);
ARRAY;

# dialect: snowflake
APPROX_TOP_K(tbl.str_col, 5);
ARRAY;

# dialect: snowflake
APPROX_TOP_K(tbl.str_col, 5, 1000);
ARRAY;

# dialect: snowflake
MINHASH(5, tbl.int_col);
VARIANT;

# dialect: snowflake
MINHASH(5, tbl.int_col, tbl.str_col);
VARIANT;

# dialect: snowflake
MINHASH(5, *);
VARIANT;

# dialect: snowflake
MINHASH_COMBINE(tbl.variant_col);
VARIANT;

# dialect: snowflake
APPROXIMATE_SIMILARITY(tbl.variant_col);
DOUBLE;

# dialect: snowflake
APPROXIMATE_JACCARD_INDEX(tbl.variant_col);
DOUBLE;

# dialect: snowflake
MIN(tbl.double_col);
DOUBLE;

# dialect: snowflake
MIN(tbl.int_col);
INT;

# dialect: snowflake
MIN(tbl.bigint_col);
BIGINT;

# dialect: snowflake
MIN(CAST(100 AS DECIMAL(10,2)));
DECIMAL(10, 2);

# dialect: snowflake
MIN(tbl.bigint_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
VECTOR_COSINE_SIMILARITY([1,2,3], [4,5,6]);
DOUBLE;

# dialect: snowflake
VECTOR_INNER_PRODUCT([1,2,3], [4,5,6]);
DOUBLE;

# dialect: snowflake
VECTOR_L1_DISTANCE([1,2,3], [4,5,6]);
DOUBLE;

# dialect: snowflake
VECTOR_L2_DISTANCE([1,2,3], [4,5,6]);
DOUBLE;

# dialect: snowflake
ZIPF(1, 10, RANDOM());
BIGINT;

# dialect: snowflake
ZIPF(2, 100, 1234);
BIGINT;

# dialect: snowflake
XMLGET(PARSE_XML('<root><level2>content</level2></root>'), 'level2');
OBJECT;

# dialect: snowflake
XMLGET(PARSE_XML('<root><item>a</item><item>b</item></root>'), 'item', 1);
OBJECT;

# dialect: snowflake
MODE(tbl.double_col);
DOUBLE;

# dialect: snowflake
MODE(tbl.date_col);
DATE;

# dialect: snowflake
MODE(tbl.timestamp_col);
TIMESTAMP;

# dialect: snowflake
MODE(tbl.bool_col);
BOOLEAN;

# dialect: snowflake
MODE(CAST(100 AS DECIMAL(10,2)));
DECIMAL(10, 2);

# dialect: snowflake
MODE(tbl.bigint_col) OVER (PARTITION BY 1);
BIGINT;

# dialect: snowflake
MODE(CAST(NULL AS INT));
INT;

# dialect: snowflake
MODE(tbl.str_col) OVER (PARTITION BY tbl.int_col);
VARCHAR;

# dialect: snowflake
VAR_SAMP(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
VAR_SAMP(tbl.double_col);
DOUBLE;

# dialect: snowflake
VAR_SAMP(tbl.int_col);
NUMBER(38, 6);

# dialect: snowflake
VARIANCE_SAMP(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
VARIANCE_SAMP(tbl.double_col);
DOUBLE;

# dialect: snowflake
VARIANCE_SAMP(tbl.int_col);
NUMBER(38, 6);

# dialect: snowflake
VARIANCE(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
VARIANCE(tbl.double_col);
DOUBLE;

# dialect: snowflake
VARIANCE(tbl.int_col);
NUMBER(38, 6);

# dialect: snowflake
VAR_POP(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
VAR_POP(tbl.double_col);
DOUBLE;

# dialect: snowflake
VAR_POP(tbl.int_col);
NUMBER(38, 6);

# dialect: snowflake
VARIANCE_POP(tbl.decfloat_col);
DECFLOAT;

# dialect: snowflake
VARIANCE_POP(tbl.double_col);
DOUBLE;

# dialect: snowflake
VARIANCE_POP(tbl.int_col);
NUMBER(38, 6);

# dialect: snowflake
VARIANCE_POP(1::NUMBER(38, 6));
NUMBER(38, 12);

# dialect: snowflake
VARIANCE_POP(1::NUMBER(38, 15));
NUMBER(38, 15);

# dialect: snowflake
VARIANCE_POP(1::NUMBER(30, 5));
NUMBER(38, 12);

# dialect: snowflake
ENCRYPT(tbl.str_col, 'passphrase');
BINARY;

# dialect: snowflake
ENCRYPT(tbl.str_col, 'passphrase', 'aad');
BINARY;

# dialect: snowflake
ENCRYPT(tbl.str_col, 'passphrase', 'aad', 'AES-GCM');
BINARY;

# dialect: snowflake
ENCRYPT_RAW(tbl.str_col, tbl.key_col, tbl.iv_col);
BINARY;

# dialect: snowflake
ENCRYPT_RAW(tbl.str_col, tbl.key_col, tbl.iv_col, tbl.aad_col);
BINARY;

# dialect: snowflake
ENCRYPT_RAW(tbl.str_col, tbl.key_col, tbl.iv_col, tbl.aad_col, 'AES-GCM');
BINARY;

# dialect: snowflake
DECRYPT(tbl.encrypted_col, 'passphrase');
BINARY;

# dialect: snowflake
DECRYPT(tbl.encrypted_col, 'passphrase', 'aad');
BINARY;

# dialect: snowflake
DECRYPT(tbl.encrypted_col, 'passphrase', 'aad', 'AES-GCM');
BINARY;

# dialect: snowflake
DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col);
BINARY;

# dialect: snowflake
DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col);
BINARY;

# dialect: snowflake
DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col, 'AES-GCM');
BINARY;

# dialect: snowflake
DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col, 'AES-GCM', HEX_DECODE_BINARY('ff'));
BINARY;

# dialect: snowflake
TRY_DECRYPT(tbl.encrypted_col, 'passphrase');
BINARY;

# dialect: snowflake
TRY_DECRYPT(tbl.encrypted_col, 'passphrase', 'aad');
BINARY;

# dialect: snowflake
TRY_DECRYPT(tbl.encrypted_col, 'passphrase', 'aad', 'AES-GCM');
BINARY;

# dialect: snowflake
TRY_DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col);
BINARY;

# dialect: snowflake
TRY_DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col);
BINARY;

# dialect: snowflake
TRY_DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col, 'AES-GCM');
BINARY;

# dialect: snowflake
TRY_DECRYPT_RAW(tbl.encrypted_col, tbl.key_col, tbl.iv_col, tbl.aad_col, 'AES-GCM', HEX_DECODE_BINARY('ff'));
BINARY;

# dialect: snowflake
SEQ1();
INT;

# dialect: snowflake
SEQ1(1);
INT;

# dialect: snowflake
SEQ2();
INT;

# dialect: snowflake
SEQ2(1);
INT;

# dialect: snowflake
SEQ4();
INT;

# dialect: snowflake
SEQ4(1);
INT;

# dialect: snowflake
SEQ8();
BIGINT;

# dialect: snowflake
SEQ8(1);
BIGINT;

--------------------------------------
-- T-SQL
--------------------------------------

# dialect: tsql
SYSDATETIMEOFFSET();
TIMESTAMPTZ;

# dialect: tsql
RADIANS(90);
INT;

# dialect: tsql
SIN(tbl.int_col);
FLOAT;

# dialect: tsql
SIN(tbl.float_col);
FLOAT;

# dialect: tsql
COS(tbl.int_col);
FLOAT;

# dialect: tsql
COS(tbl.float_col);
FLOAT;

# dialect: tsql
TAN(tbl.int_col);
FLOAT;

# dialect: tsql
TAN(tbl.float_col);
FLOAT;

# dialect: tsql
COT(tbl.int_col);
FLOAT;

# dialect: tsql
COT(tbl.float_col);
FLOAT;

# dialect: tsql
ATN2(tbl.int_col, tbl.int_col);
FLOAT;

# dialect: tsql
ATN2(tbl.int_col, tbl.float_col);
FLOAT;

# dialect: tsql
ATN2(tbl.float_col, tbl.int_col);
FLOAT;

# dialect: tsql
ATN2(tbl.float_col, tbl.float_col);
FLOAT;

# dialect: tsql 
ASIN(tbl.int_col);
FLOAT;

# dialect: tsql 
ASIN(tbl.float_col);
FLOAT;

# dialect: tsql 
ACOS(tbl.int_col);
FLOAT;

# dialect: tsql 
ACOS(tbl.float_col);
FLOAT;

# dialect: tsql 
ATAN(tbl.int_col);
FLOAT;

# dialect: tsql 
ATAN(tbl.float_col);
FLOAT;

# dialect: tsql
CURRENT_TIMEZONE();
NVARCHAR;

# dialect: tsql
SOUNDEX(tbl.str_col);
VARCHAR;

# dialect: tsql
STUFF(tbl.str_col, tbl.int_col, tbl.int_col, tbl.str_col);
VARCHAR;

--------------------------------------
-- MySQL
--------------------------------------

# dialect: mysql
DEGREES(tbl.double_col);
DOUBLE;

# dialect: mysql
DEGREES(tbl.int_col);
DOUBLE;

# dialect: mysql
LOCALTIME;
DATETIME;

# dialect: mysql
ELT(1, 'a', 'b');
VARCHAR;

# dialect: mysql
DAYOFWEEK(tbl.date_col);
INT;

# dialect: mysql
DAYOFMONTH(tbl.date_col);
INT;

# dialect: mysql
DAYOFYEAR(tbl.date_col);
INT;

# dialect: mysql
MONTH(tbl.date_col);
INT;

# dialect: mysql
WEEK(tbl.date_col);
INT;

# dialect: mysql
WEEK(tbl.date_col, int_col);
INT;

# dialect: mysql
QUARTER(tbl.date_col);
INT;

# dialect: mysql
HOUR(tbl.time_col);
INT;

# dialect: mysql
SECOND(tbl.time_col);
INT;

# dialect: mysql
SIN(tbl.int_col);
DOUBLE;

# dialect: mysql
SIN(tbl.double_col);
DOUBLE;

# dialect: mysql 
COS(tbl.int_col);
DOUBLE;

# dialect: mysql 
COS(tbl.double_col);
DOUBLE;

# dialect: mysql 
TAN(tbl.int_col);
DOUBLE;

# dialect: mysql 
TAN(tbl.double_col);
DOUBLE;

# dialect: mysql 
COT(tbl.int_col);
DOUBLE;

# dialect: mysql 
COT(tbl.double_col);
DOUBLE;

# dialect: mysql 
ASIN(tbl.int_col);
DOUBLE;

# dialect: mysql 
ASIN(tbl.double_col);
DOUBLE;

# dialect: mysql 
ACOS(tbl.int_col);
DOUBLE;

# dialect: mysql 
ACOS(tbl.double_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.int_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.double_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.int_col, tbl.double_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.double_col, tbl.int_col);
DOUBLE;

# dialect: mysql 
ATAN(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: mysql 
ATAN2(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: mysql 
ATAN2(tbl.int_col, tbl.double_col);
DOUBLE;

# dialect: mysql 
ATAN2(tbl.double_col, tbl.int_col);
DOUBLE;

# dialect: mysql 
ATAN2(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: mysql
VERSION();
VARCHAR;

--------------------------------------
-- DuckDB
--------------------------------------

# dialect: duckdb
SHA1(tbl.str_col);
VARCHAR;

# dialect: duckdb
SHA256(tbl.str_col);
VARCHAR;

# dialect: duckdb 
GET_BIT(tbl.str_col, tbl.int_col);
INT;

# dialect: duckdb
FACTORIAL(tbl.int_col);
HUGEINT;

# dialect: duckdb
SIN(tbl.int_col);
DOUBLE;

# dialect: duckdb
SIN(tbl.double_col);
DOUBLE;

# dialect: duckdb
ASIN(tbl.int_col);
DOUBLE;

# dialect: duckdb
ASIN(tbl.double_col);
DOUBLE;

# dialect: duckdb
COS(tbl.int_col);
DOUBLE;

# dialect: duckdb
COS(tbl.double_col);
DOUBLE;

# dialect: duckdb
ACOS(tbl.int_col);
DOUBLE;

# dialect: duckdb
ACOS(tbl.double_col);
DOUBLE;

# dialect: duckdb
COT(tbl.int_col);
DOUBLE;

# dialect: duckdb
COT(tbl.double_col);
DOUBLE;

# dialect: duckdb
TAN(tbl.int_col);
DOUBLE;

# dialect: duckdb
TAN(tbl.double_col);
DOUBLE;

# dialect: duckdb
ATAN(tbl.int_col);
DOUBLE;

# dialect: duckdb
ATAN(tbl.double_col);
DOUBLE;

# dialect: duckdb
ATAN2(tbl.int_col, tbl.int_col);
DOUBLE;

# dialect: duckdb
ATAN2(tbl.int_col, tbl.double_col);
DOUBLE;

# dialect: duckdb
ATAN2(tbl.double_col, tbl.int_col);
DOUBLE;

# dialect: duckdb
ATAN2(tbl.double_col, tbl.double_col);
DOUBLE;

# dialect: duckdb
ACOSH(tbl.int_col);
DOUBLE;

# dialect: duckdb
ACOSH(tbl.double_col);
DOUBLE;

# dialect: duckdb
ASINH(tbl.int_col);
DOUBLE;

# dialect: duckdb
ASINH(tbl.double_col);
DOUBLE;

# dialect: duckdb
ATANH(tbl.int_col);
DOUBLE;

# dialect: duckdb
TANH(tbl.int_col);
DOUBLE;

# dialect: duckdb
TANH(tbl.double_col);
DOUBLE;

# dialect: duckdb
COSH(tbl.int_col);
DOUBLE;

# dialect: duckdb
COSH(tbl.double_col);
DOUBLE;

# dialect: duckdb
SINH(tbl.int_col);
DOUBLE;

# dialect: duckdb
SINH(tbl.double_col);
DOUBLE;

# dialect: duckdb
ATANH(tbl.double_col);
DOUBLE;

# dialect: duckdb
ISINF(tbl.float_col);
BOOLEAN;

# dialect: duckdb
RANDOM();
DOUBLE;

# dialect: duckdb
QUARTER(tbl.date_col);
BIGINT;

# dialect: duckdb
QUARTER(tbl.timestamp_col);
BIGINT;

# dialect: duckdb
QUARTER(tbl.interval_col);
BIGINT;

# dialect: duckdb
QUARTER(tbl.timestamp_tz_col);
BIGINT;

# dialect: duckdb
MINUTE(tbl.date_col);
BIGINT;

# dialect: duckdb 
MONTH(tbl.date_col);
BIGINT;

# dialect: duckdb 
DAYOFWEEK(tbl.date_col);
BIGINT;

# dialect: duckdb 
DAYOFYEAR(tbl.date_col);
BIGINT;

# dialect: duckdb
EPOCH(tbl.interval_col);
DOUBLE;

# dialect: duckdb
DAYOFMONTH(tbl.date_col);
BIGINT;

# dialect: duckdb
DAY(tbl.date_col);
BIGINT;

# dialect: duckdb
HOUR(tbl.date_col);
BIGINT;

# dialect: duckdb
SECOND(tbl.date_col);
BIGINT;

# dialect: duckdb
TO_DAYS(tbl.int_col);
INTERVAL;

# dialect: duckdb
BIT_LENGTH(tbl.str_col);
BIGINT;

# dialect: duckdb
MAKE_TIME(tbl.bigint_col, tbl.bigint_col, tbl.double_col);
TIME;

# dialect: duckdb
LENGTH(tbl.str_col);
BIGINT;

# dialect: duckdb
TRANSLATE(tbl.str_col, tbl.str_col, tbl.str_col);
VARCHAR;

--------------------------------------
-- Presto / Trino
--------------------------------------

# dialect: presto, trino
MD5(tbl.bin_col);
VARBINARY;

# dialect: presto, trino
LEVENSHTEIN_DISTANCE(tbl.str_col, tbl.str_col);
BIGINT;

# dialect: presto, trino
LENGTH(tbl.str_col);
BIGINT;

# dialect: presto, trino
POSITION(tbl.str_col IN tbl.str_col);
BIGINT;

# dialect: presto, trino
STRPOS(tbl.str_col, tbl.str_col);
BIGINT;

# dialect: presto, trino
BITWISE_AND(tbl.bigint_col, tbl.bigint_col);
BIGINT;

# dialect: presto, trino
BITWISE_NOT(tbl.bigint_col);
BIGINT;

# dialect: presto, trino
BITWISE_OR(tbl.bigint_col, tbl.bigint_col);
BIGINT;

# dialect: presto, trino
BITWISE_XOR(tbl.bigint_col, tbl.bigint_col);
BIGINT;

# dialect: presto, trino
WIDTH_BUCKET(tbl.double_col, tbl.array_col);
BIGINT;