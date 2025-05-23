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

--------------------------------------
-- Snowflake
--------------------------------------

LEAST(x::DECIMAL(18, 2));
DECIMAL(18, 2);
