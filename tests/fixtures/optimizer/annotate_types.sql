5;
INT;

-5;
INT;

~5;
INT;

(5);
INT;

5.3;
DOUBLE;

'bla';
VARCHAR;

true;
bool;

not true;
bool;

false;
bool;

x is null;
bool;

x is not null;
bool;

EXISTS(SELECT 1);
bool;

ALL(SELECT 1);
bool;

ANY(SELECT 1);
bool;

null;
UNKNOWN;

# dialect: spark
null;
NULL;

# dialect: databricks
null;
NULL;

null and false;
bool;

null + 1;
int;

CASE WHEN x THEN NULL ELSE 1 END;
INT;

CASE WHEN x THEN 1 ELSE NULL END;
INT;

IF(true, 1, null);
INT;

IF(true, null, 1);
INT;

STRUCT(1 AS col);
STRUCT<col INT>;

# Note: ensure the struct is annotated as UNKNOWN when any of its arguments are UNKNOWN
STRUCT(1, f2);
UNKNOWN;

STRUCT(1 AS col, 2.5 AS row);
STRUCT<col INT, row DOUBLE>;

STRUCT(1);
STRUCT<INT>;

STRUCT(1 AS col, 2.5 AS row, struct(3.5 AS inner_col, 4 AS inner_row) AS nested_struct);
STRUCT<col INT, row DOUBLE, nested_struct STRUCT<inner_col DOUBLE, inner_row INT>>;

STRUCT(1 AS col, 2.5, ARRAY[1, 2, 3] AS nested_array, 'foo');
STRUCT<col INT, DOUBLE, nested_array ARRAY<INT>, VARCHAR>;

STRUCT(1, 2.5, 'bar');
STRUCT<INT, DOUBLE, VARCHAR>;

STRUCT(1 AS "CaseSensitive");
STRUCT<"CaseSensitive" INT>;

# dialect: duckdb
STRUCT_PACK(a := 1, b := 2.5);
STRUCT<a INT, b DOUBLE>;

# dialect: presto
ROW(1, 2.5, 'foo');
STRUCT<INT, DOUBLE, VARCHAR>;

# dialect: bigquery
EXTRACT(date from x);
DATE;

# dialect: bigquery
EXTRACT(time from x);
TIME;

# dialect: bigquery
EXTRACT(day from x);
INT;

CASE WHEN x THEN CAST(y AS DECIMAL(18, 2)) ELSE NULL END;
DECIMAL(18,2);

CASE WHEN x THEN NULL ELSE CAST(y AS DECIMAL(18, 2)) END;
DECIMAL(18,2);

# dialect: bigquery
CASE WHEN TRUE THEN '2010-01-01' ELSE DATE '2020-02-02' END;
DATE;

# dialect: bigquery
CASE WHEN TRUE THEN '2010-01-01' WHEN FALSE THEN DATE '2020-02-02' ELSE '1990-01-01' END;
DATE;

# dialect: bigquery
CASE WHEN TRUE THEN DATETIME '2020-02-02 00:00:00' ELSE '2010-01-01' END;
DATETIME;

# dialect: bigquery
CASE WHEN TRUE THEN TIMESTAMP '2020-02-02 00:00:00' ELSE '2010-01-01' END;
TIMESTAMP;

# dialect: bigquery
NULL;
INT64;

# dialect: bigquery
ARRAY(SELECT 'foo' UNION ALL SELECT 'bar');
ARRAY<STRING>;

# dialect: bigquery
ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3);
ARRAY<INT64>;

# dialect: bigquery
ARRAY(SELECT 1 UNION ALL SELECT 2.5);
ARRAY<FLOAT64>;

1 + (SELECT 2.5 AS c);
DOUBLE;

# dialect: bigquery
CASE WHEN TRUE THEN 2.5 ELSE CAST(3.5 AS BIGNUMERIC) END;
BIGNUMERIC;

# dialect: bigquery
CASE WHEN TRUE THEN CAST(3.5 AS BIGNUMERIC) ELSE 2.5 END;
BIGNUMERIC;

# dialect: bigquery
CASE WHEN TRUE THEN 3/10 ELSE CAST(3.5 AS BIGNUMERIC) END;
FLOAT64;

# dialect: bigquery
CASE WHEN TRUE THEN CAST(3.5 AS BIGNUMERIC) ELSE 3/10 END;
FLOAT64;

# dialect: bigquery
CASE WHEN TRUE THEN 2.4 ELSE 2.5 END;
FLOAT64;

# dialect: bigquery
CASE WHEN x < y THEN 3/10 WHEN x > y THEN 2 ELSE CAST(3.5 AS BIGNUMERIC) END;
FLOAT64;

# dialect: bigquery
CASE WHEN x < y THEN 2 WHEN x > y THEN 3/10 ELSE CAST(3.5 AS BIGNUMERIC) END;
FLOAT64;

# dialect: bigquery
CASE WHEN x < y THEN CAST(3.5 AS BIGNUMERIC) WHEN x > y THEN 3/10 ELSE 2 END;
FLOAT64;

# dialect: snowflake
BITSHIFTLEFT(255, 4);
INT;

# dialect: snowflake
BITSHIFTRIGHT(1024, 2);
INT;

# dialect: snowflake
BITSHIFTLEFT(CAST(255 AS BINARY), 4);
BINARY;

# dialect: snowflake
BITSHIFTRIGHT(CAST(255 AS BINARY), 4);
BINARY;

# dialect: snowflake
BITSHIFTLEFT(X'FF', 4);
BINARY;

# dialect: snowflake
BITSHIFTRIGHT(X'FF', 4);
BINARY;

# dialect: snowflake
BITOR(BITSHIFTLEFT(5, 16), BITSHIFTLEFT(3, 8));
INT;

# dialect: snowflake
BITAND(BITSHIFTLEFT(255, 4), BITSHIFTLEFT(15, 2));
INT;

# dialect: bigquery
CAST(1 AS BIGNUMERIC) + 1.5;
BIGNUMERIC;

# dialect: bigquery
1.5 + CAST(1 AS BIGNUMERIC);
BIGNUMERIC;

# dialect: bigquery
1.5 + CAST(1 AS FLOAT64);
FLOAT64;

# dialect: bigquery
CAST(1 AS FLOAT64) + 1.5;
FLOAT64;

# dialect: bigquery
CAST(1 AS INT) + 1.5;
FLOAT64;

# dialect: bigquery
1.5 + CAST(1 AS INT);
FLOAT64;

# dialect: bigquery
IF(1 = 1, CAST(1 AS BIGNUMERIC) * 1.5, CAST(2 AS BIGNUMERIC));
BIGNUMERIC;
