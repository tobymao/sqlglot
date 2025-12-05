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
