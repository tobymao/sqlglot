SELECT a FROM x;
SELECT "a" FROM "x";

SELECT "a" FROM "x";
SELECT "a" FROM "x";

SELECT x.a AS a FROM db.x;
SELECT "x"."a" AS "a" FROM "db"."x";

SELECT @x;
SELECT @x;

# dialect: snowflake
SELECT * FROM DUAL;
SELECT * FROM DUAL;

# dialect: snowflake
SELECT * FROM "DUAL";
SELECT * FROM "DUAL";

# dialect: snowflake
SELECT * FROM "dual";
SELECT * FROM "dual";

# dialect: snowflake
SELECT dual FROM t;
SELECT "dual" FROM "t";

# dialect: snowflake
SELECT * FROM t AS dual;
SELECT * FROM "t" AS "dual";

# dialect: bigquery
SELECT `p.d.udf`(data).* FROM `p.d.t`;
SELECT `p.d.udf`(`data`).* FROM `p.d.t`;
