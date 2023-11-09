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
SELECT dual FROM t;
SELECT "dual" FROM "t";
