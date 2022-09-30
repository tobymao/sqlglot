SELECT a FROM x;
SELECT "a" FROM "x";

SELECT "a" FROM "x";
SELECT "a" FROM "x";

SELECT x.a AS a FROM db.x;
SELECT "x"."a" AS "a" FROM "db"."x";

# unquote_values_columns: true
SELECT x.a AS a FROM (VALUES (1)) AS x(a);
SELECT "x".a AS "a" FROM (VALUES (1)) AS "x"(a);

# unquote_values_columns: true
SELECT y.a AS a FROM (SELECT x.a FROM (VALUES (1)) AS x(a)) y;
SELECT "y"."a" AS "a" FROM (SELECT "x".a FROM (VALUES (1)) AS "x"(a)) AS "y";

# unquote_values_columns: true
SELECT x.a AS a, y.b AS b FROM (SELECT x.a FROM (VALUES (1)) AS x(a)) x JOIN (VALUES (1)) AS y(b) ON x.a = y.b;
SELECT "x"."a" AS "a", "y".b AS "b" FROM (SELECT "x".a FROM (VALUES (1)) AS "x"(a)) AS "x" JOIN (VALUES (1)) AS "y"(b) ON "x"."a" = "y".b;
