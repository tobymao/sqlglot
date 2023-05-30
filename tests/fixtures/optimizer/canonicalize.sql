SELECT w.d + w.e AS c FROM w AS w;
SELECT CONCAT("w"."d", "w"."e") AS "c" FROM "w" AS "w";

SELECT CAST(w.d AS DATE) > w.e AS a FROM w AS w;
SELECT CAST("w"."d" AS DATE) > CAST("w"."e" AS DATE) AS "a" FROM "w" AS "w";

SELECT CAST(1 AS VARCHAR) AS a FROM w AS w;
SELECT CAST(1 AS VARCHAR) AS "a" FROM "w" AS "w";

SELECT CAST(1 + 3.2 AS DOUBLE) AS a FROM w AS w;
SELECT 1 + 3.2 AS "a" FROM "w" AS "w";

SELECT CAST('2022-01-01' AS DATE) + INTERVAL '1' day;
SELECT CAST('2022-01-01' AS DATE) + INTERVAL '1' day AS "_col_0";

--------------------------------------
-- Ensure boolean predicates
--------------------------------------

SELECT a FROM x WHERE b;
SELECT "x"."a" AS "a" FROM "x" AS "x" WHERE "x"."b" <> 0;

SELECT a FROM x GROUP BY a HAVING SUM(b);
SELECT "x"."a" AS "a" FROM "x" AS "x" GROUP BY "x"."a" HAVING SUM("x"."b") <> 0;

SELECT a FROM x GROUP BY a HAVING SUM(b) AND TRUE;
SELECT "x"."a" AS "a" FROM "x" AS "x" GROUP BY "x"."a" HAVING SUM("x"."b") <> 0 AND TRUE;

SELECT a FROM x WHERE 1;
SELECT "x"."a" AS "a" FROM "x" AS "x" WHERE 1 <> 0;
