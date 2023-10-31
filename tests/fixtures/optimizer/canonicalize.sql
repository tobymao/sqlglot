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

SELECT a FROM x WHERE COALESCE(0, 1);
SELECT "x"."a" AS "a" FROM "x" AS "x" WHERE COALESCE(0 <> 0, 1 <> 0);

SELECT a FROM x WHERE CASE WHEN COALESCE(b, 1) THEN 1 ELSE 0 END;
SELECT "x"."a" AS "a" FROM "x" AS "x" WHERE CASE WHEN COALESCE("x"."b" <> 0, 1 <> 0) THEN 1 ELSE 0 END <> 0;

--------------------------------------
-- Replace date functions
--------------------------------------
DATE('2023-01-01');
CAST('2023-01-01' AS DATE);

TIMESTAMP('2023-01-01');
CAST('2023-01-01' AS TIMESTAMP);

TIMESTAMP('2023-01-01', '12:00:00');
TIMESTAMP('2023-01-01', '12:00:00');

--------------------------------------
-- Coerce date function args
--------------------------------------
'2023-01-01' + INTERVAL '1' DAY;
CAST('2023-01-01' AS DATE) + INTERVAL '1' DAY;

'2023-01-01' + INTERVAL '1' HOUR;
CAST('2023-01-01' AS DATETIME) + INTERVAL '1' HOUR;

'2023-01-01 00:00:01' + INTERVAL '1' HOUR;
CAST('2023-01-01 00:00:01' AS DATETIME) + INTERVAL '1' HOUR;

CAST('2023-01-01' AS DATE) + INTERVAL '1' HOUR;
CAST(CAST('2023-01-01' AS DATE) AS DATETIME) + INTERVAL '1' HOUR;

SELECT t.d + INTERVAL '1' HOUR FROM temporal AS t;
SELECT CAST("t"."d" AS DATETIME) + INTERVAL '1' HOUR AS "_col_0" FROM "temporal" AS "t";

DATE_ADD(CAST("x" AS DATE), 1, 'YEAR');
DATE_ADD(CAST("x" AS DATE), 1, 'YEAR');

DATE_ADD('2023-01-01', 1, 'YEAR');
DATE_ADD(CAST('2023-01-01' AS DATE), 1, 'YEAR');

DATE_ADD('2023-01-01 00:00:00', 1, 'DAY');
DATE_ADD(CAST('2023-01-01 00:00:00' AS DATETIME), 1, 'DAY');

SELECT DATE_ADD(t.d, 1, 'HOUR') FROM temporal AS t;
SELECT DATE_ADD(CAST("t"."d" AS DATETIME), 1, 'HOUR') AS "_col_0" FROM "temporal" AS "t";

SELECT DATE_TRUNC('SECOND', t.d) FROM temporal AS t;
SELECT DATE_TRUNC('SECOND', CAST("t"."d" AS DATETIME)) AS "_col_0" FROM "temporal" AS "t";

DATE_TRUNC('DAY', '2023-01-01');
DATE_TRUNC('DAY', CAST('2023-01-01' AS DATE));

DATEDIFF('2023-01-01', '2023-01-02', DAY);
DATEDIFF(CAST('2023-01-01' AS DATETIME), CAST('2023-01-02' AS DATETIME), DAY);
