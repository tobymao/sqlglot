SELECT x.a AS a FROM (SELECT x.a FROM x AS x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 AND y.a = 1;
SELECT x.a AS a FROM (SELECT x.a FROM x AS x WHERE x.a = 1 AND x.b = 1) AS x JOIN y ON y.a = 1 WHERE TRUE AND TRUE AND TRUE;

WITH x AS (SELECT y.a FROM y) SELECT * FROM x WHERE x.a = 1;
WITH x AS (SELECT y.a FROM y WHERE y.a = 1) SELECT * FROM x WHERE TRUE;

SELECT x.a FROM (SELECT * FROM x) AS x CROSS JOIN y WHERE y.a = 1 OR (x.a = 1 AND x.b = 1);
SELECT x.a FROM (SELECT * FROM x) AS x CROSS JOIN y WHERE (x.a = 1 AND x.b = 1) OR y.a = 1;

SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE (x.a = y.a AND x.a = 1 AND x.b = 1) OR x.a = y.a;
SELECT x.a FROM (SELECT * FROM x) AS x JOIN y ON x.a = y.a WHERE TRUE;

SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE (x.a = y.a AND x.a = 1 AND x.b = 1) OR x.a = y.b;
SELECT x.a FROM (SELECT * FROM x) AS x JOIN y ON (x.a = 1 AND x.a = y.a AND x.b = 1) OR x.a = y.b WHERE (x.a = 1 AND x.a = y.a AND x.b = 1) OR x.a = y.b;

SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x) AS x WHERE x.c = 1;
SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x WHERE x.b * 1 = 1) AS x WHERE TRUE;

SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x) AS x WHERE x.c = 1 or x.c = 2;
SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x WHERE x.b * 1 = 1 OR x.b * 1 = 2) AS x WHERE TRUE;

SELECT x.a AS a FROM (SELECT x.a FROM x AS x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 AND (x.c = 1 OR y.c = 1);
SELECT x.a AS a FROM (SELECT x.a FROM x AS x WHERE x.a = 1 AND x.b = 1) AS x JOIN y ON x.c = 1 OR y.c = 1 WHERE TRUE AND TRUE AND (TRUE);

SELECT x.a FROM x AS x JOIN (SELECT y.a FROM y AS y) AS y ON y.a = 1 AND x.a = y.a;
SELECT x.a FROM x AS x JOIN (SELECT y.a FROM y AS y WHERE y.a = 1) AS y ON x.a = y.a AND TRUE;

SELECT x.a AS a FROM x AS x JOIN (SELECT * FROM y AS y) AS y ON y.a = 1 WHERE x.a = 1 AND x.b = 1 AND y.a = x.a;
SELECT x.a AS a FROM x AS x JOIN (SELECT * FROM y AS y WHERE y.a = 1) AS y ON x.a = y.a AND TRUE WHERE x.a = 1 AND TRUE AND x.b = 1;

SELECT x.a AS a FROM x AS x CROSS JOIN (SELECT * FROM y AS y) AS y WHERE x.a = 1 AND x.b = 1 AND y.a = x.a AND y.a = 1;
SELECT x.a AS a FROM x AS x JOIN (SELECT * FROM y AS y WHERE y.a = 1) AS y ON x.a = y.a AND TRUE WHERE x.a = 1 AND TRUE AND x.b = 1 AND TRUE;

with t1 as (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE row_num = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE row_num = 1;

WITH m AS (SELECT a, b FROM (VALUES (1, 2)) AS a1(a, b)), n AS (SELECT a, b FROM m WHERE m.a = 1), o AS (SELECT a, b FROM m WHERE m.a = 2) SELECT n.a, n.b, n.a, o.b FROM n FULL OUTER JOIN o ON n.a = o.a;
WITH m AS (SELECT a, b FROM (VALUES (1, 2)) AS a1(a, b)), n AS (SELECT a, b FROM m WHERE m.a = 1), o AS (SELECT a, b FROM m WHERE m.a = 2) SELECT n.a, n.b, n.a, o.b FROM n FULL OUTER JOIN o ON n.a = o.a;

-- Pushdown predicate to HAVING (CNF)
SELECT x.cnt AS cnt FROM (SELECT COUNT(1) AS cnt FROM x AS x) AS x WHERE x.cnt > 0;
SELECT x.cnt AS cnt FROM (SELECT COUNT(1) AS cnt FROM x AS x HAVING COUNT(1) > 0) AS x WHERE TRUE;

-- Pushdown predicate to HAVING (DNF)
SELECT x.cnt AS cnt FROM (SELECT COUNT(1) AS cnt, COUNT(x.a) AS cnt_a, COUNT(x.b) AS cnt_b FROM x AS x) AS x WHERE (x.cnt_a > 0 AND x.cnt_b > 0) OR x.cnt > 0;
SELECT x.cnt AS cnt FROM (SELECT COUNT(1) AS cnt, COUNT(x.a) AS cnt_a, COUNT(x.b) AS cnt_b FROM x AS x HAVING COUNT(1) > 0 OR (COUNT(x.a) > 0 AND COUNT(x.b) > 0)) AS x WHERE x.cnt > 0 OR (x.cnt_a > 0 AND x.cnt_b > 0);
