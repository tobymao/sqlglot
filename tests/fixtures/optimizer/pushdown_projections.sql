SELECT a FROM (SELECT * FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0;

SELECT 1 FROM (SELECT * FROM x) WHERE b = 2;
SELECT 1 AS "1" FROM (SELECT x.b AS b FROM x AS x) AS _q_0 WHERE _q_0.b = 2;

SELECT a, b, a from x;
SELECT x.a AS a, x.b AS b, x.a AS a FROM x AS x;

SELECT (SELECT c FROM y WHERE q.b = y.b) FROM (SELECT * FROM x) AS q;
SELECT (SELECT y.c AS c FROM y AS y WHERE q.b = y.b) AS _col_0 FROM (SELECT x.b AS b FROM x AS x) AS q;

SELECT a FROM x JOIN (SELECT b, c FROM y) AS z ON x.b = z.b;
SELECT x.a AS a FROM x AS x JOIN (SELECT y.b AS b FROM y AS y) AS z ON x.b = z.b;

SELECT x1.a FROM (SELECT * FROM x) AS x1, (SELECT * FROM x) AS x2;
SELECT x1.a AS a FROM (SELECT x.a AS a FROM x AS x) AS x1, (SELECT 1 AS _ FROM x AS x) AS x2;

SELECT a FROM (SELECT DISTINCT a, b FROM x);
SELECT _q_0.a AS a FROM (SELECT DISTINCT x.a AS a, x.b AS b FROM x AS x) AS _q_0;

SELECT a FROM (SELECT a, b FROM x UNION ALL SELECT a, b FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x UNION ALL SELECT x.a AS a FROM x AS x) AS _q_0;

WITH t1 AS (SELECT x.a AS a, x.b AS b FROM x UNION ALL SELECT z.b AS b, z.c AS c FROM z) SELECT a, b FROM t1;
WITH t1 AS (SELECT x.a AS a, x.b AS b FROM x AS x UNION ALL SELECT z.b AS b, z.c AS c FROM z AS z) SELECT t1.a AS a, t1.b AS b FROM t1 AS t1;

SELECT a FROM (SELECT a, b FROM x UNION SELECT a, b FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x UNION SELECT x.a AS a, x.b AS b FROM x AS x) AS _q_0;

WITH y AS (SELECT * FROM x) SELECT a FROM y;
WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y;

WITH z AS (SELECT * FROM x), q AS (SELECT b FROM z) SELECT b FROM q;
WITH z AS (SELECT x.b AS b FROM x AS x), q AS (SELECT z.b AS b FROM z AS z) SELECT q.b AS b FROM q AS q;

WITH z AS (SELECT * FROM x) SELECT a FROM z UNION SELECT a FROM z;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT z.a AS a FROM z AS z UNION SELECT z.a AS a FROM z AS z;

SELECT b FROM (SELECT a, SUM(b) AS b FROM x GROUP BY a);
SELECT _q_0.b AS b FROM (SELECT SUM(x.b) AS b FROM x AS x GROUP BY x.a) AS _q_0;

SELECT b FROM (SELECT a, SUM(b) AS b FROM x ORDER BY a);
SELECT _q_0.b AS b FROM (SELECT x.a AS a, SUM(x.b) AS b FROM x AS x ORDER BY a) AS _q_0;

SELECT x FROM (VALUES(1, 2)) AS q(x, y);
SELECT q.x AS x FROM (VALUES (1, 2)) AS q(x, y);

SELECT x FROM UNNEST([1, 2]) AS q(x, y);
SELECT q.x AS x FROM UNNEST(ARRAY(1, 2)) AS q(x, y);

WITH t1 AS (SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb)]) AS "q"("cola", "colb")) SELECT cola FROM t1;
WITH t1 AS (SELECT "q".cola AS cola FROM UNNEST(ARRAY(STRUCT(1 AS cola, 'test' AS colb))) AS "q"("cola", "colb")) SELECT t1.cola AS cola FROM t1 AS t1;

SELECT x FROM VALUES(1, 2) AS q(x, y);
SELECT q.x AS x FROM (VALUES (1, 2)) AS q(x, y);

SELECT i.a FROM x AS i LEFT JOIN (SELECT a, b FROM (SELECT a, b FROM x)) AS j ON i.a = j.a;
SELECT i.a AS a FROM x AS i LEFT JOIN (SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0) AS j ON i.a = j.a;

WITH cte AS (SELECT source.a AS a, ROW_NUMBER() OVER (PARTITION BY source.id, source.timestamp ORDER BY source.a DESC) AS index FROM source AS source QUALIFY index) SELECT cte.a AS a FROM cte;
WITH cte AS (SELECT source.a AS a FROM source AS source QUALIFY ROW_NUMBER() OVER (PARTITION BY source.id, source.timestamp ORDER BY source.a DESC)) SELECT cte.a AS a FROM cte AS cte;

WITH cte AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT cte.a FROM cte AS cte(a);
WITH cte AS (SELECT 1 AS x) SELECT cte.a AS a FROM cte AS cte(a);

WITH cte(x, y, z) AS (SELECT 1, 2, 3) SELECT a, z FROM cte AS cte(a);
WITH cte AS (SELECT 1 AS x, 3 AS z) SELECT cte.a AS a, cte.z AS z FROM cte AS cte(a);

WITH cte(x, y, z) AS (SELECT 1, 2, 3) SELECT a, z FROM (SELECT * FROM cte AS cte(b)) AS cte(a);
WITH cte AS (SELECT 1 AS x, 3 AS z) SELECT cte.a AS a, cte.z AS z FROM (SELECT cte.b AS a, cte.z AS z FROM cte AS cte(b)) AS cte;

WITH y AS (SELECT a FROM x) SELECT 1 FROM y;
WITH y AS (SELECT 1 AS _ FROM x AS x) SELECT 1 AS "1" FROM y AS y;

WITH y AS (SELECT SUM(a) FROM x) SELECT 1 FROM y;
WITH y AS (SELECT MAX(1) AS _ FROM x AS x) SELECT 1 AS "1" FROM y AS y;

WITH y AS (SELECT a FROM x GROUP BY a) SELECT 1 FROM y;
WITH y AS (SELECT 1 AS _ FROM x AS x GROUP BY x.a) SELECT 1 AS "1" FROM y AS y;

--------------------------------------
-- Unknown Star Expansion
--------------------------------------

SELECT a FROM (SELECT * FROM zz) WHERE b = 1;
SELECT _q_0.a AS a FROM (SELECT zz.a AS a, zz.b AS b FROM zz AS zz) AS _q_0 WHERE _q_0.b = 1;

SELECT a FROM (SELECT * FROM aa UNION ALL SELECT * FROM bb UNION ALL SELECT * from cc);
SELECT _q_0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa UNION ALL SELECT bb.a AS a FROM bb AS bb UNION ALL SELECT cc.a AS a FROM cc AS cc) AS _q_0;

SELECT a FROM (SELECT a FROM aa UNION ALL SELECT * FROM bb UNION ALL SELECT * from cc);
SELECT _q_0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa UNION ALL SELECT bb.a AS a FROM bb AS bb UNION ALL SELECT cc.a AS a FROM cc AS cc) AS _q_0;

SELECT a FROM (SELECT * FROM aa CROSS JOIN bb);
SELECT _q_0.a AS a FROM (SELECT a AS a FROM aa AS aa CROSS JOIN bb AS bb) AS _q_0;

SELECT a FROM (SELECT aa.* FROM aa);
SELECT _q_0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa) AS _q_0;

SELECT a FROM (SELECT * FROM (SELECT * FROM aa));
SELECT _q_1.a AS a FROM (SELECT _q_0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa) AS _q_0) AS _q_1;

with cte1 as (SELECT cola, colb FROM tb UNION ALL SELECT colc, cold FROM tb2) SELECT cola FROM cte1;
WITH cte1 AS (SELECT tb.cola AS cola FROM tb AS tb UNION ALL SELECT tb2.colc AS colc FROM tb2 AS tb2) SELECT cte1.cola AS cola FROM cte1 AS cte1;

SELECT * FROM ((SELECT c FROM t1) JOIN t2);
SELECT * FROM ((SELECT t1.c AS c FROM t1 AS t1) AS _q_0, t2 AS t2);
