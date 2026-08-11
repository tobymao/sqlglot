SELECT a FROM (SELECT * FROM x);
SELECT _0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _0;

SELECT 1 FROM (SELECT * FROM x) WHERE b = 2;
SELECT 1 AS "1" FROM (SELECT x.b AS b FROM x AS x) AS _0 WHERE _0.b = 2;

SELECT a, b, a from x;
SELECT x.a AS a, x.b AS b, x.a AS a FROM x AS x;

SELECT (SELECT c FROM y WHERE q.b = y.b) FROM (SELECT * FROM x) AS q;
SELECT (SELECT y.c AS c FROM y AS y WHERE q.b = y.b) AS _col_0 FROM (SELECT x.b AS b FROM x AS x) AS q;

SELECT a FROM x JOIN (SELECT b, c FROM y) AS z ON x.b = z.b;
SELECT x.a AS a FROM x AS x JOIN (SELECT y.b AS b FROM y AS y) AS z ON x.b = z.b;

SELECT x1.a FROM (SELECT * FROM x) AS x1, (SELECT * FROM x) AS x2;
SELECT x1.a AS a FROM (SELECT x.a AS a FROM x AS x) AS x1, (SELECT 1 AS _ FROM x AS x) AS x2;

SELECT a FROM (SELECT DISTINCT a, b FROM x);
SELECT _0.a AS a FROM (SELECT DISTINCT x.a AS a, x.b AS b FROM x AS x) AS _0;

SELECT a FROM (SELECT a, b FROM x UNION ALL SELECT a, b FROM x);
SELECT _0.a AS a FROM (SELECT x.a AS a FROM x AS x UNION ALL SELECT x.a AS a FROM x AS x) AS _0;

WITH t1 AS (SELECT x.a AS a, x.b AS b FROM x UNION ALL SELECT z.b AS b, z.c AS c FROM z) SELECT a, b FROM t1;
WITH t1 AS (SELECT x.a AS a, x.b AS b FROM x AS x UNION ALL SELECT z.b AS b, z.c AS c FROM z AS z) SELECT t1.a AS a, t1.b AS b FROM t1 AS t1;

SELECT a FROM (SELECT a, b FROM x UNION SELECT a, b FROM x);
SELECT _0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x UNION SELECT x.a AS a, x.b AS b FROM x AS x) AS _0;

SELECT a FROM (SELECT a, b FROM x INTERSECT ALL SELECT a, b FROM x);
SELECT _0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x INTERSECT ALL SELECT x.a AS a, x.b AS b FROM x AS x) AS _0;

SELECT a FROM (SELECT a, b FROM x EXCEPT ALL SELECT a, b FROM x);
SELECT _0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x EXCEPT ALL SELECT x.a AS a, x.b AS b FROM x AS x) AS _0;

SELECT a FROM (SELECT a, b FROM x GROUP BY ALL);
SELECT _0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x GROUP BY ALL) AS _0;

WITH y AS (SELECT * FROM x) SELECT a FROM y;
WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y;

WITH z AS (SELECT * FROM x), q AS (SELECT b FROM z) SELECT b FROM q;
WITH z AS (SELECT x.b AS b FROM x AS x), q AS (SELECT z.b AS b FROM z AS z) SELECT q.b AS b FROM q AS q;

WITH z AS (SELECT * FROM x) SELECT a FROM z UNION SELECT a FROM z;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT z.a AS a FROM z AS z UNION SELECT z.a AS a FROM z AS z;

SELECT b FROM (SELECT a, SUM(b) AS b FROM x GROUP BY a);
SELECT _0.b AS b FROM (SELECT SUM(x.b) AS b FROM x AS x GROUP BY x.a) AS _0;

WITH x AS (SELECT 0 AS c, SUM(d) AS s FROM t GROUP BY 1) SELECT s FROM x;
WITH x AS (SELECT 0 AS c, SUM(t.d) AS s FROM t AS t GROUP BY 1) SELECT x.s AS s FROM x AS x;

WITH x AS (SELECT z, 0 AS c, a, SUM(d) AS s FROM t GROUP BY z, 2, a) SELECT c, a, s FROM x;
WITH x AS (SELECT 0 AS c, t.a AS a, SUM(t.d) AS s FROM t AS t GROUP BY t.z, 1, t.a) SELECT x.c AS c, x.a AS a, x.s AS s FROM x AS x;

WITH x AS (SELECT a, b, 0 AS c, SUM(d) AS s FROM t GROUP BY 1, 2, 3) SELECT a, s FROM x;
WITH x AS (SELECT t.a AS a, 0 AS c, SUM(t.d) AS s FROM t AS t GROUP BY t.a, t.b, 2) SELECT x.a AS a, x.s AS s FROM x AS x;

WITH x AS (SELECT z, 0 AS c, SUM(d) AS s FROM t GROUP BY z, 2, 2) SELECT c, s FROM x;
WITH x AS (SELECT 0 AS c, SUM(t.d) AS s FROM t AS t GROUP BY t.z, 1, 1) SELECT x.c AS c, x.s AS s FROM x AS x;

SELECT b FROM (SELECT a, SUM(b) AS b FROM x ORDER BY a);
SELECT _0.b AS b FROM (SELECT x.a AS a, SUM(x.b) AS b FROM x AS x ORDER BY a) AS _0;

SELECT x FROM (VALUES(1, 2)) AS q(x, y);
SELECT q.x AS x FROM (VALUES (1, 2)) AS q(x, y);

SELECT x FROM UNNEST([1, 2]) AS q(x, y);
SELECT q.x AS x FROM UNNEST(ARRAY(1, 2)) AS q(x, y);

WITH t1 AS (SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, 'test' AS colb)]) AS "q"("cola", "colb")) SELECT cola FROM t1;
WITH t1 AS (SELECT "q".cola AS cola FROM UNNEST(ARRAY(STRUCT(1 AS cola, 'test' AS colb))) AS "q"("cola", "colb")) SELECT t1.cola AS cola FROM t1 AS t1;

SELECT x FROM VALUES(1, 2) AS q(x, y);
SELECT q.x AS x FROM (VALUES (1, 2)) AS q(x, y);

SELECT i.a FROM x AS i LEFT JOIN (SELECT a, b FROM (SELECT a, b FROM x)) AS j ON i.a = j.a;
SELECT i.a AS a FROM x AS i LEFT JOIN (SELECT _0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _0) AS j ON i.a = j.a;

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

WITH cte AS (SELECT col FROM t) SELECT IF(1 IN UNNEST(col), 1, 0) AS col FROM cte;
WITH cte AS (SELECT t.col AS col FROM t AS t) SELECT CASE WHEN 1 IN (SELECT UNNEST(cte.col)) THEN 1 ELSE 0 END AS col FROM cte AS cte;

--------------------------------------
-- Unknown Star Expansion
--------------------------------------

SELECT a FROM (SELECT * FROM zz) WHERE b = 1;
SELECT _0.a AS a FROM (SELECT zz.a AS a, zz.b AS b FROM zz AS zz) AS _0 WHERE _0.b = 1;

SELECT a FROM (SELECT * FROM aa UNION ALL SELECT * FROM bb UNION ALL SELECT * from cc);
SELECT _0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa UNION ALL SELECT bb.a AS a FROM bb AS bb UNION ALL SELECT cc.a AS a FROM cc AS cc) AS _0;

SELECT a FROM (SELECT a FROM aa UNION ALL SELECT * FROM bb UNION ALL SELECT * from cc);
SELECT _0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa UNION ALL SELECT bb.a AS a FROM bb AS bb UNION ALL SELECT cc.a AS a FROM cc AS cc) AS _0;

SELECT a FROM (SELECT * FROM aa CROSS JOIN bb);
SELECT _0.a AS a FROM (SELECT a AS a FROM aa AS aa CROSS JOIN bb AS bb) AS _0;

SELECT a FROM (SELECT aa.* FROM aa);
SELECT _0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa) AS _0;

SELECT a FROM (SELECT * FROM (SELECT * FROM aa));
SELECT _1.a AS a FROM (SELECT _0.a AS a FROM (SELECT aa.a AS a FROM aa AS aa) AS _0) AS _1;

with cte1 as (SELECT cola, colb FROM tb UNION ALL SELECT colc, cold FROM tb2) SELECT cola FROM cte1;
WITH cte1 AS (SELECT tb.cola AS cola FROM tb AS tb UNION ALL SELECT tb2.colc AS colc FROM tb2 AS tb2) SELECT cte1.cola AS cola FROM cte1 AS cte1;

SELECT * FROM ((SELECT c FROM t1) JOIN t2);
SELECT * FROM ((SELECT t1.c AS c FROM t1 AS t1) AS _0, t2 AS t2);

SELECT a, d FROM (SELECT 1 a, 2 c, 3 d, 4 e UNION ALL BY NAME SELECT 6 c, 7 d, 8 a, 9 e);
SELECT _0.a AS a, _0.d AS d FROM (SELECT 1 AS a, 3 AS d UNION ALL BY NAME SELECT 7 AS d, 8 AS a) AS _0;

SELECT a, b FROM (WITH cte1 AS (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) (SELECT a, b, c FROM cte1));
SELECT _0.a AS a, _0.b AS b FROM (WITH cte1 AS (SELECT 1 AS a, 2 AS b) SELECT cte1.a AS a, cte1.b AS b FROM cte1 AS cte1) AS _0;

--------------------------------------
-- Star used by a function
--------------------------------------

# dialect: snowflake
SELECT OBJECT_CONSTRUCT(*) FROM (SELECT a, b FROM x) AS t;
SELECT OBJECT_CONSTRUCT(*) AS _COL_0 FROM (SELECT a AS a, b AS b FROM x AS x) AS t;

# dialect: snowflake
WITH base AS (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) SELECT OBJECT_INSERT(OBJECT_CONSTRUCT(*), 'e', 5) FROM base;
WITH base AS (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) SELECT OBJECT_INSERT(OBJECT_CONSTRUCT(*), 'e', 5) AS _COL_0 FROM base AS base;

# dialect: snowflake
WITH base AS (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) SELECT obj:A, obj:B FROM (SELECT OBJECT_INSERT(OBJECT_CONSTRUCT(*), 'e', 5) AS obj, a FROM base) AS t;
WITH base AS (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) SELECT GET_PATH(t.obj, 'A') AS A, GET_PATH(t.obj, 'B') AS B FROM (SELECT OBJECT_INSERT(OBJECT_CONSTRUCT(*), 'e', 5) AS obj FROM base AS base) AS t;

# dialect: snowflake
WITH cte AS (SELECT 1 AS a, 2 as b) SELECT HASH_AGG(*) FROM cte;
WITH cte AS (SELECT 1 AS a, 2 AS b) SELECT HASH_AGG(*) AS _COL_0 FROM cte AS cte;

# dialect: snowflake
WITH cte AS (SELECT a, b FROM x) SELECT COUNT(* EXCLUDE a) FROM cte;
WITH cte AS (SELECT a AS a, b AS b FROM x AS x) SELECT COUNT(* EXCLUDE (a)) AS _COL_0 FROM cte AS cte;

WITH cte1 AS (SELECT a, SUM(b) AS sale FROM x GROUP BY a), cte2 AS (SELECT cte1.a, COUNT(*) AS cnt FROM cte1 GROUP BY cte1.a) SELECT a, cnt FROM cte2;
WITH cte1 AS (SELECT x.a AS a FROM x AS x GROUP BY x.a), cte2 AS (SELECT cte1.a AS a, COUNT(*) AS cnt FROM cte1 AS cte1 GROUP BY cte1.a) SELECT cte2.a AS a, cte2.cnt AS cnt FROM cte2 AS cte2;

--------------------------------------
-- Set-returning functions affect cardinality and are retained even when unused
--------------------------------------
SELECT d FROM (SELECT EXPLODE(e) AS col, d FROM w);
SELECT _0.d AS d FROM (SELECT EXPLODE(w.e) AS col, w.d AS d FROM w AS w) AS _0;

SELECT d FROM (SELECT POSEXPLODE(e) AS col, d FROM w);
SELECT _0.d AS d FROM (SELECT POSEXPLODE(w.e) AS col, w.d AS d FROM w AS w) AS _0;

SELECT d FROM (SELECT INLINE(e) AS col, d FROM w);
SELECT _0.d AS d FROM (SELECT INLINE(w.e) AS col, w.d AS d FROM w AS w) AS _0;

-- Window functions do not affect cardinality and stay prunable
SELECT d FROM (SELECT d, ROW_NUMBER() OVER (PARTITION BY e ORDER BY d) AS rn FROM w);
SELECT _0.d AS d FROM (SELECT w.d AS d FROM w AS w) AS _0;

# dialect: bigquery
# title: a GROUP BY / HAVING column shadowed by a colliding projection alias is kept by pushdown
SELECT t.n FROM (SELECT a, ARRAY_AGG(b) AS agg, COUNT(*) AS n FROM (SELECT a, b FROM x) AS agg GROUP BY a HAVING a >= 1) AS t;
SELECT t.n AS n FROM (SELECT COUNT(*) AS n FROM (SELECT x.a AS a FROM x AS x) AS agg GROUP BY a HAVING a >= 1) AS t;

# dialect: bigquery
# title: shadowed HAVING column left unqualified by qualify's early return is kept by pushdown
SELECT t.n FROM (SELECT ARRAY_AGG(b) AS agg, COUNT(*) AS n FROM (SELECT a, b FROM x) AS agg GROUP BY a HAVING a >= 1 AND SUM(b) > 0) AS t;
SELECT t.n AS n FROM (SELECT COUNT(*) AS n FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS agg GROUP BY a HAVING a >= 1 AND SUM(b) > 0) AS t;

# dialect: bigquery
# title: shadowed column referenced only in QUALIFY is kept by pushdown
SELECT t.rn FROM (SELECT ARRAY_AGG(b) OVER (PARTITION BY b) AS agg, ROW_NUMBER() OVER (ORDER BY b) AS rn FROM (SELECT a, b FROM x) AS agg QUALIFY a >= 1) AS t;
SELECT t.rn AS rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY agg.b) AS rn FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS agg QUALIFY a >= 1) AS t;

# dialect: bigquery
# title: shadowed clause column is pushed down into a CTE
WITH agg AS (SELECT a, b FROM x) SELECT t.n FROM (SELECT ARRAY_AGG(b) AS agg, COUNT(*) AS n FROM agg GROUP BY a HAVING a >= 1) AS t;
WITH agg AS (SELECT x.a AS a FROM x AS x) SELECT t.n AS n FROM (SELECT COUNT(*) AS n FROM agg AS agg GROUP BY a HAVING a >= 1) AS t;

# dialect: bigquery
# title: shadowed clause column resolves to the correct side of a join
SELECT t.n FROM (SELECT ARRAY_AGG(q.b) AS q, COUNT(*) AS n FROM (SELECT a, b FROM x) AS q CROSS JOIN (SELECT c FROM y) AS r GROUP BY a HAVING a > 0) AS t;
SELECT t.n AS n FROM (SELECT COUNT(*) AS n FROM (SELECT x.a AS a FROM x AS x) AS q CROSS JOIN (SELECT 1 AS _ FROM y AS y) AS r GROUP BY a HAVING a > 0) AS t;

--------------------------------------
-- GROUP BY ALL implicitly groups by every non-aggregate projection, so those
-- projections must be retained even when unreferenced by the outer scope
--------------------------------------
SELECT t.a FROM (SELECT a, b, SUM(b) AS s FROM x GROUP BY ALL) t;
SELECT t.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x GROUP BY ALL) AS t;

-- An implicit key can be a non-column expression, not just a bare column
SELECT t.a, t.c FROM (SELECT a, b, b + 1 AS c, SUM(a) AS s FROM x GROUP BY ALL) t;
SELECT t.a AS a, t.c AS c FROM (SELECT x.a AS a, x.b AS b, x.b + 1 AS c FROM x AS x GROUP BY ALL) AS t;

-- ALL is a grouping-sets modifier (not implicit-key inference) once CUBE/ROLLUP/an explicit
-- list is present, so those columns stay prunable like any other explicit GROUP BY column
SELECT t.s FROM (SELECT a, b, SUM(b) AS s FROM x GROUP BY ALL CUBE (a, b)) t;
SELECT t.s AS s FROM (SELECT SUM(x.b) AS s FROM x AS x GROUP BY ALL CUBE (x.a, x.b)) AS t;

SELECT t.s FROM (SELECT a, b, SUM(b) AS s FROM x GROUP BY ALL a, b) t;
SELECT t.s AS s FROM (SELECT SUM(x.b) AS s FROM x AS x GROUP BY ALL x.a, x.b) AS t;
