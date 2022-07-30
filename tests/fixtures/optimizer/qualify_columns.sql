--------------------------------------
-- Qualify columns
--------------------------------------
SELECT a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT a FROM x AS z;
SELECT z.a AS a FROM x AS z;

SELECT a AS a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT x.a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT x.a AS a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT a AS b FROM x;
SELECT x.a AS b FROM x AS x;

SELECT 1, 2 FROM x;
SELECT 1 AS "_col_0", 2 AS "_col_1" FROM x AS x;

SELECT a + b FROM x;
SELECT x.a + x.b AS "_col_0" FROM x AS x;

SELECT a + b FROM x;
SELECT x.a + x.b AS "_col_0" FROM x AS x;

SELECT a, SUM(b) FROM x WHERE a > 1 AND b > 1 GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS "_col_1" FROM x AS x WHERE x.a > 1 AND x.b > 1 GROUP BY a;

SELECT a AS j, b FROM x ORDER BY j;
SELECT x.a AS j, x.b AS b FROM x AS x ORDER BY j;

SELECT a AS j, b FROM x GROUP BY j;
SELECT x.a AS j, x.b AS b FROM x AS x GROUP BY j;

SELECT a AS a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY a;

SELECT a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY a;

SELECT a FROM x ORDER BY b;
SELECT x.a AS a FROM x AS x ORDER BY x.b;

# dialect: bigquery
SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS row_num FROM x QUALIFY row_num = 1;
SELECT ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS row_num FROM x AS x QUALIFY row_num = 1;

# dialect: bigquery
SELECT x.b, x.a FROM x LEFT JOIN y ON x.b = y.b QUALIFY ROW_NUMBER() OVER(PARTITION BY b ORDER BY a DESC) = 1;
SELECT x.b AS b, x.a AS a FROM x AS x LEFT JOIN y AS y ON x.b = y.b QUALIFY ROW_NUMBER() OVER (PARTITION BY b ORDER BY a DESC) = 1;

--------------------------------------
-- Derived tables
--------------------------------------
SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y;
SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y;

SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y(a);
SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y;

SELECT y.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS y(c);
SELECT y.c AS c FROM (SELECT x.a AS c, x.b AS b FROM x AS x) AS y;

SELECT a FROM (SELECT a FROM x AS x) y;
SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y;

SELECT a FROM (SELECT a AS a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x AS x) AS "_q_0";

SELECT a FROM (SELECT a FROM (SELECT a FROM x));
SELECT "_q_1".a AS a FROM (SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x AS x) AS "_q_0") AS "_q_1";

SELECT x.a FROM x AS x JOIN (SELECT * FROM x);
SELECT x.a AS a FROM x AS x JOIN (SELECT x.a AS a, x.b AS b FROM x AS x) AS "_q_0";

--------------------------------------
-- Joins
--------------------------------------
SELECT a, c FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT a, c FROM x, y;
SELECT x.a AS a, y.c AS c FROM x AS x, y AS y;

--------------------------------------
-- Unions
--------------------------------------
SELECT a FROM x UNION SELECT a FROM x;
SELECT x.a AS a FROM x AS x UNION SELECT x.a AS a FROM x AS x;

SELECT a FROM x UNION SELECT a FROM x UNION SELECT a FROM x;
SELECT x.a AS a FROM x AS x UNION SELECT x.a AS a FROM x AS x UNION SELECT x.a AS a FROM x AS x;

SELECT a FROM (SELECT a FROM x UNION SELECT a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x AS x UNION SELECT x.a AS a FROM x AS x) AS "_q_0";

--------------------------------------
-- Subqueries
--------------------------------------
SELECT a FROM x WHERE b IN (SELECT c FROM y);
SELECT x.a AS a FROM x AS x WHERE x.b IN (SELECT y.c AS c FROM y AS y);

SELECT (SELECT c FROM y) FROM x;
SELECT (SELECT y.c AS c FROM y AS y) AS "_col_0" FROM x AS x;

SELECT a FROM (SELECT a FROM x) WHERE a IN (SELECT b FROM (SELECT b FROM y));
SELECT "_q_1".a AS a FROM (SELECT x.a AS a FROM x AS x) AS "_q_1" WHERE "_q_1".a IN (SELECT "_q_0".b AS b FROM (SELECT y.b AS b FROM y AS y) AS "_q_0");

--------------------------------------
-- Correlated subqueries
--------------------------------------
SELECT a FROM x WHERE b IN (SELECT c FROM y WHERE y.b = x.a);
SELECT x.a AS a FROM x AS x WHERE x.b IN (SELECT y.c AS c FROM y AS y WHERE y.b = x.a);

SELECT a FROM x WHERE b IN (SELECT c FROM y WHERE y.b = a);
SELECT x.a AS a FROM x AS x WHERE x.b IN (SELECT y.c AS c FROM y AS y WHERE y.b = x.a);

SELECT a FROM x WHERE b IN (SELECT b FROM y AS x);
SELECT x.a AS a FROM x AS x WHERE x.b IN (SELECT x.b AS b FROM y AS x);

SELECT a FROM x AS i WHERE b IN (SELECT b FROM y AS j WHERE j.b IN (SELECT c FROM y AS k WHERE k.b = j.b));
SELECT i.a AS a FROM x AS i WHERE i.b IN (SELECT j.b AS b FROM y AS j WHERE j.b IN (SELECT k.c AS c FROM y AS k WHERE k.b = j.b));

# dialect: bigquery
SELECT aa FROM x, UNNEST(a) AS aa;
SELECT aa AS aa FROM x AS x, UNNEST(x.a) AS aa;

SELECT aa FROM x, UNNEST(a) AS t(aa);
SELECT t.aa AS aa FROM x AS x, UNNEST(x.a) AS t(aa);

--------------------------------------
-- Expand *
--------------------------------------
SELECT * FROM x;
SELECT x.a AS a, x.b AS b FROM x AS x;

SELECT x.* FROM x;
SELECT x.a AS a, x.b AS b FROM x AS x;

SELECT * FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b, y.b AS b, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT x.* FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT x.*, y.* FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b, y.b AS b, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT a FROM (SELECT * FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS "_q_0";

SELECT * FROM (SELECT a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x AS x) AS "_q_0";

--------------------------------------
-- CTEs
--------------------------------------
WITH z AS (SELECT x.a AS a FROM x) SELECT z.a AS a FROM z;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT z.a AS a FROM z;

WITH z(a) AS (SELECT a FROM x) SELECT * FROM z;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT z.a AS a FROM z;

WITH z AS (SELECT a FROM x) SELECT * FROM z as q;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT q.a AS a FROM z AS q;

WITH z AS (SELECT a FROM x) SELECT * FROM z;
WITH z AS (SELECT x.a AS a FROM x AS x) SELECT z.a AS a FROM z;

WITH z AS (SELECT a FROM x), q AS (SELECT * FROM z) SELECT * FROM q;
WITH z AS (SELECT x.a AS a FROM x AS x), q AS (SELECT z.a AS a FROM z) SELECT q.a AS a FROM q;

WITH z AS (SELECT * FROM x) SELECT * FROM z UNION SELECT * FROM z;
WITH z AS (SELECT x.a AS a, x.b AS b FROM x AS x) SELECT z.a AS a, z.b AS b FROM z UNION SELECT z.a AS a, z.b AS b FROM z;

WITH z AS (SELECT * FROM x), q AS (SELECT b FROM z) SELECT b FROM q;
WITH z AS (SELECT x.a AS a, x.b AS b FROM x AS x), q AS (SELECT z.b AS b FROM z) SELECT q.b AS b FROM q;

WITH z AS ((SELECT b FROM x UNION ALL SELECT b FROM y) ORDER BY b) SELECT * FROM z;
WITH z AS ((SELECT x.b AS b FROM x AS x UNION ALL SELECT y.b AS b FROM y AS y) ORDER BY b) SELECT z.b AS b FROM z;

--------------------------------------
-- Except and Replace
--------------------------------------
SELECT * REPLACE(a AS d) FROM x;
SELECT x.a AS d, x.b AS b FROM x AS x;

SELECT * EXCEPT(b) REPLACE(a AS d) FROM x;
SELECT x.a AS d FROM x AS x;

SELECT x.* EXCEPT(a), y.* FROM x, y;
SELECT x.b AS b, y.b AS b, y.c AS c FROM x AS x, y AS y;

SELECT * EXCEPT(a) FROM x;
SELECT x.b AS b FROM x AS x;

--------------------------------------
-- Using
--------------------------------------
SELECT x.b FROM x JOIN y USING (b);
SELECT x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT x.b FROM x JOIN y USING (b) JOIN z USING (b);
SELECT x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b JOIN z AS z ON x.b = z.b;

SELECT b FROM x AS x2 JOIN y AS y2 USING (b);
SELECT COALESCE(x2.b, y2.b) AS b FROM x AS x2 JOIN y AS y2 ON x2.b = y2.b;

SELECT b FROM x JOIN y USING (b) WHERE b = 1 and y.b = 2;
SELECT COALESCE(x.b, y.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b WHERE COALESCE(x.b, y.b) = 1 AND y.b = 2;

SELECT b FROM x JOIN y USING (b) JOIN z USING (b);
SELECT COALESCE(x.b, y.b, z.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b JOIN z AS z ON x.b = z.b;
