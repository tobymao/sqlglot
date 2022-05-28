--------------------------------------
-- Qualify columns
--------------------------------------
SELECT a FROM x;
SELECT x.a AS a FROM x;

SELECT a FROM x AS z;
SELECT z.a AS a FROM x AS z;

SELECT a AS a FROM x;
SELECT x.a AS a FROM x;

SELECT x.a FROM x;
SELECT x.a AS a FROM x;

SELECT x.a AS a FROM x;
SELECT x.a AS a FROM x;

SELECT a AS b FROM x;
SELECT x.a AS b FROM x;

SELECT 1, 2 FROM x;
SELECT 1 AS "_col_0", 2 AS "_col_1" FROM x;

SELECT a + b FROM x;
SELECT x.a + x.b AS "_col_0" FROM x;

SELECT a + b FROM x;
SELECT x.a + x.b AS "_col_0" FROM x;

SELECT a, SUM(b) FROM x WHERE a > 1 AND b > 1 GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS "_col_1" FROM x WHERE x.a > 1 AND x.b > 1 GROUP BY a;

SELECT a AS j, b FROM x ORDER BY j;
SELECT x.a AS j, x.b AS b FROM x ORDER BY j;

SELECT a AS j, b FROM x GROUP BY j;
SELECT x.a AS j, x.b AS b FROM x GROUP BY j;

SELECT a AS a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x ORDER BY a;

SELECT a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x ORDER BY a;

--------------------------------------
-- Derived tables
--------------------------------------
SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y;
SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y;

SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y(a);
SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y;

SELECT y.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x) AS y(c);
SELECT y.c AS c FROM (SELECT x.a AS c, x.b AS b FROM x) AS y;

SELECT a FROM (SELECT a FROM x) y;
SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y;

SELECT a FROM (SELECT a AS a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x) AS "_q_0";

SELECT a FROM (SELECT a FROM (SELECT a FROM x));
SELECT "_q_1".a AS a FROM (SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x) AS "_q_0") AS "_q_1";

SELECT x.a FROM x AS x JOIN (SELECT * FROM x);
SELECT x.a AS a FROM x AS x JOIN (SELECT x.a AS a, x.b AS b FROM x) AS "_q_0";

--------------------------------------
-- Joins
--------------------------------------
SELECT a, c FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x JOIN y ON x.b = y.b;

SELECT a, c FROM x, y;
SELECT x.a AS a, y.c AS c FROM x, y;

--------------------------------------
-- Unions
--------------------------------------
SELECT a FROM x UNION SELECT a FROM x;
SELECT x.a AS a FROM x UNION SELECT x.a AS a FROM x;

SELECT a FROM x UNION SELECT a FROM x UNION SELECT a FROM x;
SELECT x.a AS a FROM x UNION SELECT x.a AS a FROM x UNION SELECT x.a AS a FROM x;

SELECT a FROM (SELECT a FROM x UNION SELECT a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x UNION SELECT x.a AS a FROM x) AS "_q_0";

--------------------------------------
-- Subqueries
--------------------------------------
SELECT a FROM x WHERE b IN (SELECT c FROM y);
SELECT x.a AS a FROM x WHERE x.b IN (SELECT y.c AS c FROM y);

SELECT (SELECT c FROM y) FROM x;
SELECT (SELECT y.c AS c FROM y) AS "_col_0" FROM x;

SELECT a FROM (SELECT a FROM x) WHERE a IN (SELECT b FROM (SELECT b FROM y));
SELECT "_q_1".a AS a FROM (SELECT x.a AS a FROM x) AS "_q_1" WHERE "_q_1".a IN (SELECT "_q_0".b AS b FROM (SELECT y.b AS b FROM y) AS "_q_0");

--------------------------------------
-- Correlated subqueries
--------------------------------------
SELECT a FROM x WHERE b IN (SELECT c FROM y WHERE y.b = x.a);
SELECT x.a AS a FROM x WHERE x.b IN (SELECT y.c AS c FROM y WHERE y.b = x.a);

SELECT a FROM x WHERE b IN (SELECT c FROM y WHERE y.b = a);
SELECT x.a AS a FROM x WHERE x.b IN (SELECT y.c AS c FROM y WHERE y.b = x.a);

SELECT a FROM x WHERE b IN (SELECT b FROM y AS x);
SELECT x.a AS a FROM x WHERE x.b IN (SELECT x.b AS b FROM y AS x);

--------------------------------------
-- Expand *
--------------------------------------
SELECT * FROM x;
SELECT x.a AS a, x.b AS b FROM x;

SELECT x.* FROM x;
SELECT x.a AS a, x.b AS b FROM x;

SELECT * FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b, y.b AS b, y.c AS c FROM x JOIN y ON x.b = y.b;

SELECT x.* FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b FROM x JOIN y ON x.b = y.b;

SELECT x.*, y.* FROM x JOIN y ON x.b = y.b;
SELECT x.a AS a, x.b AS b, y.b AS b, y.c AS c FROM x JOIN y ON x.b = y.b;

SELECT a FROM (SELECT * FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS "_q_0";

SELECT * FROM (SELECT a FROM x);
SELECT "_q_0".a AS a FROM (SELECT x.a AS a FROM x) AS "_q_0";

--------------------------------------
-- CTEs
--------------------------------------
WITH z AS (SELECT x.a AS a FROM x) SELECT z.a AS a FROM z;
WITH z AS (SELECT x.a AS a FROM x) SELECT z.a AS a FROM z;

WITH z(a) AS (SELECT a FROM x) SELECT * FROM z;
WITH z AS (SELECT x.a AS a FROM x) SELECT z.a AS a FROM z;

WITH z AS (SELECT a FROM x) SELECT * FROM z as q;
WITH z AS (SELECT x.a AS a FROM x) SELECT q.a AS a FROM z AS q;

WITH z AS (SELECT a FROM x) SELECT * FROM z;
WITH z AS (SELECT x.a AS a FROM x) SELECT z.a AS a FROM z;

WITH z AS (SELECT a FROM x), q AS (SELECT * FROM z) SELECT * FROM q;
WITH z AS (SELECT x.a AS a FROM x), q AS (SELECT z.a AS a FROM z) SELECT q.a AS a FROM q;

WITH z AS (SELECT * FROM x) SELECT * FROM z UNION SELECT * FROM z;
WITH z AS (SELECT x.a AS a, x.b AS b FROM x) SELECT z.a AS a, z.b AS b FROM z UNION SELECT z.a AS a, z.b AS b FROM z;

WITH z AS (SELECT * FROM x), q AS (SELECT b FROM z) SELECT b FROM q;
WITH z AS (SELECT x.a AS a, x.b AS b FROM x), q AS (SELECT z.b AS b FROM z) SELECT q.b AS b FROM q;

--------------------------------------
-- TODO: Laterals
--------------------------------------

--------------------------------------
-- TODO: Recursive CTEs
--------------------------------------
