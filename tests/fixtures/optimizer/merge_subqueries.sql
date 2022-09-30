-- Simple
SELECT a, b FROM (SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x AS x;

-- Inner table alias is merged
SELECT a, b FROM (SELECT a, b FROM x AS q) AS r;
SELECT q.a AS a, q.b AS b FROM x AS q;

-- Double nesting
SELECT a, b FROM (SELECT a, b FROM (SELECT a, b FROM x));
SELECT x.a AS a, x.b AS b FROM x AS x;

-- WHERE clause is merged
SELECT a, SUM(b) FROM (SELECT a, b FROM x WHERE a > 1) GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS "_col_1" FROM x AS x WHERE x.a > 1 GROUP BY x.a;

-- Outer query has join
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

-- Outer query has join
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

# leave_tables_isolated: true
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.a > 1) AS x JOIN y AS y ON x.b = y.b;

-- Join on derived table
SELECT a, c FROM x JOIN (SELECT b, c FROM y) AS y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

-- Inner query has a join
SELECT a, c FROM (SELECT a, c FROM x JOIN y ON x.b = y.b);
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

-- Inner query has conflicting name in outer query
SELECT a, c FROM (SELECT q.a, q.b FROM x AS q) AS x JOIN y AS q ON x.b = q.b;
SELECT q_2.a AS a, q.c AS c FROM x AS q_2 JOIN y AS q ON q_2.b = q.b;

-- Inner query has conflicting name in joined source
SELECT x.a, q.c FROM (SELECT a, x.b FROM x JOIN y AS q ON x.b = q.b) AS x JOIN y AS q ON x.b = q.b;
SELECT x.a AS a, q.c AS c FROM x AS x JOIN y AS q_2 ON x.b = q_2.b JOIN y AS q ON x.b = q.b;

-- Inner query has multiple conflicting names
SELECT x.a, q.c, r.c FROM (SELECT q.a, r.b FROM x AS q JOIN y AS r ON q.b = r.b) AS x JOIN y AS q ON x.b = q.b JOIN y AS r ON x.b = r.b;
SELECT q_2.a AS a, q.c AS c, r.c AS c FROM x AS q_2 JOIN y AS r_2 ON q_2.b = r_2.b JOIN y AS q ON r_2.b = q.b JOIN y AS r ON r_2.b = r.b;

-- Inner queries have conflicting names with each other
SELECT r.b FROM (SELECT b FROM x AS x) AS q JOIN (SELECT b FROM x) AS r ON q.b = r.b;
SELECT x_2.b AS b FROM x AS x JOIN x AS x_2 ON x.b = x_2.b;

-- WHERE clause in joined derived table is merged to ON clause
SELECT x.a, y.c FROM x JOIN (SELECT b, c FROM y WHERE c > 1) AS y;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON y.c > 1;

-- Comma JOIN in outer query
SELECT x.a, y.c FROM (SELECT a FROM x) AS x, (SELECT c FROM y) AS y;
SELECT x.a AS a, y.c AS c FROM x AS x, y AS y;

-- Comma JOIN in inner query
SELECT x.a, x.c FROM (SELECT x.a, z.c FROM x, y AS z) AS x;
SELECT x.a AS a, z.c AS c FROM x AS x CROSS JOIN y AS z;

-- (Regression) Column in ORDER BY
SELECT * FROM (SELECT * FROM (SELECT * FROM x)) ORDER BY a LIMIT 1;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY x.a LIMIT 1;

-- CTE
WITH x AS (SELECT a, b FROM x) SELECT a, b FROM x;
SELECT x.a AS a, x.b AS b FROM x AS x;

-- CTE with outer table alias
WITH y AS (SELECT a, b FROM x) SELECT a, b FROM y AS z;
SELECT x.a AS a, x.b AS b FROM x AS x;

-- Nested CTE
WITH x AS (SELECT a FROM x), x2 AS (SELECT a FROM x) SELECT a FROM x2;
SELECT x.a AS a FROM x AS x;

-- CTE WHERE clause is merged
WITH x AS (SELECT a, b FROM x WHERE a > 1) SELECT a, SUM(b) FROM x GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS "_col_1" FROM x AS x WHERE x.a > 1 GROUP BY x.a;

-- CTE Outer query has join
WITH x AS (SELECT a, b FROM x WHERE a > 1) SELECT a, c FROM x AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

-- CTE with inner table alias
WITH y AS (SELECT a, b FROM x AS q) SELECT a, b FROM y AS z;
SELECT q.a AS a, q.b AS b FROM x AS q;

-- Duplicate queries to CTE
WITH x AS (SELECT a, b FROM x) SELECT x.a, y.b FROM x JOIN x AS y;
WITH x AS (SELECT x.a AS a, x.b AS b FROM x AS x) SELECT x.a AS a, y.b AS b FROM x JOIN x AS y;

-- Nested CTE
SELECT * FROM (WITH x AS (SELECT a, b FROM x) SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x AS x;

-- Inner select is an expression
SELECT a FROM (SELECT a FROM (SELECT COALESCE(a) AS a FROM x LEFT JOIN y ON x.a = y.b) AS x) AS x;
SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b;

-- CTE select is an expression
WITH x AS (SELECT COALESCE(a) AS a FROM x LEFT JOIN y ON x.a = y.b) SELECT a FROM (SELECT a FROM x AS x) AS x;
SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b;
