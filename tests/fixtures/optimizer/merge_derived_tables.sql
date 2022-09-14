-- Simple
SELECT a, b FROM (SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x;

-- Inner table alias is merged
SELECT a, b FROM (SELECT a, b FROM x AS q) AS r;
SELECT q.a AS a, q.b AS b FROM x AS q;

-- Double nesting
SELECT a, b FROM (SELECT a, b FROM (SELECT a, b FROM x));
SELECT x.a AS a, x.b AS b FROM x;

-- WHERE clause is merged
SELECT a, SUM(b) FROM (SELECT a, b FROM x WHERE a > 1) GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS "_col_1" FROM x WHERE x.a > 1 GROUP BY x.a;

-- Outer query has join
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x JOIN y ON x.b = y.b WHERE x.a > 1;

-- Join on derived table
SELECT a, c FROM x JOIN (SELECT b, c FROM y) AS y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x JOIN y ON x.b = y.b;

-- Inner query has a join
SELECT a, c FROM (SELECT a, c FROM x JOIN y ON x.b = y.b);
SELECT x.a AS a, y.c AS c FROM x JOIN y ON x.b = y.b;

-- Inner query has conflicting name in outer query
SELECT a, c FROM (SELECT q.a, q.b FROM x AS q) AS x JOIN y AS q ON x.b = q.b;
SELECT q_2.a AS a, q.c AS c FROM x AS q_2 JOIN y AS q ON q_2.b = q.b;

-- Inner query has conflicting name in joined source
SELECT x.a, q.c FROM (SELECT a, x.b FROM x JOIN y AS q ON x.b = q.b) AS x JOIN y AS q ON x.b = q.b;
SELECT x.a AS a, q.c AS c FROM x JOIN y AS q_2 ON x.b = q_2.b JOIN y AS q ON x.b = q.b;

-- Inner query has multiple conflicting names
SELECT x.a, q.c, r.c FROM (SELECT q.a, r.b FROM x AS q JOIN y AS r ON q.b = r.b) AS x JOIN y AS q ON x.b = q.b JOIN y AS r ON x.b = r.b;
SELECT q_2.a AS a, q.c AS c, r.c AS c FROM x AS q_2 JOIN y AS r_2 ON q_2.b = r_2.b JOIN y AS q ON r_2.b = q.b JOIN y AS r ON r_2.b = r.b;

-- Inner queries have conflicting names with each other
SELECT r.b FROM (SELECT b FROM x AS x) AS q JOIN (SELECT b FROM x) AS r ON q.b = r.b;
SELECT x_2.b AS b FROM x AS x JOIN x AS x_2 ON x.b = x_2.b;

-- WHERE clause in joined derived table is merged
SELECT x.a, y.c FROM x JOIN (SELECT b, c FROM y WHERE c > 1) AS y;
SELECT x.a AS a, y.c AS c FROM x JOIN y WHERE y.c > 1;

-- WHERE clause in outer joined derived table is merged to ON clause
SELECT x.a, y.c FROM x LEFT JOIN (SELECT b, c FROM y WHERE c > 1) AS y;
SELECT x.a AS a, y.c AS c FROM x LEFT JOIN y ON y.c > 1;

-- Comma JOIN in outer query
SELECT x.a, y.c FROM (SELECT a FROM x) AS x, (SELECT c FROM y) AS y;
SELECT x.a AS a, y.c AS c FROM x, y;

-- Comma JOIN in inner query
SELECT x.a, x.c FROM (SELECT x.a, z.c FROM x, y AS z) AS x;
SELECT x.a AS a, z.c AS c FROM x CROSS JOIN y AS z;
