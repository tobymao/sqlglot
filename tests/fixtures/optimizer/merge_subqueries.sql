# title: Simple
SELECT a, b FROM (SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: Inner table alias is merged
SELECT a, b FROM (SELECT a, b FROM x AS q) AS r;
SELECT q.a AS a, q.b AS b FROM x AS q;

# title: Double nesting
SELECT a, b FROM (SELECT a, b FROM (SELECT a, b FROM x));
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: WHERE clause is merged
SELECT a, SUM(b) AS b FROM (SELECT a, b FROM x WHERE a > 1) GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS b FROM x AS x WHERE x.a > 1 GROUP BY x.a;

# title: Outer query has join
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

# title: Leave tables isolated
# leave_tables_isolated: true
SELECT a, c FROM (SELECT a, b FROM x WHERE a > 1) AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.a > 1) AS x JOIN y AS y ON x.b = y.b;

# title: Join on derived table
SELECT a, c FROM x JOIN (SELECT b, c FROM y) AS y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Inner query has a join
SELECT a, c FROM (SELECT a, c FROM x JOIN y ON x.b = y.b);
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Inner query has conflicting name in outer query
SELECT a, c FROM (SELECT q.a, q.b FROM x AS q) AS x JOIN y AS q ON x.b = q.b;
SELECT q_2.a AS a, q.c AS c FROM x AS q_2 JOIN y AS q ON q_2.b = q.b;

# title: Inner query has conflicting name in joined source
SELECT x.a, q.c FROM (SELECT a, x.b FROM x JOIN y AS q ON x.b = q.b) AS x JOIN y AS q ON x.b = q.b;
SELECT x.a AS a, q.c AS c FROM x AS x JOIN y AS q_2 ON x.b = q_2.b JOIN y AS q ON x.b = q.b;

# title: Inner query has multiple conflicting names
SELECT x.a, q.c, r.c FROM (SELECT q.a, r.b FROM x AS q JOIN y AS r ON q.b = r.b) AS x JOIN y AS q ON x.b = q.b JOIN y AS r ON x.b = r.b ORDER BY x.a, q.c, r.c;
SELECT q_2.a AS a, q.c AS c, r.c AS c FROM x AS q_2 JOIN y AS r_2 ON q_2.b = r_2.b JOIN y AS q ON r_2.b = q.b JOIN y AS r ON r_2.b = r.b ORDER BY q_2.a, q.c, r.c;

# title: Inner queries have conflicting names with each other
SELECT r.b FROM (SELECT b FROM x AS x) AS q JOIN (SELECT b FROM x) AS r ON q.b = r.b;
SELECT x_2.b AS b FROM x AS x JOIN x AS x_2 ON x.b = x_2.b;

# title: WHERE clause in joined derived table is merged to ON clause
SELECT x.a, y.c FROM x JOIN (SELECT b, c FROM y WHERE c > 1) AS y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b AND y.c > 1;

# title: Comma JOIN in outer query
SELECT x.a, y.c FROM (SELECT a FROM x) AS x, (SELECT c FROM y) AS y;
SELECT x.a AS a, y.c AS c FROM x AS x, y AS y;

# title: Comma JOIN in inner query
SELECT x.a, x.c FROM (SELECT x.a, z.c FROM x, y AS z) AS x;
SELECT x.a AS a, z.c AS c FROM x AS x CROSS JOIN y AS z;

# title: (Regression) Column in ORDER BY
SELECT * FROM (SELECT * FROM (SELECT * FROM x)) ORDER BY a LIMIT 1;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY x.a LIMIT 1;

# title: CTE
WITH x AS (SELECT a, b FROM x) SELECT a, b FROM x;
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: CTE with outer table alias
WITH y AS (SELECT a, b FROM x) SELECT a, b FROM y AS z;
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: Nested CTE
WITH x2 AS (SELECT a FROM x), x3 AS (SELECT a FROM x2) SELECT a FROM x3;
SELECT x.a AS a FROM x AS x;

# title: CTE WHERE clause is merged
WITH x AS (SELECT a, b FROM x WHERE a > 1) SELECT a, SUM(b) AS b FROM x GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS b FROM x AS x WHERE x.a > 1 GROUP BY x.a;

# title: CTE Outer query has join
WITH x2 AS (SELECT a, b FROM x WHERE a > 1) SELECT a, c FROM x2 AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

# title: CTE with inner table alias
WITH y AS (SELECT a, b FROM x AS q) SELECT a, b FROM y AS z;
SELECT q.a AS a, q.b AS b FROM x AS q;

# title: Nested CTE
SELECT * FROM (WITH x AS (SELECT a, b FROM x) SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: Inner select is an expression
SELECT a FROM (SELECT a FROM (SELECT COALESCE(a) AS a FROM x LEFT JOIN y ON x.a = y.b) AS x) AS x;
SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b;

# title: CTE select is an expression
WITH x2 AS (SELECT COALESCE(a) AS a FROM x LEFT JOIN y ON x.a = y.b) SELECT a FROM (SELECT a FROM x2 AS x) AS x;
SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b;

# title: Full outer join
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x FULL OUTER JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x FULL OUTER JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;

# title: Full outer join, no predicates
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x FULL OUTER JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x FULL OUTER JOIN y AS y ON x.b = y.b;

# title: Left join
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x LEFT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x LEFT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b WHERE x.b = 1;

# title: Left join, no predicates
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x LEFT JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x LEFT JOIN y AS y ON x.b = y.b;

# title: Right join
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x RIGHT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x RIGHT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;

# title: Right join, no predicates
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x RIGHT JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x RIGHT JOIN y AS y ON x.b = y.b;

# title: Inner join
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x INNER JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x INNER JOIN y AS y ON x.b = y.b AND y.b = 2 WHERE x.b = 1;

# title: Inner join, no predicates
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x INNER JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b;
SELECT x.b AS b, y.b AS b2 FROM x AS x INNER JOIN y AS y ON x.b = y.b;

# title: Cross join
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x CROSS JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y;
SELECT x.b AS b, y.b AS b2 FROM x AS x JOIN y AS y ON y.b = 2 WHERE x.b = 1;

# title: Cross join, no predicates
SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x CROSS JOIN (SELECT y.b AS b FROM y AS y) AS y;
SELECT x.b AS b, y.b AS b2 FROM x AS x CROSS JOIN y AS y;
