-- No derived tables
SELECT * FROM x;
SELECT * FROM x;

-- Unaliased derived tables
SELECT a FROM (SELECT b FROM (SELECT c FROM x));
WITH cte AS (SELECT c FROM x), cte_2 AS (SELECT b FROM cte AS cte) SELECT a FROM cte_2 AS cte_2;

-- Joined derived table inside nested derived table
SELECT b FROM (SELECT b FROM (SELECT b FROM x JOIN (SELECT b FROM y) AS y ON x.b = y.b));
WITH y_2 AS (SELECT b FROM y), cte AS (SELECT b FROM x JOIN y_2 AS y ON x.b = y.b), cte_2 AS (SELECT b FROM cte AS cte) SELECT b FROM cte_2 AS cte_2;

-- Aliased derived tables
SELECT a FROM (SELECT b FROM (SELECT c FROM x) AS y) AS z;
WITH y AS (SELECT c FROM x), z AS (SELECT b FROM y AS y) SELECT a FROM z AS z;

-- Existing CTEs
WITH q AS (SELECT c FROM x) SELECT a FROM (SELECT b FROM q AS y) AS z;
WITH q AS (SELECT c FROM x), z AS (SELECT b FROM q AS y) SELECT a FROM z AS z;

-- Derived table inside CTE
WITH x AS (SELECT a FROM (SELECT a FROM x) AS y) SELECT a FROM x;
WITH y AS (SELECT a FROM x), x AS (SELECT a FROM y AS y) SELECT a FROM x;

-- Name conflicts with existing outer derived table
SELECT a FROM (SELECT b FROM (SELECT c FROM x) AS y) AS y;
WITH y AS (SELECT c FROM x), y_2 AS (SELECT b FROM y AS y) SELECT a FROM y_2 AS y;

-- Name conflicts with outer join
SELECT a, b FROM (SELECT c FROM (SELECT d FROM x) AS x) AS y JOIN x ON x.a = y.a;
WITH x_2 AS (SELECT d FROM x), y AS (SELECT c FROM x_2 AS x) SELECT a, b FROM y AS y JOIN x ON x.a = y.a;

-- Name conflicts with table name that is selected in another branch
SELECT * FROM (SELECT * FROM (SELECT a FROM x) AS x) AS y JOIN (SELECT * FROM x) AS z ON x.a = y.a;
WITH x_2 AS (SELECT a FROM x), y AS (SELECT * FROM x_2 AS x), z AS (SELECT * FROM x) SELECT * FROM y AS y JOIN z AS z ON x.a = y.a;

-- Name conflicts with table alias
SELECT a FROM (SELECT a FROM (SELECT a FROM x) AS y) AS z CROSS JOIN q AS y;
WITH y AS (SELECT a FROM x), z AS (SELECT a FROM y AS y) SELECT a FROM z AS z CROSS JOIN q AS y;

-- Name conflicts with existing CTE
WITH y AS (SELECT a FROM (SELECT a FROM x) AS y) SELECT a FROM y;
WITH y_2 AS (SELECT a FROM x), y AS (SELECT a FROM y_2 AS y) SELECT a FROM y;

-- Union of selects with derived tables
(SELECT a FROM (SELECT b FROM x)) UNION (SELECT a FROM (SELECT b FROM y));
WITH cte AS (SELECT b FROM x), cte_2 AS (SELECT b FROM y) (SELECT a FROM cte AS cte) UNION (SELECT a FROM cte_2 AS cte_2);

-- Subquery
SELECT a FROM x WHERE b = (SELECT y.c FROM y);
SELECT a FROM x WHERE b = (SELECT y.c FROM y);

-- Correlated subquery
SELECT a FROM x WHERE b = (SELECT c FROM y WHERE y.a = x.a);
SELECT a FROM x WHERE b = (SELECT c FROM y WHERE y.a = x.a);

-- Duplicate CTE
SELECT a FROM (SELECT b FROM x) AS y CROSS JOIN (SELECT b FROM x) AS z;
WITH y AS (SELECT b FROM x) SELECT a FROM y AS y CROSS JOIN y AS z;

-- Doubly duplicate CTE
SELECT * FROM (SELECT * FROM x JOIN (SELECT * FROM x) AS y) AS z JOIN (SELECT * FROM x JOIN (SELECT * FROM x) AS y) AS q;
WITH y AS (SELECT * FROM x), z AS (SELECT * FROM x, y AS y) SELECT * FROM z AS z, z AS q;

-- Another duplicate...
SELECT x.id FROM (SELECT * FROM x AS x JOIN y AS y ON x.id = y.id) AS x JOIN (SELECT * FROM x AS x JOIN y AS y ON x.id = y.id) AS y ON x.id = y.id;
WITH x_2 AS (SELECT * FROM x AS x JOIN y AS y ON x.id = y.id) SELECT x.id FROM x_2 AS x JOIN x_2 AS y ON x.id = y.id;

-- Root subquery
(SELECT * FROM (SELECT * FROM x)) LIMIT 1;
(WITH cte AS (SELECT * FROM x) SELECT * FROM cte AS cte) LIMIT 1;

-- Existing duplicate CTE
WITH y AS (SELECT a FROM x) SELECT a FROM (SELECT a FROM x) AS y CROSS JOIN y AS z;
WITH y AS (SELECT a FROM x) SELECT a FROM y AS y CROSS JOIN y AS z;

-- Nested CTE
WITH cte1 AS (SELECT a FROM x) SELECT a FROM (WITH cte2 AS (SELECT a FROM cte1) SELECT a FROM cte2);
WITH cte1 AS (SELECT a FROM x), cte2 AS (SELECT a FROM cte1), cte AS (SELECT a FROM cte2 AS cte2) SELECT a FROM cte AS cte;

-- Nested CTE inside CTE
WITH cte1 AS (WITH cte2 AS (SELECT a FROM x) SELECT t.a FROM cte2 AS t) SELECT a FROM cte1;
WITH cte2 AS (SELECT a FROM x), cte1 AS (SELECT t.a FROM cte2 AS t) SELECT a FROM cte1;

-- Duplicate CTE nested in CTE
WITH cte1 AS (SELECT a FROM x), cte2 AS (WITH cte3 AS (SELECT a FROM x) SELECT a FROM cte3) SELECT a FROM cte2;
WITH cte1 AS (SELECT a FROM x), cte2 AS (SELECT a FROM cte1 AS cte3) SELECT a FROM cte2;

-- Wrapped subquery joined with table
SELECT * FROM ((SELECT c FROM t1) JOIN t2);
WITH cte AS (SELECT c FROM t1) SELECT * FROM (cte AS cte, t2);

-- Wrapped subquery with redundant parentheses
SELECT * FROM (((SELECT * FROM tbl)));
WITH cte AS (SELECT * FROM tbl) SELECT * FROM cte AS cte;
