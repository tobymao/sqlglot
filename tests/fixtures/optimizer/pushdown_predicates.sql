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

SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;
SELECT x.a, u.val FROM x AS x JOIN UNNEST(ARRAY(0, 1)) AS u("val") ON u.val < x.a WHERE TRUE;

# dialect: presto
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;

# dialect: trino
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;

# dialect: athena
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;
SELECT x.a, u.val FROM x AS x CROSS JOIN UNNEST(ARRAY[0, 1]) AS u("val") WHERE x.a > u.val;

# dialect: presto
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") CROSS JOIN x AS x WHERE x.a > u.val;
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") JOIN x AS x ON u.val < x.a WHERE TRUE;

# dialect: trino
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") CROSS JOIN x AS x WHERE x.a > u.val;
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") JOIN x AS x ON u.val < x.a WHERE TRUE;

# dialect: athena
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") CROSS JOIN x AS x WHERE x.a > u.val;
SELECT x.a, u.val FROM UNNEST(ARRAY[0, 1]) AS u("val") JOIN x AS x ON u.val < x.a WHERE TRUE;

-- DNF: cross-table predicate is only pushed to the last eligible JOIN (not to an earlier JOIN that doesn't yet have all referenced tables in scope)
SELECT a.id, b.val, c.name FROM t_a AS a INNER JOIN t_b AS b ON b.a_id = a.id INNER JOIN t_c AS c ON c.b_id = b.id WHERE (b.flag = 1 AND c.active = 1) OR (b.flag = 2 AND c.active = 0);
SELECT a.id, b.val, c.name FROM t_a AS a INNER JOIN t_b AS b ON a.id = b.a_id INNER JOIN t_c AS c ON ((b.flag = 1 AND c.active = 1) OR (b.flag = 2 AND c.active = 0)) AND b.id = c.b_id WHERE (b.flag = 1 AND c.active = 1) OR (b.flag = 2 AND c.active = 0);

-- DNF: single-table predicate is pushed to its own JOIN regardless of join order
SELECT a.id, b.val FROM t_a AS a INNER JOIN t_b AS b ON b.a_id = a.id WHERE (b.flag = 1 AND b.active = 1) OR (b.flag = 2 AND b.active = 0);
SELECT a.id, b.val FROM t_a AS a INNER JOIN t_b AS b ON ((b.active = 0 AND b.flag = 2) OR (b.active = 1 AND b.flag = 1)) AND a.id = b.a_id WHERE (b.active = 0 AND b.flag = 2) OR (b.active = 1 AND b.flag = 1);

-- Predicate is not pushed into a subquery with LIMIT: filtering before the limit changes which rows the limit keeps
SELECT s.a FROM (SELECT a, b FROM x ORDER BY a LIMIT 10) AS s WHERE s.b = 1;
SELECT s.a FROM (SELECT a, b FROM x ORDER BY a LIMIT 10) AS s WHERE s.b = 1;

-- Predicate is not pushed into a subquery with OFFSET: filtering before the offset changes which rows are skipped
SELECT s.a FROM (SELECT a, b FROM x ORDER BY a OFFSET 3) AS s WHERE s.b = 1;
SELECT s.a FROM (SELECT a, b FROM x ORDER BY a OFFSET 3) AS s WHERE s.b = 1;

-- Predicate is not pushed into a subquery with QUALIFY: filtering before the window filter changes its results
SELECT s.a FROM (SELECT a, b FROM x QUALIFY ROW_NUMBER() OVER (ORDER BY a) <= 10) AS s WHERE s.b = 1;
SELECT s.a FROM (SELECT a, b FROM x QUALIFY ROW_NUMBER() OVER (ORDER BY a) <= 10) AS s WHERE s.b = 1;

-- The RHS of a RIGHT JOIN is preserved, so a WHERE predicate on it can't become a match-only ON predicate
SELECT x.a, y.b FROM x RIGHT JOIN y ON x.a = y.b WHERE y.b = 3;
SELECT x.a, y.b FROM x RIGHT JOIN y ON x.a = y.b WHERE y.b = 3;

-- A WHERE predicate on the preserved RHS of a RIGHT JOIN can still be pushed into an isolated source
SELECT x.a, y.b FROM x RIGHT JOIN (SELECT b FROM y) AS y ON x.a = y.b WHERE y.b = 3;
SELECT x.a, y.b FROM x RIGHT JOIN (SELECT b FROM y WHERE b = 3) AS y ON x.a = y.b WHERE TRUE;

-- A RIGHT JOIN preserves its own source, so an ON predicate can't be pushed into it as a filter
SELECT x.a, y.b FROM x RIGHT JOIN (SELECT b FROM y) AS y ON x.a = y.b AND y.b = 3;
SELECT x.a, y.b FROM x RIGHT JOIN (SELECT b FROM y) AS y ON x.a = y.b AND y.b = 3;

-- A FULL JOIN preserves its own source, so an ON predicate can't be pushed into it as a filter
SELECT x.a, y.b FROM x FULL JOIN (SELECT b FROM y) AS y ON x.a = y.b AND y.b = 3;
SELECT x.a, y.b FROM x FULL JOIN (SELECT b FROM y) AS y ON x.a = y.b AND y.b = 3;

-- A FULL JOIN preserves both sides, so a WHERE predicate on either of them can't be pushed down
SELECT x.a, y.b FROM x FULL JOIN y ON x.a = y.b WHERE y.b = 3;
SELECT x.a, y.b FROM x FULL JOIN y ON x.a = y.b WHERE y.b = 3;

-- Pushdown predicate through window functions when filtering strictly on partition columns
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.a = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x WHERE x.a = 1) SELECT t1.a, t1.b FROM t1 WHERE TRUE;

-- Predicate cannot be pushed down because it filters on a non-partition column
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.b = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.b = 1;

-- Multiple window functions: predicate must be in ALL partition keys
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS rn1, RANK() OVER (PARTITION BY x.a, x.b) AS rn2 FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.a = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS rn1, RANK() OVER (PARTITION BY x.a, x.b) AS rn2 FROM x WHERE x.a = 1) SELECT t1.a, t1.b FROM t1 WHERE TRUE;

-- Multiple window functions: predicate not in ALL partition keys (rn1 only partitions by x.a)
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS rn1, RANK() OVER (PARTITION BY x.a, x.b) AS rn2 FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.b = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS rn1, RANK() OVER (PARTITION BY x.a, x.b) AS rn2 FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.b = 1;

-- Window function without PARTITION BY (blocks all pushdown)
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.a = 1;
WITH t1 AS (SELECT x.a, x.b, ROW_NUMBER() OVER (ORDER BY x.a) AS row_num FROM x) SELECT t1.a, t1.b FROM t1 WHERE t1.a = 1;
