--------------------------------------
-- Qualify columns
--------------------------------------
SELECT a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT "a" FROM x;
SELECT x.a AS a FROM x AS x;

# execute: false
SELECT a FROM zz GROUP BY a ORDER BY a;
SELECT zz.a AS a FROM zz AS zz GROUP BY zz.a ORDER BY a;

# execute: false
SELECT x, p FROM (SELECT x from xx) xx CROSS JOIN yy;
SELECT xx.x AS x, yy.p AS p FROM (SELECT xx.x AS x FROM xx AS xx) AS xx CROSS JOIN yy AS yy;

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

# execute: false
SELECT 1, 2 + 3 FROM x;
SELECT 1 AS "1", 2 + 3 AS _col_1 FROM x AS x;

# execute: false
SELECT a + b FROM x;
SELECT x.a + x.b AS _col_0 FROM x AS x;

SELECT l.a FROM x l WHERE a IN (select a FROM x ORDER by a);
SELECT l.a AS a FROM x AS l WHERE l.a IN (SELECT x.a AS a FROM x AS x ORDER BY a);

# execute: false
SELECT a, SUM(b) FROM x WHERE a > 1 AND b > 1 GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS _col_1 FROM x AS x WHERE x.a > 1 AND x.b > 1 GROUP BY x.a;

SELECT SUM(a) AS c FROM x HAVING SUM(a) > 3;
SELECT SUM(x.a) AS c FROM x AS x HAVING SUM(x.a) > 3;

SELECT SUM(a) AS a FROM x HAVING SUM(a) > 3;
SELECT SUM(x.a) AS a FROM x AS x HAVING SUM(x.a) > 3;

SELECT SUM(a) AS c FROM x HAVING c > 3;
SELECT SUM(x.a) AS c FROM x AS x HAVING SUM(x.a) > 3;

# execute: false
SELECT SUM(a) AS a FROM x HAVING a > 3;
SELECT SUM(x.a) AS a FROM x AS x HAVING SUM(x.a) > 3;

SELECT SUM(a) AS c FROM x HAVING SUM(b) > 3;
SELECT SUM(x.a) AS c FROM x AS x HAVING SUM(x.b) > 3;

SELECT a AS j, b FROM x ORDER BY j;
SELECT x.a AS j, x.b AS b FROM x AS x ORDER BY j;

SELECT a AS j, b AS a FROM x ORDER BY 1;
SELECT x.a AS j, x.b AS a FROM x AS x ORDER BY x.a;

SELECT SUM(a) AS c, SUM(b) AS d FROM x ORDER BY 1, 2;
SELECT SUM(x.a) AS c, SUM(x.b) AS d FROM x AS x ORDER BY SUM(x.a), SUM(x.b);

# execute: false
SELECT CAST(a AS INT) FROM x ORDER BY a;
SELECT CAST(x.a AS INT) AS a FROM x AS x ORDER BY a;

# execute: false
SELECT SUM(a), SUM(b) AS c FROM x ORDER BY 1, 2;
SELECT SUM(x.a) AS _col_0, SUM(x.b) AS c FROM x AS x ORDER BY SUM(x.a), SUM(x.b);

SELECT a AS j, b FROM x GROUP BY j, b;
SELECT x.a AS j, x.b AS b FROM x AS x GROUP BY x.a, x.b;

SELECT a, b FROM x GROUP BY 1, 2;
SELECT x.a AS a, x.b AS b FROM x AS x GROUP BY x.a, x.b;

SELECT a, b FROM x ORDER BY 1, 2;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY x.a, x.b;

SELECT 2 FROM x GROUP BY 1;
SELECT 2 AS "2" FROM x AS x GROUP BY 1;

SELECT 'a' AS a FROM x GROUP BY 1;
SELECT 'a' AS a FROM x AS x GROUP BY 1;

# execute: false
-- this query seems to be invalid in postgres and duckdb but valid in bigquery
SELECT 2 a FROM x GROUP BY 1 HAVING a > 1;
SELECT 2 AS a FROM x AS x GROUP BY 1 HAVING a > 1;

SELECT 2 d FROM x GROUP BY d HAVING d > 1;
SELECT 2 AS d FROM x AS x GROUP BY 1 HAVING d > 1;

SELECT 2 d FROM x GROUP BY 1 ORDER BY 1;
SELECT 2 AS d FROM x AS x GROUP BY 1 ORDER BY d;

# execute: false
SELECT DATE(a), DATE(b) AS c FROM x GROUP BY 1, 2;
SELECT DATE(x.a) AS _col_0, DATE(x.b) AS c FROM x AS x GROUP BY DATE(x.a), DATE(x.b);

SELECT SUM(x.a) AS c FROM x JOIN y ON x.b = y.b GROUP BY c;
SELECT SUM(x.a) AS c FROM x AS x JOIN y AS y ON x.b = y.b GROUP BY y.c;

SELECT COALESCE(x.a) AS d FROM x JOIN y ON x.b = y.b GROUP BY d;
SELECT COALESCE(x.a) AS d FROM x AS x JOIN y AS y ON x.b = y.b GROUP BY COALESCE(x.a);

SELECT a + 1 AS d FROM x WHERE d > 1;
SELECT x.a + 1 AS d FROM x AS x WHERE x.a + 1 > 1;

# execute: false
SELECT a + 1 AS d, d + 2 FROM x;
SELECT x.a + 1 AS d, x.a + 1 + 2 AS _col_1 FROM x AS x;

SELECT a AS a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY a;

SELECT a, b FROM x ORDER BY a;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY a;

SELECT a FROM x ORDER BY b;
SELECT x.a AS a FROM x AS x ORDER BY x.b;

SELECT SUM(a) AS a FROM x ORDER BY SUM(a);
SELECT SUM(x.a) AS a FROM x AS x ORDER BY SUM(x.a);

# execute: false
SELECT AGGREGATE(ARRAY(a, x.b), 0, (x, acc) -> x + acc + a) AS sum_agg FROM x;
SELECT AGGREGATE(ARRAY(x.a, x.b), 0, (x, acc) -> x + acc + x.a) AS sum_agg FROM x AS x;

# dialect: starrocks
# execute: false
SELECT DATE_TRUNC('week', a) AS a FROM x;
SELECT DATE_TRUNC('week', x.a) AS a FROM x AS x;

# dialect: bigquery
# execute: false
SELECT DATE_TRUNC(a, MONTH) AS a FROM x;
SELECT DATE_TRUNC(x.a, MONTH) AS a FROM x AS x;

# execute: false
SELECT x FROM READ_PARQUET('path.parquet', hive_partition=1);
SELECT _q_0.x AS x FROM READ_PARQUET('path.parquet', hive_partition = 1) AS _q_0;

# execute: false
select * from (values (1, 2));
SELECT _q_0._col_0 AS _col_0, _q_0._col_1 AS _col_1 FROM (VALUES (1, 2)) AS _q_0(_col_0, _col_1);

# execute: false
select * from (values (1, 2)) x;
SELECT x._col_0 AS _col_0, x._col_1 AS _col_1 FROM (VALUES (1, 2)) AS x(_col_0, _col_1);

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
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0;

SELECT a FROM (SELECT a FROM (SELECT a FROM x));
SELECT _q_1.a AS a FROM (SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0) AS _q_1;

SELECT x.a FROM x AS x JOIN (SELECT * FROM x) AS y ON x.a = y.a;
SELECT x.a AS a FROM x AS x JOIN (SELECT x.a AS a, x.b AS b FROM x AS x) AS y ON x.a = y.a;

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
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x UNION SELECT x.a AS a FROM x AS x) AS _q_0;

--------------------------------------
-- Subqueries
--------------------------------------
SELECT a FROM x WHERE b IN (SELECT c FROM y);
SELECT x.a AS a FROM x AS x WHERE x.b IN (SELECT y.c AS c FROM y AS y);

# execute: false
SELECT (SELECT c FROM y) FROM x;
SELECT (SELECT y.c AS c FROM y AS y) AS _col_0 FROM x AS x;

SELECT a FROM (SELECT a FROM x) WHERE a IN (SELECT b FROM (SELECT b FROM y));
SELECT _q_1.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_1 WHERE _q_1.a IN (SELECT _q_0.b AS b FROM (SELECT y.b AS b FROM y AS y) AS _q_0);

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

# execute: false
SELECT (SELECT n.a FROM n WHERE n.id = m.id) FROM m AS m;
SELECT (SELECT n.a AS a FROM n AS n WHERE n.id = m.id) AS _col_0 FROM m AS m;

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
SELECT _q_0.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS _q_0;

SELECT * FROM (SELECT a FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0;

SELECT * FROM x GROUP BY 1, 2;
SELECT x.a AS a, x.b AS b FROM x AS x GROUP BY x.a, x.b;

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
# execute: false
SELECT * REPLACE(a AS d) FROM x;
SELECT x.a AS d, x.b AS b FROM x AS x;

# execute: false
SELECT * EXCEPT(b) REPLACE(a AS d) FROM x;
SELECT x.a AS d FROM x AS x;

SELECT x.* EXCEPT(a), y.* FROM x, y;
SELECT x.b AS b, y.b AS b, y.c AS c FROM x AS x, y AS y;

SELECT * EXCEPT(a) FROM x;
SELECT x.b AS b FROM x AS x;

# execute: false
# note: this query would fail in the engine level because there are 0 selected columns
SELECT * EXCEPT (a, b) FROM x;
SELECT * EXCEPT (x.a, x.b) FROM x AS x;

--------------------------------------
-- Using
--------------------------------------
SELECT x.b FROM x JOIN y USING (b);
SELECT x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b;

# execute: false
WITH cte AS (SELECT a.b.c.d.f.g FROM tbl1) SELECT g FROM (SELECT g FROM tbl2) tbl2 JOIN cte USING(g);
WITH cte AS (SELECT tbl1.a.b.c.d.f.g AS g FROM tbl1 AS tbl1) SELECT COALESCE(tbl2.g, cte.g) AS g FROM (SELECT tbl2.g AS g FROM tbl2 AS tbl2) AS tbl2 JOIN cte ON tbl2.g = cte.g;

SELECT x.b FROM x JOIN y USING (b) JOIN z USING (b);
SELECT x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b JOIN z AS z ON x.b = z.b;

SELECT b FROM x AS x2 JOIN y AS y2 USING (b);
SELECT COALESCE(x2.b, y2.b) AS b FROM x AS x2 JOIN y AS y2 ON x2.b = y2.b;

SELECT b FROM x JOIN y USING (b) WHERE b = 1 and y.b = 2;
SELECT COALESCE(x.b, y.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b WHERE COALESCE(x.b, y.b) = 1 AND y.b = 2;

SELECT b FROM x JOIN y USING (b) JOIN z USING (b);
SELECT COALESCE(x.b, y.b, z.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b JOIN z AS z ON x.b = z.b;

SELECT * FROM x JOIN y USING(b);
SELECT x.a AS a, COALESCE(x.b, y.b) AS b, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT x.* FROM x JOIN y USING(b);
SELECT x.a AS a, COALESCE(x.b, y.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT * FROM x LEFT JOIN y USING(b);
SELECT x.a AS a, COALESCE(x.b, y.b) AS b, y.c AS c FROM x AS x LEFT JOIN y AS y ON x.b = y.b;

SELECT b FROM x JOIN y USING(b);
SELECT COALESCE(x.b, y.b) AS b FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT b, c FROM x JOIN y USING(b);
SELECT COALESCE(x.b, y.b) AS b, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

SELECT b, c FROM y JOIN z USING(b, c);
SELECT COALESCE(y.b, z.b) AS b, COALESCE(y.c, z.c) AS c FROM y AS y JOIN z AS z ON y.b = z.b AND y.c = z.c;

SELECT * FROM y JOIN z USING(b, c);
SELECT COALESCE(y.b, z.b) AS b, COALESCE(y.c, z.c) AS c FROM y AS y JOIN z AS z ON y.b = z.b AND y.c = z.c;

SELECT * FROM y JOIN z USING(b, c) WHERE b = 2 AND c = 3;
SELECT COALESCE(y.b, z.b) AS b, COALESCE(y.c, z.c) AS c FROM y AS y JOIN z AS z ON y.b = z.b AND y.c = z.c WHERE COALESCE(y.b, z.b) = 2 AND COALESCE(y.c, z.c) = 3;

-- We can safely convert `b` to `x.b` in the following two queries, because the original queries
-- would be invalid if `b` also existed in `t`'s schema (which we don't know), due to ambiguity.

# execute: false
SELECT b FROM x JOIN t USING(a);
SELECT x.b AS b FROM x AS x JOIN t AS t ON x.a = t.a;

# execute: false
SELECT b FROM t JOIN x USING(a);
SELECT x.b AS b FROM t AS t JOIN x AS x ON t.a = x.a;

# execute: false
SELECT a FROM t1 JOIN t2 USING(a);
SELECT COALESCE(t1.a, t2.a) AS a FROM t1 AS t1 JOIN t2 AS t2 ON t1.a = t2.a;

--------------------------------------
-- Hint with table reference
--------------------------------------
# dialect: spark
SELECT /*+ BROADCAST(y) */ x.b FROM x JOIN y ON x.b = y.b;
SELECT /*+ BROADCAST(y) */ x.b AS b FROM x AS x JOIN y AS y ON x.b = y.b;

--------------------------------------
-- UDTF
--------------------------------------
# execute: false
SELECT c FROM x LATERAL VIEW EXPLODE (a) AS c;
SELECT _q_0.c AS c FROM x AS x LATERAL VIEW EXPLODE(x.a) _q_0 AS c;

# execute: false
SELECT c FROM xx LATERAL VIEW EXPLODE (a) AS c;
SELECT _q_0.c AS c FROM xx AS xx LATERAL VIEW EXPLODE(xx.a) _q_0 AS c;

# execute: false
SELECT c FROM x LATERAL VIEW EXPLODE (a) t AS c;
SELECT t.c AS c FROM x AS x LATERAL VIEW EXPLODE(x.a) t AS c;

# execute: false
SELECT aa FROM x, UNNEST(a) AS t(aa);
SELECT t.aa AS aa FROM x AS x, UNNEST(x.a) AS t(aa);

# dialect: bigquery
# execute: false
SELECT aa FROM x, UNNEST(a) AS aa;
SELECT aa AS aa FROM x AS x, UNNEST(x.a) AS aa;

# dialect: presto
SELECT x.a, i.b FROM x CROSS JOIN UNNEST(SPLIT(b, ',')) AS i(b);
SELECT x.a AS a, i.b AS b FROM x AS x CROSS JOIN UNNEST(SPLIT(x.b, ',')) AS i(b);

# execute: false
SELECT c FROM (SELECT 1 a) AS x LATERAL VIEW EXPLODE(a) AS c;
SELECT _q_0.c AS c FROM (SELECT 1 AS a) AS x LATERAL VIEW EXPLODE(x.a) _q_0 AS c;

--------------------------------------
-- Window functions
--------------------------------------
# title: ORDER BY in window function
SELECT a + 1 AS a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) AS row_num FROM x;
SELECT x.a + 1 AS a, ROW_NUMBER() OVER (PARTITION BY x.b ORDER BY x.a) AS row_num FROM x AS x;

# dialect: bigquery
SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS row_num FROM x QUALIFY row_num = 1;
SELECT ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) AS row_num FROM x AS x QUALIFY ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.b) = 1;

# dialect: bigquery
SELECT x.b, x.a FROM x LEFT JOIN y ON x.b = y.b QUALIFY ROW_NUMBER() OVER(PARTITION BY x.b ORDER BY x.a DESC) = 1;
SELECT x.b AS b, x.a AS a FROM x AS x LEFT JOIN y AS y ON x.b = y.b QUALIFY ROW_NUMBER() OVER (PARTITION BY x.b ORDER BY x.a DESC) = 1;

SELECT * FROM x QUALIFY COUNT(a) OVER (PARTITION BY b) > 1;
SELECT x.a AS a, x.b AS b FROM x AS x QUALIFY COUNT(x.a) OVER (PARTITION BY x.b) > 1;

--------------------------------------
-- Expand laterals
--------------------------------------
# execute: false
select 2 AS d, d + 1 FROM x WHERE d = 2 GROUP BY d;
SELECT 2 AS d, 2 + 1 AS _col_1 FROM x AS x WHERE 2 = 2 GROUP BY 1;

# title: expand alias reference
SELECT
  x.a + 1 AS i,
  i + 1 AS j,
  j + 1 AS k
FROM x;
SELECT x.a + 1 AS i, x.a + 1 + 1 AS j, x.a + 1 + 1 + 1 AS k FROM x AS x;

# title: noop - reference comes before alias
# execute: false
SELECT i + 1 AS j, x.a + 1 AS i FROM x;
SELECT i + 1 AS j, x.a + 1 AS i FROM x AS x;

# title: subquery
SELECT
  *
FROM (
  SELECT
    x.a + 1 AS i,
    i + 1 AS j
  FROM x
);
SELECT _q_0.i AS i, _q_0.j AS j FROM (SELECT x.a + 1 AS i, x.a + 1 + 1 AS j FROM x AS x) AS _q_0;

--------------------------------------
-- Wrapped tables / join constructs
--------------------------------------
# execute: false
SELECT * FROM ((tbl));
SELECT * FROM ((tbl AS tbl));

SELECT a, c FROM (x LEFT JOIN y ON a = c);
SELECT x.a AS a, y.c AS c FROM (x AS x LEFT JOIN y AS y ON x.a = y.c);

# execute: false
SELECT * FROM ((a CROSS JOIN ((b CROSS JOIN c) CROSS JOIN (d CROSS JOIN e))));
SELECT * FROM ((a AS a CROSS JOIN ((b AS b CROSS JOIN c AS c) CROSS JOIN (d AS d CROSS JOIN e AS e))));

# execute: false
SELECT * FROM ((SELECT * FROM tbl));
SELECT * FROM ((SELECT * FROM tbl AS tbl) AS _q_0);

# execute: false
SELECT * FROM ((SELECT c FROM t1) JOIN t2);
SELECT * FROM ((SELECT t1.c AS c FROM t1 AS t1) AS _q_0, t2 AS t2);

# execute: false
SELECT * FROM ((SELECT * FROM x) INNER JOIN y ON a = c);
SELECT y.b AS b, y.c AS c, _q_0.a AS a, _q_0.b AS b FROM ((SELECT x.a AS a, x.b AS b FROM x AS x) AS _q_0 INNER JOIN y AS y ON _q_0.a = y.c);

SELECT x.a, y.b, z.c FROM x LEFT JOIN (y INNER JOIN z ON y.c = z.c) ON x.b = y.b;
SELECT x.a AS a, y.b AS b, z.c AS c FROM x AS x LEFT JOIN (y AS y INNER JOIN z AS z ON y.c = z.c) ON x.b = y.b;

SELECT * FROM ((SELECT * FROM x) INNER JOIN (SELECT * FROM y) ON a = c);
SELECT _q_0.a AS a, _q_0.b AS b, _q_1.b AS b, _q_1.c AS c FROM ((SELECT x.a AS a, x.b AS b FROM x AS x) AS _q_0 INNER JOIN (SELECT y.b AS b, y.c AS c FROM y AS y) AS _q_1 ON _q_0.a = _q_1.c);

SELECT b FROM ((SELECT a FROM x) INNER JOIN y ON a = b);
SELECT y.b AS b FROM ((SELECT x.a AS a FROM x AS x) AS _q_0 INNER JOIN y AS y ON _q_0.a = y.b);
