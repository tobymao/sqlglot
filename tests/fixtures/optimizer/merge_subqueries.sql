# title: Simple
SELECT a, b FROM (SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: Wrap addition in a multiplication
SELECT c * 2 AS d FROM (SELECT a + b AS c FROM x);
SELECT (x.a + x.b) * 2 AS d FROM x AS x;

# title: Wrap addition in an addition
# note: The "simplify" rule will unwrap this
SELECT c + d AS e FROM (SELECT a + b AS c, a AS d FROM x);
SELECT (x.a + x.b) + x.a AS e FROM x AS x;

# title: Wrap multiplication in an addition
# note: The "simplify" rule will unwrap this
WITH cte AS (SELECT a * b AS c, a AS d FROM x) SELECT c + d AS e FROM cte;
SELECT (x.a * x.b) + x.a AS e FROM x AS x;

# title: Don't wrap function
SELECT 2 * foo AS bar FROM (SELECT CAST(b AS DOUBLE) AS foo FROM x);
SELECT 2 * CAST(x.b AS DOUBLE) AS bar FROM x AS x;

# title: Don't wrap a wrapped expression
SELECT foo * 2 AS bar FROM (SELECT (1 + 2 + 3) AS foo FROM x);
SELECT (1 + 2 + 3) * 2 AS bar FROM x AS x;

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
SELECT x.a, y.c FROM x JOIN (SELECT b, c FROM y WHERE c > 1) AS y ON x.b = y.b ORDER BY x.a;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b AND y.c > 1 ORDER BY x.a;

# title: Comma JOIN in outer query
SELECT x.a, y.c FROM (SELECT a FROM x) AS x, (SELECT c FROM y) AS y;
SELECT x.a AS a, y.c AS c FROM x AS x, y AS y;

# title: Comma JOIN in inner query
SELECT x.a, x.c FROM (SELECT x.a, z.c FROM x, y AS z) AS x;
SELECT x.a AS a, z.c AS c FROM x AS x, y AS z;

# title: (Regression) Column in ORDER BY
SELECT * FROM (SELECT * FROM (SELECT * FROM x)) ORDER BY a LIMIT 1;
SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY x.a LIMIT 1;

# title: CTE
WITH x AS (SELECT a, b FROM main.x) SELECT a, b FROM x;
SELECT x.a AS a, x.b AS b FROM main.x AS x;

# title: CTE with outer table alias
WITH y AS (SELECT a, b FROM x) SELECT a, b FROM y AS z;
SELECT x.a AS a, x.b AS b FROM x AS x;

# title: Nested CTE
WITH x2 AS (SELECT a FROM main.x), x3 AS (SELECT a FROM x2) SELECT a FROM x3;
SELECT x.a AS a FROM main.x AS x;

# title: CTE WHERE clause is merged
WITH x AS (SELECT a, b FROM main.x WHERE a > 1) SELECT a, SUM(b) AS b FROM x GROUP BY a;
SELECT x.a AS a, SUM(x.b) AS b FROM main.x AS x WHERE x.a > 1 GROUP BY x.a;

# title: CTE Outer query has join
WITH x2 AS (SELECT a, b FROM x WHERE a > 1) SELECT a, c FROM x2 AS x JOIN y ON x.b = y.b;
SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1;

# title: CTE with inner table alias
WITH y AS (SELECT a, b FROM x AS q) SELECT a, b FROM y AS z;
SELECT q.a AS a, q.b AS b FROM x AS q;

# title: Nested CTE
SELECT * FROM (WITH x AS (SELECT a, b FROM main.x) SELECT a, b FROM x);
SELECT x.a AS a, x.b AS b FROM main.x AS x;

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

# title: Broadcast hint
# dialect: spark
WITH m AS (SELECT x.a, x.b FROM x), n AS (SELECT y.b, y.c FROM y), joined as (SELECT /*+ BROADCAST(k) */ m.a, k.c FROM m JOIN n AS k ON m.b = k.b) SELECT joined.a, joined.c FROM joined;
SELECT /*+ BROADCAST(y) */ x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Broadcast hint multiple tables
# dialect: spark
WITH m AS (SELECT x.a, x.b FROM x), n AS (SELECT y.b, y.c FROM y), joined as (SELECT /*+ BROADCAST(m, n) */ m.a, n.c FROM m JOIN n ON m.b = n.b) SELECT joined.a, joined.c FROM joined;
SELECT /*+ BROADCAST(x, y) */ x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Multiple Table Hints
# dialect: spark
WITH m AS (SELECT x.a, x.b FROM x), n AS (SELECT y.b, y.c FROM y), joined as (SELECT /*+ BROADCAST(m), MERGE(m, n) */ m.a, n.c FROM m JOIN n ON m.b = n.b) SELECT joined.a, joined.c FROM joined;
SELECT /*+ BROADCAST(x), MERGE(x, y) */ x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Mix Table and Column Hints
# dialect: spark
WITH m AS (SELECT x.a, x.b FROM x), n AS (SELECT y.b, y.c FROM y), joined as (SELECT /*+ BROADCAST(m), MERGE(m, n) */ m.a, n.c FROM m JOIN n ON m.b = n.b) SELECT /*+ COALESCE(3) */ joined.a, joined.c FROM joined;
SELECT /*+ COALESCE(3), BROADCAST(x), MERGE(x, y) */ x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Hint Subquery
# dialect: spark
SELECT
    subquery.a,
    subquery.c
FROM (
    SELECT /*+ BROADCAST(m), MERGE(m, n) */ m.a, n.c FROM (SELECT x.a, x.b FROM x) AS m JOIN (SELECT y.b, y.c FROM y) AS n ON m.b = n.b
) AS subquery;
SELECT /*+ BROADCAST(x), MERGE(x, y) */ x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b;

# title: Subquery Test
# dialect: spark
SELECT /*+ BROADCAST(x) */
  x.a,
  x.c
FROM (
  SELECT
    x.a,
    x.c
  FROM (
    SELECT
      x.a,
      COUNT(1) AS c
    FROM x
    GROUP BY x.a
  ) AS x
) AS x;
SELECT /*+ BROADCAST(x) */ x.a AS a, x.c AS c FROM (SELECT x.a AS a, COUNT(1) AS c FROM x AS x GROUP BY x.a) AS x;

# title: Test preventing merge of window expressions where clause
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
  ORDER BY x.a, x.b, row_num
)
SELECT
  t1.a,
  t1.b
FROM
  t1
WHERE
  row_num = 1;
WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x ORDER BY x.a, x.b, row_num) SELECT t1.a AS a, t1.b AS b FROM t1 AS t1 WHERE t1.row_num = 1;

# title: Test preventing merge of window expressions join clause
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
)
SELECT
  t1.a,
  t1.b
FROM t1 JOIN y ON t1.a = y.c AND t1.row_num = 1;
WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x) SELECT t1.a AS a, t1.b AS b FROM t1 AS t1 JOIN y AS y ON t1.a = y.c AND t1.row_num = 1;

# title: Test preventing merge of window expressions agg function
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
)
SELECT
  SUM(t1.row_num) as total_rows
FROM
  t1;
WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x) SELECT SUM(t1.row_num) AS total_rows FROM t1 AS t1;

# title: Test prevent merging of window if in group by func
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
)
SELECT
  t1.row_num AS row_num,
  SUM(t1.a) AS total
FROM
  t1
GROUP BY t1.row_num
ORDER BY t1.row_num;
WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x) SELECT t1.row_num AS row_num, SUM(t1.a) AS total FROM t1 AS t1 GROUP BY t1.row_num ORDER BY row_num;

# title: Test prevent merging of window if in order by func
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
)
SELECT
  t1.row_num AS row_num,
  t1.a AS a
FROM
  t1
ORDER BY t1.row_num, t1.a;
WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x) SELECT t1.row_num AS row_num, t1.a AS a FROM t1 AS t1 ORDER BY t1.row_num, t1.a;

# title: Test allow merging of window function
with t1 as (
  SELECT
    x.a,
    x.b,
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
  ORDER BY x.a, x.b, row_num
)
SELECT
  t1.a,
  t1.b,
  t1.row_num
FROM
  t1;
SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x ORDER BY x.a, x.b, row_num;

# title: Don't merge window functions, inner table is aliased in outer query
with t1 as (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) as row_num
  FROM
    x
)
SELECT
  t2.row_num
FROM
  t1 AS t2
WHERE
  t2.row_num = 2;
WITH t1 AS (SELECT ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x) SELECT t2.row_num AS row_num FROM t1 AS t2 WHERE t2.row_num = 2;

# title: Values Test
# dialect: spark
WITH t1 AS (
  SELECT
    a1.cola
  FROM
    VALUES (1) AS a1(cola)
), t2 AS (
  SELECT
    a2.cola
  FROM
    VALUES (1) AS a2(cola)
)
SELECT /*+ BROADCAST(t2) */
  t1.cola,
  t2.cola,
FROM
  t1
  JOIN
  t2
  ON
    t1.cola = t2.cola;
SELECT /*+ BROADCAST(a2) */ a1.cola AS cola, a2.cola AS cola FROM VALUES (1) AS a1(cola) JOIN VALUES (1) AS a2(cola) ON a1.cola = a2.cola;

# title: Nested subquery selects from same table as another subquery
WITH i AS (
  SELECT
    x.a AS a
  FROM x AS x
), j AS (
  SELECT
    x.a,
    x.b
  FROM x AS x
), k AS (
  SELECT
    j.a,
    j.b
  FROM j AS j
)
SELECT
  i.a,
  k.b
FROM i AS i
LEFT JOIN k AS k
ON  i.a = k.a;
SELECT x.a AS a, x_2.b AS b FROM x AS x LEFT JOIN x AS x_2 ON x.a = x_2.a;

# title: Outer select joins on inner select join
WITH i AS (
  SELECT
    x.a AS a
  FROM y AS y
  JOIN x AS x
    ON y.b = x.b
)
SELECT
  x.a AS a
FROM x AS x
LEFT JOIN i AS i
  ON x.a = i.a;
WITH i AS (SELECT x.a AS a FROM y AS y JOIN x AS x ON y.b = x.b) SELECT x.a AS a FROM x AS x LEFT JOIN i AS i ON x.a = i.a;

# title: Outer scope selects from wrapped table with a join (unknown schema)
# execute: false
WITH _q_0 AS (SELECT t1.c AS c FROM t1 AS t1) SELECT * FROM (_q_0 AS _q_0 CROSS JOIN t2 AS t2);
WITH _q_0 AS (SELECT t1.c AS c FROM t1 AS t1) SELECT * FROM (_q_0 AS _q_0 CROSS JOIN t2 AS t2);

# title: Outer scope selects single column from wrapped table with a join
WITH _q_0 AS (
  SELECT
    x.a AS a
  FROM x AS x
), y_2 AS (
  SELECT
    y.b AS b
  FROM y AS y
)
SELECT
  y.b AS b
FROM (
  _q_0 AS _q_0
    JOIN y_2 AS y
      ON _q_0.a = y.b
);
SELECT y.b AS b FROM (x AS x JOIN y AS y ON x.a = y.b);

# title: merge cte into subquery with overlapping alias
WITH q AS (
  SELECT
    y.b AS a
  FROM y AS y
)
SELECT
  q.a AS a
FROM x AS q
WHERE
  q.a IN (
    SELECT
      q.a AS a
    FROM q AS q
  );
SELECT q.a AS a FROM x AS q WHERE q.a IN (SELECT y.b AS a FROM y AS y);

# title: dont merge when inner query has ORDER BY and outer query is UNION
WITH q AS (
  SELECT
    x.a AS a
  FROM x
  ORDER BY x.a
)
SELECT
  q.a AS a
FROM q
UNION ALL
SELECT
  1 AS a;
WITH q AS (SELECT x.a AS a FROM x AS x ORDER BY x.a) SELECT q.a AS a FROM q AS q UNION ALL SELECT 1 AS a;

# title: Consecutive inner - outer conflicting names
WITH tbl AS (select 1 as id)
SELECT
  id
FROM (
  SELECT OTBL.id
  FROM (
    SELECT OTBL.id
    FROM (
      SELECT OTBL.id
      FROM tbl AS OTBL
      LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id
    ) AS OTBL
    LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id
  ) AS OTBL
  LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id
) AS ITBL;
WITH tbl AS (SELECT 1 AS id) SELECT OTBL.id AS id FROM tbl AS OTBL LEFT OUTER JOIN tbl AS ITBL_2 ON OTBL.id = ITBL_2.id LEFT OUTER JOIN tbl AS ITBL_3 ON OTBL.id = ITBL_3.id LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id;
