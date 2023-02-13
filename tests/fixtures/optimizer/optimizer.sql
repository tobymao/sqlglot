# title: lateral
# execute: false
SELECT a, m FROM z LATERAL VIEW EXPLODE([1, 2]) q AS m;
WITH "z_2" AS (
  SELECT
    "z"."a" AS "a"
  FROM "z" AS "z"
)
SELECT
  "z"."a" AS "a",
  "q"."m" AS "m"
FROM "z_2" AS "z"
LATERAL VIEW
EXPLODE(ARRAY(1, 2)) q AS "m";

# title: unnest
# execute: false
SELECT x FROM UNNEST([1, 2]) AS q(x, y);
SELECT
  "q"."x" AS "x"
FROM UNNEST(ARRAY(1, 2)) AS "q"("x", "y");

# title: Union in CTE
WITH cte AS (
    (
        SELECT
            a
            FROM
            x
    )
    UNION ALL
    (
        SELECT
            b AS a
        FROM
            y
    )
)
SELECT
    *
FROM
    cte;
WITH "cte" AS (
  (
    SELECT
      "x"."a" AS "a"
    FROM "x" AS "x"
  )
  UNION ALL
  (
    SELECT
      "y"."b" AS "a"
    FROM "y" AS "y"
  )
)
SELECT
  "cte"."a" AS "a"
FROM "cte";

# title: Chained CTEs
WITH cte1 AS (
    SELECT a
    FROM x
), cte2 AS (
    SELECT a + 1 AS a
    FROM cte1
)
SELECT
    a
FROM cte1
UNION ALL
SELECT
    a
FROM cte2;
WITH "cte1" AS (
  SELECT
    "x"."a" AS "a"
  FROM "x" AS "x"
)
SELECT
  "cte1"."a" AS "a"
FROM "cte1"
UNION ALL
SELECT
  "cte1"."a" + 1 AS "a"
FROM "cte1";

# title: Correlated subquery
SELECT a, SUM(b) AS sum_b
FROM (
    SELECT x.a, y.b
    FROM x, y
    WHERE (SELECT max(b) FROM y WHERE x.b = y.b) >= 0 AND x.b = y.b
) d
WHERE (TRUE AND TRUE OR 'a' = 'b') AND a > 1
GROUP BY a;
WITH "_u_0" AS (
  SELECT
    MAX("y"."b") AS "_col_0",
    "y"."b" AS "_u_1"
  FROM "y" AS "y"
  GROUP BY
    "y"."b"
)
SELECT
  "x"."a" AS "a",
  SUM("y"."b") AS "sum_b"
FROM "x" AS "x"
LEFT JOIN "_u_0" AS "_u_0"
  ON "x"."b" = "_u_0"."_u_1"
JOIN "y" AS "y"
  ON "x"."b" = "y"."b"
WHERE
  "_u_0"."_col_0" >= 0 AND "x"."a" > 1
GROUP BY
  "x"."a";

# title: Root subquery
(SELECT a FROM x) LIMIT 1;
(
  SELECT
    "x"."a" AS "a"
  FROM "x" AS "x"
)
LIMIT 1;

# title: Root subquery is union
(SELECT b FROM x UNION SELECT b FROM y ORDER BY b) LIMIT 1;
(
  SELECT
    "x"."b" AS "b"
  FROM "x" AS "x"
  UNION
  SELECT
    "y"."b" AS "b"
  FROM "y" AS "y"
  ORDER BY
    "b"
)
LIMIT 1;

# title: broadcast
# dialect: spark
SELECT /*+ BROADCAST(y) */ x.b FROM x JOIN y ON x.b = y.b;
SELECT /*+ BROADCAST(`y`) */
  `x`.`b` AS `b`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

# title: aggregate
# execute: false
SELECT AGGREGATE(ARRAY(x.a, x.b), 0, (x, acc) -> x + acc + a) AS sum_agg FROM x;
SELECT
  AGGREGATE(ARRAY("x"."a", "x"."b"), 0, ("x", "acc") -> "x" + "acc" + "x"."a") AS "sum_agg"
FROM "x" AS "x";

# title: values
SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  "tab"."cola" AS "cola",
  "tab"."colb" AS "colb"
FROM (VALUES
  (1, 'test'),
  (2, 'test2')) AS "tab"("cola", "colb");

# title: spark values
# dialect: spark
SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  `tab`.`cola` AS `cola`,
  `tab`.`colb` AS `colb`
FROM VALUES
  (1, 'test'),
  (2, 'test2') AS `tab`(`cola`, `colb`);

# title: complex CTE dependencies
WITH m AS (
  SELECT a, b FROM (VALUES (1, 2)) AS a1(a, b)
), n AS (
  SELECT a, b FROM m WHERE m.a = 1
), o AS (
  SELECT a, b FROM m WHERE m.a = 2
) SELECT
    n.a,
    n.b,
    o.b
FROM n
FULL OUTER JOIN o ON n.a = o.a
CROSS JOIN n AS n2
WHERE o.b > 0 AND n.a = n2.a;
WITH "m" AS (
  SELECT
    "a1"."a" AS "a",
    "a1"."b" AS "b"
  FROM (VALUES
    (1, 2)) AS "a1"("a", "b")
), "n" AS (
  SELECT
    "m"."a" AS "a",
    "m"."b" AS "b"
  FROM "m"
  WHERE
    "m"."a" = 1
), "o" AS (
  SELECT
    "m"."a" AS "a",
    "m"."b" AS "b"
  FROM "m"
  WHERE
    "m"."a" = 2
)
SELECT
  "n"."a" AS "a",
  "n"."b" AS "b",
  "o"."b" AS "b"
FROM "n"
FULL JOIN "o"
  ON "n"."a" = "o"."a"
JOIN "n" AS "n2"
  ON "n"."a" = "n2"."a"
WHERE
  "o"."b" > 0;

# title: Broadcast hint
# dialect: spark
WITH m AS (
  SELECT
    x.a,
    x.b
  FROM x
), n AS (
  SELECT
    y.b,
    y.c
  FROM y
), joined as (
  SELECT /*+ BROADCAST(n) */
    m.a,
    n.c
  FROM m JOIN n ON m.b = n.b
)
SELECT
  joined.a,
  joined.c
FROM joined;
SELECT /*+ BROADCAST(`y`) */
  `x`.`a` AS `a`,
  `y`.`c` AS `c`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

# title: Mix Table and Column Hints
# dialect: spark
WITH m AS (
  SELECT
    x.a,
    x.b
  FROM x
), n AS (
  SELECT
    y.b,
    y.c
  FROM y
), joined as (
  SELECT /*+ BROADCAST(m), MERGE(m, n) */
    m.a,
    n.c
  FROM m JOIN n ON m.b = n.b
)
SELECT
  /*+ COALESCE(3) */
  joined.a,
  joined.c
FROM joined;
SELECT /*+ COALESCE(3),
  BROADCAST(`x`),
  MERGE(`x`, `y`) */
  `x`.`a` AS `a`,
  `y`.`c` AS `c`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

WITH cte1 AS (
  WITH cte2 AS (
    SELECT a, b FROM x
  )
  SELECT a1
  FROM (
    WITH cte3 AS (SELECT 1)
    SELECT a AS a1, b AS b1 FROM cte2
  )
)
SELECT a1 FROM cte1;
SELECT
  "x"."a" AS "a1"
FROM "x" AS "x";

# title: recursive cte
WITH RECURSIVE cte1 AS (
  SELECT *
  FROM (
      SELECT 1 AS a, 2 AS b
  ) base
  CROSS JOIN (SELECT 3 c) y
  UNION ALL
  SELECT *
  FROM cte1
  WHERE a < 1
)
SELECT *
FROM cte1;
WITH RECURSIVE "base" AS (
  SELECT
    1 AS "a",
    2 AS "b"
), "y" AS (
  SELECT
    3 AS "c"
), "cte1" AS (
  SELECT
    "base"."a" AS "a",
    "base"."b" AS "b",
    "y"."c" AS "c"
  FROM "base" AS "base"
  CROSS JOIN "y" AS "y"
  UNION ALL
  SELECT
    "cte1"."a" AS "a",
    "cte1"."b" AS "b",
    "cte1"."c" AS "c"
  FROM "cte1"
  WHERE
    "cte1"."a" < 1
)
SELECT
  "cte1"."a" AS "a",
  "cte1"."b" AS "b",
  "cte1"."c" AS "c"
FROM "cte1";

# title: right join should not push down to from
SELECT x.a, y.b
FROM x
RIGHT JOIN y
ON x.a = y.b
WHERE x.b = 1;
SELECT
  "x"."a" AS "a",
  "y"."b" AS "b"
FROM "x" AS "x"
RIGHT JOIN "y" AS "y"
  ON "x"."a" = "y"."b"
WHERE
  "x"."b" = 1;

# title: right join can push down to itself
SELECT x.a, y.b
FROM x
RIGHT JOIN y
ON x.a = y.b
WHERE y.b = 1;
WITH "y_2" AS (
  SELECT
    "y"."b" AS "b"
  FROM "y" AS "y"
  WHERE
    "y"."b" = 1
)
SELECT
  "x"."a" AS "a",
  "y"."b" AS "b"
FROM "x" AS "x"
RIGHT JOIN "y_2" AS "y"
  ON "x"."a" = "y"."b";


# title: lateral column alias reference
# execute: false
SELECT x.a + 1 AS c, c + 1 AS d FROM x;
SELECT
  "x"."a" + 1 AS "c",
  "x"."a" + 2 AS "d"
FROM "x" AS "x";

# title: column reference takes priority over lateral column alias reference
SELECT x.a + 1 AS b, b + 1 AS c FROM x;
SELECT
  "x"."a" + 1 AS "b",
  "x"."b" + 1 AS "c"
FROM "x" AS "x";
