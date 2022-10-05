SELECT a, m FROM z LATERAL VIEW EXPLODE([1, 2]) q AS m;
SELECT
  "z"."a" AS "a",
  "q"."m" AS "m"
FROM "z" AS "z"
LATERAL VIEW
EXPLODE(ARRAY(1, 2)) q AS "m";

SELECT x FROM UNNEST([1, 2]) AS q(x, y);
SELECT
  "q"."x" AS "x"
FROM UNNEST(ARRAY(1, 2)) AS "q"("x", "y");

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
            a
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
      "y"."a" AS "a"
    FROM "y" AS "y"
  )
)
SELECT
  "cte"."a" AS "a"
FROM "cte";

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

SELECT a, SUM(b)
FROM (
    SELECT x.a, y.b
    FROM x, y
    WHERE (SELECT max(b) FROM y WHERE x.a = y.a) >= 0 AND x.a = y.a
) d
WHERE (TRUE AND TRUE OR 'a' = 'b') AND a > 1
GROUP BY a;
WITH "_u_0" AS (
  SELECT
    MAX("y"."b") AS "_col_0",
    "y"."a" AS "_u_1"
  FROM "y" AS "y"
  GROUP BY
    "y"."a"
)
SELECT
  "x"."a" AS "a",
  SUM("y"."b") AS "_col_1"
FROM "x" AS "x"
LEFT JOIN "_u_0" AS "_u_0"
  ON "x"."a" = "_u_0"."_u_1"
JOIN "y" AS "y"
  ON "x"."a" = "y"."a"
WHERE
  "_u_0"."_col_0" >= 0
  AND "x"."a" > 1
  AND NOT "_u_0"."_u_1" IS NULL
GROUP BY
  "x"."a";

(SELECT a FROM x) LIMIT 1;
(
  SELECT
    "x"."a" AS "a"
  FROM "x" AS "x"
)
LIMIT 1;

(SELECT b FROM x UNION SELECT b FROM y) LIMIT 1;
(
  SELECT
    "x"."b" AS "b"
  FROM "x" AS "x"
  UNION
  SELECT
    "y"."b" AS "b"
  FROM "y" AS "y"
)
LIMIT 1;

# dialect: spark
SELECT /*+ BROADCAST(y) */ x.b FROM x JOIN y ON x.b = y.b;
SELECT /*+ BROADCAST(`y`) */
  `x`.`b` AS `b`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

SELECT AGGREGATE(ARRAY(x.a, x.b), 0, (x, acc) -> x + acc + a) AS sum_agg FROM x;
SELECT
  AGGREGATE(ARRAY("x"."a", "x"."b"), 0, ("x", "acc") -> "x" + "acc" + "x"."a") AS "sum_agg"
FROM "x" AS "x";

SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  "tab"."cola" AS "cola",
  "tab"."colb" AS "colb"
FROM (VALUES
  (1, 'test'),
  (2, 'test2')) AS "tab"("cola", "colb");

# dialect: spark
SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  `tab`.`cola` AS `cola`,
  `tab`.`colb` AS `colb`
FROM VALUES
  (1, 'test'),
  (2, 'test2') AS `tab`(`cola`, `colb`);

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
