SELECT * FROM test;
SELECT
  *
FROM test;
WITH cte1 AS (
    SELECT a
    FROM cte
    WHERE x IN (1, 2, 3)
), cte2 AS (
    SELECT RANK() OVER(PARTITION BY a, b ORDER BY x DESC) a, b
    FROM cte
    CROSS JOIN (
        SELECT 1
        UNION ALL
        SELECT 2
    ) x
)
SELECT a, b c FROM (
    SELECT a w, 1 + 1 AS c
    FROM foo
    GROUP BY a, b
) x
LEFT JOIN (
    SELECT a, b
    FROM (SELECT * FROM bar GROUP BY a HAVING a > 1) z
) y ON x.a = y.b;
WITH cte1 AS (
    SELECT
      a
    FROM cte
    WHERE
      x IN (1, 2, 3)
), cte2 AS (
    SELECT
      RANK() OVER(PARTITION BY a, b ORDER BY x DESC) AS a,
      b
    FROM cte
    CROSS JOIN (
        SELECT
          1
        UNION ALL
        SELECT
          2
    ) AS x
)
SELECT
  a,
  b AS c
FROM (
    SELECT
      a AS w,
      1 + 1 AS c
    FROM foo
    GROUP BY
      a,
      b
) AS x
LEFT JOIN (
    SELECT
      a,
      b
    FROM (
        SELECT
          *
        FROM bar
        GROUP BY
          a
        HAVING
          a > 1
    ) AS z
) AS y ON
  x.a = y.b;
