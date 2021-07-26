SELECT * FROM test;
SELECT
  *
FROM test;
WITH cte1 AS (
    SELECT a, z and e AS b
    FROM cte
    WHERE x IN (1, 2, 3) AND z < -1 OR z > 1 AND w = 'AND'
), cte2 AS (
    SELECT RANK() OVER(PARTITION BY a, b ORDER BY x DESC) a, b
    FROM cte
    CROSS JOIN (
        SELECT 1
        UNION ALL
        SELECT 2
        UNION ALL
        SELECT CASE x AND 1 + 1 = 2
        WHEN true THEN 1
        WHEN x and y THEN 2
        ELSE 3 END
        UNION ALL
        SELECT 1
        FROM (SELECT 1) AS x, y, (SELECT 2) z
    ) x
)
SELECT a, b c FROM (
    SELECT a w, 1 + 1 AS c
    FROM foo
    GROUP BY a, b
) x
LEFT JOIN (
    SELECT a, b
    FROM (SELECT * FROM bar WHERE (c > 1 AND d > 1) OR e > 1 GROUP BY a HAVING a > 1 LIMIT 10) z
) y ON x.a = y.b AND x.a > 1 OR (x.c = y.d OR x.c = y.e);
WITH cte1 AS (
    SELECT
      a,
      z AND e AS b
    FROM cte
    WHERE
      x IN (1, 2, 3)
      AND z < -1
      OR z > 1
      AND w = 'AND'
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
        UNION ALL
        SELECT
          CASE x AND 1 + 1 = 2
            WHEN true THEN 1
            WHEN x AND y THEN 2
            ELSE 3
          END
        UNION ALL
        SELECT
          1
        FROM (
            SELECT
              1
        ) AS x, y, (
            SELECT
              2
        ) AS z
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
        WHERE
          (c > 1 AND d > 1)
          OR e > 1
        GROUP BY
          a
        HAVING
          a > 1
        LIMIT 10
    ) AS z
) AS y
  ON x.a = y.b
  AND x.a > 1
  OR (x.c = y.d OR x.c = y.e);
SELECT myCol1, myCol2 FROM baseTable LATERAL VIEW OUTER explode(col1) myTable1 AS myCol1 LATERAL VIEW explode(col2) myTable2 AS myCol2
where a > 1 and b > 2 or c > 3;
SELECT
  myCol1,
  myCol2
FROM baseTable
LATERAL VIEW OUTER
EXPLODE(col1) myTable1 AS myCol1
LATERAL VIEW
EXPLODE(col2) myTable2 AS myCol2
WHERE
  a > 1
  AND b > 2
  OR c > 3;
SELECT * FROM (WITH y AS ( SELECT 1 AS z) SELECT z from y) x;
SELECT
  *
FROM (
    WITH y AS (
        SELECT
          1 AS z
    )
    SELECT
      z
    FROM y
) AS x;
INSERT OVERWRITE TABLE x VALUES (1, 2.0, '3.0'), (4, 5.0, '6.0');
INSERT OVERWRITE TABLE x VALUES
  (1, 2.0, '3.0'),
  (4, 5.0, '6.0');
