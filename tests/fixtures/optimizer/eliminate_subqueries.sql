SELECT 1 AS x, 2 AS y
UNION ALL
SELECT 1 AS x, 2 AS y;
WITH _e_0 AS (
  SELECT
    1 AS x,
    2 AS y
)
SELECT
  *
FROM _e_0
UNION ALL
SELECT
  *
FROM _e_0;

SELECT x.id
FROM (
    SELECT *
    FROM x AS x
    JOIN y AS y
      ON x.id = y.id
) AS x
JOIN (
    SELECT *
    FROM x AS x
    JOIN y AS y
      ON x.id = y.id
) AS y
ON x.id = y.id;
WITH _e_0 AS (
  SELECT
    *
  FROM x AS x
  JOIN y AS y
    ON x.id = y.id
)
SELECT
  x.id
FROM _e_0 AS x
JOIN _e_0 AS y
  ON x.id = y.id;
