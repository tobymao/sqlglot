# title: Remove left join on distinct derived table
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x;

# title: Remove left join on grouped derived table
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b,
    SUM(y.c)
  FROM y
  GROUP BY y.b
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x;

# title: Remove left join on aggregate derived table
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    SUM(y.b) AS b
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x;

# title: Noop - not all distinct columns in condition
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b,
    y.c
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b,
    y.c
  FROM y
) AS y
  ON x.b = y.b;

# title: Noop - not all grouped columns in condition
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b,
    y.c
  FROM y
  GROUP BY
    y.b,
    y.c
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b,
    y.c
  FROM y
  GROUP BY
    y.b,
    y.c
) AS y
  ON x.b = y.b;

# title: Noop - not left join
SELECT
  x.a
FROM x
JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x
JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;

# title: Noop - unqualified columns
SELECT
  a
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  a
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;

# title: Noop - cross join
SELECT
  a
FROM x
CROSS JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y;
SELECT
  a
FROM x
CROSS JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y;

# title: Noop - column is used
SELECT
  x.a,
  y.b
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;
SELECT
  x.a,
  y.b
FROM x
LEFT JOIN (
  SELECT DISTINCT
    y.b
  FROM y
) AS y
  ON x.b = y.b;

# title: Multiple group by columns
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b AS b,
    y.c + 1 AS d,
    COUNT(1)
  FROM y
  GROUP BY y.b, y.c + 1
) AS y
  ON x.b = y.b
  AND 1 = y.d;
SELECT
  x.a
FROM x;

# title: Chained left joins
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b AS b
  FROM y
  GROUP BY y.b
) AS y
  ON x.b = y.b
LEFT JOIN (
  SELECT
    y.b AS c
  FROM y
  GROUP BY y.b
) AS z
  ON y.b = z.c;
SELECT
  x.a
FROM x;

# title: CTE
WITH z AS (
  SELECT DISTINCT
    y.b
  FROM y
)
SELECT
  x.a
FROM x
LEFT JOIN z
  ON x.b = z.b;
WITH z AS (
  SELECT DISTINCT
    y.b
  FROM y
)
SELECT
  x.a
FROM x;

# title: Noop - Not all grouped expressions are in outputs
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b
  FROM y
  GROUP BY
    y.b,
    y.c
) AS y
  ON x.b = y.b;
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b
  FROM y
  GROUP BY
    y.b,
    y.c
) AS y
  ON x.b = y.b;

# title: Cross join on aggregate derived table
SELECT
  x.a
FROM x
CROSS JOIN (
  SELECT
    SUM(y.b) AS b
  FROM y
) AS y;
SELECT
  x.a
FROM x;

# title: Cross join on derived table with LIMIT 1
SELECT
  x.a
FROM x
CROSS JOIN (
  SELECT
    y.b AS b
  FROM y
  LIMIT 1
) AS y;
SELECT
  x.a
FROM x;

# title: Cross join on derived table with no FROM clause
SELECT
  x.a
FROM x
CROSS JOIN (
  SELECT
    1 AS b,
    2 AS c
) AS y;
SELECT
  x.a
FROM x;

# title: Noop - cross join on non-aggregate subquery
SELECT
  x.a
FROM x
CROSS JOIN (
  SELECT
    y.b
  FROM y
) AS y;
SELECT
  x.a
FROM x
CROSS JOIN (
  SELECT
    y.b
  FROM y
) AS y;
