

# title: Remove left join on FROM-less subquery with WHERE
# execute: true
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    1 AS b
  WHERE
    FALSE
) AS y
  ON x.a = y.b;
SELECT
  x.a
FROM x;

# title: Noop - left join on non-unique subquery
# execute: true
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b AS b
  FROM y
) AS y
  ON x.a = y.b;
SELECT
  x.a
FROM x
LEFT JOIN (
  SELECT
    y.b AS b
  FROM y
) AS y
  ON x.a = y.b;
