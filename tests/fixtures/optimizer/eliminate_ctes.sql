# title: CTE
WITH q AS (
  SELECT
    a
  FROM x
)
SELECT
  a
FROM x;
SELECT
  a
FROM x;

# title: Nested CTE
SELECT
  a
FROM (
  WITH q AS (
    SELECT
      a
    FROM x
  )
  SELECT a FROM x
);
SELECT
  a
FROM (
  SELECT
    a
  FROM x
);

# title: Chained CTE
WITH q AS (
  SELECT
    a
  FROM x
), r AS (
  SELECT
    a
  FROM q
)
SELECT
  a
FROM x;
SELECT
  a
FROM x;
