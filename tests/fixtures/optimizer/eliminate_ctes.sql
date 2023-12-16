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

# title: CTE reference in subquery where alias matches outer table name
WITH q AS (
  SELECT
    a
  FROM y
)
SELECT
  a
FROM x AS q
WHERE
  a IN (
    SELECT
      a
    FROM q
  );
WITH q AS (
  SELECT
    a
  FROM y
)
SELECT
  a
FROM x AS q
WHERE
  a IN (
    SELECT
      a
    FROM q
  );

# title: CTE reference in subquery where alias matches outer table name and outer alias is also CTE
WITH q AS (
  SELECT
    a
  FROM y
), q2 AS (
  SELECT
    a
  FROM y
)
SELECT
  a
FROM q2 AS q
WHERE
  a IN (
    SELECT
      a
    FROM q
  );
WITH q AS (
  SELECT
    a
  FROM y
), q2 AS (
  SELECT
    a
  FROM y
)
SELECT
  a
FROM q2 AS q
WHERE
  a IN (
    SELECT
      a
    FROM q
  );