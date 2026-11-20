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


# Title: Do not remove CTE if it is an RHS of a SEMI/ANTI join
WITH t1 AS (
  SELECT
    1 AS foo
), t2 AS (
  SELECT
    1 AS foo
)
SELECT
  *
FROM t1
LEFT ANTI JOIN t2
  ON t1.foo = t2.foo;
WITH t1 AS (
  SELECT
    1 AS foo
), t2 AS (
  SELECT
    1 AS foo
)
SELECT
  *
FROM t1
LEFT ANTI JOIN t2
  ON t1.foo = t2.foo
