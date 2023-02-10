# title: expand alias reference
SELECT
  x.a + 1 AS i,
  i + 1 AS j,
  j + 1 AS k
FROM x;
SELECT
  x.a + 1 AS i,
  x.a + 1 + 1 AS j,
  x.a + 1 + 1 + 1 AS k
FROM x;

# title: noop - reference comes before alias
SELECT
  b + 1 AS j,
  x.a + 1 AS i
FROM x;
SELECT
  b + 1 AS j,
  x.a + 1 AS i
FROM x;


# title: subquery
SELECT
  *
FROM (
  SELECT
    x.a + 1 AS i,
    i + 1 AS j
  FROM x
);
SELECT
  *
FROM (
  SELECT
    x.a + 1 AS i,
    x.a + 1 + 1 AS j
  FROM x
);
