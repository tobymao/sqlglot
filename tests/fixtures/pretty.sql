SELECT * FROM test;
SELECT
  *
FROM test;
SELECT a, b c FROM (
    SELECT a w, 1 + 1 AS c
    FROM foo
    GROUP BY a, b
) x
LEFT JOIN (
    SELECT a, b
    FROM (SELECT * FROM bar GROUP BY a HAVING a > 1) z
) y ON x.a = y.b;
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
