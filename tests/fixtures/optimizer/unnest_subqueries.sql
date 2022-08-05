--------------------------------------
-- Unnest Subqueries
--------------------------------------
SELECT *
FROM x AS x
WHERE
  x.a IN (SELECT y.a AS a FROM y)
  AND x.a IN (SELECT y.b AS b FROM y)
  AND x.a = ANY (SELECT y.a AS a FROM y)
  AND x.a = (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a)
  AND x.a > (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a)
  AND x.a <> ANY (SELECT y.a AS a FROM y WHERE y.a = x.a)
  AND x.a NOT IN (SELECT y.a AS a FROM y WHERE y.a = x.a)
  AND x.a IN (SELECT y.a AS a FROM y WHERE y.b = x.a)
  AND x.a < (SELECT SUM(y.a) AS a FROM y WHERE y.a = x.a and y.a = x.b and y.b <> x.d)
  AND EXISTS (SELECT y.a AS a, y.b AS b FROM y WHERE x.a = y.a)
  AND x.a IN (SELECT y.a AS a FROM y LIMIT 10)
  AND x.a IN (SELECT y.a AS a FROM y OFFSET 10)
  AND x.a IN (SELECT y.a AS a, y.b AS b FROM y)
  AND x.a > ANY (SELECT y.a FROM y)
  AND x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a LIMIT 10)
  AND x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a OFFSET 10)
;
SELECT
  *
FROM x AS x
LEFT JOIN (
  SELECT
    y.a AS a
  FROM y
  GROUP BY
    y.a
) AS "_u_0"
  ON x.a = "_u_0"."a"
LEFT JOIN (
  SELECT
    y.b AS b
  FROM y
  GROUP BY
    y.b
) AS "_u_1"
  ON x.a = "_u_1"."b"
LEFT JOIN (
  SELECT
    y.a AS a
  FROM y
  GROUP BY
    y.a
) AS "_u_2"
  ON x.a = "_u_2"."a"
LEFT JOIN (
  SELECT
    SUM(y.b) AS b,
    y.a AS _u_4
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.a
) AS "_u_3"
  ON x.a = "_u_3"."_u_4"
LEFT JOIN (
  SELECT
    SUM(y.b) AS b,
    y.a AS _u_6
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.a
) AS "_u_5"
  ON x.a = "_u_5"."_u_6"
LEFT JOIN (
  SELECT
    y.a AS a
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.a
) AS "_u_7"
  ON "_u_7".a = x.a
LEFT JOIN (
  SELECT
    y.a AS a
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.a
) AS "_u_8"
  ON "_u_8".a = x.a
LEFT JOIN (
  SELECT
    ARRAY_AGG(y.a) AS a,
    y.b AS _u_10
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.b
) AS "_u_9"
  ON "_u_9"."_u_10" = x.a
LEFT JOIN (
  SELECT
    SUM(y.a) AS a,
    y.a AS _u_12,
    ARRAY_AGG(y.b) AS _u_13
  FROM y
  WHERE
    TRUE
    AND TRUE
    AND TRUE
  GROUP BY
    y.a
) AS "_u_11"
  ON "_u_11"."_u_12" = x.a
  AND "_u_11"."_u_12" = x.b
LEFT JOIN (
  SELECT
    y.a AS a
  FROM y
  WHERE
    TRUE
  GROUP BY
    y.a
) AS "_u_14"
  ON x.a = "_u_14".a
WHERE
  NOT "_u_0"."a" IS NULL
  AND NOT "_u_1"."b" IS NULL
  AND NOT "_u_2"."a" IS NULL
  AND (
    x.a = "_u_3".b
    AND NOT "_u_3"."_u_4" IS NULL
  )
  AND (
    x.a > "_u_5".b
    AND NOT "_u_5"."_u_6" IS NULL
  )
  AND (
    None = "_u_7".a
    AND NOT "_u_7".a IS NULL
  )
  AND NOT (
    x.a = "_u_8".a
    AND NOT "_u_8".a IS NULL
  )
  AND (
    ARRAY_ANY("_u_9".a, _x -> _x = x.a)
    AND NOT "_u_9"."_u_10" IS NULL
  )
  AND (
    (
      (
        x.a < "_u_11".a
        AND NOT "_u_11"."_u_12" IS NULL
      )
      AND NOT "_u_11"."_u_12" IS NULL
    )
    AND ARRAY_ANY("_u_11"."_u_13", "_x" -> "_x" <> x.d)
  )
  AND (
    NOT "_u_14".a IS NULL
    AND NOT "_u_14".a IS NULL
  )
  AND x.a IN (
    SELECT
      y.a AS a
    FROM y
    LIMIT 10
  )
  AND x.a IN (
    SELECT
      y.a AS a
    FROM y
    OFFSET 10
  )
  AND x.a IN (
    SELECT
      y.a AS a,
      y.b AS b
    FROM y
  )
  AND x.a > ANY (
    SELECT
      y.a
    FROM y
  )
  AND x.a = (
    SELECT
      SUM(y.c) AS c
    FROM y
    WHERE
      y.a = x.a
    LIMIT 10
  )
  AND x.a = (
    SELECT
      SUM(y.c) AS c
    FROM y
    WHERE
      y.a = x.a
    OFFSET 10
  );

