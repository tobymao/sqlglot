SELECT a, SUM(b)
FROM (
    SELECT x.a, y.b
    FROM x, y
    WHERE (SELECT max(b) FROM y WHERE x.a = y.a) >= 0 AND x.a = y.a
) d
WHERE (TRUE AND TRUE OR 'a' = 'b') AND a > 1
GROUP BY a;
SELECT
  "d"."a" AS "a",
  SUM("d"."b") AS "_col_1"
FROM (
    SELECT
      "x"."a" AS "a",
      "y"."b" AS "b"
    FROM "x" AS "x"
    JOIN (
        SELECT
          MAX("y"."b") AS "_col_0",
          "y"."a"
        FROM "y" AS "y"
        GROUP BY
          "y"."a"
    ) AS "_d_0"
      ON "_d_0"."a" = "x"."a"
      AND "_d_0"."_col_0" >= 0
    JOIN "y" AS "y"
      ON "x"."a" = "y"."a"
) AS "d"
WHERE
  "d"."a" > 1
GROUP BY
  "d"."a";
SELECT x.a, SUM(y.b)
FROM x, y
WHERE
  x.a = y.a
  AND y.b = (
    SELECT y.b
    FROM y, z
    WHERE
      x.b = y.b
      AND y.b = z.c
  )
GROUP BY x.a;
SELECT
  "x"."a" AS "a",
  SUM("y"."b") AS "_col_1"
FROM "x" AS "x"
JOIN (
    SELECT
      "y"."b" AS "b"
    FROM "y" AS "y"
    JOIN "z" AS "z"
      ON "y"."b" = "z"."c"
    GROUP BY
      "y"."b"
) AS "_d_0"
  ON "_d_0"."b" = "x"."b"
  AND "y"."b" = "_d_0"."b"
JOIN "y" AS "y"
  ON "x"."a" = "y"."a"
GROUP BY
  "x"."a";
