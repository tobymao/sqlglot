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
  FROM (
    SELECT
      "x"."a" AS "a"
    FROM "x" AS "x"
    WHERE
      "x"."a" > 1
  ) AS "x"
  LEFT JOIN (
    SELECT
      MAX("y"."b") AS "_col_0",
      "y"."a" AS "_u_1"
    FROM "y" AS "y"
    GROUP BY
      "y"."a"
  ) AS "_u_0"
    ON "x"."a" = "_u_0"."_u_1"
  JOIN (
    SELECT
      "y"."a" AS "a",
      "y"."b" AS "b"
    FROM "y" AS "y"
  ) AS "y"
    ON "x"."a" = "y"."a"
  WHERE
    "_u_0"."_col_0" >= 0
    AND NOT "_u_0"."_u_1" IS NULL
) AS "d"
GROUP BY
  "d"."a";
