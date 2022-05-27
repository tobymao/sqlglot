SELECT a, SUM(b)
FROM (
    SELECT x.a, b
    FROM x, y
    WHERE (SELECT max(b) FROM y WHERE x.a = y.a) >= 0 AND x.a = y.a
) d
WHERE TRUE AND TRUE OR 'a' = 'b'
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
        WHERE
          TRUE
        GROUP BY
          "y"."a"
    ) AS "_d_0"
      ON "_d_0"."a" = "x"."a"
      AND "_d_0"."_col_0" >= 0
    JOIN "y" AS "y"
      ON "x"."a" = "y"."a"
    WHERE
      TRUE
) AS "d"
WHERE
  TRUE
GROUP BY
  "d"."a";
