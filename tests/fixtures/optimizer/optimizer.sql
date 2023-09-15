# title: lateral
# execute: false
SELECT a, m FROM z LATERAL VIEW EXPLODE([1, 2]) q AS m;
SELECT
  "z"."a" AS "a",
  "q"."m" AS "m"
FROM "z" AS "z"
LATERAL VIEW
EXPLODE(ARRAY(1, 2)) q AS "m";

# title: unnest
# execute: false
SELECT x FROM UNNEST([1, 2]) AS q(x, y);
SELECT
  "q"."x" AS "x"
FROM UNNEST(ARRAY(1, 2)) AS "q"("x", "y");

# title: Union in CTE
WITH cte AS (
    (
        SELECT
            a
            FROM
            x
    )
    UNION ALL
    (
        SELECT
            b AS a
        FROM
            y
    )
)
SELECT
    *
FROM
    cte;
WITH "cte" AS (
  (
    SELECT
      "x"."a" AS "a"
    FROM "x" AS "x"
  )
  UNION ALL
  (
    SELECT
      "y"."b" AS "a"
    FROM "y" AS "y"
  )
)
SELECT
  "cte"."a" AS "a"
FROM "cte";

# title: Chained CTEs
WITH cte1 AS (
    SELECT a
    FROM x
), cte2 AS (
    SELECT a + 1 AS a
    FROM cte1
)
SELECT
    a
FROM cte1
UNION ALL
SELECT
    a
FROM cte2;
WITH "cte1" AS (
  SELECT
    "x"."a" AS "a"
  FROM "x" AS "x"
)
SELECT
  "cte1"."a" AS "a"
FROM "cte1"
UNION ALL
SELECT
  "cte1"."a" + 1 AS "a"
FROM "cte1";

# title: Correlated subquery
SELECT a, SUM(b) AS sum_b
FROM (
    SELECT x.a, y.b
    FROM x, y
    WHERE (SELECT max(b) FROM y WHERE x.b = y.b) >= 0 AND x.b = y.b
) d
WHERE (TRUE AND TRUE OR 'a' = 'b') AND a > 1
GROUP BY a;
WITH "_u_0" AS (
  SELECT
    MAX("y"."b") AS "_col_0",
    "y"."b" AS "_u_1"
  FROM "y" AS "y"
  GROUP BY
    "y"."b"
)
SELECT
  "x"."a" AS "a",
  SUM("y"."b") AS "sum_b"
FROM "x" AS "x"
LEFT JOIN "_u_0" AS "_u_0"
  ON "x"."b" = "_u_0"."_u_1"
JOIN "y" AS "y"
  ON "x"."b" = "y"."b"
WHERE
  "_u_0"."_col_0" >= 0 AND "x"."a" > 1
GROUP BY
  "x"."a";

# title: Root subquery
(SELECT a FROM x) LIMIT 1;
(
  SELECT
    "x"."a" AS "a"
  FROM "x" AS "x"
)
LIMIT 1;

# title: Root subquery is union
(SELECT b FROM x UNION SELECT b FROM y ORDER BY b) LIMIT 1;
(
  SELECT
    "x"."b" AS "b"
  FROM "x" AS "x"
  UNION
  SELECT
    "y"."b" AS "b"
  FROM "y" AS "y"
  ORDER BY
    "b"
)
LIMIT 1;

# title: broadcast
# dialect: spark
SELECT /*+ BROADCAST(y) */ x.b FROM x JOIN y ON x.b = y.b;
SELECT /*+ BROADCAST(`y`) */
  `x`.`b` AS `b`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

# title: aggregate
# execute: false
SELECT AGGREGATE(ARRAY(x.a, x.b), 0, (x, acc) -> x + acc + a) AS sum_agg FROM x;
SELECT
  AGGREGATE(ARRAY("x"."a", "x"."b"), 0, ("x", "acc") -> "x" + "acc" + "x"."a") AS "sum_agg"
FROM "x" AS "x";

# title: values
SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  "tab"."cola" AS "cola",
  "tab"."colb" AS "colb"
FROM (VALUES
  (1, 'test'),
  (2, 'test2')) AS "tab"("cola", "colb");

# title: spark values
# dialect: spark
SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb);
SELECT
  `tab`.`cola` AS `cola`,
  `tab`.`colb` AS `colb`
FROM VALUES
  (1, 'test'),
  (2, 'test2') AS `tab`(`cola`, `colb`);

# title: complex CTE dependencies
WITH m AS (
  SELECT a, b FROM (VALUES (1, 2)) AS a1(a, b)
), n AS (
  SELECT a, b FROM m WHERE m.a = 1
), o AS (
  SELECT a, b FROM m WHERE m.a = 2
) SELECT
    n.a,
    n.b,
    o.b
FROM n
FULL OUTER JOIN o ON n.a = o.a
CROSS JOIN n AS n2
WHERE o.b > 0 AND n.a = n2.a;
WITH "m" AS (
  SELECT
    "a1"."a" AS "a",
    "a1"."b" AS "b"
  FROM (VALUES
    (1, 2)) AS "a1"("a", "b")
), "n" AS (
  SELECT
    "m"."a" AS "a",
    "m"."b" AS "b"
  FROM "m"
  WHERE
    "m"."a" = 1
), "o" AS (
  SELECT
    "m"."a" AS "a",
    "m"."b" AS "b"
  FROM "m"
  WHERE
    "m"."a" = 2
)
SELECT
  "n"."a" AS "a",
  "n"."b" AS "b",
  "o"."b" AS "b"
FROM "n"
JOIN "n" AS "n2"
  ON "n"."a" = "n2"."a"
FULL JOIN "o"
  ON "n"."a" = "o"."a"
WHERE
  "o"."b" > 0;

# title: Broadcast hint
# dialect: spark
WITH m AS (
  SELECT
    x.a,
    x.b
  FROM x
), n AS (
  SELECT
    y.b,
    y.c
  FROM y
), joined as (
  SELECT /*+ BROADCAST(n) */
    m.a,
    n.c
  FROM m JOIN n ON m.b = n.b
)
SELECT
  joined.a,
  joined.c
FROM joined;
SELECT /*+ BROADCAST(`y`) */
  `x`.`a` AS `a`,
  `y`.`c` AS `c`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

# title: Mix Table and Column Hints
# dialect: spark
WITH m AS (
  SELECT
    x.a,
    x.b
  FROM x
), n AS (
  SELECT
    y.b,
    y.c
  FROM y
), joined as (
  SELECT /*+ BROADCAST(m), MERGE(m, n) */
    m.a,
    n.c
  FROM m JOIN n ON m.b = n.b
)
SELECT
  /*+ COALESCE(3) */
  joined.a,
  joined.c
FROM joined;
SELECT /*+ COALESCE(3),
  BROADCAST(`x`),
  MERGE(`x`, `y`) */
  `x`.`a` AS `a`,
  `y`.`c` AS `c`
FROM `x` AS `x`
JOIN `y` AS `y`
  ON `x`.`b` = `y`.`b`;

WITH cte1 AS (
  WITH cte2 AS (
    SELECT a, b FROM x
  )
  SELECT a1
  FROM (
    WITH cte3 AS (SELECT 1)
    SELECT a AS a1, b AS b1 FROM cte2
  )
)
SELECT a1 FROM cte1;
SELECT
  "x"."a" AS "a1"
FROM "x" AS "x";

# title: recursive cte
WITH RECURSIVE cte1 AS (
  SELECT *
  FROM (
      SELECT 1 AS a, 2 AS b
  ) base
  CROSS JOIN (SELECT 3 c) y
  UNION ALL
  SELECT *
  FROM cte1
  WHERE a < 1
)
SELECT *
FROM cte1;
WITH RECURSIVE "base" AS (
  SELECT
    1 AS "a",
    2 AS "b"
), "y" AS (
  SELECT
    3 AS "c"
), "cte1" AS (
  SELECT
    "base"."a" AS "a",
    "base"."b" AS "b",
    "y"."c" AS "c"
  FROM "base" AS "base"
  CROSS JOIN "y" AS "y"
  UNION ALL
  SELECT
    "cte1"."a" AS "a",
    "cte1"."b" AS "b",
    "cte1"."c" AS "c"
  FROM "cte1"
  WHERE
    "cte1"."a" < 1
)
SELECT
  "cte1"."a" AS "a",
  "cte1"."b" AS "b",
  "cte1"."c" AS "c"
FROM "cte1";

# title: right join should not push down to from
SELECT x.a, y.b
FROM x
RIGHT JOIN y
ON x.a = y.b
WHERE x.b = 1;
SELECT
  "x"."a" AS "a",
  "y"."b" AS "b"
FROM "x" AS "x"
RIGHT JOIN "y" AS "y"
  ON "x"."a" = "y"."b"
WHERE
  "x"."b" = 1;

# title: right join can push down to itself
SELECT x.a, y.b
FROM x
RIGHT JOIN y
ON x.a = y.b
WHERE y.b = 1;
WITH "y_2" AS (
  SELECT
    "y"."b" AS "b"
  FROM "y" AS "y"
  WHERE
    "y"."b" = 1
)
SELECT
  "x"."a" AS "a",
  "y"."b" AS "b"
FROM "x" AS "x"
RIGHT JOIN "y_2" AS "y"
  ON "x"."a" = "y"."b";


# title: lateral column alias reference
SELECT x.a + 1 AS c, c + 1 AS d FROM x;
SELECT
  "x"."a" + 1 AS "c",
  "x"."a" + 2 AS "d"
FROM "x" AS "x";

# title: column reference takes priority over lateral column alias reference
SELECT x.a + 1 AS b, b + 1 AS c FROM x;
SELECT
  "x"."a" + 1 AS "b",
  "x"."b" + 1 AS "c"
FROM "x" AS "x";

# title: unqualified struct element is selected in the outer query
# execute: false
WITH "cte" AS (
  SELECT
    FROM_JSON("value", 'STRUCT<f1: STRUCT<f2: STRUCT<f3: STRUCT<f4: STRING>>>>') AS "struct"
  FROM "tbl"
) SELECT "struct"."f1"."f2"."f3"."f4" AS "f4" FROM "cte";
SELECT
  FROM_JSON("tbl"."value", 'STRUCT<f1: STRUCT<f2: STRUCT<f3: STRUCT<f4: STRING>>>>')."f1"."f2"."f3"."f4" AS "f4"
FROM "tbl" AS "tbl";

# title: qualified struct element is selected in the outer query
# execute: false
WITH "cte" AS (
  SELECT
    FROM_JSON("value", 'STRUCT<f1: STRUCT<f2: INTEGER>, STRUCT<f3: STRING>>') AS "struct"
  FROM "tbl"
) SELECT "cte"."struct"."f1"."f2" AS "f2", "cte"."struct"."f1"."f3" AS "f3" FROM "cte";
SELECT
  FROM_JSON("tbl"."value", 'STRUCT<f1: STRUCT<f2: INTEGER>, STRUCT<f3: STRING>>')."f1"."f2" AS "f2",
  FROM_JSON("tbl"."value", 'STRUCT<f1: STRUCT<f2: INTEGER>, STRUCT<f3: STRING>>')."f1"."f3" AS "f3"
FROM "tbl" AS "tbl";

# title: left join doesnt push down predicate to join in merge subqueries
# execute: false
SELECT
  main_query.id,
  main_query.score
FROM (
  SELECT
    alias_1.id,
    score
  FROM (
    SELECT
      company_table.score AS score,
      id
    FROM company_table
  ) AS alias_1
  JOIN (
    SELECT
      id
    FROM (
      SELECT
        company_table_2.id,
        CASE WHEN unlocked.company_id IS NULL THEN 0 ELSE 1 END AS is_exported
      FROM company_table AS company_table_2
      LEFT JOIN unlocked AS unlocked
        ON company_table_2.id = unlocked.company_id
    )
    WHERE
      NOT id IS NULL AND is_exported = FALSE
  ) AS alias_2
    ON (
      alias_1.id = alias_2.id
    )
) AS main_query;
SELECT
  "company_table"."id" AS "id",
  "company_table"."score" AS "score"
FROM "company_table" AS "company_table"
JOIN "company_table" AS "company_table_2"
  ON "company_table"."id" = "company_table_2"."id"
LEFT JOIN "unlocked" AS "unlocked"
  ON "company_table_2"."id" = "unlocked"."company_id"
WHERE
  CASE WHEN "unlocked"."company_id" IS NULL THEN 0 ELSE 1 END = FALSE
  AND NOT "company_table_2"."id" IS NULL;

# title: db.table alias clash
# execute: false
select * from db1.tbl, db2.tbl;
SELECT
  *
FROM "db1"."tbl" AS "tbl"
CROSS JOIN "db2"."tbl" AS "tbl_2";

# execute: false
SELECT
*,
IFF(
  IFF(
	uploaded_at >= '2022-06-16',
	'workday',
	'bamboohr'
  ) = source_system,
  1,
  0
) AS sort_order
FROM
unioned
WHERE
(
  source_system = 'workday'
  AND '9999-01-01' >= '2022-06-16'
)
OR (
  source_system = 'bamboohr'
  AND '0001-01-01' < '2022-06-16'
) QUALIFY ROW_NUMBER() OVER (
  PARTITION BY unique_filter_key
  ORDER BY
	sort_order DESC,
	1
) = 1;
SELECT
  *,
  IFF(
    IFF("unioned"."uploaded_at" >= '2022-06-16', 'workday', 'bamboohr') = "unioned"."source_system",
    1,
    0
  ) AS "sort_order"
FROM "unioned" AS "unioned"
WHERE
  "unioned"."source_system" = 'bamboohr' OR "unioned"."source_system" = 'workday'
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY "unioned"."unique_filter_key" ORDER BY "unioned"."sort_order" DESC, 1) = 1;

# title: pivoted source with explicit selections
# execute: false
SELECT * FROM (SELECT a, b, c FROM sc.tb) PIVOT (SUM(c) FOR b IN ('x','y','z'));
SELECT
  "_q_1"."a" AS "a",
  "_q_1"."x" AS "x",
  "_q_1"."y" AS "y",
  "_q_1"."z" AS "z"
FROM (
  SELECT
    "tb"."a" AS "a",
    "tb"."b" AS "b",
    "tb"."c" AS "c"
  FROM "sc"."tb" AS "tb"
) AS "_q_0" PIVOT(SUM("_q_0"."c") FOR "_q_0"."b" IN ('x', 'y', 'z')) AS "_q_1";

# title: pivoted source with implicit selections
# execute: false
SELECT * FROM (SELECT * FROM u) PIVOT (SUM(f) FOR h IN ('x', 'y'));
SELECT
  "_q_1"."g" AS "g",
  "_q_1"."x" AS "x",
  "_q_1"."y" AS "y"
FROM (
  SELECT
    "u"."f" AS "f",
    "u"."g" AS "g",
    "u"."h" AS "h"
  FROM "u" AS "u"
) AS "_q_0" PIVOT(SUM("_q_0"."f") FOR "_q_0"."h" IN ('x', 'y')) AS "_q_1";

# title: selecting explicit qualified columns from pivoted source with explicit selections
# execute: false
SELECT piv.x, piv.y FROM (SELECT f, h FROM u) PIVOT (SUM(f) FOR h IN ('x', 'y')) AS piv;
SELECT
  "piv"."x" AS "x",
  "piv"."y" AS "y"
FROM (
  SELECT
    "u"."f" AS "f",
    "u"."h" AS "h"
  FROM "u" AS "u"
) AS "_q_0" PIVOT(SUM("_q_0"."f") FOR "_q_0"."h" IN ('x', 'y')) AS "piv";

# title: selecting explicit unqualified columns from pivoted source with implicit selections
# execute: false
SELECT x, y FROM u PIVOT (SUM(f) FOR h IN ('x', 'y'));
SELECT
  "_q_0"."x" AS "x",
  "_q_0"."y" AS "y"
FROM "u" AS "u" PIVOT(SUM("u"."f") FOR "u"."h" IN ('x', 'y')) AS "_q_0";

# title: selecting all columns from a pivoted CTE source, using alias for the aggregation and generating bigquery
# execute: false
# dialect: bigquery
WITH u_cte(f, g, h) AS (SELECT * FROM u) SELECT * FROM u_cte PIVOT(SUM(f) AS sum FOR h IN ('x', 'y'));
WITH `u_cte` AS (
  SELECT
    `u`.`f` AS `f`,
    `u`.`g` AS `g`,
    `u`.`h` AS `h`
  FROM `u` AS `u`
)
SELECT
  `_q_0`.`g` AS `g`,
  `_q_0`.`sum_x` AS `sum_x`,
  `_q_0`.`sum_y` AS `sum_y`
FROM `u_cte` AS `u_cte` PIVOT(SUM(`u_cte`.`f`) AS `sum` FOR `u_cte`.`h` IN ('x', 'y')) AS `_q_0`;

# title: selecting all columns from a pivoted source and generating snowflake
# execute: false
# dialect: snowflake
SELECT * FROM u PIVOT (SUM(f) FOR h IN ('x', 'y'));
SELECT
  "_q_0"."G" AS "G",
  "_q_0"."'x'" AS "'x'",
  "_q_0"."'y'" AS "'y'"
FROM "U" AS "U" PIVOT(SUM("U"."F") FOR "U"."H" IN ('x', 'y')) AS "_q_0"
;

# title: selecting all columns from a pivoted source and generating spark
# note: spark doesn't allow pivot aliases or qualified columns for the pivot's "field" (`h`)
# execute: false
# dialect: spark
SELECT * FROM u PIVOT (SUM(f) FOR h IN ('x', 'y'));
SELECT
  `_q_0`.`g` AS `g`,
  `_q_0`.`x` AS `x`,
  `_q_0`.`y` AS `y`
FROM (
  SELECT
    *
  FROM `u` AS `u` PIVOT(SUM(`u`.`f`) FOR `h` IN ('x', 'y'))
) AS `_q_0`;

# title: quoting is maintained
# dialect: snowflake
with cte1("id", foo) as (select 1, 2) select "id" from cte1;
WITH "CTE1" AS (
  SELECT
    1 AS "id"
)
SELECT
  "CTE1"."id" AS "id"
FROM "CTE1";

# title: ensures proper quoting happens after all optimizations
# execute: false
SELECT "foO".x FROM (SELECT 1 AS x) AS "foO";
WITH "foO" AS (
  SELECT
    1 AS "x"
)
SELECT
  "foO"."x" AS "x"
FROM "foO" AS "foO";

# title: lateral subquery
# execute: false
# dialect: postgres
SELECT u.user_id, l.log_date
FROM   users u
CROSS JOIN LATERAL (
    SELECT l.log_date
    FROM   logs l
    WHERE  l.user_id = u.user_id AND l.log_date <= 100
    ORDER  BY l.log_date DESC NULLS LAST
    LIMIT  1
) l;
SELECT
  "u"."user_id" AS "user_id",
  "l"."log_date" AS "log_date"
FROM "users" AS "u"
CROSS JOIN LATERAL (
  SELECT
    "l"."log_date" AS "log_date"
  FROM "logs" AS "l"
  WHERE
    "l"."log_date" <= 100 AND "l"."user_id" = "u"."user_id"
  ORDER BY
    "l"."log_date" DESC NULLS LAST
  LIMIT 1
) AS "l";

# title: bigquery column identifiers are case-insensitive
# execute: false
# dialect: bigquery
WITH cte AS (
    SELECT
        refresh_date AS `reFREsh_date`,
        term AS `TeRm`,
        `rank`
    FROM `bigquery-public-data.GooGle_tReNDs.TOp_TeRmS`
)
SELECT
   refresh_date AS `Day`,
   term AS Top_Term,
   rank,
FROM cte
WHERE
   rank = 1
   AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)
GROUP BY `dAy`, `top_term`, rank
ORDER BY `DaY` DESC;
SELECT
  `TOp_TeRmS`.`refresh_date` AS `day`,
  `TOp_TeRmS`.`term` AS `top_term`,
  `TOp_TeRmS`.`rank` AS `rank`
FROM `bigquery-public-data`.`GooGle_tReNDs`.`TOp_TeRmS` AS `TOp_TeRmS`
WHERE
  `TOp_TeRmS`.`rank` = 1
  AND CAST(`TOp_TeRmS`.`refresh_date` AS DATE) >= DATE_SUB(CURRENT_DATE, INTERVAL 2 WEEK)
GROUP BY
  `day`,
  `top_term`,
  `rank`
ORDER BY
  `day` DESC;


# title: group by keys cannot be simplified
SELECT a + 1 + 1 + 1 + 1 AS b, 2 + 1 AS c FROM x GROUP BY a + 1 + 1 HAVING a + 1 + 1 + 1 + 1 > 1;
SELECT
  "x"."a" + 1 + 1 + 1 + 1 AS "b",
  3 AS "c"
FROM "x" AS "x"
GROUP BY
  "x"."a" + 1 + 1
HAVING
  "x"."a" + 1 + 1 + 1 + 1 > 1;

# title: replace alias with mult expression without wrapping it
WITH cte AS (SELECT a * b AS c, a AS d, b as e FROM x) SELECT c + d - (c - e) AS f FROM cte;
SELECT
  "x"."a" * "x"."b" + "x"."a" - (
    "x"."a" * "x"."b" - "x"."b"
  ) AS "f"
FROM "x" AS "x";

# title: wrapped table without alias
# execute: false
SELECT * FROM (tbl);
SELECT
  *
FROM (
  "tbl" AS "tbl"
);

# title: wrapped table with alias
# execute: false
SELECT * FROM (tbl AS tbl);
SELECT
  *
FROM (
  "tbl" AS "tbl"
);

# title: wrapped join of tables without alias
SELECT a, c FROM (x LEFT JOIN y ON a = c);
SELECT
  "x"."a" AS "a",
  "y"."c" AS "c"
FROM (
  "x" AS "x"
    LEFT JOIN "y" AS "y"
      ON "x"."a" = "y"."c"
);

# title: wrapped join of tables with alias
# execute: false
SELECT a, c FROM (x LEFT JOIN y ON a = c) AS t;
SELECT
  "x"."a" AS "a",
  "y"."c" AS "c"
FROM "x" AS "x"
LEFT JOIN "y" AS "y"
  ON "x"."a" = "y"."c";

# title: chained wrapped joins without aliases
# execute: false
SELECT * FROM ((a CROSS JOIN ((b CROSS JOIN c) CROSS JOIN (d CROSS JOIN e))));
SELECT
  *
FROM (
  (
    "a" AS "a"
      CROSS JOIN (
        (
          "b" AS "b"
            CROSS JOIN "c" AS "c"
        )
        CROSS JOIN (
          "d" AS "d"
            CROSS JOIN "e" AS "e"
        )
      )
  )
);

# title: chained wrapped joins with aliases
# execute: false
SELECT * FROM ((a AS foo CROSS JOIN b AS bar) CROSS JOIN c AS baz);
SELECT
  *
FROM (
  (
    "a" AS "foo"
      CROSS JOIN "b" AS "bar"
  )
  CROSS JOIN "c" AS "baz"
);

# title: table joined with join construct
SELECT x.a, y.b, z.c FROM x LEFT JOIN (y INNER JOIN z ON y.c = z.c) ON x.b = y.b;
SELECT
  "x"."a" AS "a",
  "y"."b" AS "b",
  "z"."c" AS "c"
FROM "x" AS "x"
LEFT JOIN (
  "y" AS "y"
    JOIN "z" AS "z"
      ON "y"."c" = "z"."c"
)
  ON "x"."b" = "y"."b";

# title: select * from table joined with join construct
# execute: false
SELECT * FROM x LEFT JOIN (y INNER JOIN z ON y.c = z.c) ON x.b = y.b;
SELECT
  "y"."b" AS "b",
  "y"."c" AS "c",
  "z"."a" AS "a",
  "z"."c" AS "c",
  "x"."a" AS "a",
  "x"."b" AS "b"
FROM "x" AS "x"
LEFT JOIN (
  "y" AS "y"
    JOIN "z" AS "z"
      ON "y"."c" = "z"."c"
)
  ON "x"."b" = "y"."b";

# title: select * from wrapped subquery
# execute: false
SELECT * FROM ((SELECT * FROM tbl));
WITH "_q_0" AS (
  SELECT
    *
  FROM "tbl" AS "tbl"
)
SELECT
  *
FROM (
  "_q_0" AS "_q_0"
);

# title: select * from wrapped subquery joined to a table (known schema)
SELECT * FROM ((SELECT * FROM x) INNER JOIN y ON a = c);
SELECT
  "x"."a" AS "a",
  "x"."b" AS "b",
  "y"."b" AS "b",
  "y"."c" AS "c"
FROM (
  "x" AS "x"
    JOIN "y" AS "y"
      ON "x"."a" = "y"."c"
);

# title: select * from wrapped subquery joined to a table (unknown schema)
# execute: false
SELECT * FROM ((SELECT c FROM t1) JOIN t2);
WITH "_q_0" AS (
  SELECT
    "t1"."c" AS "c"
  FROM "t1" AS "t1"
)
SELECT
  *
FROM (
  "_q_0" AS "_q_0"
    CROSS JOIN "t2" AS "t2"
);

# title: select specific columns from wrapped subquery joined to a table
SELECT b FROM ((SELECT a FROM x) INNER JOIN y ON a = b);
SELECT
  "y"."b" AS "b"
FROM (
  "x" AS "x"
    JOIN "y" AS "y"
      ON "x"."a" = "y"."b"
);

# title: select * from wrapped join of subqueries (unknown schema)
# execute: false
SELECT * FROM ((SELECT * FROM t1) JOIN (SELECT * FROM t2));
WITH "_q_0" AS (
  SELECT
    *
  FROM "t1" AS "t1"
), "_q_1" AS (
  SELECT
    *
  FROM "t2" AS "t2"
)
SELECT
  *
FROM (
  "_q_0" AS "_q_0"
    CROSS JOIN "_q_1" AS "_q_1"
);

# title: select * from wrapped join of subqueries (known schema)
SELECT * FROM ((SELECT * FROM x) INNER JOIN (SELECT * FROM y) ON a = c);
SELECT
  "x"."a" AS "a",
  "x"."b" AS "b",
  "y"."b" AS "b",
  "y"."c" AS "c"
FROM (
  "x" AS "x"
    JOIN "y" AS "y"
      ON "x"."a" = "y"."c"
);

# title: replace scalar subquery, wrap resulting column in a MAX
SELECT a, SUM(c) / (SELECT SUM(c) FROM y) * 100 AS foo FROM y INNER JOIN x ON y.b = x.b GROUP BY a;
WITH "_u_0" AS (
  SELECT
    SUM("y"."c") AS "_col_0"
  FROM "y" AS "y"
)
SELECT
  "x"."a" AS "a",
  SUM("y"."c") / MAX("_u_0"."_col_0") * 100 AS "foo"
FROM "y" AS "y"
CROSS JOIN "_u_0" AS "_u_0"
JOIN "x" AS "x"
  ON "y"."b" = "x"."b"
GROUP BY
  "x"."a";

# title: select * from a cte, which had one of its two columns aliased
WITH cte(x, y) AS (SELECT 1, 2) SELECT * FROM cte AS cte(a);
WITH "cte" AS (
  SELECT
    1 AS "x",
    2 AS "y"
)
SELECT
  "cte"."a" AS "a",
  "cte"."y" AS "y"
FROM "cte" AS "cte"("a");

# title: select single column from a cte using its alias
WITH cte(x) AS (SELECT 1) SELECT a FROM cte AS cte(a);
WITH "cte" AS (
  SELECT
    1 AS "x"
)
SELECT
  "cte"."a" AS "a"
FROM "cte" AS "cte"("a");

# title: joined ctes with a "using" clause, one of which has had its column aliased
WITH m(a) AS (SELECT 1), n(b) AS (SELECT 1) SELECT * FROM m JOIN n AS foo(a) USING (a);
WITH "m" AS (
  SELECT
    1 AS "a"
), "n" AS (
  SELECT
    1 AS "b"
)
SELECT
  COALESCE("m"."a", "foo"."a") AS "a"
FROM "m"
JOIN "n" AS "foo"("a")
  ON "m"."a" = "foo"."a";

# title: reduction of string concatenation that uses CONCAT(..), || and +
# execute: false
SELECT CONCAT('a', 'b') || CONCAT(CONCAT('c', 'd'), CONCAT('e', 'f')) + ('g' || 'h' || 'i');
SELECT
  'abcdefghi' AS "_col_0";

# title: complex query with derived tables and redundant parentheses
# execute: false
# dialect: snowflake
SELECT
  ("SUBQUERY_0"."KEY") AS "SUBQUERY_1_COL_0"
FROM
  (
    SELECT
      *
    FROM
      (((
          SELECT
            *
          FROM
            (
              SELECT
                event_name AS key,
                insert_ts
              FROM
                (
                  SELECT
                    insert_ts,
                    event_name
                  FROM
                    sales
                  WHERE
                    insert_ts > '2023-08-07 21:03:35.590 -0700'
                )
            )
      ))) AS "SF_CONNECTOR_QUERY_ALIAS"
  ) AS "SUBQUERY_0";
SELECT
  "SALES"."EVENT_NAME" AS "SUBQUERY_1_COL_0"
FROM "SALES" AS "SALES"
WHERE
  "SALES"."INSERT_TS" > '2023-08-07 21:03:35.590 -0700';

# title: using join without select *
# execute: false
with
    alias1 as (select * from table1),
    alias2 as (select * from table2),
    alias3 as (
        select
            cid,
            min(od) as m_od,
            count(odi) as c_od,
        from alias2
        group by 1
    )
select
    alias1.cid,
    alias3.m_od,
    coalesce(alias3.c_od, 0) as c_od,
from alias1
left join alias3 using (cid);
WITH "alias3" AS (
  SELECT
    "table2"."cid" AS "cid",
    MIN("table2"."od") AS "m_od",
    COUNT("table2"."odi") AS "c_od"
  FROM "table2" AS "table2"
  GROUP BY
    "table2"."cid"
)
SELECT
  "table1"."cid" AS "cid",
  "alias3"."m_od" AS "m_od",
  COALESCE("alias3"."c_od", 0) AS "c_od"
FROM "table1" AS "table1"
LEFT JOIN "alias3"
  ON "table1"."cid" = "alias3"."cid";
