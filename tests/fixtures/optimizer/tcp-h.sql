--------------------------------------
-- TCP-H 2
--------------------------------------
select
   s_acctbal,
   s_name,
   n_name,
   p_partkey,
   p_mfgr,
   s_address,
   s_phone,
   s_comment
from
   part,
   supplier,
   partsupp,
   nation,
   region
where
   p_partkey = ps_partkey
   and s_suppkey = ps_suppkey
   and p_size = 15
   and p_type like '%BRASS'
   and s_nationkey = n_nationkey
   and n_regionkey = r_regionkey
   and r_name = 'EUROPE'
   and ps_supplycost = (
           select
                   min(ps_supplycost)
           from
                   partsupp,
                   supplier,
                   nation,
                   region
           where
                   p_partkey = ps_partkey
                   and s_suppkey = ps_suppkey
                   and s_nationkey = n_nationkey
                   and n_regionkey = r_regionkey
                   and r_name = 'EUROPE'
   )
order by
   s_acctbal desc,
   n_name,
   s_name,
   p_partkey
limit
   100;
SELECT
  "supplier"."s_acctbal" AS "s_acctbal",
  "supplier"."s_name" AS "s_name",
  "nation"."n_name" AS "n_name",
  "part"."p_partkey" AS "p_partkey",
  "part"."p_mfgr" AS "p_mfgr",
  "supplier"."s_address" AS "s_address",
  "supplier"."s_phone" AS "s_phone",
  "supplier"."s_comment" AS "s_comment"
FROM "part" AS "part"
JOIN (
    SELECT
      MIN("partsupp"."ps_supplycost") AS "_col_0",
      "partsupp"."ps_partkey"
    FROM "partsupp" AS "partsupp"
    JOIN "supplier" AS "supplier"
      ON "supplier"."s_nationkey" = "nation"."n_nationkey"
      AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
    JOIN "nation" AS "nation"
      ON "nation"."n_regionkey" = "region"."r_regionkey"
    JOIN "region" AS "region"
      ON "region"."r_name" = 'EUROPE'
    GROUP BY
      "partsupp"."ps_partkey"
) AS "_d_0"
  ON "_d_0"."ps_partkey" = "part"."p_partkey"
  AND "partsupp"."ps_supplycost" = "_d_0"."_col_0"
JOIN "supplier" AS "supplier"
  ON "supplier"."s_nationkey" = "nation"."n_nationkey"
  AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
JOIN "partsupp" AS "partsupp"
  ON "part"."p_partkey" = "partsupp"."ps_partkey"
JOIN "nation" AS "nation"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
JOIN "region" AS "region"
  ON "region"."r_name" = 'EUROPE'
WHERE
  "part"."p_size" = 15
  AND "part"."p_type" LIKE '%BRASS'
ORDER BY
  "supplier"."s_acctbal" DESC,
  "nation"."n_name",
  "supplier"."s_name",
  "part"."p_partkey"
LIMIT 100;
