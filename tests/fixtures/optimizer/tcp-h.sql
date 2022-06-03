--------------------------------------
-- TCP-H 1
--------------------------------------
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-12-01' - interval '90' day
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;
SELECT
  "lineitem"."l_returnflag" AS "l_returnflag",
  "lineitem"."l_linestatus" AS "l_linestatus",
  SUM("lineitem"."l_quantity") AS "sum_qty",
  SUM("lineitem"."l_extendedprice") AS "sum_base_price",
  SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "sum_disc_price",
  SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") * (1 + "lineitem"."l_tax")) AS "sum_charge",
  AVG("lineitem"."l_quantity") AS "avg_qty",
  AVG("lineitem"."l_extendedprice") AS "avg_price",
  AVG("lineitem"."l_discount") AS "avg_disc",
  COUNT(*) AS "count_order"
FROM "lineitem" AS "lineitem"
WHERE
  "lineitem"."l_shipdate" <= CAST('1998-12-01' AS DATE) - INTERVAL '90' "day"
GROUP BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus"
ORDER BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus";
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
--------------------------------------
-- TCP-H 3
--------------------------------------
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate
limit
        10;
SELECT
  "lineitem"."l_orderkey" AS "l_orderkey",
  SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "revenue",
  "orders"."o_orderdate" AS "o_orderdate",
  "orders"."o_shippriority" AS "o_shippriority"
FROM "customer" AS "customer"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "orders"."o_orderdate" < CAST('1995-03-15' AS DATE)
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_shipdate" > CAST('1995-03-15' AS DATE)
WHERE
  "customer"."c_mktsegment" = 'BUILDING'
GROUP BY
  "lineitem"."l_orderkey",
  "orders"."o_orderdate",
  "orders"."o_shippriority"
ORDER BY
  "revenue" DESC,
  "orders"."o_orderdate"
LIMIT 10;
--------------------------------------
-- TCP-H 4
--------------------------------------
select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1993-07-01' + interval '3' month
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority;
SELECT
  "orders"."o_orderpriority" AS "o_orderpriority",
  COUNT(*) AS "order_count"
FROM "orders" AS "orders"
JOIN (
    SELECT
      "lineitem"."l_orderkey"
    FROM "lineitem" AS "lineitem"
    WHERE
      "lineitem"."l_commitdate" < "lineitem"."l_receiptdate"
    GROUP BY
      "lineitem"."l_orderkey"
) AS "_d_0"
  ON "_d_0"."l_orderkey" = "orders"."o_orderkey"
WHERE
  "orders"."o_orderdate" < CAST('1993-07-01' AS DATE) + INTERVAL '3' "month"
  AND "orders"."o_orderdate" >= CAST('1993-07-01' AS DATE)
GROUP BY
  "orders"."o_orderpriority"
ORDER BY
  "orders"."o_orderpriority";
