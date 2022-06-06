--------------------------------------
-- TPC-H 1
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
  "lineitem"."l_shipdate" <= CAST('1998-12-01' AS DATE) - INTERVAL '90' day
GROUP BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus"
ORDER BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus";

--------------------------------------
-- TPC-H 2
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
JOIN "partsupp" AS "partsupp"
  ON "part"."p_partkey" = "partsupp"."ps_partkey"
JOIN (
    SELECT
      MIN("partsupp"."ps_supplycost") AS "_col_0",
      "partsupp"."ps_partkey"
    FROM "partsupp" AS "partsupp"
    JOIN "region" AS "region"
      ON "region"."r_name" = 'EUROPE'
    JOIN "nation" AS "nation"
      ON "nation"."n_regionkey" = "region"."r_regionkey"
    JOIN "supplier" AS "supplier"
      ON "supplier"."s_nationkey" = "nation"."n_nationkey"
      AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
    GROUP BY
      "partsupp"."ps_partkey"
) AS "_d_0"
  ON "_d_0"."ps_partkey" = "part"."p_partkey"
  AND "partsupp"."ps_supplycost" = "_d_0"."_col_0"
JOIN "region" AS "region"
  ON "region"."r_name" = 'EUROPE'
JOIN "nation" AS "nation"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
JOIN "supplier" AS "supplier"
  ON "supplier"."s_nationkey" = "nation"."n_nationkey"
  AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
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
-- TPC-H 3
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
  AND "orders"."o_orderdate" < CAST('1995-03-15' AS DATE)
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "lineitem"."l_shipdate" > CAST('1995-03-15' AS DATE)
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
-- TPC-H 4
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
  "orders"."o_orderdate" < CAST('1993-07-01' AS DATE) + INTERVAL '3' month
  AND "orders"."o_orderdate" >= CAST('1993-07-01' AS DATE)
GROUP BY
  "orders"."o_orderpriority"
ORDER BY
  "orders"."o_orderpriority";


--------------------------------------
-- TPC-H 5
--------------------------------------
select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
group by
        n_name
order by
        revenue desc;
SELECT
  "nation"."n_name" AS "n_name",
  SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "revenue"
FROM "customer" AS "customer"
JOIN "region" AS "region"
  ON "region"."r_name" = 'ASIA'
JOIN "nation" AS "nation"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
JOIN "supplier" AS "supplier"
  ON "customer"."c_nationkey" = "supplier"."s_nationkey"
  AND "supplier"."s_nationkey" = "nation"."n_nationkey"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "orders"."o_orderdate" < CAST('1994-01-01' AS DATE) + INTERVAL '1' year
  AND "orders"."o_orderdate" >= CAST('1994-01-01' AS DATE)
GROUP BY
  "nation"."n_name"
ORDER BY
  "revenue" DESC;

--------------------------------------
-- TPC-H 6
--------------------------------------
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24;
SELECT
  SUM("lineitem"."l_extendedprice" * "lineitem"."l_discount") AS "revenue"
FROM "lineitem" AS "lineitem"
WHERE
  "lineitem"."l_discount" BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND "lineitem"."l_quantity" < 24
  AND "lineitem"."l_shipdate" < CAST('1994-01-01' AS DATE) + INTERVAL '1' year
  AND "lineitem"."l_shipdate" >= CAST('1994-01-01' AS DATE);

--------------------------------------
-- TPC-H 7
--------------------------------------
select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year;
SELECT
  "shipping"."supp_nation" AS "supp_nation",
  "shipping"."cust_nation" AS "cust_nation",
  "shipping"."l_year" AS "l_year",
  SUM("shipping"."volume") AS "revenue"
FROM (
    SELECT
      "n1"."n_name" AS "supp_nation",
      "n2"."n_name" AS "cust_nation",
      EXTRACT(year FROM "lineitem"."l_shipdate") AS "l_year",
      "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "volume"
    FROM "supplier" AS "supplier"
    JOIN "nation" AS "n1"
      ON ("n1"."n_name" = 'FRANCE' OR "n1"."n_name" = 'GERMANY')
      AND "supplier"."s_nationkey" = "n1"."n_nationkey"
    JOIN "nation" AS "n2"
      ON ("n1"."n_name" = 'FRANCE' OR "n2"."n_name" = 'FRANCE')
      AND ("n1"."n_name" = 'GERMANY' OR "n2"."n_name" = 'GERMANY')
      AND ("n2"."n_name" = 'FRANCE' OR "n2"."n_name" = 'GERMANY')
    JOIN "customer" AS "customer"
      ON "customer"."c_nationkey" = "n2"."n_nationkey"
    JOIN "orders" AS "orders"
      ON "customer"."c_custkey" = "orders"."o_custkey"
    JOIN "lineitem" AS "lineitem"
      ON "lineitem"."l_shipdate" BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
      AND "orders"."o_orderkey" = "lineitem"."l_orderkey"
      AND "supplier"."s_suppkey" = "lineitem"."l_suppkey"
) AS "shipping"
GROUP BY
  "shipping"."supp_nation",
  "shipping"."cust_nation",
  "shipping"."l_year"
ORDER BY
  "shipping"."supp_nation",
  "shipping"."cust_nation",
  "shipping"."l_year";

--------------------------------------
-- TPC-H 8
--------------------------------------
select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;
SELECT
  "all_nations"."o_year" AS "o_year",
  SUM(CASE
    WHEN "all_nations"."nation" = 'BRAZIL' THEN "all_nations"."volume"
    ELSE 0
  END) / SUM("all_nations"."volume") AS "mkt_share"
FROM (
    SELECT
      EXTRACT(year FROM "orders"."o_orderdate") AS "o_year",
      "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "volume",
      "n2"."n_name" AS "nation"
    FROM "part" AS "part"
    JOIN "region" AS "region"
      ON "region"."r_name" = 'AMERICA'
    JOIN "nation" AS "n1"
      ON "n1"."n_regionkey" = "region"."r_regionkey"
    JOIN "customer" AS "customer"
      ON "customer"."c_nationkey" = "n1"."n_nationkey"
    JOIN "orders" AS "orders"
      ON "orders"."o_custkey" = "customer"."c_custkey"
      AND "orders"."o_orderdate" BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
    JOIN "lineitem" AS "lineitem"
      ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
      AND "part"."p_partkey" = "lineitem"."l_partkey"
    JOIN "supplier" AS "supplier"
      ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
    JOIN "nation" AS "n2"
      ON "supplier"."s_nationkey" = "n2"."n_nationkey"
    WHERE
      "part"."p_type" = 'ECONOMY ANODIZED STEEL'
) AS "all_nations"
GROUP BY
  "all_nations"."o_year"
ORDER BY
  "all_nations"."o_year";

--------------------------------------
-- TPC-H 9
-- TODO add transitive predicate push down
--------------------------------------
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%green%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc;
SELECT
  "profit"."nation" AS "nation",
  "profit"."o_year" AS "o_year",
  SUM("profit"."amount") AS "sum_profit"
FROM (
    SELECT
      "nation"."n_name" AS "nation",
      EXTRACT(year FROM "orders"."o_orderdate") AS "o_year",
      "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") - "partsupp"."ps_supplycost" * "lineitem"."l_quantity" AS "amount"
    FROM "part" AS "part"
    JOIN "lineitem" AS "lineitem"
      ON "part"."p_partkey" = "lineitem"."l_partkey"
    JOIN "supplier" AS "supplier"
      ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
    JOIN "partsupp" AS "partsupp"
      ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
      AND "partsupp"."ps_suppkey" = "lineitem"."l_suppkey"
    JOIN "orders" AS "orders"
      ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
    JOIN "nation" AS "nation"
      ON "supplier"."s_nationkey" = "nation"."n_nationkey"
    WHERE
      "part"."p_name" LIKE '%green%'
) AS "profit"
GROUP BY
  "profit"."nation",
  "profit"."o_year"
ORDER BY
  "profit"."nation",
  "profit"."o_year" DESC;

--------------------------------------
-- TPC-H 10
--------------------------------------
select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc
limit
        20;
SELECT
  "customer"."c_custkey" AS "c_custkey",
  "customer"."c_name" AS "c_name",
  SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "revenue",
  "customer"."c_acctbal" AS "c_acctbal",
  "nation"."n_name" AS "n_name",
  "customer"."c_address" AS "c_address",
  "customer"."c_phone" AS "c_phone",
  "customer"."c_comment" AS "c_comment"
FROM "customer" AS "customer"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND "orders"."o_orderdate" < CAST('1993-10-01' AS DATE) + INTERVAL '3' month
  AND "orders"."o_orderdate" >= CAST('1993-10-01' AS DATE)
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "lineitem"."l_returnflag" = 'R'
JOIN "nation" AS "nation"
  ON "customer"."c_nationkey" = "nation"."n_nationkey"
GROUP BY
  "customer"."c_custkey",
  "customer"."c_name",
  "customer"."c_acctbal",
  "customer"."c_phone",
  "nation"."n_name",
  "customer"."c_address",
  "customer"."c_comment"
ORDER BY
  "revenue" DESC
LIMIT 20;

--------------------------------------
-- TPC-H 11
--------------------------------------
