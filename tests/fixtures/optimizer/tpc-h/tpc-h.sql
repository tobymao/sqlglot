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
        CAST(l_shipdate AS DATE) <= date '1998-12-01' - interval '90' day
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
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "sum_disc_price",
  SUM(
    "lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    ) * (
      1 + "lineitem"."l_tax"
    )
  ) AS "sum_charge",
  AVG("lineitem"."l_quantity") AS "avg_qty",
  AVG("lineitem"."l_extendedprice") AS "avg_price",
  AVG("lineitem"."l_discount") AS "avg_disc",
  COUNT(*) AS "count_order"
FROM "lineitem" AS "lineitem"
WHERE
  CAST("lineitem"."l_shipdate" AS DATE) <= CAST('1998-09-02' AS DATE)
GROUP BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus"
ORDER BY
  "l_returnflag",
  "l_linestatus";

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
WITH "partsupp_2" AS (
  SELECT
    "partsupp"."ps_partkey" AS "ps_partkey",
    "partsupp"."ps_suppkey" AS "ps_suppkey",
    "partsupp"."ps_supplycost" AS "ps_supplycost"
  FROM "partsupp" AS "partsupp"
), "region_2" AS (
  SELECT
    "region"."r_regionkey" AS "r_regionkey",
    "region"."r_name" AS "r_name"
  FROM "region" AS "region"
  WHERE
    "region"."r_name" = 'EUROPE'
), "_u_0" AS (
  SELECT
    MIN("partsupp"."ps_supplycost") AS "_col_0",
    "partsupp"."ps_partkey" AS "_u_1"
  FROM "partsupp_2" AS "partsupp"
  JOIN "supplier" AS "supplier"
    ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
  JOIN "nation" AS "nation"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
  JOIN "region_2" AS "region"
    ON "nation"."n_regionkey" = "region"."r_regionkey"
  GROUP BY
    "partsupp"."ps_partkey"
)
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
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "part"."p_partkey"
JOIN "partsupp_2" AS "partsupp"
  ON "part"."p_partkey" = "partsupp"."ps_partkey"
JOIN "supplier" AS "supplier"
  ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
JOIN "nation" AS "nation"
  ON "nation"."n_nationkey" = "supplier"."s_nationkey"
JOIN "region_2" AS "region"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE
  "_u_0"."_col_0" = "partsupp"."ps_supplycost"
  AND "part"."p_size" = 15
  AND "part"."p_type" LIKE '%BRASS'
ORDER BY
  "s_acctbal" DESC,
  "n_name",
  "s_name",
  "p_partkey"
LIMIT 100;

--------------------------------------
-- TPC-H 3
--------------------------------------
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        CAST(o_orderdate AS STRING) AS o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < '1995-03-15'
        and l_shipdate > '1995-03-15'
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
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "revenue",
  "orders"."o_orderdate" AS "o_orderdate",
  "orders"."o_shippriority" AS "o_shippriority"
FROM "customer" AS "customer"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND "orders"."o_orderdate" < '1995-03-15'
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "lineitem"."l_shipdate" > '1995-03-15'
WHERE
  "customer"."c_mktsegment" = 'BUILDING'
GROUP BY
  "lineitem"."l_orderkey",
  "orders"."o_orderdate",
  "orders"."o_shippriority"
ORDER BY
  "revenue" DESC,
  "o_orderdate"
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
        CAST(o_orderdate AS DATE) >= date '1993-07-01'
        and CAST(o_orderdate AS DATE) < date '1993-07-01' + interval '3' month
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
WITH "_u_0" AS (
  SELECT
    "lineitem"."l_orderkey" AS "l_orderkey"
  FROM "lineitem" AS "lineitem"
  WHERE
    "lineitem"."l_commitdate" < "lineitem"."l_receiptdate"
  GROUP BY
    "lineitem"."l_orderkey"
)
SELECT
  "orders"."o_orderpriority" AS "o_orderpriority",
  COUNT(*) AS "order_count"
FROM "orders" AS "orders"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."l_orderkey" = "orders"."o_orderkey"
WHERE
  NOT "_u_0"."l_orderkey" IS NULL
  AND CAST("orders"."o_orderdate" AS DATE) < CAST('1993-10-01' AS DATE)
  AND CAST("orders"."o_orderdate" AS DATE) >= CAST('1993-07-01' AS DATE)
GROUP BY
  "orders"."o_orderpriority"
ORDER BY
  "o_orderpriority";

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
        and CAST(o_orderdate AS DATE) >= date '1994-01-01'
        and CAST(o_orderdate AS DATE) < date '1994-01-01' + interval '1' year
group by
        n_name
order by
        revenue desc;
SELECT
  "nation"."n_name" AS "n_name",
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "revenue"
FROM "customer" AS "customer"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND CAST("orders"."o_orderdate" AS DATE) < CAST('1995-01-01' AS DATE)
  AND CAST("orders"."o_orderdate" AS DATE) >= CAST('1994-01-01' AS DATE)
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
JOIN "supplier" AS "supplier"
  ON "customer"."c_nationkey" = "supplier"."s_nationkey"
  AND "lineitem"."l_suppkey" = "supplier"."s_suppkey"
JOIN "nation" AS "nation"
  ON "nation"."n_nationkey" = "supplier"."s_nationkey"
JOIN "region" AS "region"
  ON "nation"."n_regionkey" = "region"."r_regionkey" AND "region"."r_name" = 'ASIA'
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
        CAST(l_shipdate AS DATE) >= date '1994-01-01'
        and CAST(l_shipdate AS DATE) < date '1994-01-01' + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24;
SELECT
  SUM("lineitem"."l_extendedprice" * "lineitem"."l_discount") AS "revenue"
FROM "lineitem" AS "lineitem"
WHERE
  "lineitem"."l_discount" <= 0.07
  AND "lineitem"."l_discount" >= 0.05
  AND "lineitem"."l_quantity" < 24
  AND CAST("lineitem"."l_shipdate" AS DATE) < CAST('1995-01-01' AS DATE)
  AND CAST("lineitem"."l_shipdate" AS DATE) >= CAST('1994-01-01' AS DATE);

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
                        extract(year from cast(l_shipdate as date)) as l_year,
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
                        and CAST(l_shipdate AS DATE) between date '1995-01-01' and date '1996-12-31'
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
  "n1"."n_name" AS "supp_nation",
  "n2"."n_name" AS "cust_nation",
  EXTRACT(year FROM CAST("lineitem"."l_shipdate" AS DATE)) AS "l_year",
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "revenue"
FROM "supplier" AS "supplier"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
  AND CAST("lineitem"."l_shipdate" AS DATE) <= CAST('1996-12-31' AS DATE)
  AND CAST("lineitem"."l_shipdate" AS DATE) >= CAST('1995-01-01' AS DATE)
JOIN "nation" AS "n1"
  ON (
    "n1"."n_name" = 'FRANCE' OR "n1"."n_name" = 'GERMANY'
  )
  AND "n1"."n_nationkey" = "supplier"."s_nationkey"
JOIN "orders" AS "orders"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
JOIN "customer" AS "customer"
  ON "customer"."c_custkey" = "orders"."o_custkey"
JOIN "nation" AS "n2"
  ON "customer"."c_nationkey" = "n2"."n_nationkey"
  AND (
    "n1"."n_name" = 'FRANCE' OR "n2"."n_name" = 'FRANCE'
  )
  AND (
    "n1"."n_name" = 'GERMANY' OR "n2"."n_name" = 'GERMANY'
  )
  AND (
    "n2"."n_name" = 'FRANCE' OR "n2"."n_name" = 'GERMANY'
  )
GROUP BY
  "n1"."n_name",
  "n2"."n_name",
  EXTRACT(year FROM CAST("lineitem"."l_shipdate" AS DATE))
ORDER BY
  "supp_nation",
  "cust_nation",
  "l_year";

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
                        extract(year from cast(o_orderdate as date)) as o_year,
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
                        and CAST(o_orderdate AS DATE) between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;
SELECT
  EXTRACT(year FROM CAST("orders"."o_orderdate" AS DATE)) AS "o_year",
  SUM(
    CASE
      WHEN "n2"."n_name" = 'BRAZIL'
      THEN "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      )
      ELSE 0
    END
  ) / SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "mkt_share"
FROM "part" AS "part"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_partkey" = "part"."p_partkey"
JOIN "orders" AS "orders"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND CAST("orders"."o_orderdate" AS DATE) <= CAST('1996-12-31' AS DATE)
  AND CAST("orders"."o_orderdate" AS DATE) >= CAST('1995-01-01' AS DATE)
JOIN "supplier" AS "supplier"
  ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
JOIN "customer" AS "customer"
  ON "customer"."c_custkey" = "orders"."o_custkey"
JOIN "nation" AS "n2"
  ON "n2"."n_nationkey" = "supplier"."s_nationkey"
JOIN "nation" AS "n1"
  ON "customer"."c_nationkey" = "n1"."n_nationkey"
JOIN "region" AS "region"
  ON "n1"."n_regionkey" = "region"."r_regionkey" AND "region"."r_name" = 'AMERICA'
WHERE
  "part"."p_type" = 'ECONOMY ANODIZED STEEL'
GROUP BY
  EXTRACT(year FROM CAST("orders"."o_orderdate" AS DATE))
ORDER BY
  "o_year";

--------------------------------------
-- TPC-H 9
--------------------------------------
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from cast(o_orderdate as date)) as o_year,
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
  "nation"."n_name" AS "nation",
  EXTRACT(year FROM CAST("orders"."o_orderdate" AS DATE)) AS "o_year",
  SUM(
    "lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    ) - "partsupp"."ps_supplycost" * "lineitem"."l_quantity"
  ) AS "sum_profit"
FROM "part" AS "part"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_partkey" = "part"."p_partkey"
JOIN "orders" AS "orders"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
JOIN "partsupp" AS "partsupp"
  ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
  AND "lineitem"."l_suppkey" = "partsupp"."ps_suppkey"
JOIN "supplier" AS "supplier"
  ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
JOIN "nation" AS "nation"
  ON "nation"."n_nationkey" = "supplier"."s_nationkey"
WHERE
  "part"."p_name" LIKE '%green%'
GROUP BY
  "nation"."n_name",
  EXTRACT(year FROM CAST("orders"."o_orderdate" AS DATE))
ORDER BY
  "nation",
  "o_year" DESC;

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
        and CAST(o_orderdate AS DATE) >= date '1993-10-01'
        and CAST(o_orderdate AS DATE) < date '1993-10-01' + interval '3' month
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
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "revenue",
  "customer"."c_acctbal" AS "c_acctbal",
  "nation"."n_name" AS "n_name",
  "customer"."c_address" AS "c_address",
  "customer"."c_phone" AS "c_phone",
  "customer"."c_comment" AS "c_comment"
FROM "customer" AS "customer"
JOIN "nation" AS "nation"
  ON "customer"."c_nationkey" = "nation"."n_nationkey"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
  AND CAST("orders"."o_orderdate" AS DATE) < CAST('1994-01-01' AS DATE)
  AND CAST("orders"."o_orderdate" AS DATE) >= CAST('1993-10-01' AS DATE)
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey" AND "lineitem"."l_returnflag" = 'R'
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
select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
from
        partsupp,
        supplier,
        nation
where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                        select
                                sum(ps_supplycost * ps_availqty) * 0.0001
                        from
                                partsupp,
                                supplier,
                                nation
                        where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = 'GERMANY'
                )
order by
        value desc;
WITH "supplier_2" AS (
  SELECT
    "supplier"."s_suppkey" AS "s_suppkey",
    "supplier"."s_nationkey" AS "s_nationkey"
  FROM "supplier" AS "supplier"
), "nation_2" AS (
  SELECT
    "nation"."n_nationkey" AS "n_nationkey",
    "nation"."n_name" AS "n_name"
  FROM "nation" AS "nation"
  WHERE
    "nation"."n_name" = 'GERMANY'
), "_u_0" AS (
  SELECT
    SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") * 0.0001 AS "_col_0"
  FROM "partsupp" AS "partsupp"
  JOIN "supplier_2" AS "supplier"
    ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
  JOIN "nation_2" AS "nation"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
)
SELECT
  "partsupp"."ps_partkey" AS "ps_partkey",
  SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "value"
FROM "partsupp" AS "partsupp"
CROSS JOIN "_u_0" AS "_u_0"
JOIN "supplier_2" AS "supplier"
  ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
JOIN "nation_2" AS "nation"
  ON "nation"."n_nationkey" = "supplier"."s_nationkey"
GROUP BY
  "partsupp"."ps_partkey"
HAVING
  MAX("_u_0"."_col_0") < SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty")
ORDER BY
  "value" DESC;

--------------------------------------
-- TPC-H 12
--------------------------------------
select
        l_shipmode,
        sum(case
                when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders,
        lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and CAST(l_receiptdate AS DATE) >= date '1994-01-01'
        and CAST(l_receiptdate AS DATE) < date '1994-01-01' + interval '1' year
group by
        l_shipmode
order by
        l_shipmode;
SELECT
  "lineitem"."l_shipmode" AS "l_shipmode",
  SUM(
    CASE
      WHEN "orders"."o_orderpriority" = '1-URGENT' OR "orders"."o_orderpriority" = '2-HIGH'
      THEN 1
      ELSE 0
    END
  ) AS "high_line_count",
  SUM(
    CASE
      WHEN "orders"."o_orderpriority" <> '1-URGENT' AND "orders"."o_orderpriority" <> '2-HIGH'
      THEN 1
      ELSE 0
    END
  ) AS "low_line_count"
FROM "orders" AS "orders"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_commitdate" < "lineitem"."l_receiptdate"
  AND "lineitem"."l_commitdate" > "lineitem"."l_shipdate"
  AND "lineitem"."l_orderkey" = "orders"."o_orderkey"
  AND "lineitem"."l_shipmode" IN ('MAIL', 'SHIP')
  AND CAST("lineitem"."l_receiptdate" AS DATE) < CAST('1995-01-01' AS DATE)
  AND CAST("lineitem"."l_receiptdate" AS DATE) >= CAST('1994-01-01' AS DATE)
GROUP BY
  "lineitem"."l_shipmode"
ORDER BY
  "l_shipmode";

--------------------------------------
-- TPC-H 13
--------------------------------------
select
        c_count,
        count(*) as custdist
from
        (
                select
                        c_custkey,
                        count(o_orderkey)
                from
                        customer left outer join orders on
                                c_custkey = o_custkey
                                and o_comment not like '%special%requests%'
                group by
                        c_custkey
        ) as c_orders (c_custkey, c_count)
group by
        c_count
order by
        custdist desc,
        c_count desc;
WITH "orders_2" AS (
  SELECT
    "orders"."o_orderkey" AS "o_orderkey",
    "orders"."o_custkey" AS "o_custkey",
    "orders"."o_comment" AS "o_comment"
  FROM "orders" AS "orders"
  WHERE
    NOT "orders"."o_comment" LIKE '%special%requests%'
), "c_orders" AS (
  SELECT
    COUNT("orders"."o_orderkey") AS "c_count"
  FROM "customer" AS "customer"
  LEFT JOIN "orders_2" AS "orders"
    ON "customer"."c_custkey" = "orders"."o_custkey"
  GROUP BY
    "customer"."c_custkey"
)
SELECT
  "c_orders"."c_count" AS "c_count",
  COUNT(*) AS "custdist"
FROM "c_orders" AS "c_orders"
GROUP BY
  "c_orders"."c_count"
ORDER BY
  "custdist" DESC,
  "c_count" DESC;

--------------------------------------
-- TPC-H 14
--------------------------------------
select
        100.00 * sum(case
                when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
        lineitem,
        part
where
        l_partkey = p_partkey
        and CAST(l_shipdate AS DATE) >= date '1995-09-01'
        and CAST(l_shipdate AS DATE) < date '1995-09-01' + interval '1' month;
SELECT
  100.00 * SUM(
    CASE
      WHEN "part"."p_type" LIKE 'PROMO%'
      THEN "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      )
      ELSE 0
    END
  ) / SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "promo_revenue"
FROM "lineitem" AS "lineitem"
JOIN "part" AS "part"
  ON "lineitem"."l_partkey" = "part"."p_partkey"
WHERE
  CAST("lineitem"."l_shipdate" AS DATE) < CAST('1995-10-01' AS DATE)
  AND CAST("lineitem"."l_shipdate" AS DATE) >= CAST('1995-09-01' AS DATE);

--------------------------------------
-- TPC-H 15
--------------------------------------
with revenue (supplier_no, total_revenue) as (
        select
                l_suppkey,
                sum(l_extendedprice * (1 - l_discount))
        from
                lineitem
        where
                CAST(l_shipdate AS DATE) >= date '1996-01-01'
                and CAST(l_shipdate AS DATE) < date '1996-01-01' + interval '3' month
        group by
                l_suppkey)
select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
from
        supplier,
        revenue
where
        s_suppkey = supplier_no
        and total_revenue = (
                select
                        max(total_revenue)
                from
                        revenue
        )
order by
        s_suppkey;
WITH "revenue" AS (
  SELECT
    "lineitem"."l_suppkey" AS "supplier_no",
    SUM("lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    )) AS "total_revenue"
  FROM "lineitem" AS "lineitem"
  WHERE
    CAST("lineitem"."l_shipdate" AS DATE) < CAST('1996-04-01' AS DATE)
    AND CAST("lineitem"."l_shipdate" AS DATE) >= CAST('1996-01-01' AS DATE)
  GROUP BY
    "lineitem"."l_suppkey"
), "_u_0" AS (
  SELECT
    MAX("revenue"."total_revenue") AS "_col_0"
  FROM "revenue" AS "revenue"
)
SELECT
  "supplier"."s_suppkey" AS "s_suppkey",
  "supplier"."s_name" AS "s_name",
  "supplier"."s_address" AS "s_address",
  "supplier"."s_phone" AS "s_phone",
  "revenue"."total_revenue" AS "total_revenue"
FROM "supplier" AS "supplier"
JOIN "revenue" AS "revenue"
  ON "revenue"."supplier_no" = "supplier"."s_suppkey"
JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_col_0" = "revenue"."total_revenue"
ORDER BY
  "s_suppkey";

--------------------------------------
-- TPC-H 16
--------------------------------------
select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
from
        partsupp,
        part
where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
        and p_type not like 'MEDIUM POLISHED%'
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps_suppkey not in (
                select
                        s_suppkey
                from
                        supplier
                where
                        s_comment like '%Customer%Complaints%'
        )
group by
        p_brand,
        p_type,
        p_size
order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size;
WITH "_u_0" AS (
  SELECT
    "supplier"."s_suppkey" AS "s_suppkey"
  FROM "supplier" AS "supplier"
  WHERE
    "supplier"."s_comment" LIKE '%Customer%Complaints%'
  GROUP BY
    "supplier"."s_suppkey"
)
SELECT
  "part"."p_brand" AS "p_brand",
  "part"."p_type" AS "p_type",
  "part"."p_size" AS "p_size",
  COUNT(DISTINCT "partsupp"."ps_suppkey") AS "supplier_cnt"
FROM "partsupp" AS "partsupp"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."s_suppkey" = "partsupp"."ps_suppkey"
JOIN "part" AS "part"
  ON "part"."p_brand" <> 'Brand#45'
  AND "part"."p_partkey" = "partsupp"."ps_partkey"
  AND "part"."p_size" IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND NOT "part"."p_type" LIKE 'MEDIUM POLISHED%'
WHERE
  "_u_0"."s_suppkey" IS NULL
GROUP BY
  "part"."p_brand",
  "part"."p_type",
  "part"."p_size"
ORDER BY
  "supplier_cnt" DESC,
  "p_brand",
  "p_type",
  "p_size";

--------------------------------------
-- TPC-H 17
--------------------------------------
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
        lineitem,
        part
where
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container = 'MED BOX'
        and l_quantity < (
                select
                        0.2 * avg(l_quantity)
                from
                        lineitem
                where
                        l_partkey = p_partkey
        );
WITH "_u_0" AS (
  SELECT
    0.2 * AVG("lineitem"."l_quantity") AS "_col_0",
    "lineitem"."l_partkey" AS "_u_1"
  FROM "lineitem" AS "lineitem"
  GROUP BY
    "lineitem"."l_partkey"
)
SELECT
  SUM("lineitem"."l_extendedprice") / 7.0 AS "avg_yearly"
FROM "lineitem" AS "lineitem"
JOIN "part" AS "part"
  ON "lineitem"."l_partkey" = "part"."p_partkey"
  AND "part"."p_brand" = 'Brand#23'
  AND "part"."p_container" = 'MED BOX'
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "part"."p_partkey"
WHERE
  "_u_0"."_col_0" > "lineitem"."l_quantity";

--------------------------------------
-- TPC-H 18
--------------------------------------
select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity) total_quantity
from
        customer,
        orders,
        lineitem
where
        o_orderkey in (
                select
                        l_orderkey
                from
                        lineitem
                group by
                        l_orderkey having
                                sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
order by
        o_totalprice desc,
        o_orderdate
limit
        100;
WITH "_u_0" AS (
  SELECT
    "lineitem"."l_orderkey" AS "l_orderkey"
  FROM "lineitem" AS "lineitem"
  GROUP BY
    "lineitem"."l_orderkey"
  HAVING
    SUM("lineitem"."l_quantity") > 300
)
SELECT
  "customer"."c_name" AS "c_name",
  "customer"."c_custkey" AS "c_custkey",
  "orders"."o_orderkey" AS "o_orderkey",
  "orders"."o_orderdate" AS "o_orderdate",
  "orders"."o_totalprice" AS "o_totalprice",
  SUM("lineitem"."l_quantity") AS "total_quantity"
FROM "customer" AS "customer"
JOIN "orders" AS "orders"
  ON "customer"."c_custkey" = "orders"."o_custkey"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."l_orderkey" = "orders"."o_orderkey"
JOIN "lineitem" AS "lineitem"
  ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE
  NOT "_u_0"."l_orderkey" IS NULL
GROUP BY
  "customer"."c_name",
  "customer"."c_custkey",
  "orders"."o_orderkey",
  "orders"."o_orderdate",
  "orders"."o_totalprice"
ORDER BY
  "o_totalprice" DESC,
  "o_orderdate"
LIMIT 100;

--------------------------------------
-- TPC-H 19
--------------------------------------
select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        lineitem,
        part
where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 1 and l_quantity <= 11
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 20
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 20 and l_quantity <= 30
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        );
SELECT
  SUM("lineitem"."l_extendedprice" * (
    1 - "lineitem"."l_discount"
  )) AS "revenue"
FROM "lineitem" AS "lineitem"
JOIN "part" AS "part"
  ON (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 11
    AND "lineitem"."l_quantity" >= 1
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#12'
    AND "part"."p_container" IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND "part"."p_size" <= 5
    AND "part"."p_size" >= 1
  )
  OR (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 20
    AND "lineitem"."l_quantity" >= 10
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#23'
    AND "part"."p_container" IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND "part"."p_size" <= 10
    AND "part"."p_size" >= 1
  )
  OR (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 30
    AND "lineitem"."l_quantity" >= 20
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#34'
    AND "part"."p_container" IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND "part"."p_size" <= 15
    AND "part"."p_size" >= 1
  )
WHERE
  (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 11
    AND "lineitem"."l_quantity" >= 1
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#12'
    AND "part"."p_container" IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND "part"."p_size" <= 5
    AND "part"."p_size" >= 1
  )
  OR (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 20
    AND "lineitem"."l_quantity" >= 10
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#23'
    AND "part"."p_container" IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND "part"."p_size" <= 10
    AND "part"."p_size" >= 1
  )
  OR (
    "lineitem"."l_partkey" = "part"."p_partkey"
    AND "lineitem"."l_quantity" <= 30
    AND "lineitem"."l_quantity" >= 20
    AND "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'
    AND "lineitem"."l_shipmode" IN ('AIR', 'AIR REG')
    AND "part"."p_brand" = 'Brand#34'
    AND "part"."p_container" IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND "part"."p_size" <= 15
    AND "part"."p_size" >= 1
  );

--------------------------------------
-- TPC-H 20
--------------------------------------
select
        s_name,
        s_address
from
        supplier,
        nation
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        part
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and CAST(l_shipdate AS DATE) >= date '1994-01-01'
                                        and CAST(l_shipdate AS DATE) < date '1994-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
order by
        s_name;
WITH "_u_0" AS (
  SELECT
    "part"."p_partkey" AS "p_partkey"
  FROM "part" AS "part"
  WHERE
    "part"."p_name" LIKE 'forest%'
  GROUP BY
    "part"."p_partkey"
), "_u_1" AS (
  SELECT
    0.5 * SUM("lineitem"."l_quantity") AS "_col_0",
    "lineitem"."l_partkey" AS "_u_2",
    "lineitem"."l_suppkey" AS "_u_3"
  FROM "lineitem" AS "lineitem"
  WHERE
    CAST("lineitem"."l_shipdate" AS DATE) < CAST('1995-01-01' AS DATE)
    AND CAST("lineitem"."l_shipdate" AS DATE) >= CAST('1994-01-01' AS DATE)
  GROUP BY
    "lineitem"."l_partkey",
    "lineitem"."l_suppkey"
), "_u_4" AS (
  SELECT
    "partsupp"."ps_suppkey" AS "ps_suppkey"
  FROM "partsupp" AS "partsupp"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."p_partkey" = "partsupp"."ps_partkey"
  LEFT JOIN "_u_1" AS "_u_1"
    ON "_u_1"."_u_2" = "partsupp"."ps_partkey" AND "_u_1"."_u_3" = "partsupp"."ps_suppkey"
  WHERE
    "_u_1"."_col_0" < "partsupp"."ps_availqty" AND NOT "_u_0"."p_partkey" IS NULL
  GROUP BY
    "partsupp"."ps_suppkey"
)
SELECT
  "supplier"."s_name" AS "s_name",
  "supplier"."s_address" AS "s_address"
FROM "supplier" AS "supplier"
LEFT JOIN "_u_4" AS "_u_4"
  ON "_u_4"."ps_suppkey" = "supplier"."s_suppkey"
JOIN "nation" AS "nation"
  ON "nation"."n_name" = 'CANADA' AND "nation"."n_nationkey" = "supplier"."s_nationkey"
WHERE
  NOT "_u_4"."ps_suppkey" IS NULL
ORDER BY
  "s_name";

--------------------------------------
-- TPC-H 21
--------------------------------------
select
        s_name,
        count(*) as numwait
from
        supplier,
        lineitem l1,
        orders,
        nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        numwait desc,
        s_name
limit
        100;
WITH "_u_0" AS (
  SELECT
    "l2"."l_orderkey" AS "l_orderkey",
    ARRAY_AGG("l2"."l_suppkey") AS "_u_1"
  FROM "lineitem" AS "l2"
  GROUP BY
    "l2"."l_orderkey"
), "_u_2" AS (
  SELECT
    "l3"."l_orderkey" AS "l_orderkey",
    ARRAY_AGG("l3"."l_suppkey") AS "_u_3"
  FROM "lineitem" AS "l3"
  WHERE
    "l3"."l_commitdate" < "l3"."l_receiptdate"
  GROUP BY
    "l3"."l_orderkey"
)
SELECT
  "supplier"."s_name" AS "s_name",
  COUNT(*) AS "numwait"
FROM "supplier" AS "supplier"
JOIN "lineitem" AS "l1"
  ON "l1"."l_commitdate" < "l1"."l_receiptdate"
  AND "l1"."l_suppkey" = "supplier"."s_suppkey"
JOIN "nation" AS "nation"
  ON "nation"."n_name" = 'SAUDI ARABIA'
  AND "nation"."n_nationkey" = "supplier"."s_nationkey"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."l_orderkey" = "l1"."l_orderkey"
LEFT JOIN "_u_2" AS "_u_2"
  ON "_u_2"."l_orderkey" = "l1"."l_orderkey"
JOIN "orders" AS "orders"
  ON "l1"."l_orderkey" = "orders"."o_orderkey" AND "orders"."o_orderstatus" = 'F'
WHERE
  (
    "_u_2"."l_orderkey" IS NULL
    OR NOT ARRAY_ANY("_u_2"."_u_3", "_x" -> "l1"."l_suppkey" <> "_x")
  )
  AND NOT "_u_0"."l_orderkey" IS NULL
  AND ARRAY_ANY("_u_0"."_u_1", "_x" -> "l1"."l_suppkey" <> "_x")
GROUP BY
  "supplier"."s_name"
ORDER BY
  "numwait" DESC,
  "s_name"
LIMIT 100;

--------------------------------------
-- TPC-H 22
--------------------------------------
select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c_phone, 1, 2) as cntrycode,
                        c_acctbal
                from
                        customer
                where
                        substring(c_phone, 1, 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        customer
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone, 1, 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode;
WITH "_u_0" AS (
  SELECT
    AVG("customer"."c_acctbal") AS "_col_0"
  FROM "customer" AS "customer"
  WHERE
    "customer"."c_acctbal" > 0.00
    AND SUBSTRING("customer"."c_phone", 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), "_u_1" AS (
  SELECT
    "orders"."o_custkey" AS "_u_2"
  FROM "orders" AS "orders"
  GROUP BY
    "orders"."o_custkey"
)
SELECT
  SUBSTRING("customer"."c_phone", 1, 2) AS "cntrycode",
  COUNT(*) AS "numcust",
  SUM("customer"."c_acctbal") AS "totacctbal"
FROM "customer" AS "customer"
JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_col_0" < "customer"."c_acctbal"
LEFT JOIN "_u_1" AS "_u_1"
  ON "_u_1"."_u_2" = "customer"."c_custkey"
WHERE
  "_u_1"."_u_2" IS NULL
  AND SUBSTRING("customer"."c_phone", 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
GROUP BY
  SUBSTRING("customer"."c_phone", 1, 2)
ORDER BY
  "cntrycode";

