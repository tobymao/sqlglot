--------------------------------------
-- TPC-DS 1
--------------------------------------
WITH customer_total_return
     AS (SELECT sr_customer_sk     AS ctr_customer_sk,
                sr_store_sk        AS ctr_store_sk,
                Sum(sr_return_amt) AS ctr_total_return
         FROM   store_returns,
                date_dim
         WHERE  sr_returned_date_sk = d_date_sk
                AND d_year = 2001
         GROUP  BY sr_customer_sk,
                   sr_store_sk)
SELECT c_customer_id
FROM   customer_total_return ctr1,
       store,
       customer
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
       AND s_store_sk = ctr1.ctr_store_sk
       AND s_state = 'TN'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id
LIMIT 100;
WITH "customer_total_return" AS (
  SELECT
    "store_returns"."sr_customer_sk" AS "ctr_customer_sk",
    "store_returns"."sr_store_sk" AS "ctr_store_sk",
    SUM("store_returns"."sr_return_amt") AS "ctr_total_return"
  FROM "store_returns" AS "store_returns"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_year" = 2001
    AND "store_returns"."sr_returned_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "store_returns"."sr_customer_sk",
    "store_returns"."sr_store_sk"
), "_u_0" AS (
  SELECT
    AVG("ctr2"."ctr_total_return") * 1.2 AS "_col_0",
    "ctr2"."ctr_store_sk" AS "_u_1"
  FROM "customer_total_return" AS "ctr2"
  GROUP BY
    "ctr2"."ctr_store_sk"
)
SELECT
  "customer"."c_customer_id" AS "c_customer_id"
FROM "customer_total_return" AS "ctr1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "ctr1"."ctr_store_sk" = "_u_0"."_u_1"
JOIN "store" AS "store"
  ON "store"."s_state" = 'TN' AND "store"."s_store_sk" = "ctr1"."ctr_store_sk"
JOIN "customer" AS "customer"
  ON "ctr1"."ctr_customer_sk" = "customer"."c_customer_sk"
WHERE
  "ctr1"."ctr_total_return" > "_u_0"."_col_0"
ORDER BY
  "c_customer_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 2
--------------------------------------
WITH wscs
     AS (SELECT sold_date_sk,
                sales_price
         FROM   (SELECT ws_sold_date_sk    sold_date_sk,
                        ws_ext_sales_price sales_price
                 FROM   web_sales)
         UNION ALL
         (SELECT cs_sold_date_sk    sold_date_sk,
                 cs_ext_sales_price sales_price
          FROM   catalog_sales)),
     wswscs
     AS (SELECT d_week_seq,
                Sum(CASE
                      WHEN ( d_day_name = 'Sunday' ) THEN sales_price
                      ELSE NULL
                    END) sun_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Monday' ) THEN sales_price
                      ELSE NULL
                    END) mon_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Tuesday' ) THEN sales_price
                      ELSE NULL
                    END) tue_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Wednesday' ) THEN sales_price
                      ELSE NULL
                    END) wed_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Thursday' ) THEN sales_price
                      ELSE NULL
                    END) thu_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Friday' ) THEN sales_price
                      ELSE NULL
                    END) fri_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Saturday' ) THEN sales_price
                      ELSE NULL
                    END) sat_sales
         FROM   wscs,
                date_dim
         WHERE  d_date_sk = sold_date_sk
         GROUP  BY d_week_seq)
SELECT d_week_seq1,
       Round(sun_sales1 / sun_sales2, 2),
       Round(mon_sales1 / mon_sales2, 2),
       Round(tue_sales1 / tue_sales2, 2),
       Round(wed_sales1 / wed_sales2, 2),
       Round(thu_sales1 / thu_sales2, 2),
       Round(fri_sales1 / fri_sales2, 2),
       Round(sat_sales1 / sat_sales2, 2)
FROM   (SELECT wswscs.d_week_seq d_week_seq1,
               sun_sales         sun_sales1,
               mon_sales         mon_sales1,
               tue_sales         tue_sales1,
               wed_sales         wed_sales1,
               thu_sales         thu_sales1,
               fri_sales         fri_sales1,
               sat_sales         sat_sales1
        FROM   wswscs,
               date_dim
        WHERE  date_dim.d_week_seq = wswscs.d_week_seq
               AND d_year = 1998) y,
       (SELECT wswscs.d_week_seq d_week_seq2,
               sun_sales         sun_sales2,
               mon_sales         mon_sales2,
               tue_sales         tue_sales2,
               wed_sales         wed_sales2,
               thu_sales         thu_sales2,
               fri_sales         fri_sales2,
               sat_sales         sat_sales2
        FROM   wswscs,
               date_dim
        WHERE  date_dim.d_week_seq = wswscs.d_week_seq
               AND d_year = 1998 + 1) z
WHERE  d_week_seq1 = d_week_seq2 - 53
ORDER  BY d_week_seq1;
WITH "wscs" AS (
  SELECT
    "web_sales"."ws_sold_date_sk" AS "sold_date_sk",
    "web_sales"."ws_ext_sales_price" AS "sales_price"
  FROM "web_sales" AS "web_sales"
  UNION ALL
  (
    SELECT
      "catalog_sales"."cs_sold_date_sk" AS "sold_date_sk",
      "catalog_sales"."cs_ext_sales_price" AS "sales_price"
    FROM "catalog_sales" AS "catalog_sales"
  )
), "wswscs" AS (
  SELECT
    "date_dim"."d_week_seq" AS "d_week_seq",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Sunday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "sun_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Monday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "mon_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Tuesday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "tue_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Wednesday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "wed_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Thursday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "thu_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Friday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "fri_sales",
    SUM(
      CASE
        WHEN (
          "date_dim"."d_day_name" = 'Saturday'
        )
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "sat_sales"
  FROM "wscs"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "wscs"."sold_date_sk"
  GROUP BY
    "date_dim"."d_week_seq"
)
SELECT
  "wswscs"."d_week_seq" AS "d_week_seq1",
  ROUND("wswscs"."sun_sales" / "wswscs_2"."sun_sales", 2) AS "_col_1",
  ROUND("wswscs"."mon_sales" / "wswscs_2"."mon_sales", 2) AS "_col_2",
  ROUND("wswscs"."tue_sales" / "wswscs_2"."tue_sales", 2) AS "_col_3",
  ROUND("wswscs"."wed_sales" / "wswscs_2"."wed_sales", 2) AS "_col_4",
  ROUND("wswscs"."thu_sales" / "wswscs_2"."thu_sales", 2) AS "_col_5",
  ROUND("wswscs"."fri_sales" / "wswscs_2"."fri_sales", 2) AS "_col_6",
  ROUND("wswscs"."sat_sales" / "wswscs_2"."sat_sales", 2) AS "_col_7"
FROM "wswscs"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_week_seq" = "wswscs"."d_week_seq" AND "date_dim"."d_year" = 1998
JOIN "wswscs" AS "wswscs_2"
  ON "wswscs"."d_week_seq" = "wswscs_2"."d_week_seq" - 53
CROSS JOIN "date_dim" AS "date_dim_2"
WHERE
  "date_dim_2"."d_week_seq" = "wswscs_2"."d_week_seq" AND "date_dim_2"."d_year" = 1999
ORDER BY
  "d_week_seq1";

--------------------------------------
-- TPC-DS 3
--------------------------------------
SELECT dt.d_year,
               item.i_brand_id          brand_id,
               item.i_brand             brand,
               Sum(ss_ext_discount_amt) sum_agg
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manufact_id = 427
       AND dt.d_moy = 11
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
ORDER  BY dt.d_year,
          sum_agg DESC,
          brand_id
LIMIT 100;
SELECT
  "date_dim"."d_year" AS "d_year",
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  SUM("store_sales"."ss_ext_discount_amt") AS "sum_agg"
FROM "date_dim" AS "date_dim"
JOIN "store_sales" AS "store_sales"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_manufact_id" = 427 AND "store_sales"."ss_item_sk" = "item"."i_item_sk"
WHERE
  "date_dim"."d_moy" = 11
GROUP BY
  "date_dim"."d_year",
  "item"."i_brand",
  "item"."i_brand_id"
ORDER BY
  "date_dim"."d_year",
  "sum_agg" DESC,
  "brand_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 4
--------------------------------------
WITH year_total
     AS (SELECT c_customer_id                       customer_id,
                c_first_name                        customer_first_name,
                c_last_name                         customer_last_name,
                c_preferred_cust_flag               customer_preferred_cust_flag
                ,
                c_birth_country
                customer_birth_country,
                c_login                             customer_login,
                c_email_address                     customer_email_address,
                d_year                              dyear,
                Sum(( ( ss_ext_list_price - ss_ext_wholesale_cost
                        - ss_ext_discount_amt
                      )
                      +
                          ss_ext_sales_price ) / 2) year_total,
                's'                                 sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year
         UNION ALL
         SELECT c_customer_id                             customer_id,
                c_first_name                              customer_first_name,
                c_last_name                               customer_last_name,
                c_preferred_cust_flag
                customer_preferred_cust_flag,
                c_birth_country                           customer_birth_country
                ,
                c_login
                customer_login,
                c_email_address                           customer_email_address
                ,
                d_year                                    dyear
                ,
                Sum(( ( ( cs_ext_list_price
                          - cs_ext_wholesale_cost
                          - cs_ext_discount_amt
                        ) +
                              cs_ext_sales_price ) / 2 )) year_total,
                'c'                                       sale_type
         FROM   customer,
                catalog_sales,
                date_dim
         WHERE  c_customer_sk = cs_bill_customer_sk
                AND cs_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year
         UNION ALL
         SELECT c_customer_id                             customer_id,
                c_first_name                              customer_first_name,
                c_last_name                               customer_last_name,
                c_preferred_cust_flag
                customer_preferred_cust_flag,
                c_birth_country                           customer_birth_country
                ,
                c_login
                customer_login,
                c_email_address                           customer_email_address
                ,
                d_year                                    dyear
                ,
                Sum(( ( ( ws_ext_list_price
                          - ws_ext_wholesale_cost
                          - ws_ext_discount_amt
                        ) +
                              ws_ext_sales_price ) / 2 )) year_total,
                'w'                                       sale_type
         FROM   customer,
                web_sales,
                date_dim
         WHERE  c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year)
SELECT t_s_secyear.customer_id,
               t_s_secyear.customer_first_name,
               t_s_secyear.customer_last_name,
               t_s_secyear.customer_preferred_cust_flag
FROM   year_total t_s_firstyear,
       year_total t_s_secyear,
       year_total t_c_firstyear,
       year_total t_c_secyear,
       year_total t_w_firstyear,
       year_total t_w_secyear
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_c_secyear.customer_id
       AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id
       AND t_s_firstyear.sale_type = 's'
       AND t_c_firstyear.sale_type = 'c'
       AND t_w_firstyear.sale_type = 'w'
       AND t_s_secyear.sale_type = 's'
       AND t_c_secyear.sale_type = 'c'
       AND t_w_secyear.sale_type = 'w'
       AND t_s_firstyear.dyear = 2001
       AND t_s_secyear.dyear = 2001 + 1
       AND t_c_firstyear.dyear = 2001
       AND t_c_secyear.dyear = 2001 + 1
       AND t_w_firstyear.dyear = 2001
       AND t_w_secyear.dyear = 2001 + 1
       AND t_s_firstyear.year_total > 0
       AND t_c_firstyear.year_total > 0
       AND t_w_firstyear.year_total > 0
       AND CASE
             WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total /
                                                    t_c_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN
                   t_s_secyear.year_total /
                   t_s_firstyear.year_total
                   ELSE NULL
                 END
       AND CASE
             WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total /
                                                    t_c_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_w_firstyear.year_total > 0 THEN
                   t_w_secyear.year_total /
                   t_w_firstyear.year_total
                   ELSE NULL
                 END
ORDER  BY t_s_secyear.customer_id,
          t_s_secyear.customer_first_name,
          t_s_secyear.customer_last_name,
          t_s_secyear.customer_preferred_cust_flag
LIMIT 100;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_customer_id" AS "c_customer_id",
    "customer"."c_first_name" AS "c_first_name",
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_preferred_cust_flag" AS "c_preferred_cust_flag",
    "customer"."c_birth_country" AS "c_birth_country",
    "customer"."c_login" AS "c_login",
    "customer"."c_email_address" AS "c_email_address"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
), "cte" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "customer"."c_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "date_dim"."d_year" AS "dyear",
    SUM(
      (
        (
          "store_sales"."ss_ext_list_price" - "store_sales"."ss_ext_wholesale_cost" - "store_sales"."ss_ext_discount_amt"
        ) + "store_sales"."ss_ext_sales_price"
      ) / 2
    ) AS "year_total",
    's' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "store_sales" AS "store_sales"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
), "cte_2" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "customer"."c_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "date_dim"."d_year" AS "dyear",
    SUM(
      (
        (
          (
            "catalog_sales"."cs_ext_list_price" - "catalog_sales"."cs_ext_wholesale_cost" - "catalog_sales"."cs_ext_discount_amt"
          ) + "catalog_sales"."cs_ext_sales_price"
        ) / 2
      )
    ) AS "year_total",
    'c' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "catalog_sales" AS "catalog_sales"
    ON "customer"."c_customer_sk" = "catalog_sales"."cs_bill_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
), "cte_3" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "customer"."c_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "date_dim"."d_year" AS "dyear",
    SUM(
      (
        (
          (
            "web_sales"."ws_ext_list_price" - "web_sales"."ws_ext_wholesale_cost" - "web_sales"."ws_ext_discount_amt"
          ) + "web_sales"."ws_ext_sales_price"
        ) / 2
      )
    ) AS "year_total",
    'w' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "web_sales" AS "web_sales"
    ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
), "cte_4" AS (
  SELECT
    "cte_2"."customer_id" AS "customer_id",
    "cte_2"."customer_first_name" AS "customer_first_name",
    "cte_2"."customer_last_name" AS "customer_last_name",
    "cte_2"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "cte_2"."dyear" AS "dyear",
    "cte_2"."year_total" AS "year_total",
    "cte_2"."sale_type" AS "sale_type"
  FROM "cte_2" AS "cte_2"
  UNION ALL
  SELECT
    "cte_3"."customer_id" AS "customer_id",
    "cte_3"."customer_first_name" AS "customer_first_name",
    "cte_3"."customer_last_name" AS "customer_last_name",
    "cte_3"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "cte_3"."dyear" AS "dyear",
    "cte_3"."year_total" AS "year_total",
    "cte_3"."sale_type" AS "sale_type"
  FROM "cte_3" AS "cte_3"
), "year_total" AS (
  SELECT
    "cte"."customer_id" AS "customer_id",
    "cte"."customer_first_name" AS "customer_first_name",
    "cte"."customer_last_name" AS "customer_last_name",
    "cte"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "cte"."dyear" AS "dyear",
    "cte"."year_total" AS "year_total",
    "cte"."sale_type" AS "sale_type"
  FROM "cte" AS "cte"
  UNION ALL
  SELECT
    "cte_4"."customer_id" AS "customer_id",
    "cte_4"."customer_first_name" AS "customer_first_name",
    "cte_4"."customer_last_name" AS "customer_last_name",
    "cte_4"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag",
    "cte_4"."dyear" AS "dyear",
    "cte_4"."year_total" AS "year_total",
    "cte_4"."sale_type" AS "sale_type"
  FROM "cte_4" AS "cte_4"
)
SELECT
  "t_s_secyear"."customer_id" AS "customer_id",
  "t_s_secyear"."customer_first_name" AS "customer_first_name",
  "t_s_secyear"."customer_last_name" AS "customer_last_name",
  "t_s_secyear"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag"
FROM "year_total" AS "t_s_firstyear"
JOIN "year_total" AS "t_s_secyear"
  ON "t_s_secyear"."customer_id" = "t_s_firstyear"."customer_id"
  AND "t_s_secyear"."dyear" = 2002
  AND "t_s_secyear"."sale_type" = 's'
JOIN "year_total" AS "t_c_secyear"
  ON "t_c_secyear"."dyear" = 2002
  AND "t_c_secyear"."sale_type" = 'c'
  AND "t_s_firstyear"."customer_id" = "t_c_secyear"."customer_id"
JOIN "year_total" AS "t_w_firstyear"
  ON "t_s_firstyear"."customer_id" = "t_w_firstyear"."customer_id"
  AND "t_w_firstyear"."dyear" = 2001
  AND "t_w_firstyear"."sale_type" = 'w'
  AND "t_w_firstyear"."year_total" > 0
JOIN "year_total" AS "t_w_secyear"
  ON "t_s_firstyear"."customer_id" = "t_w_secyear"."customer_id"
  AND "t_w_secyear"."dyear" = 2002
  AND "t_w_secyear"."sale_type" = 'w'
JOIN "year_total" AS "t_c_firstyear"
  ON "t_c_firstyear"."dyear" = 2001
  AND "t_c_firstyear"."sale_type" = 'c'
  AND "t_c_firstyear"."year_total" > 0
  AND "t_s_firstyear"."customer_id" = "t_c_firstyear"."customer_id"
  AND CASE
    WHEN "t_c_firstyear"."year_total" > 0
    THEN "t_c_secyear"."year_total" / "t_c_firstyear"."year_total"
    ELSE NULL
  END > CASE
    WHEN "t_s_firstyear"."year_total" > 0
    THEN "t_s_secyear"."year_total" / "t_s_firstyear"."year_total"
    ELSE NULL
  END
  AND CASE
    WHEN "t_c_firstyear"."year_total" > 0
    THEN "t_c_secyear"."year_total" / "t_c_firstyear"."year_total"
    ELSE NULL
  END > CASE
    WHEN "t_w_firstyear"."year_total" > 0
    THEN "t_w_secyear"."year_total" / "t_w_firstyear"."year_total"
    ELSE NULL
  END
WHERE
  "t_s_firstyear"."dyear" = 2001
  AND "t_s_firstyear"."sale_type" = 's'
  AND "t_s_firstyear"."year_total" > 0
ORDER BY
  "t_s_secyear"."customer_id",
  "t_s_secyear"."customer_first_name",
  "t_s_secyear"."customer_last_name",
  "t_s_secyear"."customer_preferred_cust_flag"
LIMIT 100;

--------------------------------------
-- TPC-DS 5
--------------------------------------
WITH ssr AS
(
         SELECT   s_store_id,
                  Sum(sales_price) AS sales,
                  Sum(profit)      AS profit,
                  Sum(return_amt)  AS returns1,
                  Sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT ss_store_sk             AS store_sk,
                                ss_sold_date_sk         AS date_sk,
                                ss_ext_sales_price      AS sales_price,
                                ss_net_profit           AS profit,
                                Cast(0 AS DECIMAL(7,2)) AS return_amt,
                                Cast(0 AS DECIMAL(7,2)) AS net_loss
                         FROM   store_sales
                         UNION ALL
                         SELECT sr_store_sk             AS store_sk,
                                sr_returned_date_sk     AS date_sk,
                                Cast(0 AS DECIMAL(7,2)) AS sales_price,
                                Cast(0 AS DECIMAL(7,2)) AS profit,
                                sr_return_amt           AS return_amt,
                                sr_net_loss             AS net_loss
                         FROM   store_returns ) salesreturns,
                  date_dim,
                  store
         WHERE    date_sk = d_date_sk
         AND      d_date BETWEEN Cast('2002-08-22' AS DATE) AND      (
                           Cast('2002-08-22' AS DATE) + INTERVAL '14' day)
         AND      store_sk = s_store_sk
         GROUP BY s_store_id) , csr AS
(
         SELECT   cp_catalog_page_id,
                  sum(sales_price) AS sales,
                  sum(profit)      AS profit,
                  sum(return_amt)  AS returns1,
                  sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT cs_catalog_page_sk      AS page_sk,
                                cs_sold_date_sk         AS date_sk,
                                cs_ext_sales_price      AS sales_price,
                                cs_net_profit           AS profit,
                                cast(0 AS decimal(7,2)) AS return_amt,
                                cast(0 AS decimal(7,2)) AS net_loss
                         FROM   catalog_sales
                         UNION ALL
                         SELECT cr_catalog_page_sk      AS page_sk,
                                cr_returned_date_sk     AS date_sk,
                                cast(0 AS decimal(7,2)) AS sales_price,
                                cast(0 AS decimal(7,2)) AS profit,
                                cr_return_amount        AS return_amt,
                                cr_net_loss             AS net_loss
                         FROM   catalog_returns ) salesreturns,
                  date_dim,
                  catalog_page
         WHERE    date_sk = d_date_sk
         AND      d_date BETWEEN cast('2002-08-22' AS date) AND      (
                           cast('2002-08-22' AS date) + INTERVAL '14' day)
         AND      page_sk = cp_catalog_page_sk
         GROUP BY cp_catalog_page_id) , wsr AS
(
         SELECT   web_site_id,
                  sum(sales_price) AS sales,
                  sum(profit)      AS profit,
                  sum(return_amt)  AS returns1,
                  sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT ws_web_site_sk          AS wsr_web_site_sk,
                                ws_sold_date_sk         AS date_sk,
                                ws_ext_sales_price      AS sales_price,
                                ws_net_profit           AS profit,
                                cast(0 AS decimal(7,2)) AS return_amt,
                                cast(0 AS decimal(7,2)) AS net_loss
                         FROM   web_sales
                         UNION ALL
                         SELECT          ws_web_site_sk          AS wsr_web_site_sk,
                                         wr_returned_date_sk     AS date_sk,
                                         cast(0 AS decimal(7,2)) AS sales_price,
                                         cast(0 AS decimal(7,2)) AS profit,
                                         wr_return_amt           AS return_amt,
                                         wr_net_loss             AS net_loss
                         FROM            web_returns
                         LEFT OUTER JOIN web_sales
                         ON              (
                                                         wr_item_sk = ws_item_sk
                                         AND             wr_order_number = ws_order_number) ) salesreturns,
                  date_dim,
                  web_site
         WHERE    date_sk = d_date_sk
         AND      d_date BETWEEN cast('2002-08-22' AS date) AND      (
                           cast('2002-08-22' AS date) + INTERVAL '14' day)
         AND      wsr_web_site_sk = web_site_sk
         GROUP BY web_site_id)
SELECT
         channel ,
         id ,
         sum(sales)   AS sales ,
         sum(returns1) AS returns1 ,
         sum(profit)  AS profit
FROM     (
                SELECT 'store channel' AS channel ,
                       'store'
                              || s_store_id AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   ssr
                UNION ALL
                SELECT 'catalog channel' AS channel ,
                       'catalog_page'
                              || cp_catalog_page_id AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   csr
                UNION ALL
                SELECT 'web channel' AS channel ,
                       'web_site'
                              || web_site_id AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   wsr ) x
GROUP BY rollup (channel, id)
ORDER BY channel ,
         id
LIMIT 100;
WITH "salesreturns" AS (
  SELECT
    "store_sales"."ss_store_sk" AS "store_sk",
    "store_sales"."ss_sold_date_sk" AS "date_sk",
    "store_sales"."ss_ext_sales_price" AS "sales_price",
    "store_sales"."ss_net_profit" AS "profit",
    CAST(0 AS DECIMAL(7, 2)) AS "return_amt",
    CAST(0 AS DECIMAL(7, 2)) AS "net_loss"
  FROM "store_sales" AS "store_sales"
  UNION ALL
  SELECT
    "store_returns"."sr_store_sk" AS "store_sk",
    "store_returns"."sr_returned_date_sk" AS "date_sk",
    CAST(0 AS DECIMAL(7, 2)) AS "sales_price",
    CAST(0 AS DECIMAL(7, 2)) AS "profit",
    "store_returns"."sr_return_amt" AS "return_amt",
    "store_returns"."sr_net_loss" AS "net_loss"
  FROM "store_returns" AS "store_returns"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  WHERE
    CAST("date_dim"."d_date" AS DATE) <= CAST('2002-09-05' AS DATE)
    AND CAST("date_dim"."d_date" AS DATE) >= CAST('2002-08-22' AS DATE)
), "ssr" AS (
  SELECT
    "store"."s_store_id" AS "s_store_id",
    SUM("salesreturns"."sales_price") AS "sales",
    SUM("salesreturns"."profit") AS "profit",
    SUM("salesreturns"."return_amt") AS "returns1",
    SUM("salesreturns"."net_loss") AS "profit_loss"
  FROM "salesreturns" AS "salesreturns"
  JOIN "date_dim_2" AS "date_dim"
    ON "salesreturns"."date_sk" = "date_dim"."d_date_sk"
  JOIN "store" AS "store"
    ON "salesreturns"."store_sk" = "store"."s_store_sk"
  GROUP BY
    "store"."s_store_id"
), "salesreturns_2" AS (
  SELECT
    "catalog_sales"."cs_catalog_page_sk" AS "page_sk",
    "catalog_sales"."cs_sold_date_sk" AS "date_sk",
    "catalog_sales"."cs_ext_sales_price" AS "sales_price",
    "catalog_sales"."cs_net_profit" AS "profit",
    CAST(0 AS DECIMAL(7, 2)) AS "return_amt",
    CAST(0 AS DECIMAL(7, 2)) AS "net_loss"
  FROM "catalog_sales" AS "catalog_sales"
  UNION ALL
  SELECT
    "catalog_returns"."cr_catalog_page_sk" AS "page_sk",
    "catalog_returns"."cr_returned_date_sk" AS "date_sk",
    CAST(0 AS DECIMAL(7, 2)) AS "sales_price",
    CAST(0 AS DECIMAL(7, 2)) AS "profit",
    "catalog_returns"."cr_return_amount" AS "return_amt",
    "catalog_returns"."cr_net_loss" AS "net_loss"
  FROM "catalog_returns" AS "catalog_returns"
), "csr" AS (
  SELECT
    "catalog_page"."cp_catalog_page_id" AS "cp_catalog_page_id",
    SUM("salesreturns"."sales_price") AS "sales",
    SUM("salesreturns"."profit") AS "profit",
    SUM("salesreturns"."return_amt") AS "returns1",
    SUM("salesreturns"."net_loss") AS "profit_loss"
  FROM "salesreturns_2" AS "salesreturns"
  JOIN "date_dim_2" AS "date_dim"
    ON "salesreturns"."date_sk" = "date_dim"."d_date_sk"
  JOIN "catalog_page" AS "catalog_page"
    ON "salesreturns"."page_sk" = "catalog_page"."cp_catalog_page_sk"
  GROUP BY
    "catalog_page"."cp_catalog_page_id"
), "salesreturns_3" AS (
  SELECT
    "web_sales"."ws_web_site_sk" AS "wsr_web_site_sk",
    "web_sales"."ws_sold_date_sk" AS "date_sk",
    "web_sales"."ws_ext_sales_price" AS "sales_price",
    "web_sales"."ws_net_profit" AS "profit",
    CAST(0 AS DECIMAL(7, 2)) AS "return_amt",
    CAST(0 AS DECIMAL(7, 2)) AS "net_loss"
  FROM "web_sales" AS "web_sales"
  UNION ALL
  SELECT
    "web_sales"."ws_web_site_sk" AS "wsr_web_site_sk",
    "web_returns"."wr_returned_date_sk" AS "date_sk",
    CAST(0 AS DECIMAL(7, 2)) AS "sales_price",
    CAST(0 AS DECIMAL(7, 2)) AS "profit",
    "web_returns"."wr_return_amt" AS "return_amt",
    "web_returns"."wr_net_loss" AS "net_loss"
  FROM "web_returns" AS "web_returns"
  LEFT JOIN "web_sales" AS "web_sales"
    ON "web_returns"."wr_item_sk" = "web_sales"."ws_item_sk"
    AND "web_returns"."wr_order_number" = "web_sales"."ws_order_number"
), "wsr" AS (
  SELECT
    "web_site"."web_site_id" AS "web_site_id",
    SUM("salesreturns"."sales_price") AS "sales",
    SUM("salesreturns"."profit") AS "profit",
    SUM("salesreturns"."return_amt") AS "returns1",
    SUM("salesreturns"."net_loss") AS "profit_loss"
  FROM "salesreturns_3" AS "salesreturns"
  JOIN "date_dim_2" AS "date_dim"
    ON "salesreturns"."date_sk" = "date_dim"."d_date_sk"
  JOIN "web_site" AS "web_site"
    ON "salesreturns"."wsr_web_site_sk" = "web_site"."web_site_sk"
  GROUP BY
    "web_site"."web_site_id"
), "cte_10" AS (
  SELECT
    'catalog channel' AS "channel",
    'catalog_page' || "csr"."cp_catalog_page_id" AS "id",
    "csr"."sales" AS "sales",
    "csr"."returns1" AS "returns1",
    "csr"."profit" - "csr"."profit_loss" AS "profit"
  FROM "csr"
  UNION ALL
  SELECT
    'web channel' AS "channel",
    'web_site' || "wsr"."web_site_id" AS "id",
    "wsr"."sales" AS "sales",
    "wsr"."returns1" AS "returns1",
    "wsr"."profit" - "wsr"."profit_loss" AS "profit"
  FROM "wsr"
), "x" AS (
  SELECT
    'store channel' AS "channel",
    'store' || "ssr"."s_store_id" AS "id",
    "ssr"."sales" AS "sales",
    "ssr"."returns1" AS "returns1",
    "ssr"."profit" - "ssr"."profit_loss" AS "profit"
  FROM "ssr"
  UNION ALL
  SELECT
    "cte_10"."channel" AS "channel",
    "cte_10"."id" AS "id",
    "cte_10"."sales" AS "sales",
    "cte_10"."returns1" AS "returns1",
    "cte_10"."profit" AS "profit"
  FROM "cte_10" AS "cte_10"
)
SELECT
  "x"."channel" AS "channel",
  "x"."id" AS "id",
  SUM("x"."sales") AS "sales",
  SUM("x"."returns1") AS "returns1",
  SUM("x"."profit") AS "profit"
FROM "x" AS "x"
GROUP BY
ROLLUP (
  "x"."channel",
  "x"."id"
)
ORDER BY
  "channel",
  "id"
LIMIT 100;

--------------------------------------
-- TPC-DS 6
--------------------------------------
SELECT a.ca_state state,
               Count(*)   cnt
FROM   customer_address a,
       customer c,
       store_sales s,
       date_dim d,
       item i
WHERE  a.ca_address_sk = c.c_current_addr_sk
       AND c.c_customer_sk = s.ss_customer_sk
       AND s.ss_sold_date_sk = d.d_date_sk
       AND s.ss_item_sk = i.i_item_sk
       AND d.d_month_seq = (SELECT DISTINCT ( d_month_seq )
                            FROM   date_dim
                            WHERE  d_year = 1998
                                   AND d_moy = 7)
       AND i.i_current_price > 1.2 * (SELECT Avg(j.i_current_price)
                                      FROM   item j
                                      WHERE  j.i_category = i.i_category)
GROUP  BY a.ca_state
HAVING Count(*) >= 10
ORDER  BY cnt
LIMIT 100;
WITH "_u_0" AS (
  SELECT DISTINCT
    "date_dim"."d_month_seq" AS "_col_0"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 7 AND "date_dim"."d_year" = 1998
), "_u_1" AS (
  SELECT
    AVG("j"."i_current_price") AS "_col_0",
    "j"."i_category" AS "_u_2"
  FROM "item" AS "j"
  GROUP BY
    "j"."i_category"
)
SELECT
  "customer_address"."ca_state" AS "state",
  COUNT(*) AS "cnt"
FROM "customer_address" AS "customer_address"
CROSS JOIN "_u_0" AS "_u_0"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_month_seq" = "_u_0"."_col_0"
JOIN "store_sales" AS "store_sales"
  ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
JOIN "item" AS "item"
  ON "store_sales"."ss_item_sk" = "item"."i_item_sk"
LEFT JOIN "_u_1" AS "_u_1"
  ON "_u_1"."_u_2" = "item"."i_category"
JOIN "customer" AS "customer"
  ON "customer_address"."ca_address_sk" = "customer"."c_current_addr_sk"
  AND "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
WHERE
  "item"."i_current_price" > 1.2 * "_u_1"."_col_0"
GROUP BY
  "customer_address"."ca_state"
HAVING
  COUNT(*) >= 10
ORDER BY
  "cnt"
LIMIT 100;

--------------------------------------
-- TPC-DS 7
--------------------------------------
SELECT i_item_id,
               Avg(ss_quantity)    agg1,
               Avg(ss_list_price)  agg2,
               Avg(ss_coupon_amt)  agg3,
               Avg(ss_sales_price) agg4
FROM   store_sales,
       customer_demographics,
       date_dim,
       item,
       promotion
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND ss_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'W'
       AND cd_education_status = '2 yr Degree'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 1998
GROUP  BY i_item_id
ORDER  BY i_item_id
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  AVG("store_sales"."ss_quantity") AS "agg1",
  AVG("store_sales"."ss_list_price") AS "agg2",
  AVG("store_sales"."ss_coupon_amt") AS "agg3",
  AVG("store_sales"."ss_sales_price") AS "agg4"
FROM "store_sales" AS "store_sales"
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer_demographics"."cd_education_status" = '2 yr Degree'
  AND "customer_demographics"."cd_gender" = 'F'
  AND "customer_demographics"."cd_marital_status" = 'W'
  AND "store_sales"."ss_cdemo_sk" = "customer_demographics"."cd_demo_sk"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_year" = 1998
  AND "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
JOIN "item" AS "item"
  ON "store_sales"."ss_item_sk" = "item"."i_item_sk"
JOIN "promotion" AS "promotion"
  ON (
    "promotion"."p_channel_email" = 'N' OR "promotion"."p_channel_event" = 'N'
  )
  AND "store_sales"."ss_promo_sk" = "promotion"."p_promo_sk"
GROUP BY
  "item"."i_item_id"
ORDER BY
  "i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 8
--------------------------------------
SELECT s_store_name,
               Sum(ss_net_profit)
FROM   store_sales,
       date_dim,
       store,
       (SELECT ca_zip
        FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip
                FROM   customer_address
                WHERE  Substr(ca_zip, 1, 5) IN ( '67436', '26121', '38443',
                                                 '63157',
                                                 '68856', '19485', '86425',
                                                 '26741',
                                                 '70991', '60899', '63573',
                                                 '47556',
                                                 '56193', '93314', '87827',
                                                 '62017',
                                                 '85067', '95390', '48091',
                                                 '10261',
                                                 '81845', '41790', '42853',
                                                 '24675',
                                                 '12840', '60065', '84430',
                                                 '57451',
                                                 '24021', '91735', '75335',
                                                 '71935',
                                                 '34482', '56943', '70695',
                                                 '52147',
                                                 '56251', '28411', '86653',
                                                 '23005',
                                                 '22478', '29031', '34398',
                                                 '15365',
                                                 '42460', '33337', '59433',
                                                 '73943',
                                                 '72477', '74081', '74430',
                                                 '64605',
                                                 '39006', '11226', '49057',
                                                 '97308',
                                                 '42663', '18187', '19768',
                                                 '43454',
                                                 '32147', '76637', '51975',
                                                 '11181',
                                                 '45630', '33129', '45995',
                                                 '64386',
                                                 '55522', '26697', '20963',
                                                 '35154',
                                                 '64587', '49752', '66386',
                                                 '30586',
                                                 '59286', '13177', '66646',
                                                 '84195',
                                                 '74316', '36853', '32927',
                                                 '12469',
                                                 '11904', '36269', '17724',
                                                 '55346',
                                                 '12595', '53988', '65439',
                                                 '28015',
                                                 '63268', '73590', '29216',
                                                 '82575',
                                                 '69267', '13805', '91678',
                                                 '79460',
                                                 '94152', '14961', '15419',
                                                 '48277',
                                                 '62588', '55493', '28360',
                                                 '14152',
                                                 '55225', '18007', '53705',
                                                 '56573',
                                                 '80245', '71769', '57348',
                                                 '36845',
                                                 '13039', '17270', '22363',
                                                 '83474',
                                                 '25294', '43269', '77666',
                                                 '15488',
                                                 '99146', '64441', '43338',
                                                 '38736',
                                                 '62754', '48556', '86057',
                                                 '23090',
                                                 '38114', '66061', '18910',
                                                 '84385',
                                                 '23600', '19975', '27883',
                                                 '65719',
                                                 '19933', '32085', '49731',
                                                 '40473',
                                                 '27190', '46192', '23949',
                                                 '44738',
                                                 '12436', '64794', '68741',
                                                 '15333',
                                                 '24282', '49085', '31844',
                                                 '71156',
                                                 '48441', '17100', '98207',
                                                 '44982',
                                                 '20277', '71496', '96299',
                                                 '37583',
                                                 '22206', '89174', '30589',
                                                 '61924',
                                                 '53079', '10976', '13104',
                                                 '42794',
                                                 '54772', '15809', '56434',
                                                 '39975',
                                                 '13874', '30753', '77598',
                                                 '78229',
                                                 '59478', '12345', '55547',
                                                 '57422',
                                                 '42600', '79444', '29074',
                                                 '29752',
                                                 '21676', '32096', '43044',
                                                 '39383',
                                                 '37296', '36295', '63077',
                                                 '16572',
                                                 '31275', '18701', '40197',
                                                 '48242',
                                                 '27219', '49865', '84175',
                                                 '30446',
                                                 '25165', '13807', '72142',
                                                 '70499',
                                                 '70464', '71429', '18111',
                                                 '70857',
                                                 '29545', '36425', '52706',
                                                 '36194',
                                                 '42963', '75068', '47921',
                                                 '74763',
                                                 '90990', '89456', '62073',
                                                 '88397',
                                                 '73963', '75885', '62657',
                                                 '12530',
                                                 '81146', '57434', '25099',
                                                 '41429',
                                                 '98441', '48713', '52552',
                                                 '31667',
                                                 '14072', '13903', '44709',
                                                 '85429',
                                                 '58017', '38295', '44875',
                                                 '73541',
                                                 '30091', '12707', '23762',
                                                 '62258',
                                                 '33247', '78722', '77431',
                                                 '14510',
                                                 '35656', '72428', '92082',
                                                 '35267',
                                                 '43759', '24354', '90952',
                                                 '11512',
                                                 '21242', '22579', '56114',
                                                 '32339',
                                                 '52282', '41791', '24484',
                                                 '95020',
                                                 '28408', '99710', '11899',
                                                 '43344',
                                                 '72915', '27644', '62708',
                                                 '74479',
                                                 '17177', '32619', '12351',
                                                 '91339',
                                                 '31169', '57081', '53522',
                                                 '16712',
                                                 '34419', '71779', '44187',
                                                 '46206',
                                                 '96099', '61910', '53664',
                                                 '12295',
                                                 '31837', '33096', '10813',
                                                 '63048',
                                                 '31732', '79118', '73084',
                                                 '72783',
                                                 '84952', '46965', '77956',
                                                 '39815',
                                                 '32311', '75329', '48156',
                                                 '30826',
                                                 '49661', '13736', '92076',
                                                 '74865',
                                                 '88149', '92397', '52777',
                                                 '68453',
                                                 '32012', '21222', '52721',
                                                 '24626',
                                                 '18210', '42177', '91791',
                                                 '75251',
                                                 '82075', '44372', '45542',
                                                 '20609',
                                                 '60115', '17362', '22750',
                                                 '90434',
                                                 '31852', '54071', '33762',
                                                 '14705',
                                                 '40718', '56433', '30996',
                                                 '40657',
                                                 '49056', '23585', '66455',
                                                 '41021',
                                                 '74736', '72151', '37007',
                                                 '21729',
                                                 '60177', '84558', '59027',
                                                 '93855',
                                                 '60022', '86443', '19541',
                                                 '86886',
                                                 '30532', '39062', '48532',
                                                 '34713',
                                                 '52077', '22564', '64638',
                                                 '15273',
                                                 '31677', '36138', '62367',
                                                 '60261',
                                                 '80213', '42818', '25113',
                                                 '72378',
                                                 '69802', '69096', '55443',
                                                 '28820',
                                                 '13848', '78258', '37490',
                                                 '30556',
                                                 '77380', '28447', '44550',
                                                 '26791',
                                                 '70609', '82182', '33306',
                                                 '43224',
                                                 '22322', '86959', '68519',
                                                 '14308',
                                                 '46501', '81131', '34056',
                                                 '61991',
                                                 '19896', '87804', '65774',
                                                 '92564' )
                INTERSECT
                SELECT ca_zip
                FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip,
                               Count(*)             cnt
                        FROM   customer_address,
                               customer
                        WHERE  ca_address_sk = c_current_addr_sk
                               AND c_preferred_cust_flag = 'Y'
                        GROUP  BY ca_zip
                        HAVING Count(*) > 10)A1)A2) V1
WHERE  ss_store_sk = s_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_qoy = 2
       AND d_year = 2000
       AND ( Substr(s_zip, 1, 2) = Substr(V1.ca_zip, 1, 2) )
GROUP  BY s_store_name
ORDER  BY s_store_name
LIMIT 100;
WITH "a1" AS (
  SELECT
    SUBSTR("customer_address"."ca_zip", 1, 5) AS "ca_zip"
  FROM "customer_address" AS "customer_address", "customer" AS "customer"
  WHERE
    "customer"."c_preferred_cust_flag" = 'Y'
    AND "customer_address"."ca_address_sk" = "customer"."c_current_addr_sk"
  GROUP BY
    "customer_address"."ca_zip"
  HAVING
    COUNT(*) > 10
), "a2" AS (
  SELECT
    SUBSTR("customer_address"."ca_zip", 1, 5) AS "ca_zip"
  FROM "customer_address" AS "customer_address"
  WHERE
    SUBSTR("customer_address"."ca_zip", 1, 5) IN ('67436', '26121', '38443', '63157', '68856', '19485', '86425', '26741', '70991', '60899', '63573', '47556', '56193', '93314', '87827', '62017', '85067', '95390', '48091', '10261', '81845', '41790', '42853', '24675', '12840', '60065', '84430', '57451', '24021', '91735', '75335', '71935', '34482', '56943', '70695', '52147', '56251', '28411', '86653', '23005', '22478', '29031', '34398', '15365', '42460', '33337', '59433', '73943', '72477', '74081', '74430', '64605', '39006', '11226', '49057', '97308', '42663', '18187', '19768', '43454', '32147', '76637', '51975', '11181', '45630', '33129', '45995', '64386', '55522', '26697', '20963', '35154', '64587', '49752', '66386', '30586', '59286', '13177', '66646', '84195', '74316', '36853', '32927', '12469', '11904', '36269', '17724', '55346', '12595', '53988', '65439', '28015', '63268', '73590', '29216', '82575', '69267', '13805', '91678', '79460', '94152', '14961', '15419', '48277', '62588', '55493', '28360', '14152', '55225', '18007', '53705', '56573', '80245', '71769', '57348', '36845', '13039', '17270', '22363', '83474', '25294', '43269', '77666', '15488', '99146', '64441', '43338', '38736', '62754', '48556', '86057', '23090', '38114', '66061', '18910', '84385', '23600', '19975', '27883', '65719', '19933', '32085', '49731', '40473', '27190', '46192', '23949', '44738', '12436', '64794', '68741', '15333', '24282', '49085', '31844', '71156', '48441', '17100', '98207', '44982', '20277', '71496', '96299', '37583', '22206', '89174', '30589', '61924', '53079', '10976', '13104', '42794', '54772', '15809', '56434', '39975', '13874', '30753', '77598', '78229', '59478', '12345', '55547', '57422', '42600', '79444', '29074', '29752', '21676', '32096', '43044', '39383', '37296', '36295', '63077', '16572', '31275', '18701', '40197', '48242', '27219', '49865', '84175', '30446', '25165', '13807', '72142', '70499', '70464', '71429', '18111', '70857', '29545', '36425', '52706', '36194', '42963', '75068', '47921', '74763', '90990', '89456', '62073', '88397', '73963', '75885', '62657', '12530', '81146', '57434', '25099', '41429', '98441', '48713', '52552', '31667', '14072', '13903', '44709', '85429', '58017', '38295', '44875', '73541', '30091', '12707', '23762', '62258', '33247', '78722', '77431', '14510', '35656', '72428', '92082', '35267', '43759', '24354', '90952', '11512', '21242', '22579', '56114', '32339', '52282', '41791', '24484', '95020', '28408', '99710', '11899', '43344', '72915', '27644', '62708', '74479', '17177', '32619', '12351', '91339', '31169', '57081', '53522', '16712', '34419', '71779', '44187', '46206', '96099', '61910', '53664', '12295', '31837', '33096', '10813', '63048', '31732', '79118', '73084', '72783', '84952', '46965', '77956', '39815', '32311', '75329', '48156', '30826', '49661', '13736', '92076', '74865', '88149', '92397', '52777', '68453', '32012', '21222', '52721', '24626', '18210', '42177', '91791', '75251', '82075', '44372', '45542', '20609', '60115', '17362', '22750', '90434', '31852', '54071', '33762', '14705', '40718', '56433', '30996', '40657', '49056', '23585', '66455', '41021', '74736', '72151', '37007', '21729', '60177', '84558', '59027', '93855', '60022', '86443', '19541', '86886', '30532', '39062', '48532', '34713', '52077', '22564', '64638', '15273', '31677', '36138', '62367', '60261', '80213', '42818', '25113', '72378', '69802', '69096', '55443', '28820', '13848', '78258', '37490', '30556', '77380', '28447', '44550', '26791', '70609', '82182', '33306', '43224', '22322', '86959', '68519', '14308', '46501', '81131', '34056', '61991', '19896', '87804', '65774', '92564')
  INTERSECT
  SELECT
    "a1"."ca_zip" AS "ca_zip"
  FROM "a1" AS "a1"
)
SELECT
  "store"."s_store_name" AS "s_store_name",
  SUM("store_sales"."ss_net_profit") AS "_col_1"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_qoy" = 2
  AND "date_dim"."d_year" = 2000
  AND "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
JOIN "store" AS "store"
  ON "store_sales"."ss_store_sk" = "store"."s_store_sk"
JOIN "a2" AS "a2"
  ON SUBSTR("store"."s_zip", 1, 2) = SUBSTR("a2"."ca_zip", 1, 2)
GROUP BY
  "store"."s_store_name"
ORDER BY
  "s_store_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 9
--------------------------------------
SELECT CASE
         WHEN (SELECT Count(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 1 AND 20) > 3672 THEN
         (SELECT Avg(ss_ext_list_price)
          FROM   store_sales
          WHERE
         ss_quantity BETWEEN 1 AND 20)
         ELSE (SELECT Avg(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 1 AND 20)
       END bucket1,
       CASE
         WHEN (SELECT Count(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 21 AND 40) > 3392 THEN
         (SELECT Avg(ss_ext_list_price)
          FROM   store_sales
          WHERE
         ss_quantity BETWEEN 21 AND 40)
         ELSE (SELECT Avg(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 21 AND 40)
       END bucket2,
       CASE
         WHEN (SELECT Count(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 41 AND 60) > 32784 THEN
         (SELECT Avg(ss_ext_list_price)
          FROM   store_sales
          WHERE
         ss_quantity BETWEEN 41 AND 60)
         ELSE (SELECT Avg(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 41 AND 60)
       END bucket3,
       CASE
         WHEN (SELECT Count(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 61 AND 80) > 26032 THEN
         (SELECT Avg(ss_ext_list_price)
          FROM   store_sales
          WHERE
         ss_quantity BETWEEN 61 AND 80)
         ELSE (SELECT Avg(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 61 AND 80)
       END bucket4,
       CASE
         WHEN (SELECT Count(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 81 AND 100) > 23982 THEN
         (SELECT Avg(ss_ext_list_price)
          FROM   store_sales
          WHERE
         ss_quantity BETWEEN 81 AND 100)
         ELSE (SELECT Avg(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 81 AND 100)
       END bucket5
FROM   reason
WHERE  r_reason_sk = 1;
WITH "_u_0" AS (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 20 AND "store_sales"."ss_quantity" >= 1
), "_u_1" AS (
  SELECT
    AVG("store_sales"."ss_ext_list_price") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 20 AND "store_sales"."ss_quantity" >= 1
), "_u_2" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 20 AND "store_sales"."ss_quantity" >= 1
), "_u_3" AS (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 40 AND "store_sales"."ss_quantity" >= 21
), "_u_4" AS (
  SELECT
    AVG("store_sales"."ss_ext_list_price") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 40 AND "store_sales"."ss_quantity" >= 21
), "_u_5" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 40 AND "store_sales"."ss_quantity" >= 21
), "_u_6" AS (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 60 AND "store_sales"."ss_quantity" >= 41
), "_u_7" AS (
  SELECT
    AVG("store_sales"."ss_ext_list_price") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 60 AND "store_sales"."ss_quantity" >= 41
), "_u_8" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 60 AND "store_sales"."ss_quantity" >= 41
), "_u_9" AS (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 80 AND "store_sales"."ss_quantity" >= 61
), "_u_10" AS (
  SELECT
    AVG("store_sales"."ss_ext_list_price") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 80 AND "store_sales"."ss_quantity" >= 61
), "_u_11" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 80 AND "store_sales"."ss_quantity" >= 61
), "_u_12" AS (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 100 AND "store_sales"."ss_quantity" >= 81
), "_u_13" AS (
  SELECT
    AVG("store_sales"."ss_ext_list_price") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 100 AND "store_sales"."ss_quantity" >= 81
), "_u_14" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "_col_0"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_quantity" <= 100 AND "store_sales"."ss_quantity" >= 81
)
SELECT
  CASE WHEN "_u_0"."_col_0" > 3672 THEN "_u_1"."_col_0" ELSE "_u_2"."_col_0" END AS "bucket1",
  CASE WHEN "_u_3"."_col_0" > 3392 THEN "_u_4"."_col_0" ELSE "_u_5"."_col_0" END AS "bucket2",
  CASE WHEN "_u_6"."_col_0" > 32784 THEN "_u_7"."_col_0" ELSE "_u_8"."_col_0" END AS "bucket3",
  CASE WHEN "_u_9"."_col_0" > 26032 THEN "_u_10"."_col_0" ELSE "_u_11"."_col_0" END AS "bucket4",
  CASE WHEN "_u_12"."_col_0" > 23982 THEN "_u_13"."_col_0" ELSE "_u_14"."_col_0" END AS "bucket5"
FROM "reason" AS "reason"
CROSS JOIN "_u_0" AS "_u_0"
CROSS JOIN "_u_1" AS "_u_1"
CROSS JOIN "_u_2" AS "_u_2"
CROSS JOIN "_u_3" AS "_u_3"
CROSS JOIN "_u_4" AS "_u_4"
CROSS JOIN "_u_5" AS "_u_5"
CROSS JOIN "_u_6" AS "_u_6"
CROSS JOIN "_u_7" AS "_u_7"
CROSS JOIN "_u_8" AS "_u_8"
CROSS JOIN "_u_9" AS "_u_9"
CROSS JOIN "_u_10" AS "_u_10"
CROSS JOIN "_u_11" AS "_u_11"
CROSS JOIN "_u_12" AS "_u_12"
CROSS JOIN "_u_13" AS "_u_13"
CROSS JOIN "_u_14" AS "_u_14"
WHERE
  "reason"."r_reason_sk" = 1;

--------------------------------------
-- TPC-DS 10
--------------------------------------
SELECT cd_gender,
               cd_marital_status,
               cd_education_status,
               Count(*) cnt1,
               cd_purchase_estimate,
               Count(*) cnt2,
               cd_credit_rating,
               Count(*) cnt3,
               cd_dep_count,
               Count(*) cnt4,
               cd_dep_employed_count,
               Count(*) cnt5,
               cd_dep_college_count,
               Count(*) cnt6
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_county IN ( 'Lycoming County', 'Sheridan County',
                          'Kandiyohi County',
                          'Pike County',
                                           'Greene County' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2002
                          AND d_moy BETWEEN 4 AND 4 + 3)
       AND ( EXISTS (SELECT *
                     FROM   web_sales,
                            date_dim
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 2002
                            AND d_moy BETWEEN 4 AND 4 + 3)
              OR EXISTS (SELECT *
                         FROM   catalog_sales,
                                date_dim
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 2002
                                AND d_moy BETWEEN 4 AND 4 + 3) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date_id" AS "d_date_id",
    "date_dim"."d_date" AS "d_date",
    "date_dim"."d_month_seq" AS "d_month_seq",
    "date_dim"."d_week_seq" AS "d_week_seq",
    "date_dim"."d_quarter_seq" AS "d_quarter_seq",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_dow" AS "d_dow",
    "date_dim"."d_moy" AS "d_moy",
    "date_dim"."d_dom" AS "d_dom",
    "date_dim"."d_qoy" AS "d_qoy",
    "date_dim"."d_fy_year" AS "d_fy_year",
    "date_dim"."d_fy_quarter_seq" AS "d_fy_quarter_seq",
    "date_dim"."d_fy_week_seq" AS "d_fy_week_seq",
    "date_dim"."d_day_name" AS "d_day_name",
    "date_dim"."d_quarter_name" AS "d_quarter_name",
    "date_dim"."d_holiday" AS "d_holiday",
    "date_dim"."d_weekend" AS "d_weekend",
    "date_dim"."d_following_holiday" AS "d_following_holiday",
    "date_dim"."d_first_dom" AS "d_first_dom",
    "date_dim"."d_last_dom" AS "d_last_dom",
    "date_dim"."d_same_day_ly" AS "d_same_day_ly",
    "date_dim"."d_same_day_lq" AS "d_same_day_lq",
    "date_dim"."d_current_day" AS "d_current_day",
    "date_dim"."d_current_week" AS "d_current_week",
    "date_dim"."d_current_month" AS "d_current_month",
    "date_dim"."d_current_quarter" AS "d_current_quarter",
    "date_dim"."d_current_year" AS "d_current_year"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" <= 7 AND "date_dim"."d_moy" >= 4 AND "date_dim"."d_year" = 2002
), "_u_0" AS (
  SELECT
    "catalog_sales"."cs_ship_customer_sk" AS "_u_1"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_ship_customer_sk"
), "_u_2" AS (
  SELECT
    "web_sales"."ws_bill_customer_sk" AS "_u_3"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "web_sales"."ws_bill_customer_sk"
), "_u_4" AS (
  SELECT
    "store_sales"."ss_customer_sk" AS "_u_5"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "store_sales"."ss_customer_sk"
)
SELECT
  "customer_demographics"."cd_gender" AS "cd_gender",
  "customer_demographics"."cd_marital_status" AS "cd_marital_status",
  "customer_demographics"."cd_education_status" AS "cd_education_status",
  COUNT(*) AS "cnt1",
  "customer_demographics"."cd_purchase_estimate" AS "cd_purchase_estimate",
  COUNT(*) AS "cnt2",
  "customer_demographics"."cd_credit_rating" AS "cd_credit_rating",
  COUNT(*) AS "cnt3",
  "customer_demographics"."cd_dep_count" AS "cd_dep_count",
  COUNT(*) AS "cnt4",
  "customer_demographics"."cd_dep_employed_count" AS "cd_dep_employed_count",
  COUNT(*) AS "cnt5",
  "customer_demographics"."cd_dep_college_count" AS "cd_dep_college_count",
  COUNT(*) AS "cnt6"
FROM "customer" AS "customer"
LEFT JOIN "_u_0" AS "_u_0"
  ON "customer"."c_customer_sk" = "_u_0"."_u_1"
LEFT JOIN "_u_2" AS "_u_2"
  ON "customer"."c_customer_sk" = "_u_2"."_u_3"
LEFT JOIN "_u_4" AS "_u_4"
  ON "customer"."c_customer_sk" = "_u_4"."_u_5"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_county" IN ('Lycoming County', 'Sheridan County', 'Kandiyohi County', 'Pike County', 'Greene County')
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer_demographics"."cd_demo_sk" = "customer"."c_current_cdemo_sk"
WHERE
  (
    NOT "_u_0"."_u_1" IS NULL OR NOT "_u_2"."_u_3" IS NULL
  )
  AND NOT "_u_4"."_u_5" IS NULL
GROUP BY
  "customer_demographics"."cd_gender",
  "customer_demographics"."cd_marital_status",
  "customer_demographics"."cd_education_status",
  "customer_demographics"."cd_purchase_estimate",
  "customer_demographics"."cd_credit_rating",
  "customer_demographics"."cd_dep_count",
  "customer_demographics"."cd_dep_employed_count",
  "customer_demographics"."cd_dep_college_count"
ORDER BY
  "cd_gender",
  "cd_marital_status",
  "cd_education_status",
  "cd_purchase_estimate",
  "cd_credit_rating",
  "cd_dep_count",
  "cd_dep_employed_count",
  "cd_dep_college_count"
LIMIT 100;

--------------------------------------
-- TPC-DS 11
--------------------------------------
WITH year_total
     AS (SELECT c_customer_id                                customer_id,
                c_first_name                                 customer_first_name
                ,
                c_last_name
                customer_last_name,
                c_preferred_cust_flag
                   customer_preferred_cust_flag
                    ,
                c_birth_country
                    customer_birth_country,
                c_login                                      customer_login,
                c_email_address
                customer_email_address,
                d_year                                       dyear,
                Sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
                's'                                          sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year
         UNION ALL
         SELECT c_customer_id                                customer_id,
                c_first_name                                 customer_first_name
                ,
                c_last_name
                customer_last_name,
                c_preferred_cust_flag
                customer_preferred_cust_flag
                ,
                c_birth_country
                customer_birth_country,
                c_login                                      customer_login,
                c_email_address
                customer_email_address,
                d_year                                       dyear,
                Sum(ws_ext_list_price - ws_ext_discount_amt) year_total,
                'w'                                          sale_type
         FROM   customer,
                web_sales,
                date_dim
         WHERE  c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year)
SELECT t_s_secyear.customer_id,
               t_s_secyear.customer_first_name,
               t_s_secyear.customer_last_name,
               t_s_secyear.customer_birth_country
FROM   year_total t_s_firstyear,
       year_total t_s_secyear,
       year_total t_w_firstyear,
       year_total t_w_secyear
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
       AND t_s_firstyear.sale_type = 's'
       AND t_w_firstyear.sale_type = 'w'
       AND t_s_secyear.sale_type = 's'
       AND t_w_secyear.sale_type = 'w'
       AND t_s_firstyear.dyear = 2001
       AND t_s_secyear.dyear = 2001 + 1
       AND t_w_firstyear.dyear = 2001
       AND t_w_secyear.dyear = 2001 + 1
       AND t_s_firstyear.year_total > 0
       AND t_w_firstyear.year_total > 0
       AND CASE
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total /
                                                    t_w_firstyear.year_total
             ELSE 0.0
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN
                   t_s_secyear.year_total /
                   t_s_firstyear.year_total
                   ELSE 0.0
                 END
ORDER  BY t_s_secyear.customer_id,
          t_s_secyear.customer_first_name,
          t_s_secyear.customer_last_name,
          t_s_secyear.customer_birth_country
LIMIT 100;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_customer_id" AS "c_customer_id",
    "customer"."c_first_name" AS "c_first_name",
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_preferred_cust_flag" AS "c_preferred_cust_flag",
    "customer"."c_birth_country" AS "c_birth_country",
    "customer"."c_login" AS "c_login",
    "customer"."c_email_address" AS "c_email_address"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
), "cte" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "customer"."c_birth_country" AS "customer_birth_country",
    "date_dim"."d_year" AS "dyear",
    SUM("store_sales"."ss_ext_list_price" - "store_sales"."ss_ext_discount_amt") AS "year_total",
    's' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "store_sales" AS "store_sales"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
), "cte_2" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "customer"."c_birth_country" AS "customer_birth_country",
    "date_dim"."d_year" AS "dyear",
    SUM("web_sales"."ws_ext_list_price" - "web_sales"."ws_ext_discount_amt") AS "year_total",
    'w' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "web_sales" AS "web_sales"
    ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
), "year_total" AS (
  SELECT
    "cte"."customer_id" AS "customer_id",
    "cte"."customer_first_name" AS "customer_first_name",
    "cte"."customer_last_name" AS "customer_last_name",
    "cte"."customer_birth_country" AS "customer_birth_country",
    "cte"."dyear" AS "dyear",
    "cte"."year_total" AS "year_total",
    "cte"."sale_type" AS "sale_type"
  FROM "cte" AS "cte"
  UNION ALL
  SELECT
    "cte_2"."customer_id" AS "customer_id",
    "cte_2"."customer_first_name" AS "customer_first_name",
    "cte_2"."customer_last_name" AS "customer_last_name",
    "cte_2"."customer_birth_country" AS "customer_birth_country",
    "cte_2"."dyear" AS "dyear",
    "cte_2"."year_total" AS "year_total",
    "cte_2"."sale_type" AS "sale_type"
  FROM "cte_2" AS "cte_2"
)
SELECT
  "t_s_secyear"."customer_id" AS "customer_id",
  "t_s_secyear"."customer_first_name" AS "customer_first_name",
  "t_s_secyear"."customer_last_name" AS "customer_last_name",
  "t_s_secyear"."customer_birth_country" AS "customer_birth_country"
FROM "year_total" AS "t_s_firstyear"
JOIN "year_total" AS "t_s_secyear"
  ON "t_s_secyear"."customer_id" = "t_s_firstyear"."customer_id"
  AND "t_s_secyear"."dyear" = 2002
  AND "t_s_secyear"."sale_type" = 's'
JOIN "year_total" AS "t_w_secyear"
  ON "t_s_firstyear"."customer_id" = "t_w_secyear"."customer_id"
  AND "t_w_secyear"."dyear" = 2002
  AND "t_w_secyear"."sale_type" = 'w'
JOIN "year_total" AS "t_w_firstyear"
  ON "t_s_firstyear"."customer_id" = "t_w_firstyear"."customer_id"
  AND "t_w_firstyear"."dyear" = 2001
  AND "t_w_firstyear"."sale_type" = 'w'
  AND "t_w_firstyear"."year_total" > 0
  AND CASE
    WHEN "t_w_firstyear"."year_total" > 0
    THEN "t_w_secyear"."year_total" / "t_w_firstyear"."year_total"
    ELSE 0.0
  END > CASE
    WHEN "t_s_firstyear"."year_total" > 0
    THEN "t_s_secyear"."year_total" / "t_s_firstyear"."year_total"
    ELSE 0.0
  END
WHERE
  "t_s_firstyear"."dyear" = 2001
  AND "t_s_firstyear"."sale_type" = 's'
  AND "t_s_firstyear"."year_total" > 0
ORDER BY
  "t_s_secyear"."customer_id",
  "t_s_secyear"."customer_first_name",
  "t_s_secyear"."customer_last_name",
  "t_s_secyear"."customer_birth_country"
LIMIT 100;

--------------------------------------
-- TPC-DS 12
--------------------------------------
SELECT
         i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price ,
         Sum(ws_ext_sales_price)                                                              AS itemrevenue ,
         Sum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     web_sales ,
         item ,
         date_dim
WHERE    ws_item_sk = i_item_sk
AND      i_category IN ('Home',
                        'Men',
                        'Women')
AND      ws_sold_date_sk = d_date_sk
AND      d_date BETWEEN Cast('2000-05-11' AS DATE) AND      (
                  Cast('2000-05-11' AS DATE) + INTERVAL '30' day)
GROUP BY i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price
ORDER BY i_category ,
         i_class ,
         i_item_id ,
         i_item_desc ,
         revenueratio
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "item"."i_category" AS "i_category",
  "item"."i_class" AS "i_class",
  "item"."i_current_price" AS "i_current_price",
  SUM("web_sales"."ws_ext_sales_price") AS "itemrevenue",
  SUM("web_sales"."ws_ext_sales_price") * 100 / SUM(SUM("web_sales"."ws_ext_sales_price")) OVER (PARTITION BY "item"."i_class") AS "revenueratio"
FROM "web_sales" AS "web_sales"
JOIN "item" AS "item"
  ON "item"."i_category" IN ('Home', 'Men', 'Women')
  AND "web_sales"."ws_item_sk" = "item"."i_item_sk"
JOIN "date_dim" AS "date_dim"
  ON CAST("date_dim"."d_date" AS DATE) <= CAST('2000-06-10' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('2000-05-11' AS DATE)
  AND "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "item"."i_category",
  "item"."i_class",
  "item"."i_current_price"
ORDER BY
  "i_category",
  "i_class",
  "i_item_id",
  "i_item_desc",
  "revenueratio"
LIMIT 100;

--------------------------------------
-- TPC-DS 13
--------------------------------------
SELECT Avg(ss_quantity),
       Avg(ss_ext_sales_price),
       Avg(ss_ext_wholesale_cost),
       Sum(ss_ext_wholesale_cost)
FROM   store_sales,
       store,
       customer_demographics,
       household_demographics,
       customer_address,
       date_dim
WHERE  s_store_sk = ss_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 2001
       AND ( ( ss_hdemo_sk = hd_demo_sk
               AND cd_demo_sk = ss_cdemo_sk
               AND cd_marital_status = 'U'
               AND cd_education_status = 'Advanced Degree'
               AND ss_sales_price BETWEEN 100.00 AND 150.00
               AND hd_dep_count = 3 )
              OR ( ss_hdemo_sk = hd_demo_sk
                   AND cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'M'
                   AND cd_education_status = 'Primary'
                   AND ss_sales_price BETWEEN 50.00 AND 100.00
                   AND hd_dep_count = 1 )
              OR ( ss_hdemo_sk = hd_demo_sk
                   AND cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'D'
                   AND cd_education_status = 'Secondary'
                   AND ss_sales_price BETWEEN 150.00 AND 200.00
                   AND hd_dep_count = 1 ) )
       AND ( ( ss_addr_sk = ca_address_sk
               AND ca_country = 'United States'
               AND ca_state IN ( 'AZ', 'NE', 'IA' )
               AND ss_net_profit BETWEEN 100 AND 200 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'MS', 'CA', 'NV' )
                   AND ss_net_profit BETWEEN 150 AND 300 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'GA', 'TX', 'NJ' )
                   AND ss_net_profit BETWEEN 50 AND 250 ) );
SELECT
  AVG("store_sales"."ss_quantity") AS "_col_0",
  AVG("store_sales"."ss_ext_sales_price") AS "_col_1",
  AVG("store_sales"."ss_ext_wholesale_cost") AS "_col_2",
  SUM("store_sales"."ss_ext_wholesale_cost") AS "_col_3"
FROM "store_sales" AS "store_sales"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
CROSS JOIN "household_demographics" AS "household_demographics"
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
  AND "customer_demographics"."cd_education_status" = 'Advanced Degree'
  AND "customer_demographics"."cd_education_status" = 'Primary'
  AND "customer_demographics"."cd_education_status" = 'Secondary'
  AND "customer_demographics"."cd_marital_status" = 'D'
  AND "customer_demographics"."cd_marital_status" = 'M'
  AND "customer_demographics"."cd_marital_status" = 'U'
  AND "household_demographics"."hd_dep_count" = 1
  AND "household_demographics"."hd_dep_count" = 3
  AND "store_sales"."ss_hdemo_sk" = "household_demographics"."hd_demo_sk"
  AND "store_sales"."ss_sales_price" <= 100.00
  AND "store_sales"."ss_sales_price" >= 150.00
JOIN "customer_address" AS "customer_address"
  ON (
    "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('AZ', 'NE', 'IA')
    AND "store_sales"."ss_addr_sk" = "customer_address"."ca_address_sk"
    AND "store_sales"."ss_net_profit" <= 200
    AND "store_sales"."ss_net_profit" >= 100
  )
  OR (
    "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('GA', 'TX', 'NJ')
    AND "store_sales"."ss_addr_sk" = "customer_address"."ca_address_sk"
    AND "store_sales"."ss_net_profit" <= 250
    AND "store_sales"."ss_net_profit" >= 50
  )
  OR (
    "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('MS', 'CA', 'NV')
    AND "store_sales"."ss_addr_sk" = "customer_address"."ca_address_sk"
    AND "store_sales"."ss_net_profit" <= 300
    AND "store_sales"."ss_net_profit" >= 150
  )
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_year" = 2001
  AND "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk";

--------------------------------------
-- TPC-DS 14
--------------------------------------
WITH cross_items
     AS (SELECT i_item_sk ss_item_sk
         FROM   item,
                (SELECT iss.i_brand_id    brand_id,
                        iss.i_class_id    class_id,
                        iss.i_category_id category_id
                 FROM   store_sales,
                        item iss,
                        date_dim d1
                 WHERE  ss_item_sk = iss.i_item_sk
                        AND ss_sold_date_sk = d1.d_date_sk
                        AND d1.d_year BETWEEN 1999 AND 1999 + 2
                 INTERSECT
                 SELECT ics.i_brand_id,
                        ics.i_class_id,
                        ics.i_category_id
                 FROM   catalog_sales,
                        item ics,
                        date_dim d2
                 WHERE  cs_item_sk = ics.i_item_sk
                        AND cs_sold_date_sk = d2.d_date_sk
                        AND d2.d_year BETWEEN 1999 AND 1999 + 2
                 INTERSECT
                 SELECT iws.i_brand_id,
                        iws.i_class_id,
                        iws.i_category_id
                 FROM   web_sales,
                        item iws,
                        date_dim d3
                 WHERE  ws_item_sk = iws.i_item_sk
                        AND ws_sold_date_sk = d3.d_date_sk
                        AND d3.d_year BETWEEN 1999 AND 1999 + 2)
         WHERE  i_brand_id = brand_id
                AND i_class_id = class_id
                AND i_category_id = category_id),
     avg_sales
     AS (SELECT Avg(quantity * list_price) average_sales
         FROM   (SELECT ss_quantity   quantity,
                        ss_list_price list_price
                 FROM   store_sales,
                        date_dim
                 WHERE  ss_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT cs_quantity   quantity,
                        cs_list_price list_price
                 FROM   catalog_sales,
                        date_dim
                 WHERE  cs_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT ws_quantity   quantity,
                        ws_list_price list_price
                 FROM   web_sales,
                        date_dim
                 WHERE  ws_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2) x)
SELECT channel,
               i_brand_id,
               i_class_id,
               i_category_id,
               Sum(sales),
               Sum(number_sales)
FROM  (SELECT 'store'                          channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(ss_quantity * ss_list_price) sales,
              Count(*)                         number_sales
       FROM   store_sales,
              item,
              date_dim
       WHERE  ss_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ss_item_sk = i_item_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(ss_quantity * ss_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'catalog'                        channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(cs_quantity * cs_list_price) sales,
              Count(*)                         number_sales
       FROM   catalog_sales,
              item,
              date_dim
       WHERE  cs_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND cs_item_sk = i_item_sk
              AND cs_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(cs_quantity * cs_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'web'                            channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(ws_quantity * ws_list_price) sales,
              Count(*)                         number_sales
       FROM   web_sales,
              item,
              date_dim
       WHERE  ws_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ws_item_sk = i_item_sk
              AND ws_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(ws_quantity * ws_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)) y
GROUP  BY rollup ( channel, i_brand_id, i_class_id, i_category_id )
ORDER  BY channel,
          i_brand_id,
          i_class_id,
          i_category_id
LIMIT 100;
WITH "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id"
  FROM "item" AS "item"
), "d1" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_year" <= 2001 AND "date_dim"."d_year" >= 1999
), "cte_4" AS (
  SELECT
    "ics"."i_brand_id" AS "i_brand_id",
    "ics"."i_class_id" AS "i_class_id",
    "ics"."i_category_id" AS "i_category_id"
  FROM "catalog_sales" AS "catalog_sales"
  CROSS JOIN "item_2" AS "ics"
  CROSS JOIN "d1" AS "d2"
  WHERE
    "catalog_sales"."cs_item_sk" = "ics"."i_item_sk"
    AND "catalog_sales"."cs_sold_date_sk" = "d2"."d_date_sk"
  INTERSECT
  SELECT
    "iws"."i_brand_id" AS "i_brand_id",
    "iws"."i_class_id" AS "i_class_id",
    "iws"."i_category_id" AS "i_category_id"
  FROM "web_sales" AS "web_sales"
  CROSS JOIN "item_2" AS "iws"
  CROSS JOIN "d1" AS "d3"
  WHERE
    "web_sales"."ws_item_sk" = "iws"."i_item_sk"
    AND "web_sales"."ws_sold_date_sk" = "d3"."d_date_sk"
), "_q_0" AS (
  SELECT
    "iss"."i_brand_id" AS "brand_id",
    "iss"."i_class_id" AS "class_id",
    "iss"."i_category_id" AS "category_id"
  FROM "store_sales" AS "store_sales"
  CROSS JOIN "item_2" AS "iss"
  CROSS JOIN "d1" AS "d1"
  WHERE
    "store_sales"."ss_item_sk" = "iss"."i_item_sk"
    AND "store_sales"."ss_sold_date_sk" = "d1"."d_date_sk"
  INTERSECT
  SELECT
    "cte_4"."i_brand_id" AS "i_brand_id",
    "cte_4"."i_class_id" AS "i_class_id",
    "cte_4"."i_category_id" AS "i_category_id"
  FROM "cte_4" AS "cte_4"
), "cte_8" AS (
  SELECT
    1 AS "_"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "d1" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  UNION ALL
  SELECT
    1 AS "_"
  FROM "web_sales" AS "web_sales"
  JOIN "d1" AS "date_dim"
    ON "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
), "x" AS (
  SELECT
    1 AS "_"
  FROM "store_sales" AS "store_sales"
  JOIN "d1" AS "date_dim"
    ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
  UNION ALL
  SELECT
    "cte_8"."_" AS "_"
  FROM "cte_8" AS "cte_8"
), "_u_0" AS (
  SELECT
    "item"."i_item_sk" AS "ss_item_sk"
  FROM "item_2" AS "item"
  JOIN "_q_0" AS "_q_0"
    ON "item"."i_brand_id" = "_q_0"."brand_id"
    AND "item"."i_category_id" = "_q_0"."category_id"
    AND "item"."i_class_id" = "_q_0"."class_id"
  GROUP BY
    "item"."i_item_sk"
), "_u_1" AS (
  SELECT
    "average_sales" AS "average_sales"
  FROM "x" AS "x"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 11 AND "date_dim"."d_year" = 2001
), "cte_9" AS (
  SELECT
    'store' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("store_sales"."ss_quantity" * "store_sales"."ss_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "store_sales" AS "store_sales"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "store_sales"."ss_item_sk" = "_u_0"."ss_item_sk"
  CROSS JOIN "_u_1" AS "_u_1"
  JOIN "item_2" AS "item"
    ON "store_sales"."ss_item_sk" = "item"."i_item_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    NOT "_u_0"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    SUM("store_sales"."ss_quantity" * "store_sales"."ss_list_price") > MAX("_u_1"."average_sales")
), "cte_10" AS (
  SELECT
    'catalog' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("catalog_sales"."cs_quantity" * "catalog_sales"."cs_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "catalog_sales" AS "catalog_sales"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "catalog_sales"."cs_item_sk" = "_u_2"."ss_item_sk"
  CROSS JOIN "_u_1" AS "_u_3"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    NOT "_u_2"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    SUM("catalog_sales"."cs_quantity" * "catalog_sales"."cs_list_price") > MAX("_u_3"."average_sales")
), "cte_11" AS (
  SELECT
    'web' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("web_sales"."ws_quantity" * "web_sales"."ws_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "web_sales" AS "web_sales"
  LEFT JOIN "_u_0" AS "_u_4"
    ON "web_sales"."ws_item_sk" = "_u_4"."ss_item_sk"
  CROSS JOIN "_u_1" AS "_u_5"
  JOIN "item_2" AS "item"
    ON "web_sales"."ws_item_sk" = "item"."i_item_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    NOT "_u_4"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    SUM("web_sales"."ws_quantity" * "web_sales"."ws_list_price") > MAX("_u_5"."average_sales")
), "cte_12" AS (
  SELECT
    "cte_10"."channel" AS "channel",
    "cte_10"."i_brand_id" AS "i_brand_id",
    "cte_10"."i_class_id" AS "i_class_id",
    "cte_10"."i_category_id" AS "i_category_id",
    "cte_10"."sales" AS "sales",
    "cte_10"."number_sales" AS "number_sales"
  FROM "cte_10" AS "cte_10"
  UNION ALL
  SELECT
    "cte_11"."channel" AS "channel",
    "cte_11"."i_brand_id" AS "i_brand_id",
    "cte_11"."i_class_id" AS "i_class_id",
    "cte_11"."i_category_id" AS "i_category_id",
    "cte_11"."sales" AS "sales",
    "cte_11"."number_sales" AS "number_sales"
  FROM "cte_11" AS "cte_11"
), "y" AS (
  SELECT
    "cte_9"."channel" AS "channel",
    "cte_9"."i_brand_id" AS "i_brand_id",
    "cte_9"."i_class_id" AS "i_class_id",
    "cte_9"."i_category_id" AS "i_category_id",
    "cte_9"."sales" AS "sales",
    "cte_9"."number_sales" AS "number_sales"
  FROM "cte_9" AS "cte_9"
  UNION ALL
  SELECT
    "cte_12"."channel" AS "channel",
    "cte_12"."i_brand_id" AS "i_brand_id",
    "cte_12"."i_class_id" AS "i_class_id",
    "cte_12"."i_category_id" AS "i_category_id",
    "cte_12"."sales" AS "sales",
    "cte_12"."number_sales" AS "number_sales"
  FROM "cte_12" AS "cte_12"
)
SELECT
  "y"."channel" AS "channel",
  "y"."i_brand_id" AS "i_brand_id",
  "y"."i_class_id" AS "i_class_id",
  "y"."i_category_id" AS "i_category_id",
  SUM("y"."sales") AS "_col_4",
  SUM("y"."number_sales") AS "_col_5"
FROM "y" AS "y"
GROUP BY
ROLLUP (
  "y"."channel",
  "y"."i_brand_id",
  "y"."i_class_id",
  "y"."i_category_id"
)
ORDER BY
  "channel",
  "i_brand_id",
  "i_class_id",
  "i_category_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 15
--------------------------------------
SELECT ca_zip,
               Sum(cs_sales_price)
FROM   catalog_sales,
       customer,
       customer_address,
       date_dim
WHERE  cs_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
              OR ca_state IN ( 'CA', 'WA', 'GA' )
              OR cs_sales_price > 500 )
       AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
GROUP  BY ca_zip
ORDER  BY ca_zip
LIMIT 100;
SELECT
  "customer_address"."ca_zip" AS "ca_zip",
  SUM("catalog_sales"."cs_sales_price") AS "_col_1"
FROM "catalog_sales" AS "catalog_sales"
JOIN "customer_address" AS "customer_address"
  ON "catalog_sales"."cs_sales_price" > 500
  OR "customer_address"."ca_state" IN ('CA', 'WA', 'GA')
  OR SUBSTR("customer_address"."ca_zip", 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
JOIN "customer" AS "customer"
  ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
  AND "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_qoy" = 1
  AND "date_dim"."d_year" = 1998
GROUP BY
  "customer_address"."ca_zip"
ORDER BY
  "ca_zip"
LIMIT 100;

--------------------------------------
-- TPC-DS 16
--------------------------------------
SELECT
         Count(DISTINCT cs_order_number) AS "order count" ,
         Sum(cs_ext_ship_cost)           AS "total shipping cost" ,
         Sum(cs_net_profit)              AS "total net profit"
FROM     catalog_sales cs1 ,
         date_dim ,
         customer_address ,
         call_center
WHERE    d_date BETWEEN '2002-3-01' AND      (
                  Cast('2002-3-01' AS DATE) + INTERVAL '60' day)
AND      cs1.cs_ship_date_sk = d_date_sk
AND      cs1.cs_ship_addr_sk = ca_address_sk
AND      ca_state = 'IA'
AND      cs1.cs_call_center_sk = cc_call_center_sk
AND      cc_county IN ('Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County' )
AND      EXISTS
         (
                SELECT *
                FROM   catalog_sales cs2
                WHERE  cs1.cs_order_number = cs2.cs_order_number
                AND    cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   catalog_returns cr1
                WHERE  cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;
WITH "_u_0" AS (
  SELECT
    "cs2"."cs_order_number" AS "_u_1",
    ARRAY_AGG("cs2"."cs_warehouse_sk") AS "_u_2"
  FROM "catalog_sales" AS "cs2"
  GROUP BY
    "cs2"."cs_order_number"
), "_u_3" AS (
  SELECT
    "cr1"."cr_order_number" AS "_u_4"
  FROM "catalog_returns" AS "cr1"
  GROUP BY
    "cr1"."cr_order_number"
)
SELECT
  COUNT(DISTINCT "catalog_sales"."cs_order_number") AS "order count",
  SUM("catalog_sales"."cs_ext_ship_cost") AS "total shipping cost",
  SUM("catalog_sales"."cs_net_profit") AS "total net profit"
FROM "catalog_sales" AS "catalog_sales"
LEFT JOIN "_u_0" AS "_u_0"
  ON "catalog_sales"."cs_order_number" = "_u_0"."_u_1"
LEFT JOIN "_u_3" AS "_u_3"
  ON "catalog_sales"."cs_order_number" = "_u_3"."_u_4"
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_ship_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_date" <= (
    CAST('2002-3-01' AS DATE) + CAST(INTERVAL '60' "day" AS DATE)
  )
  AND "date_dim"."d_date" >= '2002-3-01'
JOIN "customer_address" AS "customer_address"
  ON "catalog_sales"."cs_ship_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" = 'IA'
JOIN "call_center" AS "call_center"
  ON "call_center"."cc_county" IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
  AND "catalog_sales"."cs_call_center_sk" = "call_center"."cc_call_center_sk"
WHERE
  "_u_3"."_u_4" IS NULL
  AND ARRAY_ANY("_u_0"."_u_2", "_x" -> "catalog_sales"."cs_warehouse_sk" <> "_x")
  AND NOT "_u_0"."_u_1" IS NULL
ORDER BY
  COUNT(DISTINCT "catalog_sales"."cs_order_number")
LIMIT 100;

--------------------------------------
-- TPC-DS 17
--------------------------------------
SELECT i_item_id,
               i_item_desc,
               s_state,
               Count(ss_quantity)                                        AS
               store_sales_quantitycount,
               Avg(ss_quantity)                                          AS
               store_sales_quantityave,
               Stddev_samp(ss_quantity)                                  AS
               store_sales_quantitystdev,
               Stddev_samp(ss_quantity) / Avg(ss_quantity)               AS
               store_sales_quantitycov,
               Count(sr_return_quantity)                                 AS
               store_returns_quantitycount,
               Avg(sr_return_quantity)                                   AS
               store_returns_quantityave,
               Stddev_samp(sr_return_quantity)                           AS
               store_returns_quantitystdev,
               Stddev_samp(sr_return_quantity) / Avg(sr_return_quantity) AS
               store_returns_quantitycov,
               Count(cs_quantity)                                        AS
               catalog_sales_quantitycount,
               Avg(cs_quantity)                                          AS
               catalog_sales_quantityave,
               Stddev_samp(cs_quantity) / Avg(cs_quantity)               AS
               catalog_sales_quantitystdev,
               Stddev_samp(cs_quantity) / Avg(cs_quantity)               AS
               catalog_sales_quantitycov
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_quarter_name = '1999Q1'
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' )
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' )
GROUP  BY i_item_id,
          i_item_desc,
          s_state
ORDER  BY i_item_id,
          i_item_desc,
          s_state
LIMIT 100;
WITH "d3" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_quarter_name" AS "d_quarter_name"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_quarter_name" IN ('1999Q1', '1999Q2', '1999Q3')
)
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "store"."s_state" AS "s_state",
  COUNT("store_sales"."ss_quantity") AS "store_sales_quantitycount",
  AVG("store_sales"."ss_quantity") AS "store_sales_quantityave",
  STDDEV_SAMP("store_sales"."ss_quantity") AS "store_sales_quantitystdev",
  STDDEV_SAMP("store_sales"."ss_quantity") / AVG("store_sales"."ss_quantity") AS "store_sales_quantitycov",
  COUNT("store_returns"."sr_return_quantity") AS "store_returns_quantitycount",
  AVG("store_returns"."sr_return_quantity") AS "store_returns_quantityave",
  STDDEV_SAMP("store_returns"."sr_return_quantity") AS "store_returns_quantitystdev",
  STDDEV_SAMP("store_returns"."sr_return_quantity") / AVG("store_returns"."sr_return_quantity") AS "store_returns_quantitycov",
  COUNT("catalog_sales"."cs_quantity") AS "catalog_sales_quantitycount",
  AVG("catalog_sales"."cs_quantity") AS "catalog_sales_quantityave",
  STDDEV_SAMP("catalog_sales"."cs_quantity") / AVG("catalog_sales"."cs_quantity") AS "catalog_sales_quantitystdev",
  STDDEV_SAMP("catalog_sales"."cs_quantity") / AVG("catalog_sales"."cs_quantity") AS "catalog_sales_quantitycov"
FROM "store_sales" AS "store_sales"
CROSS JOIN "d3" AS "d3"
JOIN "catalog_sales" AS "catalog_sales"
  ON "catalog_sales"."cs_sold_date_sk" = "d3"."d_date_sk"
JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_customer_sk" = "catalog_sales"."cs_bill_customer_sk"
  AND "store_returns"."sr_item_sk" = "catalog_sales"."cs_item_sk"
  AND "store_sales"."ss_customer_sk" = "store_returns"."sr_customer_sk"
  AND "store_sales"."ss_item_sk" = "store_returns"."sr_item_sk"
  AND "store_sales"."ss_ticket_number" = "store_returns"."sr_ticket_number"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_quarter_name" = '1999Q1'
JOIN "d3" AS "d2"
  ON "store_returns"."sr_returned_date_sk" = "d2"."d_date_sk"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "store"."s_state"
ORDER BY
  "i_item_id",
  "i_item_desc",
  "s_state"
LIMIT 100;

--------------------------------------
-- TPC-DS 18
--------------------------------------
SELECT i_item_id,
               ca_country,
               ca_state,
               ca_county,
               Avg(Cast(cs_quantity AS NUMERIC(12, 2)))      agg1,
               Avg(Cast(cs_list_price AS NUMERIC(12, 2)))    agg2,
               Avg(Cast(cs_coupon_amt AS NUMERIC(12, 2)))    agg3,
               Avg(Cast(cs_sales_price AS NUMERIC(12, 2)))   agg4,
               Avg(Cast(cs_net_profit AS NUMERIC(12, 2)))    agg5,
               Avg(Cast(c_birth_year AS NUMERIC(12, 2)))     agg6,
               Avg(Cast(cd1.cd_dep_count AS NUMERIC(12, 2))) agg7
FROM   catalog_sales,
       customer_demographics cd1,
       customer_demographics cd2,
       customer,
       customer_address,
       date_dim,
       item
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd1.cd_demo_sk
       AND cs_bill_customer_sk = c_customer_sk
       AND cd1.cd_gender = 'F'
       AND cd1.cd_education_status = 'Secondary'
       AND c_current_cdemo_sk = cd2.cd_demo_sk
       AND c_current_addr_sk = ca_address_sk
       AND c_birth_month IN ( 8, 4, 2, 5,
                              11, 9 )
       AND d_year = 2001
       AND ca_state IN ( 'KS', 'IA', 'AL', 'UT',
                         'VA', 'NC', 'TX' )
GROUP  BY rollup ( i_item_id, ca_country, ca_state, ca_county )
ORDER  BY ca_country,
          ca_state,
          ca_county,
          i_item_id
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "customer_address"."ca_country" AS "ca_country",
  "customer_address"."ca_state" AS "ca_state",
  "customer_address"."ca_county" AS "ca_county",
  AVG(CAST("catalog_sales"."cs_quantity" AS DECIMAL(12, 2))) AS "agg1",
  AVG(CAST("catalog_sales"."cs_list_price" AS DECIMAL(12, 2))) AS "agg2",
  AVG(CAST("catalog_sales"."cs_coupon_amt" AS DECIMAL(12, 2))) AS "agg3",
  AVG(CAST("catalog_sales"."cs_sales_price" AS DECIMAL(12, 2))) AS "agg4",
  AVG(CAST("catalog_sales"."cs_net_profit" AS DECIMAL(12, 2))) AS "agg5",
  AVG(CAST("customer"."c_birth_year" AS DECIMAL(12, 2))) AS "agg6",
  AVG(CAST("customer_demographics"."cd_dep_count" AS DECIMAL(12, 2))) AS "agg7"
FROM "catalog_sales" AS "catalog_sales"
JOIN "customer_demographics" AS "customer_demographics"
  ON "catalog_sales"."cs_bill_cdemo_sk" = "customer_demographics"."cd_demo_sk"
  AND "customer_demographics"."cd_education_status" = 'Secondary'
  AND "customer_demographics"."cd_gender" = 'F'
JOIN "customer" AS "customer"
  ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
  AND "customer"."c_birth_month" IN (8, 4, 2, 5, 11, 9)
JOIN "customer_demographics" AS "customer_demographics_2"
  ON "customer"."c_current_cdemo_sk" = "customer_demographics_2"."cd_demo_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" IN ('KS', 'IA', 'AL', 'UT', 'VA', 'NC', 'TX')
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_year" = 2001
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
GROUP BY
ROLLUP (
  "item"."i_item_id",
  "customer_address"."ca_country",
  "customer_address"."ca_state",
  "customer_address"."ca_county"
)
ORDER BY
  "ca_country",
  "ca_state",
  "ca_county",
  "i_item_id"
LIMIT 100;
