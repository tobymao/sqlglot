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
    ON "date_dim"."d_date_sk" = "store_returns"."sr_returned_date_sk"
    AND "date_dim"."d_year" = 2001
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
  ON "_u_0"."_u_1" = "ctr1"."ctr_store_sk"
JOIN "customer" AS "customer"
  ON "ctr1"."ctr_customer_sk" = "customer"."c_customer_sk"
JOIN "store" AS "store"
  ON "ctr1"."ctr_store_sk" = "store"."s_store_sk" AND "store"."s_state" = 'TN'
WHERE
  "_u_0"."_col_0" < "ctr1"."ctr_total_return"
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
      CASE WHEN "date_dim"."d_day_name" = 'Sunday' THEN "wscs"."sales_price" ELSE NULL END
    ) AS "sun_sales",
    SUM(
      CASE WHEN "date_dim"."d_day_name" = 'Monday' THEN "wscs"."sales_price" ELSE NULL END
    ) AS "mon_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Tuesday'
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "tue_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Wednesday'
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "wed_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Thursday'
        THEN "wscs"."sales_price"
        ELSE NULL
      END
    ) AS "thu_sales",
    SUM(
      CASE WHEN "date_dim"."d_day_name" = 'Friday' THEN "wscs"."sales_price" ELSE NULL END
    ) AS "fri_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Saturday'
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
JOIN "date_dim" AS "date_dim_2"
  ON "date_dim_2"."d_week_seq" = "wswscs_2"."d_week_seq" AND "date_dim_2"."d_year" = 1999
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
  "dt"."d_year" AS "d_year",
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  SUM("store_sales"."ss_ext_discount_amt") AS "sum_agg"
FROM "date_dim" AS "dt"
JOIN "store_sales" AS "store_sales"
  ON "dt"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk" AND "item"."i_manufact_id" = 427
WHERE
  "dt"."d_moy" = 11
GROUP BY
  "dt"."d_year",
  "item"."i_brand",
  "item"."i_brand_id"
ORDER BY
  "d_year",
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
), "year_total" AS (
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
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
  UNION ALL
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
    ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
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
  UNION ALL
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
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
)
SELECT
  "t_s_secyear"."customer_id" AS "customer_id",
  "t_s_secyear"."customer_first_name" AS "customer_first_name",
  "t_s_secyear"."customer_last_name" AS "customer_last_name",
  "t_s_secyear"."customer_preferred_cust_flag" AS "customer_preferred_cust_flag"
FROM "year_total" AS "t_s_firstyear"
JOIN "year_total" AS "t_c_secyear"
  ON "t_c_secyear"."customer_id" = "t_s_firstyear"."customer_id"
  AND "t_c_secyear"."dyear" = 2002
  AND "t_c_secyear"."sale_type" = 'c'
JOIN "year_total" AS "t_s_secyear"
  ON "t_s_firstyear"."customer_id" = "t_s_secyear"."customer_id"
  AND "t_s_secyear"."dyear" = 2002
  AND "t_s_secyear"."sale_type" = 's'
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
  ON "t_c_firstyear"."customer_id" = "t_s_firstyear"."customer_id"
  AND "t_c_firstyear"."dyear" = 2001
  AND "t_c_firstyear"."sale_type" = 'c'
  AND "t_c_firstyear"."year_total" > 0
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
    ON "date_dim"."d_date_sk" = "salesreturns"."date_sk"
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
  JOIN "catalog_page" AS "catalog_page"
    ON "catalog_page"."cp_catalog_page_sk" = "salesreturns"."page_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "salesreturns"."date_sk"
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
    ON "date_dim"."d_date_sk" = "salesreturns"."date_sk"
  JOIN "web_site" AS "web_site"
    ON "salesreturns"."wsr_web_site_sk" = "web_site"."web_site_sk"
  GROUP BY
    "web_site"."web_site_id"
), "x" AS (
  SELECT
    'store channel' AS "channel",
    CONCAT('store', "ssr"."s_store_id") AS "id",
    "ssr"."sales" AS "sales",
    "ssr"."returns1" AS "returns1",
    "ssr"."profit" - "ssr"."profit_loss" AS "profit"
  FROM "ssr"
  UNION ALL
  SELECT
    'catalog channel' AS "channel",
    CONCAT('catalog_page', "csr"."cp_catalog_page_id") AS "id",
    "csr"."sales" AS "sales",
    "csr"."returns1" AS "returns1",
    "csr"."profit" - "csr"."profit_loss" AS "profit"
  FROM "csr"
  UNION ALL
  SELECT
    'web channel' AS "channel",
    CONCAT('web_site', "wsr"."web_site_id") AS "id",
    "wsr"."sales" AS "sales",
    "wsr"."returns1" AS "returns1",
    "wsr"."profit" - "wsr"."profit_loss" AS "profit"
  FROM "wsr"
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
    "date_dim"."d_month_seq" AS "d_month_seq"
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
  "a"."ca_state" AS "state",
  COUNT(*) AS "cnt"
FROM "customer_address" AS "a"
JOIN "customer" AS "c"
  ON "a"."ca_address_sk" = "c"."c_current_addr_sk"
JOIN "store_sales" AS "s"
  ON "c"."c_customer_sk" = "s"."ss_customer_sk"
JOIN "date_dim" AS "d"
  ON "d"."d_date_sk" = "s"."ss_sold_date_sk"
JOIN "item" AS "i"
  ON "i"."i_item_sk" = "s"."ss_item_sk"
JOIN "_u_0" AS "_u_0"
  ON "_u_0"."d_month_seq" = "d"."d_month_seq"
LEFT JOIN "_u_1" AS "_u_1"
  ON "_u_1"."_u_2" = "i"."i_category"
WHERE
  "i"."i_current_price" > 1.2 * "_u_1"."_col_0"
GROUP BY
  "a"."ca_state"
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
  ON "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
  AND "customer_demographics"."cd_education_status" = '2 yr Degree'
  AND "customer_demographics"."cd_gender" = 'F'
  AND "customer_demographics"."cd_marital_status" = 'W'
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_year" = 1998
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "promotion" AS "promotion"
  ON (
    "promotion"."p_channel_email" = 'N' OR "promotion"."p_channel_event" = 'N'
  )
  AND "promotion"."p_promo_sk" = "store_sales"."ss_promo_sk"
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
  FROM "customer_address" AS "customer_address"
  JOIN "customer" AS "customer"
    ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
    AND "customer"."c_preferred_cust_flag" = 'Y'
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
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_qoy" = 2
  AND "date_dim"."d_year" = 2000
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "a2" AS "a2"
  ON SUBSTR("a2"."ca_zip", 1, 2) = SUBSTR("store"."s_zip", 1, 2)
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
)
SELECT
  CASE
    WHEN MAX("_u_0"."_col_0") > 3672
    THEN MAX("_u_1"."_col_0")
    ELSE MAX("_u_2"."_col_0")
  END AS "bucket1",
  CASE
    WHEN MAX("_u_3"."_col_0") > 3392
    THEN MAX("_u_4"."_col_0")
    ELSE MAX("_u_5"."_col_0")
  END AS "bucket2",
  CASE
    WHEN MAX("_u_6"."_col_0") > 32784
    THEN MAX("_u_7"."_col_0")
    ELSE MAX("_u_8"."_col_0")
  END AS "bucket3",
  CASE
    WHEN MAX("_u_9"."_col_0") > 26032
    THEN MAX("_u_10"."_col_0")
    ELSE MAX("_u_11"."_col_0")
  END AS "bucket4",
  CASE
    WHEN MAX("_u_12"."_col_0") > 23982
    THEN MAX("_u_13"."_col_0")
    ELSE MAX("_u_14"."_col_0")
  END AS "bucket5"
FROM "reason" AS "reason"
CROSS JOIN "_u_0" AS "_u_0"
CROSS JOIN "_u_1" AS "_u_1"
CROSS JOIN "_u_10" AS "_u_10"
CROSS JOIN "_u_11" AS "_u_11"
CROSS JOIN "_u_12" AS "_u_12"
CROSS JOIN "_u_13" AS "_u_13"
CROSS JOIN "_u_14" AS "_u_14"
CROSS JOIN "_u_2" AS "_u_2"
CROSS JOIN "_u_3" AS "_u_3"
CROSS JOIN "_u_4" AS "_u_4"
CROSS JOIN "_u_5" AS "_u_5"
CROSS JOIN "_u_6" AS "_u_6"
CROSS JOIN "_u_7" AS "_u_7"
CROSS JOIN "_u_8" AS "_u_8"
CROSS JOIN "_u_9" AS "_u_9"
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
    "store_sales"."ss_customer_sk" AS "_u_1"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_customer_sk"
), "_u_2" AS (
  SELECT
    "web_sales"."ws_bill_customer_sk" AS "_u_3"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "web_sales"."ws_bill_customer_sk"
), "_u_4" AS (
  SELECT
    "catalog_sales"."cs_ship_customer_sk" AS "_u_5"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_ship_customer_sk"
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
FROM "customer" AS "c"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "c"."c_customer_sk"
LEFT JOIN "_u_2" AS "_u_2"
  ON "_u_2"."_u_3" = "c"."c_customer_sk"
LEFT JOIN "_u_4" AS "_u_4"
  ON "_u_4"."_u_5" = "c"."c_customer_sk"
JOIN "customer_address" AS "ca"
  ON "c"."c_current_addr_sk" = "ca"."ca_address_sk"
  AND "ca"."ca_county" IN ('Lycoming County', 'Sheridan County', 'Kandiyohi County', 'Pike County', 'Greene County')
JOIN "customer_demographics" AS "customer_demographics"
  ON "c"."c_current_cdemo_sk" = "customer_demographics"."cd_demo_sk"
WHERE
  NOT "_u_0"."_u_1" IS NULL
  AND (
    NOT "_u_2"."_u_3" IS NULL OR NOT "_u_4"."_u_5" IS NULL
  )
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
), "year_total" AS (
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
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
  UNION ALL
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
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "customer"."c_preferred_cust_flag",
    "customer"."c_birth_country",
    "customer"."c_login",
    "customer"."c_email_address",
    "date_dim"."d_year"
)
SELECT
  "t_s_secyear"."customer_id" AS "customer_id",
  "t_s_secyear"."customer_first_name" AS "customer_first_name",
  "t_s_secyear"."customer_last_name" AS "customer_last_name",
  "t_s_secyear"."customer_birth_country" AS "customer_birth_country"
FROM "year_total" AS "t_s_firstyear"
JOIN "year_total" AS "t_w_firstyear"
  ON "t_s_firstyear"."customer_id" = "t_w_firstyear"."customer_id"
  AND "t_w_firstyear"."dyear" = 2001
  AND "t_w_firstyear"."sale_type" = 'w'
  AND "t_w_firstyear"."year_total" > 0
JOIN "year_total" AS "t_w_secyear"
  ON "t_s_firstyear"."customer_id" = "t_w_secyear"."customer_id"
  AND "t_w_secyear"."dyear" = 2002
  AND "t_w_secyear"."sale_type" = 'w'
JOIN "year_total" AS "t_s_secyear"
  ON "t_s_firstyear"."customer_id" = "t_s_secyear"."customer_id"
  AND "t_s_secyear"."dyear" = 2002
  AND "t_s_secyear"."sale_type" = 's'
  AND CASE
    WHEN "t_s_firstyear"."year_total" > 0
    THEN "t_s_secyear"."year_total" / "t_s_firstyear"."year_total"
    ELSE 0.0
  END < CASE
    WHEN "t_w_firstyear"."year_total" > 0
    THEN "t_w_secyear"."year_total" / "t_w_firstyear"."year_total"
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
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('2000-06-10' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('2000-05-11' AS DATE)
JOIN "item" AS "item"
  ON "item"."i_category" IN ('Home', 'Men', 'Women')
  AND "item"."i_item_sk" = "web_sales"."ws_item_sk"
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
CROSS JOIN "customer_demographics" AS "customer_demographics"
JOIN "customer_address" AS "customer_address"
  ON (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('AZ', 'NE', 'IA')
    AND "store_sales"."ss_net_profit" <= 200
    AND "store_sales"."ss_net_profit" >= 100
  )
  OR (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('GA', 'TX', 'NJ')
    AND "store_sales"."ss_net_profit" <= 250
    AND "store_sales"."ss_net_profit" >= 50
  )
  OR (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('MS', 'CA', 'NV')
    AND "store_sales"."ss_net_profit" <= 300
    AND "store_sales"."ss_net_profit" >= 150
  )
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_year" = 2001
JOIN "household_demographics" AS "household_demographics"
  ON (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = 'Advanced Degree'
    AND "customer_demographics"."cd_marital_status" = 'U'
    AND "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND "household_demographics"."hd_dep_count" = 3
    AND "store_sales"."ss_sales_price" <= 150.00
    AND "store_sales"."ss_sales_price" >= 100.00
  )
  OR (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = 'Primary'
    AND "customer_demographics"."cd_marital_status" = 'M'
    AND "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND "household_demographics"."hd_dep_count" = 1
    AND "store_sales"."ss_sales_price" <= 100.00
    AND "store_sales"."ss_sales_price" >= 50.00
  )
  OR (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = 'Secondary'
    AND "customer_demographics"."cd_marital_status" = 'D'
    AND "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND "household_demographics"."hd_dep_count" = 1
    AND "store_sales"."ss_sales_price" <= 200.00
    AND "store_sales"."ss_sales_price" >= 150.00
  )
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk";

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
), "_q_0" AS (
  SELECT
    "iss"."i_brand_id" AS "brand_id",
    "iss"."i_class_id" AS "class_id",
    "iss"."i_category_id" AS "category_id"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "d1"
    ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "d1"."d_year" <= 2001
    AND "d1"."d_year" >= 1999
  JOIN "item" AS "iss"
    ON "iss"."i_item_sk" = "store_sales"."ss_item_sk"
  INTERSECT
  SELECT
    "ics"."i_brand_id" AS "i_brand_id",
    "ics"."i_class_id" AS "i_class_id",
    "ics"."i_category_id" AS "i_category_id"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim" AS "d2"
    ON "catalog_sales"."cs_sold_date_sk" = "d2"."d_date_sk"
    AND "d2"."d_year" <= 2001
    AND "d2"."d_year" >= 1999
  JOIN "item" AS "ics"
    ON "catalog_sales"."cs_item_sk" = "ics"."i_item_sk"
  INTERSECT
  SELECT
    "iws"."i_brand_id" AS "i_brand_id",
    "iws"."i_class_id" AS "i_class_id",
    "iws"."i_category_id" AS "i_category_id"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim" AS "d3"
    ON "d3"."d_date_sk" = "web_sales"."ws_sold_date_sk"
    AND "d3"."d_year" <= 2001
    AND "d3"."d_year" >= 1999
  JOIN "item" AS "iws"
    ON "iws"."i_item_sk" = "web_sales"."ws_item_sk"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_year" <= 2001 AND "date_dim"."d_year" >= 1999
), "x" AS (
  SELECT
    "store_sales"."ss_quantity" AS "quantity",
    "store_sales"."ss_list_price" AS "list_price"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  UNION ALL
  SELECT
    "catalog_sales"."cs_quantity" AS "quantity",
    "catalog_sales"."cs_list_price" AS "list_price"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  UNION ALL
  SELECT
    "web_sales"."ws_quantity" AS "quantity",
    "web_sales"."ws_list_price" AS "list_price"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
), "avg_sales" AS (
  SELECT
    AVG("x"."quantity" * "x"."list_price") AS "average_sales"
  FROM "x" AS "x"
), "_u_1" AS (
  SELECT
    "avg_sales"."average_sales" AS "average_sales"
  FROM "avg_sales"
), "_u_0" AS (
  SELECT
    "item"."i_item_sk" AS "ss_item_sk"
  FROM "item_2" AS "item"
  JOIN "_q_0" AS "_q_0"
    ON "_q_0"."brand_id" = "item"."i_brand_id"
    AND "_q_0"."category_id" = "item"."i_category_id"
    AND "_q_0"."class_id" = "item"."i_class_id"
  GROUP BY
    "item"."i_item_sk"
), "date_dim_3" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 11 AND "date_dim"."d_year" = 2001
), "y" AS (
  SELECT
    'store' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("store_sales"."ss_quantity" * "store_sales"."ss_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "store_sales" AS "store_sales"
  CROSS JOIN "_u_1" AS "_u_1"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."ss_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim_3" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  WHERE
    NOT "_u_0"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    MAX("_u_1"."average_sales") < SUM("store_sales"."ss_quantity" * "store_sales"."ss_list_price")
  UNION ALL
  SELECT
    'catalog' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("catalog_sales"."cs_quantity" * "catalog_sales"."cs_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "catalog_sales" AS "catalog_sales"
  CROSS JOIN "_u_1" AS "_u_3"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "_u_2"."ss_item_sk" = "catalog_sales"."cs_item_sk"
  JOIN "date_dim_3" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  WHERE
    NOT "_u_2"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    MAX("_u_3"."average_sales") < SUM("catalog_sales"."cs_quantity" * "catalog_sales"."cs_list_price")
  UNION ALL
  SELECT
    'web' AS "channel",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    SUM("web_sales"."ws_quantity" * "web_sales"."ws_list_price") AS "sales",
    COUNT(*) AS "number_sales"
  FROM "web_sales" AS "web_sales"
  CROSS JOIN "_u_1" AS "_u_5"
  LEFT JOIN "_u_0" AS "_u_4"
    ON "_u_4"."ss_item_sk" = "web_sales"."ws_item_sk"
  JOIN "date_dim_3" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  WHERE
    NOT "_u_4"."ss_item_sk" IS NULL
  GROUP BY
    "item"."i_brand_id",
    "item"."i_class_id",
    "item"."i_category_id"
  HAVING
    MAX("_u_5"."average_sales") < SUM("web_sales"."ws_quantity" * "web_sales"."ws_list_price")
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
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_qoy" = 1
  AND "date_dim"."d_year" = 1998
JOIN "customer" AS "customer"
  ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
  AND "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
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
  COUNT(DISTINCT "cs1"."cs_order_number") AS "order count",
  SUM("cs1"."cs_ext_ship_cost") AS "total shipping cost",
  SUM("cs1"."cs_net_profit") AS "total net profit"
FROM "catalog_sales" AS "cs1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "cs1"."cs_order_number"
LEFT JOIN "_u_3" AS "_u_3"
  ON "_u_3"."_u_4" = "cs1"."cs_order_number"
JOIN "call_center" AS "call_center"
  ON "call_center"."cc_call_center_sk" = "cs1"."cs_call_center_sk"
  AND "call_center"."cc_county" IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
JOIN "customer_address" AS "customer_address"
  ON "cs1"."cs_ship_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" = 'IA'
JOIN "date_dim" AS "date_dim"
  ON "cs1"."cs_ship_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_date" >= '2002-3-01'
  AND (
    CAST('2002-3-01' AS DATE) + INTERVAL '60' DAY
  ) >= CAST("date_dim"."d_date" AS DATE)
WHERE
  "_u_3"."_u_4" IS NULL
  AND NOT "_u_0"."_u_1" IS NULL
  AND ARRAY_ANY("_u_0"."_u_2", "_x" -> "cs1"."cs_warehouse_sk" <> "_x")
ORDER BY
  COUNT(DISTINCT "cs1"."cs_order_number")
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
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "d1"."d_quarter_name" = '1999Q1'
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_customer_sk" = "store_sales"."ss_customer_sk"
  AND "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
  AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
JOIN "catalog_sales" AS "catalog_sales"
  ON "catalog_sales"."cs_bill_customer_sk" = "store_returns"."sr_customer_sk"
  AND "catalog_sales"."cs_item_sk" = "store_returns"."sr_item_sk"
JOIN "date_dim" AS "d2"
  ON "d2"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  AND "d2"."d_quarter_name" IN ('1999Q1', '1999Q2', '1999Q3')
JOIN "date_dim" AS "d3"
  ON "catalog_sales"."cs_sold_date_sk" = "d3"."d_date_sk"
  AND "d3"."d_quarter_name" IN ('1999Q1', '1999Q2', '1999Q3')
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
  AVG(CAST("cd1"."cd_dep_count" AS DECIMAL(12, 2))) AS "agg7"
FROM "catalog_sales" AS "catalog_sales"
JOIN "customer_demographics" AS "cd1"
  ON "catalog_sales"."cs_bill_cdemo_sk" = "cd1"."cd_demo_sk"
  AND "cd1"."cd_education_status" = 'Secondary'
  AND "cd1"."cd_gender" = 'F'
JOIN "customer" AS "customer"
  ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
  AND "customer"."c_birth_month" IN (8, 4, 2, 5, 11, 9)
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_year" = 2001
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
JOIN "customer_demographics" AS "cd2"
  ON "cd2"."cd_demo_sk" = "customer"."c_current_cdemo_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" IN ('KS', 'IA', 'AL', 'UT', 'VA', 'NC', 'TX')
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

--------------------------------------
-- TPC-DS 19
--------------------------------------
SELECT i_brand_id              brand_id,
               i_brand                 brand,
               i_manufact_id,
               i_manufact,
               Sum(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item,
       customer,
       customer_address,
       store
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 38
       AND d_moy = 12
       AND d_year = 1998
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND Substr(ca_zip, 1, 5) <> Substr(s_zip, 1, 5)
       AND ss_store_sk = s_store_sk
GROUP  BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
ORDER  BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
LIMIT 100;
SELECT
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  "item"."i_manufact_id" AS "i_manufact_id",
  "item"."i_manufact" AS "i_manufact",
  SUM("store_sales"."ss_ext_sales_price") AS "ext_price"
FROM "date_dim" AS "date_dim"
JOIN "store_sales" AS "store_sales"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk" AND "item"."i_manager_id" = 38
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "customer_address" AS "customer_address"
  ON SUBSTR("customer_address"."ca_zip", 1, 5) <> SUBSTR("store"."s_zip", 1, 5)
JOIN "customer" AS "customer"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
WHERE
  "date_dim"."d_moy" = 12 AND "date_dim"."d_year" = 1998
GROUP BY
  "item"."i_brand",
  "item"."i_brand_id",
  "item"."i_manufact_id",
  "item"."i_manufact"
ORDER BY
  "ext_price" DESC,
  "brand",
  "brand_id",
  "i_manufact_id",
  "i_manufact"
LIMIT 100;

--------------------------------------
-- TPC-DS 20
--------------------------------------
SELECT
         i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price ,
         Sum(cs_ext_sales_price)                                                              AS itemrevenue ,
         Sum(cs_ext_sales_price)*100/Sum(Sum(cs_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     catalog_sales ,
         item ,
         date_dim
WHERE    cs_item_sk = i_item_sk
AND      i_category IN ('Children',
                        'Women',
                        'Electronics')
AND      cs_sold_date_sk = d_date_sk
AND      d_date BETWEEN Cast('2001-02-03' AS DATE) AND      (
                  Cast('2001-02-03' AS DATE) + INTERVAL '30' day)
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
  SUM("catalog_sales"."cs_ext_sales_price") AS "itemrevenue",
  SUM("catalog_sales"."cs_ext_sales_price") * 100 / SUM(SUM("catalog_sales"."cs_ext_sales_price")) OVER (PARTITION BY "item"."i_class") AS "revenueratio"
FROM "catalog_sales" AS "catalog_sales"
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('2001-03-05' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('2001-02-03' AS DATE)
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  AND "item"."i_category" IN ('Children', 'Women', 'Electronics')
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
-- TPC-DS 21
--------------------------------------
SELECT
         *
FROM    (
                  SELECT   w_warehouse_name ,
                           i_item_id ,
                           Sum(
                           CASE
                                    WHEN (
                                                      Cast(d_date AS DATE) < Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                                    ELSE 0
                           END) AS inv_before ,
                           Sum(
                           CASE
                                    WHEN (
                                                      Cast(d_date AS DATE) >= Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                                    ELSE 0
                           END) AS inv_after
                  FROM     inventory ,
                           warehouse ,
                           item ,
                           date_dim
                  WHERE    i_current_price BETWEEN 0.99 AND      1.49
                  AND      i_item_sk = inv_item_sk
                  AND      inv_warehouse_sk = w_warehouse_sk
                  AND      inv_date_sk = d_date_sk
                  AND      d_date BETWEEN (Cast ('2000-05-13' AS DATE) - INTERVAL '30' day) AND      (
                                    cast ('2000-05-13' AS        date) + INTERVAL '30' day)
                  GROUP BY w_warehouse_name,
                           i_item_id) x
WHERE    (
                  CASE
                           WHEN inv_before > 0 THEN inv_after / inv_before
                           ELSE NULL
                  END) BETWEEN 2.0/3.0 AND      3.0/2.0
ORDER BY w_warehouse_name ,
         i_item_id
LIMIT 100;
WITH "x" AS (
  SELECT
    "warehouse"."w_warehouse_name" AS "w_warehouse_name",
    "item"."i_item_id" AS "i_item_id",
    SUM(
      CASE
        WHEN CAST("date_dim"."d_date" AS DATE) < CAST('2000-05-13' AS DATE)
        THEN "inventory"."inv_quantity_on_hand"
        ELSE 0
      END
    ) AS "inv_before",
    SUM(
      CASE
        WHEN CAST("date_dim"."d_date" AS DATE) >= CAST('2000-05-13' AS DATE)
        THEN "inventory"."inv_quantity_on_hand"
        ELSE 0
      END
    ) AS "inv_after"
  FROM "inventory" AS "inventory"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "inventory"."inv_date_sk"
    AND CAST("date_dim"."d_date" AS DATE) <= CAST('2000-06-12' AS DATE)
    AND CAST("date_dim"."d_date" AS DATE) >= CAST('2000-04-13' AS DATE)
  JOIN "item" AS "item"
    ON "inventory"."inv_item_sk" = "item"."i_item_sk"
    AND "item"."i_current_price" <= 1.49
    AND "item"."i_current_price" >= 0.99
  JOIN "warehouse" AS "warehouse"
    ON "inventory"."inv_warehouse_sk" = "warehouse"."w_warehouse_sk"
  GROUP BY
    "warehouse"."w_warehouse_name",
    "item"."i_item_id"
)
SELECT
  "x"."w_warehouse_name" AS "w_warehouse_name",
  "x"."i_item_id" AS "i_item_id",
  "x"."inv_before" AS "inv_before",
  "x"."inv_after" AS "inv_after"
FROM "x" AS "x"
WHERE
  CASE WHEN "x"."inv_before" > 0 THEN "x"."inv_after" / "x"."inv_before" ELSE NULL END <= 1.5
  AND CASE WHEN "x"."inv_before" > 0 THEN "x"."inv_after" / "x"."inv_before" ELSE NULL END >= 0.6666666666666666666666666667
ORDER BY
  "x"."w_warehouse_name",
  "x"."i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 22
--------------------------------------
SELECT i_product_name,
               i_brand,
               i_class,
               i_category,
               Avg(inv_quantity_on_hand) qoh
FROM   inventory,
       date_dim,
       item,
       warehouse
WHERE  inv_date_sk = d_date_sk
       AND inv_item_sk = i_item_sk
       AND inv_warehouse_sk = w_warehouse_sk
       AND d_month_seq BETWEEN 1205 AND 1205 + 11
GROUP  BY rollup( i_product_name, i_brand, i_class, i_category )
ORDER  BY qoh,
          i_product_name,
          i_brand,
          i_class,
          i_category
LIMIT 100;
SELECT
  "item"."i_product_name" AS "i_product_name",
  "item"."i_brand" AS "i_brand",
  "item"."i_class" AS "i_class",
  "item"."i_category" AS "i_category",
  AVG("inventory"."inv_quantity_on_hand") AS "qoh"
FROM "inventory" AS "inventory"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "inventory"."inv_date_sk"
  AND "date_dim"."d_month_seq" <= 1216
  AND "date_dim"."d_month_seq" >= 1205
JOIN "item" AS "item"
  ON "inventory"."inv_item_sk" = "item"."i_item_sk"
JOIN "warehouse" AS "warehouse"
  ON "inventory"."inv_warehouse_sk" = "warehouse"."w_warehouse_sk"
GROUP BY
ROLLUP (
  "item"."i_product_name",
  "item"."i_brand",
  "item"."i_class",
  "item"."i_category"
)
ORDER BY
  "qoh",
  "i_product_name",
  "i_brand",
  "i_class",
  "i_category"
LIMIT 100;

--------------------------------------
-- TPC-DS 23
--------------------------------------
WITH frequent_ss_items
     AS (SELECT Substr(i_item_desc, 1, 30) itemdesc,
                i_item_sk                  item_sk,
                d_date                     solddate,
                Count(*)                   cnt
         FROM   store_sales,
                date_dim,
                item
         WHERE  ss_sold_date_sk = d_date_sk
                AND ss_item_sk = i_item_sk
                AND d_year IN ( 1998, 1998 + 1, 1998 + 2, 1998 + 3 )
         GROUP  BY Substr(i_item_desc, 1, 30),
                   i_item_sk,
                   d_date
         HAVING Count(*) > 4),
     max_store_sales
     AS (SELECT Max(csales) tpcds_cmax
         FROM   (SELECT c_customer_sk,
                        Sum(ss_quantity * ss_sales_price) csales
                 FROM   store_sales,
                        customer,
                        date_dim
                 WHERE  ss_customer_sk = c_customer_sk
                        AND ss_sold_date_sk = d_date_sk
                        AND d_year IN ( 1998, 1998 + 1, 1998 + 2, 1998 + 3 )
                 GROUP  BY c_customer_sk)),
     best_ss_customer
     AS (SELECT c_customer_sk,
                Sum(ss_quantity * ss_sales_price) ssales
         FROM   store_sales,
                customer
         WHERE  ss_customer_sk = c_customer_sk
         GROUP  BY c_customer_sk
         HAVING Sum(ss_quantity * ss_sales_price) >
                ( 95 / 100.0 ) * (SELECT *
                                  FROM   max_store_sales))
SELECT Sum(sales)
FROM   (SELECT cs_quantity * cs_list_price sales
        FROM   catalog_sales,
               date_dim
        WHERE  d_year = 1998
               AND d_moy = 6
               AND cs_sold_date_sk = d_date_sk
               AND cs_item_sk IN (SELECT item_sk
                                  FROM   frequent_ss_items)
               AND cs_bill_customer_sk IN (SELECT c_customer_sk
                                           FROM   best_ss_customer)
        UNION ALL
        SELECT ws_quantity * ws_list_price sales
        FROM   web_sales,
               date_dim
        WHERE  d_year = 1998
               AND d_moy = 6
               AND ws_sold_date_sk = d_date_sk
               AND ws_item_sk IN (SELECT item_sk
                                  FROM   frequent_ss_items)
               AND ws_bill_customer_sk IN (SELECT c_customer_sk
                                           FROM   best_ss_customer)) LIMIT 100;
WITH "frequent_ss_items" AS (
  SELECT
    "item"."i_item_sk" AS "item_sk"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_year" IN (1998, 1999, 2000, 2001)
  JOIN "item" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  GROUP BY
    SUBSTR("item"."i_item_desc", 1, 30),
    "item"."i_item_sk",
    "date_dim"."d_date"
  HAVING
    COUNT(*) > 4
), "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk"
  FROM "customer" AS "customer"
), "_q_0" AS (
  SELECT
    SUM("store_sales"."ss_quantity" * "store_sales"."ss_sales_price") AS "csales"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_year" IN (1998, 1999, 2000, 2001)
  GROUP BY
    "customer"."c_customer_sk"
), "max_store_sales" AS (
  SELECT
    MAX("_q_0"."csales") AS "tpcds_cmax"
  FROM "_q_0" AS "_q_0"
), "best_ss_customer" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk"
  FROM "store_sales" AS "store_sales"
  CROSS JOIN "max_store_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  GROUP BY
    "customer"."c_customer_sk"
  HAVING
    0.95 * MAX("max_store_sales"."tpcds_cmax") < SUM("store_sales"."ss_quantity" * "store_sales"."ss_sales_price")
), "_u_1" AS (
  SELECT
    "frequent_ss_items"."item_sk" AS "item_sk"
  FROM "frequent_ss_items"
  GROUP BY
    "frequent_ss_items"."item_sk"
), "_u_2" AS (
  SELECT
    "best_ss_customer"."c_customer_sk" AS "c_customer_sk"
  FROM "best_ss_customer"
  GROUP BY
    "best_ss_customer"."c_customer_sk"
), "date_dim_4" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 6 AND "date_dim"."d_year" = 1998
), "_q_1" AS (
  SELECT
    "catalog_sales"."cs_quantity" * "catalog_sales"."cs_list_price" AS "sales"
  FROM "catalog_sales" AS "catalog_sales"
  LEFT JOIN "_u_1" AS "_u_1"
    ON "_u_1"."item_sk" = "catalog_sales"."cs_item_sk"
  LEFT JOIN "_u_2" AS "_u_2"
    ON "_u_2"."c_customer_sk" = "catalog_sales"."cs_bill_customer_sk"
  JOIN "date_dim_4" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    NOT "_u_1"."item_sk" IS NULL AND NOT "_u_2"."c_customer_sk" IS NULL
  UNION ALL
  SELECT
    "web_sales"."ws_quantity" * "web_sales"."ws_list_price" AS "sales"
  FROM "web_sales" AS "web_sales"
  LEFT JOIN "_u_1" AS "_u_3"
    ON "_u_3"."item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "_u_2" AS "_u_4"
    ON "_u_4"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
  JOIN "date_dim_4" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  WHERE
    NOT "_u_3"."item_sk" IS NULL AND NOT "_u_4"."c_customer_sk" IS NULL
)
SELECT
  SUM("_q_1"."sales") AS "_col_0"
FROM "_q_1" AS "_q_1"
LIMIT 100;

--------------------------------------
-- TPC-DS 24
--------------------------------------
WITH ssales
     AS (SELECT c_last_name,
                c_first_name,
                s_store_name,
                ca_state,
                s_state,
                i_color,
                i_current_price,
                i_manager_id,
                i_units,
                i_size,
                Sum(ss_net_profit) netpaid
         FROM   store_sales,
                store_returns,
                store,
                item,
                customer,
                customer_address
         WHERE  ss_ticket_number = sr_ticket_number
                AND ss_item_sk = sr_item_sk
                AND ss_customer_sk = c_customer_sk
                AND ss_item_sk = i_item_sk
                AND ss_store_sk = s_store_sk
                AND c_birth_country = Upper(ca_country)
                AND s_zip = ca_zip
                AND s_market_id = 6
         GROUP  BY c_last_name,
                   c_first_name,
                   s_store_name,
                   ca_state,
                   s_state,
                   i_color,
                   i_current_price,
                   i_manager_id,
                   i_units,
                   i_size)
SELECT c_last_name,
       c_first_name,
       s_store_name,
       Sum(netpaid) paid
FROM   ssales
WHERE  i_color = 'papaya'
GROUP  BY c_last_name,
          c_first_name,
          s_store_name
HAVING Sum(netpaid) > (SELECT 0.05 * Avg(netpaid)
                       FROM   ssales);
WITH "ssales" AS (
  SELECT
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_first_name" AS "c_first_name",
    "store"."s_store_name" AS "s_store_name",
    "item"."i_color" AS "i_color",
    SUM("store_sales"."ss_net_profit") AS "netpaid"
  FROM "store_sales" AS "store_sales"
  JOIN "item" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "store" AS "store"
    ON "store"."s_market_id" = 6 AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "store_returns" AS "store_returns"
    ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
    AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
  JOIN "customer_address" AS "customer_address"
    ON "customer_address"."ca_zip" = "store"."s_zip"
  JOIN "customer" AS "customer"
    ON "customer"."c_birth_country" = UPPER("customer_address"."ca_country")
    AND "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  GROUP BY
    "customer"."c_last_name",
    "customer"."c_first_name",
    "store"."s_store_name",
    "customer_address"."ca_state",
    "store"."s_state",
    "item"."i_color",
    "item"."i_current_price",
    "item"."i_manager_id",
    "item"."i_units",
    "item"."i_size"
), "_u_0" AS (
  SELECT
    0.05 * AVG("ssales"."netpaid") AS "_col_0"
  FROM "ssales"
)
SELECT
  "ssales"."c_last_name" AS "c_last_name",
  "ssales"."c_first_name" AS "c_first_name",
  "ssales"."s_store_name" AS "s_store_name",
  SUM("ssales"."netpaid") AS "paid"
FROM "ssales"
CROSS JOIN "_u_0" AS "_u_0"
WHERE
  "ssales"."i_color" = 'papaya'
GROUP BY
  "ssales"."c_last_name",
  "ssales"."c_first_name",
  "ssales"."s_store_name"
HAVING
  MAX("_u_0"."_col_0") < SUM("ssales"."netpaid");

--------------------------------------
-- TPC-DS 25
--------------------------------------
SELECT i_item_id,
               i_item_desc,
               s_store_id,
               s_store_name,
               Max(ss_net_profit) AS store_sales_profit,
               Max(sr_net_loss)   AS store_returns_loss,
               Max(cs_net_profit) AS catalog_sales_profit
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_moy = 4
       AND d1.d_year = 2001
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_moy BETWEEN 4 AND 10
       AND d2.d_year = 2001
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_moy BETWEEN 4 AND 10
       AND d3.d_year = 2001
GROUP  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
ORDER  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "store"."s_store_id" AS "s_store_id",
  "store"."s_store_name" AS "s_store_name",
  MAX("store_sales"."ss_net_profit") AS "store_sales_profit",
  MAX("store_returns"."sr_net_loss") AS "store_returns_loss",
  MAX("catalog_sales"."cs_net_profit") AS "catalog_sales_profit"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "d1"."d_moy" = 4
  AND "d1"."d_year" = 2001
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_customer_sk" = "store_sales"."ss_customer_sk"
  AND "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
  AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
JOIN "catalog_sales" AS "catalog_sales"
  ON "catalog_sales"."cs_bill_customer_sk" = "store_returns"."sr_customer_sk"
  AND "catalog_sales"."cs_item_sk" = "store_returns"."sr_item_sk"
JOIN "date_dim" AS "d2"
  ON "d2"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  AND "d2"."d_moy" <= 10
  AND "d2"."d_moy" >= 4
  AND "d2"."d_year" = 2001
JOIN "date_dim" AS "d3"
  ON "catalog_sales"."cs_sold_date_sk" = "d3"."d_date_sk"
  AND "d3"."d_moy" <= 10
  AND "d3"."d_moy" >= 4
  AND "d3"."d_year" = 2001
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "store"."s_store_id",
  "store"."s_store_name"
ORDER BY
  "i_item_id",
  "i_item_desc",
  "s_store_id",
  "s_store_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 26
--------------------------------------
SELECT i_item_id,
               Avg(cs_quantity)    agg1,
               Avg(cs_list_price)  agg2,
               Avg(cs_coupon_amt)  agg3,
               Avg(cs_sales_price) agg4
FROM   catalog_sales,
       customer_demographics,
       date_dim,
       item,
       promotion
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd_demo_sk
       AND cs_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'W'
       AND cd_education_status = 'Secondary'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 2000
GROUP  BY i_item_id
ORDER  BY i_item_id
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  AVG("catalog_sales"."cs_quantity") AS "agg1",
  AVG("catalog_sales"."cs_list_price") AS "agg2",
  AVG("catalog_sales"."cs_coupon_amt") AS "agg3",
  AVG("catalog_sales"."cs_sales_price") AS "agg4"
FROM "catalog_sales" AS "catalog_sales"
JOIN "customer_demographics" AS "customer_demographics"
  ON "catalog_sales"."cs_bill_cdemo_sk" = "customer_demographics"."cd_demo_sk"
  AND "customer_demographics"."cd_education_status" = 'Secondary'
  AND "customer_demographics"."cd_gender" = 'F'
  AND "customer_demographics"."cd_marital_status" = 'W'
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_year" = 2000
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
JOIN "promotion" AS "promotion"
  ON "catalog_sales"."cs_promo_sk" = "promotion"."p_promo_sk"
  AND (
    "promotion"."p_channel_email" = 'N' OR "promotion"."p_channel_event" = 'N'
  )
GROUP BY
  "item"."i_item_id"
ORDER BY
  "i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 27
--------------------------------------
SELECT i_item_id,
               s_state,
               Grouping(s_state)   g_state,
               Avg(ss_quantity)    agg1,
               Avg(ss_list_price)  agg2,
               Avg(ss_coupon_amt)  agg3,
               Avg(ss_sales_price) agg4
FROM   store_sales,
       customer_demographics,
       date_dim,
       store,
       item
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_store_sk = s_store_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND cd_gender = 'M'
       AND cd_marital_status = 'D'
       AND cd_education_status = 'College'
       AND d_year = 2000
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN',
                        'TN', 'TN' )
GROUP  BY rollup ( i_item_id, s_state )
ORDER  BY i_item_id,
          s_state
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "store"."s_state" AS "s_state",
  GROUPING("store"."s_state") AS "g_state",
  AVG("store_sales"."ss_quantity") AS "agg1",
  AVG("store_sales"."ss_list_price") AS "agg2",
  AVG("store_sales"."ss_coupon_amt") AS "agg3",
  AVG("store_sales"."ss_sales_price") AS "agg4"
FROM "store_sales" AS "store_sales"
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
  AND "customer_demographics"."cd_education_status" = 'College'
  AND "customer_demographics"."cd_gender" = 'M'
  AND "customer_demographics"."cd_marital_status" = 'D'
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_year" = 2000
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "store" AS "store"
  ON "store"."s_state" IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN')
  AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
GROUP BY
ROLLUP (
  "item"."i_item_id",
  "store"."s_state"
)
ORDER BY
  "i_item_id",
  "s_state"
LIMIT 100;

--------------------------------------
-- TPC-DS 28
--------------------------------------
SELECT *
FROM   (SELECT Avg(ss_list_price)            B1_LP,
               Count(ss_list_price)          B1_CNT,
               Count(DISTINCT ss_list_price) B1_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 0 AND 5
               AND ( ss_list_price BETWEEN 18 AND 18 + 10
                      OR ss_coupon_amt BETWEEN 1939 AND 1939 + 1000
                      OR ss_wholesale_cost BETWEEN 34 AND 34 + 20 )) B1,
       (SELECT Avg(ss_list_price)            B2_LP,
               Count(ss_list_price)          B2_CNT,
               Count(DISTINCT ss_list_price) B2_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 6 AND 10
               AND ( ss_list_price BETWEEN 1 AND 1 + 10
                      OR ss_coupon_amt BETWEEN 35 AND 35 + 1000
                      OR ss_wholesale_cost BETWEEN 50 AND 50 + 20 )) B2,
       (SELECT Avg(ss_list_price)            B3_LP,
               Count(ss_list_price)          B3_CNT,
               Count(DISTINCT ss_list_price) B3_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 11 AND 15
               AND ( ss_list_price BETWEEN 91 AND 91 + 10
                      OR ss_coupon_amt BETWEEN 1412 AND 1412 + 1000
                      OR ss_wholesale_cost BETWEEN 17 AND 17 + 20 )) B3,
       (SELECT Avg(ss_list_price)            B4_LP,
               Count(ss_list_price)          B4_CNT,
               Count(DISTINCT ss_list_price) B4_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 16 AND 20
               AND ( ss_list_price BETWEEN 9 AND 9 + 10
                      OR ss_coupon_amt BETWEEN 5270 AND 5270 + 1000
                      OR ss_wholesale_cost BETWEEN 29 AND 29 + 20 )) B4,
       (SELECT Avg(ss_list_price)            B5_LP,
               Count(ss_list_price)          B5_CNT,
               Count(DISTINCT ss_list_price) B5_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 21 AND 25
               AND ( ss_list_price BETWEEN 45 AND 45 + 10
                      OR ss_coupon_amt BETWEEN 826 AND 826 + 1000
                      OR ss_wholesale_cost BETWEEN 5 AND 5 + 20 )) B5,
       (SELECT Avg(ss_list_price)            B6_LP,
               Count(ss_list_price)          B6_CNT,
               Count(DISTINCT ss_list_price) B6_CNTD
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 26 AND 30
               AND ( ss_list_price BETWEEN 174 AND 174 + 10
                      OR ss_coupon_amt BETWEEN 5548 AND 5548 + 1000
                      OR ss_wholesale_cost BETWEEN 42 AND 42 + 20 )) B6
LIMIT 100;
WITH "b1" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b1_lp",
    COUNT("store_sales"."ss_list_price") AS "b1_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b1_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 2939 AND "store_sales"."ss_coupon_amt" >= 1939
      OR "store_sales"."ss_list_price" <= 28 AND "store_sales"."ss_list_price" >= 18
      OR "store_sales"."ss_wholesale_cost" <= 54 AND "store_sales"."ss_wholesale_cost" >= 34
    )
    AND "store_sales"."ss_quantity" <= 5
    AND "store_sales"."ss_quantity" >= 0
), "b2" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b2_lp",
    COUNT("store_sales"."ss_list_price") AS "b2_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b2_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 1035 AND "store_sales"."ss_coupon_amt" >= 35
      OR "store_sales"."ss_list_price" <= 11 AND "store_sales"."ss_list_price" >= 1
      OR "store_sales"."ss_wholesale_cost" <= 70 AND "store_sales"."ss_wholesale_cost" >= 50
    )
    AND "store_sales"."ss_quantity" <= 10
    AND "store_sales"."ss_quantity" >= 6
), "b3" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b3_lp",
    COUNT("store_sales"."ss_list_price") AS "b3_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b3_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 2412 AND "store_sales"."ss_coupon_amt" >= 1412
      OR "store_sales"."ss_list_price" <= 101 AND "store_sales"."ss_list_price" >= 91
      OR "store_sales"."ss_wholesale_cost" <= 37 AND "store_sales"."ss_wholesale_cost" >= 17
    )
    AND "store_sales"."ss_quantity" <= 15
    AND "store_sales"."ss_quantity" >= 11
), "b4" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b4_lp",
    COUNT("store_sales"."ss_list_price") AS "b4_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b4_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 6270 AND "store_sales"."ss_coupon_amt" >= 5270
      OR "store_sales"."ss_list_price" <= 19 AND "store_sales"."ss_list_price" >= 9
      OR "store_sales"."ss_wholesale_cost" <= 49 AND "store_sales"."ss_wholesale_cost" >= 29
    )
    AND "store_sales"."ss_quantity" <= 20
    AND "store_sales"."ss_quantity" >= 16
), "b5" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b5_lp",
    COUNT("store_sales"."ss_list_price") AS "b5_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b5_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 1826 AND "store_sales"."ss_coupon_amt" >= 826
      OR "store_sales"."ss_list_price" <= 55 AND "store_sales"."ss_list_price" >= 45
      OR "store_sales"."ss_wholesale_cost" <= 25 AND "store_sales"."ss_wholesale_cost" >= 5
    )
    AND "store_sales"."ss_quantity" <= 25
    AND "store_sales"."ss_quantity" >= 21
), "b6" AS (
  SELECT
    AVG("store_sales"."ss_list_price") AS "b6_lp",
    COUNT("store_sales"."ss_list_price") AS "b6_cnt",
    COUNT(DISTINCT "store_sales"."ss_list_price") AS "b6_cntd"
  FROM "store_sales" AS "store_sales"
  WHERE
    (
      "store_sales"."ss_coupon_amt" <= 6548 AND "store_sales"."ss_coupon_amt" >= 5548
      OR "store_sales"."ss_list_price" <= 184 AND "store_sales"."ss_list_price" >= 174
      OR "store_sales"."ss_wholesale_cost" <= 62 AND "store_sales"."ss_wholesale_cost" >= 42
    )
    AND "store_sales"."ss_quantity" <= 30
    AND "store_sales"."ss_quantity" >= 26
)
SELECT
  "b1"."b1_lp" AS "b1_lp",
  "b1"."b1_cnt" AS "b1_cnt",
  "b1"."b1_cntd" AS "b1_cntd",
  "b2"."b2_lp" AS "b2_lp",
  "b2"."b2_cnt" AS "b2_cnt",
  "b2"."b2_cntd" AS "b2_cntd",
  "b3"."b3_lp" AS "b3_lp",
  "b3"."b3_cnt" AS "b3_cnt",
  "b3"."b3_cntd" AS "b3_cntd",
  "b4"."b4_lp" AS "b4_lp",
  "b4"."b4_cnt" AS "b4_cnt",
  "b4"."b4_cntd" AS "b4_cntd",
  "b5"."b5_lp" AS "b5_lp",
  "b5"."b5_cnt" AS "b5_cnt",
  "b5"."b5_cntd" AS "b5_cntd",
  "b6"."b6_lp" AS "b6_lp",
  "b6"."b6_cnt" AS "b6_cnt",
  "b6"."b6_cntd" AS "b6_cntd"
FROM "b1" AS "b1"
CROSS JOIN "b2" AS "b2"
CROSS JOIN "b3" AS "b3"
CROSS JOIN "b4" AS "b4"
CROSS JOIN "b5" AS "b5"
CROSS JOIN "b6" AS "b6"
LIMIT 100;

--------------------------------------
-- TPC-DS 29
--------------------------------------
SELECT i_item_id,
               i_item_desc,
               s_store_id,
               s_store_name,
               Avg(ss_quantity)        AS store_sales_quantity,
               Avg(sr_return_quantity) AS store_returns_quantity,
               Avg(cs_quantity)        AS catalog_sales_quantity
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_moy = 4
       AND d1.d_year = 1998
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_moy BETWEEN 4 AND 4 + 3
       AND d2.d_year = 1998
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
GROUP  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
ORDER  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "store"."s_store_id" AS "s_store_id",
  "store"."s_store_name" AS "s_store_name",
  AVG("store_sales"."ss_quantity") AS "store_sales_quantity",
  AVG("store_returns"."sr_return_quantity") AS "store_returns_quantity",
  AVG("catalog_sales"."cs_quantity") AS "catalog_sales_quantity"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "d1"."d_moy" = 4
  AND "d1"."d_year" = 1998
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_customer_sk" = "store_sales"."ss_customer_sk"
  AND "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
  AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
JOIN "catalog_sales" AS "catalog_sales"
  ON "catalog_sales"."cs_bill_customer_sk" = "store_returns"."sr_customer_sk"
  AND "catalog_sales"."cs_item_sk" = "store_returns"."sr_item_sk"
JOIN "date_dim" AS "d2"
  ON "d2"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  AND "d2"."d_moy" <= 7
  AND "d2"."d_moy" >= 4
  AND "d2"."d_year" = 1998
JOIN "date_dim" AS "d3"
  ON "catalog_sales"."cs_sold_date_sk" = "d3"."d_date_sk"
  AND "d3"."d_year" IN (1998, 1999, 2000)
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "store"."s_store_id",
  "store"."s_store_name"
ORDER BY
  "i_item_id",
  "i_item_desc",
  "s_store_id",
  "s_store_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 30
--------------------------------------
WITH customer_total_return
     AS (SELECT wr_returning_customer_sk AS ctr_customer_sk,
                ca_state                 AS ctr_state,
                Sum(wr_return_amt)       AS ctr_total_return
         FROM   web_returns,
                date_dim,
                customer_address
         WHERE  wr_returned_date_sk = d_date_sk
                AND d_year = 2000
                AND wr_returning_addr_sk = ca_address_sk
         GROUP  BY wr_returning_customer_sk,
                   ca_state)
SELECT c_customer_id,
               c_salutation,
               c_first_name,
               c_last_name,
               c_preferred_cust_flag,
               c_birth_day,
               c_birth_month,
               c_birth_year,
               c_birth_country,
               c_login,
               c_email_address,
               c_last_review_date,
               ctr_total_return
FROM   customer_total_return ctr1,
       customer_address,
       customer
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_state = ctr2.ctr_state)
       AND ca_address_sk = c_current_addr_sk
       AND ca_state = 'IN'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id,
          c_salutation,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_day,
          c_birth_month,
          c_birth_year,
          c_birth_country,
          c_login,
          c_email_address,
          c_last_review_date,
          ctr_total_return
LIMIT 100;
WITH "customer_total_return" AS (
  SELECT
    "web_returns"."wr_returning_customer_sk" AS "ctr_customer_sk",
    "customer_address"."ca_state" AS "ctr_state",
    SUM("web_returns"."wr_return_amt") AS "ctr_total_return"
  FROM "web_returns" AS "web_returns"
  JOIN "customer_address" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "web_returns"."wr_returning_addr_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_returns"."wr_returned_date_sk"
    AND "date_dim"."d_year" = 2000
  GROUP BY
    "web_returns"."wr_returning_customer_sk",
    "customer_address"."ca_state"
), "_u_0" AS (
  SELECT
    AVG("ctr2"."ctr_total_return") * 1.2 AS "_col_0",
    "ctr2"."ctr_state" AS "_u_1"
  FROM "customer_total_return" AS "ctr2"
  GROUP BY
    "ctr2"."ctr_state"
)
SELECT
  "customer"."c_customer_id" AS "c_customer_id",
  "customer"."c_salutation" AS "c_salutation",
  "customer"."c_first_name" AS "c_first_name",
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_preferred_cust_flag" AS "c_preferred_cust_flag",
  "customer"."c_birth_day" AS "c_birth_day",
  "customer"."c_birth_month" AS "c_birth_month",
  "customer"."c_birth_year" AS "c_birth_year",
  "customer"."c_birth_country" AS "c_birth_country",
  "customer"."c_login" AS "c_login",
  "customer"."c_email_address" AS "c_email_address",
  "customer"."c_last_review_date" AS "c_last_review_date",
  "ctr1"."ctr_total_return" AS "ctr_total_return"
FROM "customer_total_return" AS "ctr1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "ctr1"."ctr_state"
JOIN "customer" AS "customer"
  ON "ctr1"."ctr_customer_sk" = "customer"."c_customer_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" = 'IN'
WHERE
  "_u_0"."_col_0" < "ctr1"."ctr_total_return"
ORDER BY
  "c_customer_id",
  "c_salutation",
  "c_first_name",
  "c_last_name",
  "c_preferred_cust_flag",
  "c_birth_day",
  "c_birth_month",
  "c_birth_year",
  "c_birth_country",
  "c_login",
  "c_email_address",
  "c_last_review_date",
  "ctr_total_return"
LIMIT 100;

--------------------------------------
-- TPC-DS 31
--------------------------------------
WITH ss
     AS (SELECT ca_county,
                d_qoy,
                d_year,
                Sum(ss_ext_sales_price) AS store_sales
         FROM   store_sales,
                date_dim,
                customer_address
         WHERE  ss_sold_date_sk = d_date_sk
                AND ss_addr_sk = ca_address_sk
         GROUP  BY ca_county,
                   d_qoy,
                   d_year),
     ws
     AS (SELECT ca_county,
                d_qoy,
                d_year,
                Sum(ws_ext_sales_price) AS web_sales
         FROM   web_sales,
                date_dim,
                customer_address
         WHERE  ws_sold_date_sk = d_date_sk
                AND ws_bill_addr_sk = ca_address_sk
         GROUP  BY ca_county,
                   d_qoy,
                   d_year)
SELECT ss1.ca_county,
       ss1.d_year,
       ws2.web_sales / ws1.web_sales     web_q1_q2_increase,
       ss2.store_sales / ss1.store_sales store_q1_q2_increase,
       ws3.web_sales / ws2.web_sales     web_q2_q3_increase,
       ss3.store_sales / ss2.store_sales store_q2_q3_increase
FROM   ss ss1,
       ss ss2,
       ss ss3,
       ws ws1,
       ws ws2,
       ws ws3
WHERE  ss1.d_qoy = 1
       AND ss1.d_year = 2001
       AND ss1.ca_county = ss2.ca_county
       AND ss2.d_qoy = 2
       AND ss2.d_year = 2001
       AND ss2.ca_county = ss3.ca_county
       AND ss3.d_qoy = 3
       AND ss3.d_year = 2001
       AND ss1.ca_county = ws1.ca_county
       AND ws1.d_qoy = 1
       AND ws1.d_year = 2001
       AND ws1.ca_county = ws2.ca_county
       AND ws2.d_qoy = 2
       AND ws2.d_year = 2001
       AND ws1.ca_county = ws3.ca_county
       AND ws3.d_qoy = 3
       AND ws3.d_year = 2001
       AND CASE
             WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales
             ELSE NULL
           END > CASE
                   WHEN ss1.store_sales > 0 THEN
                   ss2.store_sales / ss1.store_sales
                   ELSE NULL
                 END
       AND CASE
             WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales
             ELSE NULL
           END > CASE
                   WHEN ss2.store_sales > 0 THEN
                   ss3.store_sales / ss2.store_sales
                   ELSE NULL
                 END
ORDER  BY ss1.d_year;
WITH "customer_address_2" AS (
  SELECT
    "customer_address"."ca_address_sk" AS "ca_address_sk",
    "customer_address"."ca_county" AS "ca_county"
  FROM "customer_address" AS "customer_address"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy"
  FROM "date_dim" AS "date_dim"
), "ss" AS (
  SELECT
    "customer_address"."ca_county" AS "ca_county",
    "date_dim"."d_qoy" AS "d_qoy",
    "date_dim"."d_year" AS "d_year",
    SUM("store_sales"."ss_ext_sales_price") AS "store_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "customer_address"."ca_county",
    "date_dim"."d_qoy",
    "date_dim"."d_year"
), "ws" AS (
  SELECT
    "customer_address"."ca_county" AS "ca_county",
    "date_dim"."d_qoy" AS "d_qoy",
    "date_dim"."d_year" AS "d_year",
    SUM("web_sales"."ws_ext_sales_price") AS "web_sales"
  FROM "web_sales" AS "web_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "web_sales"."ws_bill_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "customer_address"."ca_county",
    "date_dim"."d_qoy",
    "date_dim"."d_year"
)
SELECT
  "ss1"."ca_county" AS "ca_county",
  "ss1"."d_year" AS "d_year",
  "ws2"."web_sales" / "ws1"."web_sales" AS "web_q1_q2_increase",
  "ss2"."store_sales" / "ss1"."store_sales" AS "store_q1_q2_increase",
  "ws3"."web_sales" / "ws2"."web_sales" AS "web_q2_q3_increase",
  "ss3"."store_sales" / "ss2"."store_sales" AS "store_q2_q3_increase"
FROM "ss" AS "ss1"
JOIN "ss" AS "ss2"
  ON "ss1"."ca_county" = "ss2"."ca_county" AND "ss2"."d_qoy" = 2 AND "ss2"."d_year" = 2001
JOIN "ws" AS "ws1"
  ON "ss1"."ca_county" = "ws1"."ca_county" AND "ws1"."d_qoy" = 1 AND "ws1"."d_year" = 2001
JOIN "ws" AS "ws2"
  ON "ws1"."ca_county" = "ws2"."ca_county"
  AND "ws2"."d_qoy" = 2
  AND "ws2"."d_year" = 2001
  AND CASE
    WHEN "ss1"."store_sales" > 0
    THEN "ss2"."store_sales" / "ss1"."store_sales"
    ELSE NULL
  END < CASE
    WHEN "ws1"."web_sales" > 0
    THEN "ws2"."web_sales" / "ws1"."web_sales"
    ELSE NULL
  END
JOIN "ws" AS "ws3"
  ON "ws1"."ca_county" = "ws3"."ca_county" AND "ws3"."d_qoy" = 3 AND "ws3"."d_year" = 2001
JOIN "ss" AS "ss3"
  ON "ss2"."ca_county" = "ss3"."ca_county"
  AND "ss3"."d_qoy" = 3
  AND "ss3"."d_year" = 2001
  AND CASE
    WHEN "ss2"."store_sales" > 0
    THEN "ss3"."store_sales" / "ss2"."store_sales"
    ELSE NULL
  END < CASE
    WHEN "ws2"."web_sales" > 0
    THEN "ws3"."web_sales" / "ws2"."web_sales"
    ELSE NULL
  END
WHERE
  "ss1"."d_qoy" = 1 AND "ss1"."d_year" = 2001
ORDER BY
  "ss1"."d_year";

--------------------------------------
-- TPC-DS 32
--------------------------------------
SELECT
       Sum(cs_ext_discount_amt) AS "excess discount amount"
FROM   catalog_sales ,
       item ,
       date_dim
WHERE  i_manufact_id = 610
AND    i_item_sk = cs_item_sk
AND    d_date BETWEEN '2001-03-04' AND    (
              Cast('2001-03-04' AS DATE) + INTERVAL '90' day)
AND    d_date_sk = cs_sold_date_sk
AND    cs_ext_discount_amt >
       (
              SELECT 1.3 * avg(cs_ext_discount_amt)
              FROM   catalog_sales ,
                     date_dim
              WHERE  cs_item_sk = i_item_sk
              AND    d_date BETWEEN '2001-03-04' AND    (
                            cast('2001-03-04' AS date) + INTERVAL '90' day)
              AND    d_date_sk = cs_sold_date_sk )
LIMIT 100;
WITH "catalog_sales_2" AS (
  SELECT
    "catalog_sales"."cs_sold_date_sk" AS "cs_sold_date_sk",
    "catalog_sales"."cs_item_sk" AS "cs_item_sk",
    "catalog_sales"."cs_ext_discount_amt" AS "cs_ext_discount_amt"
  FROM "catalog_sales" AS "catalog_sales"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_date" >= '2001-03-04'
    AND CAST("date_dim"."d_date" AS DATE) <= CAST('2001-06-02' AS DATE)
), "_u_0" AS (
  SELECT
    1.3 * AVG("catalog_sales"."cs_ext_discount_amt") AS "_col_0",
    "catalog_sales"."cs_item_sk" AS "_u_1"
  FROM "catalog_sales_2" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_item_sk"
)
SELECT
  SUM("catalog_sales"."cs_ext_discount_amt") AS "excess discount amount"
FROM "catalog_sales_2" AS "catalog_sales"
JOIN "date_dim_2" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk" AND "item"."i_manufact_id" = 610
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "item"."i_item_sk"
WHERE
  "_u_0"."_col_0" < "catalog_sales"."cs_ext_discount_amt"
LIMIT 100;

--------------------------------------
-- TPC-DS 33
--------------------------------------
WITH ss
     AS (SELECT i_manufact_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
     cs
     AS (SELECT i_manufact_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
     ws
     AS (SELECT i_manufact_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id)
SELECT i_manufact_id,
               Sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_manufact_id
ORDER  BY total_sales
LIMIT 100;
WITH "customer_address_2" AS (
  SELECT
    "customer_address"."ca_address_sk" AS "ca_address_sk",
    "customer_address"."ca_gmt_offset" AS "ca_gmt_offset"
  FROM "customer_address" AS "customer_address"
  WHERE
    "customer_address"."ca_gmt_offset" = -5
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 3 AND "date_dim"."d_year" = 1999
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_manufact_id" AS "i_manufact_id"
  FROM "item" AS "item"
), "_u_0" AS (
  SELECT
    "item"."i_manufact_id" AS "i_manufact_id"
  FROM "item" AS "item"
  WHERE
    "item"."i_category" IN ('Books')
  GROUP BY
    "item"."i_manufact_id"
), "ss" AS (
  SELECT
    "item"."i_manufact_id" AS "i_manufact_id",
    SUM("store_sales"."ss_ext_sales_price") AS "total_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."i_manufact_id" = "item"."i_manufact_id"
  WHERE
    NOT "_u_0"."i_manufact_id" IS NULL
  GROUP BY
    "item"."i_manufact_id"
), "cs" AS (
  SELECT
    "item"."i_manufact_id" AS "i_manufact_id",
    SUM("catalog_sales"."cs_ext_sales_price") AS "total_sales"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "catalog_sales"."cs_bill_addr_sk" = "customer_address"."ca_address_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  LEFT JOIN "_u_0" AS "_u_1"
    ON "_u_1"."i_manufact_id" = "item"."i_manufact_id"
  WHERE
    NOT "_u_1"."i_manufact_id" IS NULL
  GROUP BY
    "item"."i_manufact_id"
), "ws" AS (
  SELECT
    "item"."i_manufact_id" AS "i_manufact_id",
    SUM("web_sales"."ws_ext_sales_price") AS "total_sales"
  FROM "web_sales" AS "web_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "web_sales"."ws_bill_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "_u_2"."i_manufact_id" = "item"."i_manufact_id"
  WHERE
    NOT "_u_2"."i_manufact_id" IS NULL
  GROUP BY
    "item"."i_manufact_id"
), "tmp1" AS (
  SELECT
    "ss"."i_manufact_id" AS "i_manufact_id",
    "ss"."total_sales" AS "total_sales"
  FROM "ss"
  UNION ALL
  SELECT
    "cs"."i_manufact_id" AS "i_manufact_id",
    "cs"."total_sales" AS "total_sales"
  FROM "cs"
  UNION ALL
  SELECT
    "ws"."i_manufact_id" AS "i_manufact_id",
    "ws"."total_sales" AS "total_sales"
  FROM "ws"
)
SELECT
  "tmp1"."i_manufact_id" AS "i_manufact_id",
  SUM("tmp1"."total_sales") AS "total_sales"
FROM "tmp1" AS "tmp1"
GROUP BY
  "tmp1"."i_manufact_id"
ORDER BY
  "total_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 34
--------------------------------------
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               Count(*) cnt
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ( date_dim.d_dom BETWEEN 1 AND 3
                      OR date_dim.d_dom BETWEEN 25 AND 28 )
               AND ( household_demographics.hd_buy_potential = '>10000'
                      OR household_demographics.hd_buy_potential = 'unknown' )
               AND household_demographics.hd_vehicle_count > 0
               AND ( CASE
                       WHEN household_demographics.hd_vehicle_count > 0 THEN
                       household_demographics.hd_dep_count /
                       household_demographics.hd_vehicle_count
                       ELSE NULL
                     END ) > 1.2
               AND date_dim.d_year IN ( 1999, 1999 + 1, 1999 + 2 )
               AND store.s_county IN ( 'Williamson County', 'Williamson County',
                                       'Williamson County',
                                                             'Williamson County'
                                       ,
                                       'Williamson County', 'Williamson County',
                                           'Williamson County',
                                                             'Williamson County'
                                     )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dn,
       customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 15 AND 20
ORDER  BY c_last_name,
          c_first_name,
          c_salutation,
          c_preferred_cust_flag DESC;
WITH "dn" AS (
  SELECT
    "store_sales"."ss_ticket_number" AS "ss_ticket_number",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    COUNT(*) AS "cnt"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_year" IN (1999, 2000, 2001)
    AND (
      (
        "date_dim"."d_dom" <= 28 AND "date_dim"."d_dom" >= 25
      )
      OR (
        "date_dim"."d_dom" <= 3 AND "date_dim"."d_dom" >= 1
      )
    )
  JOIN "household_demographics" AS "household_demographics"
    ON (
      "household_demographics"."hd_buy_potential" = '>10000'
      OR "household_demographics"."hd_buy_potential" = 'unknown'
    )
    AND "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND "household_demographics"."hd_vehicle_count" > 0
    AND CASE
      WHEN "household_demographics"."hd_vehicle_count" > 0
      THEN "household_demographics"."hd_dep_count" / "household_demographics"."hd_vehicle_count"
      ELSE NULL
    END > 1.2
  JOIN "store" AS "store"
    ON "store"."s_county" IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
    AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store_sales"."ss_ticket_number",
    "store_sales"."ss_customer_sk"
)
SELECT
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_first_name" AS "c_first_name",
  "customer"."c_salutation" AS "c_salutation",
  "customer"."c_preferred_cust_flag" AS "c_preferred_cust_flag",
  "dn"."ss_ticket_number" AS "ss_ticket_number",
  "dn"."cnt" AS "cnt"
FROM "dn" AS "dn"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "dn"."ss_customer_sk"
WHERE
  "dn"."cnt" <= 20 AND "dn"."cnt" >= 15
ORDER BY
  "c_last_name",
  "c_first_name",
  "c_salutation",
  "c_preferred_cust_flag" DESC;

--------------------------------------
-- TPC-DS 35
--------------------------------------
SELECT ca_state,
               cd_gender,
               cd_marital_status,
               cd_dep_count,
               Count(*) cnt1,
               Stddev_samp(cd_dep_count),
               Avg(cd_dep_count),
               Max(cd_dep_count),
               cd_dep_employed_count,
               Count(*) cnt2,
               Stddev_samp(cd_dep_employed_count),
               Avg(cd_dep_employed_count),
               Max(cd_dep_employed_count),
               cd_dep_college_count,
               Count(*) cnt3,
               Stddev_samp(cd_dep_college_count),
               Avg(cd_dep_college_count),
               Max(cd_dep_college_count)
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2001
                          AND d_qoy < 4)
       AND ( EXISTS (SELECT *
                     FROM   web_sales,
                            date_dim
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 2001
                            AND d_qoy < 4)
              OR EXISTS (SELECT *
                         FROM   catalog_sales,
                                date_dim
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 2001
                                AND d_qoy < 4) )
GROUP  BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
ORDER  BY ca_state,
          cd_gender,
          cd_marital_status,
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
    "date_dim"."d_qoy" < 4 AND "date_dim"."d_year" = 2001
), "_u_0" AS (
  SELECT
    "store_sales"."ss_customer_sk" AS "_u_1"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_customer_sk"
), "_u_2" AS (
  SELECT
    "web_sales"."ws_bill_customer_sk" AS "_u_3"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "web_sales"."ws_bill_customer_sk"
), "_u_4" AS (
  SELECT
    "catalog_sales"."cs_ship_customer_sk" AS "_u_5"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_ship_customer_sk"
)
SELECT
  "ca"."ca_state" AS "ca_state",
  "customer_demographics"."cd_gender" AS "cd_gender",
  "customer_demographics"."cd_marital_status" AS "cd_marital_status",
  "customer_demographics"."cd_dep_count" AS "cd_dep_count",
  COUNT(*) AS "cnt1",
  STDDEV_SAMP("customer_demographics"."cd_dep_count") AS "_col_5",
  AVG("customer_demographics"."cd_dep_count") AS "_col_6",
  MAX("customer_demographics"."cd_dep_count") AS "_col_7",
  "customer_demographics"."cd_dep_employed_count" AS "cd_dep_employed_count",
  COUNT(*) AS "cnt2",
  STDDEV_SAMP("customer_demographics"."cd_dep_employed_count") AS "_col_10",
  AVG("customer_demographics"."cd_dep_employed_count") AS "_col_11",
  MAX("customer_demographics"."cd_dep_employed_count") AS "_col_12",
  "customer_demographics"."cd_dep_college_count" AS "cd_dep_college_count",
  COUNT(*) AS "cnt3",
  STDDEV_SAMP("customer_demographics"."cd_dep_college_count") AS "_col_15",
  AVG("customer_demographics"."cd_dep_college_count") AS "_col_16",
  MAX("customer_demographics"."cd_dep_college_count") AS "_col_17"
FROM "customer" AS "c"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "c"."c_customer_sk"
LEFT JOIN "_u_2" AS "_u_2"
  ON "_u_2"."_u_3" = "c"."c_customer_sk"
LEFT JOIN "_u_4" AS "_u_4"
  ON "_u_4"."_u_5" = "c"."c_customer_sk"
JOIN "customer_address" AS "ca"
  ON "c"."c_current_addr_sk" = "ca"."ca_address_sk"
JOIN "customer_demographics" AS "customer_demographics"
  ON "c"."c_current_cdemo_sk" = "customer_demographics"."cd_demo_sk"
WHERE
  NOT "_u_0"."_u_1" IS NULL
  AND (
    NOT "_u_2"."_u_3" IS NULL OR NOT "_u_4"."_u_5" IS NULL
  )
GROUP BY
  "ca"."ca_state",
  "customer_demographics"."cd_gender",
  "customer_demographics"."cd_marital_status",
  "customer_demographics"."cd_dep_count",
  "customer_demographics"."cd_dep_employed_count",
  "customer_demographics"."cd_dep_college_count"
ORDER BY
  "ca_state",
  "cd_gender",
  "cd_marital_status",
  "cd_dep_count",
  "cd_dep_employed_count",
  "cd_dep_college_count"
LIMIT 100;

--------------------------------------
-- TPC-DS 36
--------------------------------------
SELECT Sum(ss_net_profit) / Sum(ss_ext_sales_price)                 AS
               gross_margin,
               i_category,
               i_class,
               Grouping(i_category) + Grouping(i_class)                     AS
               lochierarchy,
               Rank()
                 OVER (
                   partition BY Grouping(i_category)+Grouping(i_class), CASE
                 WHEN Grouping(
                 i_class) = 0 THEN i_category END
                   ORDER BY Sum(ss_net_profit)/Sum(ss_ext_sales_price) ASC) AS
               rank_within_parent
FROM   store_sales,
       date_dim d1,
       item,
       store
WHERE  d1.d_year = 2000
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN',
                        'TN', 'TN', 'TN', 'TN' )
GROUP  BY rollup( i_category, i_class )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN i_category
          END,
          rank_within_parent
LIMIT 100;
SELECT
  SUM("store_sales"."ss_net_profit") / SUM("store_sales"."ss_ext_sales_price") AS "gross_margin",
  "item"."i_category" AS "i_category",
  "item"."i_class" AS "i_class",
  GROUPING("item"."i_category") + GROUPING("item"."i_class") AS "lochierarchy",
  RANK() OVER (PARTITION BY GROUPING("item"."i_category") + GROUPING("item"."i_class"), CASE WHEN GROUPING("item"."i_class") = 0 THEN "item"."i_category" END ORDER BY SUM("store_sales"."ss_net_profit") / SUM("store_sales"."ss_ext_sales_price")) AS "rank_within_parent"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk" AND "d1"."d_year" = 2000
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "store" AS "store"
  ON "store"."s_state" IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN')
  AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
GROUP BY
ROLLUP (
  "item"."i_category",
  "item"."i_class"
)
ORDER BY
  "lochierarchy" DESC,
  CASE WHEN "lochierarchy" = 0 THEN "i_category" END,
  "rank_within_parent"
LIMIT 100;

--------------------------------------
-- TPC-DS 37
--------------------------------------
SELECT
         i_item_id ,
         i_item_desc ,
         i_current_price
FROM     item,
         inventory,
         date_dim,
         catalog_sales
WHERE    i_current_price BETWEEN 20 AND      20 + 30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      d_date BETWEEN Cast('1999-03-06' AS DATE) AND      (
                  Cast('1999-03-06' AS DATE) + INTERVAL '60' day)
AND      i_manufact_id IN (843,815,850,840)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      cs_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "item"."i_current_price" AS "i_current_price"
FROM "item" AS "item"
JOIN "catalog_sales" AS "catalog_sales"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
JOIN "inventory" AS "inventory"
  ON "inventory"."inv_item_sk" = "item"."i_item_sk"
  AND "inventory"."inv_quantity_on_hand" <= 500
  AND "inventory"."inv_quantity_on_hand" >= 100
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "inventory"."inv_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('1999-05-05' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('1999-03-06' AS DATE)
WHERE
  "item"."i_current_price" <= 50
  AND "item"."i_current_price" >= 20
  AND "item"."i_manufact_id" IN (843, 815, 850, 840)
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "item"."i_current_price"
ORDER BY
  "i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 38
--------------------------------------
SELECT Count(*)
FROM   (SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   store_sales,
               date_dim,
               customer
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_customer_sk = customer.c_customer_sk
               AND d_month_seq BETWEEN 1188 AND 1188 + 11
        INTERSECT
        SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   catalog_sales,
               date_dim,
               customer
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
               AND d_month_seq BETWEEN 1188 AND 1188 + 11
        INTERSECT
        SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   web_sales,
               date_dim,
               customer
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
               AND d_month_seq BETWEEN 1188 AND 1188 + 11) hot_cust
LIMIT 100;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_first_name" AS "c_first_name",
    "customer"."c_last_name" AS "c_last_name"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date",
    "date_dim"."d_month_seq" AS "d_month_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_month_seq" <= 1199 AND "date_dim"."d_month_seq" >= 1188
), "hot_cust" AS (
  SELECT DISTINCT
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_first_name" AS "c_first_name",
    "date_dim"."d_date" AS "d_date"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  INTERSECT
  SELECT DISTINCT
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_first_name" AS "c_first_name",
    "date_dim"."d_date" AS "d_date"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "customer_2" AS "customer"
    ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  INTERSECT
  SELECT DISTINCT
    "customer"."c_last_name" AS "c_last_name",
    "customer"."c_first_name" AS "c_first_name",
    "date_dim"."d_date" AS "d_date"
  FROM "web_sales" AS "web_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
)
SELECT
  COUNT(*) AS "_col_0"
FROM "hot_cust" AS "hot_cust"
LIMIT 100;

--------------------------------------
-- TPC-DS 39
--------------------------------------
WITH inv
     AS (SELECT w_warehouse_name,
                w_warehouse_sk,
                i_item_sk,
                d_moy,
                stdev,
                mean,
                CASE mean
                  WHEN 0 THEN NULL
                  ELSE stdev / mean
                END cov
         FROM  (SELECT w_warehouse_name,
                       w_warehouse_sk,
                       i_item_sk,
                       d_moy,
                       Stddev_samp(inv_quantity_on_hand) stdev,
                       Avg(inv_quantity_on_hand)         mean
                FROM   inventory,
                       item,
                       warehouse,
                       date_dim
                WHERE  inv_item_sk = i_item_sk
                       AND inv_warehouse_sk = w_warehouse_sk
                       AND inv_date_sk = d_date_sk
                       AND d_year = 2002
                GROUP  BY w_warehouse_name,
                          w_warehouse_sk,
                          i_item_sk,
                          d_moy) foo
         WHERE  CASE mean
                  WHEN 0 THEN 0
                  ELSE stdev / mean
                END > 1)
SELECT inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
FROM   inv inv1,
       inv inv2
WHERE  inv1.i_item_sk = inv2.i_item_sk
       AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
       AND inv1.d_moy = 1
       AND inv2.d_moy = 1 + 1
ORDER  BY inv1.w_warehouse_sk,
          inv1.i_item_sk,
          inv1.d_moy,
          inv1.mean,
          inv1.cov,
          inv2.d_moy,
          inv2.mean,
          inv2.cov;
WITH "foo" AS (
  SELECT
    "warehouse"."w_warehouse_sk" AS "w_warehouse_sk",
    "item"."i_item_sk" AS "i_item_sk",
    "date_dim"."d_moy" AS "d_moy",
    STDDEV_SAMP("inventory"."inv_quantity_on_hand") AS "stdev",
    AVG("inventory"."inv_quantity_on_hand") AS "mean"
  FROM "inventory" AS "inventory"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "inventory"."inv_date_sk" AND "date_dim"."d_year" = 2002
  JOIN "item" AS "item"
    ON "inventory"."inv_item_sk" = "item"."i_item_sk"
  JOIN "warehouse" AS "warehouse"
    ON "inventory"."inv_warehouse_sk" = "warehouse"."w_warehouse_sk"
  GROUP BY
    "warehouse"."w_warehouse_name",
    "warehouse"."w_warehouse_sk",
    "item"."i_item_sk",
    "date_dim"."d_moy"
), "inv" AS (
  SELECT
    "foo"."w_warehouse_sk" AS "w_warehouse_sk",
    "foo"."i_item_sk" AS "i_item_sk",
    "foo"."d_moy" AS "d_moy",
    "foo"."mean" AS "mean",
    CASE WHEN "foo"."mean" = 0 THEN NULL ELSE "foo"."stdev" / "foo"."mean" END AS "cov"
  FROM "foo" AS "foo"
  WHERE
    CASE WHEN "foo"."mean" = 0 THEN 0 ELSE "foo"."stdev" / "foo"."mean" END > 1
)
SELECT
  "inv1"."w_warehouse_sk" AS "w_warehouse_sk",
  "inv1"."i_item_sk" AS "i_item_sk",
  "inv1"."d_moy" AS "d_moy",
  "inv1"."mean" AS "mean",
  "inv1"."cov" AS "cov",
  "inv2"."w_warehouse_sk" AS "w_warehouse_sk",
  "inv2"."i_item_sk" AS "i_item_sk",
  "inv2"."d_moy" AS "d_moy",
  "inv2"."mean" AS "mean",
  "inv2"."cov" AS "cov"
FROM "inv" AS "inv1"
JOIN "inv" AS "inv2"
  ON "inv1"."i_item_sk" = "inv2"."i_item_sk"
  AND "inv1"."w_warehouse_sk" = "inv2"."w_warehouse_sk"
  AND "inv2"."d_moy" = 2
WHERE
  "inv1"."d_moy" = 1
ORDER BY
  "inv1"."w_warehouse_sk",
  "inv1"."i_item_sk",
  "inv1"."d_moy",
  "inv1"."mean",
  "inv1"."cov",
  "inv2"."d_moy",
  "inv2"."mean",
  "inv2"."cov";

--------------------------------------
-- TPC-DS 40
--------------------------------------
SELECT
                w_state ,
                i_item_id ,
                Sum(
                CASE
                                WHEN (
                                                                Cast(d_date AS DATE) < Cast ('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash,0)
                                ELSE 0
                END) AS sales_before ,
                Sum(
                CASE
                                WHEN (
                                                                Cast(d_date AS DATE) >= Cast ('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash,0)
                                ELSE 0
                END) AS sales_after
FROM            catalog_sales
LEFT OUTER JOIN catalog_returns
ON              (
                                cs_order_number = cr_order_number
                AND             cs_item_sk = cr_item_sk) ,
                warehouse ,
                item ,
                date_dim
WHERE           i_current_price BETWEEN 0.99 AND             1.49
AND             i_item_sk = cs_item_sk
AND             cs_warehouse_sk = w_warehouse_sk
AND             cs_sold_date_sk = d_date_sk
AND             d_date BETWEEN (Cast ('2002-06-01' AS DATE) - INTERVAL '30' day) AND             (
                                cast ('2002-06-01' AS date) + INTERVAL '30' day)
GROUP BY        w_state,
                i_item_id
ORDER BY        w_state,
                i_item_id
LIMIT 100;
SELECT
  "warehouse"."w_state" AS "w_state",
  "item"."i_item_id" AS "i_item_id",
  SUM(
    CASE
      WHEN CAST("date_dim"."d_date" AS DATE) < CAST('2002-06-01' AS DATE)
      THEN "catalog_sales"."cs_sales_price" - COALESCE("catalog_returns"."cr_refunded_cash", 0)
      ELSE 0
    END
  ) AS "sales_before",
  SUM(
    CASE
      WHEN CAST("date_dim"."d_date" AS DATE) >= CAST('2002-06-01' AS DATE)
      THEN "catalog_sales"."cs_sales_price" - COALESCE("catalog_returns"."cr_refunded_cash", 0)
      ELSE 0
    END
  ) AS "sales_after"
FROM "catalog_sales" AS "catalog_sales"
LEFT JOIN "catalog_returns" AS "catalog_returns"
  ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
  AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('2002-07-01' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('2002-05-02' AS DATE)
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  AND "item"."i_current_price" <= 1.49
  AND "item"."i_current_price" >= 0.99
JOIN "warehouse" AS "warehouse"
  ON "catalog_sales"."cs_warehouse_sk" = "warehouse"."w_warehouse_sk"
GROUP BY
  "warehouse"."w_state",
  "item"."i_item_id"
ORDER BY
  "w_state",
  "i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 41
--------------------------------------
SELECT Distinct(i_product_name)
FROM   item i1
WHERE  i_manufact_id BETWEEN 765 AND 765 + 40
       AND (SELECT Count(*) AS item_cnt
            FROM   item
            WHERE  ( i_manufact = i1.i_manufact
                     AND ( ( i_category = 'Women'
                             AND ( i_color = 'dim'
                                    OR i_color = 'green' )
                             AND ( i_units = 'Gross'
                                    OR i_units = 'Dozen' )
                             AND ( i_size = 'economy'
                                    OR i_size = 'petite' ) )
                            OR ( i_category = 'Women'
                                 AND ( i_color = 'navajo'
                                        OR i_color = 'aquamarine' )
                                 AND ( i_units = 'Case'
                                        OR i_units = 'Unknown' )
                                 AND ( i_size = 'large'
                                        OR i_size = 'N/A' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'indian'
                                        OR i_color = 'dark' )
                                 AND ( i_units = 'Oz'
                                        OR i_units = 'Lb' )
                                 AND ( i_size = 'extra large'
                                        OR i_size = 'small' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'peach'
                                        OR i_color = 'purple' )
                                 AND ( i_units = 'Tbl'
                                        OR i_units = 'Bunch' )
                                 AND ( i_size = 'economy'
                                        OR i_size = 'petite' ) ) ) )
                    OR ( i_manufact = i1.i_manufact
                         AND ( ( i_category = 'Women'
                                 AND ( i_color = 'orchid'
                                        OR i_color = 'peru' )
                                 AND ( i_units = 'Carton'
                                        OR i_units = 'Cup' )
                                 AND ( i_size = 'economy'
                                        OR i_size = 'petite' ) )
                                OR ( i_category = 'Women'
                                     AND ( i_color = 'violet'
                                            OR i_color = 'papaya' )
                                     AND ( i_units = 'Ounce'
                                            OR i_units = 'Box' )
                                     AND ( i_size = 'large'
                                            OR i_size = 'N/A' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'drab'
                                            OR i_color = 'grey' )
                                     AND ( i_units = 'Each'
                                            OR i_units = 'N/A' )
                                     AND ( i_size = 'extra large'
                                            OR i_size = 'small' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'chocolate'
                                            OR i_color = 'antique' )
                                     AND ( i_units = 'Dram'
                                            OR i_units = 'Gram' )
                                     AND ( i_size = 'economy'
                                            OR i_size = 'petite' ) ) ) )) > 0
ORDER  BY i_product_name
LIMIT 100;
SELECT DISTINCT
  "i1"."i_product_name" AS "i_product_name"
FROM "item" AS "i1"
WHERE
  "i1"."i_manufact_id" <= 805
  AND "i1"."i_manufact_id" >= 765
  AND (
    SELECT
      COUNT(*) AS "item_cnt"
    FROM "item" AS "item"
    WHERE
      (
        "i1"."i_manufact" = "item"."i_manufact"
        AND (
          (
            "item"."i_category" = 'Men'
            AND (
              "item"."i_color" = 'antique' OR "item"."i_color" = 'chocolate'
            )
            AND (
              "item"."i_size" = 'economy' OR "item"."i_size" = 'petite'
            )
            AND (
              "item"."i_units" = 'Dram' OR "item"."i_units" = 'Gram'
            )
          )
          OR (
            "item"."i_category" = 'Men'
            AND (
              "item"."i_color" = 'drab' OR "item"."i_color" = 'grey'
            )
            AND (
              "item"."i_size" = 'extra large' OR "item"."i_size" = 'small'
            )
            AND (
              "item"."i_units" = 'Each' OR "item"."i_units" = 'N/A'
            )
          )
          OR (
            "item"."i_category" = 'Women'
            AND (
              "item"."i_color" = 'orchid' OR "item"."i_color" = 'peru'
            )
            AND (
              "item"."i_size" = 'economy' OR "item"."i_size" = 'petite'
            )
            AND (
              "item"."i_units" = 'Carton' OR "item"."i_units" = 'Cup'
            )
          )
          OR (
            "item"."i_category" = 'Women'
            AND (
              "item"."i_color" = 'papaya' OR "item"."i_color" = 'violet'
            )
            AND (
              "item"."i_size" = 'N/A' OR "item"."i_size" = 'large'
            )
            AND (
              "item"."i_units" = 'Box' OR "item"."i_units" = 'Ounce'
            )
          )
        )
      )
      OR (
        "i1"."i_manufact" = "item"."i_manufact"
        AND (
          (
            "item"."i_category" = 'Men'
            AND (
              "item"."i_color" = 'dark' OR "item"."i_color" = 'indian'
            )
            AND (
              "item"."i_size" = 'extra large' OR "item"."i_size" = 'small'
            )
            AND (
              "item"."i_units" = 'Lb' OR "item"."i_units" = 'Oz'
            )
          )
          OR (
            "item"."i_category" = 'Men'
            AND (
              "item"."i_color" = 'peach' OR "item"."i_color" = 'purple'
            )
            AND (
              "item"."i_size" = 'economy' OR "item"."i_size" = 'petite'
            )
            AND (
              "item"."i_units" = 'Bunch' OR "item"."i_units" = 'Tbl'
            )
          )
          OR (
            "item"."i_category" = 'Women'
            AND (
              "item"."i_color" = 'aquamarine' OR "item"."i_color" = 'navajo'
            )
            AND (
              "item"."i_size" = 'N/A' OR "item"."i_size" = 'large'
            )
            AND (
              "item"."i_units" = 'Case' OR "item"."i_units" = 'Unknown'
            )
          )
          OR (
            "item"."i_category" = 'Women'
            AND (
              "item"."i_color" = 'dim' OR "item"."i_color" = 'green'
            )
            AND (
              "item"."i_size" = 'economy' OR "item"."i_size" = 'petite'
            )
            AND (
              "item"."i_units" = 'Dozen' OR "item"."i_units" = 'Gross'
            )
          )
        )
      )
  ) > 0
ORDER BY
  "i1"."i_product_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 42
--------------------------------------
SELECT dt.d_year,
               item.i_category_id,
               item.i_category,
               Sum(ss_ext_sales_price)
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 12
       AND dt.d_year = 2000
GROUP  BY dt.d_year,
          item.i_category_id,
          item.i_category
ORDER  BY Sum(ss_ext_sales_price) DESC,
          dt.d_year,
          item.i_category_id,
          item.i_category
LIMIT 100;
SELECT
  "dt"."d_year" AS "d_year",
  "item"."i_category_id" AS "i_category_id",
  "item"."i_category" AS "i_category",
  SUM("store_sales"."ss_ext_sales_price") AS "_col_3"
FROM "date_dim" AS "dt"
JOIN "store_sales" AS "store_sales"
  ON "dt"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk" AND "item"."i_manager_id" = 1
WHERE
  "dt"."d_moy" = 12 AND "dt"."d_year" = 2000
GROUP BY
  "dt"."d_year",
  "item"."i_category_id",
  "item"."i_category"
ORDER BY
  "_col_3" DESC,
  "d_year",
  "i_category_id",
  "i_category"
LIMIT 100;

--------------------------------------
-- TPC-DS 43
--------------------------------------
SELECT s_store_name,
               s_store_id,
               Sum(CASE
                     WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price
                     ELSE NULL
                   END) sun_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price
                     ELSE NULL
                   END) mon_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price
                     ELSE NULL
                   END) tue_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price
                     ELSE NULL
                   END) wed_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price
                     ELSE NULL
                   END) thu_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price
                     ELSE NULL
                   END) fri_sales,
               Sum(CASE
                     WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price
                     ELSE NULL
                   END) sat_sales
FROM   date_dim,
       store_sales,
       store
WHERE  d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_gmt_offset = -5
       AND d_year = 2002
GROUP  BY s_store_name,
          s_store_id
ORDER  BY s_store_name,
          s_store_id,
          sun_sales,
          mon_sales,
          tue_sales,
          wed_sales,
          thu_sales,
          fri_sales,
          sat_sales
LIMIT 100;
SELECT
  "store"."s_store_name" AS "s_store_name",
  "store"."s_store_id" AS "s_store_id",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Sunday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "sun_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Monday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "mon_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Tuesday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "tue_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Wednesday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "wed_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Thursday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "thu_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Friday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "fri_sales",
  SUM(
    CASE
      WHEN "date_dim"."d_day_name" = 'Saturday'
      THEN "store_sales"."ss_sales_price"
      ELSE NULL
    END
  ) AS "sat_sales"
FROM "date_dim" AS "date_dim"
JOIN "store_sales" AS "store_sales"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "store" AS "store"
  ON "store"."s_gmt_offset" = -5 AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
WHERE
  "date_dim"."d_year" = 2002
GROUP BY
  "store"."s_store_name",
  "store"."s_store_id"
ORDER BY
  "s_store_name",
  "s_store_id",
  "sun_sales",
  "mon_sales",
  "tue_sales",
  "wed_sales",
  "thu_sales",
  "fri_sales",
  "sat_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 44
--------------------------------------
SELECT asceding.rnk,
               i1.i_product_name best_performing,
               i2.i_product_name worst_performing
FROM  (SELECT *
       FROM   (SELECT item_sk,
                      Rank()
                        OVER (
                          ORDER BY rank_col ASC) rnk
               FROM   (SELECT ss_item_sk         item_sk,
                              Avg(ss_net_profit) rank_col
                       FROM   store_sales ss1
                       WHERE  ss_store_sk = 4
                       GROUP  BY ss_item_sk
                       HAVING Avg(ss_net_profit) > 0.9 *
                              (SELECT Avg(ss_net_profit)
                                      rank_col
                               FROM   store_sales
                               WHERE  ss_store_sk = 4
                                      AND ss_cdemo_sk IS
                                          NULL
                               GROUP  BY ss_store_sk))V1)
              V11
       WHERE  rnk < 11) asceding,
      (SELECT *
       FROM   (SELECT item_sk,
                      Rank()
                        OVER (
                          ORDER BY rank_col DESC) rnk
               FROM   (SELECT ss_item_sk         item_sk,
                              Avg(ss_net_profit) rank_col
                       FROM   store_sales ss1
                       WHERE  ss_store_sk = 4
                       GROUP  BY ss_item_sk
                       HAVING Avg(ss_net_profit) > 0.9 *
                              (SELECT Avg(ss_net_profit)
                                      rank_col
                               FROM   store_sales
                               WHERE  ss_store_sk = 4
                                      AND ss_cdemo_sk IS
                                          NULL
                               GROUP  BY ss_store_sk))V2)
              V21
       WHERE  rnk < 11) descending,
      item i1,
      item i2
WHERE  asceding.rnk = descending.rnk
       AND i1.i_item_sk = asceding.item_sk
       AND i2.i_item_sk = descending.item_sk
ORDER  BY asceding.rnk
LIMIT 100;
WITH "_u_0" AS (
  SELECT
    AVG("store_sales"."ss_net_profit") AS "rank_col"
  FROM "store_sales" AS "store_sales"
  WHERE
    "store_sales"."ss_cdemo_sk" IS NULL AND "store_sales"."ss_store_sk" = 4
  GROUP BY
    "store_sales"."ss_store_sk"
), "v1" AS (
  SELECT
    "ss1"."ss_item_sk" AS "item_sk",
    AVG("ss1"."ss_net_profit") AS "rank_col"
  FROM "store_sales" AS "ss1"
  CROSS JOIN "_u_0" AS "_u_0"
  WHERE
    "ss1"."ss_store_sk" = 4
  GROUP BY
    "ss1"."ss_item_sk"
  HAVING
    0.9 * MAX("_u_0"."rank_col") < AVG("ss1"."ss_net_profit")
), "v11" AS (
  SELECT
    "v1"."item_sk" AS "item_sk",
    RANK() OVER (ORDER BY "v1"."rank_col") AS "rnk"
  FROM "v1" AS "v1"
), "v2" AS (
  SELECT
    "ss1"."ss_item_sk" AS "item_sk",
    AVG("ss1"."ss_net_profit") AS "rank_col"
  FROM "store_sales" AS "ss1"
  CROSS JOIN "_u_0" AS "_u_1"
  WHERE
    "ss1"."ss_store_sk" = 4
  GROUP BY
    "ss1"."ss_item_sk"
  HAVING
    0.9 * MAX("_u_1"."rank_col") < AVG("ss1"."ss_net_profit")
), "v21" AS (
  SELECT
    "v2"."item_sk" AS "item_sk",
    RANK() OVER (ORDER BY "v2"."rank_col" DESC) AS "rnk"
  FROM "v2" AS "v2"
)
SELECT
  "v11"."rnk" AS "rnk",
  "i1"."i_product_name" AS "best_performing",
  "i2"."i_product_name" AS "worst_performing"
FROM "v11" AS "v11"
JOIN "v21" AS "v21"
  ON "v11"."rnk" = "v21"."rnk" AND "v21"."rnk" < 11
JOIN "item" AS "i1"
  ON "i1"."i_item_sk" = "v11"."item_sk"
JOIN "item" AS "i2"
  ON "i2"."i_item_sk" = "v21"."item_sk"
WHERE
  "v11"."rnk" < 11
ORDER BY
  "v11"."rnk"
LIMIT 100;

--------------------------------------
-- TPC-DS 45
--------------------------------------
SELECT ca_zip,
               ca_state,
               Sum(ws_sales_price)
FROM   web_sales,
       customer,
       customer_address,
       date_dim,
       item
WHERE  ws_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ws_item_sk = i_item_sk
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
              OR i_item_id IN (SELECT i_item_id
                               FROM   item
                               WHERE  i_item_sk IN ( 2, 3, 5, 7,
                                                     11, 13, 17, 19,
                                                     23, 29 )) )
       AND ws_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 2000
GROUP  BY ca_zip,
          ca_state
ORDER  BY ca_zip,
          ca_state
LIMIT 100;
WITH "_u_0" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
  WHERE
    "item"."i_item_sk" IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
  GROUP BY
    "item"."i_item_id"
)
SELECT
  "customer_address"."ca_zip" AS "ca_zip",
  "customer_address"."ca_state" AS "ca_state",
  SUM("web_sales"."ws_sales_price") AS "_col_2"
FROM "web_sales" AS "web_sales"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  AND "date_dim"."d_qoy" = 1
  AND "date_dim"."d_year" = 2000
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."i_item_id" = "item"."i_item_id"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
WHERE
  NOT "_u_0"."i_item_id" IS NULL
  OR SUBSTR("customer_address"."ca_zip", 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
GROUP BY
  "customer_address"."ca_zip",
  "customer_address"."ca_state"
ORDER BY
  "ca_zip",
  "ca_state"
LIMIT 100;

--------------------------------------
-- TPC-DS 46
--------------------------------------
SELECT c_last_name,
               c_first_name,
               ca_city,
               bought_city,
               ss_ticket_number,
               amt,
               profit
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               ca_city            bought_city,
               Sum(ss_coupon_amt) amt,
               Sum(ss_net_profit) profit
        FROM   store_sales,
               date_dim,
               store,
               household_demographics,
               customer_address
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk
               AND ( household_demographics.hd_dep_count = 6
                      OR household_demographics.hd_vehicle_count = 0 )
               AND date_dim.d_dow IN ( 6, 0 )
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 )
               AND store.s_city IN ( 'Midway', 'Fairview', 'Fairview',
                                     'Fairview',
                                     'Fairview' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  ca_city) dn,
       customer,
       customer_address current_addr
WHERE  ss_customer_sk = c_customer_sk
       AND customer.c_current_addr_sk = current_addr.ca_address_sk
       AND current_addr.ca_city <> bought_city
ORDER  BY c_last_name,
          c_first_name,
          ca_city,
          bought_city,
          ss_ticket_number
LIMIT 100;
WITH "dn" AS (
  SELECT
    "store_sales"."ss_ticket_number" AS "ss_ticket_number",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    "customer_address"."ca_city" AS "bought_city",
    SUM("store_sales"."ss_coupon_amt") AS "amt",
    SUM("store_sales"."ss_net_profit") AS "profit"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_dow" IN (6, 0)
    AND "date_dim"."d_year" IN (2000, 2001, 2002)
  JOIN "household_demographics" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND (
      "household_demographics"."hd_dep_count" = 6
      OR "household_demographics"."hd_vehicle_count" = 0
    )
  JOIN "store" AS "store"
    ON "store"."s_city" IN ('Midway', 'Fairview', 'Fairview', 'Fairview', 'Fairview')
    AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store_sales"."ss_ticket_number",
    "store_sales"."ss_customer_sk",
    "store_sales"."ss_addr_sk",
    "customer_address"."ca_city"
)
SELECT
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_first_name" AS "c_first_name",
  "current_addr"."ca_city" AS "ca_city",
  "dn"."bought_city" AS "bought_city",
  "dn"."ss_ticket_number" AS "ss_ticket_number",
  "dn"."amt" AS "amt",
  "dn"."profit" AS "profit"
FROM "dn" AS "dn"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "dn"."ss_customer_sk"
JOIN "customer_address" AS "current_addr"
  ON "current_addr"."ca_address_sk" = "customer"."c_current_addr_sk"
  AND "current_addr"."ca_city" <> "dn"."bought_city"
ORDER BY
  "c_last_name",
  "c_first_name",
  "ca_city",
  "bought_city",
  "ss_ticket_number"
LIMIT 100;

--------------------------------------
-- TPC-DS 47
--------------------------------------
WITH v1
     AS (SELECT i_category,
                i_brand,
                s_store_name,
                s_company_name,
                d_year,
                d_moy,
                Sum(ss_sales_price)         sum_sales,
                Avg(Sum(ss_sales_price))
                  OVER (
                    partition BY i_category, i_brand, s_store_name,
                  s_company_name,
                  d_year)
                                            avg_monthly_sales,
                Rank()
                  OVER (
                    partition BY i_category, i_brand, s_store_name,
                  s_company_name
                    ORDER BY d_year, d_moy) rn
         FROM   item,
                store_sales,
                date_dim,
                store
         WHERE  ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND ss_store_sk = s_store_sk
                AND ( d_year = 1999
                       OR ( d_year = 1999 - 1
                            AND d_moy = 12 )
                       OR ( d_year = 1999 + 1
                            AND d_moy = 1 ) )
         GROUP  BY i_category,
                   i_brand,
                   s_store_name,
                   s_company_name,
                   d_year,
                   d_moy),
     v2
     AS (SELECT v1.i_category,
                v1.d_year,
                v1.d_moy,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales  psum,
                v1_lead.sum_sales nsum
         FROM   v1,
                v1 v1_lag,
                v1 v1_lead
         WHERE  v1.i_category = v1_lag.i_category
                AND v1.i_category = v1_lead.i_category
                AND v1.i_brand = v1_lag.i_brand
                AND v1.i_brand = v1_lead.i_brand
                AND v1.s_store_name = v1_lag.s_store_name
                AND v1.s_store_name = v1_lead.s_store_name
                AND v1.s_company_name = v1_lag.s_company_name
                AND v1.s_company_name = v1_lead.s_company_name
                AND v1.rn = v1_lag.rn + 1
                AND v1.rn = v1_lead.rn - 1)
SELECT *
FROM   v2
WHERE  d_year = 1999
       AND avg_monthly_sales > 0
       AND CASE
             WHEN avg_monthly_sales > 0 THEN Abs(sum_sales - avg_monthly_sales)
                                             /
                                             avg_monthly_sales
             ELSE NULL
           END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          3
LIMIT 100;
WITH "v1" AS (
  SELECT
    "item"."i_category" AS "i_category",
    "item"."i_brand" AS "i_brand",
    "store"."s_store_name" AS "s_store_name",
    "store"."s_company_name" AS "s_company_name",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy",
    SUM("store_sales"."ss_sales_price") AS "sum_sales",
    AVG(SUM("store_sales"."ss_sales_price")) OVER (PARTITION BY "item"."i_category", "item"."i_brand", "store"."s_store_name", "store"."s_company_name", "date_dim"."d_year") AS "avg_monthly_sales",
    RANK() OVER (PARTITION BY "item"."i_category", "item"."i_brand", "store"."s_store_name", "store"."s_company_name" ORDER BY "date_dim"."d_year", "date_dim"."d_moy") AS "rn"
  FROM "item" AS "item"
  JOIN "store_sales" AS "store_sales"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND (
      "date_dim"."d_moy" = 1 OR "date_dim"."d_moy" = 12 OR "date_dim"."d_year" = 1999
    )
    AND (
      "date_dim"."d_moy" = 1 OR "date_dim"."d_year" = 1998 OR "date_dim"."d_year" = 1999
    )
    AND (
      "date_dim"."d_moy" = 12 OR "date_dim"."d_year" = 1999 OR "date_dim"."d_year" = 2000
    )
    AND (
      "date_dim"."d_year" = 1998 OR "date_dim"."d_year" = 1999 OR "date_dim"."d_year" = 2000
    )
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "item"."i_category",
    "item"."i_brand",
    "store"."s_store_name",
    "store"."s_company_name",
    "date_dim"."d_year",
    "date_dim"."d_moy"
)
SELECT
  "v1"."i_category" AS "i_category",
  "v1"."d_year" AS "d_year",
  "v1"."d_moy" AS "d_moy",
  "v1"."avg_monthly_sales" AS "avg_monthly_sales",
  "v1"."sum_sales" AS "sum_sales",
  "v1_lag"."sum_sales" AS "psum",
  "v1_lead"."sum_sales" AS "nsum"
FROM "v1"
JOIN "v1" AS "v1_lag"
  ON "v1"."i_brand" = "v1_lag"."i_brand"
  AND "v1"."i_category" = "v1_lag"."i_category"
  AND "v1"."rn" = "v1_lag"."rn" + 1
  AND "v1"."s_company_name" = "v1_lag"."s_company_name"
  AND "v1"."s_store_name" = "v1_lag"."s_store_name"
JOIN "v1" AS "v1_lead"
  ON "v1"."i_brand" = "v1_lead"."i_brand"
  AND "v1"."i_category" = "v1_lead"."i_category"
  AND "v1"."rn" = "v1_lead"."rn" - 1
  AND "v1"."s_company_name" = "v1_lead"."s_company_name"
  AND "v1"."s_store_name" = "v1_lead"."s_store_name"
WHERE
  "v1"."avg_monthly_sales" > 0
  AND "v1"."d_year" = 1999
  AND CASE
    WHEN "v1"."avg_monthly_sales" > 0
    THEN ABS("v1"."sum_sales" - "v1"."avg_monthly_sales") / "v1"."avg_monthly_sales"
    ELSE NULL
  END > 0.1
ORDER BY
  "v1"."sum_sales" - "v1"."avg_monthly_sales",
  "d_moy"
LIMIT 100;

--------------------------------------
-- TPC-DS 48
--------------------------------------
SELECT Sum (ss_quantity)
FROM   store_sales,
       store,
       customer_demographics,
       customer_address,
       date_dim
WHERE  s_store_sk = ss_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 1999
       AND ( ( cd_demo_sk = ss_cdemo_sk
               AND cd_marital_status = 'W'
               AND cd_education_status = 'Secondary'
               AND ss_sales_price BETWEEN 100.00 AND 150.00 )
              OR ( cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'M'
                   AND cd_education_status = 'Advanced Degree'
                   AND ss_sales_price BETWEEN 50.00 AND 100.00 )
              OR ( cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'D'
                   AND cd_education_status = '2 yr Degree'
                   AND ss_sales_price BETWEEN 150.00 AND 200.00 ) )
       AND ( ( ss_addr_sk = ca_address_sk
               AND ca_country = 'United States'
               AND ca_state IN ( 'TX', 'NE', 'MO' )
               AND ss_net_profit BETWEEN 0 AND 2000 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'CO', 'TN', 'ND' )
                   AND ss_net_profit BETWEEN 150 AND 3000 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'OK', 'PA', 'CA' )
                   AND ss_net_profit BETWEEN 50 AND 25000 ) );
SELECT
  SUM("store_sales"."ss_quantity") AS "_col_0"
FROM "store_sales" AS "store_sales"
JOIN "customer_address" AS "customer_address"
  ON (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('CO', 'TN', 'ND')
    AND "store_sales"."ss_net_profit" <= 3000
    AND "store_sales"."ss_net_profit" >= 150
  )
  OR (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('OK', 'PA', 'CA')
    AND "store_sales"."ss_net_profit" <= 25000
    AND "store_sales"."ss_net_profit" >= 50
  )
  OR (
    "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
    AND "customer_address"."ca_country" = 'United States'
    AND "customer_address"."ca_state" IN ('TX', 'NE', 'MO')
    AND "store_sales"."ss_net_profit" <= 2000
    AND "store_sales"."ss_net_profit" >= 0
  )
JOIN "customer_demographics" AS "customer_demographics"
  ON (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = '2 yr Degree'
    AND "customer_demographics"."cd_marital_status" = 'D'
    AND "store_sales"."ss_sales_price" <= 200.00
    AND "store_sales"."ss_sales_price" >= 150.00
  )
  OR (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = 'Advanced Degree'
    AND "customer_demographics"."cd_marital_status" = 'M'
    AND "store_sales"."ss_sales_price" <= 100.00
    AND "store_sales"."ss_sales_price" >= 50.00
  )
  OR (
    "customer_demographics"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "customer_demographics"."cd_education_status" = 'Secondary'
    AND "customer_demographics"."cd_marital_status" = 'W'
    AND "store_sales"."ss_sales_price" <= 150.00
    AND "store_sales"."ss_sales_price" >= 100.00
  )
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "date_dim"."d_year" = 1999
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk";

--------------------------------------
-- TPC-DS 49
--------------------------------------
SELECT 'web' AS channel,
               web.item,
               web.return_ratio,
               web.return_rank,
               web.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               Rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               Rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT ws.ws_item_sk                                       AS
                       item,
                       ( Cast(Sum(COALESCE(wr.wr_return_quantity, 0)) AS DEC(15,
                              4)) /
                         Cast(
                         Sum(COALESCE(ws.ws_quantity, 0)) AS DEC(15, 4)) ) AS
                       return_ratio,
                       ( Cast(Sum(COALESCE(wr.wr_return_amt, 0)) AS DEC(15, 4))
                         / Cast(
                         Sum(
                         COALESCE(ws.ws_net_paid, 0)) AS DEC(15,
                         4)) )                                             AS
                       currency_ratio
                FROM   web_sales ws
                       LEFT OUTER JOIN web_returns wr
                                    ON ( ws.ws_order_number = wr.wr_order_number
                                         AND ws.ws_item_sk = wr.wr_item_sk ),
                       date_dim
                WHERE  wr.wr_return_amt > 10000
                       AND ws.ws_net_profit > 1
                       AND ws.ws_net_paid > 0
                       AND ws.ws_quantity > 0
                       AND ws_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY ws.ws_item_sk) in_web) web
WHERE  ( web.return_rank <= 10
          OR web.currency_rank <= 10 )
UNION
SELECT 'catalog' AS channel,
       catalog.item,
       catalog.return_ratio,
       catalog.return_rank,
       catalog.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               Rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               Rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT cs.cs_item_sk                                       AS
                       item,
                       ( Cast(Sum(COALESCE(cr.cr_return_quantity, 0)) AS DEC(15,
                              4)) /
                         Cast(
                         Sum(COALESCE(cs.cs_quantity, 0)) AS DEC(15, 4)) ) AS
                       return_ratio,
                       ( Cast(Sum(COALESCE(cr.cr_return_amount, 0)) AS DEC(15, 4
                              )) /
                         Cast(Sum(
                         COALESCE(cs.cs_net_paid, 0)) AS DEC(
                         15, 4)) )                                         AS
                       currency_ratio
                FROM   catalog_sales cs
                       LEFT OUTER JOIN catalog_returns cr
                                    ON ( cs.cs_order_number = cr.cr_order_number
                                         AND cs.cs_item_sk = cr.cr_item_sk ),
                       date_dim
                WHERE  cr.cr_return_amount > 10000
                       AND cs.cs_net_profit > 1
                       AND cs.cs_net_paid > 0
                       AND cs.cs_quantity > 0
                       AND cs_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY cs.cs_item_sk) in_cat) catalog
WHERE  ( catalog.return_rank <= 10
          OR catalog.currency_rank <= 10 )
UNION
SELECT 'store' AS channel,
       store.item,
       store.return_ratio,
       store.return_rank,
       store.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               Rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               Rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT sts.ss_item_sk                                       AS
                       item,
                       ( Cast(Sum(COALESCE(sr.sr_return_quantity, 0)) AS DEC(15,
                              4)) /
                         Cast(
                         Sum(COALESCE(sts.ss_quantity, 0)) AS DEC(15, 4)) ) AS
                       return_ratio,
                       ( Cast(Sum(COALESCE(sr.sr_return_amt, 0)) AS DEC(15, 4))
                         / Cast(
                         Sum(
                         COALESCE(sts.ss_net_paid, 0)) AS DEC(15, 4)) )     AS
                       currency_ratio
                FROM   store_sales sts
                       LEFT OUTER JOIN store_returns sr
                                    ON ( sts.ss_ticket_number =
                                         sr.sr_ticket_number
                                         AND sts.ss_item_sk = sr.sr_item_sk ),
                       date_dim
                WHERE  sr.sr_return_amt > 10000
                       AND sts.ss_net_profit > 1
                       AND sts.ss_net_paid > 0
                       AND sts.ss_quantity > 0
                       AND ss_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY sts.ss_item_sk) in_store) store
WHERE  ( store.return_rank <= 10
          OR store.currency_rank <= 10 )
ORDER  BY 1,
          4,
          5
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 12 AND "date_dim"."d_year" = 1999
), "in_web" AS (
  SELECT
    "ws"."ws_item_sk" AS "item",
    CAST(SUM(COALESCE("wr"."wr_return_quantity", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("ws"."ws_quantity", 0)) AS DECIMAL(15, 4)) AS "return_ratio",
    CAST(SUM(COALESCE("wr"."wr_return_amt", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("ws"."ws_net_paid", 0)) AS DECIMAL(15, 4)) AS "currency_ratio"
  FROM "web_sales" AS "ws"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "ws"."ws_sold_date_sk"
  LEFT JOIN "web_returns" AS "wr"
    ON "wr"."wr_item_sk" = "ws"."ws_item_sk"
    AND "wr"."wr_order_number" = "ws"."ws_order_number"
  WHERE
    "wr"."wr_return_amt" > 10000
    AND "ws"."ws_net_paid" > 0
    AND "ws"."ws_net_profit" > 1
    AND "ws"."ws_quantity" > 0
  GROUP BY
    "ws"."ws_item_sk"
), "web" AS (
  SELECT
    "in_web"."item" AS "item",
    "in_web"."return_ratio" AS "return_ratio",
    RANK() OVER (ORDER BY "in_web"."return_ratio") AS "return_rank",
    RANK() OVER (ORDER BY "in_web"."currency_ratio") AS "currency_rank"
  FROM "in_web" AS "in_web"
), "in_cat" AS (
  SELECT
    "cs"."cs_item_sk" AS "item",
    CAST(SUM(COALESCE("cr"."cr_return_quantity", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("cs"."cs_quantity", 0)) AS DECIMAL(15, 4)) AS "return_ratio",
    CAST(SUM(COALESCE("cr"."cr_return_amount", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("cs"."cs_net_paid", 0)) AS DECIMAL(15, 4)) AS "currency_ratio"
  FROM "catalog_sales" AS "cs"
  LEFT JOIN "catalog_returns" AS "cr"
    ON "cr"."cr_item_sk" = "cs"."cs_item_sk"
    AND "cr"."cr_order_number" = "cs"."cs_order_number"
  JOIN "date_dim_2" AS "date_dim"
    ON "cs"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    "cr"."cr_return_amount" > 10000
    AND "cs"."cs_net_paid" > 0
    AND "cs"."cs_net_profit" > 1
    AND "cs"."cs_quantity" > 0
  GROUP BY
    "cs"."cs_item_sk"
), "catalog" AS (
  SELECT
    "in_cat"."item" AS "item",
    "in_cat"."return_ratio" AS "return_ratio",
    RANK() OVER (ORDER BY "in_cat"."return_ratio") AS "return_rank",
    RANK() OVER (ORDER BY "in_cat"."currency_ratio") AS "currency_rank"
  FROM "in_cat" AS "in_cat"
), "in_store" AS (
  SELECT
    "sts"."ss_item_sk" AS "item",
    CAST(SUM(COALESCE("sr"."sr_return_quantity", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("sts"."ss_quantity", 0)) AS DECIMAL(15, 4)) AS "return_ratio",
    CAST(SUM(COALESCE("sr"."sr_return_amt", 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE("sts"."ss_net_paid", 0)) AS DECIMAL(15, 4)) AS "currency_ratio"
  FROM "store_sales" AS "sts"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "sts"."ss_sold_date_sk"
  LEFT JOIN "store_returns" AS "sr"
    ON "sr"."sr_item_sk" = "sts"."ss_item_sk"
    AND "sr"."sr_ticket_number" = "sts"."ss_ticket_number"
  WHERE
    "sr"."sr_return_amt" > 10000
    AND "sts"."ss_net_paid" > 0
    AND "sts"."ss_net_profit" > 1
    AND "sts"."ss_quantity" > 0
  GROUP BY
    "sts"."ss_item_sk"
), "store" AS (
  SELECT
    "in_store"."item" AS "item",
    "in_store"."return_ratio" AS "return_ratio",
    RANK() OVER (ORDER BY "in_store"."return_ratio") AS "return_rank",
    RANK() OVER (ORDER BY "in_store"."currency_ratio") AS "currency_rank"
  FROM "in_store" AS "in_store"
)
SELECT
  'web' AS "channel",
  "web"."item" AS "item",
  "web"."return_ratio" AS "return_ratio",
  "web"."return_rank" AS "return_rank",
  "web"."currency_rank" AS "currency_rank"
FROM "web" AS "web"
WHERE
  "web"."currency_rank" <= 10 OR "web"."return_rank" <= 10
UNION
SELECT
  'catalog' AS "channel",
  "catalog"."item" AS "item",
  "catalog"."return_ratio" AS "return_ratio",
  "catalog"."return_rank" AS "return_rank",
  "catalog"."currency_rank" AS "currency_rank"
FROM "catalog" AS "catalog"
WHERE
  "catalog"."currency_rank" <= 10 OR "catalog"."return_rank" <= 10
UNION
SELECT
  'store' AS "channel",
  "store"."item" AS "item",
  "store"."return_ratio" AS "return_ratio",
  "store"."return_rank" AS "return_rank",
  "store"."currency_rank" AS "currency_rank"
FROM "store" AS "store"
WHERE
  "store"."currency_rank" <= 10 OR "store"."return_rank" <= 10
ORDER BY
  "channel",
  "return_rank",
  "currency_rank"
LIMIT 100;

--------------------------------------
-- TPC-DS 50
--------------------------------------
SELECT s_store_name,
               s_company_id,
               s_street_number,
               s_street_name,
               s_street_type,
               s_suite_number,
               s_city,
               s_county,
               s_state,
               s_zip,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS "30 days",
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 30 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 60 )
                   THEN 1
                     ELSE 0
                   END) AS "31-60 days",
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 60 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 90 )
                   THEN 1
                     ELSE 0
                   END) AS "61-90 days",
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 90 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 120 )
                   THEN 1
                     ELSE 0
                   END) AS "91-120 days",
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS ">120 days"
FROM   store_sales,
       store_returns,
       store,
       date_dim d1,
       date_dim d2
WHERE  d2.d_year = 2002
       AND d2.d_moy = 9
       AND ss_ticket_number = sr_ticket_number
       AND ss_item_sk = sr_item_sk
       AND ss_sold_date_sk = d1.d_date_sk
       AND sr_returned_date_sk = d2.d_date_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_store_sk = s_store_sk
GROUP  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip
ORDER  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip
LIMIT 100;
SELECT
  "store"."s_store_name" AS "s_store_name",
  "store"."s_company_id" AS "s_company_id",
  "store"."s_street_number" AS "s_street_number",
  "store"."s_street_name" AS "s_street_name",
  "store"."s_street_type" AS "s_street_type",
  "store"."s_suite_number" AS "s_suite_number",
  "store"."s_city" AS "s_city",
  "store"."s_county" AS "s_county",
  "store"."s_state" AS "s_state",
  "store"."s_zip" AS "s_zip",
  SUM(
    CASE
      WHEN "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" <= 30
      THEN 1
      ELSE 0
    END
  ) AS "30 days",
  SUM(
    CASE
      WHEN "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" <= 60
      AND "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" > 30
      THEN 1
      ELSE 0
    END
  ) AS "31-60 days",
  SUM(
    CASE
      WHEN "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" <= 90
      AND "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" > 60
      THEN 1
      ELSE 0
    END
  ) AS "61-90 days",
  SUM(
    CASE
      WHEN "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" <= 120
      AND "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" > 90
      THEN 1
      ELSE 0
    END
  ) AS "91-120 days",
  SUM(
    CASE
      WHEN "store_returns"."sr_returned_date_sk" - "store_sales"."ss_sold_date_sk" > 120
      THEN 1
      ELSE 0
    END
  ) AS ">120 days"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_customer_sk" = "store_sales"."ss_customer_sk"
  AND "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
  AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
JOIN "date_dim" AS "d2"
  ON "d2"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  AND "d2"."d_moy" = 9
  AND "d2"."d_year" = 2002
GROUP BY
  "store"."s_store_name",
  "store"."s_company_id",
  "store"."s_street_number",
  "store"."s_street_name",
  "store"."s_street_type",
  "store"."s_suite_number",
  "store"."s_city",
  "store"."s_county",
  "store"."s_state",
  "store"."s_zip"
ORDER BY
  "s_store_name",
  "s_company_id",
  "s_street_number",
  "s_street_name",
  "s_street_type",
  "s_suite_number",
  "s_city",
  "s_county",
  "s_state",
  "s_zip"
LIMIT 100;

--------------------------------------
-- TPC-DS 51
--------------------------------------
WITH web_v1 AS
(
         SELECT   ws_item_sk item_sk,
                  d_date,
                  sum(Sum(ws_sales_price)) OVER (partition BY ws_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row) cume_sales
         FROM     web_sales ,
                  date_dim
         WHERE    ws_sold_date_sk=d_date_sk
         AND      d_month_seq BETWEEN 1192 AND      1192+11
         AND      ws_item_sk IS NOT NULL
         GROUP BY ws_item_sk,
                  d_date), store_v1 AS
(
         SELECT   ss_item_sk item_sk,
                  d_date,
                  sum(sum(ss_sales_price)) OVER (partition BY ss_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row) cume_sales
         FROM     store_sales ,
                  date_dim
         WHERE    ss_sold_date_sk=d_date_sk
         AND      d_month_seq BETWEEN 1192 AND      1192+11
         AND      ss_item_sk IS NOT NULL
         GROUP BY ss_item_sk,
                  d_date)
SELECT
         *
FROM     (
                  SELECT   item_sk ,
                           d_date ,
                           web_sales ,
                           store_sales ,
                           max(web_sales) OVER (partition BY item_sk ORDER BY d_date rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row)   web_cumulative ,
                           max(store_sales) OVER (partition BY item_sk ORDER BY d_date rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row) store_cumulative
                  FROM     (
                                           SELECT
                                                           CASE
                                                                           WHEN web.item_sk IS NOT NULL THEN web.item_sk
                                                                           ELSE store.item_sk
                                                           END item_sk ,
                                                           CASE
                                                                           WHEN web.d_date IS NOT NULL THEN web.d_date
                                                                           ELSE store.d_date
                                                           END              d_date ,
                                                           web.cume_sales   web_sales ,
                                                           store.cume_sales store_sales
                                           FROM            web_v1 web
                                           FULL OUTER JOIN store_v1 store
                                           ON              (
                                                                           web.item_sk = store.item_sk
                                                           AND             web.d_date = store.d_date) )x )y
WHERE    web_cumulative > store_cumulative
ORDER BY item_sk ,
         d_date
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date",
    "date_dim"."d_month_seq" AS "d_month_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_month_seq" <= 1203 AND "date_dim"."d_month_seq" >= 1192
), "web_v1" AS (
  SELECT
    "web_sales"."ws_item_sk" AS "item_sk",
    "date_dim"."d_date" AS "d_date",
    SUM(SUM("web_sales"."ws_sales_price")) OVER (PARTITION BY "web_sales"."ws_item_sk" ORDER BY "date_dim"."d_date" rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cume_sales"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  WHERE
    NOT "web_sales"."ws_item_sk" IS NULL
  GROUP BY
    "web_sales"."ws_item_sk",
    "date_dim"."d_date"
), "store_v1" AS (
  SELECT
    "store_sales"."ss_item_sk" AS "item_sk",
    "date_dim"."d_date" AS "d_date",
    SUM(SUM("store_sales"."ss_sales_price")) OVER (PARTITION BY "store_sales"."ss_item_sk" ORDER BY "date_dim"."d_date" rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cume_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  WHERE
    NOT "store_sales"."ss_item_sk" IS NULL
  GROUP BY
    "store_sales"."ss_item_sk",
    "date_dim"."d_date"
), "y" AS (
  SELECT
    CASE
      WHEN NOT "web"."item_sk" IS NULL
      THEN "web"."item_sk"
      ELSE "store"."item_sk"
    END AS "item_sk",
    CASE WHEN NOT "web"."d_date" IS NULL THEN "web"."d_date" ELSE "store"."d_date" END AS "d_date",
    "web"."cume_sales" AS "web_sales",
    "store"."cume_sales" AS "store_sales",
    MAX("web"."cume_sales") OVER (PARTITION BY CASE
      WHEN NOT "web"."item_sk" IS NULL
      THEN "web"."item_sk"
      ELSE "store"."item_sk"
    END ORDER BY CASE WHEN NOT "web"."d_date" IS NULL THEN "web"."d_date" ELSE "store"."d_date" END rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "web_cumulative",
    MAX("store"."cume_sales") OVER (PARTITION BY CASE
      WHEN NOT "web"."item_sk" IS NULL
      THEN "web"."item_sk"
      ELSE "store"."item_sk"
    END ORDER BY CASE WHEN NOT "web"."d_date" IS NULL THEN "web"."d_date" ELSE "store"."d_date" END rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "store_cumulative"
  FROM "web_v1" AS "web"
  FULL JOIN "store_v1" AS "store"
    ON "store"."d_date" = "web"."d_date" AND "store"."item_sk" = "web"."item_sk"
)
SELECT
  "y"."item_sk" AS "item_sk",
  "y"."d_date" AS "d_date",
  "y"."web_sales" AS "web_sales",
  "y"."store_sales" AS "store_sales",
  "y"."web_cumulative" AS "web_cumulative",
  "y"."store_cumulative" AS "store_cumulative"
FROM "y" AS "y"
WHERE
  "y"."store_cumulative" < "y"."web_cumulative"
ORDER BY
  "y"."item_sk",
  "y"."d_date"
LIMIT 100;

--------------------------------------
-- TPC-DS 52
--------------------------------------
SELECT dt.d_year,
               item.i_brand_id         brand_id,
               item.i_brand            brand,
               Sum(ss_ext_sales_price) ext_price
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 11
       AND dt.d_year = 1999
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
ORDER  BY dt.d_year,
          ext_price DESC,
          brand_id
LIMIT 100;
SELECT
  "dt"."d_year" AS "d_year",
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  SUM("store_sales"."ss_ext_sales_price") AS "ext_price"
FROM "date_dim" AS "dt"
JOIN "store_sales" AS "store_sales"
  ON "dt"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk" AND "item"."i_manager_id" = 1
WHERE
  "dt"."d_moy" = 11 AND "dt"."d_year" = 1999
GROUP BY
  "dt"."d_year",
  "item"."i_brand",
  "item"."i_brand_id"
ORDER BY
  "d_year",
  "ext_price" DESC,
  "brand_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 53
--------------------------------------
SELECT *
FROM   (SELECT i_manufact_id,
               Sum(ss_sales_price)             sum_sales,
               Avg(Sum(ss_sales_price))
                 OVER (
                   partition BY i_manufact_id) avg_quarterly_sales
        FROM   item,
               store_sales,
               date_dim,
               store
        WHERE  ss_item_sk = i_item_sk
               AND ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND d_month_seq IN ( 1199, 1199 + 1, 1199 + 2, 1199 + 3,
                                    1199 + 4, 1199 + 5, 1199 + 6, 1199 + 7,
                                    1199 + 8, 1199 + 9, 1199 + 10, 1199 + 11 )
               AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' )
                       AND i_class IN ( 'personal', 'portable', 'reference',
                                        'self-help' )
                       AND i_brand IN ( 'scholaramalgamalg #14',
                                        'scholaramalgamalg #7'
                                        ,
                                        'exportiunivamalg #9',
                                                       'scholaramalgamalg #9' )
                     )
                      OR ( i_category IN ( 'Women', 'Music', 'Men' )
                           AND i_class IN ( 'accessories', 'classical',
                                            'fragrances',
                                            'pants' )
                           AND i_brand IN ( 'amalgimporto #1',
                                            'edu packscholar #1',
                                            'exportiimporto #1',
                                                'importoamalg #1' ) ) )
        GROUP  BY i_manufact_id,
                  d_qoy) tmp1
WHERE  CASE
         WHEN avg_quarterly_sales > 0 THEN Abs (sum_sales - avg_quarterly_sales)
                                           /
                                           avg_quarterly_sales
         ELSE NULL
       END > 0.1
ORDER  BY avg_quarterly_sales,
          sum_sales,
          i_manufact_id
LIMIT 100;
WITH "tmp1" AS (
  SELECT
    "item"."i_manufact_id" AS "i_manufact_id",
    SUM("store_sales"."ss_sales_price") AS "sum_sales",
    AVG(SUM("store_sales"."ss_sales_price")) OVER (PARTITION BY "item"."i_manufact_id") AS "avg_quarterly_sales"
  FROM "item" AS "item"
  JOIN "store_sales" AS "store_sales"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_month_seq" IN (1199, 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210)
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  WHERE
    (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
    )
    AND (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_category" IN ('Books', 'Children', 'Electronics')
    )
    AND (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
    AND (
      "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      OR "item"."i_category" IN ('Women', 'Music', 'Men')
    )
    AND (
      "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      OR "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
    )
    AND (
      "item"."i_category" IN ('Books', 'Children', 'Electronics')
      OR "item"."i_category" IN ('Women', 'Music', 'Men')
    )
    AND (
      "item"."i_category" IN ('Books', 'Children', 'Electronics')
      OR "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
    )
    AND (
      "item"."i_category" IN ('Women', 'Music', 'Men')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
    AND (
      "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
  GROUP BY
    "item"."i_manufact_id",
    "date_dim"."d_qoy"
)
SELECT
  "tmp1"."i_manufact_id" AS "i_manufact_id",
  "tmp1"."sum_sales" AS "sum_sales",
  "tmp1"."avg_quarterly_sales" AS "avg_quarterly_sales"
FROM "tmp1" AS "tmp1"
WHERE
  CASE
    WHEN "tmp1"."avg_quarterly_sales" > 0
    THEN ABS("tmp1"."sum_sales" - "tmp1"."avg_quarterly_sales") / "tmp1"."avg_quarterly_sales"
    ELSE NULL
  END > 0.1
ORDER BY
  "tmp1"."avg_quarterly_sales",
  "tmp1"."sum_sales",
  "tmp1"."i_manufact_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 54
--------------------------------------
WITH my_customers
     AS (SELECT DISTINCT c_customer_sk,
                         c_current_addr_sk
         FROM   (SELECT cs_sold_date_sk     sold_date_sk,
                        cs_bill_customer_sk customer_sk,
                        cs_item_sk          item_sk
                 FROM   catalog_sales
                 UNION ALL
                 SELECT ws_sold_date_sk     sold_date_sk,
                        ws_bill_customer_sk customer_sk,
                        ws_item_sk          item_sk
                 FROM   web_sales) cs_or_ws_sales,
                item,
                date_dim,
                customer
         WHERE  sold_date_sk = d_date_sk
                AND item_sk = i_item_sk
                AND i_category = 'Sports'
                AND i_class = 'fitness'
                AND c_customer_sk = cs_or_ws_sales.customer_sk
                AND d_moy = 5
                AND d_year = 2000),
     my_revenue
     AS (SELECT c_customer_sk,
                Sum(ss_ext_sales_price) AS revenue
         FROM   my_customers,
                store_sales,
                customer_address,
                store,
                date_dim
         WHERE  c_current_addr_sk = ca_address_sk
                AND ca_county = s_county
                AND ca_state = s_state
                AND ss_sold_date_sk = d_date_sk
                AND c_customer_sk = ss_customer_sk
                AND d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1
                                         FROM   date_dim
                                         WHERE  d_year = 2000
                                                AND d_moy = 5) AND
                                        (SELECT DISTINCT
                                        d_month_seq + 3
                                         FROM   date_dim
                                         WHERE  d_year = 2000
                                                AND d_moy = 5)
         GROUP  BY c_customer_sk),
     segments
     AS (SELECT Cast(( revenue / 50 ) AS INT) AS segment
         FROM   my_revenue)
SELECT segment,
               Count(*)     AS num_customers,
               segment * 50 AS segment_base
FROM   segments
GROUP  BY segment
ORDER  BY segment,
          num_customers
LIMIT 100;
WITH "cs_or_ws_sales" AS (
  SELECT
    "catalog_sales"."cs_sold_date_sk" AS "sold_date_sk",
    "catalog_sales"."cs_bill_customer_sk" AS "customer_sk",
    "catalog_sales"."cs_item_sk" AS "item_sk"
  FROM "catalog_sales" AS "catalog_sales"
  UNION ALL
  SELECT
    "web_sales"."ws_sold_date_sk" AS "sold_date_sk",
    "web_sales"."ws_bill_customer_sk" AS "customer_sk",
    "web_sales"."ws_item_sk" AS "item_sk"
  FROM "web_sales" AS "web_sales"
), "my_customers" AS (
  SELECT DISTINCT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_current_addr_sk" AS "c_current_addr_sk"
  FROM "cs_or_ws_sales" AS "cs_or_ws_sales"
  JOIN "customer" AS "customer"
    ON "cs_or_ws_sales"."customer_sk" = "customer"."c_customer_sk"
  JOIN "date_dim" AS "date_dim"
    ON "cs_or_ws_sales"."sold_date_sk" = "date_dim"."d_date_sk"
    AND "date_dim"."d_moy" = 5
    AND "date_dim"."d_year" = 2000
  JOIN "item" AS "item"
    ON "cs_or_ws_sales"."item_sk" = "item"."i_item_sk"
    AND "item"."i_category" = 'Sports'
    AND "item"."i_class" = 'fitness'
), "_u_0" AS (
  SELECT DISTINCT
    "date_dim"."d_month_seq" + 1 AS "_col_0"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 5 AND "date_dim"."d_year" = 2000
), "_u_1" AS (
  SELECT DISTINCT
    "date_dim"."d_month_seq" + 3 AS "_col_0"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 5 AND "date_dim"."d_year" = 2000
), "my_revenue" AS (
  SELECT
    SUM("store_sales"."ss_ext_sales_price") AS "revenue"
  FROM "my_customers"
  JOIN "customer_address" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "my_customers"."c_current_addr_sk"
  JOIN "store_sales" AS "store_sales"
    ON "my_customers"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "store" AS "store"
    ON "customer_address"."ca_county" = "store"."s_county"
    AND "customer_address"."ca_state" = "store"."s_state"
  JOIN "_u_0" AS "_u_0"
    ON "_u_0"."_col_0" <= "date_dim"."d_month_seq"
  JOIN "_u_1" AS "_u_1"
    ON "_u_1"."_col_0" >= "date_dim"."d_month_seq"
  GROUP BY
    "my_customers"."c_customer_sk"
)
SELECT
  CAST((
    "my_revenue"."revenue" / 50
  ) AS INT) AS "segment",
  COUNT(*) AS "num_customers",
  CAST((
    "my_revenue"."revenue" / 50
  ) AS INT) * 50 AS "segment_base"
FROM "my_revenue"
GROUP BY
  CAST((
    "my_revenue"."revenue" / 50
  ) AS INT)
ORDER BY
  "segment",
  "num_customers"
LIMIT 100;

--------------------------------------
-- TPC-DS 55
--------------------------------------
SELECT i_brand_id              brand_id,
               i_brand                 brand,
               Sum(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 33
       AND d_moy = 12
       AND d_year = 1998
GROUP  BY i_brand,
          i_brand_id
ORDER  BY ext_price DESC,
          i_brand_id
LIMIT 100;
SELECT
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  SUM("store_sales"."ss_ext_sales_price") AS "ext_price"
FROM "date_dim" AS "date_dim"
JOIN "store_sales" AS "store_sales"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk" AND "item"."i_manager_id" = 33
WHERE
  "date_dim"."d_moy" = 12 AND "date_dim"."d_year" = 1998
GROUP BY
  "item"."i_brand",
  "item"."i_brand_id"
ORDER BY
  "ext_price" DESC,
  "brand_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 56
--------------------------------------
WITH ss
     AS (SELECT i_item_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
               Sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_item_id
ORDER  BY total_sales
LIMIT 100;
WITH "customer_address_2" AS (
  SELECT
    "customer_address"."ca_address_sk" AS "ca_address_sk",
    "customer_address"."ca_gmt_offset" AS "ca_gmt_offset"
  FROM "customer_address" AS "customer_address"
  WHERE
    "customer_address"."ca_gmt_offset" = -6
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 3 AND "date_dim"."d_year" = 1998
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
), "_u_0" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
  WHERE
    "item"."i_color" IN ('firebrick', 'rosy', 'white')
  GROUP BY
    "item"."i_item_id"
), "ss" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("store_sales"."ss_ext_sales_price") AS "total_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_0"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "cs" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("catalog_sales"."cs_ext_sales_price") AS "total_sales"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "catalog_sales"."cs_bill_addr_sk" = "customer_address"."ca_address_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  LEFT JOIN "_u_0" AS "_u_1"
    ON "_u_1"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_1"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "ws" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("web_sales"."ws_ext_sales_price") AS "total_sales"
  FROM "web_sales" AS "web_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "web_sales"."ws_bill_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "_u_2"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_2"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "tmp1" AS (
  SELECT
    "ss"."i_item_id" AS "i_item_id",
    "ss"."total_sales" AS "total_sales"
  FROM "ss"
  UNION ALL
  SELECT
    "cs"."i_item_id" AS "i_item_id",
    "cs"."total_sales" AS "total_sales"
  FROM "cs"
  UNION ALL
  SELECT
    "ws"."i_item_id" AS "i_item_id",
    "ws"."total_sales" AS "total_sales"
  FROM "ws"
)
SELECT
  "tmp1"."i_item_id" AS "i_item_id",
  SUM("tmp1"."total_sales") AS "total_sales"
FROM "tmp1" AS "tmp1"
GROUP BY
  "tmp1"."i_item_id"
ORDER BY
  "total_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 57
--------------------------------------
WITH v1
     AS (SELECT i_category,
                i_brand,
                cc_name,
                d_year,
                d_moy,
                Sum(cs_sales_price)                                    sum_sales
                ,
                Avg(Sum(cs_sales_price))
                  OVER (
                    partition BY i_category, i_brand, cc_name, d_year)
                avg_monthly_sales
                   ,
                Rank()
                  OVER (
                    partition BY i_category, i_brand, cc_name
                    ORDER BY d_year, d_moy)                            rn
         FROM   item,
                catalog_sales,
                date_dim,
                call_center
         WHERE  cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND cc_call_center_sk = cs_call_center_sk
                AND ( d_year = 2000
                       OR ( d_year = 2000 - 1
                            AND d_moy = 12 )
                       OR ( d_year = 2000 + 1
                            AND d_moy = 1 ) )
         GROUP  BY i_category,
                   i_brand,
                   cc_name,
                   d_year,
                   d_moy),
     v2
     AS (SELECT v1.i_brand,
                v1.d_year,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales  psum,
                v1_lead.sum_sales nsum
         FROM   v1,
                v1 v1_lag,
                v1 v1_lead
         WHERE  v1.i_category = v1_lag.i_category
                AND v1.i_category = v1_lead.i_category
                AND v1.i_brand = v1_lag.i_brand
                AND v1.i_brand = v1_lead.i_brand
                AND v1. cc_name = v1_lag. cc_name
                AND v1. cc_name = v1_lead. cc_name
                AND v1.rn = v1_lag.rn + 1
                AND v1.rn = v1_lead.rn - 1)
SELECT *
FROM   v2
WHERE  d_year = 2000
       AND avg_monthly_sales > 0
       AND CASE
             WHEN avg_monthly_sales > 0 THEN Abs(sum_sales - avg_monthly_sales)
                                             /
                                             avg_monthly_sales
             ELSE NULL
           END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          3
LIMIT 100;
WITH "v1" AS (
  SELECT
    "item"."i_category" AS "i_category",
    "item"."i_brand" AS "i_brand",
    "call_center"."cc_name" AS "cc_name",
    "date_dim"."d_year" AS "d_year",
    SUM("catalog_sales"."cs_sales_price") AS "sum_sales",
    AVG(SUM("catalog_sales"."cs_sales_price")) OVER (PARTITION BY "item"."i_category", "item"."i_brand", "call_center"."cc_name", "date_dim"."d_year") AS "avg_monthly_sales",
    RANK() OVER (PARTITION BY "item"."i_category", "item"."i_brand", "call_center"."cc_name" ORDER BY "date_dim"."d_year", "date_dim"."d_moy") AS "rn"
  FROM "item" AS "item"
  JOIN "catalog_sales" AS "catalog_sales"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  JOIN "call_center" AS "call_center"
    ON "call_center"."cc_call_center_sk" = "catalog_sales"."cs_call_center_sk"
  JOIN "date_dim" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
    AND (
      "date_dim"."d_moy" = 1 OR "date_dim"."d_moy" = 12 OR "date_dim"."d_year" = 2000
    )
    AND (
      "date_dim"."d_moy" = 1 OR "date_dim"."d_year" = 1999 OR "date_dim"."d_year" = 2000
    )
    AND (
      "date_dim"."d_moy" = 12 OR "date_dim"."d_year" = 2000 OR "date_dim"."d_year" = 2001
    )
    AND (
      "date_dim"."d_year" = 1999 OR "date_dim"."d_year" = 2000 OR "date_dim"."d_year" = 2001
    )
  GROUP BY
    "item"."i_category",
    "item"."i_brand",
    "call_center"."cc_name",
    "date_dim"."d_year",
    "date_dim"."d_moy"
)
SELECT
  "v1"."i_brand" AS "i_brand",
  "v1"."d_year" AS "d_year",
  "v1"."avg_monthly_sales" AS "avg_monthly_sales",
  "v1"."sum_sales" AS "sum_sales",
  "v1_lag"."sum_sales" AS "psum",
  "v1_lead"."sum_sales" AS "nsum"
FROM "v1"
JOIN "v1" AS "v1_lag"
  ON "v1"."cc_name" = "v1_lag"."cc_name"
  AND "v1"."i_brand" = "v1_lag"."i_brand"
  AND "v1"."i_category" = "v1_lag"."i_category"
  AND "v1"."rn" = "v1_lag"."rn" + 1
JOIN "v1" AS "v1_lead"
  ON "v1"."cc_name" = "v1_lead"."cc_name"
  AND "v1"."i_brand" = "v1_lead"."i_brand"
  AND "v1"."i_category" = "v1_lead"."i_category"
  AND "v1"."rn" = "v1_lead"."rn" - 1
WHERE
  "v1"."avg_monthly_sales" > 0
  AND "v1"."d_year" = 2000
  AND CASE
    WHEN "v1"."avg_monthly_sales" > 0
    THEN ABS("v1"."sum_sales" - "v1"."avg_monthly_sales") / "v1"."avg_monthly_sales"
    ELSE NULL
  END > 0.1
ORDER BY
  "v1"."sum_sales" - "v1"."avg_monthly_sales",
  "avg_monthly_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 58
--------------------------------------
WITH ss_items
     AS (SELECT i_item_id               item_id,
                Sum(ss_ext_sales_price) ss_item_rev
         FROM   store_sales,
                item,
                date_dim
         WHERE  ss_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2002-02-25'
                                                   ))
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY i_item_id),
     cs_items
     AS (SELECT i_item_id               item_id,
                Sum(cs_ext_sales_price) cs_item_rev
         FROM   catalog_sales,
                item,
                date_dim
         WHERE  cs_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2002-02-25'
                                                   ))
                AND cs_sold_date_sk = d_date_sk
         GROUP  BY i_item_id),
     ws_items
     AS (SELECT i_item_id               item_id,
                Sum(ws_ext_sales_price) ws_item_rev
         FROM   web_sales,
                item,
                date_dim
         WHERE  ws_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2002-02-25'
                                                   ))
                AND ws_sold_date_sk = d_date_sk
         GROUP  BY i_item_id)
SELECT ss_items.item_id,
               ss_item_rev,
               ss_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 *
               100 ss_dev,
               cs_item_rev,
               cs_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 *
               100 cs_dev,
               ws_item_rev,
               ws_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 *
               100 ws_dev,
               ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3
               average
FROM   ss_items,
       cs_items,
       ws_items
WHERE  ss_items.item_id = cs_items.item_id
       AND ss_items.item_id = ws_items.item_id
       AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
       AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
       AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
       AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
       AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
       AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER  BY item_id,
          ss_item_rev
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
), "_u_0" AS (
  SELECT
    "date_dim"."d_week_seq" AS "d_week_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_date" = '2002-02-25'
), "_u_1" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  JOIN "_u_0" AS "_u_0"
    ON "_u_0"."d_week_seq" = "date_dim"."d_week_seq"
  GROUP BY
    "date_dim"."d_date"
), "ss_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("store_sales"."ss_ext_sales_price") AS "ss_item_rev"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  LEFT JOIN "_u_1" AS "_u_1"
    ON "_u_1"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_1"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
), "_u_3" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  JOIN "_u_0" AS "_u_2"
    ON "_u_2"."d_week_seq" = "date_dim"."d_week_seq"
  GROUP BY
    "date_dim"."d_date"
), "cs_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("catalog_sales"."cs_ext_sales_price") AS "cs_item_rev"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  LEFT JOIN "_u_3" AS "_u_3"
    ON "_u_3"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_3"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
), "_u_5" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  JOIN "_u_0" AS "_u_4"
    ON "_u_4"."d_week_seq" = "date_dim"."d_week_seq"
  GROUP BY
    "date_dim"."d_date"
), "ws_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("web_sales"."ws_ext_sales_price") AS "ws_item_rev"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "_u_5" AS "_u_5"
    ON "_u_5"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_5"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
)
SELECT
  "ss_items"."item_id" AS "item_id",
  "ss_items"."ss_item_rev" AS "ss_item_rev",
  "ss_items"."ss_item_rev" / (
    "ss_items"."ss_item_rev" + "cs_items"."cs_item_rev" + "ws_items"."ws_item_rev"
  ) / 3 * 100 AS "ss_dev",
  "cs_items"."cs_item_rev" AS "cs_item_rev",
  "cs_items"."cs_item_rev" / (
    "ss_items"."ss_item_rev" + "cs_items"."cs_item_rev" + "ws_items"."ws_item_rev"
  ) / 3 * 100 AS "cs_dev",
  "ws_items"."ws_item_rev" AS "ws_item_rev",
  "ws_items"."ws_item_rev" / (
    "ss_items"."ss_item_rev" + "cs_items"."cs_item_rev" + "ws_items"."ws_item_rev"
  ) / 3 * 100 AS "ws_dev",
  (
    "ss_items"."ss_item_rev" + "cs_items"."cs_item_rev" + "ws_items"."ws_item_rev"
  ) / 3 AS "average"
FROM "ss_items"
JOIN "ws_items"
  ON "ss_items"."item_id" = "ws_items"."item_id"
  AND "ss_items"."ss_item_rev" <= 1.1 * "ws_items"."ws_item_rev"
  AND "ss_items"."ss_item_rev" >= 0.9 * "ws_items"."ws_item_rev"
  AND "ws_items"."ws_item_rev" <= 1.1 * "ss_items"."ss_item_rev"
  AND "ws_items"."ws_item_rev" >= 0.9 * "ss_items"."ss_item_rev"
JOIN "cs_items"
  ON "cs_items"."cs_item_rev" <= 1.1 * "ss_items"."ss_item_rev"
  AND "cs_items"."cs_item_rev" <= 1.1 * "ws_items"."ws_item_rev"
  AND "cs_items"."cs_item_rev" >= 0.9 * "ss_items"."ss_item_rev"
  AND "cs_items"."cs_item_rev" >= 0.9 * "ws_items"."ws_item_rev"
  AND "cs_items"."item_id" = "ss_items"."item_id"
  AND "ss_items"."ss_item_rev" <= 1.1 * "cs_items"."cs_item_rev"
  AND "ss_items"."ss_item_rev" >= 0.9 * "cs_items"."cs_item_rev"
  AND "ws_items"."ws_item_rev" <= 1.1 * "cs_items"."cs_item_rev"
  AND "ws_items"."ws_item_rev" >= 0.9 * "cs_items"."cs_item_rev"
ORDER BY
  "item_id",
  "ss_item_rev"
LIMIT 100;

--------------------------------------
-- TPC-DS 59
--------------------------------------
WITH wss
     AS (SELECT d_week_seq,
                ss_store_sk,
                Sum(CASE
                      WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price
                      ELSE NULL
                    END) sun_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price
                      ELSE NULL
                    END) mon_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price
                      ELSE NULL
                    END) tue_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price
                      ELSE NULL
                    END) wed_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price
                      ELSE NULL
                    END) thu_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price
                      ELSE NULL
                    END) fri_sales,
                Sum(CASE
                      WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price
                      ELSE NULL
                    END) sat_sales
         FROM   store_sales,
                date_dim
         WHERE  d_date_sk = ss_sold_date_sk
         GROUP  BY d_week_seq,
                   ss_store_sk)
SELECT s_store_name1,
               s_store_id1,
               d_week_seq1,
               sun_sales1 / sun_sales2,
               mon_sales1 / mon_sales2,
               tue_sales1 / tue_sales2,
               wed_sales1 / wed_sales2,
               thu_sales1 / thu_sales2,
               fri_sales1 / fri_sales2,
               sat_sales1 / sat_sales2
FROM   (SELECT s_store_name   s_store_name1,
               wss.d_week_seq d_week_seq1,
               s_store_id     s_store_id1,
               sun_sales      sun_sales1,
               mon_sales      mon_sales1,
               tue_sales      tue_sales1,
               wed_sales      wed_sales1,
               thu_sales      thu_sales1,
               fri_sales      fri_sales1,
               sat_sales      sat_sales1
        FROM   wss,
               store,
               date_dim d
        WHERE  d.d_week_seq = wss.d_week_seq
               AND ss_store_sk = s_store_sk
               AND d_month_seq BETWEEN 1196 AND 1196 + 11) y,
       (SELECT s_store_name   s_store_name2,
               wss.d_week_seq d_week_seq2,
               s_store_id     s_store_id2,
               sun_sales      sun_sales2,
               mon_sales      mon_sales2,
               tue_sales      tue_sales2,
               wed_sales      wed_sales2,
               thu_sales      thu_sales2,
               fri_sales      fri_sales2,
               sat_sales      sat_sales2
        FROM   wss,
               store,
               date_dim d
        WHERE  d.d_week_seq = wss.d_week_seq
               AND ss_store_sk = s_store_sk
               AND d_month_seq BETWEEN 1196 + 12 AND 1196 + 23) x
WHERE  s_store_id1 = s_store_id2
       AND d_week_seq1 = d_week_seq2 - 52
ORDER  BY s_store_name1,
          s_store_id1,
          d_week_seq1
LIMIT 100;
WITH "wss" AS (
  SELECT
    "date_dim"."d_week_seq" AS "d_week_seq",
    "store_sales"."ss_store_sk" AS "ss_store_sk",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Sunday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "sun_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Monday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "mon_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Tuesday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "tue_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Wednesday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "wed_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Thursday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "thu_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Friday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "fri_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_day_name" = 'Saturday'
        THEN "store_sales"."ss_sales_price"
        ELSE NULL
      END
    ) AS "sat_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "date_dim"."d_week_seq",
    "store_sales"."ss_store_sk"
), "x" AS (
  SELECT
    "wss"."d_week_seq" AS "d_week_seq2",
    "store"."s_store_id" AS "s_store_id2",
    "wss"."sun_sales" AS "sun_sales2",
    "wss"."mon_sales" AS "mon_sales2",
    "wss"."tue_sales" AS "tue_sales2",
    "wss"."wed_sales" AS "wed_sales2",
    "wss"."thu_sales" AS "thu_sales2",
    "wss"."fri_sales" AS "fri_sales2",
    "wss"."sat_sales" AS "sat_sales2"
  FROM "wss"
  JOIN "date_dim" AS "d"
    ON "d"."d_month_seq" <= 1219
    AND "d"."d_month_seq" >= 1208
    AND "d"."d_week_seq" = "wss"."d_week_seq"
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "wss"."ss_store_sk"
)
SELECT
  "store"."s_store_name" AS "s_store_name1",
  "store"."s_store_id" AS "s_store_id1",
  "wss"."d_week_seq" AS "d_week_seq1",
  "wss"."sun_sales" / "x"."sun_sales2" AS "_col_3",
  "wss"."mon_sales" / "x"."mon_sales2" AS "_col_4",
  "wss"."tue_sales" / "x"."tue_sales2" AS "_col_5",
  "wss"."wed_sales" / "x"."wed_sales2" AS "_col_6",
  "wss"."thu_sales" / "x"."thu_sales2" AS "_col_7",
  "wss"."fri_sales" / "x"."fri_sales2" AS "_col_8",
  "wss"."sat_sales" / "x"."sat_sales2" AS "_col_9"
FROM "wss"
JOIN "date_dim" AS "d"
  ON "d"."d_month_seq" <= 1207
  AND "d"."d_month_seq" >= 1196
  AND "d"."d_week_seq" = "wss"."d_week_seq"
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "wss"."ss_store_sk"
JOIN "x" AS "x"
  ON "store"."s_store_id" = "x"."s_store_id2"
  AND "wss"."d_week_seq" = "x"."d_week_seq2" - 52
ORDER BY
  "s_store_name1",
  "s_store_id1",
  "d_week_seq1"
LIMIT 100;

--------------------------------------
-- TPC-DS 60
--------------------------------------
WITH ss
     AS (SELECT i_item_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Jewelry' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Jewelry' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Jewelry' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
               Sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_item_id
ORDER  BY i_item_id,
          total_sales
LIMIT 100;
WITH "customer_address_2" AS (
  SELECT
    "customer_address"."ca_address_sk" AS "ca_address_sk",
    "customer_address"."ca_gmt_offset" AS "ca_gmt_offset"
  FROM "customer_address" AS "customer_address"
  WHERE
    "customer_address"."ca_gmt_offset" = -6
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 8 AND "date_dim"."d_year" = 1999
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
), "_u_0" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
  WHERE
    "item"."i_category" IN ('Jewelry')
  GROUP BY
    "item"."i_item_id"
), "ss" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("store_sales"."ss_ext_sales_price") AS "total_sales"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_0"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "cs" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("catalog_sales"."cs_ext_sales_price") AS "total_sales"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "catalog_sales"."cs_bill_addr_sk" = "customer_address"."ca_address_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  LEFT JOIN "_u_0" AS "_u_1"
    ON "_u_1"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_1"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "ws" AS (
  SELECT
    "item"."i_item_id" AS "i_item_id",
    SUM("web_sales"."ws_ext_sales_price") AS "total_sales"
  FROM "web_sales" AS "web_sales"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "web_sales"."ws_bill_addr_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "_u_2"."i_item_id" = "item"."i_item_id"
  WHERE
    NOT "_u_2"."i_item_id" IS NULL
  GROUP BY
    "item"."i_item_id"
), "tmp1" AS (
  SELECT
    "ss"."i_item_id" AS "i_item_id",
    "ss"."total_sales" AS "total_sales"
  FROM "ss"
  UNION ALL
  SELECT
    "cs"."i_item_id" AS "i_item_id",
    "cs"."total_sales" AS "total_sales"
  FROM "cs"
  UNION ALL
  SELECT
    "ws"."i_item_id" AS "i_item_id",
    "ws"."total_sales" AS "total_sales"
  FROM "ws"
)
SELECT
  "tmp1"."i_item_id" AS "i_item_id",
  SUM("tmp1"."total_sales") AS "total_sales"
FROM "tmp1" AS "tmp1"
GROUP BY
  "tmp1"."i_item_id"
ORDER BY
  "i_item_id",
  "total_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 61
--------------------------------------
SELECT promotions,
               total,
               Cast(promotions AS DECIMAL(15, 4)) /
               Cast(total AS DECIMAL(15, 4)) * 100
FROM   (SELECT Sum(ss_ext_sales_price) promotions
        FROM   store_sales,
               store,
               promotion,
               date_dim,
               customer,
               customer_address,
               item
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_promo_sk = p_promo_sk
               AND ss_customer_sk = c_customer_sk
               AND ca_address_sk = c_current_addr_sk
               AND ss_item_sk = i_item_sk
               AND ca_gmt_offset = -7
               AND i_category = 'Books'
               AND ( p_channel_dmail = 'Y'
                      OR p_channel_email = 'Y'
                      OR p_channel_tv = 'Y' )
               AND s_gmt_offset = -7
               AND d_year = 2001
               AND d_moy = 12) promotional_sales,
       (SELECT Sum(ss_ext_sales_price) total
        FROM   store_sales,
               store,
               date_dim,
               customer,
               customer_address,
               item
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_customer_sk = c_customer_sk
               AND ca_address_sk = c_current_addr_sk
               AND ss_item_sk = i_item_sk
               AND ca_gmt_offset = -7
               AND i_category = 'Books'
               AND s_gmt_offset = -7
               AND d_year = 2001
               AND d_moy = 12) all_sales
ORDER  BY promotions,
          total
LIMIT 100;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_current_addr_sk" AS "c_current_addr_sk"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 12 AND "date_dim"."d_year" = 2001
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_category" AS "i_category"
  FROM "item" AS "item"
  WHERE
    "item"."i_category" = 'Books'
), "store_2" AS (
  SELECT
    "store"."s_store_sk" AS "s_store_sk",
    "store"."s_gmt_offset" AS "s_gmt_offset"
  FROM "store" AS "store"
  WHERE
    "store"."s_gmt_offset" = -7
), "customer_address_2" AS (
  SELECT
    "customer_address"."ca_address_sk" AS "ca_address_sk",
    "customer_address"."ca_gmt_offset" AS "ca_gmt_offset"
  FROM "customer_address" AS "customer_address"
  WHERE
    "customer_address"."ca_gmt_offset" = -7
), "promotional_sales" AS (
  SELECT
    SUM("store_sales"."ss_ext_sales_price") AS "promotions"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "promotion" AS "promotion"
    ON (
      "promotion"."p_channel_dmail" = 'Y'
      OR "promotion"."p_channel_email" = 'Y'
      OR "promotion"."p_channel_tv" = 'Y'
    )
    AND "promotion"."p_promo_sk" = "store_sales"."ss_promo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
), "all_sales" AS (
  SELECT
    SUM("store_sales"."ss_ext_sales_price") AS "total"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_2" AS "customer"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "customer_address_2" AS "customer_address"
    ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
)
SELECT
  "promotional_sales"."promotions" AS "promotions",
  "all_sales"."total" AS "total",
  CAST("promotional_sales"."promotions" AS DECIMAL(15, 4)) / CAST("all_sales"."total" AS DECIMAL(15, 4)) * 100 AS "_col_2"
FROM "promotional_sales" AS "promotional_sales"
CROSS JOIN "all_sales" AS "all_sales"
ORDER BY
  "promotions",
  "total"
LIMIT 100;

--------------------------------------
-- TPC-DS 62
--------------------------------------
SELECT Substr(w_warehouse_name, 1, 20),
               sm_type,
               web_name,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS "30 days",
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 30 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 60 ) THEN 1
                     ELSE 0
                   END) AS "31-60 days",
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 60 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 90 ) THEN 1
                     ELSE 0
                   END) AS "61-90 days",
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 90 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 120 ) THEN
                     1
                     ELSE 0
                   END) AS "91-120 days",
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS ">120 days"
FROM   web_sales,
       warehouse,
       ship_mode,
       web_site,
       date_dim
WHERE  d_month_seq BETWEEN 1222 AND 1222 + 11
       AND ws_ship_date_sk = d_date_sk
       AND ws_warehouse_sk = w_warehouse_sk
       AND ws_ship_mode_sk = sm_ship_mode_sk
       AND ws_web_site_sk = web_site_sk
GROUP  BY Substr(w_warehouse_name, 1, 20),
          sm_type,
          web_name
ORDER  BY Substr(w_warehouse_name, 1, 20),
          sm_type,
          web_name
LIMIT 100;
SELECT
  SUBSTR("warehouse"."w_warehouse_name", 1, 20) AS "_col_0",
  "ship_mode"."sm_type" AS "sm_type",
  "web_site"."web_name" AS "web_name",
  SUM(
    CASE
      WHEN "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" <= 30
      THEN 1
      ELSE 0
    END
  ) AS "30 days",
  SUM(
    CASE
      WHEN "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" <= 60
      AND "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" > 30
      THEN 1
      ELSE 0
    END
  ) AS "31-60 days",
  SUM(
    CASE
      WHEN "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" <= 90
      AND "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" > 60
      THEN 1
      ELSE 0
    END
  ) AS "61-90 days",
  SUM(
    CASE
      WHEN "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" <= 120
      AND "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" > 90
      THEN 1
      ELSE 0
    END
  ) AS "91-120 days",
  SUM(
    CASE
      WHEN "web_sales"."ws_ship_date_sk" - "web_sales"."ws_sold_date_sk" > 120
      THEN 1
      ELSE 0
    END
  ) AS ">120 days"
FROM "web_sales" AS "web_sales"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "web_sales"."ws_ship_date_sk"
  AND "date_dim"."d_month_seq" <= 1233
  AND "date_dim"."d_month_seq" >= 1222
JOIN "ship_mode" AS "ship_mode"
  ON "ship_mode"."sm_ship_mode_sk" = "web_sales"."ws_ship_mode_sk"
JOIN "warehouse" AS "warehouse"
  ON "warehouse"."w_warehouse_sk" = "web_sales"."ws_warehouse_sk"
JOIN "web_site" AS "web_site"
  ON "web_sales"."ws_web_site_sk" = "web_site"."web_site_sk"
GROUP BY
  SUBSTR("warehouse"."w_warehouse_name", 1, 20),
  "ship_mode"."sm_type",
  "web_site"."web_name"
ORDER BY
  "_col_0",
  "sm_type",
  "web_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 63
--------------------------------------
SELECT *
FROM   (SELECT i_manager_id,
               Sum(ss_sales_price)            sum_sales,
               Avg(Sum(ss_sales_price))
                 OVER (
                   partition BY i_manager_id) avg_monthly_sales
        FROM   item,
               store_sales,
               date_dim,
               store
        WHERE  ss_item_sk = i_item_sk
               AND ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND d_month_seq IN ( 1200, 1200 + 1, 1200 + 2, 1200 + 3,
                                    1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7,
                                    1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11 )
               AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' )
                       AND i_class IN ( 'personal', 'portable', 'reference',
                                        'self-help' )
                       AND i_brand IN ( 'scholaramalgamalg #14',
                                        'scholaramalgamalg #7'
                                        ,
                                        'exportiunivamalg #9',
                                                       'scholaramalgamalg #9' )
                     )
                      OR ( i_category IN ( 'Women', 'Music', 'Men' )
                           AND i_class IN ( 'accessories', 'classical',
                                            'fragrances',
                                            'pants' )
                           AND i_brand IN ( 'amalgimporto #1',
                                            'edu packscholar #1',
                                            'exportiimporto #1',
                                                'importoamalg #1' ) ) )
        GROUP  BY i_manager_id,
                  d_moy) tmp1
WHERE  CASE
         WHEN avg_monthly_sales > 0 THEN Abs (sum_sales - avg_monthly_sales) /
                                         avg_monthly_sales
         ELSE NULL
       END > 0.1
ORDER  BY i_manager_id,
          avg_monthly_sales,
          sum_sales
LIMIT 100;
WITH "tmp1" AS (
  SELECT
    "item"."i_manager_id" AS "i_manager_id",
    SUM("store_sales"."ss_sales_price") AS "sum_sales",
    AVG(SUM("store_sales"."ss_sales_price")) OVER (PARTITION BY "item"."i_manager_id") AS "avg_monthly_sales"
  FROM "item" AS "item"
  JOIN "store_sales" AS "store_sales"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_month_seq" IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  WHERE
    (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
    )
    AND (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_category" IN ('Books', 'Children', 'Electronics')
    )
    AND (
      "item"."i_brand" IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
    AND (
      "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      OR "item"."i_category" IN ('Women', 'Music', 'Men')
    )
    AND (
      "item"."i_brand" IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      OR "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
    )
    AND (
      "item"."i_category" IN ('Books', 'Children', 'Electronics')
      OR "item"."i_category" IN ('Women', 'Music', 'Men')
    )
    AND (
      "item"."i_category" IN ('Books', 'Children', 'Electronics')
      OR "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
    )
    AND (
      "item"."i_category" IN ('Women', 'Music', 'Men')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
    AND (
      "item"."i_class" IN ('accessories', 'classical', 'fragrances', 'pants')
      OR "item"."i_class" IN ('personal', 'portable', 'reference', 'self-help')
    )
  GROUP BY
    "item"."i_manager_id",
    "date_dim"."d_moy"
)
SELECT
  "tmp1"."i_manager_id" AS "i_manager_id",
  "tmp1"."sum_sales" AS "sum_sales",
  "tmp1"."avg_monthly_sales" AS "avg_monthly_sales"
FROM "tmp1" AS "tmp1"
WHERE
  CASE
    WHEN "tmp1"."avg_monthly_sales" > 0
    THEN ABS("tmp1"."sum_sales" - "tmp1"."avg_monthly_sales") / "tmp1"."avg_monthly_sales"
    ELSE NULL
  END > 0.1
ORDER BY
  "tmp1"."i_manager_id",
  "tmp1"."avg_monthly_sales",
  "tmp1"."sum_sales"
LIMIT 100;

--------------------------------------
-- TPC-DS 64
--------------------------------------
WITH cs_ui
     AS (SELECT cs_item_sk,
                Sum(cs_ext_list_price) AS sale,
                Sum(cr_refunded_cash + cr_reversed_charge
                    + cr_store_credit) AS refund
         FROM   catalog_sales,
                catalog_returns
         WHERE  cs_item_sk = cr_item_sk
                AND cs_order_number = cr_order_number
         GROUP  BY cs_item_sk
         HAVING Sum(cs_ext_list_price) > 2 * Sum(
                cr_refunded_cash + cr_reversed_charge
                + cr_store_credit)),
     cross_sales
     AS (SELECT i_product_name         product_name,
                i_item_sk              item_sk,
                s_store_name           store_name,
                s_zip                  store_zip,
                ad1.ca_street_number   b_street_number,
                ad1.ca_street_name     b_streen_name,
                ad1.ca_city            b_city,
                ad1.ca_zip             b_zip,
                ad2.ca_street_number   c_street_number,
                ad2.ca_street_name     c_street_name,
                ad2.ca_city            c_city,
                ad2.ca_zip             c_zip,
                d1.d_year              AS syear,
                d2.d_year              AS fsyear,
                d3.d_year              s2year,
                Count(*)               cnt,
                Sum(ss_wholesale_cost) s1,
                Sum(ss_list_price)     s2,
                Sum(ss_coupon_amt)     s3
         FROM   store_sales,
                store_returns,
                cs_ui,
                date_dim d1,
                date_dim d2,
                date_dim d3,
                store,
                customer,
                customer_demographics cd1,
                customer_demographics cd2,
                promotion,
                household_demographics hd1,
                household_demographics hd2,
                customer_address ad1,
                customer_address ad2,
                income_band ib1,
                income_band ib2,
                item
         WHERE  ss_store_sk = s_store_sk
                AND ss_sold_date_sk = d1.d_date_sk
                AND ss_customer_sk = c_customer_sk
                AND ss_cdemo_sk = cd1.cd_demo_sk
                AND ss_hdemo_sk = hd1.hd_demo_sk
                AND ss_addr_sk = ad1.ca_address_sk
                AND ss_item_sk = i_item_sk
                AND ss_item_sk = sr_item_sk
                AND ss_ticket_number = sr_ticket_number
                AND ss_item_sk = cs_ui.cs_item_sk
                AND c_current_cdemo_sk = cd2.cd_demo_sk
                AND c_current_hdemo_sk = hd2.hd_demo_sk
                AND c_current_addr_sk = ad2.ca_address_sk
                AND c_first_sales_date_sk = d2.d_date_sk
                AND c_first_shipto_date_sk = d3.d_date_sk
                AND ss_promo_sk = p_promo_sk
                AND hd1.hd_income_band_sk = ib1.ib_income_band_sk
                AND hd2.hd_income_band_sk = ib2.ib_income_band_sk
                AND cd1.cd_marital_status <> cd2.cd_marital_status
                AND i_color IN ( 'cyan', 'peach', 'blush', 'frosted',
                                 'powder', 'orange' )
                AND i_current_price BETWEEN 58 AND 58 + 10
                AND i_current_price BETWEEN 58 + 1 AND 58 + 15
         GROUP  BY i_product_name,
                   i_item_sk,
                   s_store_name,
                   s_zip,
                   ad1.ca_street_number,
                   ad1.ca_street_name,
                   ad1.ca_city,
                   ad1.ca_zip,
                   ad2.ca_street_number,
                   ad2.ca_street_name,
                   ad2.ca_city,
                   ad2.ca_zip,
                   d1.d_year,
                   d2.d_year,
                   d3.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_streen_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1,
       cs1.s2,
       cs1.s3,
       cs2.s1,
       cs2.s2,
       cs2.s3,
       cs2.syear,
       cs2.cnt
FROM   cross_sales cs1,
       cross_sales cs2
WHERE  cs1.item_sk = cs2.item_sk
       AND cs1.syear = 2001
       AND cs2.syear = 2001 + 1
       AND cs2.cnt <= cs1.cnt
       AND cs1.store_name = cs2.store_name
       AND cs1.store_zip = cs2.store_zip
ORDER  BY cs1.product_name,
          cs1.store_name,
          cs2.cnt;
WITH "cs_ui" AS (
  SELECT
    "catalog_sales"."cs_item_sk" AS "cs_item_sk"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "catalog_returns" AS "catalog_returns"
    ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
    AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
  GROUP BY
    "catalog_sales"."cs_item_sk"
  HAVING
    2 * SUM(
      "catalog_returns"."cr_refunded_cash" + "catalog_returns"."cr_reversed_charge" + "catalog_returns"."cr_store_credit"
    ) < SUM("catalog_sales"."cs_ext_list_price")
), "cross_sales" AS (
  SELECT
    "item"."i_product_name" AS "product_name",
    "item"."i_item_sk" AS "item_sk",
    "store"."s_store_name" AS "store_name",
    "store"."s_zip" AS "store_zip",
    "ad1"."ca_street_number" AS "b_street_number",
    "ad1"."ca_street_name" AS "b_streen_name",
    "ad1"."ca_city" AS "b_city",
    "ad1"."ca_zip" AS "b_zip",
    "ad2"."ca_street_number" AS "c_street_number",
    "ad2"."ca_street_name" AS "c_street_name",
    "ad2"."ca_city" AS "c_city",
    "ad2"."ca_zip" AS "c_zip",
    "d1"."d_year" AS "syear",
    COUNT(*) AS "cnt",
    SUM("store_sales"."ss_wholesale_cost") AS "s1",
    SUM("store_sales"."ss_list_price") AS "s2",
    SUM("store_sales"."ss_coupon_amt") AS "s3"
  FROM "store_sales" AS "store_sales"
  CROSS JOIN "income_band" AS "ib2"
  JOIN "customer_address" AS "ad1"
    ON "ad1"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "cs_ui"
    ON "cs_ui"."cs_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim" AS "d1"
    ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "household_demographics" AS "hd1"
    ON "hd1"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "household_demographics" AS "hd2"
    ON "hd2"."hd_income_band_sk" = "ib2"."ib_income_band_sk"
  JOIN "item" AS "item"
    ON "item"."i_color" IN ('cyan', 'peach', 'blush', 'frosted', 'powder', 'orange')
    AND "item"."i_current_price" <= 68
    AND "item"."i_current_price" >= 59
    AND "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "promotion" AS "promotion"
    ON "promotion"."p_promo_sk" = "store_sales"."ss_promo_sk"
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "store_returns" AS "store_returns"
    ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
    AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
  JOIN "customer" AS "customer"
    ON "customer"."c_current_hdemo_sk" = "hd2"."hd_demo_sk"
    AND "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "income_band" AS "ib1"
    ON "hd1"."hd_income_band_sk" = "ib1"."ib_income_band_sk"
  JOIN "customer_address" AS "ad2"
    ON "ad2"."ca_address_sk" = "customer"."c_current_addr_sk"
  JOIN "customer_demographics" AS "cd2"
    ON "cd2"."cd_demo_sk" = "customer"."c_current_cdemo_sk"
  JOIN "date_dim" AS "d2"
    ON "customer"."c_first_sales_date_sk" = "d2"."d_date_sk"
  JOIN "date_dim" AS "d3"
    ON "customer"."c_first_shipto_date_sk" = "d3"."d_date_sk"
  JOIN "customer_demographics" AS "cd1"
    ON "cd1"."cd_demo_sk" = "store_sales"."ss_cdemo_sk"
    AND "cd1"."cd_marital_status" <> "cd2"."cd_marital_status"
  GROUP BY
    "item"."i_product_name",
    "item"."i_item_sk",
    "store"."s_store_name",
    "store"."s_zip",
    "ad1"."ca_street_number",
    "ad1"."ca_street_name",
    "ad1"."ca_city",
    "ad1"."ca_zip",
    "ad2"."ca_street_number",
    "ad2"."ca_street_name",
    "ad2"."ca_city",
    "ad2"."ca_zip",
    "d1"."d_year",
    "d2"."d_year",
    "d3"."d_year"
)
SELECT
  "cs1"."product_name" AS "product_name",
  "cs1"."store_name" AS "store_name",
  "cs1"."store_zip" AS "store_zip",
  "cs1"."b_street_number" AS "b_street_number",
  "cs1"."b_streen_name" AS "b_streen_name",
  "cs1"."b_city" AS "b_city",
  "cs1"."b_zip" AS "b_zip",
  "cs1"."c_street_number" AS "c_street_number",
  "cs1"."c_street_name" AS "c_street_name",
  "cs1"."c_city" AS "c_city",
  "cs1"."c_zip" AS "c_zip",
  "cs1"."syear" AS "syear",
  "cs1"."cnt" AS "cnt",
  "cs1"."s1" AS "s1",
  "cs1"."s2" AS "s2",
  "cs1"."s3" AS "s3",
  "cs2"."s1" AS "s1",
  "cs2"."s2" AS "s2",
  "cs2"."s3" AS "s3",
  "cs2"."syear" AS "syear",
  "cs2"."cnt" AS "cnt"
FROM "cross_sales" AS "cs1"
JOIN "cross_sales" AS "cs2"
  ON "cs1"."cnt" >= "cs2"."cnt"
  AND "cs1"."item_sk" = "cs2"."item_sk"
  AND "cs1"."store_name" = "cs2"."store_name"
  AND "cs1"."store_zip" = "cs2"."store_zip"
  AND "cs2"."syear" = 2002
WHERE
  "cs1"."syear" = 2001
ORDER BY
  "cs1"."product_name",
  "cs1"."store_name",
  "cs2"."cnt";

--------------------------------------
-- TPC-DS 65
--------------------------------------
SELECT s_store_name,
               i_item_desc,
               sc.revenue,
               i_current_price,
               i_wholesale_cost,
               i_brand
FROM   store,
       item,
       (SELECT ss_store_sk,
               Avg(revenue) AS ave
        FROM   (SELECT ss_store_sk,
                       ss_item_sk,
                       Sum(ss_sales_price) AS revenue
                FROM   store_sales,
                       date_dim
                WHERE  ss_sold_date_sk = d_date_sk
                       AND d_month_seq BETWEEN 1199 AND 1199 + 11
                GROUP  BY ss_store_sk,
                          ss_item_sk) sa
        GROUP  BY ss_store_sk) sb,
       (SELECT ss_store_sk,
               ss_item_sk,
               Sum(ss_sales_price) AS revenue
        FROM   store_sales,
               date_dim
        WHERE  ss_sold_date_sk = d_date_sk
               AND d_month_seq BETWEEN 1199 AND 1199 + 11
        GROUP  BY ss_store_sk,
                  ss_item_sk) sc
WHERE  sb.ss_store_sk = sc.ss_store_sk
       AND sc.revenue <= 0.1 * sb.ave
       AND s_store_sk = sc.ss_store_sk
       AND i_item_sk = sc.ss_item_sk
ORDER  BY s_store_name,
          i_item_desc
LIMIT 100;
WITH "store_sales_2" AS (
  SELECT
    "store_sales"."ss_sold_date_sk" AS "ss_sold_date_sk",
    "store_sales"."ss_item_sk" AS "ss_item_sk",
    "store_sales"."ss_store_sk" AS "ss_store_sk",
    "store_sales"."ss_sales_price" AS "ss_sales_price"
  FROM "store_sales" AS "store_sales"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_month_seq" AS "d_month_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_month_seq" <= 1210 AND "date_dim"."d_month_seq" >= 1199
), "sc" AS (
  SELECT
    "store_sales"."ss_store_sk" AS "ss_store_sk",
    "store_sales"."ss_item_sk" AS "ss_item_sk",
    SUM("store_sales"."ss_sales_price") AS "revenue"
  FROM "store_sales_2" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_store_sk",
    "store_sales"."ss_item_sk"
), "sa" AS (
  SELECT
    "store_sales"."ss_store_sk" AS "ss_store_sk",
    SUM("store_sales"."ss_sales_price") AS "revenue"
  FROM "store_sales_2" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_store_sk",
    "store_sales"."ss_item_sk"
), "sb" AS (
  SELECT
    "sa"."ss_store_sk" AS "ss_store_sk",
    AVG("sa"."revenue") AS "ave"
  FROM "sa" AS "sa"
  GROUP BY
    "sa"."ss_store_sk"
)
SELECT
  "store"."s_store_name" AS "s_store_name",
  "item"."i_item_desc" AS "i_item_desc",
  "sc"."revenue" AS "revenue",
  "item"."i_current_price" AS "i_current_price",
  "item"."i_wholesale_cost" AS "i_wholesale_cost",
  "item"."i_brand" AS "i_brand"
FROM "store" AS "store"
JOIN "sc" AS "sc"
  ON "sc"."ss_store_sk" = "store"."s_store_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "sc"."ss_item_sk"
JOIN "sb" AS "sb"
  ON "sb"."ss_store_sk" = "sc"."ss_store_sk" AND "sc"."revenue" <= 0.1 * "sb"."ave"
ORDER BY
  "s_store_name",
  "i_item_desc"
LIMIT 100;

--------------------------------------
-- TPC-DS 66
--------------------------------------
SELECT w_warehouse_name,
               w_warehouse_sq_ft,
               w_city,
               w_county,
               w_state,
               w_country,
               ship_carriers,
               year1,
               Sum(jan_sales)                     AS jan_sales,
               Sum(feb_sales)                     AS feb_sales,
               Sum(mar_sales)                     AS mar_sales,
               Sum(apr_sales)                     AS apr_sales,
               Sum(may_sales)                     AS may_sales,
               Sum(jun_sales)                     AS jun_sales,
               Sum(jul_sales)                     AS jul_sales,
               Sum(aug_sales)                     AS aug_sales,
               Sum(sep_sales)                     AS sep_sales,
               Sum(oct_sales)                     AS oct_sales,
               Sum(nov_sales)                     AS nov_sales,
               Sum(dec_sales)                     AS dec_sales,
               Sum(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot,
               Sum(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot,
               Sum(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot,
               Sum(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot,
               Sum(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot,
               Sum(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot,
               Sum(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot,
               Sum(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot,
               Sum(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot,
               Sum(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot,
               Sum(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot,
               Sum(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot,
               Sum(jan_net)                       AS jan_net,
               Sum(feb_net)                       AS feb_net,
               Sum(mar_net)                       AS mar_net,
               Sum(apr_net)                       AS apr_net,
               Sum(may_net)                       AS may_net,
               Sum(jun_net)                       AS jun_net,
               Sum(jul_net)                       AS jul_net,
               Sum(aug_net)                       AS aug_net,
               Sum(sep_net)                       AS sep_net,
               Sum(oct_net)                       AS oct_net,
               Sum(nov_net)                       AS nov_net,
               Sum(dec_net)                       AS dec_net
FROM   (SELECT w_warehouse_name,
               w_warehouse_sq_ft,
               w_city,
               w_county,
               w_state,
               w_country,
               'ZOUROS'
               || ','
               || 'ZHOU' AS ship_carriers,
               d_year    AS year1,
               Sum(CASE
                     WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jan_sales,
               Sum(CASE
                     WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS feb_sales,
               Sum(CASE
                     WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS mar_sales,
               Sum(CASE
                     WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS apr_sales,
               Sum(CASE
                     WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS may_sales,
               Sum(CASE
                     WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jun_sales,
               Sum(CASE
                     WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jul_sales,
               Sum(CASE
                     WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS aug_sales,
               Sum(CASE
                     WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS sep_sales,
               Sum(CASE
                     WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS oct_sales,
               Sum(CASE
                     WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS nov_sales,
               Sum(CASE
                     WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS dec_sales,
               Sum(CASE
                     WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jan_net,
               Sum(CASE
                     WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS feb_net,
               Sum(CASE
                     WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS mar_net,
               Sum(CASE
                     WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS apr_net,
               Sum(CASE
                     WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS may_net,
               Sum(CASE
                     WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jun_net,
               Sum(CASE
                     WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jul_net,
               Sum(CASE
                     WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS aug_net,
               Sum(CASE
                     WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS sep_net,
               Sum(CASE
                     WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS oct_net,
               Sum(CASE
                     WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS nov_net,
               Sum(CASE
                     WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS dec_net
        FROM   web_sales,
               warehouse,
               date_dim,
               time_dim,
               ship_mode
        WHERE  ws_warehouse_sk = w_warehouse_sk
               AND ws_sold_date_sk = d_date_sk
               AND ws_sold_time_sk = t_time_sk
               AND ws_ship_mode_sk = sm_ship_mode_sk
               AND d_year = 1998
               AND t_time BETWEEN 7249 AND 7249 + 28800
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' )
        GROUP  BY w_warehouse_name,
                  w_warehouse_sq_ft,
                  w_city,
                  w_county,
                  w_state,
                  w_country,
                  d_year
        UNION ALL
        SELECT w_warehouse_name,
               w_warehouse_sq_ft,
               w_city,
               w_county,
               w_state,
               w_country,
               'ZOUROS'
               || ','
               || 'ZHOU' AS ship_carriers,
               d_year    AS year1,
               Sum(CASE
                     WHEN d_moy = 1 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jan_sales,
               Sum(CASE
                     WHEN d_moy = 2 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS feb_sales,
               Sum(CASE
                     WHEN d_moy = 3 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS mar_sales,
               Sum(CASE
                     WHEN d_moy = 4 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS apr_sales,
               Sum(CASE
                     WHEN d_moy = 5 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS may_sales,
               Sum(CASE
                     WHEN d_moy = 6 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jun_sales,
               Sum(CASE
                     WHEN d_moy = 7 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jul_sales,
               Sum(CASE
                     WHEN d_moy = 8 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS aug_sales,
               Sum(CASE
                     WHEN d_moy = 9 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS sep_sales,
               Sum(CASE
                     WHEN d_moy = 10 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS oct_sales,
               Sum(CASE
                     WHEN d_moy = 11 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS nov_sales,
               Sum(CASE
                     WHEN d_moy = 12 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS dec_sales,
               Sum(CASE
                     WHEN d_moy = 1 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jan_net,
               Sum(CASE
                     WHEN d_moy = 2 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS feb_net,
               Sum(CASE
                     WHEN d_moy = 3 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS mar_net,
               Sum(CASE
                     WHEN d_moy = 4 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS apr_net,
               Sum(CASE
                     WHEN d_moy = 5 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS may_net,
               Sum(CASE
                     WHEN d_moy = 6 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jun_net,
               Sum(CASE
                     WHEN d_moy = 7 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jul_net,
               Sum(CASE
                     WHEN d_moy = 8 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS aug_net,
               Sum(CASE
                     WHEN d_moy = 9 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS sep_net,
               Sum(CASE
                     WHEN d_moy = 10 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS oct_net,
               Sum(CASE
                     WHEN d_moy = 11 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS nov_net,
               Sum(CASE
                     WHEN d_moy = 12 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS dec_net
        FROM   catalog_sales,
               warehouse,
               date_dim,
               time_dim,
               ship_mode
        WHERE  cs_warehouse_sk = w_warehouse_sk
               AND cs_sold_date_sk = d_date_sk
               AND cs_sold_time_sk = t_time_sk
               AND cs_ship_mode_sk = sm_ship_mode_sk
               AND d_year = 1998
               AND t_time BETWEEN 7249 AND 7249 + 28800
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' )
        GROUP  BY w_warehouse_name,
                  w_warehouse_sq_ft,
                  w_city,
                  w_county,
                  w_state,
                  w_country,
                  d_year) x
GROUP  BY w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          ship_carriers,
          year1
ORDER  BY w_warehouse_name
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_year" = 1998
), "ship_mode_2" AS (
  SELECT
    "ship_mode"."sm_ship_mode_sk" AS "sm_ship_mode_sk",
    "ship_mode"."sm_carrier" AS "sm_carrier"
  FROM "ship_mode" AS "ship_mode"
  WHERE
    "ship_mode"."sm_carrier" IN ('ZOUROS', 'ZHOU')
), "time_dim_2" AS (
  SELECT
    "time_dim"."t_time_sk" AS "t_time_sk",
    "time_dim"."t_time" AS "t_time"
  FROM "time_dim" AS "time_dim"
  WHERE
    "time_dim"."t_time" <= 36049 AND "time_dim"."t_time" >= 7249
), "warehouse_2" AS (
  SELECT
    "warehouse"."w_warehouse_sk" AS "w_warehouse_sk",
    "warehouse"."w_warehouse_name" AS "w_warehouse_name",
    "warehouse"."w_warehouse_sq_ft" AS "w_warehouse_sq_ft",
    "warehouse"."w_city" AS "w_city",
    "warehouse"."w_county" AS "w_county",
    "warehouse"."w_state" AS "w_state",
    "warehouse"."w_country" AS "w_country"
  FROM "warehouse" AS "warehouse"
), "x" AS (
  SELECT
    "warehouse"."w_warehouse_name" AS "w_warehouse_name",
    "warehouse"."w_warehouse_sq_ft" AS "w_warehouse_sq_ft",
    "warehouse"."w_city" AS "w_city",
    "warehouse"."w_county" AS "w_county",
    "warehouse"."w_state" AS "w_state",
    "warehouse"."w_country" AS "w_country",
    'ZOUROS,ZHOU' AS "ship_carriers",
    "date_dim"."d_year" AS "year1",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 1
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jan_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 2
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "feb_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 3
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "mar_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 4
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "apr_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 5
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "may_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 6
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jun_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 7
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jul_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 8
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "aug_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 9
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "sep_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 10
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "oct_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 11
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "nov_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 12
        THEN "web_sales"."ws_ext_sales_price" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "dec_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 1
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jan_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 2
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "feb_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 3
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "mar_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 4
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "apr_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 5
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "may_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 6
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jun_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 7
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "jul_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 8
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "aug_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 9
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "sep_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 10
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "oct_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 11
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "nov_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 12
        THEN "web_sales"."ws_net_paid_inc_ship" * "web_sales"."ws_quantity"
        ELSE 0
      END
    ) AS "dec_net"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "ship_mode_2" AS "ship_mode"
    ON "ship_mode"."sm_ship_mode_sk" = "web_sales"."ws_ship_mode_sk"
  JOIN "time_dim_2" AS "time_dim"
    ON "time_dim"."t_time_sk" = "web_sales"."ws_sold_time_sk"
  JOIN "warehouse_2" AS "warehouse"
    ON "warehouse"."w_warehouse_sk" = "web_sales"."ws_warehouse_sk"
  GROUP BY
    "warehouse"."w_warehouse_name",
    "warehouse"."w_warehouse_sq_ft",
    "warehouse"."w_city",
    "warehouse"."w_county",
    "warehouse"."w_state",
    "warehouse"."w_country",
    "date_dim"."d_year"
  UNION ALL
  SELECT
    "warehouse"."w_warehouse_name" AS "w_warehouse_name",
    "warehouse"."w_warehouse_sq_ft" AS "w_warehouse_sq_ft",
    "warehouse"."w_city" AS "w_city",
    "warehouse"."w_county" AS "w_county",
    "warehouse"."w_state" AS "w_state",
    "warehouse"."w_country" AS "w_country",
    'ZOUROS,ZHOU' AS "ship_carriers",
    "date_dim"."d_year" AS "year1",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 1
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jan_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 2
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "feb_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 3
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "mar_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 4
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "apr_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 5
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "may_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 6
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jun_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 7
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jul_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 8
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "aug_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 9
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "sep_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 10
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "oct_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 11
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "nov_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 12
        THEN "catalog_sales"."cs_ext_sales_price" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "dec_sales",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 1
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jan_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 2
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "feb_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 3
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "mar_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 4
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "apr_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 5
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "may_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 6
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jun_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 7
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "jul_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 8
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "aug_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 9
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "sep_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 10
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "oct_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 11
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "nov_net",
    SUM(
      CASE
        WHEN "date_dim"."d_moy" = 12
        THEN "catalog_sales"."cs_net_paid" * "catalog_sales"."cs_quantity"
        ELSE 0
      END
    ) AS "dec_net"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "ship_mode_2" AS "ship_mode"
    ON "catalog_sales"."cs_ship_mode_sk" = "ship_mode"."sm_ship_mode_sk"
  JOIN "time_dim_2" AS "time_dim"
    ON "catalog_sales"."cs_sold_time_sk" = "time_dim"."t_time_sk"
  JOIN "warehouse_2" AS "warehouse"
    ON "catalog_sales"."cs_warehouse_sk" = "warehouse"."w_warehouse_sk"
  GROUP BY
    "warehouse"."w_warehouse_name",
    "warehouse"."w_warehouse_sq_ft",
    "warehouse"."w_city",
    "warehouse"."w_county",
    "warehouse"."w_state",
    "warehouse"."w_country",
    "date_dim"."d_year"
)
SELECT
  "x"."w_warehouse_name" AS "w_warehouse_name",
  "x"."w_warehouse_sq_ft" AS "w_warehouse_sq_ft",
  "x"."w_city" AS "w_city",
  "x"."w_county" AS "w_county",
  "x"."w_state" AS "w_state",
  "x"."w_country" AS "w_country",
  "x"."ship_carriers" AS "ship_carriers",
  "x"."year1" AS "year1",
  SUM("x"."jan_sales") AS "jan_sales",
  SUM("x"."feb_sales") AS "feb_sales",
  SUM("x"."mar_sales") AS "mar_sales",
  SUM("x"."apr_sales") AS "apr_sales",
  SUM("x"."may_sales") AS "may_sales",
  SUM("x"."jun_sales") AS "jun_sales",
  SUM("x"."jul_sales") AS "jul_sales",
  SUM("x"."aug_sales") AS "aug_sales",
  SUM("x"."sep_sales") AS "sep_sales",
  SUM("x"."oct_sales") AS "oct_sales",
  SUM("x"."nov_sales") AS "nov_sales",
  SUM("x"."dec_sales") AS "dec_sales",
  SUM("x"."jan_sales" / "x"."w_warehouse_sq_ft") AS "jan_sales_per_sq_foot",
  SUM("x"."feb_sales" / "x"."w_warehouse_sq_ft") AS "feb_sales_per_sq_foot",
  SUM("x"."mar_sales" / "x"."w_warehouse_sq_ft") AS "mar_sales_per_sq_foot",
  SUM("x"."apr_sales" / "x"."w_warehouse_sq_ft") AS "apr_sales_per_sq_foot",
  SUM("x"."may_sales" / "x"."w_warehouse_sq_ft") AS "may_sales_per_sq_foot",
  SUM("x"."jun_sales" / "x"."w_warehouse_sq_ft") AS "jun_sales_per_sq_foot",
  SUM("x"."jul_sales" / "x"."w_warehouse_sq_ft") AS "jul_sales_per_sq_foot",
  SUM("x"."aug_sales" / "x"."w_warehouse_sq_ft") AS "aug_sales_per_sq_foot",
  SUM("x"."sep_sales" / "x"."w_warehouse_sq_ft") AS "sep_sales_per_sq_foot",
  SUM("x"."oct_sales" / "x"."w_warehouse_sq_ft") AS "oct_sales_per_sq_foot",
  SUM("x"."nov_sales" / "x"."w_warehouse_sq_ft") AS "nov_sales_per_sq_foot",
  SUM("x"."dec_sales" / "x"."w_warehouse_sq_ft") AS "dec_sales_per_sq_foot",
  SUM("x"."jan_net") AS "jan_net",
  SUM("x"."feb_net") AS "feb_net",
  SUM("x"."mar_net") AS "mar_net",
  SUM("x"."apr_net") AS "apr_net",
  SUM("x"."may_net") AS "may_net",
  SUM("x"."jun_net") AS "jun_net",
  SUM("x"."jul_net") AS "jul_net",
  SUM("x"."aug_net") AS "aug_net",
  SUM("x"."sep_net") AS "sep_net",
  SUM("x"."oct_net") AS "oct_net",
  SUM("x"."nov_net") AS "nov_net",
  SUM("x"."dec_net") AS "dec_net"
FROM "x" AS "x"
GROUP BY
  "x"."w_warehouse_name",
  "x"."w_warehouse_sq_ft",
  "x"."w_city",
  "x"."w_county",
  "x"."w_state",
  "x"."w_country",
  "x"."ship_carriers",
  "x"."year1"
ORDER BY
  "w_warehouse_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 67
--------------------------------------
select *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales
                ,date_dim
                ,store
                ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1181 and 1181+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;
WITH "dw1" AS (
  SELECT
    "item"."i_category" AS "i_category",
    "item"."i_class" AS "i_class",
    "item"."i_brand" AS "i_brand",
    "item"."i_product_name" AS "i_product_name",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy",
    "date_dim"."d_moy" AS "d_moy",
    "store"."s_store_id" AS "s_store_id",
    SUM(COALESCE("store_sales"."ss_sales_price" * "store_sales"."ss_quantity", 0)) AS "sumsales"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_month_seq" <= 1192
    AND "date_dim"."d_month_seq" >= 1181
  JOIN "item" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
  ROLLUP (
    "item"."i_category",
    "item"."i_class",
    "item"."i_brand",
    "item"."i_product_name",
    "date_dim"."d_year",
    "date_dim"."d_qoy",
    "date_dim"."d_moy",
    "store"."s_store_id"
  )
), "dw2" AS (
  SELECT
    "dw1"."i_category" AS "i_category",
    "dw1"."i_class" AS "i_class",
    "dw1"."i_brand" AS "i_brand",
    "dw1"."i_product_name" AS "i_product_name",
    "dw1"."d_year" AS "d_year",
    "dw1"."d_qoy" AS "d_qoy",
    "dw1"."d_moy" AS "d_moy",
    "dw1"."s_store_id" AS "s_store_id",
    "dw1"."sumsales" AS "sumsales",
    RANK() OVER (PARTITION BY "dw1"."i_category" ORDER BY "dw1"."sumsales" DESC) AS "rk"
  FROM "dw1" AS "dw1"
)
SELECT
  "dw2"."i_category" AS "i_category",
  "dw2"."i_class" AS "i_class",
  "dw2"."i_brand" AS "i_brand",
  "dw2"."i_product_name" AS "i_product_name",
  "dw2"."d_year" AS "d_year",
  "dw2"."d_qoy" AS "d_qoy",
  "dw2"."d_moy" AS "d_moy",
  "dw2"."s_store_id" AS "s_store_id",
  "dw2"."sumsales" AS "sumsales",
  "dw2"."rk" AS "rk"
FROM "dw2" AS "dw2"
WHERE
  "dw2"."rk" <= 100
ORDER BY
  "dw2"."i_category",
  "dw2"."i_class",
  "dw2"."i_brand",
  "dw2"."i_product_name",
  "dw2"."d_year",
  "dw2"."d_qoy",
  "dw2"."d_moy",
  "dw2"."s_store_id",
  "dw2"."sumsales",
  "dw2"."rk"
LIMIT 100;

--------------------------------------
-- TPC-DS 68
--------------------------------------
SELECT c_last_name,
               c_first_name,
               ca_city,
               bought_city,
               ss_ticket_number,
               extended_price,
               extended_tax,
               list_price
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               ca_city                 bought_city,
               Sum(ss_ext_sales_price) extended_price,
               Sum(ss_ext_list_price)  list_price,
               Sum(ss_ext_tax)         extended_tax
        FROM   store_sales,
               date_dim,
               store,
               household_demographics,
               customer_address
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk
               AND date_dim.d_dom BETWEEN 1 AND 2
               AND ( household_demographics.hd_dep_count = 8
                      OR household_demographics.hd_vehicle_count = 3 )
               AND date_dim.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
               AND store.s_city IN ( 'Fairview', 'Midway' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  ca_city) dn,
       customer,
       customer_address current_addr
WHERE  ss_customer_sk = c_customer_sk
       AND customer.c_current_addr_sk = current_addr.ca_address_sk
       AND current_addr.ca_city <> bought_city
ORDER  BY c_last_name,
          ss_ticket_number
LIMIT 100;
WITH "dn" AS (
  SELECT
    "store_sales"."ss_ticket_number" AS "ss_ticket_number",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    "customer_address"."ca_city" AS "bought_city",
    SUM("store_sales"."ss_ext_sales_price") AS "extended_price",
    SUM("store_sales"."ss_ext_list_price") AS "list_price",
    SUM("store_sales"."ss_ext_tax") AS "extended_tax"
  FROM "store_sales" AS "store_sales"
  JOIN "customer_address" AS "customer_address"
    ON "customer_address"."ca_address_sk" = "store_sales"."ss_addr_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_dom" <= 2
    AND "date_dim"."d_dom" >= 1
    AND "date_dim"."d_year" IN (1998, 1999, 2000)
  JOIN "household_demographics" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND (
      "household_demographics"."hd_dep_count" = 8
      OR "household_demographics"."hd_vehicle_count" = 3
    )
  JOIN "store" AS "store"
    ON "store"."s_city" IN ('Fairview', 'Midway')
    AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store_sales"."ss_ticket_number",
    "store_sales"."ss_customer_sk",
    "store_sales"."ss_addr_sk",
    "customer_address"."ca_city"
)
SELECT
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_first_name" AS "c_first_name",
  "current_addr"."ca_city" AS "ca_city",
  "dn"."bought_city" AS "bought_city",
  "dn"."ss_ticket_number" AS "ss_ticket_number",
  "dn"."extended_price" AS "extended_price",
  "dn"."extended_tax" AS "extended_tax",
  "dn"."list_price" AS "list_price"
FROM "dn" AS "dn"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "dn"."ss_customer_sk"
JOIN "customer_address" AS "current_addr"
  ON "current_addr"."ca_address_sk" = "customer"."c_current_addr_sk"
  AND "current_addr"."ca_city" <> "dn"."bought_city"
ORDER BY
  "c_last_name",
  "ss_ticket_number"
LIMIT 100;

--------------------------------------
-- TPC-DS 69
--------------------------------------
SELECT cd_gender,
               cd_marital_status,
               cd_education_status,
               Count(*) cnt1,
               cd_purchase_estimate,
               Count(*) cnt2,
               cd_credit_rating,
               Count(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'KS', 'AZ', 'NE' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2004
                          AND d_moy BETWEEN 3 AND 3 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 2004
                                AND d_moy BETWEEN 3 AND 3 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 2004
                                    AND d_moy BETWEEN 3 AND 3 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
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
    "date_dim"."d_moy" <= 5 AND "date_dim"."d_moy" >= 3 AND "date_dim"."d_year" = 2004
), "_u_0" AS (
  SELECT
    "store_sales"."ss_customer_sk" AS "_u_1"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_customer_sk"
), "_u_2" AS (
  SELECT
    "web_sales"."ws_bill_customer_sk" AS "_u_3"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "web_sales"."ws_bill_customer_sk"
), "_u_4" AS (
  SELECT
    "catalog_sales"."cs_ship_customer_sk" AS "_u_5"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_ship_customer_sk"
)
SELECT
  "customer_demographics"."cd_gender" AS "cd_gender",
  "customer_demographics"."cd_marital_status" AS "cd_marital_status",
  "customer_demographics"."cd_education_status" AS "cd_education_status",
  COUNT(*) AS "cnt1",
  "customer_demographics"."cd_purchase_estimate" AS "cd_purchase_estimate",
  COUNT(*) AS "cnt2",
  "customer_demographics"."cd_credit_rating" AS "cd_credit_rating",
  COUNT(*) AS "cnt3"
FROM "customer" AS "c"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "c"."c_customer_sk"
LEFT JOIN "_u_2" AS "_u_2"
  ON "_u_2"."_u_3" = "c"."c_customer_sk"
LEFT JOIN "_u_4" AS "_u_4"
  ON "_u_4"."_u_5" = "c"."c_customer_sk"
JOIN "customer_address" AS "ca"
  ON "c"."c_current_addr_sk" = "ca"."ca_address_sk"
  AND "ca"."ca_state" IN ('KS', 'AZ', 'NE')
JOIN "customer_demographics" AS "customer_demographics"
  ON "c"."c_current_cdemo_sk" = "customer_demographics"."cd_demo_sk"
WHERE
  "_u_2"."_u_3" IS NULL AND "_u_4"."_u_5" IS NULL AND NOT "_u_0"."_u_1" IS NULL
GROUP BY
  "customer_demographics"."cd_gender",
  "customer_demographics"."cd_marital_status",
  "customer_demographics"."cd_education_status",
  "customer_demographics"."cd_purchase_estimate",
  "customer_demographics"."cd_credit_rating"
ORDER BY
  "cd_gender",
  "cd_marital_status",
  "cd_education_status",
  "cd_purchase_estimate",
  "cd_credit_rating"
LIMIT 100;

--------------------------------------
-- TPC-DS 70
--------------------------------------
SELECT Sum(ss_net_profit)                     AS total_sum,
               s_state,
               s_county,
               Grouping(s_state) + Grouping(s_county) AS lochierarchy,
               Rank()
                 OVER (
                   partition BY Grouping(s_state)+Grouping(s_county), CASE WHEN
                 Grouping(
                 s_county) = 0 THEN s_state END
                   ORDER BY Sum(ss_net_profit) DESC)  AS rank_within_parent
FROM   store_sales,
       date_dim d1,
       store
WHERE  d1.d_month_seq BETWEEN 1200 AND 1200 + 11
       AND d1.d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN (SELECT s_state
                       FROM   (SELECT s_state                               AS
                                      s_state,
                                      Rank()
                                        OVER (
                                          partition BY s_state
                                          ORDER BY Sum(ss_net_profit) DESC) AS
                                      ranking
                               FROM   store_sales,
                                      store,
                                      date_dim
                               WHERE  d_month_seq BETWEEN 1200 AND 1200 + 11
                                      AND d_date_sk = ss_sold_date_sk
                                      AND s_store_sk = ss_store_sk
                               GROUP  BY s_state) tmp1
                       WHERE  ranking <= 5)
GROUP  BY rollup( s_state, s_county )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN s_state
          END,
          rank_within_parent
LIMIT 100;
WITH "store_sales_2" AS (
  SELECT
    "store_sales"."ss_sold_date_sk" AS "ss_sold_date_sk",
    "store_sales"."ss_store_sk" AS "ss_store_sk",
    "store_sales"."ss_net_profit" AS "ss_net_profit"
  FROM "store_sales" AS "store_sales"
), "tmp1" AS (
  SELECT
    "store"."s_state" AS "s_state",
    RANK() OVER (PARTITION BY "store"."s_state" ORDER BY SUM("store_sales"."ss_net_profit") DESC) AS "ranking"
  FROM "store_sales_2" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_month_seq" <= 1211
    AND "date_dim"."d_month_seq" >= 1200
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store"."s_state"
), "_u_0" AS (
  SELECT
    "tmp1"."s_state" AS "s_state"
  FROM "tmp1" AS "tmp1"
  WHERE
    "tmp1"."ranking" <= 5
  GROUP BY
    "tmp1"."s_state"
)
SELECT
  SUM("store_sales"."ss_net_profit") AS "total_sum",
  "store"."s_state" AS "s_state",
  "store"."s_county" AS "s_county",
  GROUPING("store"."s_state") + GROUPING("store"."s_county") AS "lochierarchy",
  RANK() OVER (PARTITION BY GROUPING("store"."s_state") + GROUPING("store"."s_county"), CASE WHEN GROUPING("store"."s_county") = 0 THEN "store"."s_state" END ORDER BY SUM("store_sales"."ss_net_profit") DESC) AS "rank_within_parent"
FROM "store_sales_2" AS "store_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND "d1"."d_month_seq" <= 1211
  AND "d1"."d_month_seq" >= 1200
JOIN "store" AS "store"
  ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."s_state" = "store"."s_state"
WHERE
  NOT "_u_0"."s_state" IS NULL
GROUP BY
ROLLUP (
  "store"."s_state",
  "store"."s_county"
)
ORDER BY
  "lochierarchy" DESC,
  CASE WHEN "lochierarchy" = 0 THEN "s_state" END,
  "rank_within_parent"
LIMIT 100;

--------------------------------------
-- TPC-DS 71
--------------------------------------
SELECT i_brand_id     brand_id,
       i_brand        brand,
       t_hour,
       t_minute,
       Sum(ext_price) ext_price
FROM   item,
       (SELECT ws_ext_sales_price AS ext_price,
               ws_sold_date_sk    AS sold_date_sk,
               ws_item_sk         AS sold_item_sk,
               ws_sold_time_sk    AS time_sk
        FROM   web_sales,
               date_dim
        WHERE  d_date_sk = ws_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001
        UNION ALL
        SELECT cs_ext_sales_price AS ext_price,
               cs_sold_date_sk    AS sold_date_sk,
               cs_item_sk         AS sold_item_sk,
               cs_sold_time_sk    AS time_sk
        FROM   catalog_sales,
               date_dim
        WHERE  d_date_sk = cs_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001
        UNION ALL
        SELECT ss_ext_sales_price AS ext_price,
               ss_sold_date_sk    AS sold_date_sk,
               ss_item_sk         AS sold_item_sk,
               ss_sold_time_sk    AS time_sk
        FROM   store_sales,
               date_dim
        WHERE  d_date_sk = ss_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001) AS tmp,
       time_dim
WHERE  sold_item_sk = i_item_sk
       AND i_manager_id = 1
       AND time_sk = t_time_sk
       AND ( t_meal_time = 'breakfast'
              OR t_meal_time = 'dinner' )
GROUP  BY i_brand,
          i_brand_id,
          t_hour,
          t_minute
ORDER  BY ext_price DESC,
          i_brand_id;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_moy" AS "d_moy"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_moy" = 11 AND "date_dim"."d_year" = 2001
), "tmp" AS (
  SELECT
    "web_sales"."ws_ext_sales_price" AS "ext_price",
    "web_sales"."ws_item_sk" AS "sold_item_sk",
    "web_sales"."ws_sold_time_sk" AS "time_sk"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  UNION ALL
  SELECT
    "catalog_sales"."cs_ext_sales_price" AS "ext_price",
    "catalog_sales"."cs_item_sk" AS "sold_item_sk",
    "catalog_sales"."cs_sold_time_sk" AS "time_sk"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  UNION ALL
  SELECT
    "store_sales"."ss_ext_sales_price" AS "ext_price",
    "store_sales"."ss_item_sk" AS "sold_item_sk",
    "store_sales"."ss_sold_time_sk" AS "time_sk"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
)
SELECT
  "item"."i_brand_id" AS "brand_id",
  "item"."i_brand" AS "brand",
  "time_dim"."t_hour" AS "t_hour",
  "time_dim"."t_minute" AS "t_minute",
  SUM("tmp"."ext_price") AS "ext_price"
FROM "item" AS "item"
JOIN "tmp" AS "tmp"
  ON "item"."i_item_sk" = "tmp"."sold_item_sk"
JOIN "time_dim" AS "time_dim"
  ON (
    "time_dim"."t_meal_time" = 'breakfast' OR "time_dim"."t_meal_time" = 'dinner'
  )
  AND "time_dim"."t_time_sk" = "tmp"."time_sk"
WHERE
  "item"."i_manager_id" = 1
GROUP BY
  "item"."i_brand",
  "item"."i_brand_id",
  "time_dim"."t_hour",
  "time_dim"."t_minute"
ORDER BY
  "ext_price" DESC,
  "brand_id";

--------------------------------------
-- TPC-DS 72
--------------------------------------
SELECT i_item_desc,
               w_warehouse_name,
               d1.d_week_seq,
               Sum(CASE
                     WHEN p_promo_sk IS NULL THEN 1
                     ELSE 0
                   END) no_promo,
               Sum(CASE
                     WHEN p_promo_sk IS NOT NULL THEN 1
                     ELSE 0
                   END) promo,
               Count(*) total_cnt
FROM   catalog_sales
       JOIN inventory
         ON ( cs_item_sk = inv_item_sk )
       JOIN warehouse
         ON ( w_warehouse_sk = inv_warehouse_sk )
       JOIN item
         ON ( i_item_sk = cs_item_sk )
       JOIN customer_demographics
         ON ( cs_bill_cdemo_sk = cd_demo_sk )
       JOIN household_demographics
         ON ( cs_bill_hdemo_sk = hd_demo_sk )
       JOIN date_dim d1
         ON ( cs_sold_date_sk = d1.d_date_sk )
       JOIN date_dim d2
         ON ( inv_date_sk = d2.d_date_sk )
       JOIN date_dim d3
         ON ( cs_ship_date_sk = d3.d_date_sk )
       LEFT OUTER JOIN promotion
                    ON ( cs_promo_sk = p_promo_sk )
       LEFT OUTER JOIN catalog_returns
                    ON ( cr_item_sk = cs_item_sk
                         AND cr_order_number = cs_order_number )
WHERE  d1.d_week_seq = d2.d_week_seq
       AND inv_quantity_on_hand < cs_quantity
       AND d3.d_date > d1.d_date + INTERVAL '5' day
       AND hd_buy_potential = '501-1000'
       AND d1.d_year = 2002
       AND cd_marital_status = 'M'
GROUP  BY i_item_desc,
          w_warehouse_name,
          d1.d_week_seq
ORDER  BY total_cnt DESC,
          i_item_desc,
          w_warehouse_name,
          d_week_seq
LIMIT 100;
SELECT
  "item"."i_item_desc" AS "i_item_desc",
  "warehouse"."w_warehouse_name" AS "w_warehouse_name",
  "d1"."d_week_seq" AS "d_week_seq",
  SUM(CASE WHEN "promotion"."p_promo_sk" IS NULL THEN 1 ELSE 0 END) AS "no_promo",
  SUM(CASE WHEN NOT "promotion"."p_promo_sk" IS NULL THEN 1 ELSE 0 END) AS "promo",
  COUNT(*) AS "total_cnt"
FROM "catalog_sales" AS "catalog_sales"
LEFT JOIN "catalog_returns" AS "catalog_returns"
  ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
  AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
JOIN "customer_demographics" AS "customer_demographics"
  ON "catalog_sales"."cs_bill_cdemo_sk" = "customer_demographics"."cd_demo_sk"
  AND "customer_demographics"."cd_marital_status" = 'M'
JOIN "date_dim" AS "d3"
  ON "catalog_sales"."cs_ship_date_sk" = "d3"."d_date_sk"
JOIN "household_demographics" AS "household_demographics"
  ON "catalog_sales"."cs_bill_hdemo_sk" = "household_demographics"."hd_demo_sk"
  AND "household_demographics"."hd_buy_potential" = '501-1000'
JOIN "inventory" AS "inventory"
  ON "catalog_sales"."cs_item_sk" = "inventory"."inv_item_sk"
  AND "catalog_sales"."cs_quantity" > "inventory"."inv_quantity_on_hand"
JOIN "item" AS "item"
  ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
LEFT JOIN "promotion" AS "promotion"
  ON "catalog_sales"."cs_promo_sk" = "promotion"."p_promo_sk"
JOIN "date_dim" AS "d2"
  ON "d2"."d_date_sk" = "inventory"."inv_date_sk"
JOIN "warehouse" AS "warehouse"
  ON "inventory"."inv_warehouse_sk" = "warehouse"."w_warehouse_sk"
JOIN "date_dim" AS "d1"
  ON "catalog_sales"."cs_sold_date_sk" = "d1"."d_date_sk"
  AND "d1"."d_week_seq" = "d2"."d_week_seq"
  AND "d1"."d_year" = 2002
  AND "d3"."d_date" > "d1"."d_date" + INTERVAL '5' DAY
GROUP BY
  "item"."i_item_desc",
  "warehouse"."w_warehouse_name",
  "d1"."d_week_seq"
ORDER BY
  "total_cnt" DESC,
  "i_item_desc",
  "w_warehouse_name",
  "d_week_seq"
LIMIT 100;

--------------------------------------
-- TPC-DS 73
--------------------------------------
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               Count(*) cnt
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND date_dim.d_dom BETWEEN 1 AND 2
               AND ( household_demographics.hd_buy_potential = '>10000'
                      OR household_demographics.hd_buy_potential = '0-500' )
               AND household_demographics.hd_vehicle_count > 0
               AND CASE
                     WHEN household_demographics.hd_vehicle_count > 0 THEN
                     household_demographics.hd_dep_count /
                     household_demographics.hd_vehicle_count
                     ELSE NULL
                   END > 1
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 )
               AND store.s_county IN ( 'Williamson County', 'Williamson County',
                                       'Williamson County',
                                                             'Williamson County'
                                     )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dj,
       customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 1 AND 5
ORDER  BY cnt DESC,
          c_last_name ASC;
WITH "dj" AS (
  SELECT
    "store_sales"."ss_ticket_number" AS "ss_ticket_number",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    COUNT(*) AS "cnt"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_dom" <= 2
    AND "date_dim"."d_dom" >= 1
    AND "date_dim"."d_year" IN (2000, 2001, 2002)
  JOIN "household_demographics" AS "household_demographics"
    ON (
      "household_demographics"."hd_buy_potential" = '0-500'
      OR "household_demographics"."hd_buy_potential" = '>10000'
    )
    AND "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND "household_demographics"."hd_vehicle_count" > 0
    AND CASE
      WHEN "household_demographics"."hd_vehicle_count" > 0
      THEN "household_demographics"."hd_dep_count" / "household_demographics"."hd_vehicle_count"
      ELSE NULL
    END > 1
  JOIN "store" AS "store"
    ON "store"."s_county" IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
    AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store_sales"."ss_ticket_number",
    "store_sales"."ss_customer_sk"
)
SELECT
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_first_name" AS "c_first_name",
  "customer"."c_salutation" AS "c_salutation",
  "customer"."c_preferred_cust_flag" AS "c_preferred_cust_flag",
  "dj"."ss_ticket_number" AS "ss_ticket_number",
  "dj"."cnt" AS "cnt"
FROM "dj" AS "dj"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "dj"."ss_customer_sk"
WHERE
  "dj"."cnt" <= 5 AND "dj"."cnt" >= 1
ORDER BY
  "cnt" DESC,
  "c_last_name";

--------------------------------------
-- TPC-DS 74
--------------------------------------
WITH year_total
     AS (SELECT c_customer_id    customer_id,
                c_first_name     customer_first_name,
                c_last_name      customer_last_name,
                d_year           AS year1,
                Sum(ss_net_paid) year_total,
                's'              sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year IN ( 1999, 1999 + 1 )
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   d_year
         UNION ALL
         SELECT c_customer_id    customer_id,
                c_first_name     customer_first_name,
                c_last_name      customer_last_name,
                d_year           AS year1,
                Sum(ws_net_paid) year_total,
                'w'              sale_type
         FROM   customer,
                web_sales,
                date_dim
         WHERE  c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year IN ( 1999, 1999 + 1 )
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   d_year)
SELECT t_s_secyear.customer_id,
               t_s_secyear.customer_first_name,
               t_s_secyear.customer_last_name
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
       AND t_s_firstyear.year1 = 1999
       AND t_s_secyear.year1 = 1999 + 1
       AND t_w_firstyear.year1 = 1999
       AND t_w_secyear.year1 = 1999 + 1
       AND t_s_firstyear.year_total > 0
       AND t_w_firstyear.year_total > 0
       AND CASE
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total /
                                                    t_w_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN
                   t_s_secyear.year_total /
                   t_s_firstyear.year_total
                   ELSE NULL
                 END
ORDER  BY 1,
          2,
          3
LIMIT 100;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_customer_id" AS "c_customer_id",
    "customer"."c_first_name" AS "c_first_name",
    "customer"."c_last_name" AS "c_last_name"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_year" IN (1999, 2000)
), "year_total" AS (
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "date_dim"."d_year" AS "year1",
    SUM("store_sales"."ss_net_paid") AS "year_total",
    's' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "store_sales" AS "store_sales"
    ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "date_dim"."d_year"
  UNION ALL
  SELECT
    "customer"."c_customer_id" AS "customer_id",
    "customer"."c_first_name" AS "customer_first_name",
    "customer"."c_last_name" AS "customer_last_name",
    "date_dim"."d_year" AS "year1",
    SUM("web_sales"."ws_net_paid") AS "year_total",
    'w' AS "sale_type"
  FROM "customer_2" AS "customer"
  JOIN "web_sales" AS "web_sales"
    ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "customer"."c_customer_id",
    "customer"."c_first_name",
    "customer"."c_last_name",
    "date_dim"."d_year"
)
SELECT
  "t_s_secyear"."customer_id" AS "customer_id",
  "t_s_secyear"."customer_first_name" AS "customer_first_name",
  "t_s_secyear"."customer_last_name" AS "customer_last_name"
FROM "year_total" AS "t_s_firstyear"
JOIN "year_total" AS "t_w_firstyear"
  ON "t_s_firstyear"."customer_id" = "t_w_firstyear"."customer_id"
  AND "t_w_firstyear"."sale_type" = 'w'
  AND "t_w_firstyear"."year1" = 1999
  AND "t_w_firstyear"."year_total" > 0
JOIN "year_total" AS "t_w_secyear"
  ON "t_s_firstyear"."customer_id" = "t_w_secyear"."customer_id"
  AND "t_w_secyear"."sale_type" = 'w'
  AND "t_w_secyear"."year1" = 2000
JOIN "year_total" AS "t_s_secyear"
  ON "t_s_firstyear"."customer_id" = "t_s_secyear"."customer_id"
  AND "t_s_secyear"."sale_type" = 's'
  AND "t_s_secyear"."year1" = 2000
  AND CASE
    WHEN "t_s_firstyear"."year_total" > 0
    THEN "t_s_secyear"."year_total" / "t_s_firstyear"."year_total"
    ELSE NULL
  END < CASE
    WHEN "t_w_firstyear"."year_total" > 0
    THEN "t_w_secyear"."year_total" / "t_w_firstyear"."year_total"
    ELSE NULL
  END
WHERE
  "t_s_firstyear"."sale_type" = 's'
  AND "t_s_firstyear"."year1" = 1999
  AND "t_s_firstyear"."year_total" > 0
ORDER BY
  "customer_id",
  "customer_first_name",
  "customer_last_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 75
--------------------------------------
WITH all_sales
     AS (SELECT d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                Sum(sales_cnt) AS sales_cnt,
                Sum(sales_amt) AS sales_amt
         FROM   (SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        cs_quantity - COALESCE(cr_return_quantity, 0)        AS
                        sales_cnt,
                        cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS
                        sales_amt
                 FROM   catalog_sales
                        JOIN item
                          ON i_item_sk = cs_item_sk
                        JOIN date_dim
                          ON d_date_sk = cs_sold_date_sk
                        LEFT JOIN catalog_returns
                               ON ( cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ss_quantity - COALESCE(sr_return_quantity, 0)     AS
                        sales_cnt,
                        ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS
                        sales_amt
                 FROM   store_sales
                        JOIN item
                          ON i_item_sk = ss_item_sk
                        JOIN date_dim
                          ON d_date_sk = ss_sold_date_sk
                        LEFT JOIN store_returns
                               ON ( ss_ticket_number = sr_ticket_number
                                    AND ss_item_sk = sr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ws_quantity - COALESCE(wr_return_quantity, 0)     AS
                        sales_cnt,
                        ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS
                        sales_amt
                 FROM   web_sales
                        JOIN item
                          ON i_item_sk = ws_item_sk
                        JOIN date_dim
                          ON d_date_sk = ws_sold_date_sk
                        LEFT JOIN web_returns
                               ON ( ws_order_number = wr_order_number
                                    AND ws_item_sk = wr_item_sk )
                 WHERE  i_category = 'Men') sales_detail
         GROUP  BY d_year,
                   i_brand_id,
                   i_class_id,
                   i_category_id,
                   i_manufact_id)
SELECT prev_yr.d_year                        AS prev_year,
               curr_yr.d_year                        AS year1,
               curr_yr.i_brand_id,
               curr_yr.i_class_id,
               curr_yr.i_category_id,
               curr_yr.i_manufact_id,
               prev_yr.sales_cnt                     AS prev_yr_cnt,
               curr_yr.sales_cnt                     AS curr_yr_cnt,
               curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
               curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM   all_sales curr_yr,
       all_sales prev_yr
WHERE  curr_yr.i_brand_id = prev_yr.i_brand_id
       AND curr_yr.i_class_id = prev_yr.i_class_id
       AND curr_yr.i_category_id = prev_yr.i_category_id
       AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
       AND curr_yr.d_year = 2002
       AND prev_yr.d_year = 2002 - 1
       AND Cast(curr_yr.sales_cnt AS DECIMAL(17, 2)) / Cast(prev_yr.sales_cnt AS
                                                                DECIMAL(17, 2))
           < 0.9
ORDER  BY sales_cnt_diff
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    "item"."i_category" AS "i_category",
    "item"."i_manufact_id" AS "i_manufact_id"
  FROM "item" AS "item"
  WHERE
    "item"."i_category" = 'Men'
), "sales_detail" AS (
  SELECT
    "date_dim"."d_year" AS "d_year",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    "item"."i_manufact_id" AS "i_manufact_id",
    "catalog_sales"."cs_quantity" - COALESCE("catalog_returns"."cr_return_quantity", 0) AS "sales_cnt",
    "catalog_sales"."cs_ext_sales_price" - COALESCE("catalog_returns"."cr_return_amount", 0.0) AS "sales_amt"
  FROM "catalog_sales" AS "catalog_sales"
  LEFT JOIN "catalog_returns" AS "catalog_returns"
    ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
    AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  UNION
  SELECT
    "date_dim"."d_year" AS "d_year",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    "item"."i_manufact_id" AS "i_manufact_id",
    "store_sales"."ss_quantity" - COALESCE("store_returns"."sr_return_quantity", 0) AS "sales_cnt",
    "store_sales"."ss_ext_sales_price" - COALESCE("store_returns"."sr_return_amt", 0.0) AS "sales_amt"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  LEFT JOIN "store_returns" AS "store_returns"
    ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
    AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
  UNION
  SELECT
    "date_dim"."d_year" AS "d_year",
    "item"."i_brand_id" AS "i_brand_id",
    "item"."i_class_id" AS "i_class_id",
    "item"."i_category_id" AS "i_category_id",
    "item"."i_manufact_id" AS "i_manufact_id",
    "web_sales"."ws_quantity" - COALESCE("web_returns"."wr_return_quantity", 0) AS "sales_cnt",
    "web_sales"."ws_ext_sales_price" - COALESCE("web_returns"."wr_return_amt", 0.0) AS "sales_amt"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  LEFT JOIN "web_returns" AS "web_returns"
    ON "web_returns"."wr_item_sk" = "web_sales"."ws_item_sk"
    AND "web_returns"."wr_order_number" = "web_sales"."ws_order_number"
), "all_sales" AS (
  SELECT
    "sales_detail"."d_year" AS "d_year",
    "sales_detail"."i_brand_id" AS "i_brand_id",
    "sales_detail"."i_class_id" AS "i_class_id",
    "sales_detail"."i_category_id" AS "i_category_id",
    "sales_detail"."i_manufact_id" AS "i_manufact_id",
    SUM("sales_detail"."sales_cnt") AS "sales_cnt",
    SUM("sales_detail"."sales_amt") AS "sales_amt"
  FROM "sales_detail" AS "sales_detail"
  GROUP BY
    "sales_detail"."d_year",
    "sales_detail"."i_brand_id",
    "sales_detail"."i_class_id",
    "sales_detail"."i_category_id",
    "sales_detail"."i_manufact_id"
)
SELECT
  "prev_yr"."d_year" AS "prev_year",
  "curr_yr"."d_year" AS "year1",
  "curr_yr"."i_brand_id" AS "i_brand_id",
  "curr_yr"."i_class_id" AS "i_class_id",
  "curr_yr"."i_category_id" AS "i_category_id",
  "curr_yr"."i_manufact_id" AS "i_manufact_id",
  "prev_yr"."sales_cnt" AS "prev_yr_cnt",
  "curr_yr"."sales_cnt" AS "curr_yr_cnt",
  "curr_yr"."sales_cnt" - "prev_yr"."sales_cnt" AS "sales_cnt_diff",
  "curr_yr"."sales_amt" - "prev_yr"."sales_amt" AS "sales_amt_diff"
FROM "all_sales" AS "curr_yr"
JOIN "all_sales" AS "prev_yr"
  ON "curr_yr"."i_brand_id" = "prev_yr"."i_brand_id"
  AND "curr_yr"."i_category_id" = "prev_yr"."i_category_id"
  AND "curr_yr"."i_class_id" = "prev_yr"."i_class_id"
  AND "curr_yr"."i_manufact_id" = "prev_yr"."i_manufact_id"
  AND "prev_yr"."d_year" = 2001
  AND CAST("curr_yr"."sales_cnt" AS DECIMAL(17, 2)) / CAST("prev_yr"."sales_cnt" AS DECIMAL(17, 2)) < 0.9
WHERE
  "curr_yr"."d_year" = 2002
ORDER BY
  "sales_cnt_diff"
LIMIT 100;

--------------------------------------
-- TPC-DS 76
--------------------------------------
SELECT channel,
               col_name,
               d_year,
               d_qoy,
               i_category,
               Count(*)             sales_cnt,
               Sum(ext_sales_price) sales_amt
FROM   (SELECT 'store'            AS channel,
               'ss_hdemo_sk'      col_name,
               d_year,
               d_qoy,
               i_category,
               ss_ext_sales_price ext_sales_price
        FROM   store_sales,
               item,
               date_dim
        WHERE  ss_hdemo_sk IS NULL
               AND ss_sold_date_sk = d_date_sk
               AND ss_item_sk = i_item_sk
        UNION ALL
        SELECT 'web'              AS channel,
               'ws_ship_hdemo_sk' col_name,
               d_year,
               d_qoy,
               i_category,
               ws_ext_sales_price ext_sales_price
        FROM   web_sales,
               item,
               date_dim
        WHERE  ws_ship_hdemo_sk IS NULL
               AND ws_sold_date_sk = d_date_sk
               AND ws_item_sk = i_item_sk
        UNION ALL
        SELECT 'catalog'          AS channel,
               'cs_warehouse_sk'  col_name,
               d_year,
               d_qoy,
               i_category,
               cs_ext_sales_price ext_sales_price
        FROM   catalog_sales,
               item,
               date_dim
        WHERE  cs_warehouse_sk IS NULL
               AND cs_sold_date_sk = d_date_sk
               AND cs_item_sk = i_item_sk) foo
GROUP  BY channel,
          col_name,
          d_year,
          d_qoy,
          i_category
ORDER  BY channel,
          col_name,
          d_year,
          d_qoy,
          i_category
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy"
  FROM "date_dim" AS "date_dim"
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_category" AS "i_category"
  FROM "item" AS "item"
), "foo" AS (
  SELECT
    'store' AS "channel",
    'ss_hdemo_sk' AS "col_name",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy",
    "item"."i_category" AS "i_category",
    "store_sales"."ss_ext_sales_price" AS "ext_sales_price"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  WHERE
    "store_sales"."ss_hdemo_sk" IS NULL
  UNION ALL
  SELECT
    'web' AS "channel",
    'ws_ship_hdemo_sk' AS "col_name",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy",
    "item"."i_category" AS "i_category",
    "web_sales"."ws_ext_sales_price" AS "ext_sales_price"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  WHERE
    "web_sales"."ws_ship_hdemo_sk" IS NULL
  UNION ALL
  SELECT
    'catalog' AS "channel",
    'cs_warehouse_sk' AS "col_name",
    "date_dim"."d_year" AS "d_year",
    "date_dim"."d_qoy" AS "d_qoy",
    "item"."i_category" AS "i_category",
    "catalog_sales"."cs_ext_sales_price" AS "ext_sales_price"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  WHERE
    "catalog_sales"."cs_warehouse_sk" IS NULL
)
SELECT
  "foo"."channel" AS "channel",
  "foo"."col_name" AS "col_name",
  "foo"."d_year" AS "d_year",
  "foo"."d_qoy" AS "d_qoy",
  "foo"."i_category" AS "i_category",
  COUNT(*) AS "sales_cnt",
  SUM("foo"."ext_sales_price") AS "sales_amt"
FROM "foo" AS "foo"
GROUP BY
  "foo"."channel",
  "foo"."col_name",
  "foo"."d_year",
  "foo"."d_qoy",
  "foo"."i_category"
ORDER BY
  "channel",
  "col_name",
  "d_year",
  "d_qoy",
  "i_category"
LIMIT 100;

--------------------------------------
-- TPC-DS 77
--------------------------------------
WITH ss AS
(
         SELECT   s_store_sk,
                  Sum(ss_ext_sales_price) AS sales,
                  Sum(ss_net_profit)      AS profit
         FROM     store_sales,
                  date_dim,
                  store
         WHERE    ss_sold_date_sk = d_date_sk
         AND      d_date BETWEEN Cast('2001-08-16' AS DATE) AND      (
                           Cast('2001-08-16' AS DATE) + INTERVAL '30' day)
         AND      ss_store_sk = s_store_sk
         GROUP BY s_store_sk) , sr AS
(
         SELECT   s_store_sk,
                  sum(sr_return_amt) AS returns1,
                  sum(sr_net_loss)   AS profit_loss
         FROM     store_returns,
                  date_dim,
                  store
         WHERE    sr_returned_date_sk = d_date_sk
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      (
                           cast('2001-08-16' AS date) + INTERVAL '30' day)
         AND      sr_store_sk = s_store_sk
         GROUP BY s_store_sk), cs AS
(
         SELECT   cs_call_center_sk,
                  sum(cs_ext_sales_price) AS sales,
                  sum(cs_net_profit)      AS profit
         FROM     catalog_sales,
                  date_dim
         WHERE    cs_sold_date_sk = d_date_sk
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      (
                           cast('2001-08-16' AS date) + INTERVAL '30' day)
         GROUP BY cs_call_center_sk ), cr AS
(
         SELECT   cr_call_center_sk,
                  sum(cr_return_amount) AS returns1,
                  sum(cr_net_loss)      AS profit_loss
         FROM     catalog_returns,
                  date_dim
         WHERE    cr_returned_date_sk = d_date_sk
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      (
                           cast('2001-08-16' AS date) + INTERVAL '30' day)
         GROUP BY cr_call_center_sk ), ws AS
(
         SELECT   wp_web_page_sk,
                  sum(ws_ext_sales_price) AS sales,
                  sum(ws_net_profit)      AS profit
         FROM     web_sales,
                  date_dim,
                  web_page
         WHERE    ws_sold_date_sk = d_date_sk
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      (
                           cast('2001-08-16' AS date) + INTERVAL '30' day)
         AND      ws_web_page_sk = wp_web_page_sk
         GROUP BY wp_web_page_sk), wr AS
(
         SELECT   wp_web_page_sk,
                  sum(wr_return_amt) AS returns1,
                  sum(wr_net_loss)   AS profit_loss
         FROM     web_returns,
                  date_dim,
                  web_page
         WHERE    wr_returned_date_sk = d_date_sk
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      (
                           cast('2001-08-16' AS date) + INTERVAL '30' day)
         AND      wr_web_page_sk = wp_web_page_sk
         GROUP BY wp_web_page_sk)
SELECT
         channel ,
         id ,
         sum(sales)   AS sales ,
         sum(returns1) AS returns1 ,
         sum(profit)  AS profit
FROM     (
                   SELECT    'store channel' AS channel ,
                             ss.s_store_sk   AS id ,
                             sales ,
                             COALESCE(returns1, 0)               AS returns1 ,
                             (profit - COALESCE(profit_loss,0)) AS profit
                   FROM      ss
                   LEFT JOIN sr
                   ON        ss.s_store_sk = sr.s_store_sk
                   UNION ALL
                   SELECT 'catalog channel' AS channel ,
                          cs_call_center_sk AS id ,
                          sales ,
                          returns1 ,
                          (profit - profit_loss) AS profit
                   FROM   cs ,
                          cr
                   UNION ALL
                   SELECT    'web channel'     AS channel ,
                             ws.wp_web_page_sk AS id ,
                             sales ,
                             COALESCE(returns1, 0)                  returns1 ,
                             (profit - COALESCE(profit_loss,0)) AS profit
                   FROM      ws
                   LEFT JOIN wr
                   ON        ws.wp_web_page_sk = wr.wp_web_page_sk ) x
GROUP BY rollup (channel, id)
ORDER BY channel ,
         id
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  WHERE
    CAST("date_dim"."d_date" AS DATE) <= CAST('2001-09-15' AS DATE)
    AND CAST("date_dim"."d_date" AS DATE) >= CAST('2001-08-16' AS DATE)
), "store_2" AS (
  SELECT
    "store"."s_store_sk" AS "s_store_sk"
  FROM "store" AS "store"
), "ss" AS (
  SELECT
    "store"."s_store_sk" AS "s_store_sk",
    SUM("store_sales"."ss_ext_sales_price") AS "sales",
    SUM("store_sales"."ss_net_profit") AS "profit"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store"."s_store_sk"
), "sr" AS (
  SELECT
    "store"."s_store_sk" AS "s_store_sk",
    SUM("store_returns"."sr_return_amt") AS "returns1",
    SUM("store_returns"."sr_net_loss") AS "profit_loss"
  FROM "store_returns" AS "store_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_returns"."sr_store_sk"
  GROUP BY
    "store"."s_store_sk"
), "cs" AS (
  SELECT
    "catalog_sales"."cs_call_center_sk" AS "cs_call_center_sk",
    SUM("catalog_sales"."cs_ext_sales_price") AS "sales",
    SUM("catalog_sales"."cs_net_profit") AS "profit"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_call_center_sk"
), "cr" AS (
  SELECT
    SUM("catalog_returns"."cr_return_amount") AS "returns1",
    SUM("catalog_returns"."cr_net_loss") AS "profit_loss"
  FROM "catalog_returns" AS "catalog_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_returns"."cr_returned_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_returns"."cr_call_center_sk"
), "web_page_2" AS (
  SELECT
    "web_page"."wp_web_page_sk" AS "wp_web_page_sk"
  FROM "web_page" AS "web_page"
), "ws" AS (
  SELECT
    "web_page"."wp_web_page_sk" AS "wp_web_page_sk",
    SUM("web_sales"."ws_ext_sales_price") AS "sales",
    SUM("web_sales"."ws_net_profit") AS "profit"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "web_page_2" AS "web_page"
    ON "web_page"."wp_web_page_sk" = "web_sales"."ws_web_page_sk"
  GROUP BY
    "web_page"."wp_web_page_sk"
), "wr" AS (
  SELECT
    "web_page"."wp_web_page_sk" AS "wp_web_page_sk",
    SUM("web_returns"."wr_return_amt") AS "returns1",
    SUM("web_returns"."wr_net_loss") AS "profit_loss"
  FROM "web_returns" AS "web_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_returns"."wr_returned_date_sk"
  JOIN "web_page_2" AS "web_page"
    ON "web_page"."wp_web_page_sk" = "web_returns"."wr_web_page_sk"
  GROUP BY
    "web_page"."wp_web_page_sk"
), "x" AS (
  SELECT
    'store channel' AS "channel",
    "ss"."s_store_sk" AS "id",
    "ss"."sales" AS "sales",
    COALESCE("sr"."returns1", 0) AS "returns1",
    "ss"."profit" - COALESCE("sr"."profit_loss", 0) AS "profit"
  FROM "ss"
  LEFT JOIN "sr"
    ON "sr"."s_store_sk" = "ss"."s_store_sk"
  UNION ALL
  SELECT
    'catalog channel' AS "channel",
    "cs"."cs_call_center_sk" AS "id",
    "cs"."sales" AS "sales",
    "cr"."returns1" AS "returns1",
    "cs"."profit" - "cr"."profit_loss" AS "profit"
  FROM "cs"
  CROSS JOIN "cr"
  UNION ALL
  SELECT
    'web channel' AS "channel",
    "ws"."wp_web_page_sk" AS "id",
    "ws"."sales" AS "sales",
    COALESCE("wr"."returns1", 0) AS "returns1",
    "ws"."profit" - COALESCE("wr"."profit_loss", 0) AS "profit"
  FROM "ws"
  LEFT JOIN "wr"
    ON "wr"."wp_web_page_sk" = "ws"."wp_web_page_sk"
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
-- TPC-DS 78
--------------------------------------
WITH ws
     AS (SELECT d_year                 AS ws_sold_year,
                ws_item_sk,
                ws_bill_customer_sk    ws_customer_sk,
                Sum(ws_quantity)       ws_qty,
                Sum(ws_wholesale_cost) ws_wc,
                Sum(ws_sales_price)    ws_sp
         FROM   web_sales
                LEFT JOIN web_returns
                       ON wr_order_number = ws_order_number
                          AND ws_item_sk = wr_item_sk
                JOIN date_dim
                  ON ws_sold_date_sk = d_date_sk
         WHERE  wr_order_number IS NULL
         GROUP  BY d_year,
                   ws_item_sk,
                   ws_bill_customer_sk),
     cs
     AS (SELECT d_year                 AS cs_sold_year,
                cs_item_sk,
                cs_bill_customer_sk    cs_customer_sk,
                Sum(cs_quantity)       cs_qty,
                Sum(cs_wholesale_cost) cs_wc,
                Sum(cs_sales_price)    cs_sp
         FROM   catalog_sales
                LEFT JOIN catalog_returns
                       ON cr_order_number = cs_order_number
                          AND cs_item_sk = cr_item_sk
                JOIN date_dim
                  ON cs_sold_date_sk = d_date_sk
         WHERE  cr_order_number IS NULL
         GROUP  BY d_year,
                   cs_item_sk,
                   cs_bill_customer_sk),
     ss
     AS (SELECT d_year                 AS ss_sold_year,
                ss_item_sk,
                ss_customer_sk,
                Sum(ss_quantity)       ss_qty,
                Sum(ss_wholesale_cost) ss_wc,
                Sum(ss_sales_price)    ss_sp
         FROM   store_sales
                LEFT JOIN store_returns
                       ON sr_ticket_number = ss_ticket_number
                          AND ss_item_sk = sr_item_sk
                JOIN date_dim
                  ON ss_sold_date_sk = d_date_sk
         WHERE  sr_ticket_number IS NULL
         GROUP  BY d_year,
                   ss_item_sk,
                   ss_customer_sk)
SELECT ss_item_sk,
               Round(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2) ratio,
               ss_qty                                              store_qty,
               ss_wc
               store_wholesale_cost,
               ss_sp
               store_sales_price,
               COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)
               other_chan_qty,
               COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0)
               other_chan_wholesale_cost,
               COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0)
               other_chan_sales_price
FROM   ss
       LEFT JOIN ws
              ON ( ws_sold_year = ss_sold_year
                   AND ws_item_sk = ss_item_sk
                   AND ws_customer_sk = ss_customer_sk )
       LEFT JOIN cs
              ON ( cs_sold_year = ss_sold_year
                   AND cs_item_sk = cs_item_sk
                   AND cs_customer_sk = ss_customer_sk )
WHERE  COALESCE(ws_qty, 0) > 0
       AND COALESCE(cs_qty, 0) > 0
       AND ss_sold_year = 1999
ORDER  BY ss_item_sk,
          ss_qty DESC,
          ss_wc DESC,
          ss_sp DESC,
          other_chan_qty,
          other_chan_wholesale_cost,
          other_chan_sales_price,
          Round(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2)
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_year" AS "d_year"
  FROM "date_dim" AS "date_dim"
), "ws" AS (
  SELECT
    "date_dim"."d_year" AS "ws_sold_year",
    "web_sales"."ws_item_sk" AS "ws_item_sk",
    "web_sales"."ws_bill_customer_sk" AS "ws_customer_sk",
    SUM("web_sales"."ws_quantity") AS "ws_qty",
    SUM("web_sales"."ws_wholesale_cost") AS "ws_wc",
    SUM("web_sales"."ws_sales_price") AS "ws_sp"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  LEFT JOIN "web_returns" AS "web_returns"
    ON "web_returns"."wr_item_sk" = "web_sales"."ws_item_sk"
    AND "web_returns"."wr_order_number" = "web_sales"."ws_order_number"
  WHERE
    "web_returns"."wr_order_number" IS NULL
  GROUP BY
    "date_dim"."d_year",
    "web_sales"."ws_item_sk",
    "web_sales"."ws_bill_customer_sk"
), "cs" AS (
  SELECT
    "date_dim"."d_year" AS "cs_sold_year",
    "catalog_sales"."cs_item_sk" AS "cs_item_sk",
    "catalog_sales"."cs_bill_customer_sk" AS "cs_customer_sk",
    SUM("catalog_sales"."cs_quantity") AS "cs_qty",
    SUM("catalog_sales"."cs_wholesale_cost") AS "cs_wc",
    SUM("catalog_sales"."cs_sales_price") AS "cs_sp"
  FROM "catalog_sales" AS "catalog_sales"
  LEFT JOIN "catalog_returns" AS "catalog_returns"
    ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
    AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  WHERE
    "catalog_returns"."cr_order_number" IS NULL
  GROUP BY
    "date_dim"."d_year",
    "catalog_sales"."cs_item_sk",
    "catalog_sales"."cs_bill_customer_sk"
), "ss" AS (
  SELECT
    "date_dim"."d_year" AS "ss_sold_year",
    "store_sales"."ss_item_sk" AS "ss_item_sk",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    SUM("store_sales"."ss_quantity") AS "ss_qty",
    SUM("store_sales"."ss_wholesale_cost") AS "ss_wc",
    SUM("store_sales"."ss_sales_price") AS "ss_sp"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  LEFT JOIN "store_returns" AS "store_returns"
    ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
    AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
  WHERE
    "store_returns"."sr_ticket_number" IS NULL
  GROUP BY
    "date_dim"."d_year",
    "store_sales"."ss_item_sk",
    "store_sales"."ss_customer_sk"
)
SELECT
  "ss"."ss_item_sk" AS "ss_item_sk",
  ROUND("ss"."ss_qty" / COALESCE("ws"."ws_qty" + "cs"."cs_qty", 1), 2) AS "ratio",
  "ss"."ss_qty" AS "store_qty",
  "ss"."ss_wc" AS "store_wholesale_cost",
  "ss"."ss_sp" AS "store_sales_price",
  COALESCE("ws"."ws_qty", 0) + COALESCE("cs"."cs_qty", 0) AS "other_chan_qty",
  COALESCE("ws"."ws_wc", 0) + COALESCE("cs"."cs_wc", 0) AS "other_chan_wholesale_cost",
  COALESCE("ws"."ws_sp", 0) + COALESCE("cs"."cs_sp", 0) AS "other_chan_sales_price"
FROM "ss"
LEFT JOIN "cs"
  ON "cs"."cs_customer_sk" = "ss"."ss_customer_sk"
  AND "cs"."cs_item_sk" = "cs"."cs_item_sk"
  AND "cs"."cs_sold_year" = "ss"."ss_sold_year"
LEFT JOIN "ws"
  ON "ss"."ss_customer_sk" = "ws"."ws_customer_sk"
  AND "ss"."ss_item_sk" = "ws"."ws_item_sk"
  AND "ss"."ss_sold_year" = "ws"."ws_sold_year"
WHERE
  "cs"."cs_qty" > 0
  AND "ss"."ss_sold_year" = 1999
  AND "ws"."ws_qty" > 0
  AND NOT "cs"."cs_qty" IS NULL
  AND NOT "ws"."ws_qty" IS NULL
ORDER BY
  "ss_item_sk",
  "ss"."ss_qty" DESC,
  "ss"."ss_wc" DESC,
  "ss"."ss_sp" DESC,
  "other_chan_qty",
  "other_chan_wholesale_cost",
  "other_chan_sales_price",
  ROUND("ss"."ss_qty" / COALESCE("ws"."ws_qty" + "cs"."cs_qty", 1), 2)
LIMIT 100;

--------------------------------------
-- TPC-DS 79
--------------------------------------
SELECT c_last_name,
               c_first_name,
               Substr(s_city, 1, 30),
               ss_ticket_number,
               amt,
               profit
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               store.s_city,
               Sum(ss_coupon_amt) amt,
               Sum(ss_net_profit) profit
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ( household_demographics.hd_dep_count = 8
                      OR household_demographics.hd_vehicle_count > 4 )
               AND date_dim.d_dow = 1
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 )
               AND store.s_number_employees BETWEEN 200 AND 295
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  store.s_city) ms,
       customer
WHERE  ss_customer_sk = c_customer_sk
ORDER  BY c_last_name,
          c_first_name,
          Substr(s_city, 1, 30),
          profit
LIMIT 100;
WITH "ms" AS (
  SELECT
    "store_sales"."ss_ticket_number" AS "ss_ticket_number",
    "store_sales"."ss_customer_sk" AS "ss_customer_sk",
    "store"."s_city" AS "s_city",
    SUM("store_sales"."ss_coupon_amt") AS "amt",
    SUM("store_sales"."ss_net_profit") AS "profit"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_dow" = 1
    AND "date_dim"."d_year" IN (2000, 2001, 2002)
  JOIN "household_demographics" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
    AND (
      "household_demographics"."hd_dep_count" = 8
      OR "household_demographics"."hd_vehicle_count" > 4
    )
  JOIN "store" AS "store"
    ON "store"."s_number_employees" <= 295
    AND "store"."s_number_employees" >= 200
    AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
  GROUP BY
    "store_sales"."ss_ticket_number",
    "store_sales"."ss_customer_sk",
    "store_sales"."ss_addr_sk",
    "store"."s_city"
)
SELECT
  "customer"."c_last_name" AS "c_last_name",
  "customer"."c_first_name" AS "c_first_name",
  SUBSTR("ms"."s_city", 1, 30) AS "_col_2",
  "ms"."ss_ticket_number" AS "ss_ticket_number",
  "ms"."amt" AS "amt",
  "ms"."profit" AS "profit"
FROM "ms" AS "ms"
JOIN "customer" AS "customer"
  ON "customer"."c_customer_sk" = "ms"."ss_customer_sk"
ORDER BY
  "c_last_name",
  "c_first_name",
  SUBSTR("ms"."s_city", 1, 30),
  "profit"
LIMIT 100;

--------------------------------------
-- TPC-DS 80
--------------------------------------
WITH ssr AS
(
                SELECT          s_store_id                                    AS store_id,
                                Sum(ss_ext_sales_price)                       AS sales,
                                Sum(COALESCE(sr_return_amt, 0))               AS returns1,
                                Sum(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
                FROM            store_sales
                LEFT OUTER JOIN store_returns
                ON              (
                                                ss_item_sk = sr_item_sk
                                AND             ss_ticket_number = sr_ticket_number),
                                date_dim,
                                store,
                                item,
                                promotion
                WHERE           ss_sold_date_sk = d_date_sk
                AND             d_date BETWEEN Cast('2000-08-26' AS DATE) AND             (
                                                Cast('2000-08-26' AS DATE) + INTERVAL '30' day)
                AND             ss_store_sk = s_store_sk
                AND             ss_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             ss_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        s_store_id) , csr AS
(
                SELECT          cp_catalog_page_id                            AS catalog_page_id,
                                sum(cs_ext_sales_price)                       AS sales,
                                sum(COALESCE(cr_return_amount, 0))            AS returns1,
                                sum(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
                FROM            catalog_sales
                LEFT OUTER JOIN catalog_returns
                ON              (
                                                cs_item_sk = cr_item_sk
                                AND             cs_order_number = cr_order_number),
                                date_dim,
                                catalog_page,
                                item,
                                promotion
                WHERE           cs_sold_date_sk = d_date_sk
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND             (
                                                cast('2000-08-26' AS date) + INTERVAL '30' day)
                AND             cs_catalog_page_sk = cp_catalog_page_sk
                AND             cs_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             cs_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        cp_catalog_page_id) , wsr AS
(
                SELECT          web_site_id,
                                sum(ws_ext_sales_price)                       AS sales,
                                sum(COALESCE(wr_return_amt, 0))               AS returns1,
                                sum(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
                FROM            web_sales
                LEFT OUTER JOIN web_returns
                ON              (
                                                ws_item_sk = wr_item_sk
                                AND             ws_order_number = wr_order_number),
                                date_dim,
                                web_site,
                                item,
                                promotion
                WHERE           ws_sold_date_sk = d_date_sk
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND             (
                                                cast('2000-08-26' AS date) + INTERVAL '30' day)
                AND             ws_web_site_sk = web_site_sk
                AND             ws_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             ws_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        web_site_id)
SELECT
         channel ,
         id ,
         sum(sales)   AS sales ,
         sum(returns1) AS returns1 ,
         sum(profit)  AS profit
FROM     (
                SELECT 'store channel' AS channel ,
                       'store'
                              || store_id AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   ssr
                UNION ALL
                SELECT 'catalog channel' AS channel ,
                       'catalog_page'
                              || catalog_page_id AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   csr
                UNION ALL
                SELECT 'web channel' AS channel ,
                       'web_site'
                              || web_site_id AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   wsr ) x
GROUP BY rollup (channel, id)
ORDER BY channel ,
         id
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  WHERE
    CAST("date_dim"."d_date" AS DATE) <= CAST('2000-09-25' AS DATE)
    AND CAST("date_dim"."d_date" AS DATE) >= CAST('2000-08-26' AS DATE)
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_current_price" AS "i_current_price"
  FROM "item" AS "item"
  WHERE
    "item"."i_current_price" > 50
), "promotion_2" AS (
  SELECT
    "promotion"."p_promo_sk" AS "p_promo_sk",
    "promotion"."p_channel_tv" AS "p_channel_tv"
  FROM "promotion" AS "promotion"
  WHERE
    "promotion"."p_channel_tv" = 'N'
), "ssr" AS (
  SELECT
    "store"."s_store_id" AS "store_id",
    SUM("store_sales"."ss_ext_sales_price") AS "sales",
    SUM(COALESCE("store_returns"."sr_return_amt", 0)) AS "returns1",
    SUM("store_sales"."ss_net_profit" - COALESCE("store_returns"."sr_net_loss", 0)) AS "profit"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "promotion_2" AS "promotion"
    ON "promotion"."p_promo_sk" = "store_sales"."ss_promo_sk"
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  LEFT JOIN "store_returns" AS "store_returns"
    ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
    AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
  GROUP BY
    "store"."s_store_id"
), "csr" AS (
  SELECT
    "catalog_page"."cp_catalog_page_id" AS "catalog_page_id",
    SUM("catalog_sales"."cs_ext_sales_price") AS "sales",
    SUM(COALESCE("catalog_returns"."cr_return_amount", 0)) AS "returns1",
    SUM("catalog_sales"."cs_net_profit" - COALESCE("catalog_returns"."cr_net_loss", 0)) AS "profit"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "catalog_page" AS "catalog_page"
    ON "catalog_page"."cp_catalog_page_sk" = "catalog_sales"."cs_catalog_page_sk"
  LEFT JOIN "catalog_returns" AS "catalog_returns"
    ON "catalog_returns"."cr_item_sk" = "catalog_sales"."cs_item_sk"
    AND "catalog_returns"."cr_order_number" = "catalog_sales"."cs_order_number"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_sales"."cs_item_sk" = "item"."i_item_sk"
  JOIN "promotion_2" AS "promotion"
    ON "catalog_sales"."cs_promo_sk" = "promotion"."p_promo_sk"
  GROUP BY
    "catalog_page"."cp_catalog_page_id"
), "wsr" AS (
  SELECT
    "web_site"."web_site_id" AS "web_site_id",
    SUM("web_sales"."ws_ext_sales_price") AS "sales",
    SUM(COALESCE("web_returns"."wr_return_amt", 0)) AS "returns1",
    SUM("web_sales"."ws_net_profit" - COALESCE("web_returns"."wr_net_loss", 0)) AS "profit"
  FROM "web_sales" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
  JOIN "promotion_2" AS "promotion"
    ON "promotion"."p_promo_sk" = "web_sales"."ws_promo_sk"
  LEFT JOIN "web_returns" AS "web_returns"
    ON "web_returns"."wr_item_sk" = "web_sales"."ws_item_sk"
    AND "web_returns"."wr_order_number" = "web_sales"."ws_order_number"
  JOIN "web_site" AS "web_site"
    ON "web_sales"."ws_web_site_sk" = "web_site"."web_site_sk"
  GROUP BY
    "web_site"."web_site_id"
), "x" AS (
  SELECT
    'store channel' AS "channel",
    CONCAT('store', "ssr"."store_id") AS "id",
    "ssr"."sales" AS "sales",
    "ssr"."returns1" AS "returns1",
    "ssr"."profit" AS "profit"
  FROM "ssr"
  UNION ALL
  SELECT
    'catalog channel' AS "channel",
    CONCAT('catalog_page', "csr"."catalog_page_id") AS "id",
    "csr"."sales" AS "sales",
    "csr"."returns1" AS "returns1",
    "csr"."profit" AS "profit"
  FROM "csr"
  UNION ALL
  SELECT
    'web channel' AS "channel",
    CONCAT('web_site', "wsr"."web_site_id") AS "id",
    "wsr"."sales" AS "sales",
    "wsr"."returns1" AS "returns1",
    "wsr"."profit" AS "profit"
  FROM "wsr"
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
-- TPC-DS 81
--------------------------------------
WITH customer_total_return
     AS (SELECT cr_returning_customer_sk   AS ctr_customer_sk,
                ca_state                   AS ctr_state,
                Sum(cr_return_amt_inc_tax) AS ctr_total_return
         FROM   catalog_returns,
                date_dim,
                customer_address
         WHERE  cr_returned_date_sk = d_date_sk
                AND d_year = 1999
                AND cr_returning_addr_sk = ca_address_sk
         GROUP  BY cr_returning_customer_sk,
                   ca_state)
SELECT c_customer_id,
               c_salutation,
               c_first_name,
               c_last_name,
               ca_street_number,
               ca_street_name,
               ca_street_type,
               ca_suite_number,
               ca_city,
               ca_county,
               ca_state,
               ca_zip,
               ca_country,
               ca_gmt_offset,
               ca_location_type,
               ctr_total_return
FROM   customer_total_return ctr1,
       customer_address,
       customer
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_state = ctr2.ctr_state)
       AND ca_address_sk = c_current_addr_sk
       AND ca_state = 'TX'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id,
          c_salutation,
          c_first_name,
          c_last_name,
          ca_street_number,
          ca_street_name,
          ca_street_type,
          ca_suite_number,
          ca_city,
          ca_county,
          ca_state,
          ca_zip,
          ca_country,
          ca_gmt_offset,
          ca_location_type,
          ctr_total_return
LIMIT 100;
WITH "customer_total_return" AS (
  SELECT
    "catalog_returns"."cr_returning_customer_sk" AS "ctr_customer_sk",
    "customer_address"."ca_state" AS "ctr_state",
    SUM("catalog_returns"."cr_return_amt_inc_tax") AS "ctr_total_return"
  FROM "catalog_returns" AS "catalog_returns"
  JOIN "customer_address" AS "customer_address"
    ON "catalog_returns"."cr_returning_addr_sk" = "customer_address"."ca_address_sk"
  JOIN "date_dim" AS "date_dim"
    ON "catalog_returns"."cr_returned_date_sk" = "date_dim"."d_date_sk"
    AND "date_dim"."d_year" = 1999
  GROUP BY
    "catalog_returns"."cr_returning_customer_sk",
    "customer_address"."ca_state"
), "_u_0" AS (
  SELECT
    AVG("ctr2"."ctr_total_return") * 1.2 AS "_col_0",
    "ctr2"."ctr_state" AS "_u_1"
  FROM "customer_total_return" AS "ctr2"
  GROUP BY
    "ctr2"."ctr_state"
)
SELECT
  "customer"."c_customer_id" AS "c_customer_id",
  "customer"."c_salutation" AS "c_salutation",
  "customer"."c_first_name" AS "c_first_name",
  "customer"."c_last_name" AS "c_last_name",
  "customer_address"."ca_street_number" AS "ca_street_number",
  "customer_address"."ca_street_name" AS "ca_street_name",
  "customer_address"."ca_street_type" AS "ca_street_type",
  "customer_address"."ca_suite_number" AS "ca_suite_number",
  "customer_address"."ca_city" AS "ca_city",
  "customer_address"."ca_county" AS "ca_county",
  "customer_address"."ca_state" AS "ca_state",
  "customer_address"."ca_zip" AS "ca_zip",
  "customer_address"."ca_country" AS "ca_country",
  "customer_address"."ca_gmt_offset" AS "ca_gmt_offset",
  "customer_address"."ca_location_type" AS "ca_location_type",
  "ctr1"."ctr_total_return" AS "ctr_total_return"
FROM "customer_total_return" AS "ctr1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "ctr1"."ctr_state"
JOIN "customer" AS "customer"
  ON "ctr1"."ctr_customer_sk" = "customer"."c_customer_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_state" = 'TX'
WHERE
  "_u_0"."_col_0" < "ctr1"."ctr_total_return"
ORDER BY
  "c_customer_id",
  "c_salutation",
  "c_first_name",
  "c_last_name",
  "ca_street_number",
  "ca_street_name",
  "ca_street_type",
  "ca_suite_number",
  "ca_city",
  "ca_county",
  "ca_state",
  "ca_zip",
  "ca_country",
  "ca_gmt_offset",
  "ca_location_type",
  "ctr_total_return"
LIMIT 100;

--------------------------------------
-- TPC-DS 82
--------------------------------------
SELECT
         i_item_id ,
         i_item_desc ,
         i_current_price
FROM     item,
         inventory,
         date_dim,
         store_sales
WHERE    i_current_price BETWEEN 63 AND      63+30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      d_date BETWEEN Cast('1998-04-27' AS DATE) AND      (
                  Cast('1998-04-27' AS DATE) + INTERVAL '60' day)
AND      i_manufact_id IN (57,293,427,320)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      ss_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "item"."i_current_price" AS "i_current_price"
FROM "item" AS "item"
JOIN "inventory" AS "inventory"
  ON "inventory"."inv_item_sk" = "item"."i_item_sk"
  AND "inventory"."inv_quantity_on_hand" <= 500
  AND "inventory"."inv_quantity_on_hand" >= 100
JOIN "store_sales" AS "store_sales"
  ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "inventory"."inv_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('1998-06-26' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('1998-04-27' AS DATE)
WHERE
  "item"."i_current_price" <= 93
  AND "item"."i_current_price" >= 63
  AND "item"."i_manufact_id" IN (57, 293, 427, 320)
GROUP BY
  "item"."i_item_id",
  "item"."i_item_desc",
  "item"."i_current_price"
ORDER BY
  "i_item_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 83
--------------------------------------
WITH sr_items
     AS (SELECT i_item_id               item_id,
                Sum(sr_return_quantity) sr_item_qty
         FROM   store_returns,
                item,
                date_dim
         WHERE  sr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE
                                      d_date IN ( '1999-06-30',
                                                  '1999-08-28',
                                                  '1999-11-18'
                                                )))
                AND sr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id),
     cr_items
     AS (SELECT i_item_id               item_id,
                Sum(cr_return_quantity) cr_item_qty
         FROM   catalog_returns,
                item,
                date_dim
         WHERE  cr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE
                                      d_date IN ( '1999-06-30',
                                                  '1999-08-28',
                                                  '1999-11-18'
                                                )))
                AND cr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id),
     wr_items
     AS (SELECT i_item_id               item_id,
                Sum(wr_return_quantity) wr_item_qty
         FROM   web_returns,
                item,
                date_dim
         WHERE  wr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE
                                      d_date IN ( '1999-06-30',
                                                  '1999-08-28',
                                                  '1999-11-18'
                                                )))
                AND wr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id)
SELECT sr_items.item_id,
               sr_item_qty,
               sr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 *
               100 sr_dev,
               cr_item_qty,
               cr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 *
               100 cr_dev,
               wr_item_qty,
               wr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 *
               100 wr_dev,
               ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0
               average
FROM   sr_items,
       cr_items,
       wr_items
WHERE  sr_items.item_id = cr_items.item_id
       AND sr_items.item_id = wr_items.item_id
ORDER  BY sr_items.item_id,
          sr_item_qty
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
), "item_2" AS (
  SELECT
    "item"."i_item_sk" AS "i_item_sk",
    "item"."i_item_id" AS "i_item_id"
  FROM "item" AS "item"
), "_u_0" AS (
  SELECT
    "date_dim"."d_week_seq" AS "d_week_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_date" IN ('1999-06-30', '1999-08-28', '1999-11-18')
  GROUP BY
    "date_dim"."d_week_seq"
), "_u_1" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."d_week_seq" = "date_dim"."d_week_seq"
  WHERE
    NOT "_u_0"."d_week_seq" IS NULL
  GROUP BY
    "date_dim"."d_date"
), "sr_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("store_returns"."sr_return_quantity") AS "sr_item_qty"
  FROM "store_returns" AS "store_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_returns"."sr_returned_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "store_returns"."sr_item_sk"
  LEFT JOIN "_u_1" AS "_u_1"
    ON "_u_1"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_1"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
), "_u_3" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  LEFT JOIN "_u_0" AS "_u_2"
    ON "_u_2"."d_week_seq" = "date_dim"."d_week_seq"
  WHERE
    NOT "_u_2"."d_week_seq" IS NULL
  GROUP BY
    "date_dim"."d_date"
), "cr_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("catalog_returns"."cr_return_quantity") AS "cr_item_qty"
  FROM "catalog_returns" AS "catalog_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_returns"."cr_returned_date_sk" = "date_dim"."d_date_sk"
  JOIN "item_2" AS "item"
    ON "catalog_returns"."cr_item_sk" = "item"."i_item_sk"
  LEFT JOIN "_u_3" AS "_u_3"
    ON "_u_3"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_3"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
), "_u_5" AS (
  SELECT
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  LEFT JOIN "_u_0" AS "_u_4"
    ON "_u_4"."d_week_seq" = "date_dim"."d_week_seq"
  WHERE
    NOT "_u_4"."d_week_seq" IS NULL
  GROUP BY
    "date_dim"."d_date"
), "wr_items" AS (
  SELECT
    "item"."i_item_id" AS "item_id",
    SUM("web_returns"."wr_return_quantity") AS "wr_item_qty"
  FROM "web_returns" AS "web_returns"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_returns"."wr_returned_date_sk"
  JOIN "item_2" AS "item"
    ON "item"."i_item_sk" = "web_returns"."wr_item_sk"
  LEFT JOIN "_u_5" AS "_u_5"
    ON "_u_5"."d_date" = "date_dim"."d_date"
  WHERE
    NOT "_u_5"."d_date" IS NULL
  GROUP BY
    "item"."i_item_id"
)
SELECT
  "sr_items"."item_id" AS "item_id",
  "sr_items"."sr_item_qty" AS "sr_item_qty",
  "sr_items"."sr_item_qty" / (
    "sr_items"."sr_item_qty" + "cr_items"."cr_item_qty" + "wr_items"."wr_item_qty"
  ) / 3.0 * 100 AS "sr_dev",
  "cr_items"."cr_item_qty" AS "cr_item_qty",
  "cr_items"."cr_item_qty" / (
    "sr_items"."sr_item_qty" + "cr_items"."cr_item_qty" + "wr_items"."wr_item_qty"
  ) / 3.0 * 100 AS "cr_dev",
  "wr_items"."wr_item_qty" AS "wr_item_qty",
  "wr_items"."wr_item_qty" / (
    "sr_items"."sr_item_qty" + "cr_items"."cr_item_qty" + "wr_items"."wr_item_qty"
  ) / 3.0 * 100 AS "wr_dev",
  (
    "sr_items"."sr_item_qty" + "cr_items"."cr_item_qty" + "wr_items"."wr_item_qty"
  ) / 3.0 AS "average"
FROM "sr_items"
JOIN "cr_items"
  ON "cr_items"."item_id" = "sr_items"."item_id"
JOIN "wr_items"
  ON "sr_items"."item_id" = "wr_items"."item_id"
ORDER BY
  "sr_items"."item_id",
  "sr_item_qty"
LIMIT 100;

--------------------------------------
-- TPC-DS 84
--------------------------------------
SELECT c_customer_id   AS customer_id,
               c_last_name
               || ', '
               || c_first_name AS customername
FROM   customer,
       customer_address,
       customer_demographics,
       household_demographics,
       income_band,
       store_returns
WHERE  ca_city = 'Green Acres'
       AND c_current_addr_sk = ca_address_sk
       AND ib_lower_bound >= 54986
       AND ib_upper_bound <= 54986 + 50000
       AND ib_income_band_sk = hd_income_band_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND sr_cdemo_sk = cd_demo_sk
ORDER  BY c_customer_id
LIMIT 100;
SELECT
  "customer"."c_customer_id" AS "customer_id",
  CONCAT("customer"."c_last_name", ', ', "customer"."c_first_name") AS "customername"
FROM "customer" AS "customer"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_city" = 'Green Acres'
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer"."c_current_cdemo_sk" = "customer_demographics"."cd_demo_sk"
JOIN "household_demographics" AS "household_demographics"
  ON "customer"."c_current_hdemo_sk" = "household_demographics"."hd_demo_sk"
JOIN "income_band" AS "income_band"
  ON "household_demographics"."hd_income_band_sk" = "income_band"."ib_income_band_sk"
  AND "income_band"."ib_lower_bound" >= 54986
  AND "income_band"."ib_upper_bound" <= 104986
JOIN "store_returns" AS "store_returns"
  ON "customer_demographics"."cd_demo_sk" = "store_returns"."sr_cdemo_sk"
ORDER BY
  "customer"."c_customer_id"
LIMIT 100;

--------------------------------------
-- TPC-DS 85
--------------------------------------
SELECT Substr(r_reason_desc, 1, 20),
               Avg(ws_quantity),
               Avg(wr_refunded_cash),
               Avg(wr_fee)
FROM   web_sales,
       web_returns,
       web_page,
       customer_demographics cd1,
       customer_demographics cd2,
       customer_address,
       date_dim,
       reason
WHERE  ws_web_page_sk = wp_web_page_sk
       AND ws_item_sk = wr_item_sk
       AND ws_order_number = wr_order_number
       AND ws_sold_date_sk = d_date_sk
       AND d_year = 2001
       AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
       AND cd2.cd_demo_sk = wr_returning_cdemo_sk
       AND ca_address_sk = wr_refunded_addr_sk
       AND r_reason_sk = wr_reason_sk
       AND ( ( cd1.cd_marital_status = 'W'
               AND cd1.cd_marital_status = cd2.cd_marital_status
               AND cd1.cd_education_status = 'Primary'
               AND cd1.cd_education_status = cd2.cd_education_status
               AND ws_sales_price BETWEEN 100.00 AND 150.00 )
              OR ( cd1.cd_marital_status = 'D'
                   AND cd1.cd_marital_status = cd2.cd_marital_status
                   AND cd1.cd_education_status = 'Secondary'
                   AND cd1.cd_education_status = cd2.cd_education_status
                   AND ws_sales_price BETWEEN 50.00 AND 100.00 )
              OR ( cd1.cd_marital_status = 'M'
                   AND cd1.cd_marital_status = cd2.cd_marital_status
                   AND cd1.cd_education_status = 'Advanced Degree'
                   AND cd1.cd_education_status = cd2.cd_education_status
                   AND ws_sales_price BETWEEN 150.00 AND 200.00 ) )
       AND ( ( ca_country = 'United States'
               AND ca_state IN ( 'KY', 'ME', 'IL' )
               AND ws_net_profit BETWEEN 100 AND 200 )
              OR ( ca_country = 'United States'
                   AND ca_state IN ( 'OK', 'NE', 'MN' )
                   AND ws_net_profit BETWEEN 150 AND 300 )
              OR ( ca_country = 'United States'
                   AND ca_state IN ( 'FL', 'WI', 'KS' )
                   AND ws_net_profit BETWEEN 50 AND 250 ) )
GROUP  BY r_reason_desc
ORDER  BY Substr(r_reason_desc, 1, 20),
          Avg(ws_quantity),
          Avg(wr_refunded_cash),
          Avg(wr_fee)
LIMIT 100;
SELECT
  SUBSTR("reason"."r_reason_desc", 1, 20) AS "_col_0",
  AVG("web_sales"."ws_quantity") AS "_col_1",
  AVG("web_returns"."wr_refunded_cash") AS "_col_2",
  AVG("web_returns"."wr_fee") AS "_col_3"
FROM "web_sales" AS "web_sales"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk" AND "date_dim"."d_year" = 2001
JOIN "web_page" AS "web_page"
  ON "web_page"."wp_web_page_sk" = "web_sales"."ws_web_page_sk"
JOIN "web_returns" AS "web_returns"
  ON "web_returns"."wr_item_sk" = "web_sales"."ws_item_sk"
  AND "web_returns"."wr_order_number" = "web_sales"."ws_order_number"
JOIN "customer_demographics" AS "cd2"
  ON "cd2"."cd_demo_sk" = "web_returns"."wr_returning_cdemo_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer_address"."ca_address_sk" = "web_returns"."wr_refunded_addr_sk"
  AND (
    (
      "customer_address"."ca_country" = 'United States'
      AND "customer_address"."ca_state" IN ('FL', 'WI', 'KS')
      AND "web_sales"."ws_net_profit" <= 250
      AND "web_sales"."ws_net_profit" >= 50
    )
    OR (
      "customer_address"."ca_country" = 'United States'
      AND "customer_address"."ca_state" IN ('KY', 'ME', 'IL')
      AND "web_sales"."ws_net_profit" <= 200
      AND "web_sales"."ws_net_profit" >= 100
    )
    OR (
      "customer_address"."ca_country" = 'United States'
      AND "customer_address"."ca_state" IN ('OK', 'NE', 'MN')
      AND "web_sales"."ws_net_profit" <= 300
      AND "web_sales"."ws_net_profit" >= 150
    )
  )
JOIN "reason" AS "reason"
  ON "reason"."r_reason_sk" = "web_returns"."wr_reason_sk"
JOIN "customer_demographics" AS "cd1"
  ON "cd1"."cd_demo_sk" = "web_returns"."wr_refunded_cdemo_sk"
  AND (
    (
      "cd1"."cd_education_status" = "cd2"."cd_education_status"
      AND "cd1"."cd_education_status" = 'Advanced Degree'
      AND "cd1"."cd_marital_status" = "cd2"."cd_marital_status"
      AND "cd1"."cd_marital_status" = 'M'
      AND "web_sales"."ws_sales_price" <= 200.00
      AND "web_sales"."ws_sales_price" >= 150.00
    )
    OR (
      "cd1"."cd_education_status" = "cd2"."cd_education_status"
      AND "cd1"."cd_education_status" = 'Primary'
      AND "cd1"."cd_marital_status" = "cd2"."cd_marital_status"
      AND "cd1"."cd_marital_status" = 'W'
      AND "web_sales"."ws_sales_price" <= 150.00
      AND "web_sales"."ws_sales_price" >= 100.00
    )
    OR (
      "cd1"."cd_education_status" = "cd2"."cd_education_status"
      AND "cd1"."cd_education_status" = 'Secondary'
      AND "cd1"."cd_marital_status" = "cd2"."cd_marital_status"
      AND "cd1"."cd_marital_status" = 'D'
      AND "web_sales"."ws_sales_price" <= 100.00
      AND "web_sales"."ws_sales_price" >= 50.00
    )
  )
GROUP BY
  "reason"."r_reason_desc"
ORDER BY
  "_col_0",
  "_col_1",
  "_col_2",
  "_col_3"
LIMIT 100;

--------------------------------------
-- TPC-DS 86
--------------------------------------
SELECT Sum(ws_net_paid)                         AS total_sum,
               i_category,
               i_class,
               Grouping(i_category) + Grouping(i_class) AS lochierarchy,
               Rank()
                 OVER (
                   partition BY Grouping(i_category)+Grouping(i_class), CASE
                 WHEN Grouping(
                 i_class) = 0 THEN i_category END
                   ORDER BY Sum(ws_net_paid) DESC)      AS rank_within_parent
FROM   web_sales,
       date_dim d1,
       item
WHERE  d1.d_month_seq BETWEEN 1183 AND 1183 + 11
       AND d1.d_date_sk = ws_sold_date_sk
       AND i_item_sk = ws_item_sk
GROUP  BY rollup( i_category, i_class )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN i_category
          END,
          rank_within_parent
LIMIT 100;
SELECT
  SUM("web_sales"."ws_net_paid") AS "total_sum",
  "item"."i_category" AS "i_category",
  "item"."i_class" AS "i_class",
  GROUPING("item"."i_category") + GROUPING("item"."i_class") AS "lochierarchy",
  RANK() OVER (PARTITION BY GROUPING("item"."i_category") + GROUPING("item"."i_class"), CASE WHEN GROUPING("item"."i_class") = 0 THEN "item"."i_category" END ORDER BY SUM("web_sales"."ws_net_paid") DESC) AS "rank_within_parent"
FROM "web_sales" AS "web_sales"
JOIN "date_dim" AS "d1"
  ON "d1"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  AND "d1"."d_month_seq" <= 1194
  AND "d1"."d_month_seq" >= 1183
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "web_sales"."ws_item_sk"
GROUP BY
ROLLUP (
  "item"."i_category",
  "item"."i_class"
)
ORDER BY
  "lochierarchy" DESC,
  CASE WHEN "lochierarchy" = 0 THEN "i_category" END,
  "rank_within_parent"
LIMIT 100;

--------------------------------------
-- TPC-DS 87
--------------------------------------
select count(*)
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
) cool_cust;
WITH "customer_2" AS (
  SELECT
    "customer"."c_customer_sk" AS "c_customer_sk",
    "customer"."c_first_name" AS "c_first_name",
    "customer"."c_last_name" AS "c_last_name"
  FROM "customer" AS "customer"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date",
    "date_dim"."d_month_seq" AS "d_month_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_month_seq" <= 1199 AND "date_dim"."d_month_seq" >= 1188
), "cool_cust" AS (
  (
    SELECT DISTINCT
      "customer"."c_last_name" AS "c_last_name",
      "customer"."c_first_name" AS "c_first_name",
      "date_dim"."d_date" AS "d_date"
    FROM "store_sales" AS "store_sales"
    JOIN "customer_2" AS "customer"
      ON "customer"."c_customer_sk" = "store_sales"."ss_customer_sk"
    JOIN "date_dim_2" AS "date_dim"
      ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  )
  EXCEPT
  (
    SELECT DISTINCT
      "customer"."c_last_name" AS "c_last_name",
      "customer"."c_first_name" AS "c_first_name",
      "date_dim"."d_date" AS "d_date"
    FROM "catalog_sales" AS "catalog_sales"
    JOIN "customer_2" AS "customer"
      ON "catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk"
    JOIN "date_dim_2" AS "date_dim"
      ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  )
  EXCEPT
  (
    SELECT DISTINCT
      "customer"."c_last_name" AS "c_last_name",
      "customer"."c_first_name" AS "c_first_name",
      "date_dim"."d_date" AS "d_date"
    FROM "web_sales" AS "web_sales"
    JOIN "customer_2" AS "customer"
      ON "customer"."c_customer_sk" = "web_sales"."ws_bill_customer_sk"
    JOIN "date_dim_2" AS "date_dim"
      ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  )
)
SELECT
  COUNT(*) AS "_col_0"
FROM "cool_cust" AS "cool_cust";

--------------------------------------
-- TPC-DS 88
--------------------------------------
select  *
from
 (select count(*) h8_30_to_9
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s8;
WITH "store_sales_2" AS (
  SELECT
    "store_sales"."ss_sold_time_sk" AS "ss_sold_time_sk",
    "store_sales"."ss_hdemo_sk" AS "ss_hdemo_sk",
    "store_sales"."ss_store_sk" AS "ss_store_sk"
  FROM "store_sales" AS "store_sales"
), "household_demographics_2" AS (
  SELECT
    "household_demographics"."hd_demo_sk" AS "hd_demo_sk",
    "household_demographics"."hd_dep_count" AS "hd_dep_count",
    "household_demographics"."hd_vehicle_count" AS "hd_vehicle_count"
  FROM "household_demographics" AS "household_demographics"
  WHERE
    (
      "household_demographics"."hd_dep_count" = -1
      OR "household_demographics"."hd_dep_count" = 2
      OR "household_demographics"."hd_dep_count" = 3
    )
    AND (
      "household_demographics"."hd_dep_count" = 2
      OR "household_demographics"."hd_dep_count" = 3
      OR "household_demographics"."hd_vehicle_count" <= 1
    )
    AND (
      "household_demographics"."hd_dep_count" = 3
      OR "household_demographics"."hd_vehicle_count" <= 4
    )
    AND "household_demographics"."hd_vehicle_count" <= 5
), "store_2" AS (
  SELECT
    "store"."s_store_sk" AS "s_store_sk",
    "store"."s_store_name" AS "s_store_name"
  FROM "store" AS "store"
  WHERE
    "store"."s_store_name" = 'ese'
), "s1" AS (
  SELECT
    COUNT(*) AS "h8_30_to_9"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 8
    AND "time_dim"."t_minute" >= 30
), "s2" AS (
  SELECT
    COUNT(*) AS "h9_to_9_30"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 9
    AND "time_dim"."t_minute" < 30
), "s3" AS (
  SELECT
    COUNT(*) AS "h9_30_to_10"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 9
    AND "time_dim"."t_minute" >= 30
), "s4" AS (
  SELECT
    COUNT(*) AS "h10_to_10_30"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 10
    AND "time_dim"."t_minute" < 30
), "s5" AS (
  SELECT
    COUNT(*) AS "h10_30_to_11"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 10
    AND "time_dim"."t_minute" >= 30
), "s6" AS (
  SELECT
    COUNT(*) AS "h11_to_11_30"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 11
    AND "time_dim"."t_minute" < 30
), "s7" AS (
  SELECT
    COUNT(*) AS "h11_30_to_12"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 11
    AND "time_dim"."t_minute" >= 30
), "s8" AS (
  SELECT
    COUNT(*) AS "h12_to_12_30"
  FROM "store_sales_2" AS "store_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  JOIN "store_2" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  JOIN "time_dim" AS "time_dim"
    ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
    AND "time_dim"."t_hour" = 12
    AND "time_dim"."t_minute" < 30
)
SELECT
  "s1"."h8_30_to_9" AS "h8_30_to_9",
  "s2"."h9_to_9_30" AS "h9_to_9_30",
  "s3"."h9_30_to_10" AS "h9_30_to_10",
  "s4"."h10_to_10_30" AS "h10_to_10_30",
  "s5"."h10_30_to_11" AS "h10_30_to_11",
  "s6"."h11_to_11_30" AS "h11_to_11_30",
  "s7"."h11_30_to_12" AS "h11_30_to_12",
  "s8"."h12_to_12_30" AS "h12_to_12_30"
FROM "s1" AS "s1"
CROSS JOIN "s2" AS "s2"
CROSS JOIN "s3" AS "s3"
CROSS JOIN "s4" AS "s4"
CROSS JOIN "s5" AS "s5"
CROSS JOIN "s6" AS "s6"
CROSS JOIN "s7" AS "s7"
CROSS JOIN "s8" AS "s8";

--------------------------------------
-- TPC-DS 89
--------------------------------------
SELECT  *
FROM  (SELECT i_category,
              i_class,
              i_brand,
              s_store_name,
              s_company_name,
              d_moy,
              Sum(ss_sales_price) sum_sales,
              Avg(Sum(ss_sales_price))
                OVER (
                  partition BY i_category, i_brand, s_store_name, s_company_name
                )
                                  avg_monthly_sales
       FROM   item,
              store_sales,
              date_dim,
              store
       WHERE  ss_item_sk = i_item_sk
              AND ss_sold_date_sk = d_date_sk
              AND ss_store_sk = s_store_sk
              AND d_year IN ( 2002 )
              AND ( ( i_category IN ( 'Home', 'Men', 'Sports' )
                      AND i_class IN ( 'paint', 'accessories', 'fitness' ) )
                     OR ( i_category IN ( 'Shoes', 'Jewelry', 'Women' )
                          AND i_class IN ( 'mens', 'pendants', 'swimwear' ) ) )
       GROUP  BY i_category,
                 i_class,
                 i_brand,
                 s_store_name,
                 s_company_name,
                 d_moy) tmp1
WHERE  CASE
         WHEN ( avg_monthly_sales <> 0 ) THEN (
         Abs(sum_sales - avg_monthly_sales) / avg_monthly_sales )
         ELSE NULL
       END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          s_store_name
LIMIT 100;
WITH "tmp1" AS (
  SELECT
    "item"."i_category" AS "i_category",
    "item"."i_class" AS "i_class",
    "item"."i_brand" AS "i_brand",
    "store"."s_store_name" AS "s_store_name",
    "store"."s_company_name" AS "s_company_name",
    "date_dim"."d_moy" AS "d_moy",
    SUM("store_sales"."ss_sales_price") AS "sum_sales",
    AVG(SUM("store_sales"."ss_sales_price")) OVER (PARTITION BY "item"."i_category", "item"."i_brand", "store"."s_store_name", "store"."s_company_name") AS "avg_monthly_sales"
  FROM "item" AS "item"
  JOIN "store_sales" AS "store_sales"
    ON "item"."i_item_sk" = "store_sales"."ss_item_sk"
  JOIN "date_dim" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
    AND "date_dim"."d_year" IN (2002)
  JOIN "store" AS "store"
    ON "store"."s_store_sk" = "store_sales"."ss_store_sk"
  WHERE
    (
      "item"."i_category" IN ('Home', 'Men', 'Sports')
      OR "item"."i_category" IN ('Shoes', 'Jewelry', 'Women')
    )
    AND (
      "item"."i_category" IN ('Home', 'Men', 'Sports')
      OR "item"."i_class" IN ('mens', 'pendants', 'swimwear')
    )
    AND (
      "item"."i_category" IN ('Shoes', 'Jewelry', 'Women')
      OR "item"."i_class" IN ('paint', 'accessories', 'fitness')
    )
    AND (
      "item"."i_class" IN ('mens', 'pendants', 'swimwear')
      OR "item"."i_class" IN ('paint', 'accessories', 'fitness')
    )
  GROUP BY
    "item"."i_category",
    "item"."i_class",
    "item"."i_brand",
    "store"."s_store_name",
    "store"."s_company_name",
    "date_dim"."d_moy"
)
SELECT
  "tmp1"."i_category" AS "i_category",
  "tmp1"."i_class" AS "i_class",
  "tmp1"."i_brand" AS "i_brand",
  "tmp1"."s_store_name" AS "s_store_name",
  "tmp1"."s_company_name" AS "s_company_name",
  "tmp1"."d_moy" AS "d_moy",
  "tmp1"."sum_sales" AS "sum_sales",
  "tmp1"."avg_monthly_sales" AS "avg_monthly_sales"
FROM "tmp1" AS "tmp1"
WHERE
  CASE
    WHEN "tmp1"."avg_monthly_sales" <> 0
    THEN (
      ABS("tmp1"."sum_sales" - "tmp1"."avg_monthly_sales") / "tmp1"."avg_monthly_sales"
    )
    ELSE NULL
  END > 0.1
ORDER BY
  "tmp1"."sum_sales" - "tmp1"."avg_monthly_sales",
  "tmp1"."s_store_name"
LIMIT 100;

--------------------------------------
-- TPC-DS 90
--------------------------------------
SELECT Cast(amc AS DECIMAL(15, 4)) / Cast(pmc AS DECIMAL(15, 4))
               am_pm_ratio
FROM   (SELECT Count(*) amc
        FROM   web_sales,
               household_demographics,
               time_dim,
               web_page
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 12 AND 12 + 1
               AND household_demographics.hd_dep_count = 8
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1,
       (SELECT Count(*) pmc
        FROM   web_sales,
               household_demographics,
               time_dim,
               web_page
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 20 AND 20 + 1
               AND household_demographics.hd_dep_count = 8
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER  BY am_pm_ratio
LIMIT 100;
WITH "web_sales_2" AS (
  SELECT
    "web_sales"."ws_sold_time_sk" AS "ws_sold_time_sk",
    "web_sales"."ws_ship_hdemo_sk" AS "ws_ship_hdemo_sk",
    "web_sales"."ws_web_page_sk" AS "ws_web_page_sk"
  FROM "web_sales" AS "web_sales"
), "household_demographics_2" AS (
  SELECT
    "household_demographics"."hd_demo_sk" AS "hd_demo_sk",
    "household_demographics"."hd_dep_count" AS "hd_dep_count"
  FROM "household_demographics" AS "household_demographics"
  WHERE
    "household_demographics"."hd_dep_count" = 8
), "web_page_2" AS (
  SELECT
    "web_page"."wp_web_page_sk" AS "wp_web_page_sk",
    "web_page"."wp_char_count" AS "wp_char_count"
  FROM "web_page" AS "web_page"
  WHERE
    "web_page"."wp_char_count" <= 5200 AND "web_page"."wp_char_count" >= 5000
), "at1" AS (
  SELECT
    COUNT(*) AS "amc"
  FROM "web_sales_2" AS "web_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "web_sales"."ws_ship_hdemo_sk"
  JOIN "time_dim" AS "time_dim"
    ON "time_dim"."t_hour" <= 13
    AND "time_dim"."t_hour" >= 12
    AND "time_dim"."t_time_sk" = "web_sales"."ws_sold_time_sk"
  JOIN "web_page_2" AS "web_page"
    ON "web_page"."wp_web_page_sk" = "web_sales"."ws_web_page_sk"
), "pt" AS (
  SELECT
    COUNT(*) AS "pmc"
  FROM "web_sales_2" AS "web_sales"
  JOIN "household_demographics_2" AS "household_demographics"
    ON "household_demographics"."hd_demo_sk" = "web_sales"."ws_ship_hdemo_sk"
  JOIN "time_dim" AS "time_dim"
    ON "time_dim"."t_hour" <= 21
    AND "time_dim"."t_hour" >= 20
    AND "time_dim"."t_time_sk" = "web_sales"."ws_sold_time_sk"
  JOIN "web_page_2" AS "web_page"
    ON "web_page"."wp_web_page_sk" = "web_sales"."ws_web_page_sk"
)
SELECT
  CAST("at1"."amc" AS DECIMAL(15, 4)) / CAST("pt"."pmc" AS DECIMAL(15, 4)) AS "am_pm_ratio"
FROM "at1" AS "at1"
CROSS JOIN "pt" AS "pt"
ORDER BY
  "am_pm_ratio"
LIMIT 100;

--------------------------------------
-- TPC-DS 91
--------------------------------------
SELECT cc_call_center_id Call_Center,
       cc_name           Call_Center_Name,
       cc_manager        Manager,
       Sum(cr_net_loss)  Returns_Loss
FROM   call_center,
       catalog_returns,
       date_dim,
       customer,
       customer_address,
       customer_demographics,
       household_demographics
WHERE  cr_call_center_sk = cc_call_center_sk
       AND cr_returned_date_sk = d_date_sk
       AND cr_returning_customer_sk = c_customer_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND ca_address_sk = c_current_addr_sk
       AND d_year = 1999
       AND d_moy = 12
       AND ( ( cd_marital_status = 'M'
               AND cd_education_status = 'Unknown' )
              OR ( cd_marital_status = 'W'
                   AND cd_education_status = 'Advanced Degree' ) )
       AND hd_buy_potential LIKE 'Unknown%'
       AND ca_gmt_offset = -7
GROUP  BY cc_call_center_id,
          cc_name,
          cc_manager,
          cd_marital_status,
          cd_education_status
ORDER  BY Sum(cr_net_loss) DESC;
SELECT
  "call_center"."cc_call_center_id" AS "call_center",
  "call_center"."cc_name" AS "call_center_name",
  "call_center"."cc_manager" AS "manager",
  SUM("catalog_returns"."cr_net_loss") AS "returns_loss"
FROM "call_center" AS "call_center"
JOIN "household_demographics" AS "household_demographics"
  ON "household_demographics"."hd_buy_potential" LIKE 'Unknown%'
JOIN "customer" AS "customer"
  ON "customer"."c_current_hdemo_sk" = "household_demographics"."hd_demo_sk"
JOIN "catalog_returns" AS "catalog_returns"
  ON "call_center"."cc_call_center_sk" = "catalog_returns"."cr_call_center_sk"
  AND "catalog_returns"."cr_returning_customer_sk" = "customer"."c_customer_sk"
JOIN "customer_address" AS "customer_address"
  ON "customer"."c_current_addr_sk" = "customer_address"."ca_address_sk"
  AND "customer_address"."ca_gmt_offset" = -7
JOIN "customer_demographics" AS "customer_demographics"
  ON "customer"."c_current_cdemo_sk" = "customer_demographics"."cd_demo_sk"
  AND (
    "customer_demographics"."cd_education_status" = 'Advanced Degree'
    OR "customer_demographics"."cd_education_status" = 'Unknown'
  )
  AND (
    "customer_demographics"."cd_education_status" = 'Advanced Degree'
    OR "customer_demographics"."cd_marital_status" = 'M'
  )
  AND (
    "customer_demographics"."cd_education_status" = 'Unknown'
    OR "customer_demographics"."cd_marital_status" = 'W'
  )
  AND (
    "customer_demographics"."cd_marital_status" = 'M'
    OR "customer_demographics"."cd_marital_status" = 'W'
  )
JOIN "date_dim" AS "date_dim"
  ON "catalog_returns"."cr_returned_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_moy" = 12
  AND "date_dim"."d_year" = 1999
GROUP BY
  "call_center"."cc_call_center_id",
  "call_center"."cc_name",
  "call_center"."cc_manager",
  "customer_demographics"."cd_marital_status",
  "customer_demographics"."cd_education_status"
ORDER BY
  "returns_loss" DESC;

--------------------------------------
-- TPC-DS 92
--------------------------------------
SELECT
         Sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM     web_sales ,
         item ,
         date_dim
WHERE    i_manufact_id = 718
AND      i_item_sk = ws_item_sk
AND      d_date BETWEEN '2002-03-29' AND      (
                  Cast('2002-03-29' AS DATE) +  INTERVAL '90' day)
AND      d_date_sk = ws_sold_date_sk
AND      ws_ext_discount_amt >
         (
                SELECT 1.3 * avg(ws_ext_discount_amt)
                FROM   web_sales ,
                       date_dim
                WHERE  ws_item_sk = i_item_sk
                AND    d_date BETWEEN '2002-03-29' AND    (
                              cast('2002-03-29' AS date) + INTERVAL '90' day)
                AND    d_date_sk = ws_sold_date_sk )
ORDER BY sum(ws_ext_discount_amt)
LIMIT 100;
WITH "web_sales_2" AS (
  SELECT
    "web_sales"."ws_sold_date_sk" AS "ws_sold_date_sk",
    "web_sales"."ws_item_sk" AS "ws_item_sk",
    "web_sales"."ws_ext_discount_amt" AS "ws_ext_discount_amt"
  FROM "web_sales" AS "web_sales"
), "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_date" AS "d_date"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_date" >= '2002-03-29'
    AND CAST("date_dim"."d_date" AS DATE) <= CAST('2002-06-27' AS DATE)
), "_u_0" AS (
  SELECT
    1.3 * AVG("web_sales"."ws_ext_discount_amt") AS "_col_0",
    "web_sales"."ws_item_sk" AS "_u_1"
  FROM "web_sales_2" AS "web_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
  GROUP BY
    "web_sales"."ws_item_sk"
)
SELECT
  SUM("web_sales"."ws_ext_discount_amt") AS "Excess Discount Amount"
FROM "web_sales_2" AS "web_sales"
JOIN "date_dim_2" AS "date_dim"
  ON "date_dim"."d_date_sk" = "web_sales"."ws_sold_date_sk"
JOIN "item" AS "item"
  ON "item"."i_item_sk" = "web_sales"."ws_item_sk" AND "item"."i_manufact_id" = 718
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "item"."i_item_sk"
WHERE
  "_u_0"."_col_0" < "web_sales"."ws_ext_discount_amt"
ORDER BY
  SUM("web_sales"."ws_ext_discount_amt")
LIMIT 100;

--------------------------------------
-- TPC-DS 93
--------------------------------------
SELECT ss_customer_sk,
               Sum(act_sales) sumsales
FROM   (SELECT ss_item_sk,
               ss_ticket_number,
               ss_customer_sk,
               CASE
                 WHEN sr_return_quantity IS NOT NULL THEN
                 ( ss_quantity - sr_return_quantity ) * ss_sales_price
                 ELSE ( ss_quantity * ss_sales_price )
               END act_sales
        FROM   store_sales
               LEFT OUTER JOIN store_returns
                            ON ( sr_item_sk = ss_item_sk
                                 AND sr_ticket_number = ss_ticket_number ),
               reason
        WHERE  sr_reason_sk = r_reason_sk
               AND r_reason_desc = 'reason 38') t
GROUP  BY ss_customer_sk
ORDER  BY sumsales,
          ss_customer_sk
LIMIT 100;
SELECT
  "store_sales"."ss_customer_sk" AS "ss_customer_sk",
  SUM(
    CASE
      WHEN NOT "store_returns"."sr_return_quantity" IS NULL
      THEN (
        "store_sales"."ss_quantity" - "store_returns"."sr_return_quantity"
      ) * "store_sales"."ss_sales_price"
      ELSE (
        "store_sales"."ss_quantity" * "store_sales"."ss_sales_price"
      )
    END
  ) AS "sumsales"
FROM "store_sales" AS "store_sales"
JOIN "reason" AS "reason"
  ON "reason"."r_reason_desc" = 'reason 38'
LEFT JOIN "store_returns" AS "store_returns"
  ON "store_returns"."sr_item_sk" = "store_sales"."ss_item_sk"
  AND "store_returns"."sr_ticket_number" = "store_sales"."ss_ticket_number"
WHERE
  "reason"."r_reason_sk" = "store_returns"."sr_reason_sk"
GROUP BY
  "store_sales"."ss_customer_sk"
ORDER BY
  "sumsales",
  "ss_customer_sk"
LIMIT 100;

--------------------------------------
-- TPC-DS 94
--------------------------------------
SELECT
         Count(DISTINCT ws_order_number) AS "order count" ,
         Sum(ws_ext_ship_cost)           AS "total shipping cost" ,
         Sum(ws_net_profit)              AS "total net profit"
FROM     web_sales ws1 ,
         date_dim ,
         customer_address ,
         web_site
WHERE    d_date BETWEEN '2000-3-01' AND      (
                  Cast('2000-3-01' AS DATE) + INTERVAL '60' day)
AND      ws1.ws_ship_date_sk = d_date_sk
AND      ws1.ws_ship_addr_sk = ca_address_sk
AND      ca_state = 'MT'
AND      ws1.ws_web_site_sk = web_site_sk
AND      web_company_name = 'pri'
AND      EXISTS
         (
                SELECT *
                FROM   web_sales ws2
                WHERE  ws1.ws_order_number = ws2.ws_order_number
                AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   web_returns wr1
                WHERE  ws1.ws_order_number = wr1.wr_order_number)
ORDER BY count(DISTINCT ws_order_number)
LIMIT 100;
WITH "_u_0" AS (
  SELECT
    "ws2"."ws_order_number" AS "_u_1",
    ARRAY_AGG("ws2"."ws_warehouse_sk") AS "_u_2"
  FROM "web_sales" AS "ws2"
  GROUP BY
    "ws2"."ws_order_number"
), "_u_3" AS (
  SELECT
    "wr1"."wr_order_number" AS "_u_4"
  FROM "web_returns" AS "wr1"
  GROUP BY
    "wr1"."wr_order_number"
)
SELECT
  COUNT(DISTINCT "ws1"."ws_order_number") AS "order count",
  SUM("ws1"."ws_ext_ship_cost") AS "total shipping cost",
  SUM("ws1"."ws_net_profit") AS "total net profit"
FROM "web_sales" AS "ws1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_u_1" = "ws1"."ws_order_number"
LEFT JOIN "_u_3" AS "_u_3"
  ON "_u_3"."_u_4" = "ws1"."ws_order_number"
JOIN "customer_address" AS "customer_address"
  ON "customer_address"."ca_address_sk" = "ws1"."ws_ship_addr_sk"
  AND "customer_address"."ca_state" = 'MT'
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date" >= '2000-3-01'
  AND "date_dim"."d_date_sk" = "ws1"."ws_ship_date_sk"
  AND (
    CAST('2000-3-01' AS DATE) + INTERVAL '60' DAY
  ) >= CAST("date_dim"."d_date" AS DATE)
JOIN "web_site" AS "web_site"
  ON "web_site"."web_company_name" = 'pri'
  AND "web_site"."web_site_sk" = "ws1"."ws_web_site_sk"
WHERE
  "_u_3"."_u_4" IS NULL
  AND NOT "_u_0"."_u_1" IS NULL
  AND ARRAY_ANY("_u_0"."_u_2", "_x" -> "ws1"."ws_warehouse_sk" <> "_x")
ORDER BY
  COUNT(DISTINCT "ws1"."ws_order_number")
LIMIT 100;

--------------------------------------
-- TPC-DS 95
--------------------------------------
WITH ws_wh AS
(
       SELECT ws1.ws_order_number,
              ws1.ws_warehouse_sk wh1,
              ws2.ws_warehouse_sk wh2
       FROM   web_sales ws1,
              web_sales ws2
       WHERE  ws1.ws_order_number = ws2.ws_order_number
       AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
SELECT
         Count(DISTINCT ws_order_number) AS "order count" ,
         Sum(ws_ext_ship_cost)           AS "total shipping cost" ,
         Sum(ws_net_profit)              AS "total net profit"
FROM     web_sales ws1 ,
         date_dim ,
         customer_address ,
         web_site
WHERE    d_date BETWEEN '2000-4-01' AND      (
                  Cast('2000-4-01' AS DATE) + INTERVAL '60' day)
AND      ws1.ws_ship_date_sk = d_date_sk
AND      ws1.ws_ship_addr_sk = ca_address_sk
AND      ca_state = 'IN'
AND      ws1.ws_web_site_sk = web_site_sk
AND      web_company_name = 'pri'
AND      ws1.ws_order_number IN
         (
                SELECT ws_order_number
                FROM   ws_wh)
AND      ws1.ws_order_number IN
         (
                SELECT wr_order_number
                FROM   web_returns,
                       ws_wh
                WHERE  wr_order_number = ws_wh.ws_order_number)
ORDER BY count(DISTINCT ws_order_number)
LIMIT 100;
WITH "ws_wh" AS (
  SELECT
    "ws1"."ws_order_number" AS "ws_order_number"
  FROM "web_sales" AS "ws1"
  JOIN "web_sales" AS "ws2"
    ON "ws1"."ws_order_number" = "ws2"."ws_order_number"
    AND "ws1"."ws_warehouse_sk" <> "ws2"."ws_warehouse_sk"
), "_u_0" AS (
  SELECT
    "ws_wh"."ws_order_number" AS "ws_order_number"
  FROM "ws_wh"
  GROUP BY
    "ws_wh"."ws_order_number"
), "_u_1" AS (
  SELECT
    "web_returns"."wr_order_number" AS "wr_order_number"
  FROM "web_returns" AS "web_returns"
  JOIN "ws_wh"
    ON "web_returns"."wr_order_number" = "ws_wh"."ws_order_number"
  GROUP BY
    "web_returns"."wr_order_number"
)
SELECT
  COUNT(DISTINCT "ws1"."ws_order_number") AS "order count",
  SUM("ws1"."ws_ext_ship_cost") AS "total shipping cost",
  SUM("ws1"."ws_net_profit") AS "total net profit"
FROM "web_sales" AS "ws1"
LEFT JOIN "_u_0" AS "_u_0"
  ON "_u_0"."ws_order_number" = "ws1"."ws_order_number"
LEFT JOIN "_u_1" AS "_u_1"
  ON "_u_1"."wr_order_number" = "ws1"."ws_order_number"
JOIN "customer_address" AS "customer_address"
  ON "customer_address"."ca_address_sk" = "ws1"."ws_ship_addr_sk"
  AND "customer_address"."ca_state" = 'IN'
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date" >= '2000-4-01'
  AND "date_dim"."d_date_sk" = "ws1"."ws_ship_date_sk"
  AND (
    CAST('2000-4-01' AS DATE) + INTERVAL '60' DAY
  ) >= CAST("date_dim"."d_date" AS DATE)
JOIN "web_site" AS "web_site"
  ON "web_site"."web_company_name" = 'pri'
  AND "web_site"."web_site_sk" = "ws1"."ws_web_site_sk"
WHERE
  NOT "_u_0"."ws_order_number" IS NULL AND NOT "_u_1"."wr_order_number" IS NULL
ORDER BY
  COUNT(DISTINCT "ws1"."ws_order_number")
LIMIT 100;

--------------------------------------
-- TPC-DS 96
--------------------------------------
SELECT Count(*)
FROM   store_sales,
       household_demographics,
       time_dim,
       store
WHERE  ss_sold_time_sk = time_dim.t_time_sk
       AND ss_hdemo_sk = household_demographics.hd_demo_sk
       AND ss_store_sk = s_store_sk
       AND time_dim.t_hour = 15
       AND time_dim.t_minute >= 30
       AND household_demographics.hd_dep_count = 7
       AND store.s_store_name = 'ese'
ORDER  BY Count(*)
LIMIT 100;
SELECT
  COUNT(*) AS "_col_0"
FROM "store_sales" AS "store_sales"
JOIN "household_demographics" AS "household_demographics"
  ON "household_demographics"."hd_demo_sk" = "store_sales"."ss_hdemo_sk"
  AND "household_demographics"."hd_dep_count" = 7
JOIN "store" AS "store"
  ON "store"."s_store_name" = 'ese' AND "store"."s_store_sk" = "store_sales"."ss_store_sk"
JOIN "time_dim" AS "time_dim"
  ON "store_sales"."ss_sold_time_sk" = "time_dim"."t_time_sk"
  AND "time_dim"."t_hour" = 15
  AND "time_dim"."t_minute" >= 30
ORDER BY
  COUNT(*)
LIMIT 100;

--------------------------------------
-- TPC-DS 97
--------------------------------------
WITH ssci
     AS (SELECT ss_customer_sk customer_sk,
                ss_item_sk     item_sk
         FROM   store_sales,
                date_dim
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_month_seq BETWEEN 1196 AND 1196 + 11
         GROUP  BY ss_customer_sk,
                   ss_item_sk),
     csci
     AS (SELECT cs_bill_customer_sk customer_sk,
                cs_item_sk          item_sk
         FROM   catalog_sales,
                date_dim
         WHERE  cs_sold_date_sk = d_date_sk
                AND d_month_seq BETWEEN 1196 AND 1196 + 11
         GROUP  BY cs_bill_customer_sk,
                   cs_item_sk)
SELECT Sum(CASE
                     WHEN ssci.customer_sk IS NOT NULL
                          AND csci.customer_sk IS NULL THEN 1
                     ELSE 0
                   END) store_only,
               Sum(CASE
                     WHEN ssci.customer_sk IS NULL
                          AND csci.customer_sk IS NOT NULL THEN 1
                     ELSE 0
                   END) catalog_only,
               Sum(CASE
                     WHEN ssci.customer_sk IS NOT NULL
                          AND csci.customer_sk IS NOT NULL THEN 1
                     ELSE 0
                   END) store_and_catalog
FROM   ssci
       FULL OUTER JOIN csci
                    ON ( ssci.customer_sk = csci.customer_sk
                         AND ssci.item_sk = csci.item_sk )
LIMIT 100;
WITH "date_dim_2" AS (
  SELECT
    "date_dim"."d_date_sk" AS "d_date_sk",
    "date_dim"."d_month_seq" AS "d_month_seq"
  FROM "date_dim" AS "date_dim"
  WHERE
    "date_dim"."d_month_seq" <= 1207 AND "date_dim"."d_month_seq" >= 1196
), "ssci" AS (
  SELECT
    "store_sales"."ss_customer_sk" AS "customer_sk",
    "store_sales"."ss_item_sk" AS "item_sk"
  FROM "store_sales" AS "store_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  GROUP BY
    "store_sales"."ss_customer_sk",
    "store_sales"."ss_item_sk"
), "csci" AS (
  SELECT
    "catalog_sales"."cs_bill_customer_sk" AS "customer_sk",
    "catalog_sales"."cs_item_sk" AS "item_sk"
  FROM "catalog_sales" AS "catalog_sales"
  JOIN "date_dim_2" AS "date_dim"
    ON "catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk"
  GROUP BY
    "catalog_sales"."cs_bill_customer_sk",
    "catalog_sales"."cs_item_sk"
)
SELECT
  SUM(
    CASE
      WHEN "csci"."customer_sk" IS NULL AND NOT "ssci"."customer_sk" IS NULL
      THEN 1
      ELSE 0
    END
  ) AS "store_only",
  SUM(
    CASE
      WHEN "ssci"."customer_sk" IS NULL AND NOT "csci"."customer_sk" IS NULL
      THEN 1
      ELSE 0
    END
  ) AS "catalog_only",
  SUM(
    CASE
      WHEN NOT "csci"."customer_sk" IS NULL AND NOT "ssci"."customer_sk" IS NULL
      THEN 1
      ELSE 0
    END
  ) AS "store_and_catalog"
FROM "ssci"
FULL JOIN "csci"
  ON "csci"."customer_sk" = "ssci"."customer_sk" AND "csci"."item_sk" = "ssci"."item_sk"
LIMIT 100;

--------------------------------------
-- TPC-DS 98
--------------------------------------
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       Sum(ss_ext_sales_price)                                   AS itemrevenue,
       Sum(ss_ext_sales_price) * 100 / Sum(Sum(ss_ext_sales_price))
                                         OVER (
                                           PARTITION BY i_class) AS revenueratio
FROM   store_sales,
       item,
       date_dim
WHERE  ss_item_sk = i_item_sk
       AND i_category IN ( 'Men', 'Home', 'Electronics' )
       AND ss_sold_date_sk = d_date_sk
       AND d_date BETWEEN CAST('2000-05-18' AS DATE) AND (
                          CAST('2000-05-18' AS DATE) + INTERVAL '30' DAY )
GROUP  BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
ORDER  BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;
SELECT
  "item"."i_item_id" AS "i_item_id",
  "item"."i_item_desc" AS "i_item_desc",
  "item"."i_category" AS "i_category",
  "item"."i_class" AS "i_class",
  "item"."i_current_price" AS "i_current_price",
  SUM("store_sales"."ss_ext_sales_price") AS "itemrevenue",
  SUM("store_sales"."ss_ext_sales_price") * 100 / SUM(SUM("store_sales"."ss_ext_sales_price")) OVER (PARTITION BY "item"."i_class") AS "revenueratio"
FROM "store_sales" AS "store_sales"
JOIN "date_dim" AS "date_dim"
  ON "date_dim"."d_date_sk" = "store_sales"."ss_sold_date_sk"
  AND CAST("date_dim"."d_date" AS DATE) <= CAST('2000-06-17' AS DATE)
  AND CAST("date_dim"."d_date" AS DATE) >= CAST('2000-05-18' AS DATE)
JOIN "item" AS "item"
  ON "item"."i_category" IN ('Men', 'Home', 'Electronics')
  AND "item"."i_item_sk" = "store_sales"."ss_item_sk"
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
  "revenueratio";

--------------------------------------
-- TPC-DS 99
--------------------------------------
SELECT Substr(w_warehouse_name, 1, 20),
               sm_type,
               cc_name,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS "30 days",
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 30 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 60 ) THEN 1
                     ELSE 0
                   END) AS "31-60 days",
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 60 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 90 ) THEN 1
                     ELSE 0
                   END) AS "61-90 days",
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 90 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 120 ) THEN
                     1
                     ELSE 0
                   END) AS "91-120 days",
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS ">120 days"
FROM   catalog_sales,
       warehouse,
       ship_mode,
       call_center,
       date_dim
WHERE  d_month_seq BETWEEN 1200 AND 1200 + 11
       AND cs_ship_date_sk = d_date_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_ship_mode_sk = sm_ship_mode_sk
       AND cs_call_center_sk = cc_call_center_sk
GROUP  BY Substr(w_warehouse_name, 1, 20),
          sm_type,
          cc_name
ORDER  BY Substr(w_warehouse_name, 1, 20),
          sm_type,
          cc_name
LIMIT 100;
SELECT
  SUBSTR("warehouse"."w_warehouse_name", 1, 20) AS "_col_0",
  "ship_mode"."sm_type" AS "sm_type",
  "call_center"."cc_name" AS "cc_name",
  SUM(
    CASE
      WHEN "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" <= 30
      THEN 1
      ELSE 0
    END
  ) AS "30 days",
  SUM(
    CASE
      WHEN "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" <= 60
      AND "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" > 30
      THEN 1
      ELSE 0
    END
  ) AS "31-60 days",
  SUM(
    CASE
      WHEN "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" <= 90
      AND "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" > 60
      THEN 1
      ELSE 0
    END
  ) AS "61-90 days",
  SUM(
    CASE
      WHEN "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" <= 120
      AND "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" > 90
      THEN 1
      ELSE 0
    END
  ) AS "91-120 days",
  SUM(
    CASE
      WHEN "catalog_sales"."cs_ship_date_sk" - "catalog_sales"."cs_sold_date_sk" > 120
      THEN 1
      ELSE 0
    END
  ) AS ">120 days"
FROM "catalog_sales" AS "catalog_sales"
JOIN "call_center" AS "call_center"
  ON "call_center"."cc_call_center_sk" = "catalog_sales"."cs_call_center_sk"
JOIN "date_dim" AS "date_dim"
  ON "catalog_sales"."cs_ship_date_sk" = "date_dim"."d_date_sk"
  AND "date_dim"."d_month_seq" <= 1211
  AND "date_dim"."d_month_seq" >= 1200
JOIN "ship_mode" AS "ship_mode"
  ON "catalog_sales"."cs_ship_mode_sk" = "ship_mode"."sm_ship_mode_sk"
JOIN "warehouse" AS "warehouse"
  ON "catalog_sales"."cs_warehouse_sk" = "warehouse"."w_warehouse_sk"
GROUP BY
  SUBSTR("warehouse"."w_warehouse_name", 1, 20),
  "ship_mode"."sm_type",
  "call_center"."cc_name"
ORDER BY
  "_col_0",
  "sm_type",
  "cc_name"
LIMIT 100;

