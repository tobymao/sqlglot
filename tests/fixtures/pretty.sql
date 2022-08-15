SELECT * FROM test;
SELECT
  *
FROM test;

WITH a AS ((SELECT 1 AS b) UNION ALL (SELECT 2 AS b)) SELECT * FROM a;
WITH a AS (
  (
    SELECT
      1 AS b
  )
  UNION ALL
  (
    SELECT
      2 AS b
  )
)
SELECT
  *
FROM a;

WITH cte1 AS (
    SELECT a, z and e AS b
    FROM cte
    WHERE x IN (1, 2, 3) AND z < -1 OR z > 1 AND w = 'AND'
), cte2 AS (
    SELECT RANK() OVER (PARTITION BY a, b ORDER BY x DESC) a, b
    FROM cte
    CROSS JOIN (
        SELECT 1
        UNION ALL
        SELECT 2
        UNION ALL
        SELECT CASE x AND 1 + 1 = 2
        WHEN TRUE THEN 1 AND 4 + 3 AND Z
        WHEN x and y THEN 2
        ELSE 3 AND 4 AND g END
        UNION ALL
        SELECT 1
        FROM (SELECT 1) AS x, y, (SELECT 2) z
        UNION ALL
        SELECT MAX(COALESCE(x AND y, a and b and c, d and e)), FOO(CASE WHEN a and b THEN c and d ELSE 3 END)
        GROUP BY x, GROUPING SETS (a, (b, c)) CUBE(y, z)
    ) x
)
SELECT a, b c FROM (
    SELECT a w, 1 + 1 AS c
    FROM foo
    WHERE w IN (SELECT z FROM q)
    GROUP BY a, b
) x
LEFT JOIN (
    SELECT a, b
    FROM (SELECT * FROM bar WHERE (c > 1 AND d > 1) OR e > 1 GROUP BY a HAVING a > 1 LIMIT 10) z
) y ON x.a = y.b AND x.a > 1 OR (x.c = y.d OR x.c = y.e);
WITH cte1 AS (
  SELECT
    a,
    z
    AND e AS b
  FROM cte
  WHERE
    x IN (1, 2, 3)
    AND z < -1
    OR z > 1
    AND w = 'AND'
), cte2 AS (
  SELECT
    RANK() OVER (PARTITION BY a, b ORDER BY x DESC) AS a,
    b
  FROM cte
  CROSS JOIN (
    SELECT
      1
    UNION ALL
    SELECT
      2
    UNION ALL
    SELECT
      CASE x
        AND 1 + 1 = 2
        WHEN TRUE
        THEN 1
          AND 4 + 3
          AND Z
        WHEN x
          AND y
        THEN 2
        ELSE 3
          AND 4
          AND g
      END
    UNION ALL
    SELECT
      1
    FROM (
      SELECT
        1
    ) AS x, y, (
      SELECT
        2
    ) AS z
    UNION ALL
    SELECT
      MAX(COALESCE(x
          AND y, a
          AND b
          AND c, d
      AND e)),
      FOO(CASE
          WHEN a
            AND b
          THEN c
            AND d
          ELSE 3
      END)
    GROUP BY
      x
    GROUPING SETS (
      a,
      (b, c)
    )
    CUBE (
      y,
      z
    )
  ) AS x
)
SELECT
  a,
  b AS c
FROM (
  SELECT
    a AS w,
    1 + 1 AS c
  FROM foo
  WHERE
    w IN (
      SELECT
        z
      FROM q
    )
  GROUP BY
    a,
    b
) AS x
LEFT JOIN (
  SELECT
    a,
    b
  FROM (
    SELECT
      *
    FROM bar
    WHERE
      (
        c > 1
        AND d > 1
      )
      OR e > 1
    GROUP BY
      a
    HAVING
      a > 1
    LIMIT 10
  ) AS z
) AS y
  ON x.a = y.b
  AND x.a > 1
  OR (
    x.c = y.d
    OR x.c = y.e
  );

SELECT myCol1, myCol2 FROM baseTable LATERAL VIEW OUTER explode(col1) myTable1 AS myCol1 LATERAL VIEW explode(col2) myTable2 AS myCol2
where a > 1 and b > 2 or c > 3;

SELECT
  myCol1,
  myCol2
FROM baseTable
LATERAL VIEW OUTER
EXPLODE(col1) myTable1 AS myCol1
LATERAL VIEW
EXPLODE(col2) myTable2 AS myCol2
WHERE
  a > 1
  AND b > 2
  OR c > 3;

SELECT * FROM (WITH y AS ( SELECT 1 AS z) SELECT z from y) x;
SELECT
  *
FROM (
  WITH y AS (
    SELECT
      1 AS z
  )
  SELECT
    z
  FROM y
) AS x;

INSERT OVERWRITE TABLE x VALUES (1, 2.0, '3.0'), (4, 5.0, '6.0');
INSERT OVERWRITE TABLE x VALUES
  (1, 2.0, '3.0'),
  (4, 5.0, '6.0');

WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
    ), top_regions AS (
    SELECT region
    FROM regional_sales
    WHERE total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)
)
SELECT region,
product,
SUM(quantity) AS product_units,
SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;
WITH regional_sales AS (
  SELECT
    region,
    SUM(amount) AS total_sales
  FROM orders
  GROUP BY
    region
), top_regions AS (
  SELECT
    region
  FROM regional_sales
  WHERE
    total_sales > (
      SELECT
        SUM(total_sales) / 10
      FROM regional_sales
    )
)
SELECT
  region,
  product,
  SUM(quantity) AS product_units,
  SUM(amount) AS product_sales
FROM orders
WHERE
  region IN (
    SELECT
      region
    FROM top_regions
  )
GROUP BY
  region,
  product;

CREATE TABLE `t_customer_account` ( `id` int, `customer_id` int, `bank` varchar(100), `account_no` varchar(100));
CREATE TABLE `t_customer_account` (
  `id` INT,
  `customer_id` INT,
  `bank` VARCHAR(100),
  `account_no` VARCHAR(100)
);

CREATE TABLE `t_customer_account` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `customer_id` int(11) DEFAULT NULL COMMENT '客户id',
  `bank` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '行别',
  `account_no` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '账号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='客户账户表';
CREATE TABLE `t_customer_account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `customer_id` INT(11) DEFAULT NULL COMMENT '客户id',
  `bank` VARCHAR(100) DEFAULT NULL COLLATE utf8_bin COMMENT '行别',
  `account_no` VARCHAR(100) DEFAULT NULL COLLATE utf8_bin COMMENT '账号',
  PRIMARY KEY(`id`)
)
ENGINE=InnoDB
AUTO_INCREMENT=1
DEFAULT CHARACTER SET=utf8
COLLATE=utf8_bin
COMMENT='客户账户表';
