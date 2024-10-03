SET x TO 1;
SET x = 1;

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
        GROUP BY x, GROUPING SETS (a, (b, c)), CUBE(y, z)
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
    z AND e AS b
  FROM cte
  WHERE
    x IN (1, 2, 3) AND z < -1 OR z > 1 AND w = 'AND'
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
      CASE x AND 1 + 1 = 2
        WHEN TRUE
        THEN 1 AND 4 + 3 AND Z
        WHEN x AND y
        THEN 2
        ELSE 3 AND 4 AND g
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
      MAX(COALESCE(x AND y, a AND b AND c, d AND e)),
      FOO(CASE WHEN a AND b THEN c AND d ELSE 3 END)
    GROUP BY
      x,
      GROUPING SETS (
        a,
        (b, c)
      ),
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
        c > 1 AND d > 1
      ) OR e > 1
    GROUP BY
      a
    HAVING
      a > 1
    LIMIT 10
  ) AS z
) AS y
  ON x.a = y.b AND x.a > 1 OR (
    x.c = y.d OR x.c = y.e
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
  a > 1 AND b > 2 OR c > 3;

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
INSERT OVERWRITE TABLE x
VALUES
  (1, 2.0, '3.0'),
  (4, 5.0, '6.0');

INSERT INTO TABLE foo REPLACE WHERE cond SELECT * FROM bar;
INSERT INTO foo
REPLACE WHERE cond
SELECT
  *
FROM bar;

INSERT OVERWRITE TABLE zipcodes PARTITION(state = '0') VALUES (896, 'US', 'TAMPA', 33607);
INSERT OVERWRITE TABLE zipcodes PARTITION(state = '0')
VALUES
  (896, 'US', 'TAMPA', 33607);

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

CREATE TABLE "t_customer_account" ( "id" int, "customer_id" int, "bank" varchar(100), "account_no" varchar(100));
CREATE TABLE "t_customer_account" (
  "id" INT,
  "customer_id" INT,
  "bank" VARCHAR(100),
  "account_no" VARCHAR(100)
);


SELECT
x("aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccc", "ddddddddddddd", "eeeeeeeeeeeee", "fffffff"),
array("aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccc", "ddddddddddddd", "eeeeeeeeeeeee", "fffffff"),
array("aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccc", "ddddddddddddd", "eeeeeeeeeeeee", "fffffff", array("aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccc", "ddddddddddddd", "eeeeeeeeeeeee", "fffffff")),
array(array("aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccc", "ddddddddddddd", "eeeeeeeeeeeee", "fffffff")),
;
SELECT
  X(
    "aaaaaaaaaaaaaa",
    "bbbbbbbbbbbbb",
    "ccccccccc",
    "ddddddddddddd",
    "eeeeeeeeeeeee",
    "fffffff"
  ),
  ARRAY(
    "aaaaaaaaaaaaaa",
    "bbbbbbbbbbbbb",
    "ccccccccc",
    "ddddddddddddd",
    "eeeeeeeeeeeee",
    "fffffff"
  ),
  ARRAY(
    "aaaaaaaaaaaaaa",
    "bbbbbbbbbbbbb",
    "ccccccccc",
    "ddddddddddddd",
    "eeeeeeeeeeeee",
    "fffffff",
    ARRAY(
      "aaaaaaaaaaaaaa",
      "bbbbbbbbbbbbb",
      "ccccccccc",
      "ddddddddddddd",
      "eeeeeeeeeeeee",
      "fffffff"
    )
  ),
  ARRAY(
    ARRAY(
      "aaaaaaaaaaaaaa",
      "bbbbbbbbbbbbb",
      "ccccccccc",
      "ddddddddddddd",
      "eeeeeeeeeeeee",
      "fffffff"
    )
  );
/*
  multi
  line
  comment
*/
SELECT * FROM foo;
/*
  multi
  line
  comment
*/
SELECT
  *
FROM foo;
SELECT x FROM a.b.c /*x*/, e.f.g /*x*/;
SELECT
  x
FROM a.b.c /* x */, e.f.g /* x */;
SELECT x FROM (SELECT * FROM bla /*x*/WHERE id = 1) /*x*/;
SELECT
  x
FROM (
  SELECT
    *
  FROM bla /* x */
  WHERE
    id = 1
) /* x */;
SELECT * /* multi
   line
   comment */;
SELECT
  * /* multi
   line
   comment */;
WITH table_data AS (
    SELECT 'bob' AS name, ARRAY['banana', 'apple', 'orange'] AS fruit_basket
)
SELECT
    name,
    fruit,
    basket_index
FROM table_data
CROSS JOIN UNNEST(fruit_basket) WITH ORDINALITY AS fruit(basket_index);
WITH table_data AS (
  SELECT
    'bob' AS name,
    ARRAY('banana', 'apple', 'orange') AS fruit_basket
)
SELECT
  name,
  fruit,
  basket_index
FROM table_data
CROSS JOIN UNNEST(fruit_basket) WITH ORDINALITY AS fruit(basket_index);
SELECT A.* EXCEPT A.COL_1, A.COL_2 FROM TABLE_1 A;
SELECT
  A.*
  EXCEPT (A.COL_1),
  A.COL_2
FROM TABLE_1 AS A;

SELECT *
FROM a
JOIN b
  JOIN c
    ON b.id = c.id
  ON a.id = b.id
CROSS JOIN d
JOIN e
  ON d.id = e.id;
SELECT
  *
FROM a
JOIN b
  JOIN c
    ON b.id = c.id
  ON a.id = b.id
CROSS JOIN d
JOIN e
  ON d.id = e.id;

SELECT * FROM a JOIN b JOIN c USING (e) JOIN d USING (f) USING (g);
SELECT
  *
FROM a
JOIN b
  JOIN c
    USING (e)
  JOIN d
    USING (f)
  USING (g);

('aaaaaaaaaaa', 'bbbbbbbbbbbbbbbb', 'ccccccccccccc', 'ddddddddddd', 'eeeeeeeeeeeeeeeeeeeee');
(
  'aaaaaaaaaaa',
  'bbbbbbbbbbbbbbbb',
  'ccccccccccccc',
  'ddddddddddd',
  'eeeeeeeeeeeeeeeeeeeee'
);

/* COMMENT */
INSERT FIRST WHEN salary > 4000 THEN INTO emp2
             WHEN salary > 5000 THEN INTO emp3
             WHEN salary > 6000 THEN INTO emp4
SELECT salary FROM employees;
/* COMMENT */
INSERT FIRST
  WHEN salary > 4000 THEN INTO emp2
  WHEN salary > 5000 THEN INTO emp3
  WHEN salary > 6000 THEN INTO emp4
SELECT
  salary
FROM employees;

SELECT *
FROM foo
wHERE 1=1
    AND
        -- my comment
        EXISTS (
            SELECT 1
            FROM bar
        );
SELECT
  *
FROM foo
WHERE
  1 = 1 AND EXISTS(
    SELECT
      1
    FROM bar
  ) /* my comment */;

SELECT 1
FROM foo
WHERE 1=1
AND -- first comment
    -- second comment
    foo.a = 1;
SELECT
  1
FROM foo
WHERE
  1 = 1 AND /* first comment */ foo.a /* second comment */ = 1;
