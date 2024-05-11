# title: Create with CTE
WITH cte AS (SELECT b FROM y) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS SELECT cte.b AS b FROM cte AS cte;

# title: Create with CTE, query also has CTE
WITH cte1 AS (SELECT b FROM y) CREATE TABLE s AS WITH cte2 AS (SELECT b FROM cte1) SELECT * FROM cte2;
WITH cte1 AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS WITH cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) SELECT cte2.b AS b FROM cte2 AS cte2;

# title: Create without CTE
CREATE TABLE foo AS SELECT a FROM tbl;
CREATE TABLE foo AS SELECT tbl.a AS a FROM tbl AS tbl;

# title: Create with complex CTE with derived table
WITH cte AS (SELECT a FROM (SELECT a FROM x)) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0) CREATE TABLE s AS SELECT cte.a AS a FROM cte AS cte;

# title: Create wtih multiple CTEs
WITH cte1 AS (SELECT b FROM y), cte2 AS (SELECT b FROM cte1) CREATE TABLE s AS SELECT * FROM cte2;
WITH cte1 AS (SELECT y.b AS b FROM y AS y), cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) CREATE TABLE s AS SELECT cte2.b AS b FROM cte2 AS cte2;

# title: Create with multiple CTEs, selecting only from the first CTE (unnecessary code)
WITH cte1 AS (SELECT b FROM y), cte2 AS (SELECT b FROM cte1) CREATE TABLE s AS SELECT * FROM cte1;
WITH cte1 AS (SELECT y.b AS b FROM y AS y), cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) CREATE TABLE s AS SELECT cte1.b AS b FROM cte1 AS cte1;

# title: Create with multiple derived tables
CREATE TABLE s AS SELECT * FROM (SELECT b FROM (SELECT b FROM y));
CREATE TABLE s AS SELECT _q_1.b AS b FROM (SELECT _q_0.b AS b FROM (SELECT y.b AS b FROM y AS y) AS _q_0) AS _q_1;

# title: Create with a CTE and a derived table
WITH cte AS (SELECT b FROM y) CREATE TABLE s AS SELECT * FROM (SELECT b FROM (SELECT b FROM cte));
WITH cte AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS SELECT _q_1.b AS b FROM (SELECT _q_0.b AS b FROM (SELECT cte.b AS b FROM cte AS cte) AS _q_0) AS _q_1;

# title: Insert with CTE
# dialect: spark
WITH cte AS (SELECT b FROM y) INSERT INTO s SELECT * FROM cte;
WITH cte AS (SELECT y.b AS b FROM y AS y) INSERT INTO s SELECT cte.b AS b FROM cte AS cte;

# title: Insert without CTE
INSERT INTO foo SELECT a FROM tbl;
INSERT INTO foo SELECT tbl.a AS a FROM tbl AS tbl;
