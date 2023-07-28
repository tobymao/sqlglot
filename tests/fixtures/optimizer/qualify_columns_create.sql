# title: Qualify columns for a create with CTE
# execute: false
WITH cte AS (SELECT a FROM db.y) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT a AS a FROM db.y AS y) CREATE TABLE s AS SELECT cte.a AS a FROM cte;

# title: Qualify columns for a create with complex CTE with subquery
# execute: false
WITH cte AS (SELECT a FROM (SELECT a from db.x)) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM db.x AS x) AS _q_0) CREATE TABLE s AS SELECT cte.a AS a FROM cte;

# title: Qualify columns for multiple CTEs
# execute: false
WITH cte1 AS (SELECT a FROM db.y), cte2 AS (SELECT a FROM cte1) CREATE TABLE s AS SELECT * FROM cte2;
WITH cte1 AS (SELECT a AS a FROM db.y AS y), cte2 AS (SELECT cte1.a AS a FROM cte1) CREATE TABLE s AS SELECT cte2.a AS a FROM cte2;

# title: Qualify columns for multiple CTEs, selecting only from the first CTE (unnecessary code)
# execute: false
WITH cte1 AS (SELECT a FROM db.y), cte2 AS (SELECT a FROM cte1) CREATE TABLE s AS SELECT * FROM cte1;
WITH cte1 AS (SELECT a AS a FROM db.y AS y), cte2 AS (SELECT cte1.a AS a FROM cte1) CREATE TABLE s AS SELECT cte1.a AS a FROM cte1;

# title: Qualify columns with subqueries
# execute: false
CREATE TABLE s AS SELECT * FROM (SELECT a FROM (SELECT a FROM db.y));
CREATE TABLE s AS SELECT _q_1.a AS a FROM (SELECT _q_0.a AS a FROM (SELECT a AS a FROM db.y AS y) AS _q_0) AS _q_1;

# title: Qualify columns with a cte and subquery
# execute: false
WITH cte AS (SELECT a FROM db.y) CREATE TABLE s AS SELECT * FROM (SELECT a FROM (SELECT a FROM cte));
WITH cte AS (SELECT a AS a FROM db.y AS y) CREATE TABLE s AS SELECT _q_1.a AS a FROM (SELECT _q_0.a AS a FROM (SELECT cte.a AS a FROM cte AS cte) AS _q_0) AS _q_1;
