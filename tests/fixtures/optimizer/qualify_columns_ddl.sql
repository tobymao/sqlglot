# title: Create with CTE
WITH cte AS (SELECT b FROM y) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS SELECT cte.b AS b FROM cte AS cte;

# title: Create with CTE, query also has CTE
WITH cte1 AS (SELECT b FROM y) CREATE TABLE s AS WITH cte2 AS (SELECT b FROM cte1) SELECT * FROM cte2;
WITH cte1 AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS WITH cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) SELECT cte2.b AS b FROM cte2 AS cte2;

# title: Create without CTE
# schema: {"tbl": {"a": "INT"}}
CREATE TABLE foo AS SELECT a FROM tbl;
CREATE TABLE foo AS SELECT tbl.a AS a FROM tbl AS tbl;

# title: Create with complex CTE with derived table
WITH cte AS (SELECT a FROM (SELECT a FROM x)) CREATE TABLE s AS SELECT * FROM cte;
WITH cte AS (SELECT _0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _0) CREATE TABLE s AS SELECT cte.a AS a FROM cte AS cte;

# title: Create wtih multiple CTEs
WITH cte1 AS (SELECT b FROM y), cte2 AS (SELECT b FROM cte1) CREATE TABLE s AS SELECT * FROM cte2;
WITH cte1 AS (SELECT y.b AS b FROM y AS y), cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) CREATE TABLE s AS SELECT cte2.b AS b FROM cte2 AS cte2;

# title: Create with multiple CTEs, selecting only from the first CTE (unnecessary code)
WITH cte1 AS (SELECT b FROM y), cte2 AS (SELECT b FROM cte1) CREATE TABLE s AS SELECT * FROM cte1;
WITH cte1 AS (SELECT y.b AS b FROM y AS y), cte2 AS (SELECT cte1.b AS b FROM cte1 AS cte1) CREATE TABLE s AS SELECT cte1.b AS b FROM cte1 AS cte1;

# title: Create with multiple derived tables
CREATE TABLE s AS SELECT * FROM (SELECT b FROM (SELECT b FROM y));
CREATE TABLE s AS SELECT _1.b AS b FROM (SELECT _0.b AS b FROM (SELECT y.b AS b FROM y AS y) AS _0) AS _1;

# title: Create with a CTE and a derived table
WITH cte AS (SELECT b FROM y) CREATE TABLE s AS SELECT * FROM (SELECT b FROM (SELECT b FROM cte));
WITH cte AS (SELECT y.b AS b FROM y AS y) CREATE TABLE s AS SELECT _1.b AS b FROM (SELECT _0.b AS b FROM (SELECT cte.b AS b FROM cte AS cte) AS _0) AS _1;

# title: Insert with CTE
# dialect: spark
WITH cte AS (SELECT b FROM y) INSERT INTO s SELECT * FROM cte;
WITH cte AS (SELECT y.b AS b FROM y AS y) INSERT INTO s SELECT cte.b AS b FROM cte AS cte;

# title: Insert without CTE
# schema: {"tbl": {"a": "INT"}}
INSERT INTO foo SELECT a FROM tbl;
INSERT INTO foo SELECT tbl.a AS a FROM tbl AS tbl;

# title: Insert column list is validated against the target
INSERT INTO x (a, b) SELECT b, c FROM y;
INSERT INTO x (a, b) SELECT y.b AS b, y.c AS c FROM y AS y;

# title: Update resolves columns against the target and FROM relations, assigned columns always belong to the target
UPDATE x SET b = c FROM y WHERE a = y.b;
UPDATE x SET b = y.c FROM y AS y WHERE x.a = y.b;

# title: Update target named through a FROM alias
# dialect: tsql
UPDATE t SET a = c FROM x AS t JOIN y ON t.b = y.b;
UPDATE t SET a = y.c FROM x AS t JOIN y AS y ON t.b = y.b;

# title: Delete with USING and a correlated subquery
DELETE FROM x USING y WHERE x.a = y.b AND c IN (SELECT b FROM z WHERE z.c = a);
DELETE FROM x USING y AS y WHERE x.a = y.b AND y.c IN (SELECT z.b AS b FROM z AS z WHERE z.c = x.a);

# title: Merge resolves columns against the target and USING relation
MERGE INTO x AS t USING (SELECT b, c FROM y) AS s ON t.a = s.b WHEN MATCHED THEN UPDATE SET b = c WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.b, c);
MERGE INTO x AS t USING (SELECT y.b AS b, y.c AS c FROM y AS y) AS s ON t.a = s.b WHEN MATCHED THEN UPDATE SET b = s.c WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.b, s.c);

# title: Update target is outside its FROM relations' USING join chain
UPDATE x SET a = z.c FROM y JOIN z USING (b) WHERE x.b = z.b;
UPDATE x SET a = z.c FROM y AS y JOIN z AS z ON y.b = z.b WHERE x.b = z.b;
